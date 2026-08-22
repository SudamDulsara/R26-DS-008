from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Mapping, Optional, Union
from zoneinfo import ZoneInfo

from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger
from news_pipeline.unification.gpt_contract import (
    GPT_PROMPT_VERSION_V2_9,
    GPT_PROMPT_VERSION_V2_10,
)
from news_pipeline.unification.production import (
    GPT_PUBLICATION_STATUS_PENDING_REVIEW,
    GPT_PUBLICATION_STATUS_PUBLISHABLE,
    GPT_PUBLICATION_STATUS_UNAVAILABLE,
    REVIEWED_CORRECTION_FOLLOWUP_TARGET,
    REVIEWED_CORRECTION_LINEAGE_TARGET,
    REVIEWED_CORRECTION_TARGET,
    _load_generation_candidates,
    build_generation_identity,
    gpt_publication_state,
    load_cached_version,
    resolve_reviewed_correction_lineage,
)


FINAL_PUBLICATION_VERSION = "hybrid_final_publication_v2"
PUBLICATION_IDENTITY_CACHE_REVISION = "generation_identity_contract_v1"
LOCAL_TIME_ZONE = ZoneInfo("Asia/Colombo")
COMPLETION_REVIEW_TARGET = "v2_9_completion_raw_candidate"
CORRECTION_REVIEW_TARGET = "v2_10_prison_correction_raw_candidate"
REMEDIATION_REVIEW_TARGET = "v2_10_reviewed_remediation_raw_candidate"
COMPLETION_REASONING_EFFORT = "none"
COMPLETION_MAX_OUTPUT_TOKENS = 8192
logger = get_logger()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _optional_identity_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _publication_identity_cache_key(
    *,
    cluster: Mapping[str, Any],
    members: list[dict[str, Any]],
    article_records: Mapping[int, Mapping[str, Any]],
    config: PipelineConfig,
) -> str:
    representative_article_id = cluster.get("representative_article_id")
    if representative_article_id is None:
        raise ValueError("cluster has no representative article")
    representative_article_id = int(representative_article_id)
    articles = []
    for member in sorted(members, key=lambda item: int(item["article_id"])):
        article_id = int(member["article_id"])
        article = article_records.get(article_id)
        if article is None:
            raise ValueError(
                f"article record is missing for member article {article_id}"
            )
        articles.append(
            {
                "article_id": article_id,
                "url": _optional_identity_text(
                    article.get("url") or member.get("url")
                ),
                "publisher": _optional_identity_text(
                    article.get("source") or member.get("source")
                )
                or "",
                "title": _optional_identity_text(
                    article.get("title") or member.get("title")
                ),
                "published_date": _optional_identity_text(
                    article.get("published_date")
                    or member.get("published_date")
                ),
                "clean_text": _optional_identity_text(
                    article.get("clean_text")
                )
                or "",
                "is_representative": (
                    article_id == representative_article_id
                ),
            }
        )
    payload = {
        "cache_revision": PUBLICATION_IDENTITY_CACHE_REVISION,
        "cluster_key": str(cluster["cluster_key"]),
        "representative_article_id": representative_article_id,
        "articles": articles,
        "request_contract": {
            "model": config.gpt_model,
            "prompt_version": config.gpt_prompt_version,
            "schema_version": config.gpt_schema_version,
            "reasoning_effort": config.gpt_reasoning_effort,
            "max_output_tokens": config.gpt_max_output_tokens,
        },
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class _CachedGenerationIdentity:
    source_fingerprint_sha256: str
    input_fingerprint_sha256: str
    request_fingerprint_sha256: str


class _PublicationIdentityCache:
    def __init__(self, connection: Any, *, enabled: bool) -> None:
        self.connection = connection
        self.enabled = enabled
        self.pending: dict[str, tuple[Any, ...]] = {}
        self.memory: dict[str, _CachedGenerationIdentity] = {}
        self.metrics: dict[str, Any] = {
            "enabled": enabled,
            "hits": 0,
            "misses": 0,
            "uncached_builds": 0,
            "cache_writes": 0,
            "key_seconds": 0.0,
            "lookup_seconds": 0.0,
            "build_seconds": 0.0,
            "write_seconds": 0.0,
        }

    def get_or_build(
        self,
        *,
        cluster: Mapping[str, Any],
        members: list[dict[str, Any]],
        article_records: Mapping[int, Mapping[str, Any]],
        config: PipelineConfig,
    ) -> _CachedGenerationIdentity:
        key_started = perf_counter()
        cache_key = _publication_identity_cache_key(
            cluster=cluster,
            members=members,
            article_records=article_records,
            config=config,
        )
        self.metrics["key_seconds"] += perf_counter() - key_started

        if self.enabled:
            memory_identity = self.memory.get(cache_key)
            if memory_identity is not None:
                self.metrics["hits"] += 1
                return memory_identity
            lookup_started = perf_counter()
            row = self.connection.execute(
                """
                SELECT source_fingerprint_sha256,
                       input_fingerprint_sha256,
                       request_fingerprint_sha256
                FROM publication_generation_identity_cache
                WHERE cache_key_sha256 = ?
                  AND cache_revision = ?
                """,
                (cache_key, PUBLICATION_IDENTITY_CACHE_REVISION),
            ).fetchone()
            self.metrics["lookup_seconds"] += perf_counter() - lookup_started
            if row is not None:
                identity = _CachedGenerationIdentity(
                    source_fingerprint_sha256=str(row[0]),
                    input_fingerprint_sha256=str(row[1]),
                    request_fingerprint_sha256=str(row[2]),
                )
                self.memory[cache_key] = identity
                self.metrics["hits"] += 1
                return identity
            self.metrics["misses"] += 1
        else:
            self.metrics["uncached_builds"] += 1

        build_started = perf_counter()
        built = build_generation_identity(
            cluster=cluster,
            members=members,
            article_records_by_id=article_records,
            config=config,
        )
        self.metrics["build_seconds"] += perf_counter() - build_started
        identity = _CachedGenerationIdentity(
            source_fingerprint_sha256=built.source_fingerprint_sha256,
            input_fingerprint_sha256=built.input_fingerprint_sha256,
            request_fingerprint_sha256=built.request_fingerprint_sha256,
        )
        if self.enabled:
            now = datetime.now(LOCAL_TIME_ZONE).isoformat(timespec="seconds")
            self.memory[cache_key] = identity
            self.pending[cache_key] = (
                cache_key,
                PUBLICATION_IDENTITY_CACHE_REVISION,
                str(cluster["cluster_key"]),
                config.gpt_prompt_version,
                config.gpt_schema_version,
                config.gpt_model,
                config.gpt_reasoning_effort,
                int(config.gpt_max_output_tokens),
                identity.source_fingerprint_sha256,
                identity.input_fingerprint_sha256,
                identity.request_fingerprint_sha256,
                now,
                now,
            )
        return identity

    def persist(self) -> None:
        if not self.pending:
            return
        write_started = perf_counter()
        before_changes = self.connection.total_changes
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO publication_generation_identity_cache (
                cache_key_sha256, cache_revision, cluster_key,
                prompt_version, schema_version, model_name,
                reasoning_effort, max_output_tokens,
                source_fingerprint_sha256, input_fingerprint_sha256,
                request_fingerprint_sha256, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self.pending.values(),
        )
        self.metrics["cache_writes"] = (
            self.connection.total_changes - before_changes
        )
        self.metrics["write_seconds"] += perf_counter() - write_started


def _reviewed_v2_9_completion(
    connection: Any,
    *,
    cluster: Mapping[str, Any],
    members: list[dict[str, Any]],
    article_records: Mapping[int, Mapping[str, Any]],
    config: PipelineConfig,
    identity_cache: _PublicationIdentityCache,
) -> Optional[dict[str, Any]]:
    """Return an exact-current, reviewed v2.9 completion candidate."""
    completion_config = replace(
        config,
        gpt_prompt_version=GPT_PROMPT_VERSION_V2_9,
        gpt_reasoning_effort=COMPLETION_REASONING_EFFORT,
        gpt_max_output_tokens=COMPLETION_MAX_OUTPUT_TOKENS,
    )
    try:
        identity = identity_cache.get_or_build(
            cluster=cluster,
            members=members,
            article_records=article_records,
            config=completion_config,
        )
    except (TypeError, ValueError):
        return None
    version = load_cached_version(
        connection,
        identity.request_fingerprint_sha256,
    )
    if version is None or not version.get("human_review_source_sha256"):
        return None
    review = _json_mapping(version.get("human_review_scores_json"))
    if review.get("review_target") != COMPLETION_REVIEW_TARGET:
        return None
    if (
        gpt_publication_state(version)["publication_status"]
        != GPT_PUBLICATION_STATUS_PUBLISHABLE
    ):
        return None
    return version


def _reviewed_v2_10_correction(
    connection: Any,
    *,
    cluster: Mapping[str, Any],
    members: list[dict[str, Any]],
    article_records: Mapping[int, Mapping[str, Any]],
    config: PipelineConfig,
    identity_cache: _PublicationIdentityCache,
) -> Optional[dict[str, Any]]:
    """Return an exact-current, accepted reviewed v2.10 correction."""
    correction_config = replace(
        config,
        gpt_prompt_version=GPT_PROMPT_VERSION_V2_10,
        gpt_reasoning_effort=COMPLETION_REASONING_EFFORT,
        gpt_max_output_tokens=COMPLETION_MAX_OUTPUT_TOKENS,
    )
    try:
        identity = identity_cache.get_or_build(
            cluster=cluster,
            members=members,
            article_records=article_records,
            config=correction_config,
        )
    except (TypeError, ValueError):
        return None
    version = load_cached_version(
        connection,
        identity.request_fingerprint_sha256,
    )
    if version is None or not version.get("human_review_source_sha256"):
        return None
    review = _json_mapping(version.get("human_review_scores_json"))
    if (
        review.get("review_target") != CORRECTION_REVIEW_TARGET
        or str(version.get("human_review_decision") or "").strip()
        != "accept"
        or gpt_publication_state(version)["publication_status"]
        != GPT_PUBLICATION_STATUS_PUBLISHABLE
    ):
        return None
    return version


def _reviewed_v2_10_pending_correction(
    connection: Any,
    *,
    cluster: Mapping[str, Any],
    members: list[dict[str, Any]],
    article_records: Mapping[int, Mapping[str, Any]],
    primary_version: Mapping[str, Any],
    config: PipelineConfig,
) -> Optional[tuple[dict[str, Any], str]]:
    """Return the reviewed accepted tip of any-depth correction lineage."""
    try:
        lineage = resolve_reviewed_correction_lineage(
            connection,
            cluster=cluster,
            members=members,
            article_records_by_id=article_records,
            primary_version=primary_version,
            config=config,
        )
    except (TypeError, ValueError):
        return None
    if lineage.accepted_version is None or not lineage.steps:
        return None
    return lineage.accepted_version, str(lineage.steps[-1].review_target)


def _reviewed_v2_10_remediation(
    connection: Any,
    *,
    story_id: str,
    source_fingerprint_sha256: str,
) -> Optional[dict[str, Any]]:
    """Return the latest exact-current, accepted remediation candidate."""
    versions = connection.execute(
        """
        SELECT *
        FROM unified_story_versions
        WHERE cluster_key = ?
          AND source_fingerprint_sha256 = ?
          AND human_review_source_sha256 IS NOT NULL
        ORDER BY id DESC
        """,
        (story_id, source_fingerprint_sha256),
    )
    for row in versions:
        version = dict(row)
        review = _json_mapping(version.get("human_review_scores_json"))
        if review.get("review_target") != REMEDIATION_REVIEW_TARGET:
            continue
        if (
            str(version.get("human_review_decision") or "").strip()
            in {"accept", "minor_issue"}
            and gpt_publication_state(version)["publication_status"]
            == GPT_PUBLICATION_STATUS_PUBLISHABLE
        ):
            return version
        return None
    return None


def _internally_reviewed_v2_10_remediation(
    connection: Any,
    *,
    story_id: str,
    source_fingerprint_sha256: str,
) -> Optional[dict[str, Any]]:
    """Return an exact-current, internally audited remediation result."""
    table_exists = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'remediation_quality_reviews'
        """
    ).fetchone()
    if table_exists is None:
        return None
    row = connection.execute(
        """
        SELECT version.*
        FROM unified_story_versions AS version
        JOIN remediation_quality_reviews AS review
          ON review.unified_story_version_id = version.id
        WHERE version.cluster_key = ?
          AND version.source_fingerprint_sha256 = ?
          AND review.story_id = ?
          AND review.review_target = ?
          AND review.decision = 'accept'
          AND review.review_method = 'internal_evidence_audit'
        ORDER BY version.id DESC
        LIMIT 1
        """,
        (
            story_id,
            source_fingerprint_sha256,
            story_id,
            REMEDIATION_REVIEW_TARGET,
        ),
    ).fetchone()
    if row is None:
        return None
    version = dict(row)
    if (
        gpt_publication_state(version)["publication_status"]
        != GPT_PUBLICATION_STATUS_PUBLISHABLE
    ):
        return None
    return version


def _timezone_aware(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return datetime.now(LOCAL_TIME_ZONE).isoformat(timespec="seconds")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(LOCAL_TIME_ZONE).isoformat(timespec="seconds")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TIME_ZONE)
    return parsed.isoformat(timespec="seconds")


def _last_updated(
    cluster: Mapping[str, Any],
    version: Mapping[str, Any],
) -> str:
    candidates = [
        str(value)
        for value in (
            cluster.get("created_at"),
            version.get("created_at"),
            version.get("updated_at"),
            version.get("human_review_imported_at"),
        )
        if value
    ]
    if not candidates:
        return _timezone_aware(None)
    parsed = []
    for value in candidates:
        try:
            item = datetime.fromisoformat(value)
        except ValueError:
            continue
        if item.tzinfo is None:
            item = item.replace(tzinfo=LOCAL_TIME_ZONE)
        parsed.append(item)
    return (
        max(parsed).isoformat(timespec="seconds")
        if parsed
        else _timezone_aware(None)
    )


def _ensure_tables(connection: Any) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS final_unified_stories (
            story_id TEXT PRIMARY KEY,
            cluster_id INTEGER NOT NULL UNIQUE,
            title TEXT NOT NULL,
            story TEXT NOT NULL,
            last_updated TEXT NOT NULL,
            article_count INTEGER NOT NULL,
            FOREIGN KEY (cluster_id) REFERENCES story_clusters(id)
        );

        CREATE TABLE IF NOT EXISTS final_story_sources (
            story_id TEXT NOT NULL,
            article_id INTEGER NOT NULL,
            publisher TEXT,
            source_title TEXT,
            url TEXT,
            published_date TEXT,
            similarity_score REAL,
            is_representative INTEGER NOT NULL,
            referenced_by_gpt INTEGER NOT NULL,
            evidence_span_ids_json TEXT NOT NULL,
            PRIMARY KEY (story_id, article_id),
            FOREIGN KEY (story_id)
                REFERENCES final_unified_stories(story_id)
                ON DELETE CASCADE,
            FOREIGN KEY (article_id) REFERENCES articles(id)
        );

        CREATE TABLE IF NOT EXISTS final_story_claims (
            story_id TEXT NOT NULL,
            claim_index INTEGER NOT NULL,
            claim_text TEXT NOT NULL,
            source_article_ids_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (story_id, claim_index),
            FOREIGN KEY (story_id)
                REFERENCES final_unified_stories(story_id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS final_story_conflicts (
            story_id TEXT NOT NULL,
            conflict_index INTEGER NOT NULL,
            description TEXT NOT NULL,
            source_article_ids_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (story_id, conflict_index),
            FOREIGN KEY (story_id)
                REFERENCES final_unified_stories(story_id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS final_story_publication_states (
            story_id TEXT PRIMARY KEY,
            cluster_id INTEGER NOT NULL UNIQUE,
            publication_status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL,
            unified_story_version_id INTEGER,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (cluster_id) REFERENCES story_clusters(id),
            FOREIGN KEY (unified_story_version_id)
                REFERENCES unified_story_versions(id)
        );

        CREATE TABLE IF NOT EXISTS publication_generation_identity_cache (
            cache_key_sha256 TEXT PRIMARY KEY,
            cache_revision TEXT NOT NULL,
            cluster_key TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            model_name TEXT NOT NULL,
            reasoning_effort TEXT NOT NULL,
            max_output_tokens INTEGER NOT NULL,
            source_fingerprint_sha256 TEXT NOT NULL,
            input_fingerprint_sha256 TEXT NOT NULL,
            request_fingerprint_sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_final_story_state_status
        ON final_story_publication_states(publication_status, story_id);

        CREATE INDEX IF NOT EXISTS idx_final_story_sources_article
        ON final_story_sources(article_id, story_id);

        CREATE INDEX IF NOT EXISTS idx_articles_publication_disposition
        ON articles(
            url,
            title,
            published_date,
            clean_status,
            dedupe_status,
            quality_flags,
            duplicate_of_id
        );

        CREATE INDEX IF NOT EXISTS idx_publication_identity_cache_request
        ON publication_generation_identity_cache(
            request_fingerprint_sha256
        );
        """
    )


_PUBLICATION_TABLE_SPECS = {
    "final_unified_stories": (
        ("story_id",),
        (
            "story_id",
            "cluster_id",
            "title",
            "story",
            "last_updated",
            "article_count",
        ),
    ),
    "final_story_sources": (
        ("story_id", "article_id"),
        (
            "story_id",
            "article_id",
            "publisher",
            "source_title",
            "url",
            "published_date",
            "similarity_score",
            "is_representative",
            "referenced_by_gpt",
            "evidence_span_ids_json",
        ),
    ),
    "final_story_claims": (
        ("story_id", "claim_index"),
        (
            "story_id",
            "claim_index",
            "claim_text",
            "source_article_ids_json",
            "evidence_json",
        ),
    ),
    "final_story_conflicts": (
        ("story_id", "conflict_index"),
        (
            "story_id",
            "conflict_index",
            "description",
            "source_article_ids_json",
            "evidence_json",
        ),
    ),
    "final_story_publication_states": (
        ("story_id",),
        (
            "story_id",
            "cluster_id",
            "publication_status",
            "reason_codes_json",
            "unified_story_version_id",
            "updated_at",
        ),
    ),
}


def _publication_row_changes(
    connection: Any,
    *,
    table: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    key_columns, columns = _PUBLICATION_TABLE_SPECS[table]
    existing = {}
    for row in connection.execute(
        f"SELECT {', '.join(columns)} FROM {table}"
    ):
        values = tuple(row[column] for column in columns)
        key = tuple(row[column] for column in key_columns)
        existing[key] = values
    desired = {}
    desired_rows = {}
    for row in rows:
        key = tuple(row[column] for column in key_columns)
        if key in desired:
            raise RuntimeError(f"duplicate publication row for {table}: {key}")
        desired[key] = tuple(row[column] for column in columns)
        desired_rows[key] = row
    new_keys = desired.keys() - existing.keys()
    common_keys = desired.keys() & existing.keys()
    changed_keys = {
        key for key in common_keys if desired[key] != existing[key]
    }
    return {
        "table": table,
        "key_columns": key_columns,
        "columns": columns,
        "desired_rows": desired_rows,
        "new_keys": new_keys,
        "changed_keys": changed_keys,
        "stale_keys": existing.keys() - desired.keys(),
        "unchanged": len(common_keys) - len(changed_keys),
    }


def _sync_publication_tables(
    connection: Any,
    *,
    rows_by_table: Mapping[str, list[dict[str, Any]]],
    incremental: bool,
) -> dict[str, Any]:
    started = perf_counter()
    if not incremental:
        deleted = sum(
            int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
            )
            for table in _PUBLICATION_TABLE_SPECS
        )
        for table in (
            "final_story_sources",
            "final_story_claims",
            "final_story_conflicts",
            "final_unified_stories",
            "final_story_publication_states",
        ):
            connection.execute(f"DELETE FROM {table}")
        inserted = 0
        for table in (
            "final_unified_stories",
            "final_story_sources",
            "final_story_claims",
            "final_story_conflicts",
            "final_story_publication_states",
        ):
            _, columns = _PUBLICATION_TABLE_SPECS[table]
            rows = rows_by_table[table]
            placeholders = ", ".join(f":{column}" for column in columns)
            connection.executemany(
                f"INSERT INTO {table} ({', '.join(columns)}) "
                f"VALUES ({placeholders})",
                rows,
            )
            inserted += len(rows)
        return {
            "incremental": False,
            "inserted": inserted,
            "updated": 0,
            "deleted": deleted,
            "unchanged": 0,
            "seconds": round(perf_counter() - started, 6),
        }

    changes = {
        table: _publication_row_changes(
            connection,
            table=table,
            rows=rows_by_table[table],
        )
        for table in _PUBLICATION_TABLE_SPECS
    }
    deleted = 0
    for table in (
        "final_story_sources",
        "final_story_claims",
        "final_story_conflicts",
        "final_story_publication_states",
        "final_unified_stories",
    ):
        change = changes[table]
        key_columns = change["key_columns"]
        stale_keys = list(change["stale_keys"])
        if not stale_keys:
            continue
        predicate = " AND ".join(
            f"{column} = ?" for column in key_columns
        )
        connection.executemany(
            f"DELETE FROM {table} WHERE {predicate}",
            stale_keys,
        )
        deleted += len(stale_keys)

    inserted = 0
    updated = 0
    unchanged = 0
    for table in (
        "final_unified_stories",
        "final_story_publication_states",
        "final_story_sources",
        "final_story_claims",
        "final_story_conflicts",
    ):
        change = changes[table]
        key_columns = change["key_columns"]
        columns = change["columns"]
        desired_rows = change["desired_rows"]
        new_keys = list(change["new_keys"])
        changed_keys = list(change["changed_keys"])
        if new_keys:
            placeholders = ", ".join(f":{column}" for column in columns)
            connection.executemany(
                f"INSERT INTO {table} ({', '.join(columns)}) "
                f"VALUES ({placeholders})",
                [desired_rows[key] for key in new_keys],
            )
            inserted += len(new_keys)
        if changed_keys:
            value_columns = tuple(
                column for column in columns if column not in key_columns
            )
            assignments = ", ".join(
                f"{column} = :{column}" for column in value_columns
            )
            predicate = " AND ".join(
                f"{column} = :{column}" for column in key_columns
            )
            connection.executemany(
                f"UPDATE {table} SET {assignments} WHERE {predicate}",
                [desired_rows[key] for key in changed_keys],
            )
            updated += len(changed_keys)
        unchanged += int(change["unchanged"])
    return {
        "incremental": True,
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "unchanged": unchanged,
        "seconds": round(perf_counter() - started, 6),
    }


def _write_csv(
    path: Path,
    headers: list[str],
    rows: list[dict[str, Any]],
) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _article_dispositions(connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            discovered.id AS discovered_url_id,
            discovered.url,
            discovered.source,
            discovered.discovered_at,
            discovered.discovery_method,
            discovered.status AS url_status,
            discovered.last_error_code,
            article.id AS article_id,
            COALESCE(article.title, discovered.rss_title) AS title,
            COALESCE(
                article.published_date,
                discovered.rss_published
            ) AS published_date,
            article.clean_status,
            article.dedupe_status,
            article.quality_flags,
            article.duplicate_of_id,
            published.story_id,
            duplicate_published.story_id AS duplicate_story_id
        FROM discovered_urls AS discovered
        LEFT JOIN articles AS article
          INDEXED BY idx_articles_publication_disposition
          ON article.url = discovered.url
        LEFT JOIN final_story_sources AS published
          ON published.article_id = article.id
        LEFT JOIN final_story_sources AS duplicate_published
          ON duplicate_published.article_id = article.duplicate_of_id
        ORDER BY discovered.id
        """
    ).fetchall()
    dispositions = []
    for row in rows:
        reason_codes = []
        if row["story_id"] is not None:
            disposition = "published"
        elif row["dedupe_status"] == "exact_duplicate":
            disposition = "exact_duplicate"
            reason_codes.append("exact_content_duplicate")
        elif row["clean_status"] == "retryable_extraction" or row[
            "last_error_code"
        ] == "article_body_incomplete":
            disposition = "extraction_incomplete"
            reason_codes.append("article_body_incomplete")
        elif row["clean_status"] == "unsupported_media":
            disposition = "unsupported_media"
            try:
                reason_codes.extend(json.loads(row["quality_flags"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                reason_codes.append("unsupported_image_only_media")
        elif row["clean_status"] in {"quality_quarantine", "rejected"}:
            disposition = "quality_quarantine"
            try:
                reason_codes.extend(json.loads(row["quality_flags"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                reason_codes.append("quality_status_unparseable")
        elif row["url_status"] in {"discovered", "fetch_failed"}:
            disposition = "pending_retry"
            if row["last_error_code"]:
                reason_codes.append(str(row["last_error_code"]))
        elif row["url_status"] in {"exhausted", "rejected"}:
            disposition = "fetch_failed"
            if row["last_error_code"]:
                reason_codes.append(str(row["last_error_code"]))
        elif row["clean_status"] == "pending":
            disposition = "pending_cleaning"
        elif row["dedupe_status"] == "pending":
            disposition = "pending_deduplication"
        else:
            disposition = "not_published"
            reason_codes.append("unclassified_pipeline_state")
        dispositions.append(
            {
                "discovered_url_id": int(row["discovered_url_id"]),
                "article_id": row["article_id"],
                "publisher": row["source"],
                "title": row["title"],
                "url": row["url"],
                "published_date": row["published_date"],
                "discovered_at": row["discovered_at"],
                "discovery_method": row["discovery_method"],
                "disposition": disposition,
                "story_id": row["story_id"],
                "duplicate_of_article_id": row["duplicate_of_id"],
                "duplicate_story_id": row["duplicate_story_id"],
                "reason_codes_json": _json(sorted(set(reason_codes))),
            }
        )
    return dispositions


def materialize_gpt_only_publication(
    *,
    output_dir: Union[str, Path],
    published_output_dir: Optional[Union[str, Path]] = None,
    config: Optional[PipelineConfig] = None,
    use_generation_identity_cache: bool = True,
    incremental_publication_writes: bool = True,
) -> dict[str, Any]:
    """Materialize the complete autonomous consumer and audit surfaces."""
    selected_config = config or load_config()
    publication_config = replace(
        selected_config,
        gpt_prompt_version=(
            selected_config.gpt_only_publication_prompt_version
        ),
    )
    selected_dir = Path(output_dir)
    published_dir = (
        Path(published_output_dir)
        if published_output_dir is not None
        else selected_dir
    )
    selected_dir.mkdir(parents=True, exist_ok=True)
    connection = get_connection(selected_config)
    stories: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    claims: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    states: list[dict[str, Any]] = []
    completion_override_story_ids: list[str] = []
    completion_pending_story_ids: list[str] = []
    correction_override_story_ids: list[str] = []
    pending_correction_override_story_ids: list[str] = []
    followup_correction_override_story_ids: list[str] = []
    lineage_correction_override_story_ids: list[str] = []
    remediation_override_story_ids: list[str] = []
    internal_review_override_story_ids: list[str] = []
    singleton_passthrough_story_ids: list[str] = []
    evidence_safe_fallback_story_ids: list[str] = []
    dispositions: list[dict[str, Any]] = []
    disposition_seconds = 0.0
    publication_write_metrics: dict[str, Any] = {}
    identity_cache: Optional[_PublicationIdentityCache] = None
    try:
        _ensure_tables(connection)
        identity_cache = _PublicationIdentityCache(
            connection,
            enabled=use_generation_identity_cache,
        )
        candidates = _load_generation_candidates(
            connection,
            include_singletons=True,
        )
        for cluster, members, article_records in candidates:
            story_id = str(cluster["cluster_key"])
            if int(cluster["article_count"]) == 1:
                if len(members) != 1:
                    raise RuntimeError(
                        f"{story_id}: singleton story must have one member"
                    )
                member = members[0]
                article_id = int(member["article_id"])
                updated_at = _last_updated(cluster, {})
                states.append(
                    {
                        "story_id": story_id,
                        "cluster_id": int(cluster["id"]),
                        "publication_status": (
                            GPT_PUBLICATION_STATUS_PUBLISHABLE
                        ),
                        "reason_codes_json": _json(
                            ["single_source_passthrough"]
                        ),
                        "unified_story_version_id": None,
                        "updated_at": updated_at,
                    }
                )
                stories.append(
                    {
                        "story_id": story_id,
                        "cluster_id": int(cluster["id"]),
                        "title": str(member.get("title") or ""),
                        "story": str(member.get("clean_text") or ""),
                        "last_updated": updated_at,
                        "article_count": 1,
                    }
                )
                sources.append(
                    {
                        "story_id": story_id,
                        "article_id": article_id,
                        "publisher": member.get("source"),
                        "source_title": member.get("title"),
                        "url": member.get("url"),
                        "published_date": member.get("published_date"),
                        "similarity_score": 1.0,
                        "is_representative": 1,
                        "referenced_by_gpt": 0,
                        "evidence_span_ids_json": _json([]),
                    }
                )
                singleton_passthrough_story_ids.append(story_id)
                continue
            try:
                identity = identity_cache.get_or_build(
                    cluster=cluster,
                    members=members,
                    article_records=article_records,
                    config=publication_config,
                )
            except (TypeError, ValueError):
                version = None
                state = gpt_publication_state(version)
                state["reason_codes"] = sorted(
                    {
                        *state["reason_codes"],
                        "invalid_cluster_input",
                    }
                )
            else:
                version = load_cached_version(
                    connection,
                    identity.request_fingerprint_sha256,
                )
                state = gpt_publication_state(version)
                if (
                    state["publication_status"]
                    == GPT_PUBLICATION_STATUS_UNAVAILABLE
                ):
                    correction_version = _reviewed_v2_10_correction(
                        connection,
                        cluster=cluster,
                        members=members,
                        article_records=article_records,
                        config=selected_config,
                        identity_cache=identity_cache,
                    )
                    if correction_version is not None:
                        version = correction_version
                        state = gpt_publication_state(version)
                        correction_override_story_ids.append(story_id)
                    else:
                        completion_version = _reviewed_v2_9_completion(
                            connection,
                            cluster=cluster,
                            members=members,
                            article_records=article_records,
                            config=selected_config,
                            identity_cache=identity_cache,
                        )
                        if completion_version is not None:
                            version = completion_version
                            if (
                                str(
                                    version.get("human_review_decision")
                                    or ""
                                ).strip()
                                == "accept"
                            ):
                                state = gpt_publication_state(version)
                                completion_override_story_ids.append(story_id)
                            else:
                                state = {
                                    "publication_status": (
                                        GPT_PUBLICATION_STATUS_PENDING_REVIEW
                                    ),
                                    "reason_codes": [
                                        "human_review_minor_issue_requires_"
                                        "correction"
                                    ],
                                }
                                completion_pending_story_ids.append(story_id)
                if (
                    state["publication_status"]
                    == GPT_PUBLICATION_STATUS_PENDING_REVIEW
                    and version is not None
                ):
                    pending_correction = (
                        _reviewed_v2_10_pending_correction(
                            connection,
                            cluster=cluster,
                            members=members,
                            article_records=article_records,
                            primary_version=version,
                            config=selected_config,
                        )
                    )
                    if pending_correction is not None:
                        version, correction_target = pending_correction
                        state = gpt_publication_state(version)
                        if (
                            correction_target
                            == REVIEWED_CORRECTION_FOLLOWUP_TARGET
                        ):
                            followup_correction_override_story_ids.append(
                                story_id
                            )
                        elif (
                            correction_target
                            == REVIEWED_CORRECTION_LINEAGE_TARGET
                        ):
                            lineage_correction_override_story_ids.append(
                                story_id
                            )
                        else:
                            pending_correction_override_story_ids.append(
                                story_id
                            )
                if (
                    state["publication_status"]
                    != GPT_PUBLICATION_STATUS_PUBLISHABLE
                ):
                    remediation_version = _reviewed_v2_10_remediation(
                        connection,
                        story_id=story_id,
                        source_fingerprint_sha256=(
                            identity.source_fingerprint_sha256
                        ),
                    )
                    if remediation_version is not None:
                        version = remediation_version
                        state = gpt_publication_state(version)
                        remediation_override_story_ids.append(story_id)
                    else:
                        reviewed_version = (
                            _internally_reviewed_v2_10_remediation(
                                connection,
                                story_id=story_id,
                                source_fingerprint_sha256=(
                                    identity.source_fingerprint_sha256
                                ),
                            )
                        )
                        if reviewed_version is not None:
                            version = reviewed_version
                            state = gpt_publication_state(version)
                            internal_review_override_story_ids.append(
                                story_id
                            )
            use_evidence_safe_fallback = (
                state["publication_status"]
                != GPT_PUBLICATION_STATUS_PUBLISHABLE
            )
            if use_evidence_safe_fallback:
                original_status = str(state["publication_status"])
                state = {
                    "publication_status": GPT_PUBLICATION_STATUS_PUBLISHABLE,
                    "reason_codes": sorted(
                        {
                            *state.get("reason_codes", []),
                            "evidence_safe_fallback",
                            f"generated_candidate_{original_status}",
                        }
                    ),
                }
            updated_at = _last_updated(cluster, version or {})
            states.append(
                {
                    "story_id": story_id,
                    "cluster_id": int(cluster["id"]),
                    "publication_status": state["publication_status"],
                    "reason_codes_json": _json(state["reason_codes"]),
                    "unified_story_version_id": (
                        int(version["id"]) if version is not None else None
                    ),
                    "updated_at": updated_at,
                }
            )
            if use_evidence_safe_fallback:
                representative = next(
                    (
                        member
                        for member in members
                        if bool(member.get("is_representative"))
                    ),
                    members[0],
                )
                stories.append(
                    {
                        "story_id": story_id,
                        "cluster_id": int(cluster["id"]),
                        "title": str(representative.get("title") or ""),
                        "story": str(
                            representative.get("clean_text") or ""
                        ),
                        "last_updated": updated_at,
                        "article_count": int(cluster["article_count"]),
                    }
                )
                for member in sorted(
                    members,
                    key=lambda item: int(item["article_id"]),
                ):
                    sources.append(
                        {
                            "story_id": story_id,
                            "article_id": int(member["article_id"]),
                            "publisher": member.get("source"),
                            "source_title": member.get("title"),
                            "url": member.get("url"),
                            "published_date": member.get("published_date"),
                            "similarity_score": member.get(
                                "similarity_score"
                            ),
                            "is_representative": int(
                                bool(member.get("is_representative"))
                            ),
                            "referenced_by_gpt": 0,
                            "evidence_span_ids_json": _json([]),
                        }
                    )
                evidence_safe_fallback_story_ids.append(story_id)
                continue
            if version is None:
                raise RuntimeError(
                    f"{story_id}: publishable state has no GPT version"
                )
            resolved = _json_mapping(version.get("resolved_output_json"))
            if not resolved:
                raise RuntimeError(
                    f"{story_id}: publishable GPT version is unresolved"
                )
            story_claims = resolved.get("claims") or []
            story_conflicts = (
                resolved.get("conflicts_or_uncertainties") or []
            )
            stories.append(
                {
                    "story_id": story_id,
                    "cluster_id": int(cluster["id"]),
                    "title": str(resolved["display_title"]),
                    "story": str(resolved["unified_story"]),
                    "last_updated": updated_at,
                    "article_count": int(cluster["article_count"]),
                }
            )
            evidence_by_article: dict[int, set[str]] = {}
            for record in (*story_claims, *story_conflicts):
                for evidence in record.get("evidence") or []:
                    article_id = int(evidence["article_id"])
                    evidence_by_article.setdefault(article_id, set()).add(
                        str(evidence["evidence_span_id"])
                    )
            for member in sorted(
                members,
                key=lambda item: int(item["article_id"]),
            ):
                article_id = int(member["article_id"])
                span_ids = sorted(evidence_by_article.get(article_id, set()))
                sources.append(
                    {
                        "story_id": story_id,
                        "article_id": article_id,
                        "publisher": member.get("source"),
                        "source_title": member.get("title"),
                        "url": member.get("url"),
                        "published_date": member.get("published_date"),
                        "similarity_score": member.get("similarity_score"),
                        "is_representative": int(
                            bool(member.get("is_representative"))
                        ),
                        "referenced_by_gpt": int(bool(span_ids)),
                        "evidence_span_ids_json": _json(span_ids),
                    }
                )
            for index, claim in enumerate(story_claims):
                claims.append(
                    {
                        "story_id": story_id,
                        "claim_index": index,
                        "claim_text": str(claim["claim_text"]),
                        "source_article_ids_json": _json(
                            claim["source_article_ids"]
                        ),
                        "evidence_json": _json(claim["evidence"]),
                    }
                )
            for index, conflict in enumerate(story_conflicts):
                conflicts.append(
                    {
                        "story_id": story_id,
                        "conflict_index": index,
                        "description": str(conflict["description"]),
                        "source_article_ids_json": _json(
                            conflict["source_article_ids"]
                        ),
                        "evidence_json": _json(conflict["evidence"]),
                    }
                )

        stories.sort(key=lambda row: row["story_id"])
        sources.sort(key=lambda row: (row["story_id"], row["article_id"]))
        claims.sort(key=lambda row: (row["story_id"], row["claim_index"]))
        conflicts.sort(
            key=lambda row: (row["story_id"], row["conflict_index"])
        )
        states.sort(key=lambda row: row["story_id"])
        if len(states) != len(candidates):
            raise RuntimeError("publication-state reconciliation failed")
        publishable_states = sum(
            row["publication_status"]
            == GPT_PUBLICATION_STATUS_PUBLISHABLE
            for row in states
        )
        if publishable_states != len(stories):
            raise RuntimeError("publishable story reconciliation failed")
        story_ids = {row["story_id"] for row in stories}
        if any(
            row["story_id"] not in story_ids
            for row in (*sources, *claims, *conflicts)
        ):
            raise RuntimeError("supporting-table reconciliation failed")

        connection.execute("BEGIN")
        identity_cache.persist()
        publication_write_metrics = _sync_publication_tables(
            connection,
            rows_by_table={
                "final_unified_stories": stories,
                "final_story_sources": sources,
                "final_story_claims": claims,
                "final_story_conflicts": conflicts,
                "final_story_publication_states": states,
            },
            incremental=incremental_publication_writes,
        )
        connection.commit()
        disposition_started = perf_counter()
        dispositions = _article_dispositions(connection)
        disposition_seconds = perf_counter() - disposition_started
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    story_path = selected_dir / "final_unified_stories.csv"
    source_path = selected_dir / "final_story_sources.csv"
    claim_path = selected_dir / "final_story_claims.csv"
    conflict_path = selected_dir / "final_story_conflicts.csv"
    state_path = selected_dir / "final_story_publication_states.csv"
    disposition_path = selected_dir / "article_processing_disposition.csv"
    _write_csv(
        story_path,
        [
            "story_id",
            "cluster_id",
            "title",
            "story",
            "last_updated",
            "article_count",
        ],
        stories,
    )
    _write_csv(
        source_path,
        [
            "story_id",
            "article_id",
            "publisher",
            "source_title",
            "url",
            "published_date",
            "similarity_score",
            "is_representative",
            "referenced_by_gpt",
            "evidence_span_ids_json",
        ],
        sources,
    )
    _write_csv(
        claim_path,
        [
            "story_id",
            "claim_index",
            "claim_text",
            "source_article_ids_json",
            "evidence_json",
        ],
        claims,
    )
    _write_csv(
        conflict_path,
        [
            "story_id",
            "conflict_index",
            "description",
            "source_article_ids_json",
            "evidence_json",
        ],
        conflicts,
    )
    _write_csv(
        state_path,
        [
            "story_id",
            "cluster_id",
            "publication_status",
            "reason_codes_json",
            "unified_story_version_id",
            "updated_at",
        ],
        states,
    )
    _write_csv(
        disposition_path,
        [
            "discovered_url_id",
            "article_id",
            "publisher",
            "title",
            "url",
            "published_date",
            "discovered_at",
            "discovery_method",
            "disposition",
            "story_id",
            "duplicate_of_article_id",
            "duplicate_story_id",
            "reason_codes_json",
        ],
        dispositions,
    )
    status_counts = {
        status: sum(
            row["publication_status"] == status for row in states
        )
        for status in sorted(
            {str(row["publication_status"]) for row in states}
        )
    }
    disposition_counts = {
        disposition: sum(row["disposition"] == disposition for row in dispositions)
        for disposition in sorted({str(row["disposition"]) for row in dispositions})
    }
    manifest_path = selected_dir / "final_publication_manifest.json"
    manifest = {
        "publication_version": FINAL_PUBLICATION_VERSION,
        "materialized_at": datetime.now(LOCAL_TIME_ZONE).isoformat(
            timespec="seconds"
        ),
        "mode": (
            "autonomous_gpt_with_single_source_and_evidence_safe_fallback"
        ),
        "prompt_version": publication_config.gpt_prompt_version,
        "prompt_selection": {
            "primary_prompt_version": (
                publication_config.gpt_prompt_version
            ),
            "reviewed_completion_prompt_version": (
                GPT_PROMPT_VERSION_V2_9
            ),
            "reviewed_completion_story_count": len(
                completion_override_story_ids
            ),
            "reviewed_completion_story_ids": sorted(
                completion_override_story_ids
            ),
            "reviewed_completion_pending_correction_count": len(
                completion_pending_story_ids
            ),
            "reviewed_completion_pending_correction_story_ids": sorted(
                completion_pending_story_ids
            ),
            "reviewed_completion_requires_primary_unavailable": True,
            "reviewed_completion_requires_accept_for_publication": True,
            "reviewed_correction_prompt_version": (
                GPT_PROMPT_VERSION_V2_10
            ),
            "reviewed_correction_story_count": len(
                correction_override_story_ids
            ),
            "reviewed_correction_story_ids": sorted(
                correction_override_story_ids
            ),
            "reviewed_correction_requires_primary_unavailable": True,
            "reviewed_correction_requires_accept_for_publication": True,
            "reviewed_pending_correction_story_count": len(
                pending_correction_override_story_ids
            ),
            "reviewed_pending_correction_story_ids": sorted(
                pending_correction_override_story_ids
            ),
            "reviewed_pending_correction_requires_primary_pending": True,
            "reviewed_pending_correction_requires_accept": True,
            "reviewed_followup_correction_story_count": len(
                followup_correction_override_story_ids
            ),
            "reviewed_followup_correction_story_ids": sorted(
                followup_correction_override_story_ids
            ),
            "reviewed_followup_correction_requires_prior_review": True,
            "reviewed_followup_correction_requires_accept": True,
            "reviewed_lineage_correction_story_count": len(
                lineage_correction_override_story_ids
            ),
            "reviewed_lineage_correction_story_ids": sorted(
                lineage_correction_override_story_ids
            ),
            "reviewed_lineage_correction_requires_complete_lineage": True,
            "reviewed_lineage_correction_requires_accept": True,
            "reviewed_remediation_story_count": len(
                remediation_override_story_ids
            ),
            "reviewed_remediation_story_ids": sorted(
                remediation_override_story_ids
            ),
            "reviewed_remediation_requires_exact_current_source": True,
            "reviewed_remediation_allows_accept_or_minor_issue": True,
            "internal_quality_review_story_count": len(
                internal_review_override_story_ids
            ),
            "internal_quality_review_story_ids": sorted(
                internal_review_override_story_ids
            ),
            "internal_quality_review_requires_warning_free_validation": True,
            "internal_quality_review_method": "internal_evidence_audit",
        },
        "model": selected_config.gpt_model,
        "network_calls_made": 0,
        "generation_calls_made": 0,
        "deterministic_substitutions": 0,
        "singleton_passthrough_story_count": len(
            singleton_passthrough_story_ids
        ),
        "evidence_safe_fallback_story_count": len(
            evidence_safe_fallback_story_ids
        ),
        "evidence_safe_fallback_story_ids": sorted(
            evidence_safe_fallback_story_ids
        ),
        "counts": {
            "eligible_clusters": len(states),
            "final_unified_stories": len(stories),
            "gpt_unified_stories": (
                len(stories)
                - len(singleton_passthrough_story_ids)
                - len(evidence_safe_fallback_story_ids)
            ),
            "singleton_passthrough_stories": len(
                singleton_passthrough_story_ids
            ),
            "evidence_safe_fallback_stories": len(
                evidence_safe_fallback_story_ids
            ),
            "final_story_sources": len(sources),
            "final_story_claims": len(claims),
            "final_story_conflicts": len(conflicts),
            "explicit_nonpublishable_states": len(states) - len(stories),
            "publication_statuses": status_counts,
            "article_dispositions": disposition_counts,
            "discovered_urls": len(dispositions),
        },
        "reconciliation": {
            "every_cluster_has_one_state": len(states) == len(candidates),
            "publishable_state_count_matches_final_story_count": (
                status_counts.get(
                    GPT_PUBLICATION_STATUS_PUBLISHABLE,
                    0,
                )
                == len(stories)
            ),
            "no_nonpublishable_story_rows": all(
                state["publication_status"]
                == GPT_PUBLICATION_STATUS_PUBLISHABLE
                for state in states
                if state["story_id"] in story_ids
            ),
            "every_discovered_url_has_disposition": all(
                bool(row["disposition"]) for row in dispositions
            ),
            "published_article_dispositions_match_sources": (
                disposition_counts.get("published", 0) == len(sources)
            ),
        },
        "paths": {
            "final_unified_stories": str(
                published_dir / story_path.name
            ),
            "final_story_sources": str(
                published_dir / source_path.name
            ),
            "final_story_claims": str(
                published_dir / claim_path.name
            ),
            "final_story_conflicts": str(
                published_dir / conflict_path.name
            ),
            "final_story_publication_states": str(
                published_dir / state_path.name
            ),
            "article_processing_disposition": str(
                published_dir / disposition_path.name
            ),
            "manifest": str(published_dir / manifest_path.name),
        },
        "artifact_sha256": {
            "final_unified_stories": _sha256(story_path),
            "final_story_sources": _sha256(source_path),
            "final_story_claims": _sha256(claim_path),
            "final_story_conflicts": _sha256(conflict_path),
            "final_story_publication_states": _sha256(state_path),
            "article_processing_disposition": _sha256(disposition_path),
        },
    }
    fingerprint_payload = {
        key: manifest[key]
        for key in (
            "publication_version",
            "mode",
            "prompt_version",
            "prompt_selection",
            "model",
            "counts",
            "reconciliation",
            "artifact_sha256",
        )
    }
    manifest["publication_fingerprint_sha256"] = hashlib.sha256(
        json.dumps(
            fingerprint_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    identity_metrics = dict(identity_cache.metrics) if identity_cache else {}
    for key in (
        "key_seconds",
        "lookup_seconds",
        "build_seconds",
        "write_seconds",
    ):
        if key in identity_metrics:
            identity_metrics[key] = round(float(identity_metrics[key]), 6)
    runtime_metrics = {
        "generation_identity_cache": identity_metrics,
        "publication_rows": publication_write_metrics,
        "article_disposition_seconds": round(disposition_seconds, 6),
    }
    logger.info(
        "Publication identities: %s cache hits, %s misses, %s uncached "
        "builds; %s cache rows written",
        identity_metrics.get("hits", 0),
        identity_metrics.get("misses", 0),
        identity_metrics.get("uncached_builds", 0),
        identity_metrics.get("cache_writes", 0),
    )
    logger.info(
        "Publication row sync: %s inserted, %s updated, %s deleted, "
        "%s unchanged in %.3fs",
        publication_write_metrics.get("inserted", 0),
        publication_write_metrics.get("updated", 0),
        publication_write_metrics.get("deleted", 0),
        publication_write_metrics.get("unchanged", 0),
        publication_write_metrics.get("seconds", 0.0),
    )
    return {
        **manifest,
        "manifest_sha256": _sha256(manifest_path),
        "_runtime_metrics": runtime_metrics,
    }
