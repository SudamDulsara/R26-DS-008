from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional, Union
from zoneinfo import ZoneInfo

from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.unification.gpt_contract import (
    GPT_PROMPT_VERSION_V2_9,
    GPT_PROMPT_VERSION_V2_10,
)
from news_pipeline.unification.production import (
    GPT_PUBLICATION_STATUS_PENDING_REVIEW,
    GPT_PUBLICATION_STATUS_PUBLISHABLE,
    GPT_PUBLICATION_STATUS_UNAVAILABLE,
    _load_generation_candidates,
    build_generation_identity,
    gpt_publication_state,
    load_cached_version,
)


FINAL_PUBLICATION_VERSION = "gpt_only_final_publication_v1"
LOCAL_TIME_ZONE = ZoneInfo("Asia/Colombo")
COMPLETION_REVIEW_TARGET = "v2_9_completion_raw_candidate"
CORRECTION_REVIEW_TARGET = "v2_10_prison_correction_raw_candidate"
COMPLETION_REASONING_EFFORT = "none"
COMPLETION_MAX_OUTPUT_TOKENS = 8192


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


def _reviewed_v2_9_completion(
    connection: Any,
    *,
    cluster: Mapping[str, Any],
    members: list[dict[str, Any]],
    article_records: Mapping[int, Mapping[str, Any]],
    config: PipelineConfig,
) -> Optional[dict[str, Any]]:
    """Return an exact-current, reviewed v2.9 completion candidate."""
    completion_config = replace(
        config,
        gpt_prompt_version=GPT_PROMPT_VERSION_V2_9,
        gpt_reasoning_effort=COMPLETION_REASONING_EFFORT,
        gpt_max_output_tokens=COMPLETION_MAX_OUTPUT_TOKENS,
    )
    try:
        identity = build_generation_identity(
            cluster=cluster,
            members=members,
            article_records_by_id=article_records,
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
) -> Optional[dict[str, Any]]:
    """Return an exact-current, accepted reviewed v2.10 correction."""
    correction_config = replace(
        config,
        gpt_prompt_version=GPT_PROMPT_VERSION_V2_10,
        gpt_reasoning_effort=COMPLETION_REASONING_EFFORT,
        gpt_max_output_tokens=COMPLETION_MAX_OUTPUT_TOKENS,
    )
    try:
        identity = build_generation_identity(
            cluster=cluster,
            members=members,
            article_records_by_id=article_records,
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

        CREATE INDEX IF NOT EXISTS idx_final_story_state_status
        ON final_story_publication_states(publication_status, story_id);
        """
    )


def _write_csv(
    path: Path,
    headers: list[str],
    rows: list[dict[str, Any]],
) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def materialize_gpt_only_publication(
    *,
    output_dir: Union[str, Path],
    config: Optional[PipelineConfig] = None,
) -> dict[str, Any]:
    """Materialize the fail-closed GPT-only consumer and audit surfaces."""
    selected_config = config or load_config()
    publication_config = replace(
        selected_config,
        gpt_prompt_version=(
            selected_config.gpt_only_publication_prompt_version
        ),
    )
    selected_dir = Path(output_dir)
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
    try:
        candidates = _load_generation_candidates(connection)
        for cluster, members, article_records in candidates:
            story_id = str(cluster["cluster_key"])
            try:
                identity = build_generation_identity(
                    cluster=cluster,
                    members=members,
                    article_records_by_id=article_records,
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
            if (
                state["publication_status"]
                != GPT_PUBLICATION_STATUS_PUBLISHABLE
            ):
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

        _ensure_tables(connection)
        connection.execute("BEGIN")
        for table in (
            "final_story_sources",
            "final_story_claims",
            "final_story_conflicts",
            "final_unified_stories",
            "final_story_publication_states",
        ):
            connection.execute(f"DELETE FROM {table}")
        connection.executemany(
            """
            INSERT INTO final_unified_stories (
                story_id, cluster_id, title, story,
                last_updated, article_count
            ) VALUES (
                :story_id, :cluster_id, :title, :story,
                :last_updated, :article_count
            )
            """,
            stories,
        )
        connection.executemany(
            """
            INSERT INTO final_story_sources (
                story_id, article_id, publisher, source_title, url,
                published_date, similarity_score, is_representative,
                referenced_by_gpt, evidence_span_ids_json
            ) VALUES (
                :story_id, :article_id, :publisher, :source_title, :url,
                :published_date, :similarity_score, :is_representative,
                :referenced_by_gpt, :evidence_span_ids_json
            )
            """,
            sources,
        )
        connection.executemany(
            """
            INSERT INTO final_story_claims (
                story_id, claim_index, claim_text,
                source_article_ids_json, evidence_json
            ) VALUES (
                :story_id, :claim_index, :claim_text,
                :source_article_ids_json, :evidence_json
            )
            """,
            claims,
        )
        connection.executemany(
            """
            INSERT INTO final_story_conflicts (
                story_id, conflict_index, description,
                source_article_ids_json, evidence_json
            ) VALUES (
                :story_id, :conflict_index, :description,
                :source_article_ids_json, :evidence_json
            )
            """,
            conflicts,
        )
        connection.executemany(
            """
            INSERT INTO final_story_publication_states (
                story_id, cluster_id, publication_status,
                reason_codes_json, unified_story_version_id, updated_at
            ) VALUES (
                :story_id, :cluster_id, :publication_status,
                :reason_codes_json, :unified_story_version_id, :updated_at
            )
            """,
            states,
        )
        connection.commit()
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
    status_counts = {
        status: sum(
            row["publication_status"] == status for row in states
        )
        for status in sorted(
            {str(row["publication_status"]) for row in states}
        )
    }
    manifest_path = selected_dir / "final_publication_manifest.json"
    manifest = {
        "publication_version": FINAL_PUBLICATION_VERSION,
        "materialized_at": datetime.now(LOCAL_TIME_ZONE).isoformat(
            timespec="seconds"
        ),
        "mode": "gpt_only_with_fail_closed_quarantine",
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
        },
        "model": selected_config.gpt_model,
        "network_calls_made": 0,
        "generation_calls_made": 0,
        "deterministic_substitutions": 0,
        "counts": {
            "eligible_clusters": len(states),
            "final_unified_stories": len(stories),
            "final_story_sources": len(sources),
            "final_story_claims": len(claims),
            "final_story_conflicts": len(conflicts),
            "explicit_nonpublishable_states": len(states) - len(stories),
            "publication_statuses": status_counts,
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
        },
        "paths": {
            "final_unified_stories": str(story_path),
            "final_story_sources": str(source_path),
            "final_story_claims": str(claim_path),
            "final_story_conflicts": str(conflict_path),
            "final_story_publication_states": str(state_path),
            "manifest": str(manifest_path),
        },
        "artifact_sha256": {
            "final_unified_stories": _sha256(story_path),
            "final_story_sources": _sha256(source_path),
            "final_story_claims": _sha256(claim_path),
            "final_story_conflicts": _sha256(conflict_path),
            "final_story_publication_states": _sha256(state_path),
        },
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    return {
        **manifest,
        "manifest_sha256": _sha256(manifest_path),
    }
