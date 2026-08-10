from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Optional, Sequence

from pydantic import ValidationError

from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger
from news_pipeline.unification.fact_validation import (
    validate_claim_projection,
    validate_numerical_conflict_coverage,
    validate_resolved_fact_shapes,
    validate_semantic_support_strands,
)
from news_pipeline.unification.gpt_contract import (
    GPT_INPUT_SCHEMA_VERSION_V2,
    GPT_OUTPUT_SCHEMA_VERSION_V2,
    GPT_PROMPT_VERSION_V2,
    GPT_PROMPT_VERSION_V2_1,
    GPT_PROMPT_VERSION_V2_2,
    GPT_PROMPT_VERSION_V2_3,
    GPT_PROMPT_VERSION_V2_4,
    GPT_PROMPT_VERSION_V2_5,
    GPT_PROMPT_VERSION_V2_6,
    GPT_PROMPT_VERSION_V2_7,
    GPT_PROMPT_VERSION_V2_8,
    GPT_PROMPT_VERSION_V2_9,
    GPT_PROMPT_VERSION_V2_10,
    GPT_RESOLVED_SCHEMA_VERSION_V2,
    GPTUnifiedStoryInputV2,
    GPTUnifiedStoryResponseV2,
    GPTValidationIssue,
    GPTValidationReport,
    build_gpt_input_v2_from_records,
    build_structured_response_request_v2,
    resolve_gpt_response_v2,
    repair_uniquely_truncated_evidence_span_ids,
    upgrade_gpt_input_v2_to_prompt_v2_1,
    upgrade_gpt_input_v2_to_prompt_v2_2,
    upgrade_gpt_input_v2_to_prompt_v2_3,
    upgrade_gpt_input_v2_to_prompt_v2_4,
    upgrade_gpt_input_v2_to_prompt_v2_5,
    upgrade_gpt_input_v2_to_prompt_v2_6,
    upgrade_gpt_input_v2_to_prompt_v2_7,
    upgrade_gpt_input_v2_to_prompt_v2_8,
    upgrade_gpt_input_v2_to_prompt_v2_9,
    upgrade_gpt_input_v2_to_prompt_v2_10,
    validate_gpt_response_v2,
)
from news_pipeline.unification.gpt_preflight import (
    GPTPreflight,
    MODEL_PRICING,
    PreflightedGPTGenerator,
)
from news_pipeline.unification.openai_adapter import (
    AdapterOutcome,
    OpenAIResponsesAdapter,
    StructuredResponseRequest,
)


logger = get_logger()

GENERATION_STATUS_ACCEPTED = "accepted"
GENERATION_STATUS_FALLBACK = "fallback"
VALIDATION_STATUS_ACCEPTED = "accepted"
VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS = "accepted_with_warnings"
HUMAN_REVIEW_BLOCKING_DECISIONS = frozenset(
    {
        "needs_changes",
        "reject",
        "major_issue",
        "provider_candidate_unreviewed",
    }
)
HUMAN_REVIEW_OVERRIDE_DECISIONS = frozenset(
    {"accept", "minor_issue"}
)
HUMAN_REVIEW_FACT_SHAPE_OVERRIDE_TARGETS = frozenset(
    {
        "rejected_gpt_candidate",
        "prompt_v2_2_gpt_candidate",
        "gpt_only_quarantine_candidate",
        "phase4_prompt_v2_8_gpt_only_candidate",
        "phase4_semantic_gate_holdout_candidate",
    }
)
HUMAN_REVIEW_WARNING_OVERRIDE_TARGETS = frozenset(
    {
        *HUMAN_REVIEW_FACT_SHAPE_OVERRIDE_TARGETS,
        "phase4_semantic_gate_holdout_candidate",
        "v2_9_completion_raw_candidate",
        "v2_10_prison_correction_raw_candidate",
    }
)
GPT_PUBLICATION_STATUS_PUBLISHABLE = "publishable"
GPT_PUBLICATION_STATUS_PENDING_REVIEW = "pending_review"
GPT_PUBLICATION_STATUS_REJECTED = "rejected"
GPT_PUBLICATION_STATUS_UNAVAILABLE = "unavailable"
GPT_REVIEW_QUEUE_STATUS_PENDING = "pending_review"
GPT_REVIEW_QUEUE_STATUS_APPROVED = "approved"
GPT_REVIEW_QUEUE_STATUS_REJECTED = "rejected"


def human_review_blocks_version(
    version: Optional[Mapping[str, Any]],
) -> bool:
    if version is None:
        return False
    decision = str(version.get("human_review_decision") or "").strip()
    if decision in HUMAN_REVIEW_BLOCKING_DECISIONS:
        return True
    try:
        review = json.loads(
            str(version.get("human_review_scores_json") or "")
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return review.get("unsupported_material_claim") == "yes"


def human_review_overrides_fact_shape(
    version: Optional[Mapping[str, Any]],
) -> bool:
    if version is None:
        return False
    decision = str(version.get("human_review_decision") or "").strip()
    if decision not in HUMAN_REVIEW_OVERRIDE_DECISIONS:
        return False
    try:
        review = json.loads(
            str(version.get("human_review_scores_json") or "")
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return bool(
        version.get("generation_status") == GENERATION_STATUS_FALLBACK
        and version.get("validation_status") == "fact_shape_failed"
        and version.get("response_id")
        and version.get("resolved_output_json")
        and review.get("review_target")
        in HUMAN_REVIEW_FACT_SHAPE_OVERRIDE_TARGETS
        and review.get("validator_assessment") == "false_positive"
        and review.get("unsupported_material_claim") == "no"
    )


def human_review_overrides_validator_warning(
    version: Optional[Mapping[str, Any]],
) -> bool:
    if version is None:
        return False
    decision = str(version.get("human_review_decision") or "").strip()
    if decision not in HUMAN_REVIEW_OVERRIDE_DECISIONS:
        return False
    try:
        review = json.loads(
            str(version.get("human_review_scores_json") or "")
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return bool(
        version.get("generation_status") == GENERATION_STATUS_ACCEPTED
        and version.get("validation_status")
        == VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS
        and version.get("response_id")
        and version.get("resolved_output_json")
        and review.get("review_target")
        in HUMAN_REVIEW_WARNING_OVERRIDE_TARGETS
        and review.get("validator_assessment") == "false_positive"
        and review.get("unsupported_material_claim") == "no"
    )


def _append_provenance_warnings(
    report: GPTValidationReport,
    warnings: Sequence[GPTValidationIssue],
) -> GPTValidationReport:
    if not warnings:
        return report
    return GPTValidationReport(
        issues=report.issues,
        warnings=(*report.warnings, *warnings),
    )


def _review_demotes_unaccounted_article_only(
    version: Mapping[str, Any],
    report: GPTValidationReport,
) -> GPTValidationReport:
    """Demote only audited duplicate-article coverage issues to warnings."""
    if not report.issues or any(
        issue.code != "unaccounted_article_id"
        for issue in report.issues
    ):
        return report
    decision = str(version.get("human_review_decision") or "").strip()
    review = _review_payload(version)
    if (
        decision not in HUMAN_REVIEW_OVERRIDE_DECISIONS
        or review.get("review_target")
        not in HUMAN_REVIEW_WARNING_OVERRIDE_TARGETS
        or review.get("validator_assessment") != "false_positive"
        or review.get("unsupported_material_claim") != "no"
    ):
        return report
    return GPTValidationReport(
        issues=(),
        warnings=(*report.warnings, *report.issues),
    )


def version_is_deployable_gpt(
    version: Optional[Mapping[str, Any]],
) -> bool:
    return bool(
        version is not None
        and not human_review_blocks_version(version)
        and (
            (
                version.get("generation_status")
                == GENERATION_STATUS_ACCEPTED
                and version.get("validation_status")
                == VALIDATION_STATUS_ACCEPTED
            )
            or human_review_overrides_fact_shape(version)
            or human_review_overrides_validator_warning(version)
        )
    )


def version_is_pending_validator_warning(
    version: Optional[Mapping[str, Any]],
) -> bool:
    return bool(
        version is not None
        and not human_review_blocks_version(version)
        and version.get("generation_status") == GENERATION_STATUS_ACCEPTED
        and version.get("validation_status")
        == VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS
        and version.get("response_id")
        and version.get("resolved_output_json")
        and not human_review_overrides_validator_warning(version)
    )


def effective_fallback_reason(
    version: Optional[Mapping[str, Any]],
    default: str = "unknown",
) -> str:
    if (
        version is not None
        and human_review_blocks_version(version)
    ):
        return (
            "human_review_"
            + str(version.get("human_review_decision")).strip()
        )
    if version is not None and version.get("fallback_reason"):
        return str(version["fallback_reason"])
    return default


def _review_payload(
    version: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    if version is None:
        return {}
    try:
        value = json.loads(
            str(version.get("human_review_scores_json") or "")
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _validation_issue_codes(version: Mapping[str, Any]) -> set[str]:
    try:
        report = json.loads(str(version.get("validation_json") or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return set()
    codes: set[str] = set()

    def collect(value: Any) -> None:
        if isinstance(value, Mapping):
            code = value.get("code")
            if isinstance(code, str) and code.strip():
                codes.add(code.strip())
            for nested in value.values():
                collect(nested)
        elif isinstance(value, list):
            for nested in value:
                collect(nested)

    collect(report)
    return codes


def gpt_publication_state(
    version: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return the fail-closed GPT-only publication and review state."""
    if version is None:
        return {
            "publication_status": GPT_PUBLICATION_STATUS_UNAVAILABLE,
            "review_queue_status": None,
            "reason_codes": ["missing_cached_candidate"],
        }

    reason_codes = _validation_issue_codes(version)
    stored_fallback_reason = version.get("fallback_reason")
    if stored_fallback_reason:
        reason_codes.add(str(stored_fallback_reason))
    if human_review_blocks_version(version):
        reason_codes.add(effective_fallback_reason(version))
    decision = str(version.get("human_review_decision") or "").strip()
    review = _review_payload(version)
    if decision:
        reason_codes.add(f"human_review_{decision}")
    if review.get("unsupported_material_claim") == "yes":
        reason_codes.add("unsupported_material_claim")

    has_candidate = bool(
        version.get("response_id") and version.get("output_json")
    )
    if not has_candidate:
        return {
            "publication_status": GPT_PUBLICATION_STATUS_UNAVAILABLE,
            "review_queue_status": None,
            "reason_codes": sorted(reason_codes),
        }
    if human_review_blocks_version(version):
        return {
            "publication_status": GPT_PUBLICATION_STATUS_REJECTED,
            "review_queue_status": GPT_REVIEW_QUEUE_STATUS_REJECTED,
            "reason_codes": sorted(reason_codes),
        }
    if version_is_deployable_gpt(version):
        return {
            "publication_status": GPT_PUBLICATION_STATUS_PUBLISHABLE,
            "review_queue_status": (
                GPT_REVIEW_QUEUE_STATUS_APPROVED
                if (
                    human_review_overrides_fact_shape(version)
                    or human_review_overrides_validator_warning(version)
                )
                else None
            ),
            "reason_codes": sorted(reason_codes),
        }
    return {
        "publication_status": GPT_PUBLICATION_STATUS_PENDING_REVIEW,
        "review_queue_status": GPT_REVIEW_QUEUE_STATUS_PENDING,
        "reason_codes": sorted(reason_codes),
    }


def _candidate_snapshot(
    version: Mapping[str, Any],
) -> tuple[Optional[str], Optional[str]]:
    try:
        candidate = json.loads(str(version.get("output_json") or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None, None
    if not isinstance(candidate, Mapping):
        return None, None
    title = candidate.get("display_title")
    story = candidate.get("unified_story")
    return (
        str(title) if isinstance(title, str) else None,
        str(story) if isinstance(story, str) else None,
    )


def _review_queue_table_exists(
    connection: sqlite3.Connection,
) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'gpt_unification_review_queue'
        """
    ).fetchone()
    return row is not None


def sync_gpt_unification_review_queue(
    connection: sqlite3.Connection,
    version: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """Upsert validator/human exceptions into the GPT review queue.

    Normally accepted GPT versions do not create queue rows. Validator-blocked
    candidates remain pending, audited human overrides remain approved, and
    human-blocked candidates remain rejected.
    """
    if not _review_queue_table_exists(connection):
        return None
    state = gpt_publication_state(version)
    fingerprint = str(version["request_fingerprint_sha256"])
    existing = connection.execute(
        """
        SELECT *
        FROM gpt_unification_review_queue
        WHERE request_fingerprint_sha256 = ?
        """,
        (fingerprint,),
    ).fetchone()
    queue_status = state["review_queue_status"]
    if queue_status is None:
        if existing is None:
            return None
        queue_status = GPT_REVIEW_QUEUE_STATUS_APPROVED

    title, story = _candidate_snapshot(version)
    now = datetime.now().isoformat(timespec="seconds")
    detected_at = (
        str(existing["detected_at"])
        if existing is not None
        else str(version.get("created_at") or now)
    )
    reviewed_at = version.get("human_review_imported_at")
    connection.execute(
        """
        INSERT INTO gpt_unification_review_queue (
            unified_story_version_id,
            cluster_id,
            story_id,
            request_fingerprint_sha256,
            queue_status,
            reason_codes_json,
            validation_status,
            fallback_reason,
            candidate_title,
            candidate_story,
            prompt_version,
            model_name,
            response_id,
            detected_at,
            updated_at,
            reviewed_at,
            review_decision,
            review_notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(unified_story_version_id) DO UPDATE SET
            cluster_id = excluded.cluster_id,
            story_id = excluded.story_id,
            request_fingerprint_sha256 = (
                excluded.request_fingerprint_sha256
            ),
            queue_status = excluded.queue_status,
            reason_codes_json = excluded.reason_codes_json,
            validation_status = excluded.validation_status,
            fallback_reason = excluded.fallback_reason,
            candidate_title = excluded.candidate_title,
            candidate_story = excluded.candidate_story,
            prompt_version = excluded.prompt_version,
            model_name = excluded.model_name,
            response_id = excluded.response_id,
            updated_at = excluded.updated_at,
            reviewed_at = excluded.reviewed_at,
            review_decision = excluded.review_decision,
            review_notes = excluded.review_notes
        """,
        (
            int(version["id"]),
            version.get("cluster_id"),
            str(version["cluster_key"]),
            fingerprint,
            queue_status,
            _canonical_json(state["reason_codes"]),
            str(version.get("validation_status") or "unknown"),
            version.get("fallback_reason"),
            title,
            story,
            str(version["prompt_version"]),
            str(version["model_name"]),
            version.get("response_id"),
            detected_at,
            str(version.get("updated_at") or now),
            reviewed_at,
            version.get("human_review_decision"),
            version.get("human_review_notes"),
        ),
    )
    row = connection.execute(
        """
        SELECT *
        FROM gpt_unification_review_queue
        WHERE request_fingerprint_sha256 = ?
        """,
        (fingerprint,),
    ).fetchone()
    return dict(row) if row is not None else None


def rebuild_gpt_unification_review_queue(
    connection: sqlite3.Connection,
    *,
    prompt_version: Optional[str] = None,
) -> dict[str, int]:
    """Backfill the queue from persisted GPT versions without network calls."""
    if not _review_queue_table_exists(connection):
        return {}
    query = "SELECT * FROM unified_story_versions"
    parameters: tuple[Any, ...] = ()
    if prompt_version is not None:
        query += " WHERE prompt_version = ?"
        parameters = (prompt_version,)
    for row in connection.execute(query, parameters):
        sync_gpt_unification_review_queue(connection, dict(row))
    connection.commit()
    return {
        str(row["queue_status"]): int(row["count"])
        for row in connection.execute(
            """
            SELECT queue_status, COUNT(*) AS count
            FROM gpt_unification_review_queue
            GROUP BY queue_status
            """
        )
    }


@dataclass(frozen=True)
class GenerationIdentity:
    contract_input: GPTUnifiedStoryInputV2
    request: StructuredResponseRequest
    source_fingerprint_sha256: str
    input_fingerprint_sha256: str
    request_fingerprint_sha256: str


def _field_value(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(field_name)
    return getattr(value, field_name, None)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(
        _canonical_json(value).encode("utf-8")
    ).hexdigest()


def build_generation_identity(
    *,
    cluster: Mapping[str, Any],
    members: list[Mapping[str, Any]],
    article_records_by_id: Mapping[int, Mapping[str, Any]],
    config: PipelineConfig,
) -> GenerationIdentity:
    representative_article_id = cluster.get("representative_article_id")
    if representative_article_id is None:
        raise ValueError("cluster has no representative article")

    contract_input = build_gpt_input_v2_from_records(
        cluster_key=str(cluster["cluster_key"]),
        # SQLite row IDs can change when clustering rebuilds its tables.
        # The content-derived cluster key is the stable production identity.
        cluster_id=None,
        representative_article_id=int(representative_article_id),
        member_records=members,
        article_records_by_id=article_records_by_id,
    )
    if config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_1:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_1(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_2:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_2(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_3:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_3(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_4:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_4(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_5:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_5(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_6:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_6(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_7:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_7(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_8:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_8(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_9:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_9(
            contract_input
        )
    elif config.gpt_prompt_version == GPT_PROMPT_VERSION_V2_10:
        contract_input = upgrade_gpt_input_v2_to_prompt_v2_10(
            contract_input
        )
    elif config.gpt_prompt_version != GPT_PROMPT_VERSION_V2:
        raise ValueError(
            "production unification supports only prompt v2, v2.1, "
            "v2.2, v2.3, v2.4, v2.5, v2.6, v2.7, v2.8, v2.9, "
            "or v2.10"
        )
    if config.gpt_schema_version != GPT_OUTPUT_SCHEMA_VERSION_V2:
        raise ValueError(
            "production unification supports only output schema v2"
        )

    request = build_structured_response_request_v2(
        contract_input,
        config,
    )
    source_payload = {
        "cluster_key": contract_input.cluster_key,
        "representative_article_id": (
            contract_input.representative_article_id
        ),
        "articles": [
            article.model_dump(mode="json")
            for article in contract_input.articles
        ],
    }
    input_payload = contract_input.model_dump(mode="json")
    source_fingerprint = _fingerprint(source_payload)
    input_fingerprint = _fingerprint(input_payload)
    request_payload = {
        "source_fingerprint_sha256": source_fingerprint,
        "input_fingerprint_sha256": input_fingerprint,
        "model": request.model,
        "instructions": request.instructions,
        "input": request.input,
        "text_format_schema": request.text_format.model_json_schema(),
        "max_output_tokens": request.max_output_tokens,
        "reasoning_effort": request.reasoning_effort,
    }
    if request.text_verbosity is not None:
        request_payload["text_verbosity"] = request.text_verbosity
    request_fingerprint = _fingerprint(request_payload)
    return GenerationIdentity(
        contract_input=contract_input,
        request=request,
        source_fingerprint_sha256=source_fingerprint,
        input_fingerprint_sha256=input_fingerprint,
        request_fingerprint_sha256=request_fingerprint,
    )


def _load_generation_candidates(
    connection: sqlite3.Connection,
) -> list[
    tuple[dict[str, Any], list[dict[str, Any]], dict[int, dict[str, Any]]]
]:
    clusters = [
        dict(row)
        for row in connection.execute(
            """
            SELECT
                id,
                cluster_key,
                representative_article_id,
                model_name,
                model_revision,
                article_count,
                created_at
            FROM story_clusters
            ORDER BY id
            """
        )
    ]
    member_rows = [
        dict(row)
        for row in connection.execute(
            """
            SELECT
                members.cluster_id,
                members.article_id,
                members.is_representative,
                members.similarity_score,
                articles.url,
                articles.source,
                articles.title,
                articles.published_date,
                articles.clean_text,
                articles.clean_hash
            FROM story_cluster_members AS members
            JOIN articles ON articles.id = members.article_id
            ORDER BY members.cluster_id, members.article_id
            """
        )
    ]
    members_by_cluster = defaultdict(list)
    articles_by_cluster = defaultdict(dict)
    for member in member_rows:
        cluster_id = int(member["cluster_id"])
        article_id = int(member["article_id"])
        members_by_cluster[cluster_id].append(member)
        articles_by_cluster[cluster_id][article_id] = member

    return [
        (
            cluster,
            members_by_cluster.get(int(cluster["id"]), []),
            articles_by_cluster.get(int(cluster["id"]), {}),
        )
        for cluster in clusters
    ]


def _select_generation_candidates(
    candidates: list[
        tuple[
            dict[str, Any],
            list[dict[str, Any]],
            dict[int, dict[str, Any]],
        ]
    ],
    cluster_keys: Optional[Sequence[str]],
) -> list[
    tuple[dict[str, Any], list[dict[str, Any]], dict[int, dict[str, Any]]]
]:
    if cluster_keys is None:
        return candidates
    selected_keys = tuple(str(value).strip() for value in cluster_keys)
    if not selected_keys:
        return []
    if (
        any(not value for value in selected_keys)
        or len(selected_keys) != len(set(selected_keys))
    ):
        raise ValueError(
            "cluster_keys must contain unique nonblank cluster keys"
        )
    candidates_by_key = {
        str(candidate[0]["cluster_key"]): candidate
        for candidate in candidates
    }
    missing = [
        cluster_key
        for cluster_key in selected_keys
        if cluster_key not in candidates_by_key
    ]
    if missing:
        raise ValueError(
            "requested generation clusters are missing: "
            + ", ".join(missing)
        )
    return [
        candidates_by_key[cluster_key]
        for cluster_key in selected_keys
    ]


def load_cached_version(
    connection: sqlite3.Connection,
    request_fingerprint_sha256: str,
) -> Optional[dict[str, Any]]:
    row = connection.execute(
        """
        SELECT *
        FROM unified_story_versions
        WHERE request_fingerprint_sha256 = ?
        """,
        (request_fingerprint_sha256,),
    ).fetchone()
    return dict(row) if row is not None else None


_VERSION_COLUMNS = (
    "cluster_id",
    "cluster_key",
    "source_fingerprint_sha256",
    "input_fingerprint_sha256",
    "request_fingerprint_sha256",
    "model_name",
    "model_snapshot",
    "prompt_version",
    "input_schema_version",
    "output_schema_version",
    "resolved_schema_version",
    "reasoning_effort",
    "max_output_tokens",
    "generation_status",
    "validation_status",
    "output_json",
    "resolved_output_json",
    "validation_json",
    "preflight_json",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "estimated_cost_usd",
    "response_id",
    "attempts",
    "fallback_reason",
    "human_review_decision",
    "human_review_scores_json",
    "human_review_notes",
    "human_review_source_sha256",
    "human_review_imported_at",
    "created_at",
    "updated_at",
)


def _persist_version(
    connection: sqlite3.Connection,
    values: Mapping[str, Any],
    *,
    allow_status_demotion: bool = False,
) -> dict[str, Any]:
    existing = load_cached_version(
        connection,
        str(values["request_fingerprint_sha256"]),
    )
    if (
        not allow_status_demotion
        and existing is not None
        and existing["generation_status"] == GENERATION_STATUS_ACCEPTED
        and values["generation_status"] != GENERATION_STATUS_ACCEPTED
    ):
        return existing

    placeholders = ", ".join("?" for _ in _VERSION_COLUMNS)
    update_columns = [
        column
        for column in _VERSION_COLUMNS
        if column not in {"request_fingerprint_sha256", "created_at"}
    ]
    updates = ", ".join(
        f"{column} = excluded.{column}"
        for column in update_columns
    )
    connection.execute(
        f"""
        INSERT INTO unified_story_versions (
            {", ".join(_VERSION_COLUMNS)}
        )
        VALUES ({placeholders})
        ON CONFLICT(request_fingerprint_sha256) DO UPDATE SET
            {updates}
        """,
        tuple(values.get(column) for column in _VERSION_COLUMNS),
    )
    connection.commit()
    persisted = load_cached_version(
        connection,
        str(values["request_fingerprint_sha256"]),
    )
    if persisted is None:
        raise RuntimeError("unified story version was not persisted")
    sync_gpt_unification_review_queue(connection, persisted)
    connection.commit()
    return persisted


def _base_version_values(
    *,
    cluster: Mapping[str, Any],
    identity: GenerationIdentity,
) -> dict[str, Any]:
    now = datetime.now().isoformat(timespec="seconds")
    return {
        "cluster_id": int(cluster["id"]),
        "cluster_key": str(cluster["cluster_key"]),
        "source_fingerprint_sha256": (
            identity.source_fingerprint_sha256
        ),
        "input_fingerprint_sha256": identity.input_fingerprint_sha256,
        "request_fingerprint_sha256": (
            identity.request_fingerprint_sha256
        ),
        "model_name": identity.request.model,
        "model_snapshot": None,
        "prompt_version": identity.contract_input.prompt_version,
        "input_schema_version": GPT_INPUT_SCHEMA_VERSION_V2,
        "output_schema_version": GPT_OUTPUT_SCHEMA_VERSION_V2,
        "resolved_schema_version": None,
        "reasoning_effort": identity.request.reasoning_effort,
        "max_output_tokens": identity.request.max_output_tokens,
        "generation_status": GENERATION_STATUS_FALLBACK,
        "validation_status": "not_run",
        "output_json": None,
        "resolved_output_json": None,
        "validation_json": _canonical_json(
            {
                "provenance": None,
                "fact_shape": None,
                "claim_projection": None,
                "semantic_support": None,
            }
        ),
        "preflight_json": None,
        "input_tokens": None,
        "output_tokens": None,
        "total_tokens": None,
        "estimated_cost_usd": None,
        "response_id": None,
        "attempts": 0,
        "fallback_reason": None,
        "created_at": now,
        "updated_at": now,
    }


def _nonnegative_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _usage_metadata(
    response: Any,
    *,
    model_name: str,
) -> dict[str, Any]:
    usage = _field_value(response, "usage")
    input_tokens = _nonnegative_int(
        _field_value(usage, "input_tokens")
    )
    output_tokens = _nonnegative_int(
        _field_value(usage, "output_tokens")
    )
    total_tokens = _nonnegative_int(
        _field_value(usage, "total_tokens")
    )
    estimated_cost = None
    pricing = MODEL_PRICING.get(model_name)
    if (
        pricing is not None
        and input_tokens is not None
        and output_tokens is not None
    ):
        estimated_cost = (
            Decimal(input_tokens)
            * pricing.input_usd_per_million_tokens
            + Decimal(output_tokens)
            * pricing.output_usd_per_million_tokens
        ) / Decimal("1000000")
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "estimated_cost_usd": (
            format(estimated_cost, "f")
            if estimated_cost is not None
            else None
        ),
    }


def _validation_payload(
    provenance: Any,
    fact_shape: Any,
    claim_projection: Any = None,
    semantic_support: Any = None,
    numerical_conflict: Any = None,
) -> str:
    return _canonical_json(
        {
            "provenance": (
                {
                    "accepted": provenance.accepted,
                    "issues": [
                        {
                            "code": issue.code,
                            "location": issue.location,
                            "message": issue.message,
                        }
                        for issue in provenance.issues
                    ],
                    "warnings": [
                        {
                            "code": warning.code,
                            "location": warning.location,
                            "message": warning.message,
                        }
                        for warning in provenance.warnings
                    ],
                }
                if provenance is not None
                else None
            ),
            "fact_shape": (
                fact_shape.to_dict()
                if fact_shape is not None
                else None
            ),
            "claim_projection": (
                claim_projection.to_dict()
                if claim_projection is not None
                else None
            ),
            "semantic_support": (
                semantic_support.to_dict()
                if semantic_support is not None
                else None
            ),
            "numerical_conflict": (
                numerical_conflict.to_dict()
                if numerical_conflict is not None
                else None
            ),
        }
    )


def _fallback_values(
    base: Mapping[str, Any],
    *,
    reason: str,
    validation_status: str = "not_run",
    preflight: Any = None,
    response: Any = None,
    attempts: int = 0,
    output: Any = None,
    resolved_output: Any = None,
    validation_json: Optional[str] = None,
) -> dict[str, Any]:
    values = dict(base)
    values.update(
        {
            "generation_status": GENERATION_STATUS_FALLBACK,
            "validation_status": validation_status,
            "fallback_reason": reason,
            "attempts": attempts,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "preflight_json": (
                _canonical_json(preflight.to_dict())
                if preflight is not None
                else None
            ),
            "output_json": (
                _canonical_json(output) if output is not None else None
            ),
            "resolved_output_json": (
                _canonical_json(resolved_output)
                if resolved_output is not None
                else None
            ),
        }
    )
    if validation_json is not None:
        values["validation_json"] = validation_json
    if response is not None:
        usage = _usage_metadata(
            response,
            model_name=str(base["model_name"]),
        )
        values.update(usage)
        response_id = _field_value(response, "id")
        response_model = _field_value(response, "model")
        values["response_id"] = (
            str(response_id).strip() if response_id else None
        )
        values["model_snapshot"] = (
            str(response_model).strip()
            if response_model
            else str(base["model_name"])
        )
    return values


def _interpret_generation(
    *,
    base: Mapping[str, Any],
    identity: GenerationIdentity,
    preflight: Any,
    generation: Any,
) -> dict[str, Any]:
    response = generation.response
    if generation.outcome is AdapterOutcome.REFUSAL:
        return _fallback_values(
            base,
            reason="refusal",
            validation_status="refused",
            preflight=preflight,
            response=response,
            attempts=generation.attempts,
        )

    if _field_value(response, "status") == "incomplete":
        return _fallback_values(
            base,
            reason="incomplete",
            validation_status="incomplete",
            preflight=preflight,
            response=response,
            attempts=generation.attempts,
        )

    try:
        parsed = _field_value(response, "output_parsed")
        if parsed is not None:
            structured_output = GPTUnifiedStoryResponseV2.model_validate(
                parsed
            )
        else:
            output_text = _field_value(response, "output_text")
            if not isinstance(output_text, str) or not output_text.strip():
                raise ValueError("missing structured output")
            structured_output = (
                GPTUnifiedStoryResponseV2.model_validate_json(output_text)
            )
    except (ValidationError, ValueError, TypeError):
        return _fallback_values(
            base,
            reason="malformed_schema",
            validation_status="structure_failed",
            preflight=preflight,
            response=response,
            attempts=generation.attempts,
        )

    output_payload = structured_output.model_dump(mode="json")
    structured_output, repair_warnings = (
        repair_uniquely_truncated_evidence_span_ids(
            identity.contract_input,
            structured_output,
        )
    )
    provenance = validate_gpt_response_v2(
        identity.contract_input,
        structured_output,
    )
    provenance = _append_provenance_warnings(
        provenance,
        repair_warnings,
    )
    if not provenance.accepted:
        return _fallback_values(
            base,
            reason="provenance_validation_failed",
            validation_status="provenance_failed",
            preflight=preflight,
            response=response,
            attempts=generation.attempts,
            output=output_payload,
            validation_json=_validation_payload(provenance, None),
        )

    resolved = resolve_gpt_response_v2(
        identity.contract_input,
        structured_output,
        validation=provenance,
    )
    resolved_payload = resolved.model_dump(mode="json")
    semantic_support = validate_semantic_support_strands(
        resolved,
        identity.contract_input,
    )
    numerical_conflict = validate_numerical_conflict_coverage(
        resolved,
        identity.contract_input,
    )
    fact_shape = validate_resolved_fact_shapes(
        resolved,
        identity.contract_input,
    )
    claim_projection = validate_claim_projection(
        resolved,
        identity.contract_input,
    )
    validation_json = _validation_payload(
        provenance,
        fact_shape,
        claim_projection,
        semantic_support,
        numerical_conflict,
    )
    if not fact_shape.accepted:
        return _fallback_values(
            base,
            reason="fact_shape_validation_failed",
            validation_status="fact_shape_failed",
            preflight=preflight,
            response=response,
            attempts=generation.attempts,
            output=output_payload,
            resolved_output=resolved_payload,
            validation_json=validation_json,
        )

    if not claim_projection.accepted:
        return _fallback_values(
            base,
            reason="claim_projection_validation_failed",
            validation_status="claim_projection_failed",
            preflight=preflight,
            response=response,
            attempts=generation.attempts,
            output=output_payload,
            resolved_output=resolved_payload,
            validation_json=validation_json,
        )

    values = _fallback_values(
        base,
        reason="",
        validation_status=(
            VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS
            if (
                provenance.warnings
                or not semantic_support.accepted
                or not numerical_conflict.accepted
            )
            else VALIDATION_STATUS_ACCEPTED
        ),
        preflight=preflight,
        response=response,
        attempts=generation.attempts,
        output=output_payload,
        resolved_output=resolved_payload,
        validation_json=validation_json,
    )
    values.update(
        {
            "generation_status": GENERATION_STATUS_ACCEPTED,
            "fallback_reason": None,
            "resolved_schema_version": (
                GPT_RESOLVED_SCHEMA_VERSION_V2
            ),
        }
    )
    return values


def _revalidate_cached_values(
    *,
    cached: Mapping[str, Any],
    identity: GenerationIdentity,
    cluster_id: int,
) -> dict[str, Any]:
    values = dict(cached)
    values["cluster_id"] = cluster_id
    values["updated_at"] = datetime.now().isoformat(timespec="seconds")

    try:
        structured_output = GPTUnifiedStoryResponseV2.model_validate_json(
            str(cached["output_json"])
        )
    except (ValidationError, ValueError, TypeError):
        values.update(
            {
                "generation_status": GENERATION_STATUS_FALLBACK,
                "validation_status": "structure_failed",
                "resolved_schema_version": None,
                "resolved_output_json": None,
                "validation_json": _validation_payload(None, None),
                "fallback_reason": "malformed_schema",
            }
        )
        return values

    structured_output, repair_warnings = (
        repair_uniquely_truncated_evidence_span_ids(
            identity.contract_input,
            structured_output,
        )
    )
    provenance = validate_gpt_response_v2(
        identity.contract_input,
        structured_output,
    )
    provenance = _append_provenance_warnings(
        provenance,
        repair_warnings,
    )
    provenance = _review_demotes_unaccounted_article_only(
        cached,
        provenance,
    )
    if not provenance.accepted:
        values.update(
            {
                "generation_status": GENERATION_STATUS_FALLBACK,
                "validation_status": "provenance_failed",
                "resolved_schema_version": None,
                "resolved_output_json": None,
                "validation_json": _validation_payload(provenance, None),
                "fallback_reason": "provenance_validation_failed",
            }
        )
        return values

    resolved = resolve_gpt_response_v2(
        identity.contract_input,
        structured_output,
        validation=provenance,
    )
    semantic_support = validate_semantic_support_strands(
        resolved,
        identity.contract_input,
    )
    numerical_conflict = validate_numerical_conflict_coverage(
        resolved,
        identity.contract_input,
    )
    fact_shape = validate_resolved_fact_shapes(
        resolved,
        identity.contract_input,
    )
    claim_projection = validate_claim_projection(
        resolved,
        identity.contract_input,
    )
    values.update(
        {
            "resolved_output_json": _canonical_json(
                resolved.model_dump(mode="json")
            ),
            "validation_json": _validation_payload(
                provenance,
                fact_shape,
                claim_projection,
                semantic_support,
                numerical_conflict,
            ),
        }
    )
    if not fact_shape.accepted:
        values.update(
            {
                "generation_status": GENERATION_STATUS_FALLBACK,
                "validation_status": "fact_shape_failed",
                "resolved_schema_version": None,
                "fallback_reason": "fact_shape_validation_failed",
            }
        )
        return values

    if not claim_projection.accepted:
        values.update(
            {
                "generation_status": GENERATION_STATUS_FALLBACK,
                "validation_status": "claim_projection_failed",
                "resolved_schema_version": None,
                "fallback_reason": (
                    "claim_projection_validation_failed"
                ),
            }
        )
        return values

    values.update(
        {
            "generation_status": GENERATION_STATUS_ACCEPTED,
            "validation_status": (
                VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS
                if (
                    provenance.warnings
                    or not semantic_support.accepted
                    or not numerical_conflict.accepted
                )
                else VALIDATION_STATUS_ACCEPTED
            ),
            "resolved_schema_version": GPT_RESOLVED_SCHEMA_VERSION_V2,
            "fallback_reason": None,
        }
    )
    return values


def revalidate_cached_unification(
    *,
    config: Optional[PipelineConfig] = None,
    cluster_keys: Optional[Sequence[str]] = None,
) -> dict[str, Any]:
    """Re-run local validators against persisted outputs without API calls."""
    config = config or load_config()
    connection = get_connection(config)
    candidates = _select_generation_candidates(
        _load_generation_candidates(connection),
        cluster_keys,
    )
    stats = {
        "clusters_seen": len(candidates),
        "stored_outputs": 0,
        "accepted": 0,
        "already_accepted": 0,
        "promoted": 0,
        "fallbacks": 0,
        "pending_review": 0,
        "missing_outputs": 0,
        "fallback_reasons": {},
        "generation_calls": 0,
        "estimated_cost_usd": "0",
        "ongoing_generation_policy": None,
        "shadow_mode": config.gpt_shadow_mode,
    }

    def record_fallback(reason: str) -> None:
        stats["fallbacks"] += 1
        reasons = stats["fallback_reasons"]
        reasons[reason] = reasons.get(reason, 0) + 1

    try:
        for cluster, members, article_records in candidates:
            try:
                identity = build_generation_identity(
                    cluster=cluster,
                    members=members,
                    article_records_by_id=article_records,
                    config=config,
                )
            except (ValueError, ValidationError):
                record_fallback("invalid_cluster_input")
                continue

            cached = load_cached_version(
                connection,
                identity.request_fingerprint_sha256,
            )
            if cached is None or not cached.get("output_json"):
                stats["missing_outputs"] += 1
                continue

            stats["stored_outputs"] += 1
            was_accepted = version_is_deployable_gpt(cached)
            values = _revalidate_cached_values(
                cached=cached,
                identity=identity,
                cluster_id=int(cluster["id"]),
            )
            persisted = _persist_version(
                connection,
                values,
                allow_status_demotion=True,
            )
            is_accepted = version_is_deployable_gpt(persisted)
            if is_accepted:
                stats["accepted"] += 1
                if was_accepted:
                    stats["already_accepted"] += 1
                else:
                    stats["promoted"] += 1
                continue
            if version_is_pending_validator_warning(persisted):
                stats["pending_review"] += 1
                continue

            record_fallback(effective_fallback_reason(persisted))
    finally:
        connection.close()

    logger.info(
        "GPT revalidation: %s accepted (%s promoted), %s pending review, "
        "%s fallbacks, %s missing outputs",
        stats["accepted"],
        stats["promoted"],
        stats["pending_review"],
        stats["fallbacks"],
        stats["missing_outputs"],
    )
    return stats


def verify_unification_cache(
    *,
    config: Optional[PipelineConfig] = None,
) -> dict[str, Any]:
    """Verify cache coverage without constructing an API client."""
    config = config or load_config()
    connection = get_connection(config)
    candidates = _load_generation_candidates(connection)
    stats = {
        "clusters_seen": len(candidates),
        "cache_hits": 0,
        "accepted": 0,
        "cached_fallbacks": 0,
        "cached_pending_review": 0,
        "cache_misses": 0,
        "invalid_inputs": 0,
        "generation_calls": 0,
        "estimated_cost_usd": "0",
        "cache_complete": False,
    }
    try:
        for cluster, members, article_records in candidates:
            try:
                identity = build_generation_identity(
                    cluster=cluster,
                    members=members,
                    article_records_by_id=article_records,
                    config=config,
                )
            except (ValueError, ValidationError):
                stats["invalid_inputs"] += 1
                continue

            cached = load_cached_version(
                connection,
                identity.request_fingerprint_sha256,
            )
            if cached is None:
                stats["cache_misses"] += 1
                continue
            if version_is_deployable_gpt(cached):
                stats["cache_hits"] += 1
                stats["accepted"] += 1
                continue
            if version_is_pending_validator_warning(cached):
                stats["cache_hits"] += 1
                stats["cached_pending_review"] += 1
                continue
            if (
                (
                    cached["generation_status"] == GENERATION_STATUS_FALLBACK
                    or human_review_blocks_version(cached)
                )
                and cached.get("response_id")
            ):
                stats["cache_hits"] += 1
                stats["cached_fallbacks"] += 1
                continue
            stats["cache_misses"] += 1
    finally:
        connection.close()

    stats["cache_complete"] = (
        stats["cache_hits"] == stats["clusters_seen"]
        and stats["cache_misses"] == 0
        and stats["invalid_inputs"] == 0
    )
    logger.info(
        "GPT cache verification: %s/%s cached (%s accepted, "
        "%s pending review, %s fallbacks), %s misses, "
        "0 generation calls",
        stats["cache_hits"],
        stats["clusters_seen"],
        stats["accepted"],
        stats["cached_pending_review"],
        stats["cached_fallbacks"],
        stats["cache_misses"],
    )
    return stats


def run_gpt_unification(
    *,
    no_gpt: bool = False,
    force: bool = False,
    config: Optional[PipelineConfig] = None,
    generator: Any = None,
    preflight: Any = None,
    cluster_keys: Optional[Sequence[str]] = None,
) -> dict[str, Any]:
    config = config or load_config()
    connection = get_connection(config)
    try:
        candidates = _select_generation_candidates(
            _load_generation_candidates(connection),
            cluster_keys,
        )
    except Exception:
        connection.close()
        raise
    stats = {
        "clusters_seen": len(candidates),
        "eligible_clusters": 0,
        "accepted": 0,
        "cache_hits": 0,
        "cached_fallbacks": 0,
        "pending_review": 0,
        "generation_calls": 0,
        "fallbacks": 0,
        "invalid_inputs": 0,
        "fallback_reasons": {},
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "estimated_cost_usd": "0",
    }
    if not candidates:
        connection.close()
        logger.info(
            "GPT unification: no new or changed story clusters; "
            "generation calls: 0"
        )
        return stats
    total_cost = Decimal("0")

    prepared_candidates = []
    provider_request_count = 0
    for cluster, members, article_records in candidates:
        try:
            identity = build_generation_identity(
                cluster=cluster,
                members=members,
                article_records_by_id=article_records,
                config=config,
            )
        except (ValueError, ValidationError):
            prepared_candidates.append(
                (cluster, members, article_records, None, None)
            )
            continue

        cached = load_cached_version(
            connection,
            identity.request_fingerprint_sha256,
        )
        prepared_candidates.append(
            (cluster, members, article_records, identity, cached)
        )
        reusable_without_provider = (
            not force
            and (
                version_is_deployable_gpt(cached)
                or version_is_pending_validator_warning(cached)
                or (
                    cached is not None
                    and (
                        cached["generation_status"]
                        == GENERATION_STATUS_FALLBACK
                        or human_review_blocks_version(cached)
                    )
                    and cached.get("response_id")
                )
            )
        )
        if not reusable_without_provider:
            provider_request_count += 1

    stats["provider_request_candidates"] = provider_request_count

    offline_reason = None
    if no_gpt:
        offline_reason = "offline_requested"
    elif not config.gpt_enabled:
        offline_reason = "gpt_disabled"
    elif not config.openai_api_key and generator is None:
        offline_reason = "missing_api_key"

    gated_generator = None
    if offline_reason is None:
        if generator is None and config.gpt_only_publication_enabled:
            from news_pipeline.unification.ongoing_generation_policy import (
                enforce_ongoing_generation_policy,
            )

            policy_gate = enforce_ongoing_generation_policy(
                config=config,
                selected_cluster_count=provider_request_count,
            )
            preflight = preflight or policy_gate.preflight
            stats["ongoing_generation_policy"] = policy_gate.to_dict()
        generator = generator or OpenAIResponsesAdapter.from_config(config)
        preflight = preflight or GPTPreflight.from_config(config)
        gated_generator = PreflightedGPTGenerator(
            preflight,
            generator,
        )

    def record_fallback(reason: str) -> None:
        stats["fallbacks"] += 1
        reasons = stats["fallback_reasons"]
        reasons[reason] = reasons.get(reason, 0) + 1

    try:
        for (
            cluster,
            members,
            article_records,
            identity,
            cached,
        ) in prepared_candidates:
            if identity is None:
                stats["invalid_inputs"] += 1
                record_fallback("invalid_cluster_input")
                logger.warning(
                    "GPT unification fallback for cluster %s: "
                    "invalid_cluster_input",
                    cluster.get("cluster_key"),
                )
                continue

            stats["eligible_clusters"] += 1
            if (
                not force
                and version_is_deployable_gpt(cached)
            ):
                stats["accepted"] += 1
                stats["cache_hits"] += 1
                continue
            if (
                not force
                and version_is_pending_validator_warning(cached)
            ):
                stats["cache_hits"] += 1
                stats["pending_review"] += 1
                continue
            if (
                not force
                and cached is not None
                and (
                    cached["generation_status"] == GENERATION_STATUS_FALLBACK
                    or human_review_blocks_version(cached)
                )
                and cached.get("response_id")
            ):
                # A provider response was already paid for and reached a
                # terminal local outcome. Reuse its safe V2 fallback unless
                # the operator explicitly requests a forced regeneration.
                stats["cache_hits"] += 1
                stats["cached_fallbacks"] += 1
                record_fallback(effective_fallback_reason(cached))
                continue

            base = _base_version_values(
                cluster=cluster,
                identity=identity,
            )
            if offline_reason is not None:
                _persist_version(
                    connection,
                    _fallback_values(
                        base,
                        reason=offline_reason,
                    ),
                )
                record_fallback(offline_reason)
                continue

            try:
                gated_result = gated_generator.generate(identity.request)
            except Exception as error:
                diagnostic_error = getattr(error, "last_error", error)
                body = getattr(diagnostic_error, "body", None)
                error_code = (
                    body.get("code")
                    if isinstance(body, Mapping)
                    else None
                )
                error_param = (
                    body.get("param")
                    if isinstance(body, Mapping)
                    else None
                )
                logger.warning(
                    "GPT unification provider error for cluster %s: "
                    "type=%s status=%s code=%s param=%s request_id=%s",
                    cluster.get("cluster_key"),
                    type(diagnostic_error).__name__,
                    getattr(diagnostic_error, "status_code", None),
                    error_code,
                    error_param,
                    getattr(diagnostic_error, "request_id", None),
                )
                _persist_version(
                    connection,
                    _fallback_values(
                        base,
                        reason="provider_error",
                        validation_status="provider_error",
                    ),
                )
                record_fallback("provider_error")
                continue

            if gated_result.used_v2_fallback:
                reason = f"preflight_{gated_result.preflight.reason.value}"
                _persist_version(
                    connection,
                    _fallback_values(
                        base,
                        reason=reason,
                        validation_status="preflight_fallback",
                        preflight=gated_result.preflight,
                    ),
                )
                record_fallback(reason)
                continue

            stats["generation_calls"] += 1
            values = _interpret_generation(
                base=base,
                identity=identity,
                preflight=gated_result.preflight,
                generation=gated_result.generation,
            )
            persisted = _persist_version(connection, values)
            stats["input_tokens"] += int(values.get("input_tokens") or 0)
            stats["output_tokens"] += int(
                values.get("output_tokens") or 0
            )
            stats["total_tokens"] += int(values.get("total_tokens") or 0)
            if version_is_deployable_gpt(persisted):
                stats["accepted"] += 1
            elif version_is_pending_validator_warning(persisted):
                stats["pending_review"] += 1
            else:
                record_fallback(effective_fallback_reason(persisted))
            if values.get("estimated_cost_usd") is not None:
                total_cost += Decimal(str(values["estimated_cost_usd"]))
    finally:
        connection.close()

    stats["estimated_cost_usd"] = format(total_cost, "f")
    logger.info(
        "GPT unification: %s accepted (%s cache hits), %s pending review, "
        "%s fallbacks",
        stats["accepted"],
        stats["cache_hits"],
        stats["pending_review"],
        stats["fallbacks"],
    )
    return stats
