from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Optional, Sequence

from pydantic import ValidationError

from news_pipeline.clustering.semantic_partition import (
    apply_semantic_partition,
    validate_semantic_partition,
)
from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger
from news_pipeline.unification.autonomous_audit import (
    AUTONOMOUS_AUDIT_VERSION,
    AutonomousAuditResponse,
    build_autonomous_audit_request,
    classify_audit_route,
    decide_autonomous_audit,
)
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
    GPTCorrectionUnifiedStoryResponseV2,
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
    response_usage_cost_usd,
)
from news_pipeline.unification.openai_adapter import (
    AdapterOutcome,
    OpenAIResponsesAdapter,
    StructuredResponseRequest,
)


logger = get_logger()

AUDIT_CAPACITY_MIN_OUTPUT_TOKENS = 2048
AUDIT_CAPACITY_VALIDATION_ALLOWANCE_CHARS = 4096

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
REVIEWED_CORRECTION_TARGET = "v2_10_reviewed_correction_raw_candidate"
REVIEWED_CORRECTION_FOLLOWUP_TARGET = (
    "v2_10_reviewed_correction_followup_raw_candidate"
)
REVIEWED_CORRECTION_LINEAGE_TARGET = (
    "v2_10_reviewed_correction_lineage_raw_candidate"
)
MAX_REVIEWED_CORRECTION_DEPTH = 16
REVIEWED_CORRECTION_PROMPT_SAFETY_MODE = (
    "review_requirements_devanagari_redaction_v1"
)
REVIEWED_CORRECTION_OUTPUT_CONSTRAINT_MODE = (
    "provider_schema_no_devanagari_pattern_v1"
)
REVIEWED_CORRECTION_REDACTION_MARKER = "[removed Devanagari token]"
HUMAN_REVIEW_ACCEPTABLE_WARNING_CODES = frozenset(
    {
        "truncated_evidence_span_id_repaired",
        "unknown_evidence_span_id",
    }
)
_DEVANAGARI_RE = re.compile(r"[\u0900-\u097f]+")
REVIEWED_CORRECTION_OUTPUT_RULES = (
    "The title, unified_story, claims, and conflict descriptions must not "
    "contain Devanagari characters (Unicode U+0900-U+097F), including "
    "defective tokens quoted in the requirements. Never copy or quote "
    "those tokens into the output. Replace them with natural Sinhala "
    "wording appropriate to the supported source context."
)
HUMAN_REVIEW_FACT_SHAPE_OVERRIDE_TARGETS = frozenset(
    {
        "rejected_gpt_candidate",
        "prompt_v2_2_gpt_candidate",
        "gpt_only_quarantine_candidate",
        "phase4_prompt_v2_8_gpt_only_candidate",
        "phase4_semantic_gate_holdout_candidate",
        "v2_9_completion_raw_candidate",
        "v2_10_reviewed_correction_raw_candidate",
        "v2_10_reviewed_correction_followup_raw_candidate",
        "v2_10_reviewed_correction_lineage_raw_candidate",
        "v2_10_reviewed_remediation_raw_candidate",
    }
)
HUMAN_REVIEW_WARNING_OVERRIDE_TARGETS = frozenset(
    {
        *HUMAN_REVIEW_FACT_SHAPE_OVERRIDE_TARGETS,
        "phase4_semantic_gate_holdout_candidate",
        "v2_9_completion_raw_candidate",
        "v2_10_prison_correction_raw_candidate",
        "v2_10_reviewed_remediation_raw_candidate",
    }
)
HUMAN_REVIEW_HARD_VALIDATOR_OVERRIDE_TARGETS = frozenset(
    {
        "gpt_only_quarantine_candidate",
        "v2_9_completion_raw_candidate",
        "v2_10_reviewed_correction_raw_candidate",
        "v2_10_reviewed_correction_followup_raw_candidate",
        "v2_10_reviewed_correction_lineage_raw_candidate",
        "v2_10_reviewed_remediation_raw_candidate",
    }
)
GPT_PUBLICATION_STATUS_PUBLISHABLE = "publishable"
GPT_PUBLICATION_STATUS_PENDING_REVIEW = "pending_review"
GPT_PUBLICATION_STATUS_REJECTED = "rejected"
GPT_PUBLICATION_STATUS_UNAVAILABLE = "unavailable"
GPT_REVIEW_QUEUE_STATUS_PENDING = "pending_review"
GPT_REVIEW_QUEUE_STATUS_APPROVED = "approved"
GPT_REVIEW_QUEUE_STATUS_REJECTED = "rejected"
GPT_REVIEW_QUEUE_STATUS_SUPERSEDED = "superseded"


def sanitize_reviewed_correction_requirements_for_prompt(
    requirements: str,
) -> str:
    """Remove quoted Devanagari defects from provider-facing notes only."""
    return _DEVANAGARI_RE.sub(
        REVIEWED_CORRECTION_REDACTION_MARKER,
        requirements,
    )


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
        and not review.get("correction_required")
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
    issue_codes = _validation_issue_codes(version)
    assessment_is_sufficient = (
        review.get("validator_assessment") == "false_positive"
        or (
            decision == "accept"
            and review.get("validator_assessment") == "unclear"
            and bool(issue_codes)
            and issue_codes <= HUMAN_REVIEW_ACCEPTABLE_WARNING_CODES
        )
    )
    return bool(
        version.get("generation_status") == GENERATION_STATUS_ACCEPTED
        and version.get("validation_status")
        == VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS
        and version.get("response_id")
        and version.get("resolved_output_json")
        and review.get("review_target")
        in HUMAN_REVIEW_WARNING_OVERRIDE_TARGETS
        and assessment_is_sufficient
        and review.get("unsupported_material_claim") == "no"
        and not review.get("correction_required")
    )


def human_review_overrides_hard_validator(
    version: Optional[Mapping[str, Any]],
) -> bool:
    """Allow only an explicit audited false-positive validator review."""
    if version is None:
        return False
    decision = str(version.get("human_review_decision") or "").strip()
    if decision != "accept":
        return False
    review = _review_payload(version)
    return bool(
        version.get("generation_status") == GENERATION_STATUS_FALLBACK
        and version.get("validation_status")
        in {"provenance_failed", "claim_projection_failed"}
        and version.get("response_id")
        and version.get("resolved_output_json")
        and review.get("review_target")
        in HUMAN_REVIEW_HARD_VALIDATOR_OVERRIDE_TARGETS
        and review.get("validator_assessment") == "false_positive"
        and review.get("unsupported_material_claim") == "no"
        and not review.get("correction_required")
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
        or review.get("correction_required")
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
                and (
                    version.get("validation_status")
                    == VALIDATION_STATUS_ACCEPTED
                    or (
                        version.get("validation_status")
                        == VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS
                        and version.get("autonomous_audit_status")
                        == "accepted"
                    )
                )
            )
            or human_review_overrides_fact_shape(version)
            or human_review_overrides_validator_warning(version)
            or human_review_overrides_hard_validator(version)
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
                    or human_review_overrides_hard_validator(version)
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
    now = datetime.now().isoformat(timespec="seconds")
    connection.execute(
        """
        UPDATE gpt_unification_review_queue
        SET queue_status = ?,
            updated_at = ?
        WHERE story_id = ?
          AND unified_story_version_id < ?
          AND queue_status = ?
          AND review_decision IS NULL
        """,
        (
            GPT_REVIEW_QUEUE_STATUS_SUPERSEDED,
            now,
            str(version["cluster_key"]),
            int(version["id"]),
            GPT_REVIEW_QUEUE_STATUS_PENDING,
        ),
    )
    if queue_status is None:
        if existing is None:
            return None
        queue_status = GPT_REVIEW_QUEUE_STATUS_APPROVED

    title, story = _candidate_snapshot(version)
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
    query += " ORDER BY id"
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


def _build_audit_capacity_probe(
    *,
    identity: GenerationIdentity,
    route: Any,
    config: PipelineConfig,
) -> StructuredResponseRequest:
    """Build a local-only upper-bound request used to reserve audit capacity."""
    draft_allowance_chars = identity.request.max_output_tokens * 4
    return build_autonomous_audit_request(
        contract_input=identity.contract_input,
        candidate={
            "budget_capacity_reservation": "x" * draft_allowance_chars,
        },
        validation={
            "budget_capacity_reservation": (
                "x" * AUDIT_CAPACITY_VALIDATION_ALLOWANCE_CHARS
            ),
        },
        route=route,
        config=config,
    )


def _budget_safe_audit_route(
    route: Any,
    *,
    config: PipelineConfig,
) -> Any | None:
    """Return the standard Luna route when a preferred route cannot fit."""
    if (
        route.model == config.gpt_audit_model
        and route.reasoning_effort == config.gpt_audit_reasoning_effort
    ):
        return None
    reasons = tuple(route.reasons)
    if "budget_safe_luna_route" not in reasons:
        reasons += ("budget_safe_luna_route",)
    return replace(
        route,
        model=config.gpt_audit_model,
        reasoning_effort=config.gpt_audit_reasoning_effort,
        reasons=reasons,
    )


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


def build_reviewed_correction_followup_requirements(
    *,
    original_requirements: str,
    reviewed_candidate_decision: str,
    reviewed_candidate_notes: str,
) -> str:
    """Build stable requirements for a reviewed correction follow-up."""
    original = str(original_requirements or "").strip()
    decision = str(reviewed_candidate_decision or "").strip()
    notes = str(reviewed_candidate_notes or "").strip()
    if not original or decision not in {"minor_issue", "major_issue"}:
        raise ValueError("follow-up correction review is ineligible")
    if not notes:
        raise ValueError("follow-up correction notes are required")
    return _canonical_json(
        {
            "original_correction_requirements": original,
            "reviewed_candidate_decision": decision,
            "reviewed_candidate_correction_requirements": notes,
        }
    )


@dataclass(frozen=True)
class ReviewedCorrectionLineageStep:
    depth: int
    requirements: str
    request_fingerprint_sha256: str
    version_id: int
    review_target: Optional[str]
    review_decision: Optional[str]
    review_source_sha256: Optional[str]
    review_notes: Optional[str]


@dataclass(frozen=True)
class ReviewedCorrectionLineage:
    status: str
    steps: tuple[ReviewedCorrectionLineageStep, ...]
    next_requirements: Optional[str]
    next_depth: Optional[int]
    next_request_fingerprint_sha256: Optional[str]
    accepted_version: Optional[dict[str, Any]]


def resolve_reviewed_correction_lineage(
    connection: sqlite3.Connection,
    *,
    cluster: Mapping[str, Any],
    members: list[Mapping[str, Any]],
    article_records_by_id: Mapping[int, Mapping[str, Any]],
    primary_version: Mapping[str, Any],
    config: PipelineConfig,
    allow_unreviewed_redacted_schema_migration: bool = False,
) -> ReviewedCorrectionLineage:
    """Resolve an exact-current, reviewed correction lineage."""
    primary_review = _review_payload(primary_version)
    requirements = str(primary_version.get("human_review_notes") or "").strip()
    if (
        str(primary_version.get("human_review_decision") or "").strip()
        != "minor_issue"
        or primary_review.get("review_target")
        != "gpt_only_quarantine_candidate"
        or primary_review.get("correction_required") is False
        or not requirements
        or not primary_version.get("human_review_source_sha256")
    ):
        return ReviewedCorrectionLineage(
            status="ineligible_primary_review",
            steps=(),
            next_requirements=None,
            next_depth=None,
            next_request_fingerprint_sha256=None,
            accepted_version=None,
        )

    correction_config = replace(
        config,
        gpt_prompt_version=GPT_PROMPT_VERSION_V2_10,
        gpt_reasoning_effort="none",
        gpt_max_output_tokens=8192,
    )
    steps: list[ReviewedCorrectionLineageStep] = []
    seen_fingerprints: set[str] = set()
    for depth in range(MAX_REVIEWED_CORRECTION_DEPTH):
        candidate_identities = []
        candidate_versions = []
        for enforce_rules, redact_requirements, constrain_output in (
            (False, False, False),
            (True, False, False),
            (True, True, False),
            (True, True, True),
        ):
            candidate_identity = build_generation_identity(
                cluster=cluster,
                members=members,
                article_records_by_id=article_records_by_id,
                config=correction_config,
                correction_requirements=requirements,
                enforce_correction_output_rules=enforce_rules,
                redact_devanagari_from_correction_prompt=(
                    redact_requirements
                ),
                enforce_correction_output_schema=constrain_output,
            )
            candidate_versions.append(load_cached_version(
                connection,
                candidate_identity.request_fingerprint_sha256,
            ))
            candidate_identities.append(candidate_identity)
        if not candidate_identities:
            raise RuntimeError("correction identity resolution failed")

        constrained_version = candidate_versions[-1]
        redacted_version = candidate_versions[-2]
        if constrained_version is not None:
            identity = candidate_identities[-1]
            version = constrained_version
        elif (
            allow_unreviewed_redacted_schema_migration
            and redacted_version is not None
            and not redacted_version.get("human_review_source_sha256")
        ):
            return ReviewedCorrectionLineage(
                status="ready_for_generation",
                steps=tuple(steps),
                next_requirements=requirements,
                next_depth=depth,
                next_request_fingerprint_sha256=(
                    candidate_identities[-1].request_fingerprint_sha256
                ),
                accepted_version=None,
            )
        else:
            existing_index = next(
                (
                    index
                    for index in range(len(candidate_versions) - 2, -1, -1)
                    if candidate_versions[index] is not None
                ),
                None,
            )
            if existing_index is None:
                identity = candidate_identities[-1]
                version = None
            else:
                identity = candidate_identities[existing_index]
                version = candidate_versions[existing_index]
        fingerprint = identity.request_fingerprint_sha256
        if fingerprint in seen_fingerprints:
            raise ValueError("reviewed correction lineage contains a cycle")
        seen_fingerprints.add(fingerprint)
        if version is None:
            return ReviewedCorrectionLineage(
                status="ready_for_generation",
                steps=tuple(steps),
                next_requirements=requirements,
                next_depth=depth,
                next_request_fingerprint_sha256=fingerprint,
                accepted_version=None,
            )

        review = _review_payload(version)
        target = str(review.get("review_target") or "").strip() or None
        decision = (
            str(version.get("human_review_decision") or "").strip() or None
        )
        notes = str(version.get("human_review_notes") or "").strip() or None
        review_sha = (
            str(version.get("human_review_source_sha256") or "").strip()
            or None
        )
        steps.append(
            ReviewedCorrectionLineageStep(
                depth=depth,
                requirements=requirements,
                request_fingerprint_sha256=fingerprint,
                version_id=int(version["id"]),
                review_target=target,
                review_decision=decision,
                review_source_sha256=review_sha,
                review_notes=notes,
            )
        )
        legacy_target = (
            REVIEWED_CORRECTION_TARGET
            if depth == 0
            else REVIEWED_CORRECTION_FOLLOWUP_TARGET
        )
        if not review_sha or target not in {
            legacy_target,
            REVIEWED_CORRECTION_LINEAGE_TARGET,
        }:
            return ReviewedCorrectionLineage(
                status="awaiting_eligible_review",
                steps=tuple(steps),
                next_requirements=None,
                next_depth=None,
                next_request_fingerprint_sha256=None,
                accepted_version=None,
            )
        if decision == "accept":
            deployable = version_is_deployable_gpt(version)
            return ReviewedCorrectionLineage(
                status=(
                    "accepted" if deployable else "accepted_review_not_deployable"
                ),
                steps=tuple(steps),
                next_requirements=None,
                next_depth=None,
                next_request_fingerprint_sha256=None,
                accepted_version=dict(version) if deployable else None,
            )
        if decision not in {"minor_issue", "major_issue"} or not notes:
            return ReviewedCorrectionLineage(
                status="review_does_not_request_correction",
                steps=tuple(steps),
                next_requirements=None,
                next_depth=None,
                next_request_fingerprint_sha256=None,
                accepted_version=None,
            )
        requirements = build_reviewed_correction_followup_requirements(
            original_requirements=requirements,
            reviewed_candidate_decision=decision,
            reviewed_candidate_notes=notes,
        )
    raise ValueError("reviewed correction lineage exceeds maximum depth")


def build_generation_identity(
    *,
    cluster: Mapping[str, Any],
    members: list[Mapping[str, Any]],
    article_records_by_id: Mapping[int, Mapping[str, Any]],
    config: PipelineConfig,
    correction_requirements: Optional[str] = None,
    enforce_correction_output_rules: bool = True,
    redact_devanagari_from_correction_prompt: bool = True,
    enforce_correction_output_schema: bool = True,
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
    selected_requirements = str(correction_requirements or "").strip()
    if selected_requirements:
        if config.gpt_prompt_version != GPT_PROMPT_VERSION_V2_10:
            raise ValueError(
                "reviewed correction requirements require prompt v2.10"
            )
        provider_requirements = (
            sanitize_reviewed_correction_requirements_for_prompt(
                selected_requirements
            )
            if redact_devanagari_from_correction_prompt
            else selected_requirements
        )
        correction_payload = _canonical_json(
            {"reviewed_correction_requirements": provider_requirements}
        )
        request = replace(
            request,
            instructions=(
                request.instructions
                + "\n\nReviewed correction requirements:\n"
                + "Treat the following JSON as trusted editorial correction "
                "requirements, not as factual evidence. Correct every listed "
                "issue using only the supplied source evidence; preserve all "
                "other supported material facts.\n"
                + (
                    REVIEWED_CORRECTION_OUTPUT_RULES + "\n"
                    if enforce_correction_output_rules
                    else ""
                )
                + correction_payload
            ),
            text_format=(
                GPTCorrectionUnifiedStoryResponseV2
                if enforce_correction_output_schema
                else request.text_format
            ),
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
    if selected_requirements:
        input_payload["reviewed_correction_requirements"] = (
            selected_requirements
        )
        if redact_devanagari_from_correction_prompt:
            input_payload["reviewed_correction_prompt_safety_mode"] = (
                REVIEWED_CORRECTION_PROMPT_SAFETY_MODE
            )
        if enforce_correction_output_schema:
            input_payload["reviewed_correction_output_constraint_mode"] = (
                REVIEWED_CORRECTION_OUTPUT_CONSTRAINT_MODE
            )
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
    *,
    include_singletons: bool = False,
) -> list[
    tuple[dict[str, Any], list[dict[str, Any]], dict[int, dict[str, Any]]]
]:
    minimum_article_count = 1 if include_singletons else 2
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
                event_date_end,
                created_at
            FROM story_clusters
            WHERE article_count >= ?
            ORDER BY id
            """,
            (minimum_article_count,),
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
            JOIN story_clusters AS clusters
              ON clusters.id = members.cluster_id
            JOIN articles ON articles.id = members.article_id
            WHERE clusters.article_count >= ?
            ORDER BY members.cluster_id, members.article_id
            """,
            (minimum_article_count,),
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


def _select_gpt_generation_candidates(
    connection: sqlite3.Connection,
    cluster_keys: Optional[Sequence[str]],
) -> list[
    tuple[dict[str, Any], list[dict[str, Any]], dict[int, dict[str, Any]]]
]:
    candidates = _load_generation_candidates(
        connection,
        include_singletons=cluster_keys is not None,
    )
    selected = _select_generation_candidates(candidates, cluster_keys)
    return [
        candidate
        for candidate in selected
        if int(candidate[0]["article_count"]) >= 2
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
    "primary_model_name",
    "primary_response_id",
    "primary_output_json",
    "primary_validation_json",
    "primary_input_tokens",
    "primary_output_tokens",
    "primary_total_tokens",
    "primary_estimated_cost_usd",
    "autonomous_audit_status",
    "autonomous_audit_model",
    "autonomous_audit_response_id",
    "autonomous_audit_route_json",
    "autonomous_audit_input_tokens",
    "autonomous_audit_output_tokens",
    "autonomous_audit_total_tokens",
    "autonomous_audit_estimated_cost_usd",
    "autonomous_audit_created_at",
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
        "primary_model_name": None,
        "primary_response_id": None,
        "primary_output_json": None,
        "primary_validation_json": None,
        "primary_input_tokens": None,
        "primary_output_tokens": None,
        "primary_total_tokens": None,
        "primary_estimated_cost_usd": None,
        "autonomous_audit_status": None,
        "autonomous_audit_model": None,
        "autonomous_audit_response_id": None,
        "autonomous_audit_route_json": None,
        "autonomous_audit_input_tokens": None,
        "autonomous_audit_output_tokens": None,
        "autonomous_audit_total_tokens": None,
        "autonomous_audit_estimated_cost_usd": None,
        "autonomous_audit_created_at": None,
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
        estimated_cost = response_usage_cost_usd(response, model_name)
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


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _combined_provider_values(
    *,
    primary: Mapping[str, Any],
    audited: Mapping[str, Any],
    route: Any,
    audit_assessment: Any = None,
    audit_decision: Any = None,
) -> dict[str, Any]:
    values = dict(audited)
    primary_input = int(primary.get("input_tokens") or 0)
    primary_output = int(primary.get("output_tokens") or 0)
    primary_total = int(primary.get("total_tokens") or 0)
    audit_input = int(audited.get("input_tokens") or 0)
    audit_output = int(audited.get("output_tokens") or 0)
    audit_total = int(audited.get("total_tokens") or 0)
    primary_cost = _decimal_or_zero(primary.get("estimated_cost_usd"))
    audit_cost = _decimal_or_zero(audited.get("estimated_cost_usd"))
    values.update(
        {
            "input_tokens": primary_input + audit_input,
            "output_tokens": primary_output + audit_output,
            "total_tokens": primary_total + audit_total,
            "estimated_cost_usd": format(primary_cost + audit_cost, "f"),
            "primary_model_name": primary.get("model_name"),
            "primary_response_id": primary.get("response_id"),
            "primary_output_json": primary.get("output_json"),
            "primary_validation_json": primary.get("validation_json"),
            "primary_input_tokens": primary.get("input_tokens"),
            "primary_output_tokens": primary.get("output_tokens"),
            "primary_total_tokens": primary.get("total_tokens"),
            "primary_estimated_cost_usd": primary.get(
                "estimated_cost_usd"
            ),
            "autonomous_audit_status": (
                "accepted"
                if audited.get("generation_status")
                == GENERATION_STATUS_ACCEPTED
                else "failed"
            ),
            "autonomous_audit_model": audited.get("model_name"),
            "autonomous_audit_response_id": audited.get("response_id"),
            "autonomous_audit_route_json": _canonical_json(
                {
                    "audit_version": AUTONOMOUS_AUDIT_VERSION,
                    "complexity": route.complexity,
                    "risk_tier": route.risk_tier,
                    "reasons": list(route.reasons),
                    "model": route.model,
                    "reasoning_effort": route.reasoning_effort,
                    "policy_decision_reasons": list(
                        audit_decision.reasons
                        if audit_decision is not None
                        else ()
                    ),
                    "change_level": (
                        audit_assessment.change_level
                        if audit_assessment is not None
                        else None
                    ),
                    "correction_categories": (
                        list(audit_assessment.correction_categories)
                        if audit_assessment is not None
                        else []
                    ),
                }
            ),
            "autonomous_audit_input_tokens": audited.get("input_tokens"),
            "autonomous_audit_output_tokens": audited.get("output_tokens"),
            "autonomous_audit_total_tokens": audited.get("total_tokens"),
            "autonomous_audit_estimated_cost_usd": audited.get(
                "estimated_cost_usd"
            ),
            "autonomous_audit_created_at": datetime.now().isoformat(
                timespec="seconds"
            ),
        }
    )
    return values


def _cached_candidate_requires_autonomous_audit(
    cached: Optional[Mapping[str, Any]],
    *,
    config: PipelineConfig,
) -> bool:
    if not config.gpt_autonomous_audit_enabled or cached is None:
        return False
    if version_is_deployable_gpt(cached):
        return False
    if not (
        cached.get("response_id")
        and cached.get("output_json")
        and cached.get("resolved_output_json")
    ):
        return False
    validation_status = str(cached.get("validation_status") or "")
    audit_status = str(cached.get("autonomous_audit_status") or "")
    if not audit_status:
        return validation_status in {
            "fact_shape_failed",
            "claim_projection_failed",
            VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS,
        }
    # Budget preflight and provider transport failures did not produce an
    # audit judgment. They are retryable on a later run. An audit that reached
    # the model and rejected the candidate remains terminal.
    return (
        audit_status == "failed"
        and validation_status
        in {
            "autonomous_audit_preflight",
            "autonomous_audit_failed",
        }
    )


def _audit_circuit_breaker_status(
    connection: sqlite3.Connection,
    *,
    config: PipelineConfig,
) -> dict[str, Any]:
    """Use prior model audit judgments to fail safely, not language rules."""
    change_levels = {"none": 0, "editorial": 0, "material": 0}
    rows = connection.execute(
        """
        SELECT stats_json
        FROM pipeline_runs
        WHERE status = 'completed' AND stats_json IS NOT NULL
        ORDER BY id DESC
        LIMIT 20
        """
    ).fetchall()
    for row in rows:
        try:
            payload = json.loads(str(row["stats_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        unification = payload.get("unification", {})
        if not isinstance(unification, Mapping):
            continue
        by_risk = unification.get("audit_change_levels_by_risk", {})
        if not isinstance(by_risk, Mapping):
            continue
        low = by_risk.get("low", {})
        if not isinstance(low, Mapping):
            continue
        for level in change_levels:
            try:
                change_levels[level] += int(low.get(level) or 0)
            except (TypeError, ValueError):
                continue
    evaluated = sum(change_levels.values())
    material_rate = (
        change_levels["material"] / evaluated if evaluated else None
    )
    configured_mode = config.gpt_audit_policy_mode
    effective_mode = configured_mode
    if configured_mode == "shadow":
        state = "collecting_shadow_evidence"
    elif configured_mode == "all":
        state = "full_audit_configured"
    elif evaluated < config.gpt_audit_circuit_min_evaluated:
        state = "insufficient_evidence_full_audit"
        effective_mode = "all"
    elif (
        material_rate is not None
        and material_rate
        > config.gpt_audit_circuit_max_material_rate
    ):
        state = "material_change_rate_open_full_audit"
        effective_mode = "all"
    else:
        state = "closed_risk_tiered_active"
    return {
        "state": state,
        "configured_mode": configured_mode,
        "effective_mode": effective_mode,
        "evaluated_low_risk_audits": evaluated,
        "low_risk_change_levels": change_levels,
        "material_change_rate": material_rate,
        "minimum_evaluated": config.gpt_audit_circuit_min_evaluated,
        "maximum_material_change_rate": (
            config.gpt_audit_circuit_max_material_rate
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
    structured_output_override: Optional[
        GPTUnifiedStoryResponseV2
    ] = None,
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
        if structured_output_override is not None:
            structured_output = structured_output_override
        else:
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
    values = _fallback_values(
        base,
        reason="",
        validation_status=(
            VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS
            if (
                provenance.warnings
                or not fact_shape.accepted
                or not claim_projection.accepted
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


def _parse_autonomous_audit_response(response: Any) -> AutonomousAuditResponse:
    parsed = _field_value(response, "output_parsed")
    if parsed is not None:
        return AutonomousAuditResponse.model_validate(parsed)
    output_text = _field_value(response, "output_text")
    if not isinstance(output_text, str) or not output_text.strip():
        raise ValueError("missing autonomous audit structured output")
    return AutonomousAuditResponse.model_validate_json(output_text)


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
    values.update(
        {
            "generation_status": GENERATION_STATUS_ACCEPTED,
            "validation_status": (
                VALIDATION_STATUS_ACCEPTED_WITH_WARNINGS
                if (
                    provenance.warnings
                    or not fact_shape.accepted
                    or not claim_projection.accepted
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
    candidates = _select_gpt_generation_candidates(
        connection,
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
        "audit_calls": 0,
        "provider_calls": 0,
        "audit_accepted": 0,
        "audit_failed": 0,
        "audit_provider_failed": 0,
        "audit_rejected": 0,
        "audit_skipped_budget": 0,
        "audit_routes": {},
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


def _merge_counter_mapping(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        current = merged.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            merged[key] = _merge_counter_mapping(current, value)
        elif isinstance(current, int) and isinstance(value, int):
            merged[key] = current + value
        elif current is None:
            merged[key] = value
        else:
            merged[key] = value
    return merged


def _merge_unification_pass_stats(
    first: dict[str, Any],
    followup: Mapping[str, Any],
) -> dict[str, Any]:
    summed_fields = {
        "clusters_seen",
        "eligible_clusters",
        "accepted",
        "cache_hits",
        "cached_fallbacks",
        "pending_review",
        "generation_calls",
        "audit_calls",
        "provider_calls",
        "audit_accepted",
        "audit_failed",
        "audit_provider_failed",
        "audit_rejected",
        "audit_skipped_budget",
        "atomic_budget_deferred",
        "budget_deferred",
        "audit_budget_safe_routes",
        "audit_policy_would_skip",
        "audit_policy_sampled",
        "audits_skipped_low_risk",
        "shadow_avoidable_audit_calls",
        "fallbacks",
        "provider_failed",
        "invalid_inputs",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "provider_request_candidates",
        "provider_request_backlog",
        "deferred_provider_candidates",
        "semantic_partitions_applied",
        "semantic_partition_groups",
        "semantic_partition_multi_groups",
        "semantic_partition_singletons",
    }
    counter_fields = {
        "audit_routes",
        "audit_risk_tiers",
        "audit_change_levels",
        "audit_change_levels_by_risk",
        "fallback_reasons",
        "cached_fallback_reasons",
    }
    list_fields = {
        "semantic_partition_cluster_keys",
        "semantic_partition_followup_keys",
    }
    for field in summed_fields:
        first[field] = int(first.get(field) or 0) + int(
            followup.get(field) or 0
        )
    for field in counter_fields:
        first[field] = _merge_counter_mapping(
            first.get(field) or {},
            followup.get(field) or {},
        )
    for field in list_fields:
        first[field] = sorted(
            {
                str(value)
                for value in (
                    list(first.get(field) or [])
                    + list(followup.get(field) or [])
                )
            }
        )
    first["estimated_cost_usd"] = format(
        _decimal_or_zero(first.get("estimated_cost_usd"))
        + _decimal_or_zero(followup.get("estimated_cost_usd")),
        "f",
    )
    first["shadow_avoidable_audit_cost_usd"] = format(
        _decimal_or_zero(first.get("shadow_avoidable_audit_cost_usd"))
        + _decimal_or_zero(
            followup.get("shadow_avoidable_audit_cost_usd")
        ),
        "f",
    )
    return first


def run_gpt_unification(
    *,
    no_gpt: bool = False,
    force: bool = False,
    config: Optional[PipelineConfig] = None,
    generator: Any = None,
    preflight: Any = None,
    cluster_keys: Optional[Sequence[str]] = None,
    correction_requirements_by_story: Optional[
        Mapping[str, str]
    ] = None,
    _provider_candidates_remaining: Optional[int] = None,
) -> dict[str, Any]:
    config = config or load_config()
    connection = get_connection(config)
    try:
        candidates = _select_gpt_generation_candidates(
            connection,
            cluster_keys,
        )
        audit_circuit_breaker = _audit_circuit_breaker_status(
            connection,
            config=config,
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
        "audit_calls": 0,
        "provider_calls": 0,
        "audit_accepted": 0,
        "audit_failed": 0,
        "audit_provider_failed": 0,
        "audit_rejected": 0,
        "audit_skipped_budget": 0,
        "atomic_budget_deferred": 0,
        "budget_deferred": 0,
        "audit_budget_safe_routes": 0,
        "audit_routes": {},
        "audit_policy_mode": config.gpt_audit_policy_mode,
        "audit_policy_effective_mode": (
            audit_circuit_breaker["effective_mode"]
        ),
        "audit_circuit_breaker": audit_circuit_breaker,
        "audit_risk_tiers": {},
        "audit_policy_would_skip": 0,
        "audit_policy_sampled": 0,
        "audits_skipped_low_risk": 0,
        "shadow_avoidable_audit_calls": 0,
        "shadow_avoidable_audit_cost_usd": "0",
        "audit_change_levels": {},
        "audit_change_levels_by_risk": {},
        "semantic_partitions_applied": 0,
        "semantic_partition_groups": 0,
        "semantic_partition_multi_groups": 0,
        "semantic_partition_singletons": 0,
        "semantic_partition_cluster_keys": [],
        "semantic_partition_followup_keys": [],
        "fallbacks": 0,
        "provider_failed": 0,
        "invalid_inputs": 0,
        "fallback_reasons": {},
        "cached_fallback_reasons": {},
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "estimated_cost_usd": "0",
        "ongoing_generation_policy": None,
    }
    if not candidates:
        connection.close()
        logger.info("Unification summary:")
        logger.info("  No new or changed multi-article stories.")
        logger.info("  GPT calls made: 0 generation | 0 audit")
        return stats
    total_cost = Decimal("0")
    audit_policy_config = replace(
        config,
        gpt_audit_policy_mode=str(
            audit_circuit_breaker["effective_mode"]
        ),
    )

    prepared_candidates = []
    selected_correction_requirements = {
        str(key): str(value).strip()
        for key, value in (correction_requirements_by_story or {}).items()
    }
    candidate_keys = {
        str(candidate[0]["cluster_key"]) for candidate in candidates
    }
    if correction_requirements_by_story is not None and (
        any(
            not key or not value
            for key, value in selected_correction_requirements.items()
        )
        or set(selected_correction_requirements) != candidate_keys
    ):
        connection.close()
        raise ValueError(
            "correction requirements must exactly cover selected stories"
        )
    provider_candidates = []
    for cluster, members, article_records in candidates:
        try:
            identity = build_generation_identity(
                cluster=cluster,
                members=members,
                article_records_by_id=article_records,
                config=config,
                correction_requirements=(
                    selected_correction_requirements.get(
                        str(cluster["cluster_key"])
                    )
                ),
            )
        except (ValueError, ValidationError):
            prepared_candidates.append(
                (cluster, members, article_records, None, None, True)
            )
            continue

        cached = load_cached_version(
            connection,
            identity.request_fingerprint_sha256,
        )
        reusable_without_provider = (
            not force
            and not _cached_candidate_requires_autonomous_audit(
                cached,
                config=config,
            )
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
        prepared = (
            cluster,
            members,
            article_records,
            identity,
            cached,
            reusable_without_provider,
        )
        prepared_candidates.append(prepared)
        if not reusable_without_provider:
            provider_candidates.append(prepared)

    provider_candidates.sort(
        key=lambda candidate: (
            str(candidate[0].get("event_date_end") or ""),
            int(candidate[0]["article_count"]),
            int(candidate[0]["id"]),
        ),
        reverse=True,
    )
    provider_request_backlog = len(provider_candidates)
    provider_candidates_remaining = (
        config.gpt_max_clusters_per_run
        if _provider_candidates_remaining is None
        else max(0, int(_provider_candidates_remaining))
    )
    provider_request_limit = provider_request_backlog
    if config.gpt_only_publication_enabled:
        provider_request_limit = min(
            provider_request_backlog,
            config.gpt_max_clusters_per_run,
            provider_candidates_remaining,
        )
    selected_provider_keys = {
        str(candidate[0]["cluster_key"])
        for candidate in provider_candidates[:provider_request_limit]
    }
    prepared_candidates = [
        candidate
        for candidate in prepared_candidates
        if (
            candidate[5]
            or str(candidate[0]["cluster_key"]) in selected_provider_keys
        )
    ]
    provider_request_count = len(selected_provider_keys)

    stats["provider_request_candidates"] = provider_request_count
    stats["provider_request_backlog"] = provider_request_backlog
    stats["deferred_provider_candidates"] = (
        provider_request_backlog - provider_request_count
    )

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
        if "preflight" in reason:
            stats["budget_deferred"] += 1

    def record_cached_fallback(reason: str) -> None:
        stats["cached_fallbacks"] += 1
        reasons = stats["cached_fallback_reasons"]
        reasons[reason] = reasons.get(reason, 0) + 1

    try:
        for (
            cluster,
            members,
            article_records,
            identity,
            cached,
            _reusable_without_provider,
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
                and not _cached_candidate_requires_autonomous_audit(
                    cached,
                    config=config,
                )
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
                and not _cached_candidate_requires_autonomous_audit(
                    cached,
                    config=config,
                )
            ):
                # A provider response was already paid for and reached a
                # terminal local outcome. Reuse its safe V2 fallback unless
                # the operator explicitly requests a forced regeneration.
                stats["cache_hits"] += 1
                record_cached_fallback(effective_fallback_reason(cached))
                continue

            base = _base_version_values(
                cluster=cluster,
                identity=identity,
            )
            audit_only_cached = (
                not force
                and _cached_candidate_requires_autonomous_audit(
                    cached,
                    config=config,
                )
            )
            if offline_reason is not None:
                if audit_only_cached:
                    stats["cache_hits"] += 1
                    record_cached_fallback(
                        f"autonomous_audit_{offline_reason}"
                    )
                    continue
                _persist_version(
                    connection,
                    _fallback_values(
                        base,
                        reason=offline_reason,
                    ),
                )
                record_fallback(offline_reason)
                continue

            route = None
            audit_capacity_report = None
            used_budget_safe_audit_route = False
            if config.gpt_autonomous_audit_enabled:
                route = classify_audit_route(
                    cluster=cluster,
                    members=members,
                    config=config,
                )
                stats["audit_risk_tiers"][route.risk_tier] = (
                    stats["audit_risk_tiers"].get(route.risk_tier, 0) + 1
                )
                reserve_audit_before_primary = bool(
                    audit_policy_config.gpt_audit_policy_mode
                    in {"all", "shadow"}
                    or route.risk_tier != "low"
                )
                if not audit_only_cached and reserve_audit_before_primary:
                    capacity_request = _build_audit_capacity_probe(
                        identity=identity,
                        route=route,
                        config=config,
                    )
                    fitted_capacity_request = (
                        gated_generator.fit_request_to_budget(
                            capacity_request,
                            minimum_output_tokens=AUDIT_CAPACITY_MIN_OUTPUT_TOKENS,
                        )
                    )
                    if fitted_capacity_request is None:
                        budget_route = _budget_safe_audit_route(
                            route,
                            config=config,
                        )
                        if budget_route is not None:
                            budget_capacity_request = _build_audit_capacity_probe(
                                identity=identity,
                                route=budget_route,
                                config=config,
                            )
                            fitted_capacity_request = (
                                gated_generator.fit_request_to_budget(
                                    budget_capacity_request,
                                    minimum_output_tokens=(
                                        AUDIT_CAPACITY_MIN_OUTPUT_TOKENS
                                    ),
                                )
                            )
                            if fitted_capacity_request is not None:
                                route = budget_route
                                used_budget_safe_audit_route = True
                    capacity_request = fitted_capacity_request
                    if capacity_request is None:
                        reason = "preflight_atomic_story_budget_unavailable"
                        _persist_version(
                            connection,
                            _fallback_values(
                                base,
                                reason=reason,
                                validation_status="preflight_fallback",
                            ),
                        )
                        stats["atomic_budget_deferred"] += 1
                        record_fallback(reason)
                        continue
                    audit_capacity_report = (
                        gated_generator.reserve_capacity(capacity_request)
                    )
                    if (
                        audit_capacity_report is not None
                        and not audit_capacity_report.should_generate
                    ):
                        reason = (
                            "preflight_atomic_story_"
                            + audit_capacity_report.reason.value
                        )
                        _persist_version(
                            connection,
                            _fallback_values(
                                base,
                                reason=reason,
                                validation_status="preflight_fallback",
                                preflight=audit_capacity_report,
                            ),
                        )
                        stats["atomic_budget_deferred"] += 1
                        record_fallback(reason)
                        continue

            gated_result = None
            if not audit_only_cached:
                try:
                    gated_result = gated_generator.generate(identity.request)
                except Exception as error:
                    gated_generator.release_capacity(audit_capacity_report)
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
                    stats["provider_failed"] += 1
                    record_fallback("provider_error")
                    continue

                if gated_result.used_v2_fallback:
                    gated_generator.release_capacity(audit_capacity_report)
                    reason = (
                        f"preflight_{gated_result.preflight.reason.value}"
                    )
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
                stats["provider_calls"] += 1
                primary_values = _interpret_generation(
                    base=base,
                    identity=identity,
                    preflight=gated_result.preflight,
                    generation=gated_result.generation,
                )
            else:
                primary_values = dict(cached)

            if not config.gpt_autonomous_audit_enabled:
                persisted = _persist_version(connection, primary_values)
                stats["input_tokens"] += int(
                    primary_values.get("input_tokens") or 0
                )
                stats["output_tokens"] += int(
                    primary_values.get("output_tokens") or 0
                )
                stats["total_tokens"] += int(
                    primary_values.get("total_tokens") or 0
                )
                if version_is_deployable_gpt(persisted):
                    stats["accepted"] += 1
                elif version_is_pending_validator_warning(persisted):
                    stats["pending_review"] += 1
                else:
                    record_fallback(effective_fallback_reason(persisted))
                if primary_values.get("estimated_cost_usd") is not None:
                    total_cost += _decimal_or_zero(
                        primary_values["estimated_cost_usd"]
                    )
                continue

            audit_decision = decide_autonomous_audit(
                route=route,
                primary=primary_values,
                request_fingerprint_sha256=(
                    identity.request_fingerprint_sha256
                ),
                config=audit_policy_config,
                force_audit=audit_only_cached,
            )
            if not audit_decision.would_audit_under_risk_policy:
                stats["audit_policy_would_skip"] += 1
            if audit_decision.sampled:
                stats["audit_policy_sampled"] += 1
            if not audit_decision.should_audit:
                persisted = _persist_version(connection, primary_values)
                stats["audits_skipped_low_risk"] += 1
                stats["input_tokens"] += int(
                    primary_values.get("input_tokens") or 0
                )
                stats["output_tokens"] += int(
                    primary_values.get("output_tokens") or 0
                )
                stats["total_tokens"] += int(
                    primary_values.get("total_tokens") or 0
                )
                if version_is_deployable_gpt(persisted):
                    stats["accepted"] += 1
                elif version_is_pending_validator_warning(persisted):
                    stats["pending_review"] += 1
                else:
                    record_fallback(effective_fallback_reason(persisted))
                if primary_values.get("estimated_cost_usd") is not None:
                    total_cost += _decimal_or_zero(
                        primary_values["estimated_cost_usd"]
                    )
                continue

            try:
                candidate_payload = json.loads(
                    str(primary_values.get("output_json") or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                candidate_payload = {}
            try:
                validation_payload = json.loads(
                    str(primary_values.get("validation_json") or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                validation_payload = {}
            audit_request = build_autonomous_audit_request(
                contract_input=identity.contract_input,
                candidate=(
                    candidate_payload
                    if isinstance(candidate_payload, Mapping)
                    else {}
                ),
                validation=(
                    validation_payload
                    if isinstance(validation_payload, Mapping)
                    else {}
                ),
                route=route,
                config=config,
            )
            gated_generator.release_capacity(audit_capacity_report)
            fitted_audit_request = gated_generator.fit_request_to_budget(
                audit_request,
                minimum_output_tokens=AUDIT_CAPACITY_MIN_OUTPUT_TOKENS,
            )
            if fitted_audit_request is None:
                budget_route = _budget_safe_audit_route(
                    route,
                    config=config,
                )
                if budget_route is not None:
                    budget_audit_request = build_autonomous_audit_request(
                        contract_input=identity.contract_input,
                        candidate=(
                            candidate_payload
                            if isinstance(candidate_payload, Mapping)
                            else {}
                        ),
                        validation=(
                            validation_payload
                            if isinstance(validation_payload, Mapping)
                            else {}
                        ),
                        route=budget_route,
                        config=config,
                    )
                    fitted_audit_request = (
                        gated_generator.fit_request_to_budget(
                            budget_audit_request,
                            minimum_output_tokens=(
                                AUDIT_CAPACITY_MIN_OUTPUT_TOKENS
                            ),
                        )
                    )
                    if fitted_audit_request is not None:
                        route = budget_route
                        used_budget_safe_audit_route = True
            if fitted_audit_request is not None:
                audit_request = fitted_audit_request
            stats["audit_routes"][route.complexity] = (
                stats["audit_routes"].get(route.complexity, 0) + 1
            )
            if used_budget_safe_audit_route:
                stats["audit_budget_safe_routes"] += 1
            try:
                audit_result = gated_generator.generate(audit_request)
            except Exception as error:
                diagnostic_error = getattr(error, "last_error", error)
                logger.warning(
                    "GPT autonomous audit provider error for cluster %s: "
                    "type=%s request_id=%s",
                    cluster.get("cluster_key"),
                    type(diagnostic_error).__name__,
                    getattr(diagnostic_error, "request_id", None),
                )
                failed = dict(primary_values)
                failed.update(
                    {
                        "generation_status": GENERATION_STATUS_FALLBACK,
                        "validation_status": "autonomous_audit_failed",
                        "fallback_reason": "autonomous_audit_provider_error",
                        "autonomous_audit_status": "failed",
                        "autonomous_audit_model": route.model,
                        "autonomous_audit_route_json": _canonical_json(
                            {
                                "audit_version": AUTONOMOUS_AUDIT_VERSION,
                                "complexity": route.complexity,
                                "risk_tier": route.risk_tier,
                                "reasons": list(route.reasons),
                                "model": route.model,
                                "reasoning_effort": route.reasoning_effort,
                            }
                        ),
                    }
                )
                _persist_version(
                    connection,
                    failed,
                    allow_status_demotion=audit_only_cached,
                )
                if not audit_only_cached:
                    stats["input_tokens"] += int(
                        primary_values.get("input_tokens") or 0
                    )
                    stats["output_tokens"] += int(
                        primary_values.get("output_tokens") or 0
                    )
                    stats["total_tokens"] += int(
                        primary_values.get("total_tokens") or 0
                    )
                    total_cost += _decimal_or_zero(
                        primary_values.get("estimated_cost_usd")
                    )
                stats["audit_failed"] += 1
                stats["audit_provider_failed"] += 1
                record_fallback("autonomous_audit_provider_error")
                continue

            if audit_result.used_v2_fallback:
                failed = dict(primary_values)
                failed.update(
                    {
                        "generation_status": GENERATION_STATUS_FALLBACK,
                        "validation_status": "autonomous_audit_preflight",
                        "fallback_reason": (
                            "autonomous_audit_preflight_"
                            + audit_result.preflight.reason.value
                        ),
                        "autonomous_audit_status": "failed",
                        "autonomous_audit_model": route.model,
                    }
                )
                _persist_version(
                    connection,
                    failed,
                    allow_status_demotion=audit_only_cached,
                )
                if not audit_only_cached:
                    stats["input_tokens"] += int(
                        primary_values.get("input_tokens") or 0
                    )
                    stats["output_tokens"] += int(
                        primary_values.get("output_tokens") or 0
                    )
                    stats["total_tokens"] += int(
                        primary_values.get("total_tokens") or 0
                    )
                    total_cost += _decimal_or_zero(
                        primary_values.get("estimated_cost_usd")
                    )
                stats["audit_skipped_budget"] += 1
                record_fallback(str(failed["fallback_reason"]))
                continue

            stats["audit_calls"] += 1
            stats["provider_calls"] += 1
            try:
                audit_assessment = _parse_autonomous_audit_response(
                    audit_result.generation.response
                )
            except (ValidationError, ValueError, TypeError):
                audit_assessment = None
            audit_base = dict(base)
            audit_base.update(
                {
                    "model_name": route.model,
                    "reasoning_effort": route.reasoning_effort,
                    "max_output_tokens": audit_request.max_output_tokens,
                }
            )
            if (
                audit_assessment is not None
                and audit_assessment.cluster_coherence
                == "partition_required"
                and correction_requirements_by_story is None
            ):
                source_article_ids = {
                    int(article.article_id)
                    for article in identity.contract_input.articles
                }
                try:
                    validated_groups = validate_semantic_partition(
                        source_article_ids,
                        audit_assessment.article_groups,
                    )
                    partition_result = apply_semantic_partition(
                        connection,
                        cluster_key=str(cluster["cluster_key"]),
                        groups=validated_groups,
                        audit_version=AUTONOMOUS_AUDIT_VERSION,
                    )
                except (TypeError, ValueError, sqlite3.DatabaseError) as error:
                    logger.warning(
                        "Rejected invalid semantic partition for cluster %s: %s",
                        cluster.get("cluster_key"),
                        error,
                    )
                    audit_assessment = None
                else:
                    audited_values = _fallback_values(
                        audit_base,
                        reason="semantic_partition_applied",
                        validation_status="semantic_partition_applied",
                        preflight=audit_result.preflight,
                        response=audit_result.generation.response,
                        attempts=audit_result.generation.attempts,
                        output=audit_assessment.model_dump(mode="json"),
                    )
                    values = _combined_provider_values(
                        primary=primary_values,
                        audited=audited_values,
                        route=route,
                        audit_assessment=audit_assessment,
                        audit_decision=audit_decision,
                    )
                    values["autonomous_audit_status"] = "partitioned"
                    _persist_version(
                        connection,
                        values,
                        allow_status_demotion=True,
                    )
                    usage_values = (
                        audited_values if audit_only_cached else values
                    )
                    stats["input_tokens"] += int(
                        usage_values.get("input_tokens") or 0
                    )
                    stats["output_tokens"] += int(
                        usage_values.get("output_tokens") or 0
                    )
                    stats["total_tokens"] += int(
                        usage_values.get("total_tokens") or 0
                    )
                    if usage_values.get("estimated_cost_usd") is not None:
                        total_cost += _decimal_or_zero(
                            usage_values["estimated_cost_usd"]
                        )
                    stats["semantic_partitions_applied"] += 1
                    stats["semantic_partition_groups"] += (
                        partition_result.group_count
                    )
                    stats["semantic_partition_multi_groups"] += len(
                        partition_result.multi_article_cluster_keys
                    )
                    stats["semantic_partition_singletons"] += (
                        partition_result.singleton_count
                    )
                    stats["semantic_partition_cluster_keys"].append(
                        partition_result.old_cluster_key
                    )
                    stats["semantic_partition_followup_keys"].extend(
                        partition_result.multi_article_cluster_keys
                    )
                    stats["audit_change_levels"]["material"] = (
                        stats["audit_change_levels"].get("material", 0) + 1
                    )
                    tier_changes = stats[
                        "audit_change_levels_by_risk"
                    ].setdefault(route.risk_tier, {})
                    tier_changes["material"] = (
                        tier_changes.get("material", 0) + 1
                    )
                    logger.info(
                        "Semantic audit partitioned cluster %s into %s groups "
                        "(%s multi-article, %s singleton)",
                        partition_result.old_cluster_key,
                        partition_result.group_count,
                        len(partition_result.multi_article_cluster_keys),
                        partition_result.singleton_count,
                    )
                    continue
            audited_values = _interpret_generation(
                base=audit_base,
                identity=identity,
                preflight=audit_result.preflight,
                generation=audit_result.generation,
                structured_output_override=(
                    audit_assessment.corrected_story
                    if audit_assessment is not None
                    else None
                ),
            )
            values = _combined_provider_values(
                primary=primary_values,
                audited=audited_values,
                route=route,
                audit_assessment=audit_assessment,
                audit_decision=audit_decision,
            )
            change_level = (
                audit_assessment.change_level
                if audit_assessment is not None
                else "unknown"
            )
            stats["audit_change_levels"][change_level] = (
                stats["audit_change_levels"].get(change_level, 0) + 1
            )
            tier_changes = stats["audit_change_levels_by_risk"].setdefault(
                route.risk_tier,
                {},
            )
            tier_changes[change_level] = tier_changes.get(change_level, 0) + 1
            if (
                audit_policy_config.gpt_audit_policy_mode == "shadow"
                and not audit_decision.would_audit_under_risk_policy
            ):
                stats["shadow_avoidable_audit_calls"] += 1
                avoidable_cost = _decimal_or_zero(
                    stats["shadow_avoidable_audit_cost_usd"]
                ) + _decimal_or_zero(
                    audited_values.get("estimated_cost_usd")
                )
                stats["shadow_avoidable_audit_cost_usd"] = format(
                    avoidable_cost,
                    "f",
                )
            persisted = _persist_version(
                connection,
                values,
                allow_status_demotion=audit_only_cached,
            )
            usage_values = audited_values if audit_only_cached else values
            stats["input_tokens"] += int(
                usage_values.get("input_tokens") or 0
            )
            stats["output_tokens"] += int(
                usage_values.get("output_tokens") or 0
            )
            stats["total_tokens"] += int(
                usage_values.get("total_tokens") or 0
            )
            if version_is_deployable_gpt(persisted):
                stats["accepted"] += 1
                stats["audit_accepted"] += 1
            elif version_is_pending_validator_warning(persisted):
                stats["pending_review"] += 1
            else:
                stats["audit_failed"] += 1
                stats["audit_rejected"] += 1
                record_fallback(effective_fallback_reason(persisted))
            if usage_values.get("estimated_cost_usd") is not None:
                total_cost += _decimal_or_zero(
                    usage_values["estimated_cost_usd"]
                )
    finally:
        connection.close()

    stats["estimated_cost_usd"] = format(total_cost, "f")
    logger.info("Unification summary:")
    logger.info(
        "  Unified multi-article stories ready to publish: %s",
        stats["accepted"],
    )
    logger.info(
        "  Cached outcomes reused: %s total (%s cached fallbacks)",
        stats["cache_hits"],
        stats["cached_fallbacks"],
    )
    logger.info(
        "  Stories awaiting review: %s",
        stats["pending_review"],
    )
    logger.info(
        "  New safe fallbacks: %s (a source article is used instead)",
        stats["fallbacks"],
    )
    logger.info(
        "  GPT calls made: %s generation | %s audit",
        stats["generation_calls"],
        stats["audit_calls"],
    )
    logger.info(
        "  Candidates left for a later run by the run limit: %s",
        stats["deferred_provider_candidates"],
    )
    followup_keys = tuple(
        sorted(set(stats["semantic_partition_followup_keys"]))
    )
    remaining_provider_candidates = max(
        0,
        provider_candidates_remaining
        - int(stats.get("provider_request_candidates") or 0),
    )
    if followup_keys and remaining_provider_candidates > 0:
        followup_stats = run_gpt_unification(
            no_gpt=no_gpt,
            force=False,
            config=config,
            generator=generator,
            preflight=preflight,
            cluster_keys=followup_keys,
            correction_requirements_by_story=None,
            _provider_candidates_remaining=remaining_provider_candidates,
        )
        stats = _merge_unification_pass_stats(stats, followup_stats)
    return stats
