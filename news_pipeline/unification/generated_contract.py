from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from pydantic import ValidationError

from news_pipeline.unification.contract import (
    UNIFIED_STORY_CONTRACT_VERSION,
    UNIFIED_STORY_SCHEMA_VERSION,
    unified_story_contract_metadata,
    validate_unified_story_record,
)
from news_pipeline.unification.gpt_contract import (
    GPTResolvedUnifiedStoryV2,
)
from news_pipeline.unification.production import (
    GENERATION_STATUS_ACCEPTED,
    VALIDATION_STATUS_ACCEPTED,
    effective_fallback_reason,
    human_review_overrides_fact_shape,
    human_review_overrides_validator_warning,
    version_is_deployable_gpt,
)


GENERATED_STORY_CONTRACT_VERSION = (
    "unified_story_v3_generated_2026-07-24"
)
GENERATED_STORY_SCHEMA_VERSION = "generated_v3"


class GeneratedStoryContractError(ValueError):
    pass


def generated_story_contract_metadata() -> dict[str, Any]:
    return {
        "contract_version": GENERATED_STORY_CONTRACT_VERSION,
        "schema_version": GENERATED_STORY_SCHEMA_VERSION,
        "generated_output_schema": "unified_story_resolved_v2",
        "fallback_contract": unified_story_contract_metadata(),
    }


def _json_value(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _generation_metadata(
    version: Optional[Mapping[str, Any]],
    *,
    fallback_reason: str,
) -> dict[str, Any]:
    if version is None:
        return {
            "version_id": None,
            "source_fingerprint_sha256": None,
            "input_fingerprint_sha256": None,
            "request_fingerprint_sha256": None,
            "model_name": None,
            "model_snapshot": None,
            "prompt_version": None,
            "input_schema_version": None,
            "output_schema_version": None,
            "resolved_schema_version": None,
            "generation_status": "not_generated",
            "validation_status": "not_run",
            "validation": {
                "provenance": None,
                "fact_shape": None,
                "claim_projection": None,
                "semantic_support": None,
            },
            "preflight": None,
            "usage": {
                "input_tokens": None,
                "output_tokens": None,
                "total_tokens": None,
                "estimated_cost_usd": None,
            },
            "response_id": None,
            "attempts": 0,
            "fallback_reason": fallback_reason,
            "human_review": {
                "decision": None,
                "scores": None,
                "notes": None,
                "source_sha256": None,
                "imported_at": None,
            },
            "created_at": None,
            "updated_at": None,
        }

    return {
        "version_id": version.get("id"),
        "source_fingerprint_sha256": version.get(
            "source_fingerprint_sha256"
        ),
        "input_fingerprint_sha256": version.get(
            "input_fingerprint_sha256"
        ),
        "request_fingerprint_sha256": version.get(
            "request_fingerprint_sha256"
        ),
        "model_name": version.get("model_name"),
        "model_snapshot": version.get("model_snapshot"),
        "prompt_version": version.get("prompt_version"),
        "input_schema_version": version.get("input_schema_version"),
        "output_schema_version": version.get("output_schema_version"),
        "resolved_schema_version": version.get(
            "resolved_schema_version"
        ),
        "generation_status": version.get("generation_status"),
        "validation_status": version.get("validation_status"),
        "validation": _json_value(
            version.get("validation_json"),
            {
                "provenance": None,
                "fact_shape": None,
                "claim_projection": None,
                "semantic_support": None,
            },
        ),
        "preflight": _json_value(
            version.get("preflight_json"),
            None,
        ),
        "usage": {
            "input_tokens": version.get("input_tokens"),
            "output_tokens": version.get("output_tokens"),
            "total_tokens": version.get("total_tokens"),
            "estimated_cost_usd": version.get(
                "estimated_cost_usd"
            ),
        },
        "response_id": version.get("response_id"),
        "attempts": version.get("attempts", 0),
        "fallback_reason": (
            effective_fallback_reason(version, fallback_reason)
        ),
        "human_review": {
            "decision": version.get("human_review_decision"),
            "scores": _json_value(
                version.get("human_review_scores_json"),
                None,
            ),
            "notes": version.get("human_review_notes"),
            "source_sha256": version.get(
                "human_review_source_sha256"
            ),
            "imported_at": version.get(
                "human_review_imported_at"
            ),
        },
        "created_at": version.get("created_at"),
        "updated_at": version.get("updated_at"),
    }


def _generation_review_version(
    generation: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    review = generation.get("human_review")
    if not isinstance(review, Mapping):
        return None
    return {
        "generation_status": generation.get("generation_status"),
        "validation_status": generation.get("validation_status"),
        "response_id": generation.get("response_id"),
        # The generated record reached this validator only after the
        # persisted resolved payload was parsed successfully.
        "resolved_output_json": "validated_in_generated_record",
        "human_review_decision": review.get("decision"),
        "human_review_scores_json": json.dumps(
            review.get("scores"),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }


def _generation_has_human_validation_override(
    generation: Mapping[str, Any],
) -> bool:
    version = _generation_review_version(generation)
    if version is None:
        return False
    return (
        human_review_overrides_fact_shape(version)
        or human_review_overrides_validator_warning(version)
    )


def _generation_has_human_warning_override(
    generation: Mapping[str, Any],
) -> bool:
    version = _generation_review_version(generation)
    return (
        version is not None
        and human_review_overrides_validator_warning(version)
    )


def build_generated_story_record(
    extractive_story: Mapping[str, Any],
    version: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    validate_unified_story_record(dict(extractive_story))
    resolved = None
    persisted_result_invalid = False
    if version_is_deployable_gpt(version):
        resolved_payload = _json_value(
            version.get("resolved_output_json"),
            None,
        )
        try:
            resolved = GPTResolvedUnifiedStoryV2.model_validate(
                resolved_payload
            )
        except (ValidationError, TypeError):
            persisted_result_invalid = True

    if resolved is not None:
        status = "generated"
        display_title = resolved.display_title
        unified_text = resolved.unified_story
        claims = [
            claim.model_dump(mode="json")
            for claim in resolved.claims
        ]
        conflicts = [
            conflict.model_dump(mode="json")
            for conflict in resolved.conflicts_or_uncertainties
        ]
        referenced_article_ids = resolved.referenced_article_ids
        unreferenced_article_ids = resolved.unreferenced_article_ids
        output_method = "gpt"
        fallback_reason = ""
    else:
        unified_text = (
            extractive_story.get("unified_text")
            or extractive_story.get("clean_text")
            or ""
        )
        display_title = (
            extractive_story.get("display_title")
            or extractive_story.get("title")
        )
        status = "fallback" if unified_text else "unavailable"
        claims = []
        conflicts = []
        referenced_article_ids = []
        unreferenced_article_ids = [
            source["article_id"]
            for source in extractive_story.get("source_articles", [])
        ]
        output_method = "extractive_v2"
        fallback_reason = (
            "persisted_result_invalid"
            if persisted_result_invalid
            else effective_fallback_reason(version, "not_generated")
        )

    record = {
        "contract_version": GENERATED_STORY_CONTRACT_VERSION,
        "schema_version": GENERATED_STORY_SCHEMA_VERSION,
        "story_id": extractive_story["story_id"],
        "cluster_id": extractive_story["cluster_id"],
        "status": status,
        "output_method": output_method,
        "display_title": display_title,
        "unified_text": unified_text,
        "claims": claims,
        "conflicts_or_uncertainties": conflicts,
        "referenced_article_ids": referenced_article_ids,
        "unreferenced_article_ids": unreferenced_article_ids,
        "source_articles": extractive_story.get(
            "source_articles",
            [],
        ),
        "generation": _generation_metadata(
            version,
            fallback_reason=fallback_reason,
        ),
        "extractive_fallback": dict(extractive_story),
    }
    validate_generated_story_record(record)
    return record


def validate_generated_story_record(record: Mapping[str, Any]) -> None:
    if (
        record.get("contract_version")
        != GENERATED_STORY_CONTRACT_VERSION
    ):
        raise GeneratedStoryContractError(
            "unexpected generated story contract version"
        )
    if record.get("schema_version") != GENERATED_STORY_SCHEMA_VERSION:
        raise GeneratedStoryContractError(
            "unexpected generated story schema version"
        )
    if record.get("status") not in {
        "generated",
        "fallback",
        "unavailable",
    }:
        raise GeneratedStoryContractError(
            "unexpected generated story status"
        )
    fallback = record.get("extractive_fallback")
    if not isinstance(fallback, dict):
        raise GeneratedStoryContractError(
            "extractive fallback record is required"
        )
    validate_unified_story_record(fallback)
    if record.get("story_id") != fallback.get("story_id"):
        raise GeneratedStoryContractError(
            "generated and fallback story IDs differ"
        )
    if record.get("cluster_id") != fallback.get("cluster_id"):
        raise GeneratedStoryContractError(
            "generated and fallback cluster IDs differ"
        )
    generation = record.get("generation")
    if not isinstance(generation, dict):
        raise GeneratedStoryContractError(
            "generation metadata is required"
        )

    if record["status"] == "generated":
        if record.get("output_method") != "gpt":
            raise GeneratedStoryContractError(
                "generated story must use GPT output"
            )
        if not record.get("display_title") or not record.get(
            "unified_text"
        ):
            raise GeneratedStoryContractError(
                "generated story output is incomplete"
            )
        if not record.get("claims"):
            raise GeneratedStoryContractError(
                "generated story claims are required"
            )
        if (
            record.get("unreferenced_article_ids")
            and not _generation_has_human_warning_override(generation)
        ):
            raise GeneratedStoryContractError(
                "generated story left cluster articles unreferenced"
            )
        has_technical_acceptance = (
            generation.get("generation_status")
            == GENERATION_STATUS_ACCEPTED
            and generation.get("validation_status")
            == VALIDATION_STATUS_ACCEPTED
        )
        if (
            not has_technical_acceptance
            and not _generation_has_human_validation_override(generation)
        ):
            raise GeneratedStoryContractError(
                "generated story does not have accepted validation or an "
                "audited human validation override"
            )
    else:
        if record.get("output_method") != "extractive_v2":
            raise GeneratedStoryContractError(
                "fallback story must use extractive V2 output"
            )
        expected_text = (
            fallback.get("unified_text")
            or fallback.get("clean_text")
            or ""
        )
        if record.get("unified_text") != expected_text:
            raise GeneratedStoryContractError(
                "fallback story text changed"
            )
        if not generation.get("fallback_reason"):
            raise GeneratedStoryContractError(
                "fallback reason is required"
            )

    fallback_unification = fallback.get("unification", {})
    if (
        fallback_unification.get("version")
        != UNIFIED_STORY_SCHEMA_VERSION
    ):
        raise GeneratedStoryContractError(
            "fallback schema version changed"
        )
    if (
        unified_story_contract_metadata()["contract_version"]
        != UNIFIED_STORY_CONTRACT_VERSION
    ):
        raise GeneratedStoryContractError(
            "fallback contract metadata changed"
        )
