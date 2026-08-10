from __future__ import annotations

import csv
import json
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

from news_pipeline.artifact_io import sha256_file
from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger
from news_pipeline.unification.production import (
    gpt_publication_state,
    sync_gpt_unification_review_queue,
    version_is_deployable_gpt,
)


logger = get_logger()

DECISION_FIELD = "reviewer_decision"
SCORE_FIELDS = (
    "factual_accuracy_1_to_5",
    "completeness_1_to_5",
    "attribution_1_to_5",
    "conflict_handling_1_to_5",
    "clarity_coherence_1_to_5",
    "repetition_1_to_5",
)
CANDIDATE_REVIEW_FIELDS = (
    DECISION_FIELD,
    *SCORE_FIELDS,
    "unsupported_material_claim",
    "validator_assessment",
    "reviewer_notes",
)
ALLOWED_DECISIONS = frozenset(
    {"accept", "minor_issue", "major_issue"}
)
ALLOWED_BINARY_VALUES = frozenset({"yes", "no"})
ALLOWED_VALIDATOR_ASSESSMENTS = frozenset(
    {"correct_block", "false_positive", "unclear"}
)


class CandidateReviewError(ValueError):
    pass


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise CandidateReviewError(f"CSV file does not exist: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]
    if not headers or not rows:
        raise CandidateReviewError(f"CSV file is empty: {path}")
    return headers, rows


def _validated_scores(row: dict[str, str]) -> dict[str, int]:
    scores = {}
    for field in SCORE_FIELDS:
        raw = str(row.get(field) or "").strip()
        try:
            value = int(raw)
        except ValueError as exc:
            raise CandidateReviewError(
                f"{row.get('story_id')}: {field} must be an integer"
            ) from exc
        if value < 1 or value > 5:
            raise CandidateReviewError(
                f"{row.get('story_id')}: {field} must be from 1 to 5"
            )
        scores[field] = value
    return scores


def _validate_candidate_review(
    reference_headers: list[str],
    reference_rows: list[dict[str, str]],
    reviewed_headers: list[str],
    reviewed_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    if reviewed_headers != reference_headers:
        raise CandidateReviewError(
            "reviewed candidate CSV headers differ from the frozen reference"
        )
    if tuple(reviewed_headers[-len(CANDIDATE_REVIEW_FIELDS) :]) != (
        CANDIDATE_REVIEW_FIELDS
    ):
        raise CandidateReviewError(
            "candidate review columns are missing or out of order"
        )
    if len(reviewed_rows) != len(reference_rows):
        raise CandidateReviewError(
            "reviewed candidate row count differs from the reference"
        )

    immutable_fields = reviewed_headers[: -len(CANDIDATE_REVIEW_FIELDS)]
    validated = []
    seen_story_ids: set[str] = set()
    for reference, reviewed in zip(reference_rows, reviewed_rows):
        story_id = str(reviewed.get("story_id") or "").strip()
        if not story_id or story_id in seen_story_ids:
            raise CandidateReviewError(
                f"missing or duplicate candidate story_id: {story_id!r}"
            )
        seen_story_ids.add(story_id)
        changed = [
            field
            for field in immutable_fields
            if str(reference.get(field) or "")
            != str(reviewed.get(field) or "")
        ]
        if changed:
            raise CandidateReviewError(
                f"{story_id}: immutable candidate fields changed: "
                + ", ".join(changed)
            )
        decision = str(reviewed.get(DECISION_FIELD) or "").strip()
        if decision not in ALLOWED_DECISIONS:
            raise CandidateReviewError(
                f"{story_id}: invalid reviewer_decision {decision!r}"
            )
        scores = _validated_scores(reviewed)
        unsupported = str(
            reviewed.get("unsupported_material_claim") or ""
        ).strip().lower()
        if unsupported not in ALLOWED_BINARY_VALUES:
            raise CandidateReviewError(
                f"{story_id}: unsupported_material_claim must be yes or no"
            )
        validator_assessment = str(
            reviewed.get("validator_assessment") or ""
        ).strip()
        if validator_assessment not in ALLOWED_VALIDATOR_ASSESSMENTS:
            raise CandidateReviewError(
                f"{story_id}: invalid validator_assessment "
                f"{validator_assessment!r}"
            )
        notes = str(reviewed.get("reviewer_notes") or "").strip()
        notes_required = (
            decision != "accept"
            or any(score < 5 for score in scores.values())
        )
        if notes_required and not notes:
            raise CandidateReviewError(
                f"{story_id}: reviewer_notes are required for non-accept "
                "decisions and scores below 5"
            )
        validated.append(
            {
                "story_id": story_id,
                "decision": decision,
                "scores": scores,
                "unsupported_material_claim": unsupported,
                "validator_assessment": validator_assessment,
                "notes": notes,
                "row": reviewed,
            }
        )
    return validated


def _matched_candidate_version(
    connection: Any,
    item: dict[str, Any],
) -> dict[str, Any]:
    """Match an accepted, warning, or validator-blocked raw GPT candidate."""
    row = item["row"]
    matches = [
        dict(version)
        for version in connection.execute(
            """
            SELECT *
            FROM unified_story_versions
            WHERE cluster_key = ? AND response_id = ?
            """,
            (
                item["story_id"],
                str(row.get("response_id") or "").strip(),
            ),
        )
    ]
    if len(matches) != 1:
        raise CandidateReviewError(
            f"{item['story_id']}: expected one persisted GPT candidate, "
            f"found {len(matches)}"
        )
    version = matches[0]
    checks = {
        "model": version.get("model_name"),
        "prompt_version": version.get("prompt_version"),
        "validation_status": version.get("validation_status"),
        "input_tokens": version.get("input_tokens"),
        "output_tokens": version.get("output_tokens"),
        "estimated_cost_usd": version.get("estimated_cost_usd"),
    }
    mismatches = [
        field
        for field, expected in checks.items()
        if field in row
        and str(row.get(field) or "") != str(expected or "")
    ]
    raw_output = version.get("output_json")
    if not version.get("response_id") or not raw_output:
        mismatches.append("persisted_raw_candidate")
    else:
        try:
            output = json.loads(str(raw_output))
        except json.JSONDecodeError:
            mismatches.append("persisted_raw_candidate")
        else:
            output_checks = {
                "display_title": output.get("display_title"),
                "unified_story": output.get("unified_story"),
                "claims": json.dumps(
                    output.get("claims") or [],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "conflicts_or_uncertainties": json.dumps(
                    output.get("conflicts_or_uncertainties") or [],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            }
            mismatches.extend(
                field
                for field, expected in output_checks.items()
                if field in row
                and str(row.get(field) or "") != str(expected or "")
            )
    if version.get("human_review_source_sha256"):
        mismatches.append("existing_human_review")
    if mismatches:
        raise CandidateReviewError(
            f"{item['story_id']}: candidate review differs from the "
            "persisted provider result: "
            + ", ".join(sorted(set(mismatches)))
        )
    return version


def _application_markdown(report: dict[str, Any]) -> str:
    review = report["review"]
    application = report["application"]
    quality = report["quality"]
    return "\n".join(
        [
            "# GPT candidate-review application audit",
            "",
            f"- Reviewed candidates: {review['row_count']}",
            (
                "- Decisions: "
                + ", ".join(
                    f"{key}={value}"
                    for key, value in review["decisions"].items()
                )
            ),
            (
                "- Validator assessments: "
                + ", ".join(
                    f"{key}={value}"
                    for key, value in review[
                        "validator_assessments"
                    ].items()
                )
            ),
            (
                "- Publishable after audited review: "
                f"{application['publishable']}"
            ),
            (
                "- Pending review after application: "
                f"{application['pending_review']}"
            ),
            (
                "- Rejected/unavailable after application: "
                f"{application['rejected_or_unavailable']}"
            ),
            (
                "- Accept-or-minor rate: "
                f"{quality['accept_or_minor_issue_rate']:.2%}"
            ),
            f"- Major-issue rate: {quality['major_issue_rate']:.2%}",
            (
                "- Unsupported material claims: "
                f"{quality['unsupported_material_claims']}"
            ),
            (
                "- Frozen quality thresholds passed: "
                f"{quality['thresholds_passed']}"
            ),
            "- Network calls: 0",
            "- Token-count calls: 0",
            "- Generation calls: 0",
            "",
        ]
    )


def apply_gpt_candidate_review(
    reviewed_csv_path: Union[str, Path],
    reference_csv_path: Union[str, Path],
    *,
    output_dir: Optional[Union[str, Path]] = None,
    config: Optional[PipelineConfig] = None,
) -> dict[str, Any]:
    """Audit and apply a mixed raw-GPT candidate review offline."""
    config = config or load_config()
    reviewed_path = Path(reviewed_csv_path)
    reference_path = Path(reference_csv_path)
    reference_headers, reference_rows = _read_csv(reference_path)
    reviewed_headers, reviewed_rows = _read_csv(reviewed_path)
    validated = _validate_candidate_review(
        reference_headers,
        reference_rows,
        reviewed_headers,
        reviewed_rows,
    )
    review_targets = {
        str(item["row"].get("review_target") or "").strip()
        for item in validated
    }
    if len(review_targets) != 1 or not next(iter(review_targets)):
        raise CandidateReviewError(
            "candidate rows must share one non-empty review_target"
        )

    reviewed_sha256 = sha256_file(reviewed_path)
    reference_sha256 = sha256_file(reference_path)
    imported_at = datetime.now().isoformat(timespec="seconds")
    decisions = Counter(item["decision"] for item in validated)
    assessments = Counter(
        item["validator_assessment"] for item in validated
    )
    usable = decisions["accept"] + decisions["minor_issue"]
    total = len(validated)
    unsupported_count = sum(
        item["unsupported_material_claim"] == "yes"
        for item in validated
    )

    connection = get_connection(config)
    try:
        versions = {
            item["story_id"]: _matched_candidate_version(
                connection,
                item,
            )
            for item in validated
        }
        before_states = Counter(
            gpt_publication_state(version)["publication_status"]
            for version in versions.values()
        )
        artifact_dir = (
            Path(output_dir)
            if output_dir is not None
            else reviewed_path.parent / "application"
        )
        artifact_dir.mkdir(parents=True, exist_ok=True)
        archived_csv = artifact_dir / reviewed_path.name
        if (
            archived_csv.exists()
            and sha256_file(archived_csv) != reviewed_sha256
        ):
            raise CandidateReviewError(
                "review archive already exists with different content: "
                f"{archived_csv}"
            )
        shutil.copyfile(reviewed_path, archived_csv)

        connection.execute("BEGIN")
        after_versions = {}
        for item in validated:
            version = versions[item["story_id"]]
            payload = {
                "review_version": "gpt_mixed_candidate_review_v1",
                "review_target": next(iter(review_targets)),
                "scores": item["scores"],
                "unsupported_material_claim": item[
                    "unsupported_material_claim"
                ],
                "validator_assessment": item["validator_assessment"],
            }
            connection.execute(
                """
                UPDATE unified_story_versions
                SET human_review_decision = ?,
                    human_review_scores_json = ?,
                    human_review_notes = ?,
                    human_review_source_sha256 = ?,
                    human_review_imported_at = ?
                WHERE id = ?
                """,
                (
                    item["decision"],
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    item["notes"],
                    reviewed_sha256,
                    imported_at,
                    version["id"],
                ),
            )
            updated = dict(
                connection.execute(
                    "SELECT * FROM unified_story_versions WHERE id = ?",
                    (version["id"],),
                ).fetchone()
            )
            sync_gpt_unification_review_queue(connection, updated)
            after_versions[item["story_id"]] = updated

        after_states = Counter(
            gpt_publication_state(version)["publication_status"]
            for version in after_versions.values()
        )
        override_ids = sorted(
            story_id
            for story_id, version in after_versions.items()
            if version_is_deployable_gpt(version)
            and not version_is_deployable_gpt(versions[story_id])
        )
        report = {
            "review_version": "gpt_mixed_candidate_review_v1",
            "application_version": (
                "gpt_mixed_candidate_review_application_v1"
            ),
            "imported_at": imported_at,
            "integrity": {
                "headers_match": True,
                "row_count_match": True,
                "immutable_field_differences": 0,
                "invalid_or_incomplete_rows": 0,
                "provider_version_mismatches": 0,
            },
            "source": {
                "reviewed_csv": str(reviewed_path.resolve()),
                "reviewed_csv_sha256": reviewed_sha256,
                "reference_csv": str(reference_path.resolve()),
                "reference_csv_sha256": reference_sha256,
                "archived_reviewed_csv": str(archived_csv.resolve()),
            },
            "review": {
                "row_count": total,
                "review_target": next(iter(review_targets)),
                "decisions": dict(sorted(decisions.items())),
                "validator_assessments": dict(sorted(assessments.items())),
            },
            "quality": {
                "accept_or_minor_issue_rate": round(usable / total, 6),
                "major_issue_rate": round(
                    decisions["major_issue"] / total,
                    6,
                ),
                "unsupported_material_claims": unsupported_count,
                "thresholds": {
                    "accept_or_minor_issue_minimum": 0.95,
                    "major_issue_maximum": 0.02,
                    "unsupported_material_claims_maximum": 0,
                },
                "thresholds_passed": (
                    usable / total >= 0.95
                    and decisions["major_issue"] / total <= 0.02
                    and unsupported_count == 0
                ),
            },
            "application": {
                "publication_before_review": dict(
                    sorted(before_states.items())
                ),
                "publication_after_review": dict(
                    sorted(after_states.items())
                ),
                "publishable": after_states["publishable"],
                "pending_review": after_states["pending_review"],
                "rejected_or_unavailable": (
                    after_states["rejected"]
                    + after_states["unavailable"]
                ),
                "human_override_story_ids": override_ids,
            },
            "network_calls": 0,
            "token_count_calls": 0,
            "generation_calls": 0,
        }
        json_path = artifact_dir / "candidate_review_application_audit.json"
        markdown_path = artifact_dir / "candidate_review_application_audit.md"
        json_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        markdown_path.write_text(
            _application_markdown(report),
            encoding="utf-8",
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    logger.info(
        "Mixed GPT candidate review applied: %s publishable, %s pending, "
        "0 network/token-count/generation calls",
        report["application"]["publishable"],
        report["application"]["pending_review"],
    )
    return {
        **report,
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }
