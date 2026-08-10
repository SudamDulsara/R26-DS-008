from __future__ import annotations

import csv
import hashlib
import json
import shutil
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Optional, Union

from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger
from news_pipeline.unification.production import (
    GENERATION_STATUS_ACCEPTED,
    VALIDATION_STATUS_ACCEPTED,
    sync_gpt_unification_review_queue,
)


logger = get_logger()

REVIEW_AUDIT_VERSION = "phase2_unification_human_review_v1"
REVIEW_FIELDS = (
    "reviewer_decision",
    "factual_accuracy_1_to_5",
    "completeness_1_to_5",
    "clarity_1_to_5",
    "evidence_traceability_1_to_5",
    "reviewer_notes",
)
SCORE_FIELDS = REVIEW_FIELDS[1:5]
ALLOWED_DECISIONS = frozenset({"accept", "needs_changes", "reject"})


class UnificationReviewError(ValueError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise UnificationReviewError(f"review CSV does not exist: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]
    if not headers or not rows:
        raise UnificationReviewError(f"review CSV is empty: {path}")
    return headers, rows


def _validated_scores(row: dict[str, str]) -> dict[str, int]:
    scores: dict[str, int] = {}
    for field in SCORE_FIELDS:
        raw = str(row.get(field) or "").strip()
        try:
            score = int(raw)
        except ValueError as exc:
            raise UnificationReviewError(
                f"{row.get('story_id')}: {field} must be an integer"
            ) from exc
        if score < 1 or score > 5:
            raise UnificationReviewError(
                f"{row.get('story_id')}: {field} must be from 1 to 5"
            )
        scores[field] = score
    return scores


def _validate_review_pair(
    reference_headers: list[str],
    reference_rows: list[dict[str, str]],
    reviewed_headers: list[str],
    reviewed_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    if reviewed_headers != reference_headers:
        raise UnificationReviewError(
            "reviewed CSV headers differ from the reference export"
        )
    if tuple(reviewed_headers[-len(REVIEW_FIELDS) :]) != REVIEW_FIELDS:
        raise UnificationReviewError(
            "review columns are missing or are not in the expected order"
        )
    if len(reviewed_rows) != len(reference_rows):
        raise UnificationReviewError(
            "reviewed CSV row count differs from the reference export"
        )

    immutable_fields = reviewed_headers[: -len(REVIEW_FIELDS)]
    validated: list[dict[str, Any]] = []
    seen_story_ids: set[str] = set()
    for row_number, (reference, reviewed) in enumerate(
        zip(reference_rows, reviewed_rows),
        start=2,
    ):
        story_id = str(reviewed.get("story_id") or "").strip()
        if not story_id:
            raise UnificationReviewError(
                f"row {row_number}: story_id is required"
            )
        if story_id in seen_story_ids:
            raise UnificationReviewError(
                f"row {row_number}: duplicate story_id {story_id}"
            )
        seen_story_ids.add(story_id)

        changed_fields = [
            field
            for field in immutable_fields
            if str(reference.get(field) or "")
            != str(reviewed.get(field) or "")
        ]
        if changed_fields:
            raise UnificationReviewError(
                f"{story_id}: immutable fields changed: "
                + ", ".join(changed_fields)
            )

        decision = str(reviewed.get("reviewer_decision") or "").strip()
        if decision not in ALLOWED_DECISIONS:
            raise UnificationReviewError(
                f"{story_id}: invalid reviewer_decision {decision!r}"
            )
        scores = _validated_scores(reviewed)
        notes = str(reviewed.get("reviewer_notes") or "").strip()
        if (
            decision != "accept" or any(score < 5 for score in scores.values())
        ) and not notes:
            raise UnificationReviewError(
                f"{story_id}: reviewer_notes are required for this decision"
            )
        validated.append(
            {
                "story_id": story_id,
                "decision": decision,
                "scores": scores,
                "notes": notes,
                "row": reviewed,
            }
        )
    return validated


def _find_persisted_version(
    versions_by_story: dict[str, list[dict[str, Any]]],
    reviewed: dict[str, Any],
) -> dict[str, Any]:
    row = reviewed["row"]
    candidates = versions_by_story.get(reviewed["story_id"], [])
    matches = [
        version
        for version in candidates
        if str(version.get("response_id") or "")
        == str(row.get("response_id") or "")
        and str(version.get("model_name") or "")
        == str(row.get("model") or "")
        and str(version.get("prompt_version") or "")
        == str(row.get("prompt_version") or "")
    ]
    if len(matches) != 1:
        raise UnificationReviewError(
            f"{reviewed['story_id']}: expected one persisted provider result, "
            f"found {len(matches)}"
        )
    version = matches[0]
    expected_status = (
        "generated"
        if version["generation_status"] == GENERATION_STATUS_ACCEPTED
        and version["validation_status"] == VALIDATION_STATUS_ACCEPTED
        else "fallback"
    )
    expected_method = "gpt" if expected_status == "generated" else "extractive_v2"
    checks = {
        "deployed_status": expected_status,
        "deployed_output_method": expected_method,
        "validation_status": str(version.get("validation_status") or ""),
        "input_tokens": str(version.get("input_tokens") or ""),
        "output_tokens": str(version.get("output_tokens") or ""),
        "estimated_cost_usd": str(
            version.get("estimated_cost_usd") or ""
        ),
    }
    mismatches = [
        field
        for field, expected in checks.items()
        if str(row.get(field) or "") != expected
    ]
    if mismatches:
        raise UnificationReviewError(
            f"{reviewed['story_id']}: review export differs from persisted "
            "result: " + ", ".join(mismatches)
        )
    return version


def _group_decisions(
    reviewed: list[dict[str, Any]],
    field: str,
) -> dict[str, dict[str, int]]:
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for item in reviewed:
        grouped[str(item["row"].get(field) or "unknown")][
            item["decision"]
        ] += 1
    return {
        group: dict(sorted(counts.items()))
        for group, counts in sorted(grouped.items())
    }


def _build_report(
    *,
    reviewed: list[dict[str, Any]],
    versions: dict[str, dict[str, Any]],
    reviewed_path: Path,
    reference_path: Path,
    reviewed_sha256: str,
    reference_sha256: str,
    imported_at: str,
) -> dict[str, Any]:
    decision_counts = Counter(item["decision"] for item in reviewed)
    means = {
        field: round(mean(item["scores"][field] for item in reviewed), 6)
        for field in SCORE_FIELDS
    }
    technically_accepted = [
        item
        for item in reviewed
        if versions[item["story_id"]]["generation_status"]
        == GENERATION_STATUS_ACCEPTED
        and versions[item["story_id"]]["validation_status"]
        == VALIDATION_STATUS_ACCEPTED
    ]
    deployed_gpt = [
        item
        for item in technically_accepted
        if item["decision"] == "accept"
    ]
    human_blocked_gpt = [
        item["story_id"]
        for item in technically_accepted
        if item["decision"] != "accept"
    ]
    conservative_fallbacks = [
        item["story_id"]
        for item in reviewed
        if item["decision"] == "accept"
        and item not in technically_accepted
    ]
    total = len(reviewed)
    return {
        "audit_version": REVIEW_AUDIT_VERSION,
        "imported_at": imported_at,
        "source": {
            "reviewed_csv": str(reviewed_path.resolve()),
            "reviewed_csv_sha256": reviewed_sha256,
            "reference_csv": str(reference_path.resolve()),
            "reference_csv_sha256": reference_sha256,
        },
        "integrity": {
            "headers_match": True,
            "row_count_match": True,
            "immutable_field_differences": 0,
            "duplicate_story_ids": 0,
            "invalid_decisions": 0,
            "invalid_scores": 0,
            "required_notes_missing": 0,
        },
        "review": {
            "row_count": total,
            "decisions": dict(sorted(decision_counts.items())),
            "candidate_acceptance_rate": round(
                decision_counts["accept"] / total,
                6,
            ),
            "criterion_means": means,
            "decisions_by_deployed_output_method": _group_decisions(
                reviewed,
                "deployed_output_method",
            ),
            "decisions_by_validation_status": _group_decisions(
                reviewed,
                "validation_status",
            ),
        },
        "production_selection": {
            "technical_validation_accepted": len(technically_accepted),
            "generated_after_human_review": len(deployed_gpt),
            "fallback_after_human_review": total - len(deployed_gpt),
            "human_blocked_technical_accepts": human_blocked_gpt,
            "human_accepted_but_technically_blocked": conservative_fallbacks,
        },
        "gate": {
            "human_review_complete": True,
            "review_audit_passed": True,
            "all_deployed_gpt_human_accepted": True,
            "all_other_outputs_use_v2_fallback": True,
            "phase2_production_gate_passed": True,
            "candidate_quality_all_passed": (
                decision_counts["accept"] == total
            ),
        },
    }


def _markdown_report(report: dict[str, Any]) -> str:
    review = report["review"]
    selection = report["production_selection"]
    decisions = review["decisions"]
    human_blocked = selection["human_blocked_technical_accepts"]
    conservative = selection["human_accepted_but_technically_blocked"]
    lines = [
        "# Phase 2 unified-story human-review audit",
        "",
        f"- Reviewed rows: {review['row_count']}",
        f"- Accepted candidates: {decisions.get('accept', 0)}",
        f"- Candidates needing changes: {decisions.get('needs_changes', 0)}",
        f"- Rejected candidates: {decisions.get('reject', 0)}",
        (
            "- Candidate acceptance rate: "
            f"{review['candidate_acceptance_rate']:.2%}"
        ),
        (
            "- Final production selection: "
            f"{selection['generated_after_human_review']} GPT, "
            f"{selection['fallback_after_human_review']} V2 fallback"
        ),
        "- Immutable-field differences: 0",
        "- Invalid or incomplete review rows: 0",
        "- Additional API calls: 0",
        "",
        "## Mean scores",
        "",
    ]
    for field, value in review["criterion_means"].items():
        lines.append(f"- {field}: {value:.3f}")
    lines.extend(
        [
            "",
            "## Production decision",
            "",
            (
                "Phase 2's production safety gate passes after applying the "
                "human decisions. Only candidates accepted by both strict "
                "validation and human review are deployed as GPT output; all "
                "others use the frozen V2 fallback."
            ),
            "",
            "Human-blocked technical accepts:",
            "",
        ]
    )
    lines.extend(f"- `{story_id}`" for story_id in human_blocked)
    lines.extend(
        [
            "",
            "Human accepts retained as conservative technical fallbacks:",
            "",
        ]
    )
    lines.extend(f"- `{story_id}`" for story_id in conservative)
    lines.append("")
    return "\n".join(lines)


def apply_unification_review(
    reviewed_csv_path: Union[str, Path],
    reference_csv_path: Union[str, Path],
    *,
    config: Optional[PipelineConfig] = None,
    output_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Audit and apply a completed review without provider or network calls."""
    config = config or load_config()
    reviewed_path = Path(reviewed_csv_path)
    reference_path = Path(reference_csv_path)
    reference_headers, reference_rows = _read_csv(reference_path)
    reviewed_headers, reviewed_rows = _read_csv(reviewed_path)
    reviewed = _validate_review_pair(
        reference_headers,
        reference_rows,
        reviewed_headers,
        reviewed_rows,
    )
    reviewed_sha256 = _sha256(reviewed_path)
    reference_sha256 = _sha256(reference_path)
    imported_at = datetime.now().isoformat(timespec="seconds")

    connection = get_connection(config)
    try:
        cluster_keys = {
            str(row["cluster_key"])
            for row in connection.execute(
                "SELECT cluster_key FROM story_clusters"
            )
        }
        reviewed_keys = {item["story_id"] for item in reviewed}
        if cluster_keys != reviewed_keys:
            raise UnificationReviewError(
                "reviewed story IDs do not match the current cluster set"
            )

        versions_by_story: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in connection.execute(
            "SELECT * FROM unified_story_versions"
        ):
            version = dict(row)
            versions_by_story[str(version["cluster_key"])].append(version)
        matched_versions = {
            item["story_id"]: _find_persisted_version(
                versions_by_story,
                item,
            )
            for item in reviewed
        }

        report = _build_report(
            reviewed=reviewed,
            versions=matched_versions,
            reviewed_path=reviewed_path,
            reference_path=reference_path,
            reviewed_sha256=reviewed_sha256,
            reference_sha256=reference_sha256,
            imported_at=imported_at,
        )

        artifact_dir = (
            Path(output_dir)
            if output_dir is not None
            else config.reviews_dir / "phase2_unification_47"
        )
        artifact_dir.mkdir(parents=True, exist_ok=True)
        archived_csv = artifact_dir / "phase2_47_story_review_REVIEWED.csv"
        if archived_csv.exists() and _sha256(archived_csv) != reviewed_sha256:
            raise UnificationReviewError(
                f"review archive already exists with different content: "
                f"{archived_csv}"
            )

        connection.execute("BEGIN")
        for item in reviewed:
            version = matched_versions[item["story_id"]]
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
                        item["scores"],
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
        for version in matched_versions.values():
            refreshed = connection.execute(
                """
                SELECT *
                FROM unified_story_versions
                WHERE id = ?
                """,
                (version["id"],),
            ).fetchone()
            if refreshed is not None:
                sync_gpt_unification_review_queue(
                    connection,
                    dict(refreshed),
                )
        shutil.copyfile(reviewed_path, archived_csv)
        report["artifacts"] = {
            "reviewed_csv": str(archived_csv.resolve()),
            "audit_json": str(
                (artifact_dir / "phase2_47_story_review_audit.json").resolve()
            ),
            "audit_markdown": str(
                (artifact_dir / "phase2_47_story_review_audit.md").resolve()
            ),
        }
        audit_json = artifact_dir / "phase2_47_story_review_audit.json"
        audit_markdown = artifact_dir / "phase2_47_story_review_audit.md"
        audit_json.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        audit_markdown.write_text(
            _markdown_report(report),
            encoding="utf-8",
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    logger.info(
        "Human review applied: %s rows, %s GPT selected, %s V2 fallbacks, "
        "0 generation calls",
        report["review"]["row_count"],
        report["production_selection"]["generated_after_human_review"],
        report["production_selection"]["fallback_after_human_review"],
    )
    return report
