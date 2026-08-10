from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional

from pydantic import ValidationError

from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.unification.gpt_contract import (
    GPTUnifiedStoryResponseV2,
)
from news_pipeline.unification.production import (
    GPT_PUBLICATION_STATUS_PENDING_REVIEW,
    GPT_PUBLICATION_STATUS_PUBLISHABLE,
    GPT_PUBLICATION_STATUS_REJECTED,
    _load_generation_candidates,
    build_generation_identity,
    effective_fallback_reason,
    gpt_publication_state,
    load_cached_version,
    version_is_deployable_gpt,
)
from news_pipeline.unification.sentences import split_sentences


GPT_ONLY_EVALUATION_VERSION = "gpt_only_cached_candidate_evaluation_v2"
GPT_ONLY_REVIEW_VERSION = "gpt_only_candidate_review_v1"
GPT_ONLY_REVIEW_FIELDS = (
    "reviewer_decision",
    "factual_accuracy_1_to_5",
    "completeness_1_to_5",
    "attribution_1_to_5",
    "conflict_handling_1_to_5",
    "clarity_coherence_1_to_5",
    "repetition_1_to_5",
    "unsupported_material_claim",
    "validator_assessment",
    "reviewer_notes",
)
GPT_ONLY_REVIEW_HEADERS = (
    "review_order",
    "dataset_split",
    "story_id",
    "cluster_id",
    "deployed_status",
    "deployed_output_method",
    "validation_status",
    "fallback_reason",
    "display_title",
    "unified_story",
    "claims",
    "conflicts_or_uncertainties",
    "article_count",
    "source_count",
    "source_publishers",
    "source_titles",
    "source_urls",
    "source_article_texts",
    "model",
    "prompt_version",
    "input_tokens",
    "output_tokens",
    "estimated_cost_usd",
    "response_id",
    "review_target",
    "candidate_evidence",
    "validator_report",
    *GPT_ONLY_REVIEW_FIELDS,
)
GPT_ONLY_CSV_FIELDS = (
    "story_id",
    "cluster_id",
    "title",
    "story",
    "last_updated",
    "article_count",
    "candidate_status",
    "publication_status",
    "review_queue_status",
    "review_queue_reason_codes_json",
    "would_deploy_in_hybrid",
    "generation_status",
    "validation_status",
    "fallback_reason",
    "human_review_decision",
    "unsupported_material_claim",
    "model_name",
    "model_snapshot",
    "prompt_version",
    "response_id",
    "request_fingerprint_sha256",
    "claims_json",
    "conflicts_or_uncertainties_json",
)
GPT_ONLY_PUBLISHABLE_FIELDS = (
    "story_id",
    "cluster_id",
    "title",
    "story",
    "last_updated",
    "article_count",
)


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_articles(
    members: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "article_id": int(member["article_id"]),
            "source": member.get("source"),
            "title": member.get("title"),
            "url": member.get("url"),
            "published_date": member.get("published_date"),
            "clean_text": member.get("clean_text"),
        }
        for member in sorted(
            members,
            key=lambda item: int(item["article_id"]),
        )
    ]


def _base_row(
    cluster: Mapping[str, Any],
    members: list[dict[str, Any]],
) -> dict[str, Any]:
    sources = _source_articles(members)
    return {
        "story_id": str(cluster["cluster_key"]),
        "cluster_id": int(cluster["id"]),
        "title": "",
        "story": "",
        "last_updated": None,
        "article_count": len(members),
        "source_articles": sources,
        "candidate_status": "unavailable",
        "publication_status": "unavailable",
        "review_queue_status": None,
        "review_queue_reason_codes": [],
        "would_deploy_in_hybrid": False,
        "generation_status": None,
        "validation_status": None,
        "fallback_reason": None,
        "human_review_decision": None,
        "unsupported_material_claim": None,
        "model_name": None,
        "model_snapshot": None,
        "prompt_version": None,
        "response_id": None,
        "input_tokens": None,
        "output_tokens": None,
        "estimated_cost_usd": None,
        "validator_report": None,
        "request_fingerprint_sha256": None,
        "claims": [],
        "conflicts_or_uncertainties": [],
        "publishable_title": "",
        "publishable_story": "",
    }


def _candidate_row(
    *,
    cluster: Mapping[str, Any],
    members: list[dict[str, Any]],
    cached: Optional[Mapping[str, Any]],
    request_fingerprint_sha256: Optional[str],
) -> dict[str, Any]:
    row = _base_row(cluster, members)
    row["request_fingerprint_sha256"] = request_fingerprint_sha256
    if cached is None:
        row["candidate_status"] = "missing_cached_candidate"
        row["fallback_reason"] = "missing_cached_candidate"
        row["review_queue_reason_codes"] = [
            "missing_cached_candidate"
        ]
        return row

    review_scores = _json_mapping(cached.get("human_review_scores_json"))
    publication = gpt_publication_state(cached)
    row.update(
        {
            "last_updated": cached.get("updated_at"),
            **publication,
            "would_deploy_in_hybrid": version_is_deployable_gpt(cached),
            "generation_status": cached.get("generation_status"),
            "validation_status": cached.get("validation_status"),
            "fallback_reason": effective_fallback_reason(
                cached,
                default="not_deployable_in_hybrid",
            )
            if not version_is_deployable_gpt(cached)
            else None,
            "human_review_decision": cached.get("human_review_decision"),
            "unsupported_material_claim": review_scores.get(
                "unsupported_material_claim"
            ),
            "model_name": cached.get("model_name"),
            "model_snapshot": cached.get("model_snapshot"),
            "prompt_version": cached.get("prompt_version"),
            "response_id": cached.get("response_id"),
            "input_tokens": cached.get("input_tokens"),
            "output_tokens": cached.get("output_tokens"),
            "estimated_cost_usd": cached.get("estimated_cost_usd"),
            "validator_report": cached.get("validation_json"),
            "request_fingerprint_sha256": cached.get(
                "request_fingerprint_sha256"
            )
            or request_fingerprint_sha256,
        }
    )

    raw_output = cached.get("output_json")
    if not raw_output:
        row["candidate_status"] = "missing_raw_output"
        return row
    try:
        candidate = GPTUnifiedStoryResponseV2.model_validate_json(
            str(raw_output)
        )
    except (TypeError, ValueError, ValidationError):
        row["candidate_status"] = "malformed_raw_output"
        return row

    row.update(
        {
            "title": candidate.display_title,
            "story": candidate.unified_story,
            "candidate_status": "available",
            "claims": [
                claim.model_dump(mode="json")
                for claim in candidate.claims
            ],
            "conflicts_or_uncertainties": [
                conflict.model_dump(mode="json")
                for conflict in candidate.conflicts_or_uncertainties
            ],
        }
    )
    resolved = _json_mapping(cached.get("resolved_output_json"))
    row["publishable_title"] = str(
        resolved.get("display_title") or candidate.display_title
    )
    row["publishable_story"] = str(
        resolved.get("unified_story") or candidate.unified_story
    )
    return row


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            handle.write("\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=GPT_ONLY_CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    field: (
                        json.dumps(
                            row["claims"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        if field == "claims_json"
                        else json.dumps(
                            row["conflicts_or_uncertainties"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        if field == "conflicts_or_uncertainties_json"
                        else json.dumps(
                            row["review_queue_reason_codes"],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        if field == "review_queue_reason_codes_json"
                        else row.get(field)
                    )
                    for field in GPT_ONLY_CSV_FIELDS
                }
            )


def _write_publishable_csv(
    path: Path,
    rows: list[dict[str, Any]],
) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=GPT_ONLY_PUBLISHABLE_FIELDS,
        )
        writer.writeheader()
        for row in rows:
            if (
                row["publication_status"]
                != GPT_PUBLICATION_STATUS_PUBLISHABLE
            ):
                continue
            writer.writerow(
                {
                    "story_id": row["story_id"],
                    "cluster_id": row["cluster_id"],
                    "title": row["publishable_title"],
                    "story": row["publishable_story"],
                    "last_updated": row["last_updated"],
                    "article_count": row["article_count"],
                }
            )


def _candidate_evidence(
    row: Mapping[str, Any],
) -> list[dict[str, Any]]:
    sentences_by_article = {
        int(article["article_id"]): split_sentences(
            str(article.get("clean_text") or "")
        )
        for article in row.get("source_articles") or []
        if article.get("article_id")
    }
    span_ids = {
        str(span_id)
        for field in ("claims", "conflicts_or_uncertainties")
        for record in row.get(field) or []
        for span_id in record.get("evidence_span_ids") or []
    }
    evidence = []
    for span_id in sorted(span_ids):
        match = re.fullmatch(
            r"evidence_(\d+)_(\d+)_([0-9a-f]{20})",
            span_id,
        )
        article_id = int(match.group(1)) if match else None
        sentence_index = int(match.group(2)) if match else None
        sentences = sentences_by_article.get(article_id or -1, [])
        excerpt = (
            sentences[sentence_index]
            if sentence_index is not None
            and sentence_index < len(sentences)
            else None
        )
        evidence.append(
            {
                "evidence_span_id": span_id,
                "article_id": article_id,
                "sentence_index": sentence_index,
                "excerpt": excerpt,
                "resolved": excerpt is not None,
            }
        )
    return evidence


def _review_rows(
    rows: list[dict[str, Any]],
    *,
    review_target: str = "gpt_only_raw_candidate",
) -> list[dict[str, Any]]:
    review_rows = []
    for review_order, row in enumerate(rows, start=1):
        sources = row["source_articles"]
        publishers = sorted(
            {
                str(source.get("source"))
                for source in sources
                if source.get("source")
            }
        )
        deployability = (
            "candidate_unavailable"
            if row["candidate_status"] != "available"
            else "candidate_quarantined"
            if (
                row["publication_status"]
                == GPT_PUBLICATION_STATUS_PENDING_REVIEW
            )
            else "candidate_rejected"
            if (
                row["publication_status"]
                == GPT_PUBLICATION_STATUS_REJECTED
            )
            else "candidate_accepted"
        )
        review_row = {
            "review_order": review_order,
            "dataset_split": "gpt_only_evaluation",
            "story_id": row["story_id"],
            "cluster_id": row["cluster_id"],
            "deployed_status": deployability,
            "deployed_output_method": "gpt_candidate",
            "validation_status": row["validation_status"],
            "fallback_reason": row["fallback_reason"],
            "display_title": row["title"],
            "unified_story": row["story"],
            "claims": json.dumps(
                row["claims"],
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "conflicts_or_uncertainties": json.dumps(
                row["conflicts_or_uncertainties"],
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "article_count": row["article_count"],
            "source_count": len(publishers),
            "source_publishers": " | ".join(publishers),
            "source_titles": " | ".join(
                str(source.get("title") or "") for source in sources
            ),
            "source_urls": " | ".join(
                str(source.get("url") or "") for source in sources
            ),
            "source_article_texts": json.dumps(
                sources,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "model": row["model_name"],
            "prompt_version": row["prompt_version"],
            "input_tokens": row["input_tokens"],
            "output_tokens": row["output_tokens"],
            "estimated_cost_usd": row["estimated_cost_usd"],
            "response_id": row["response_id"],
            "review_target": review_target,
            "candidate_evidence": json.dumps(
                _candidate_evidence(row),
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "validator_report": row["validator_report"],
            **{field: "" for field in GPT_ONLY_REVIEW_FIELDS},
        }
        review_rows.append(review_row)
    return review_rows


def _write_review_csv(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    review_target: str = "gpt_only_raw_candidate",
) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=GPT_ONLY_REVIEW_HEADERS,
        )
        writer.writeheader()
        writer.writerows(
            _review_rows(rows, review_target=review_target)
        )


def _review_rubric(prompt_version: str) -> str:
    return "\n".join(
        [
            "# GPT-only candidate review rubric",
            "",
            f"Prompt version: `{prompt_version}`",
            "",
            "Review each raw GPT candidate only against "
            "`source_article_texts`, `candidate_evidence`, and "
            "`validator_report` in the same row.",
            "",
            "- `accept`: suitable without a substantive correction.",
            "- `minor_issue`: usable after a small non-material correction.",
            "- `major_issue`: unsupported material fact, important omission, "
            "misleading attribution/conflict handling, or unusable prose.",
            "- Score every criterion from 1 (poor) to 5 (excellent).",
            "- Set `unsupported_material_claim` to `yes` or `no`.",
            "- Set `validator_assessment` to `correct_block`, "
            "`false_positive`, or `unclear`.",
            "- Explain non-accept decisions, scores below 5, and validator "
            "assessments in `reviewer_notes`.",
            "",
            "A blank candidate remains a review row and should normally be "
            "marked `major_issue` with an explanatory note.",
            "",
            "This package was exported offline and made no API or generation "
            "calls.",
            "",
        ]
    )


def export_gpt_only_evaluation(
    *,
    config: Optional[PipelineConfig] = None,
    output_dir: Optional[Path | str] = None,
) -> dict[str, Any]:
    """Export cached raw GPT candidates without deterministic fallbacks.

    This is deliberately an offline evaluation artifact. It neither generates
    stories nor changes the guarded production export.
    """
    config = config or load_config()
    selected_dir = (
        Path(output_dir)
        if output_dir is not None
        else (
            config.reviews_dir
            / "gpt_only_evaluation"
            / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        )
    )
    selected_dir.mkdir(parents=True, exist_ok=True)

    connection = get_connection(config)
    rows: list[dict[str, Any]] = []
    try:
        for cluster, members, article_records in _load_generation_candidates(
            connection
        ):
            try:
                identity = build_generation_identity(
                    cluster=cluster,
                    members=members,
                    article_records_by_id=article_records,
                    config=config,
                )
            except (TypeError, ValueError, ValidationError):
                row = _base_row(cluster, members)
                row["candidate_status"] = "invalid_cluster_input"
                row["fallback_reason"] = "invalid_cluster_input"
                rows.append(row)
                continue

            cached = load_cached_version(
                connection,
                identity.request_fingerprint_sha256,
            )
            rows.append(
                _candidate_row(
                    cluster=cluster,
                    members=members,
                    cached=cached,
                    request_fingerprint_sha256=(
                        identity.request_fingerprint_sha256
                    ),
                )
            )
    finally:
        connection.close()

    rows.sort(key=lambda row: str(row["story_id"]))
    jsonl_path = selected_dir / "gpt_only_stories.jsonl"
    csv_path = selected_dir / "gpt_only_stories.csv"
    publishable_path = selected_dir / "gpt_only_publishable_stories.csv"
    review_path = selected_dir / "gpt_only_candidate_review.csv"
    queue_review_path = selected_dir / "gpt_only_review_queue.csv"
    rubric_path = selected_dir / "gpt_only_candidate_review_rubric.md"
    manifest_path = selected_dir / "manifest.json"
    _write_jsonl(jsonl_path, rows)
    _write_csv(csv_path, rows)
    _write_publishable_csv(publishable_path, rows)
    _write_review_csv(review_path, rows)
    queue_rows = [
        row
        for row in rows
        if (
            row["publication_status"]
            == GPT_PUBLICATION_STATUS_PENDING_REVIEW
        )
    ]
    _write_review_csv(
        queue_review_path,
        queue_rows,
        review_target="gpt_only_quarantine_candidate",
    )
    rubric_path.write_text(
        _review_rubric(config.gpt_prompt_version),
        encoding="utf-8",
    )

    available = sum(
        row["candidate_status"] == "available" for row in rows
    )
    deployable = sum(
        bool(row["would_deploy_in_hybrid"]) for row in rows
    )
    publication_counts = {
        status: sum(
            row["publication_status"] == status for row in rows
        )
        for status in (
            GPT_PUBLICATION_STATUS_PUBLISHABLE,
            GPT_PUBLICATION_STATUS_PENDING_REVIEW,
            GPT_PUBLICATION_STATUS_REJECTED,
            "unavailable",
        )
    }
    report = {
        "evaluation_version": GPT_ONLY_EVALUATION_VERSION,
        "review_version": GPT_ONLY_REVIEW_VERSION,
        "mode": "cached_raw_gpt_only",
        "prompt_version": config.gpt_prompt_version,
        "production_activation": False,
        "production_selection_changed": False,
        "network_calls_made": 0,
        "generation_calls_made": 0,
        "fallback_outputs": 0,
        "counts": {
            "clusters": len(rows),
            "available_gpt_candidates": available,
            "unavailable_gpt_candidates": len(rows) - available,
            "would_deploy_in_hybrid": deployable,
            "would_fallback_in_hybrid": len(rows) - deployable,
            "review_rows": len(rows),
            "gpt_only_publishable": publication_counts[
                GPT_PUBLICATION_STATUS_PUBLISHABLE
            ],
            "gpt_only_pending_review": publication_counts[
                GPT_PUBLICATION_STATUS_PENDING_REVIEW
            ],
            "gpt_only_rejected": publication_counts[
                GPT_PUBLICATION_STATUS_REJECTED
            ],
            "gpt_only_unavailable": publication_counts["unavailable"],
            "review_queue_rows": len(queue_rows),
        },
        "paths": {
            "jsonl": str(jsonl_path),
            "csv": str(csv_path),
            "publishable_csv": str(publishable_path),
            "review_csv": str(review_path),
            "review_queue_csv": str(queue_review_path),
            "review_rubric": str(rubric_path),
            "manifest": str(manifest_path),
        },
        "artifact_sha256": {
            "jsonl": _sha256(jsonl_path),
            "csv": _sha256(csv_path),
            "publishable_csv": _sha256(publishable_path),
            "review_csv": _sha256(review_path),
            "review_queue_csv": _sha256(queue_review_path),
            "review_rubric": _sha256(rubric_path),
        },
        "warning": (
            "Evaluation only: the full audit files can contain rejected GPT "
            "text. gpt_only_publishable_stories.csv excludes unavailable, "
            "quarantined, and rejected candidates; "
            "gpt_only_review_queue.csv contains only pending review rows. "
            "The guarded production export remains unchanged."
        ),
    }
    manifest_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    return report
