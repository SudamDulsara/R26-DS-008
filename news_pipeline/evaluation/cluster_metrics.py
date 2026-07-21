import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Optional

from news_pipeline.config import load_config
from news_pipeline.storage.logger import get_logger


logger = get_logger()

REVIEW_LABELS = ("same_event", "mixed_related", "wrong_cluster", "unsure")
DECIDED_LABELS = ("same_event", "mixed_related", "wrong_cluster")
REQUIRED_COLUMNS = {
    "cluster_id",
    "review_label",
    "review_notes",
    "flags",
    "model_name",
    "similarity_threshold",
    "article_count",
}
SIZE_BUCKETS = (
    ("1", 1, 1),
    ("2", 2, 2),
    ("3-4", 3, 4),
    ("5-9", 5, 9),
    ("10+", 10, None),
)


def read_review_rows(review_path: Path) -> list[dict]:
    if not review_path.exists():
        raise FileNotFoundError(f"Reviewed cluster file not found: {review_path}")

    with review_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing_columns = sorted(REQUIRED_COLUMNS - columns)
        if missing_columns:
            raise ValueError(
                "Reviewed cluster file is missing required columns: "
                + ", ".join(missing_columns)
            )
        return list(reader)


def build_cluster_metrics(rows: list[dict]) -> dict:
    normalized_rows = [_normalize_row(row, index) for index, row in enumerate(rows, 2)]
    _validate_unique_cluster_ids(normalized_rows)

    label_counts = Counter(row["review_label"] for row in normalized_rows)
    flag_counts = Counter(
        flag for row in normalized_rows for flag in row["flags"]
    )
    reviewed_rows = [row for row in normalized_rows if row["review_label"]]
    decided_rows = [
        row for row in normalized_rows if row["review_label"] in DECIDED_LABELS
    ]
    accepted_rows = [
        row for row in normalized_rows if row["review_label"] == "same_event"
    ]
    rejected_rows = [
        row
        for row in normalized_rows
        if row["review_label"] in {"mixed_related", "wrong_cluster"}
    ]
    flagged_rows = [row for row in normalized_rows if row["flags"]]
    unflagged_rows = [row for row in normalized_rows if not row["flags"]]
    article_counts = [row["article_count"] for row in normalized_rows]

    metrics = {
        "summary": {
            "total_clusters": len(normalized_rows),
            "reviewed_clusters": len(reviewed_rows),
            "decided_clusters": len(decided_rows),
            "accepted_clusters": len(accepted_rows),
            "rejected_clusters": len(rejected_rows),
            "unsure_clusters": label_counts["unsure"],
            "unreviewed_clusters": label_counts[""],
            "total_member_articles": sum(article_counts),
            "reviewed_member_articles": sum(
                row["article_count"] for row in reviewed_rows
            ),
            "acceptance_rate_percent": _acceptance_rate(decided_rows),
        },
        "label_counts": {
            **{label: label_counts[label] for label in REVIEW_LABELS},
            "unreviewed": label_counts[""],
        },
        "flag_metrics": {
            "flagged_clusters": len(flagged_rows),
            "unflagged_clusters": len(unflagged_rows),
            "flag_counts": dict(sorted(flag_counts.items())),
            "flagged_acceptance_rate_percent": _acceptance_rate(flagged_rows),
            "unflagged_acceptance_rate_percent": _acceptance_rate(unflagged_rows),
        },
        "cluster_sizes": {
            "minimum": min(article_counts) if article_counts else None,
            "maximum": max(article_counts) if article_counts else None,
            "mean": round(mean(article_counts), 2) if article_counts else None,
            "median": median(article_counts) if article_counts else None,
            "buckets": _build_size_buckets(normalized_rows),
        },
        "model_names": sorted(
            {row["model_name"] for row in normalized_rows if row["model_name"]}
        ),
        "model_revisions": sorted(
            {
                row["model_revision"]
                for row in normalized_rows
                if row["model_revision"]
            }
        ),
        "similarity_thresholds": sorted(
            {
                row["similarity_threshold"]
                for row in normalized_rows
                if row["similarity_threshold"] is not None
            }
        ),
        "representative_thresholds": sorted(
            {
                row["representative_threshold"]
                for row in normalized_rows
                if row["representative_threshold"] is not None
            }
        ),
        "cohesion_thresholds": sorted(
            {
                row["cohesion_threshold"]
                for row in normalized_rows
                if row["cohesion_threshold"] is not None
            }
        ),
        "failure_notes": [
            {
                "cluster_id": row["cluster_id"],
                "review_label": row["review_label"],
                "article_count": row["article_count"],
                "flags": row["flags"],
                "review_notes": row["review_notes"],
            }
            for row in normalized_rows
            if row["review_label"] in {"mixed_related", "wrong_cluster", "unsure"}
        ],
    }
    return metrics


def evaluate_reviewed_clusters(
    review_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
):
    config = load_config()
    source_path = review_path or config.reviews_dir / "cluster_reviewed.csv"
    report_dir = output_dir or config.reviews_dir
    rows = read_review_rows(source_path)
    metrics = build_cluster_metrics(rows)

    generated_at = datetime.now().astimezone()
    timestamp = generated_at.strftime("%Y-%m-%d_%H-%M-%S")
    report = {
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "review_file": str(source_path.resolve()),
        **metrics,
    }

    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"cluster_metrics_{timestamp}.json"
    markdown_path = report_dir / f"cluster_metrics_{timestamp}.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_render_markdown(report), encoding="utf-8")

    summary = report["summary"]
    logger.info("=== Cluster Evaluation Complete ===")
    logger.info("Reviewed clusters: %s", summary["reviewed_clusters"])
    logger.info("Accepted clusters: %s", summary["accepted_clusters"])
    logger.info(
        "Acceptance rate: %s",
        _display_percent(summary["acceptance_rate_percent"]),
    )
    logger.info("JSON: %s", json_path)
    logger.info("Markdown: %s", markdown_path)

    return {
        **report,
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }


def _normalize_row(row: dict, row_number: int) -> dict:
    cluster_id = str(row.get("cluster_id") or "").strip()
    if not cluster_id:
        raise ValueError(f"Missing cluster_id on CSV row {row_number}")

    label = str(row.get("review_label") or "").strip().lower()
    if label and label not in REVIEW_LABELS:
        raise ValueError(
            f"Unknown review_label '{label}' for cluster {cluster_id}; "
            f"expected one of: {', '.join(REVIEW_LABELS)}"
        )

    article_count = _parse_article_count(row.get("article_count"), cluster_id)
    threshold = _parse_optional_float(
        row.get("similarity_threshold"),
        "similarity_threshold",
        cluster_id,
    )
    representative_threshold = _parse_optional_float(
        row.get("representative_threshold"),
        "representative_threshold",
        cluster_id,
    )
    cohesion_threshold = _parse_optional_float(
        row.get("cohesion_threshold"),
        "cohesion_threshold",
        cluster_id,
    )

    return {
        "cluster_id": cluster_id,
        "review_label": label,
        "review_notes": str(row.get("review_notes") or "").strip(),
        "flags": _parse_flags(row.get("flags")),
        "model_name": str(row.get("model_name") or "").strip(),
        "model_revision": str(row.get("model_revision") or "").strip(),
        "similarity_threshold": threshold,
        "representative_threshold": representative_threshold,
        "cohesion_threshold": cohesion_threshold,
        "article_count": article_count,
    }


def _validate_unique_cluster_ids(rows: list[dict]):
    id_counts = Counter(row["cluster_id"] for row in rows)
    duplicate_ids = sorted(
        cluster_id for cluster_id, count in id_counts.items() if count > 1
    )
    if duplicate_ids:
        raise ValueError(
            "Reviewed cluster file contains duplicate cluster IDs: "
            + ", ".join(duplicate_ids)
        )


def _parse_article_count(value, cluster_id: str) -> int:
    try:
        article_count = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid article_count for cluster {cluster_id}: {value!r}"
        ) from exc
    if article_count < 1:
        raise ValueError(
            f"article_count must be positive for cluster {cluster_id}"
        )
    return article_count


def _parse_optional_float(value, field_name: str, cluster_id: str):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(
            f"Invalid {field_name} for cluster {cluster_id}: {value!r}"
        ) from exc


def _parse_flags(value) -> list[str]:
    return sorted(
        {
            flag.strip()
            for flag in str(value or "").split(";")
            if flag.strip()
        }
    )


def _acceptance_rate(rows: list[dict]):
    decided_rows = [row for row in rows if row["review_label"] in DECIDED_LABELS]
    if not decided_rows:
        return None
    accepted_count = sum(
        row["review_label"] == "same_event" for row in decided_rows
    )
    return round(accepted_count / len(decided_rows) * 100, 2)


def _build_size_buckets(rows: list[dict]) -> dict:
    buckets = {}
    for name, lower, upper in SIZE_BUCKETS:
        bucket_rows = [
            row
            for row in rows
            if row["article_count"] >= lower
            and (upper is None or row["article_count"] <= upper)
        ]
        buckets[name] = {
            "clusters": len(bucket_rows),
            "member_articles": sum(row["article_count"] for row in bucket_rows),
            "accepted": sum(
                row["review_label"] == "same_event" for row in bucket_rows
            ),
            "rejected": sum(
                row["review_label"] in {"mixed_related", "wrong_cluster"}
                for row in bucket_rows
            ),
            "unsure": sum(row["review_label"] == "unsure" for row in bucket_rows),
            "unreviewed": sum(not row["review_label"] for row in bucket_rows),
            "acceptance_rate_percent": _acceptance_rate(bucket_rows),
        }
    return buckets


def _render_markdown(report: dict) -> str:
    summary = report["summary"]
    flag_metrics = report["flag_metrics"]
    flagged_rate = _display_percent(
        flag_metrics["flagged_acceptance_rate_percent"]
    )
    unflagged_rate = _display_percent(
        flag_metrics["unflagged_acceptance_rate_percent"]
    )
    lines = [
        "# Cluster Evaluation Report",
        "",
        f"Generated: {report['generated_at']}",
        "",
        f"Reviewed file: `{report['review_file']}`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Total clusters | {summary['total_clusters']} |",
        f"| Reviewed clusters | {summary['reviewed_clusters']} |",
        f"| Accepted same-event clusters | {summary['accepted_clusters']} |",
        f"| Rejected/mixed clusters | {summary['rejected_clusters']} |",
        f"| Unsure clusters | {summary['unsure_clusters']} |",
        f"| Unreviewed clusters | {summary['unreviewed_clusters']} |",
        f"| Reviewed member articles | {summary['reviewed_member_articles']} |",
        (
            "| Acceptance rate | "
            f"{_display_percent(summary['acceptance_rate_percent'])} |"
        ),
        "",
        "Acceptance rate excludes `unsure` and unreviewed clusters.",
        "",
        "## Labels",
        "",
        "| Label | Clusters |",
        "| --- | ---: |",
    ]
    for label, count in report["label_counts"].items():
        lines.append(f"| {label} | {count} |")

    lines.extend(
        [
            "",
            "## Review flags",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| Flagged clusters | {flag_metrics['flagged_clusters']} |",
            f"| Unflagged clusters | {flag_metrics['unflagged_clusters']} |",
            f"| Flagged acceptance rate | {flagged_rate} |",
            f"| Unflagged acceptance rate | {unflagged_rate} |",
            "",
            "| Flag | Occurrences |",
            "| --- | ---: |",
        ]
    )
    if flag_metrics["flag_counts"]:
        for flag, count in flag_metrics["flag_counts"].items():
            lines.append(f"| {flag} | {count} |")
    else:
        lines.append("| None | 0 |")

    lines.extend(
        [
            "",
            "## Cluster sizes",
            "",
            (
                "| Articles per cluster | Clusters | Accepted | Rejected | "
                "Acceptance rate |"
            ),
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for name, bucket in report["cluster_sizes"]["buckets"].items():
        lines.append(
            f"| {name} | {bucket['clusters']} | {bucket['accepted']} | "
            f"{bucket['rejected']} | "
            f"{_display_percent(bucket['acceptance_rate_percent'])} |"
        )

    lines.extend(["", "## Failure notes", ""])
    if not report["failure_notes"]:
        lines.append("No non-accepted reviewed clusters.")
    else:
        for failure in report["failure_notes"]:
            notes = " ".join(failure["review_notes"].split()) or "No notes supplied."
            lines.append(
                f"- Cluster {failure['cluster_id']} "
                f"(`{failure['review_label']}`, {failure['article_count']} articles): "
                f"{notes}"
            )

    lines.extend(
        [
            "",
            "## Configuration represented in the review",
            "",
            "- Models: " + (", ".join(report["model_names"]) or "unknown"),
            "- Model revisions: "
            + (", ".join(report["model_revisions"]) or "not recorded"),
            "- Similarity thresholds: "
            + (
                ", ".join(str(value) for value in report["similarity_thresholds"])
                or "unknown"
            ),
            "- Representative thresholds: "
            + (
                ", ".join(
                    str(value) for value in report["representative_thresholds"]
                )
                or "not recorded"
            ),
            "- Cohesion thresholds: "
            + (
                ", ".join(str(value) for value in report["cohesion_thresholds"])
                or "not recorded"
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _display_percent(value) -> str:
    return "n/a" if value is None else f"{value:.2f}%"
