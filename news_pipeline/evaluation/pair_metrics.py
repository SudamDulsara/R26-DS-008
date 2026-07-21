import csv
import gc
import json
import re
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Optional

from news_pipeline.clustering.embedder import cosine_similarity, create_embedder
from news_pipeline.config import load_config
from news_pipeline.evaluation.pair_benchmark import load_benchmark_articles
from news_pipeline.storage.logger import get_logger


logger = get_logger()

PAIR_LABELS = {"same_event", "different_event", "unsure"}
DECIDED_PAIR_LABELS = {"same_event", "different_event"}
REQUIRED_COLUMNS = {
    "pair_id",
    "left_article_id",
    "right_article_id",
    "review_label",
}


def compare_pair_models(
    review_path: Path,
    model_names: Optional[list[str]] = None,
    revision_by_model: Optional[dict[str, str]] = None,
    output_dir: Optional[Path] = None,
    operational_threshold: Optional[float] = None,
):
    config = load_config()
    threshold = (
        operational_threshold
        if operational_threshold is not None
        else config.cluster_similarity_threshold
    )
    if not -1.0 <= threshold <= 1.0:
        raise ValueError("operational_threshold must be between -1.0 and 1.0")

    selected_models = _unique_nonempty(model_names or [config.cluster_model_name])
    revisions = revision_by_model or {}
    unknown_revision_models = sorted(set(revisions) - set(selected_models))
    if unknown_revision_models:
        raise ValueError(
            "Revision supplied for an unselected model: "
            + ", ".join(unknown_revision_models)
        )

    rows = read_labeled_pairs(review_path)
    decided_rows = [
        row for row in rows if row["review_label"] in DECIDED_PAIR_LABELS
    ]
    label_counts = _label_counts(rows)
    if not label_counts.get("same_event") or not label_counts.get("different_event"):
        raise ValueError(
            "Pair benchmark must contain both same_event and different_event labels"
        )

    article_ids = sorted(
        {
            article_id
            for row in decided_rows
            for article_id in (row["left_article_id"], row["right_article_id"])
        }
    )
    articles = load_benchmark_articles(article_ids, config.cluster_lead_char_limit)
    missing_ids = sorted(set(article_ids) - set(articles))
    if missing_ids:
        raise ValueError(
            "Benchmark article IDs are missing from the database: "
            + ", ".join(map(str, missing_ids))
        )

    generated_at = datetime.now().astimezone()
    timestamp = generated_at.strftime("%Y-%m-%d_%H-%M-%S")
    report_dir = output_dir or config.reviews_dir / "pair_benchmark" / "comparison"
    report_dir.mkdir(parents=True, exist_ok=True)

    model_results = []
    failures = []
    ordered_articles = [articles[article_id] for article_id in article_ids]
    texts = [article.similarity_text for article in ordered_articles]

    for model_name in selected_models:
        requested_revision = revisions.get(model_name)
        if (
            requested_revision is None
            and model_name == config.cluster_model_name
        ):
            requested_revision = config.cluster_model_revision

        embedder = None
        logger.info("Scoring pair benchmark with %s", model_name)
        try:
            embedder = create_embedder(model_name, requested_revision)
            vectors = embedder.encode(texts, batch_size=config.cluster_batch_size)
            embeddings = {
                article.id: vector
                for article, vector in zip(ordered_articles, vectors)
                if vector
            }
            if len(embeddings) != len(ordered_articles):
                raise RuntimeError(
                    f"{model_name} returned {len(embeddings)} usable embeddings "
                    f"for {len(ordered_articles)} articles"
                )
            pair_scores = {
                row["pair_id"]: cosine_similarity(
                    embeddings[row["left_article_id"]],
                    embeddings[row["right_article_id"]],
                )
                for row in decided_rows
            }
            metrics = build_model_metrics(decided_rows, pair_scores, threshold)
            scored_path = report_dir / (
                f"pair_scores_{_safe_name(model_name)}_{timestamp}.csv"
            )
            _write_scored_pairs(
                scored_path,
                decided_rows,
                pair_scores,
                model_name=embedder.model_name,
                model_revision=embedder.model_revision,
                operational_threshold=threshold,
                best_threshold=metrics["best_threshold"]["threshold"],
            )
            model_results.append(
                {
                    "model_name": embedder.model_name,
                    "model_revision": embedder.model_revision,
                    "requested_revision": requested_revision,
                    "scored_pairs_path": str(scored_path.resolve()),
                    **metrics,
                }
            )
        except Exception as exc:
            logger.exception("Pair benchmark failed for model %s", model_name)
            failures.append(
                {
                    "model_name": model_name,
                    "requested_revision": requested_revision,
                    "error": str(exc),
                }
            )
        finally:
            del embedder
            gc.collect()

    ranked_results = sorted(
        model_results,
        key=lambda result: (
            result["best_threshold"]["balanced_accuracy_percent"],
            result["roc_auc"],
            result["average_precision"],
        ),
        reverse=True,
    )
    for rank, result in enumerate(ranked_results, 1):
        result["rank"] = rank

    report = {
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "review_file": str(review_path.resolve()),
        "pair_count": len(rows),
        "decided_pair_count": len(decided_rows),
        "label_counts": label_counts,
        "operational_threshold": threshold,
        "ranking_basis": (
            "best-threshold balanced accuracy, then ROC AUC, then average precision"
        ),
        "models": ranked_results,
        "failures": failures,
    }
    json_path = report_dir / f"pair_model_comparison_{timestamp}.json"
    markdown_path = report_dir / f"pair_model_comparison_{timestamp}.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_render_markdown(report), encoding="utf-8")

    logger.info("=== Pair Model Comparison Complete ===")
    logger.info("Successful models: %s", len(ranked_results))
    logger.info("Failed models: %s", len(failures))
    logger.info("JSON: %s", json_path)
    logger.info("Markdown: %s", markdown_path)

    if not ranked_results:
        raise RuntimeError(
            "All requested models failed; see the comparison report for details"
        )
    return {
        **report,
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }


def read_labeled_pairs(review_path: Path) -> list[dict]:
    if not review_path.exists():
        raise FileNotFoundError(f"Pair benchmark file not found: {review_path}")
    with review_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(REQUIRED_COLUMNS - set(reader.fieldnames or []))
        if missing:
            raise ValueError(
                "Pair benchmark is missing required columns: "
                + ", ".join(missing)
            )
        raw_rows = list(reader)

    rows = []
    seen_pair_ids = set()
    for row_number, raw_row in enumerate(raw_rows, 2):
        row = dict(raw_row)
        pair_id = str(row.get("pair_id") or "").strip()
        if not pair_id:
            raise ValueError(f"Missing pair_id on CSV row {row_number}")
        if pair_id in seen_pair_ids:
            raise ValueError(f"Duplicate pair_id in benchmark: {pair_id}")
        label = str(row.get("review_label") or "").strip().lower()
        if label not in PAIR_LABELS:
            raise ValueError(
                f"Invalid or missing review_label for {pair_id}: {label!r}"
            )
        left_id = _parse_article_id(row.get("left_article_id"), pair_id)
        right_id = _parse_article_id(row.get("right_article_id"), pair_id)
        if left_id == right_id:
            raise ValueError(f"Pair {pair_id} repeats article {left_id}")
        expected_pair_id = f"pair_{min(left_id, right_id)}_{max(left_id, right_id)}"
        if pair_id != expected_pair_id:
            raise ValueError(
                f"Pair ID {pair_id} does not match article IDs; "
                f"expected {expected_pair_id}"
            )
        row["pair_id"] = pair_id
        row["review_label"] = label
        row["left_article_id"] = left_id
        row["right_article_id"] = right_id
        rows.append(row)
        seen_pair_ids.add(pair_id)
    return rows


def parse_revision_overrides(values: Optional[list[str]]) -> dict[str, str]:
    revisions = {}
    for value in values or []:
        model_name, separator, revision = value.partition("=")
        model_name = model_name.strip()
        revision = revision.strip()
        if not separator or not model_name or not revision:
            raise ValueError(
                "Model revisions must use MODEL=REVISION format; "
                f"got {value!r}"
            )
        if model_name in revisions:
            raise ValueError(f"Duplicate revision override for {model_name}")
        revisions[model_name] = revision
    return revisions


def build_model_metrics(
    rows: list[dict],
    pair_scores: dict[str, float],
    operational_threshold: float,
) -> dict:
    missing_scores = sorted(
        row["pair_id"] for row in rows if row["pair_id"] not in pair_scores
    )
    if missing_scores:
        raise ValueError("Missing scores for pairs: " + ", ".join(missing_scores))
    labels_and_scores = [
        (row["review_label"] == "same_event", pair_scores[row["pair_id"]])
        for row in rows
        if row["review_label"] in DECIDED_PAIR_LABELS
    ]
    positive_scores = [score for label, score in labels_and_scores if label]
    negative_scores = [score for label, score in labels_and_scores if not label]
    if not positive_scores or not negative_scores:
        raise ValueError("Metrics require both positive and negative labeled pairs")

    operational = _classification_metrics(
        labels_and_scores,
        operational_threshold,
    )
    best = _best_threshold(labels_and_scores, operational_threshold)
    false_positives = _error_examples(
        rows,
        pair_scores,
        operational_threshold,
        expected_label="different_event",
        reverse=True,
    )
    false_negatives = _error_examples(
        rows,
        pair_scores,
        operational_threshold,
        expected_label="same_event",
        reverse=False,
    )
    return {
        "roc_auc": round(_roc_auc(positive_scores, negative_scores), 6),
        "average_precision": round(_average_precision(labels_and_scores), 6),
        "score_distribution": {
            "same_event": _score_summary(positive_scores),
            "different_event": _score_summary(negative_scores),
            "mean_gap": round(mean(positive_scores) - mean(negative_scores), 6),
            "strict_separation_gap": round(
                min(positive_scores) - max(negative_scores),
                6,
            ),
        },
        "operational_threshold": operational,
        "best_threshold": best,
        "operational_false_positives": false_positives,
        "operational_false_negatives": false_negatives,
    }


def _classification_metrics(labels_and_scores, threshold: float) -> dict:
    true_positive = sum(label and score >= threshold for label, score in labels_and_scores)
    false_positive = sum(
        not label and score >= threshold for label, score in labels_and_scores
    )
    false_negative = sum(
        label and score < threshold for label, score in labels_and_scores
    )
    true_negative = sum(
        not label and score < threshold for label, score in labels_and_scores
    )
    total = len(labels_and_scores)
    precision = _safe_ratio(true_positive, true_positive + false_positive)
    recall = _safe_ratio(true_positive, true_positive + false_negative)
    specificity = _safe_ratio(true_negative, true_negative + false_positive)
    f1 = _safe_ratio(2 * precision * recall, precision + recall)
    return {
        "threshold": round(threshold, 6),
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "true_negative": true_negative,
        "accuracy_percent": _percent(_safe_ratio(true_positive + true_negative, total)),
        "precision_percent": _percent(precision),
        "recall_percent": _percent(recall),
        "specificity_percent": _percent(specificity),
        "f1_percent": _percent(f1),
        "balanced_accuracy_percent": _percent((recall + specificity) / 2),
    }


def _best_threshold(labels_and_scores, operational_threshold: float) -> dict:
    unique_scores = sorted({score for _, score in labels_and_scores})
    epsilon = 1e-9
    candidates = [unique_scores[0] - epsilon, unique_scores[-1] + epsilon]
    candidates.extend(
        (left + right) / 2
        for left, right in zip(unique_scores, unique_scores[1:])
    )
    evaluated = [
        _classification_metrics(labels_and_scores, threshold)
        for threshold in candidates
    ]
    return max(
        evaluated,
        key=lambda metrics: (
            metrics["balanced_accuracy_percent"],
            metrics["f1_percent"],
            metrics["accuracy_percent"],
            metrics["precision_percent"],
            -abs(metrics["threshold"] - operational_threshold),
        ),
    )


def _roc_auc(positive_scores: list[float], negative_scores: list[float]) -> float:
    wins = 0.0
    for positive in positive_scores:
        for negative in negative_scores:
            if positive > negative:
                wins += 1.0
            elif positive == negative:
                wins += 0.5
    return wins / (len(positive_scores) * len(negative_scores))


def _average_precision(labels_and_scores) -> float:
    ranked = sorted(labels_and_scores, key=lambda item: item[1], reverse=True)
    positive_count = sum(label for label, _ in ranked)
    true_positives = 0
    precision_sum = 0.0
    for rank, (label, _) in enumerate(ranked, 1):
        if label:
            true_positives += 1
            precision_sum += true_positives / rank
    return precision_sum / positive_count


def _score_summary(scores: list[float]) -> dict:
    return {
        "minimum": round(min(scores), 6),
        "maximum": round(max(scores), 6),
        "mean": round(mean(scores), 6),
        "median": round(median(scores), 6),
    }


def _error_examples(
    rows: list[dict],
    pair_scores: dict[str, float],
    threshold: float,
    expected_label: str,
    reverse: bool,
) -> list[dict]:
    errors = []
    for row in rows:
        score = pair_scores[row["pair_id"]]
        predicted_same = score >= threshold
        is_error = (
            expected_label == "different_event" and predicted_same
        ) or (
            expected_label == "same_event" and not predicted_same
        )
        if row["review_label"] == expected_label and is_error:
            errors.append(
                {
                    "pair_id": row["pair_id"],
                    "score": round(score, 6),
                    "left_title": str(row.get("left_title") or ""),
                    "right_title": str(row.get("right_title") or ""),
                }
            )
    return sorted(errors, key=lambda item: item["score"], reverse=reverse)[:10]


def _write_scored_pairs(
    path: Path,
    rows: list[dict],
    pair_scores: dict[str, float],
    model_name: str,
    model_revision: str,
    operational_threshold: float,
    best_threshold: float,
):
    extra_columns = [
        "evaluation_model_name",
        "evaluation_model_revision",
        "model_similarity_score",
        "predicted_at_operational_threshold",
        "correct_at_operational_threshold",
        "predicted_at_best_threshold",
        "correct_at_best_threshold",
    ]
    fieldnames = list(rows[0]) + [
        column for column in extra_columns if column not in rows[0]
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            score = pair_scores[row["pair_id"]]
            operational_prediction = _prediction(score, operational_threshold)
            best_prediction = _prediction(score, best_threshold)
            writer.writerow(
                {
                    **row,
                    "evaluation_model_name": model_name,
                    "evaluation_model_revision": model_revision,
                    "model_similarity_score": round(score, 6),
                    "predicted_at_operational_threshold": operational_prediction,
                    "correct_at_operational_threshold": int(
                        operational_prediction == row["review_label"]
                    ),
                    "predicted_at_best_threshold": best_prediction,
                    "correct_at_best_threshold": int(
                        best_prediction == row["review_label"]
                    ),
                }
            )


def _prediction(score: float, threshold: float) -> str:
    return "same_event" if score >= threshold else "different_event"


def _render_markdown(report: dict) -> str:
    counts = report["label_counts"]
    lines = [
        "# Pair Benchmark Model Comparison",
        "",
        f"Generated: {report['generated_at']}",
        "",
        f"Benchmark: `{report['review_file']}`",
        "",
        (
            f"Pairs: {report['decided_pair_count']} decided "
            f"({counts.get('same_event', 0)} same-event, "
            f"{counts.get('different_event', 0)} different-event)"
        ),
        "",
        f"Operational threshold: `{report['operational_threshold']}`",
        "",
        "## Ranking",
        "",
        (
            "| Rank | Model | Revision | ROC AUC | Avg precision | Best threshold | "
            "Best balanced accuracy | Best F1 | Accuracy at operational threshold |"
        ),
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in report["models"]:
        best = result["best_threshold"]
        operational = result["operational_threshold"]
        lines.append(
            f"| {result['rank']} | `{result['model_name']}` | "
            f"`{result['model_revision']}` | {result['roc_auc']:.4f} | "
            f"{result['average_precision']:.4f} | {best['threshold']:.6f} | "
            f"{best['balanced_accuracy_percent']:.2f}% | "
            f"{best['f1_percent']:.2f}% | "
            f"{operational['accuracy_percent']:.2f}% |"
        )

    lines.extend(
        [
            "",
            (
                "The best threshold is selected on this same benchmark and is "
                "therefore exploratory, not a held-out generalization estimate."
            ),
            "",
            "## Model details",
            "",
        ]
    )
    for result in report["models"]:
        operational = result["operational_threshold"]
        best = result["best_threshold"]
        distribution = result["score_distribution"]
        lines.extend(
            [
                f"### {result['rank']}. `{result['model_name']}`",
                "",
                f"- Revision: `{result['model_revision']}`",
                (
                    "- Operational confusion: "
                    f"TP {operational['true_positive']}, "
                    f"FP {operational['false_positive']}, "
                    f"FN {operational['false_negative']}, "
                    f"TN {operational['true_negative']}"
                ),
                (
                    f"- Operational precision/recall/F1: "
                    f"{operational['precision_percent']:.2f}% / "
                    f"{operational['recall_percent']:.2f}% / "
                    f"{operational['f1_percent']:.2f}%"
                ),
                (
                    f"- Best threshold `{best['threshold']:.6f}`: "
                    f"{best['accuracy_percent']:.2f}% accuracy, "
                    f"{best['balanced_accuracy_percent']:.2f}% balanced accuracy"
                ),
                (
                    "- Mean similarity, same/different: "
                    f"{distribution['same_event']['mean']:.6f} / "
                    f"{distribution['different_event']['mean']:.6f}"
                ),
                f"- Scored pairs: `{result['scored_pairs_path']}`",
                "",
            ]
        )

    if report["failures"]:
        lines.extend(["## Failed models", ""])
        for failure in report["failures"]:
            lines.append(
                f"- `{failure['model_name']}`: {failure['error']}"
            )
        lines.append("")
    return "\n".join(lines)


def _parse_article_id(value, pair_id: str) -> int:
    try:
        article_id = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid article ID in {pair_id}: {value!r}") from exc
    if article_id < 1:
        raise ValueError(f"Article IDs must be positive in {pair_id}")
    return article_id


def _label_counts(rows: list[dict]) -> dict[str, int]:
    return {
        label: sum(row["review_label"] == label for row in rows)
        for label in sorted(PAIR_LABELS)
    }


def _unique_nonempty(values: list[str]) -> list[str]:
    selected = []
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in selected:
            selected.append(normalized)
    if not selected:
        raise ValueError("At least one model must be selected")
    return selected


def _safe_name(model_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "__", model_name).strip("_")


def _safe_ratio(numerator: float, denominator: float) -> float:
    return 0.0 if denominator == 0 else numerator / denominator


def _percent(value: float) -> float:
    return round(value * 100, 4)
