import csv
import json
import tempfile
import unittest
from pathlib import Path

from news_pipeline.cli import build_parser
from news_pipeline.evaluation.cluster_metrics import (
    build_cluster_metrics,
    evaluate_reviewed_clusters,
    read_review_rows,
)


def _review_row(
    cluster_id,
    label,
    article_count,
    flags="",
    notes="",
):
    return {
        "cluster_id": str(cluster_id),
        "review_label": label,
        "review_notes": notes,
        "flags": flags,
        "model_name": "test-model",
        "similarity_threshold": "0.92",
        "representative_threshold": "0.92",
        "cohesion_threshold": "0.90",
        "article_count": str(article_count),
    }


class ClusterMetricsTests(unittest.TestCase):
    def test_build_cluster_metrics_summarizes_labels_flags_and_sizes(self):
        metrics = build_cluster_metrics(
            [
                _review_row(1, "same_event", 2, "indirect_graph_member"),
                _review_row(2, "same_event", 3),
                _review_row(
                    3,
                    "mixed_related",
                    5,
                    "indirect_graph_member; borderline_confidence",
                    "Related topic, but not the same event.",
                ),
                _review_row(4, "wrong_cluster", 10, notes="Unrelated articles."),
                _review_row(5, "unsure", 4, notes="Needs another review."),
                _review_row(6, "", 2),
            ]
        )

        self.assertEqual(metrics["summary"]["total_clusters"], 6)
        self.assertEqual(metrics["summary"]["reviewed_clusters"], 5)
        self.assertEqual(metrics["summary"]["decided_clusters"], 4)
        self.assertEqual(metrics["summary"]["accepted_clusters"], 2)
        self.assertEqual(metrics["summary"]["rejected_clusters"], 2)
        self.assertEqual(metrics["summary"]["acceptance_rate_percent"], 50.0)
        self.assertEqual(metrics["summary"]["total_member_articles"], 26)
        self.assertEqual(metrics["label_counts"]["unreviewed"], 1)
        self.assertEqual(metrics["flag_metrics"]["flagged_clusters"], 2)
        self.assertEqual(
            metrics["flag_metrics"]["flag_counts"]["indirect_graph_member"],
            2,
        )
        self.assertEqual(metrics["cluster_sizes"]["maximum"], 10)
        self.assertEqual(metrics["cluster_sizes"]["buckets"]["2"]["clusters"], 2)
        self.assertEqual(metrics["representative_thresholds"], [0.92])
        self.assertEqual(metrics["cohesion_thresholds"], [0.9])
        self.assertEqual(len(metrics["failure_notes"]), 3)

    def test_build_cluster_metrics_rejects_unknown_label(self):
        with self.assertRaisesRegex(ValueError, "Unknown review_label"):
            build_cluster_metrics([_review_row(1, "maybe", 2)])

    def test_build_cluster_metrics_rejects_duplicate_cluster_ids(self):
        with self.assertRaisesRegex(ValueError, "duplicate cluster IDs"):
            build_cluster_metrics(
                [
                    _review_row(1, "same_event", 2),
                    _review_row(1, "mixed_related", 3),
                ]
            )

    def test_read_review_rows_requires_expected_columns(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            review_path = Path(temp_dir) / "review.csv"
            review_path.write_text("cluster_id,review_label\n1,same_event\n")

            with self.assertRaisesRegex(ValueError, "missing required columns"):
                read_review_rows(review_path)

    def test_evaluate_reviewed_clusters_writes_json_and_markdown(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            review_path = temp_path / "reviewed.csv"
            output_dir = temp_path / "reports"
            rows = [
                _review_row(1, "same_event", 2),
                _review_row(2, "mixed_related", 3, notes="Too broad."),
            ]
            with review_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            report = evaluate_reviewed_clusters(review_path, output_dir)

            json_path = Path(report["json_path"])
            markdown_path = Path(report["markdown_path"])
            self.assertTrue(json_path.exists())
            self.assertTrue(markdown_path.exists())
            saved_report = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_report["summary"]["acceptance_rate_percent"], 50.0)
            self.assertIn(
                "Cluster Evaluation Report",
                markdown_path.read_text(encoding="utf-8"),
            )

    def test_cli_parser_accepts_cluster_evaluation_paths(self):
        args = build_parser().parse_args(
            [
                "evaluate-clusters",
                "--review-file",
                "reviewed.csv",
                "--output-dir",
                "reports",
            ]
        )

        self.assertEqual(args.command, "evaluate-clusters")
        self.assertEqual(args.review_file, Path("reviewed.csv"))
        self.assertEqual(args.output_dir, Path("reports"))


if __name__ == "__main__":
    unittest.main()
