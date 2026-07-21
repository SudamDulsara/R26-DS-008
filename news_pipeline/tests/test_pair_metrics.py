import csv
import tempfile
import unittest
from pathlib import Path

from news_pipeline.cli import build_parser
from news_pipeline.evaluation.pair_metrics import (
    build_model_metrics,
    parse_revision_overrides,
    read_labeled_pairs,
)


class PairMetricsTests(unittest.TestCase):
    def test_metrics_find_perfect_separating_threshold(self):
        rows = [
            {"pair_id": "p1", "review_label": "same_event"},
            {"pair_id": "p2", "review_label": "same_event"},
            {"pair_id": "n1", "review_label": "different_event"},
            {"pair_id": "n2", "review_label": "different_event"},
        ]
        metrics = build_model_metrics(
            rows,
            {"p1": 0.9, "p2": 0.8, "n1": 0.7, "n2": 0.6},
            operational_threshold=0.75,
        )

        self.assertEqual(metrics["roc_auc"], 1.0)
        self.assertEqual(metrics["average_precision"], 1.0)
        self.assertEqual(
            metrics["operational_threshold"]["accuracy_percent"],
            100.0,
        )
        self.assertEqual(
            metrics["best_threshold"]["balanced_accuracy_percent"],
            100.0,
        )

    def test_read_labeled_pairs_validates_and_normalizes_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "pairs.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "pair_id",
                        "left_article_id",
                        "right_article_id",
                        "review_label",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "pair_id": "pair_1_2",
                        "left_article_id": "1",
                        "right_article_id": "2",
                        "review_label": "SAME_EVENT",
                    }
                )

            rows = read_labeled_pairs(path)

        self.assertEqual(rows[0]["left_article_id"], 1)
        self.assertEqual(rows[0]["review_label"], "same_event")

    def test_revision_overrides_require_model_equals_revision(self):
        revisions = parse_revision_overrides(["model/a=abc123"])
        self.assertEqual(revisions, {"model/a": "abc123"})
        with self.assertRaisesRegex(ValueError, "MODEL=REVISION"):
            parse_revision_overrides(["missing-revision"])

    def test_cli_parser_accepts_repeated_comparison_models(self):
        args = build_parser().parse_args(
            [
                "compare-pair-models",
                "--review-file",
                "pairs.csv",
                "--model",
                "model/a",
                "--model",
                "model/b",
                "--revision",
                "model/a=abc123",
            ]
        )

        self.assertEqual(args.models, ["model/a", "model/b"])
        self.assertEqual(args.revision, ["model/a=abc123"])


if __name__ == "__main__":
    unittest.main()
