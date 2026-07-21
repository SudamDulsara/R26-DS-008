from datetime import datetime, timedelta
import unittest

from news_pipeline.clustering.embedder import HashingEmbedder
from news_pipeline.evaluation.pair_benchmark import (
    BenchmarkArticle,
    ReviewedCluster,
    build_pair_benchmark_records,
)


def _article(article_id: int, source: str, hours: int = 0):
    event_time = datetime(2026, 5, 1, 10) + timedelta(hours=hours)
    text = f"shared event words article {article_id}"
    return BenchmarkArticle(
        id=article_id,
        source=source,
        title=text,
        url=f"https://example.com/{article_id}",
        published_date=event_time.isoformat(),
        crawl_timestamp=event_time.isoformat(),
        clean_text=text,
        event_time=event_time,
        similarity_text=text,
    )


class PairBenchmarkTests(unittest.TestCase):
    def test_builds_balanced_deterministic_review_draft(self):
        articles = {
            article.id: article
            for article in [
                _article(1, "A"),
                _article(2, "B", 1),
                _article(3, "C", 2),
                _article(4, "A", 3),
                _article(5, "B", 4),
                _article(6, "C", 5),
                _article(7, "D", 6),
            ]
        }
        clusters = [
            ReviewedCluster("10", "same_event", (1, 2, 3)),
            ReviewedCluster("11", "same_event", (4, 5)),
            ReviewedCluster("12", "mixed_related", (6, 7)),
        ]
        embedder = HashingEmbedder()
        vectors = embedder.encode(
            [articles[article_id].similarity_text for article_id in articles],
            batch_size=4,
        )
        embeddings = dict(zip(articles, vectors))

        records = build_pair_benchmark_records(
            clusters=clusters,
            articles=articles,
            embeddings=embeddings,
            target_size=6,
            positive_fraction=0.5,
            window_hours=72,
            model_name=embedder.model_name,
            model_revision=embedder.model_revision,
        )

        self.assertEqual(len(records), 6)
        self.assertEqual(
            [record["review_label"] for record in records].count("same_event"),
            3,
        )
        self.assertEqual(
            [record["review_label"] for record in records].count(""),
            3,
        )
        self.assertTrue(
            any(
                record["selection_bucket"] == "mixed_cluster_internal"
                for record in records
            )
        )
        self.assertTrue(
            all(
                record["baseline_model_revision"] == "builtin-v1"
                for record in records
            )
        )

    def test_excludes_same_source_pairs(self):
        articles = {
            1: _article(1, "A"),
            2: _article(2, "A", 1),
            3: _article(3, "B", 2),
            4: _article(4, "C", 3),
        }
        clusters = [
            ReviewedCluster("10", "same_event", (1, 2, 3)),
            ReviewedCluster("11", "mixed_related", (4, 3)),
        ]
        embedder = HashingEmbedder()
        embeddings = dict(
            zip(
                articles,
                embedder.encode(
                    [article.similarity_text for article in articles.values()],
                    batch_size=4,
                ),
            )
        )

        with self.assertRaisesRegex(ValueError, "Not enough eligible"):
            build_pair_benchmark_records(
                clusters=clusters,
                articles=articles,
                embeddings=embeddings,
                target_size=20,
                positive_fraction=0.5,
                window_hours=72,
                model_name="hashing",
                model_revision="builtin-v1",
            )


if __name__ == "__main__":
    unittest.main()
