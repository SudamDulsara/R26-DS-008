from datetime import datetime
from math import cos, radians, sin
import os
import unittest
from unittest.mock import patch

from news_pipeline.cli import build_parser
from news_pipeline.clustering.candidate_builder import build_candidate_pairs
from news_pipeline.clustering.embedder import HashingEmbedder, cosine_similarity
from news_pipeline.clustering.event_clusterer import (
    ScoredPair,
    _build_story_clusters,
    _validate_similarity_threshold,
)
from news_pipeline.clustering.text import (
    ClusterArticle,
    build_similarity_text,
    parse_article_datetime,
)
from news_pipeline.config import load_config


def _article(article_id, source, title, clean_text, event_time):
    return ClusterArticle(
        id=article_id,
        source=source,
        title=title,
        published_date=event_time.isoformat(),
        crawl_timestamp=event_time.isoformat(),
        clean_text=clean_text,
        event_time=event_time,
        similarity_text=build_similarity_text(title, clean_text, 200),
    )


def _angle_embedding(degrees):
    angle = radians(degrees)
    return [cos(angle), sin(angle)]


class ClusteringTests(unittest.TestCase):
    def test_build_similarity_text_uses_title_and_lead(self):
        text = build_similarity_text("Title", "First paragraph.\n\nSecond paragraph.", 100)
        self.assertIn("Title", text)
        self.assertIn("First paragraph.", text)
        self.assertNotIn("Second paragraph.", text)

    def test_parse_article_datetime_handles_iso_values(self):
        parsed = parse_article_datetime("2026-05-08T10:30:00+05:30")
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.month, 5)
        self.assertEqual(parsed.day, 8)

    def test_candidate_builder_respects_window_and_source_filter(self):
        base_time = datetime(2026, 5, 8, 10, 0, 0)
        articles = [
            _article(1, "A", "same event title", "same event lead", base_time),
            _article(2, "B", "same event title", "same event lead", base_time),
            _article(3, "A", "same event title", "same event lead", base_time),
            _article(4, "C", "same event title", "same event lead", datetime(2026, 5, 12)),
        ]

        pairs = build_candidate_pairs(
            articles,
            window_hours=72,
            allow_same_source_pairs=False,
            min_lexical_overlap=0.0,
        )

        pair_ids = {(pair.left_id, pair.right_id) for pair in pairs}
        self.assertIn((1, 2), pair_ids)
        self.assertNotIn((1, 3), pair_ids)
        self.assertNotIn((1, 4), pair_ids)

    def test_hashing_embedder_is_deterministic_for_identical_text(self):
        embedder = HashingEmbedder()
        left, right = embedder.encode(["same event", "same event"], batch_size=2)
        self.assertAlmostEqual(cosine_similarity(left, right), 1.0)
        self.assertEqual(embedder.model_revision, "builtin-v1")

    def test_graph_clustering_links_connected_articles(self):
        base_time = datetime(2026, 5, 8, 10, 0, 0)
        articles = [
            _article(1, "A", "event", "short text", base_time),
            _article(2, "B", "event", "longer text for representative", base_time),
            _article(3, "C", "event", "connected article", base_time),
        ]
        linked_pairs = [
            ScoredPair(1, 2, 0.9, 0.5, 1.0),
            ScoredPair(2, 3, 0.88, 0.5, 1.0),
        ]

        result = _build_story_clusters(
            articles,
            linked_pairs,
            embeddings={
                1: [1.0, 0.0],
                2: [1.0, 0.0],
                3: [1.0, 0.0],
            },
            min_articles=2,
            representative_threshold=0.85,
            cohesion_threshold=0.85,
        )
        clusters = result.clusters

        self.assertEqual(len(clusters), 1)
        self.assertEqual(clusters[0].article_ids, [1, 2, 3])
        self.assertEqual(clusters[0].representative_article_id, 2)
        self.assertEqual(result.changed_components, 0)

    def test_representative_validation_splits_chain_and_keeps_remainder(self):
        base_time = datetime(2026, 5, 8, 10, 0, 0)
        articles = [
            _article(1, "A", "event", "one", base_time),
            _article(2, "B", "event", "longest representative text", base_time),
            _article(3, "C", "event", "three", base_time),
            _article(4, "D", "event", "four", base_time),
            _article(5, "E", "event", "five", base_time),
        ]
        linked_pairs = [
            ScoredPair(1, 2, 0.91, 0.5, 1.0),
            ScoredPair(2, 3, 0.91, 0.5, 1.0),
            ScoredPair(3, 4, 0.91, 0.5, 1.0),
            ScoredPair(4, 5, 0.91, 0.5, 1.0),
        ]
        embeddings = {
            article_id: _angle_embedding(angle)
            for article_id, angle in enumerate((0, 25, 50, 75, 100), 1)
        }

        result = _build_story_clusters(
            articles,
            linked_pairs,
            embeddings=embeddings,
            min_articles=2,
            representative_threshold=0.9,
            cohesion_threshold=0.9,
        )

        self.assertEqual(
            [cluster.article_ids for cluster in result.clusters],
            [[1, 2, 3], [4, 5]],
        )
        self.assertEqual(result.initial_components, 1)
        self.assertEqual(result.changed_components, 1)
        self.assertEqual(result.split_components, 1)
        self.assertEqual(result.unclustered_articles, 0)
        for cluster in result.clusters:
            self.assertTrue(
                all(score >= 0.9 for score in cluster.member_scores.values())
            )

    def test_representative_validation_tracks_unclustered_singleton(self):
        base_time = datetime(2026, 5, 8, 10, 0, 0)
        articles = [
            _article(1, "A", "event", "one", base_time),
            _article(2, "B", "event", "longest representative text", base_time),
            _article(3, "C", "event", "three", base_time),
            _article(4, "D", "event", "four", base_time),
        ]
        linked_pairs = [
            ScoredPair(1, 2, 0.91, 0.5, 1.0),
            ScoredPair(2, 3, 0.91, 0.5, 1.0),
            ScoredPair(3, 4, 0.91, 0.5, 1.0),
        ]
        embeddings = {
            article_id: _angle_embedding(angle)
            for article_id, angle in enumerate((0, 25, 50, 75), 1)
        }

        result = _build_story_clusters(
            articles,
            linked_pairs,
            embeddings=embeddings,
            min_articles=2,
            representative_threshold=0.9,
            cohesion_threshold=0.9,
        )

        self.assertEqual(
            [cluster.article_ids for cluster in result.clusters],
            [[1, 2, 3]],
        )
        self.assertEqual(result.changed_components, 1)
        self.assertEqual(result.split_components, 0)
        self.assertEqual(result.unclustered_articles, 1)

    def test_cohesion_fallback_retains_mutually_consistent_borderline_member(self):
        base_time = datetime(2026, 5, 8, 10, 0, 0)
        articles = [
            _article(1, "A", "event", "one", base_time),
            _article(2, "B", "event", "two", base_time),
            _article(3, "C", "event", "three", base_time),
            _article(4, "D", "event", "four", base_time),
        ]
        embeddings = {
            1: _angle_embedding(0),
            2: _angle_embedding(10),
            3: _angle_embedding(10),
            4: _angle_embedding(24),
        }
        linked_pairs = [
            ScoredPair(1, 2, cosine_similarity(embeddings[1], embeddings[2]), 0.5, 1.0),
            ScoredPair(1, 3, cosine_similarity(embeddings[1], embeddings[3]), 0.5, 1.0),
            ScoredPair(2, 4, cosine_similarity(embeddings[2], embeddings[4]), 0.5, 1.0),
            ScoredPair(3, 4, cosine_similarity(embeddings[3], embeddings[4]), 0.5, 1.0),
        ]

        result = _build_story_clusters(
            articles,
            linked_pairs,
            embeddings=embeddings,
            min_articles=2,
            representative_threshold=0.92,
            cohesion_threshold=0.91,
        )

        self.assertEqual(len(result.clusters), 1)
        self.assertEqual(result.clusters[0].article_ids, [1, 2, 3, 4])
        self.assertLess(result.clusters[0].member_scores[4], 0.92)
        self.assertGreaterEqual(result.clusters[0].member_scores[4], 0.91)
        self.assertEqual(result.cohesion_fallback_members, 1)

    def test_similarity_threshold_validation_rejects_out_of_range_value(self):
        with self.assertRaisesRegex(ValueError, "between -1.0 and 1.0"):
            _validate_similarity_threshold("representative_threshold", 1.1)

    def test_cli_parser_accepts_representative_threshold(self):
        args = build_parser().parse_args(
            [
                "cluster",
                "--model-revision",
                "model-commit",
                "--threshold",
                "0.92",
                "--representative-threshold",
                "0.94",
                "--cohesion-threshold",
                "0.91",
            ]
        )

        self.assertEqual(args.threshold, 0.92)
        self.assertEqual(args.model_revision, "model-commit")
        self.assertEqual(args.representative_threshold, 0.94)
        self.assertEqual(args.cohesion_threshold, 0.91)

    def test_default_cohesion_threshold_is_below_representative_threshold(self):
        with patch.dict(
            os.environ,
            {"NEWS_PIPELINE_CLUSTER_REPRESENTATIVE_THRESHOLD": "0.92"},
            clear=True,
        ):
            config = load_config()

        self.assertEqual(config.cluster_representative_threshold, 0.92)
        self.assertEqual(config.cluster_cohesion_threshold, 0.9)

    def test_default_model_revision_is_pinned(self):
        with patch.dict(os.environ, {}, clear=True):
            config = load_config()

        self.assertEqual(
            config.cluster_model_revision,
            "d128750597153bb5987e10b1c3493a34e5a4502a",
        )

    def test_custom_model_does_not_inherit_default_e5_revision(self):
        with patch.dict(
            os.environ,
            {"NEWS_PIPELINE_CLUSTER_MODEL": "example/custom-model"},
            clear=True,
        ):
            config = load_config()

        self.assertIsNone(config.cluster_model_revision)


if __name__ == "__main__":
    unittest.main()
