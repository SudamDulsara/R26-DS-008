from datetime import datetime
import unittest

from news_pipeline.clustering.candidate_builder import build_candidate_pairs
from news_pipeline.clustering.embedder import HashingEmbedder, cosine_similarity
from news_pipeline.clustering.event_clusterer import ScoredPair, _build_story_clusters
from news_pipeline.clustering.text import (
    ClusterArticle,
    build_similarity_text,
    parse_article_datetime,
)


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

        clusters = _build_story_clusters(articles, linked_pairs, min_articles=2)

        self.assertEqual(len(clusters), 1)
        self.assertEqual(clusters[0].article_ids, [1, 2, 3])
        self.assertEqual(clusters[0].representative_article_id, 2)


if __name__ == "__main__":
    unittest.main()
