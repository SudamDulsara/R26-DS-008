import unittest

from news_pipeline.evaluation.cluster_review import (
    build_cluster_flags,
    build_member_flags,
    make_snippet,
)


class ClusterReviewTests(unittest.TestCase):
    def test_make_snippet_collapses_whitespace_and_truncates(self):
        snippet = make_snippet("one\n\n two   three four", max_chars=12)

        self.assertEqual(snippet, "one two t...")

    def test_build_cluster_flags_marks_large_low_confidence_clusters(self):
        flags = build_cluster_flags(
            {
                "article_count": 12,
                "source_count": 1,
                "confidence": 0.91,
            }
        )

        self.assertIn("large_cluster", flags)
        self.assertIn("single_source", flags)
        self.assertIn("borderline_confidence", flags)

    def test_build_member_flags_marks_indirect_graph_members(self):
        flags = build_member_flags(
            [
                {"is_representative": 1, "similarity_score": 1.0},
                {"is_representative": 0, "similarity_score": 0.0},
            ]
        )

        self.assertIn("indirect_graph_member", flags)


if __name__ == "__main__":
    unittest.main()
