import unittest

from news_pipeline.evaluation.cluster_comparison import compare_membership_sets


class ClusterComparisonTests(unittest.TestCase):
    def test_classifies_unchanged_expanded_merged_and_new_clusters(self):
        baseline = {
            1: frozenset({1, 2}),
            2: frozenset({3, 4}),
            3: frozenset({5, 6}),
            4: frozenset({7, 8}),
        }
        candidate = {
            10: frozenset({1, 2}),
            11: frozenset({3, 4, 9}),
            12: frozenset({5, 6, 7, 8}),
            13: frozenset({10, 11}),
        }

        comparison = compare_membership_sets(baseline, candidate)

        self.assertEqual(comparison["summary"]["unchanged_clusters"], 1)
        self.assertEqual(comparison["summary"]["changed_candidate_clusters"], 3)
        change_types = {
            change["candidate_cluster_id"]: change["change_type"]
            for change in comparison["candidate_changes"]
        }
        self.assertEqual(change_types[11], "expanded_cluster")
        self.assertEqual(change_types[12], "merged_clusters")
        self.assertEqual(change_types[13], "new_cluster")
        self.assertEqual(comparison["newly_clustered_article_ids"], [9, 10, 11])
        self.assertEqual(comparison["no_longer_clustered_article_ids"], [])


if __name__ == "__main__":
    unittest.main()
