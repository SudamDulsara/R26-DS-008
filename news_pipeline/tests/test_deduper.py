import unittest

from news_pipeline.deduplicator.exact_deduper import (
    canonicalize_text,
    compute_clean_hash,
)


class DeduperTests(unittest.TestCase):
    def test_canonicalize_text_collapses_whitespace(self):
        self.assertEqual(canonicalize_text("a  b\n\nc"), "a b c")

    def test_compute_clean_hash_matches_equivalent_text(self):
        self.assertEqual(
            compute_clean_hash("සිංහල   පුවත"),
            compute_clean_hash("සිංහල පුවත"),
        )


if __name__ == "__main__":
    unittest.main()
