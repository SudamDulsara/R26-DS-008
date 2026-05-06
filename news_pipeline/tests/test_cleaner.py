import unittest

from news_pipeline.cleaner.sinhala_cleaner import clean_article, calculate_sinhala_purity


class CleanerTests(unittest.TestCase):
    def test_clean_article_removes_links_and_preserves_text(self):
        clean_text, purity = clean_article("සිංහල පුවතක් https://example.com")
        self.assertNotIn("https://", clean_text)
        self.assertGreater(purity, 0.5)

    def test_calculate_sinhala_purity_handles_empty_text(self):
        self.assertEqual(calculate_sinhala_purity(""), 0.0)


if __name__ == "__main__":
    unittest.main()
