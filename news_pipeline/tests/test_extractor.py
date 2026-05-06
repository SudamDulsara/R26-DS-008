import unittest

from news_pipeline.extractor.article_extractor import extract_category_from_url


class ExtractorTests(unittest.TestCase):
    def test_extracts_dinamina_category(self):
        url = "https://www.dinamina.lk/2026/04/17/politics/279937/example-story"
        self.assertEqual(extract_category_from_url(url, "Dinamina"), "politics")

    def test_returns_empty_for_other_sources(self):
        url = "https://www.bbc.com/sinhala/articles/c0kr26j5mmko"
        self.assertEqual(extract_category_from_url(url, "BBC Sinhala"), "")


if __name__ == "__main__":
    unittest.main()
