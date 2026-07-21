import unittest

from news_pipeline.extractor.metadata_extractor import (
    compute_text_hash,
    extract_metadata,
    final_metadata_flags,
)


class MetadataExtractorTests(unittest.TestCase):
    def test_prefers_trafilatura_then_rss_metadata(self):
        metadata = extract_metadata(
            url="https://example.com/2026/04/17/story",
            source="Example",
            trafilatura_data={
                "title": "Extracted title",
                "date": "2026-04-17T08:30:00+05:30",
            },
            html="",
            rss_title="RSS title",
            rss_published="Fri, 17 Apr 2026 08:30:00 +0530",
        )

        self.assertEqual(metadata.title, "Extracted title")
        self.assertEqual(metadata.title_source, "trafilatura")
        self.assertEqual(metadata.published_date_source, "trafilatura")
        self.assertTrue(metadata.published_date.startswith("2026-04-17T08:30:00"))

    def test_extracts_json_ld_metadata(self):
        html = """
        <html>
          <head>
            <script type="application/ld+json">
              {
                "@type": "NewsArticle",
                "headline": "JSON-LD headline",
                "datePublished": "2026-05-07T09:15:00+05:30",
                "author": {"name": "Reporter Name"},
                "articleSection": "local"
              }
            </script>
          </head>
        </html>
        """

        metadata = extract_metadata(
            url="https://example.com/story",
            source="Example",
            trafilatura_data={},
            html=html,
        )

        self.assertEqual(metadata.title, "JSON-LD headline")
        self.assertEqual(metadata.title_source, "json_ld")
        self.assertEqual(metadata.author, "Reporter Name")
        self.assertEqual(metadata.category, "local")
        self.assertEqual(metadata.published_date_source, "json_ld")

    def test_extracts_html_meta_and_h1_fallbacks(self):
        html = """
        <html>
          <head>
            <meta property="article:published_time" content="2026-04-16T17:17:00+05:30">
          </head>
          <body><h1>Page heading title</h1></body>
        </html>
        """

        metadata = extract_metadata(
            url="https://example.com/story",
            source="Example",
            trafilatura_data={},
            html=html,
        )

        self.assertEqual(metadata.title, "Page heading title")
        self.assertEqual(metadata.title_source, "h1")
        self.assertEqual(metadata.published_date_source, "html_meta")

    def test_falls_back_to_url_slug_and_url_date(self):
        metadata = extract_metadata(
            url="https://mawbima.lk/2026/04/16/sample-news-story/",
            source="Mawbima",
            trafilatura_data={},
            html="",
        )

        self.assertEqual(metadata.title, "sample news story")
        self.assertEqual(metadata.title_source, "url_slug")
        self.assertEqual(metadata.published_date, "2026-04-16T00:00:00")
        self.assertIn("title_from_url_slug", metadata.metadata_flags)

    def test_final_metadata_flags_marks_missing_values(self):
        flags = final_metadata_flags("", "", "", ["title_from_url_slug"])

        self.assertIn("missing_title", flags)
        self.assertIn("missing_published_date", flags)
        self.assertIn("missing_content_hash", flags)
        self.assertIn("title_from_url_slug", flags)

    def test_compute_text_hash_handles_blank_text(self):
        self.assertEqual(compute_text_hash(""), "")
        self.assertEqual(compute_text_hash("same"), compute_text_hash("same"))


if __name__ == "__main__":
    unittest.main()
