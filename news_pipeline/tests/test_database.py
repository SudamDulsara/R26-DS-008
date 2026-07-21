import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from news_pipeline.storage.database import initialize_db


class DatabaseMigrationTests(unittest.TestCase):
    def test_initialize_db_adds_representative_threshold_to_existing_clusters(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "pipeline.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE story_clusters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cluster_key TEXT UNIQUE NOT NULL,
                    representative_article_id INTEGER,
                    model_name TEXT NOT NULL,
                    text_variant TEXT NOT NULL,
                    similarity_threshold REAL NOT NULL,
                    event_date_start TEXT,
                    event_date_end TEXT,
                    article_count INTEGER NOT NULL,
                    source_count INTEGER NOT NULL,
                    confidence REAL NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                INSERT INTO story_clusters (
                    cluster_key,
                    model_name,
                    text_variant,
                    similarity_threshold,
                    article_count,
                    source_count,
                    confidence
                )
                VALUES ('legacy', 'model', 'title_lead', 0.92, 2, 2, 0.95)
                """
            )
            connection.commit()
            connection.close()

            environment = {
                "NEWS_PIPELINE_DATA_DIR": temp_dir,
                "NEWS_PIPELINE_DB_PATH": str(db_path),
            }
            with patch.dict(os.environ, environment, clear=False):
                initialize_db()

            connection = sqlite3.connect(db_path)
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(story_clusters)")
            }
            legacy_row = connection.execute(
                """
                SELECT cluster_key, representative_threshold
                FROM story_clusters
                WHERE cluster_key = 'legacy'
                """
            ).fetchone()
            connection.close()

            self.assertIn("representative_threshold", columns)
            self.assertIn("cohesion_threshold", columns)
            self.assertIn("model_revision", columns)
            self.assertEqual(legacy_row, ("legacy", None))


if __name__ == "__main__":
    unittest.main()
