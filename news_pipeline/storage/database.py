import json
import sqlite3
from datetime import datetime
from typing import Optional

from news_pipeline.config import load_config
from news_pipeline.statuses import (
    CLEAN_STATUS_CLEANED,
    CLEAN_STATUS_PENDING,
    CLEAN_STATUS_REJECTED,
    DEDUPE_STATUS_EXACT_DUPLICATE,
    DEDUPE_STATUS_PENDING,
    DEDUPE_STATUS_UNIQUE,
    URL_STATUS_DISCOVERED,
    URL_STATUS_EXHAUSTED,
    URL_STATUS_EXTRACTED,
    URL_STATUS_FETCH_FAILED,
)


sqlite3.register_adapter(datetime, lambda value: value.isoformat())


def get_connection():
    config = load_config()
    config.data_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(config.db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _ensure_column(cursor, table_name: str, column_name: str, column_def: str):
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    if column_name not in existing_columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")


def initialize_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS discovered_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL,
            discovered_at TEXT DEFAULT CURRENT_TIMESTAMP,
            rss_title TEXT,
            rss_published TEXT,
            fetched INTEGER DEFAULT 0
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL,
            title TEXT,
            title_source TEXT,
            author TEXT,
            author_source TEXT,
            published_date TEXT,
            published_date_source TEXT,
            category TEXT,
            category_source TEXT,
            raw_html TEXT,
            raw_text TEXT,
            clean_text TEXT,
            sinhala_purity REAL,
            content_hash TEXT,
            clean_hash TEXT,
            is_duplicate INTEGER DEFAULT 0,
            duplicate_of_id INTEGER,
            clean_status TEXT DEFAULT 'pending',
            dedupe_status TEXT DEFAULT 'pending',
            quality_flags TEXT DEFAULT '[]',
            metadata_flags TEXT DEFAULT '[]',
            crawl_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            cleaned_at TEXT,
            exported_at TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            stats_json TEXT,
            note TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS story_clusters (
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
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (representative_article_id) REFERENCES articles(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS story_cluster_members (
            cluster_id INTEGER NOT NULL,
            article_id INTEGER NOT NULL,
            similarity_score REAL,
            is_representative INTEGER DEFAULT 0,
            PRIMARY KEY (cluster_id, article_id),
            FOREIGN KEY (cluster_id) REFERENCES story_clusters(id)
                ON DELETE CASCADE,
            FOREIGN KEY (article_id) REFERENCES articles(id)
        )
        """
    )

    _ensure_column(cursor, "discovered_urls", "rss_title", "rss_title TEXT")
    _ensure_column(cursor, "discovered_urls", "rss_published", "rss_published TEXT")
    _ensure_column(
        cursor,
        "discovered_urls",
        "status",
        f"status TEXT DEFAULT '{URL_STATUS_DISCOVERED}'",
    )
    _ensure_column(
        cursor,
        "discovered_urls",
        "fetch_attempts",
        "fetch_attempts INTEGER DEFAULT 0",
    )
    _ensure_column(cursor, "discovered_urls", "last_error", "last_error TEXT")
    _ensure_column(
        cursor,
        "discovered_urls",
        "last_attempted_at",
        "last_attempted_at TEXT",
    )
    _ensure_column(cursor, "discovered_urls", "fetched_at", "fetched_at TEXT")

    _ensure_column(cursor, "articles", "raw_html", "raw_html TEXT")
    _ensure_column(cursor, "articles", "title_source", "title_source TEXT")
    _ensure_column(cursor, "articles", "author_source", "author_source TEXT")
    _ensure_column(
        cursor,
        "articles",
        "published_date_source",
        "published_date_source TEXT",
    )
    _ensure_column(cursor, "articles", "category_source", "category_source TEXT")
    _ensure_column(cursor, "articles", "clean_hash", "clean_hash TEXT")
    _ensure_column(
        cursor,
        "articles",
        "duplicate_of_id",
        "duplicate_of_id INTEGER",
    )
    _ensure_column(
        cursor,
        "articles",
        "clean_status",
        f"clean_status TEXT DEFAULT '{CLEAN_STATUS_PENDING}'",
    )
    _ensure_column(
        cursor,
        "articles",
        "dedupe_status",
        f"dedupe_status TEXT DEFAULT '{DEDUPE_STATUS_PENDING}'",
    )
    _ensure_column(
        cursor,
        "articles",
        "quality_flags",
        "quality_flags TEXT DEFAULT '[]'",
    )
    _ensure_column(
        cursor,
        "articles",
        "metadata_flags",
        "metadata_flags TEXT DEFAULT '[]'",
    )
    _ensure_column(cursor, "articles", "cleaned_at", "cleaned_at TEXT")
    _ensure_column(cursor, "articles", "exported_at", "exported_at TEXT")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_discovered_urls_status
        ON discovered_urls(status)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_articles_clean_status
        ON articles(clean_status)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_articles_dedupe_status
        ON articles(dedupe_status)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_articles_clean_hash
        ON articles(clean_hash)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_story_clusters_representative
        ON story_clusters(representative_article_id)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_story_cluster_members_article
        ON story_cluster_members(article_id)
        """
    )

    cursor.execute(
        """
        UPDATE discovered_urls
        SET status = CASE
            WHEN fetched = 1 AND url IN (SELECT url FROM articles) THEN ?
            WHEN fetched = 1 AND url NOT IN (SELECT url FROM articles) THEN ?
            WHEN fetched = 0
                 AND COALESCE(fetch_attempts, 0) > 0
                 AND (status IS NULL OR status = '' OR status = ?) THEN ?
            WHEN status IS NULL OR status = '' THEN ?
            ELSE ?
        END
        """,
        (
            URL_STATUS_EXTRACTED,
            URL_STATUS_EXHAUSTED,
            URL_STATUS_DISCOVERED,
            URL_STATUS_FETCH_FAILED,
            URL_STATUS_DISCOVERED,
            URL_STATUS_DISCOVERED,
        ),
    )

    cursor.execute(
        """
        UPDATE articles
        SET clean_status = CASE
            WHEN clean_text IS NOT NULL
                 AND TRIM(clean_text) != ''
                 AND (clean_status IS NULL OR clean_status = '' OR clean_status = ?) THEN ?
            WHEN sinhala_purity IS NOT NULL
                 AND (clean_status IS NULL OR clean_status = '' OR clean_status = ?) THEN ?
            WHEN clean_status IS NULL OR clean_status = '' THEN ?
            ELSE clean_status
        END
        """,
        (
            CLEAN_STATUS_PENDING,
            CLEAN_STATUS_CLEANED,
            CLEAN_STATUS_PENDING,
            CLEAN_STATUS_REJECTED,
            CLEAN_STATUS_PENDING,
        ),
    )

    cursor.execute(
        """
        UPDATE articles
        SET dedupe_status = CASE
            WHEN is_duplicate = 1
                 AND (dedupe_status IS NULL OR dedupe_status = '' OR dedupe_status = ?) THEN ?
            WHEN clean_text IS NOT NULL
                 AND TRIM(clean_text) != ''
                 AND COALESCE(is_duplicate, 0) = 0
                 AND (dedupe_status IS NULL OR dedupe_status = '' OR dedupe_status = ?) THEN ?
            WHEN dedupe_status IS NULL OR dedupe_status = '' THEN ?
            ELSE dedupe_status
        END
        """,
        (
            DEDUPE_STATUS_PENDING,
            DEDUPE_STATUS_EXACT_DUPLICATE,
            DEDUPE_STATUS_PENDING,
            DEDUPE_STATUS_UNIQUE,
            DEDUPE_STATUS_PENDING,
        ),
    )

    cursor.execute(
        """
        UPDATE articles
        SET quality_flags = '[]'
        WHERE quality_flags IS NULL OR quality_flags = ''
        """
    )
    cursor.execute(
        """
        UPDATE articles
        SET metadata_flags = '[]'
        WHERE metadata_flags IS NULL OR metadata_flags = ''
        """
    )

    conn.commit()
    conn.close()


def start_pipeline_run():
    conn = get_connection()
    cursor = conn.cursor()
    started_at = datetime.now().isoformat(timespec="seconds")
    cursor.execute(
        """
        INSERT INTO pipeline_runs (started_at, status, stats_json)
        VALUES (?, ?, ?)
        """,
        (started_at, "running", json.dumps({})),
    )
    run_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return run_id


def finish_pipeline_run(
    run_id: int,
    status: str,
    stats: dict,
    note: Optional[str] = None,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE pipeline_runs
        SET finished_at = ?, status = ?, stats_json = ?, note = ?
        WHERE id = ?
        """,
        (
            datetime.now().isoformat(timespec="seconds"),
            status,
            json.dumps(stats, ensure_ascii=False),
            note,
            run_id,
        ),
    )
    conn.commit()
    conn.close()
