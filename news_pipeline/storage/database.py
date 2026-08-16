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
    URL_ERROR_LEGACY_RETRY_STATE,
    URL_STATUS_DISCOVERED,
    URL_STATUS_EXHAUSTED,
    URL_STATUS_EXTRACTED,
    URL_STATUS_FETCH_FAILED,
    URL_STATUS_REJECTED,
)


sqlite3.register_adapter(datetime, lambda value: value.isoformat())


def get_connection(config=None):
    config = config or load_config()
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


def _backfill_pipeline_snapshot_state(cursor) -> None:
    rows = cursor.execute(
        """
        SELECT id, stats_json, snapshot_path
        FROM pipeline_runs
        WHERE status = 'completed'
        ORDER BY COALESCE(finished_at, started_at) DESC, id DESC
        """
    ).fetchall()
    snapshots = []
    for row in rows:
        snapshot_path = row["snapshot_path"]
        if not snapshot_path:
            try:
                stats = json.loads(row["stats_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                stats = {}
            export_stats = (
                stats.get("export")
                if isinstance(stats, dict)
                else None
            )
            snapshot_path = (
                export_stats.get("snapshot_dir")
                if isinstance(export_stats, dict)
                else None
            )
            if snapshot_path:
                cursor.execute(
                    """
                    UPDATE pipeline_runs
                    SET snapshot_path = ?
                    WHERE id = ?
                    """,
                    (snapshot_path, row["id"]),
                )
        if snapshot_path:
            snapshots.append((row["id"], snapshot_path))
    has_latest = cursor.execute(
        """
        SELECT 1
        FROM pipeline_runs
        WHERE is_latest_success = 1
        LIMIT 1
        """
    ).fetchone()
    if has_latest is None and snapshots:
        cursor.execute(
            """
            UPDATE pipeline_runs
            SET is_latest_success = 1
            WHERE id = ?
            """,
            (snapshots[0][0],),
        )


def _backfill_semantic_pair_constraints(connection) -> None:
    from news_pipeline.clustering.semantic_constraints import (
        persist_different_event_constraints,
    )

    rows = connection.execute(
        """
        SELECT cluster_key, output_json, autonomous_audit_route_json,
               updated_at
        FROM unified_story_versions
        WHERE fallback_reason = 'semantic_partition_applied'
          AND output_json IS NOT NULL
        """
    ).fetchall()
    for row in rows:
        try:
            output = json.loads(str(row["output_json"]))
            route = json.loads(
                str(row["autonomous_audit_route_json"] or "{}")
            )
            groups = output["article_groups"]
            if output.get("cluster_coherence") != "partition_required":
                continue
            persist_different_event_constraints(
                connection,
                groups=groups,
                audit_version=str(route.get("audit_version") or "unknown"),
                source_cluster_key=str(row["cluster_key"]),
                created_at=str(row["updated_at"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue


def initialize_db():
    config = load_config()
    conn = get_connection(config)
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
            note TEXT,
            snapshot_path TEXT,
            is_latest_success INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS source_discovery_checkpoints (
            source TEXT PRIMARY KEY,
            covered_through TEXT NOT NULL,
            last_success_at TEXT NOT NULL,
            coverage_status TEXT NOT NULL,
            boundary_url TEXT,
            details_json TEXT NOT NULL DEFAULT '{}'
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
            model_revision TEXT,
            text_variant TEXT NOT NULL,
            similarity_threshold REAL NOT NULL,
            representative_threshold REAL NOT NULL,
            cohesion_threshold REAL NOT NULL,
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS clustering_article_state (
            article_id INTEGER PRIMARY KEY,
            input_fingerprint_sha256 TEXT,
            clustering_status TEXT NOT NULL,
            cluster_key TEXT,
            processed_at TEXT NOT NULL,
            FOREIGN KEY (article_id) REFERENCES articles(id),
            FOREIGN KEY (cluster_key) REFERENCES story_clusters(cluster_key)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS clustering_embedding_cache (
            input_fingerprint_sha256 TEXT NOT NULL,
            model_name TEXT NOT NULL,
            model_revision TEXT NOT NULL,
            dimensions INTEGER NOT NULL CHECK (dimensions > 0),
            vector_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (
                input_fingerprint_sha256,
                model_name,
                model_revision
            )
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS story_cluster_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transition_batch_id TEXT NOT NULL,
            transition_type TEXT NOT NULL,
            old_cluster_key TEXT,
            new_cluster_key TEXT,
            overlap_article_count INTEGER NOT NULL,
            old_article_ids_json TEXT NOT NULL,
            new_article_ids_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE (
                transition_batch_id,
                transition_type,
                old_cluster_key,
                new_cluster_key
            )
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS semantic_pair_constraints (
            left_article_id INTEGER NOT NULL,
            right_article_id INTEGER NOT NULL,
            left_content_sha256 TEXT NOT NULL,
            right_content_sha256 TEXT NOT NULL,
            decision TEXT NOT NULL CHECK (decision = 'different_event'),
            audit_version TEXT NOT NULL,
            source_cluster_key TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (left_article_id, right_article_id),
            CHECK (left_article_id < right_article_id),
            FOREIGN KEY (left_article_id) REFERENCES articles(id),
            FOREIGN KEY (right_article_id) REFERENCES articles(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS unified_story_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id INTEGER,
            cluster_key TEXT NOT NULL,
            source_fingerprint_sha256 TEXT NOT NULL,
            input_fingerprint_sha256 TEXT NOT NULL,
            request_fingerprint_sha256 TEXT NOT NULL UNIQUE,
            model_name TEXT NOT NULL,
            model_snapshot TEXT,
            prompt_version TEXT NOT NULL,
            input_schema_version TEXT NOT NULL,
            output_schema_version TEXT NOT NULL,
            resolved_schema_version TEXT,
            reasoning_effort TEXT NOT NULL,
            max_output_tokens INTEGER NOT NULL,
            generation_status TEXT NOT NULL,
            validation_status TEXT NOT NULL,
            output_json TEXT,
            resolved_output_json TEXT,
            validation_json TEXT NOT NULL,
            preflight_json TEXT,
            input_tokens INTEGER,
            output_tokens INTEGER,
            total_tokens INTEGER,
            estimated_cost_usd TEXT,
            response_id TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            fallback_reason TEXT,
            primary_model_name TEXT,
            primary_response_id TEXT,
            primary_output_json TEXT,
            primary_validation_json TEXT,
            primary_input_tokens INTEGER,
            primary_output_tokens INTEGER,
            primary_total_tokens INTEGER,
            primary_estimated_cost_usd TEXT,
            autonomous_audit_status TEXT,
            autonomous_audit_model TEXT,
            autonomous_audit_response_id TEXT,
            autonomous_audit_route_json TEXT,
            autonomous_audit_input_tokens INTEGER,
            autonomous_audit_output_tokens INTEGER,
            autonomous_audit_total_tokens INTEGER,
            autonomous_audit_estimated_cost_usd TEXT,
            autonomous_audit_created_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS gpt_unification_review_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            unified_story_version_id INTEGER NOT NULL UNIQUE,
            cluster_id INTEGER,
            story_id TEXT NOT NULL,
            request_fingerprint_sha256 TEXT NOT NULL UNIQUE,
            queue_status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            validation_status TEXT NOT NULL,
            fallback_reason TEXT,
            candidate_title TEXT,
            candidate_story TEXT,
            prompt_version TEXT NOT NULL,
            model_name TEXT NOT NULL,
            response_id TEXT,
            detected_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            reviewed_at TEXT,
            review_decision TEXT,
            review_notes TEXT,
            FOREIGN KEY (unified_story_version_id)
                REFERENCES unified_story_versions(id) ON DELETE CASCADE,
            FOREIGN KEY (cluster_id)
                REFERENCES story_clusters(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS final_unified_stories (
            story_id TEXT PRIMARY KEY,
            cluster_id INTEGER NOT NULL UNIQUE,
            title TEXT NOT NULL,
            story TEXT NOT NULL,
            last_updated TEXT NOT NULL,
            article_count INTEGER NOT NULL,
            FOREIGN KEY (cluster_id) REFERENCES story_clusters(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS final_story_sources (
            story_id TEXT NOT NULL,
            article_id INTEGER NOT NULL,
            publisher TEXT,
            source_title TEXT,
            url TEXT,
            published_date TEXT,
            similarity_score REAL,
            is_representative INTEGER NOT NULL,
            referenced_by_gpt INTEGER NOT NULL,
            evidence_span_ids_json TEXT NOT NULL,
            PRIMARY KEY (story_id, article_id),
            FOREIGN KEY (story_id)
                REFERENCES final_unified_stories(story_id)
                ON DELETE CASCADE,
            FOREIGN KEY (article_id) REFERENCES articles(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS final_story_claims (
            story_id TEXT NOT NULL,
            claim_index INTEGER NOT NULL,
            claim_text TEXT NOT NULL,
            source_article_ids_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (story_id, claim_index),
            FOREIGN KEY (story_id)
                REFERENCES final_unified_stories(story_id)
                ON DELETE CASCADE
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS final_story_conflicts (
            story_id TEXT NOT NULL,
            conflict_index INTEGER NOT NULL,
            description TEXT NOT NULL,
            source_article_ids_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (story_id, conflict_index),
            FOREIGN KEY (story_id)
                REFERENCES final_unified_stories(story_id)
                ON DELETE CASCADE
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS final_story_publication_states (
            story_id TEXT PRIMARY KEY,
            cluster_id INTEGER NOT NULL UNIQUE,
            publication_status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL,
            unified_story_version_id INTEGER,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (cluster_id) REFERENCES story_clusters(id),
            FOREIGN KEY (unified_story_version_id)
                REFERENCES unified_story_versions(id)
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
        "last_error_code",
        "last_error_code TEXT",
    )
    _ensure_column(
        cursor,
        "discovered_urls",
        "last_http_status",
        "last_http_status INTEGER",
    )
    _ensure_column(
        cursor,
        "discovered_urls",
        "last_request_attempts",
        "last_request_attempts INTEGER DEFAULT 0",
    )
    _ensure_column(
        cursor,
        "discovered_urls",
        "last_attempted_at",
        "last_attempted_at TEXT",
    )
    _ensure_column(cursor, "discovered_urls", "fetched_at", "fetched_at TEXT")
    _ensure_column(
        cursor,
        "discovered_urls",
        "discovery_method",
        "discovery_method TEXT DEFAULT 'rss'",
    )
    _ensure_column(
        cursor,
        "discovered_urls",
        "rss_summary",
        "rss_summary TEXT",
    )

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
    _ensure_column(
        cursor,
        "articles",
        "extraction_method",
        "extraction_method TEXT",
    )
    _ensure_column(
        cursor,
        "pipeline_runs",
        "snapshot_path",
        "snapshot_path TEXT",
    )
    _ensure_column(
        cursor,
        "pipeline_runs",
        "is_latest_success",
        "is_latest_success INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(
        cursor,
        "story_clusters",
        "model_revision",
        "model_revision TEXT",
    )
    _ensure_column(
        cursor,
        "story_clusters",
        "representative_threshold",
        "representative_threshold REAL",
    )
    _ensure_column(
        cursor,
        "story_clusters",
        "cohesion_threshold",
        "cohesion_threshold REAL",
    )
    _ensure_column(
        cursor,
        "unified_story_versions",
        "human_review_decision",
        "human_review_decision TEXT",
    )
    _ensure_column(
        cursor,
        "unified_story_versions",
        "human_review_scores_json",
        "human_review_scores_json TEXT",
    )
    _ensure_column(
        cursor,
        "unified_story_versions",
        "human_review_notes",
        "human_review_notes TEXT",
    )
    _ensure_column(
        cursor,
        "unified_story_versions",
        "human_review_source_sha256",
        "human_review_source_sha256 TEXT",
    )
    _ensure_column(
        cursor,
        "unified_story_versions",
        "human_review_imported_at",
        "human_review_imported_at TEXT",
    )
    for column_name, column_def in (
        ("primary_model_name", "primary_model_name TEXT"),
        ("primary_response_id", "primary_response_id TEXT"),
        ("primary_output_json", "primary_output_json TEXT"),
        ("primary_validation_json", "primary_validation_json TEXT"),
        ("primary_input_tokens", "primary_input_tokens INTEGER"),
        ("primary_output_tokens", "primary_output_tokens INTEGER"),
        ("primary_total_tokens", "primary_total_tokens INTEGER"),
        (
            "primary_estimated_cost_usd",
            "primary_estimated_cost_usd TEXT",
        ),
        ("autonomous_audit_status", "autonomous_audit_status TEXT"),
        ("autonomous_audit_model", "autonomous_audit_model TEXT"),
        (
            "autonomous_audit_response_id",
            "autonomous_audit_response_id TEXT",
        ),
        (
            "autonomous_audit_route_json",
            "autonomous_audit_route_json TEXT",
        ),
        (
            "autonomous_audit_input_tokens",
            "autonomous_audit_input_tokens INTEGER",
        ),
        (
            "autonomous_audit_output_tokens",
            "autonomous_audit_output_tokens INTEGER",
        ),
        (
            "autonomous_audit_total_tokens",
            "autonomous_audit_total_tokens INTEGER",
        ),
        (
            "autonomous_audit_estimated_cost_usd",
            "autonomous_audit_estimated_cost_usd TEXT",
        ),
        (
            "autonomous_audit_created_at",
            "autonomous_audit_created_at TEXT",
        ),
    ):
        _ensure_column(
            cursor,
            "unified_story_versions",
            column_name,
            column_def,
        )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_discovered_urls_status
        ON discovered_urls(status)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_discovered_urls_source_published
        ON discovered_urls(source, rss_published)
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
        CREATE INDEX IF NOT EXISTS idx_clustering_article_state_status
        ON clustering_article_state(clustering_status, processed_at)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_clustering_article_state_cluster
        ON clustering_article_state(cluster_key)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_clustering_embedding_cache_model
        ON clustering_embedding_cache(model_name, model_revision)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_story_cluster_transitions_old
        ON story_cluster_transitions(old_cluster_key, created_at)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_story_cluster_transitions_new
        ON story_cluster_transitions(new_cluster_key, created_at)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_semantic_pair_constraints_decision
        ON semantic_pair_constraints(decision, left_article_id, right_article_id)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_unified_story_versions_cluster
        ON unified_story_versions(cluster_key, updated_at)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_unified_story_versions_source
        ON unified_story_versions(source_fingerprint_sha256)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_unified_story_versions_status
        ON unified_story_versions(generation_status, validation_status)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_final_story_state_status
        ON final_story_publication_states(publication_status, story_id)
        """
    )
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_latest_success
        ON pipeline_runs(is_latest_success)
        WHERE is_latest_success = 1
        """
    )
    _backfill_pipeline_snapshot_state(cursor)
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gpt_review_queue_status
        ON gpt_unification_review_queue(queue_status, updated_at)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gpt_review_queue_story
        ON gpt_unification_review_queue(story_id, updated_at)
        """
    )

    cursor.execute(
        """
        UPDATE discovered_urls
        SET status = CASE
            WHEN fetched = 1 AND url IN (SELECT url FROM articles) THEN ?
            WHEN fetched = 1 AND url NOT IN (SELECT url FROM articles) THEN ?
            WHEN status IN (?, ?, ?, ?, ?) THEN status
            WHEN COALESCE(fetch_attempts, 0) > 0 THEN ?
            ELSE ?
        END
        """,
        (
            URL_STATUS_EXTRACTED,
            URL_STATUS_EXHAUSTED,
            URL_STATUS_DISCOVERED,
            URL_STATUS_FETCH_FAILED,
            URL_STATUS_EXTRACTED,
            URL_STATUS_REJECTED,
            URL_STATUS_EXHAUSTED,
            URL_STATUS_FETCH_FAILED,
            URL_STATUS_DISCOVERED,
        ),
    )
    cursor.execute(
        """
        UPDATE discovered_urls
        SET status = ?,
            fetch_attempts = CASE
                WHEN COALESCE(fetch_attempts, 0) < ? THEN ?
                ELSE fetch_attempts
            END,
            last_error_code = COALESCE(
                NULLIF(last_error_code, ''),
                ?
            )
        WHERE status = ?
           OR (
                status = ?
                AND COALESCE(fetch_attempts, 0) >= ?
           )
        """,
        (
            URL_STATUS_EXHAUSTED,
            config.max_retries,
            config.max_retries,
            URL_ERROR_LEGACY_RETRY_STATE,
            URL_STATUS_EXHAUSTED,
            URL_STATUS_FETCH_FAILED,
            config.max_retries,
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
        WHERE NOT EXISTS (SELECT 1 FROM pipeline_runs LIMIT 1)
          AND (
                clean_status IS NULL
                OR clean_status = ''
                OR (
                    clean_status = ?
                    AND (
                        (clean_text IS NOT NULL AND TRIM(clean_text) != '')
                        OR sinhala_purity IS NOT NULL
                    )
                )
          )
        """,
        (
            CLEAN_STATUS_PENDING,
            CLEAN_STATUS_CLEANED,
            CLEAN_STATUS_PENDING,
            CLEAN_STATUS_REJECTED,
            CLEAN_STATUS_PENDING,
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
        WHERE NOT EXISTS (SELECT 1 FROM pipeline_runs LIMIT 1)
          AND (
                dedupe_status IS NULL
                OR dedupe_status = ''
                OR (
                    dedupe_status = ?
                    AND (
                        is_duplicate = 1
                        OR (
                            clean_text IS NOT NULL
                            AND TRIM(clean_text) != ''
                            AND COALESCE(is_duplicate, 0) = 0
                        )
                    )
                )
          )
        """,
        (
            DEDUPE_STATUS_PENDING,
            DEDUPE_STATUS_EXACT_DUPLICATE,
            DEDUPE_STATUS_PENDING,
            DEDUPE_STATUS_UNIQUE,
            DEDUPE_STATUS_PENDING,
            DEDUPE_STATUS_PENDING,
        ),
    )

    cursor.execute(
        """
        UPDATE articles
        SET quality_flags = '[]'
        WHERE NOT EXISTS (SELECT 1 FROM pipeline_runs LIMIT 1)
          AND (quality_flags IS NULL OR quality_flags = '')
        """
    )
    cursor.execute(
        """
        UPDATE articles
        SET metadata_flags = '[]'
        WHERE NOT EXISTS (SELECT 1 FROM pipeline_runs LIMIT 1)
          AND (metadata_flags IS NULL OR metadata_flags = '')
        """
    )
    cursor.execute(
        """
        INSERT INTO clustering_article_state (
            article_id,
            input_fingerprint_sha256,
            clustering_status,
            cluster_key,
            processed_at
        )
        SELECT
            articles.id,
            NULL,
            CASE
                WHEN clusters.cluster_key IS NOT NULL
                    THEN 'baseline_clustered'
                ELSE 'baseline_unclustered'
            END,
            clusters.cluster_key,
            CURRENT_TIMESTAMP
        FROM articles
        LEFT JOIN story_cluster_members AS members
          ON members.article_id = articles.id
        LEFT JOIN story_clusters AS clusters
          ON clusters.id = members.cluster_id
        WHERE articles.clean_status = ?
          AND articles.dedupe_status = ?
          AND articles.clean_text IS NOT NULL
          AND TRIM(articles.clean_text) != ''
          AND EXISTS (SELECT 1 FROM story_clusters)
          AND NOT EXISTS (SELECT 1 FROM clustering_article_state)
        """,
        (CLEAN_STATUS_CLEANED, DEDUPE_STATUS_UNIQUE),
    )

    _backfill_semantic_pair_constraints(conn)
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
    snapshot_path = None
    export_stats = stats.get("export")
    if status == "completed" and isinstance(export_stats, dict):
        snapshot_path = export_stats.get("snapshot_dir")
    if snapshot_path:
        cursor.execute(
            """
            UPDATE pipeline_runs
            SET is_latest_success = 0
            WHERE is_latest_success = 1
              AND id != ?
            """,
            (run_id,),
        )
    cursor.execute(
        """
        UPDATE pipeline_runs
        SET finished_at = ?,
            status = ?,
            stats_json = ?,
            note = ?,
            snapshot_path = ?,
            is_latest_success = ?
        WHERE id = ?
        """,
        (
            datetime.now().isoformat(timespec="seconds"),
            status,
            json.dumps(stats, ensure_ascii=False),
            note,
            snapshot_path,
            int(bool(snapshot_path)),
            run_id,
        ),
    )
    conn.commit()
    conn.close()


def get_latest_successful_snapshot() -> Optional[dict]:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, started_at, finished_at, snapshot_path
            FROM pipeline_runs
            WHERE is_latest_success = 1
              AND status = 'completed'
              AND snapshot_path IS NOT NULL
            """
        ).fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()
