import json
from datetime import datetime

from news_pipeline.config import load_config
from news_pipeline.statuses import CLEAN_STATUS_CLEANED, DEDUPE_STATUS_UNIQUE
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


logger = get_logger()


def _write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def export_snapshot():
    config = load_config()
    config.snapshots_dir.mkdir(parents=True, exist_ok=True)

    snapshot_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_dir = config.snapshots_dir / snapshot_name
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            id,
            url,
            source,
            title,
            title_source,
            author,
            author_source,
            published_date,
            published_date_source,
            category,
            category_source,
            clean_text,
            raw_text,
            sinhala_purity,
            content_hash,
            clean_hash,
            metadata_flags,
            crawl_timestamp
        FROM articles
        WHERE clean_status = ?
          AND dedupe_status = ?
        ORDER BY id
        """,
        (CLEAN_STATUS_CLEANED, DEDUPE_STATUS_UNIQUE),
    )
    rows = cursor.fetchall()

    fulltext_rows = []
    metadata_rows = []
    exported_ids = []

    for row in rows:
        record = dict(row)
        article_id = record.pop("id")
        raw_text = record.pop("raw_text", "")
        clean_text = record.get("clean_text", "")

        fulltext_rows.append(record)
        metadata_rows.append(
            {
                key: value
                for key, value in record.items()
                if key not in {"clean_text"}
            }
            | {
                "clean_text_length": len(clean_text or ""),
                "raw_text_length": len(raw_text or ""),
            }
        )
        exported_ids.append(article_id)

    generated_at = datetime.now().isoformat(timespec="seconds")

    cursor.execute("SELECT COUNT(*) AS count FROM discovered_urls")
    total_urls = cursor.fetchone()["count"]
    cursor.execute("SELECT status, COUNT(*) AS count FROM discovered_urls GROUP BY status")
    url_status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}

    cursor.execute("SELECT COUNT(*) AS count FROM articles")
    total_articles = cursor.fetchone()["count"]
    cursor.execute(
        "SELECT clean_status, COUNT(*) AS count FROM articles GROUP BY clean_status"
    )
    clean_status_counts = {
        row["clean_status"]: row["count"] for row in cursor.fetchall()
    }
    cursor.execute(
        "SELECT dedupe_status, COUNT(*) AS count FROM articles GROUP BY dedupe_status"
    )
    dedupe_status_counts = {
        row["dedupe_status"]: row["count"] for row in cursor.fetchall()
    }
    cursor.execute("SELECT source, COUNT(*) AS count FROM articles GROUP BY source")
    articles_by_source = {row["source"]: row["count"] for row in cursor.fetchall()}

    cursor.execute(
        """
        SELECT
            SUM(CASE WHEN title IS NULL OR TRIM(title) = '' THEN 1 ELSE 0 END)
                AS missing_title,
            SUM(CASE WHEN published_date IS NULL OR TRIM(published_date) = '' THEN 1 ELSE 0 END)
                AS missing_published_date,
            SUM(CASE WHEN content_hash IS NULL OR TRIM(content_hash) = '' THEN 1 ELSE 0 END)
                AS missing_content_hash
        FROM articles
        """
    )
    metadata_missing_counts = dict(cursor.fetchone())
    cursor.execute(
        """
        SELECT COALESCE(NULLIF(title_source, ''), 'unknown') AS source, COUNT(*) AS count
        FROM articles
        GROUP BY COALESCE(NULLIF(title_source, ''), 'unknown')
        """
    )
    title_source_counts = {row["source"]: row["count"] for row in cursor.fetchall()}
    cursor.execute(
        """
        SELECT COALESCE(NULLIF(published_date_source, ''), 'unknown') AS source,
               COUNT(*) AS count
        FROM articles
        GROUP BY COALESCE(NULLIF(published_date_source, ''), 'unknown')
        """
    )
    published_date_source_counts = {
        row["source"]: row["count"] for row in cursor.fetchall()
    }

    cursor.execute(
        """
        SELECT
            id,
            cluster_key,
            representative_article_id,
            model_name,
            model_revision,
            text_variant,
            similarity_threshold,
            representative_threshold,
            cohesion_threshold,
            event_date_start,
            event_date_end,
            article_count,
            source_count,
            confidence,
            created_at
        FROM story_clusters
        ORDER BY id
        """
    )
    cluster_rows = [dict(row) for row in cursor.fetchall()]

    cursor.execute(
        """
        SELECT
            members.cluster_id,
            clusters.cluster_key,
            members.article_id,
            members.similarity_score,
            members.is_representative,
            articles.url,
            articles.source,
            articles.title,
            articles.published_date
        FROM story_cluster_members AS members
        JOIN story_clusters AS clusters ON clusters.id = members.cluster_id
        JOIN articles ON articles.id = members.article_id
        ORDER BY members.cluster_id, members.is_representative DESC, members.article_id
        """
    )
    cluster_member_rows = [dict(row) for row in cursor.fetchall()]

    if exported_ids:
        cursor.executemany(
            "UPDATE articles SET exported_at = ? WHERE id = ?",
            [(generated_at, article_id) for article_id in exported_ids],
        )

    conn.commit()
    conn.close()

    fulltext_path = snapshot_dir / "dataset_fulltext.jsonl"
    metadata_path = snapshot_dir / "dataset_metadata.jsonl"
    clusters_path = snapshot_dir / "story_clusters.jsonl"
    cluster_members_path = snapshot_dir / "story_cluster_members.jsonl"
    report_path = snapshot_dir / "report.json"

    _write_jsonl(fulltext_path, fulltext_rows)
    _write_jsonl(metadata_path, metadata_rows)
    _write_jsonl(clusters_path, cluster_rows)
    _write_jsonl(cluster_members_path, cluster_member_rows)

    report = {
        "generated_at": generated_at,
        "snapshot_name": snapshot_name,
        "paths": {
            "fulltext": str(fulltext_path),
            "metadata": str(metadata_path),
            "story_clusters": str(clusters_path),
            "story_cluster_members": str(cluster_members_path),
        },
        "counts": {
            "discovered_urls": total_urls,
            "articles_total": total_articles,
            "exported_unique_articles": len(fulltext_rows),
            "story_clusters": len(cluster_rows),
            "story_cluster_members": len(cluster_member_rows),
        },
        "url_status_counts": url_status_counts,
        "clean_status_counts": clean_status_counts,
        "dedupe_status_counts": dedupe_status_counts,
        "articles_by_source": articles_by_source,
        "metadata_missing_counts": metadata_missing_counts,
        "title_source_counts": title_source_counts,
        "published_date_source_counts": published_date_source_counts,
    }

    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    logger.info("=== Snapshot Export Complete ===")
    logger.info("Exported %s unique cleaned articles", len(fulltext_rows))
    logger.info("Exported %s story clusters", len(cluster_rows))
    logger.info("Snapshot directory: %s", snapshot_dir)

    return {
        "snapshot_dir": str(snapshot_dir),
        "fulltext_path": str(fulltext_path),
        "metadata_path": str(metadata_path),
        "clusters_path": str(clusters_path),
        "cluster_members_path": str(cluster_members_path),
        "report_path": str(report_path),
        "exported_unique_articles": len(fulltext_rows),
        "story_clusters": len(cluster_rows),
    }
