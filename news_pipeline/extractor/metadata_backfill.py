import json
from datetime import datetime

from news_pipeline.extractor.metadata_extractor import (
    compute_text_hash,
    extract_metadata,
    final_metadata_flags,
)
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


logger = get_logger()


def run_metadata_backfill(overwrite: bool = False):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            articles.id,
            articles.url,
            articles.source,
            articles.title,
            articles.title_source,
            articles.author,
            articles.author_source,
            articles.published_date,
            articles.published_date_source,
            articles.category,
            articles.category_source,
            articles.raw_html,
            articles.raw_text,
            articles.content_hash,
            articles.metadata_flags,
            discovered_urls.rss_title,
            discovered_urls.rss_published
        FROM articles
        LEFT JOIN discovered_urls ON discovered_urls.url = articles.url
        ORDER BY articles.id
        """
    )
    rows = cursor.fetchall()

    stats = {
        "articles_checked": len(rows),
        "titles_filled": 0,
        "dates_filled": 0,
        "authors_filled": 0,
        "categories_filled": 0,
        "content_hashes_filled": 0,
        "metadata_rows_updated": 0,
    }

    for row in rows:
        current = dict(row)
        raw_text = current.get("raw_text") or ""
        content_hash = current.get("content_hash") or compute_text_hash(raw_text)
        extracted = extract_metadata(
            url=current["url"],
            source=current["source"],
            trafilatura_data={},
            html=current.get("raw_html") or "",
            rss_title=current.get("rss_title") or "",
            rss_published=current.get("rss_published") or "",
        )

        updated = _merge_metadata(current, extracted, content_hash, overwrite)
        metadata_flags = final_metadata_flags(
            updated["title"],
            updated["published_date"],
            updated["content_hash"],
            extracted.metadata_flags,
        )
        updated["metadata_flags"] = json.dumps(metadata_flags, ensure_ascii=False)

        if _row_changed(current, updated):
            _increment_stats(stats, current, updated)
            cursor.execute(
                """
                UPDATE articles
                SET title = ?,
                    title_source = ?,
                    author = ?,
                    author_source = ?,
                    published_date = ?,
                    published_date_source = ?,
                    category = ?,
                    category_source = ?,
                    content_hash = ?,
                    metadata_flags = ?
                WHERE id = ?
                """,
                (
                    updated["title"],
                    updated["title_source"],
                    updated["author"],
                    updated["author_source"],
                    updated["published_date"],
                    updated["published_date_source"],
                    updated["category"],
                    updated["category_source"],
                    updated["content_hash"],
                    updated["metadata_flags"],
                    current["id"],
                ),
            )

    conn.commit()
    conn.close()

    logger.info("=== Metadata Backfill Complete ===")
    logger.info("Articles checked: %s", stats["articles_checked"])
    logger.info("Titles filled: %s", stats["titles_filled"])
    logger.info("Dates filled: %s", stats["dates_filled"])
    logger.info("Authors filled: %s", stats["authors_filled"])
    logger.info("Categories filled: %s", stats["categories_filled"])
    logger.info("Content hashes filled: %s", stats["content_hashes_filled"])
    logger.info("Rows updated: %s", stats["metadata_rows_updated"])

    return stats | {"completed_at": datetime.now().isoformat(timespec="seconds")}


def _merge_metadata(current: dict, extracted, content_hash: str, overwrite: bool):
    return {
        "title": _choose_value(current.get("title"), extracted.title, overwrite),
        "title_source": _choose_source(
            current.get("title"),
            current.get("title_source"),
            extracted.title_source,
            overwrite,
        ),
        "author": _choose_value(current.get("author"), extracted.author, overwrite),
        "author_source": _choose_source(
            current.get("author"),
            current.get("author_source"),
            extracted.author_source,
            overwrite,
        ),
        "published_date": _choose_value(
            current.get("published_date"),
            extracted.published_date,
            overwrite,
        ),
        "published_date_source": _choose_source(
            current.get("published_date"),
            current.get("published_date_source"),
            extracted.published_date_source,
            overwrite,
        ),
        "category": _choose_value(current.get("category"), extracted.category, overwrite),
        "category_source": _choose_source(
            current.get("category"),
            current.get("category_source"),
            extracted.category_source,
            overwrite,
        ),
        "content_hash": _choose_value(
            current.get("content_hash"),
            content_hash,
            overwrite,
        ),
    }


def _choose_value(current_value, extracted_value, overwrite: bool) -> str:
    current_text = (current_value or "").strip()
    extracted_text = (extracted_value or "").strip()
    if overwrite and extracted_text:
        return extracted_text
    return current_text or extracted_text


def _choose_source(current_value, current_source, extracted_source, overwrite: bool) -> str:
    current_text = (current_value or "").strip()
    current_source_text = (current_source or "").strip()
    extracted_source_text = (extracted_source or "").strip()

    if overwrite and extracted_source_text:
        return extracted_source_text
    if current_text and current_source_text:
        return current_source_text
    if current_text:
        return "existing"
    return extracted_source_text


def _row_changed(current: dict, updated: dict) -> bool:
    for key, value in updated.items():
        if (current.get(key) or "") != (value or ""):
            return True
    return False


def _increment_stats(stats: dict, current: dict, updated: dict):
    stats["metadata_rows_updated"] += 1
    if not (current.get("title") or "").strip() and updated.get("title"):
        stats["titles_filled"] += 1
    if not (current.get("published_date") or "").strip() and updated.get("published_date"):
        stats["dates_filled"] += 1
    if not (current.get("author") or "").strip() and updated.get("author"):
        stats["authors_filled"] += 1
    if not (current.get("category") or "").strip() and updated.get("category"):
        stats["categories_filled"] += 1
    if not (current.get("content_hash") or "").strip() and updated.get("content_hash"):
        stats["content_hashes_filled"] += 1
