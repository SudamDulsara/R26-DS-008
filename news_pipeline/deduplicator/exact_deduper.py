import hashlib
import re
import unicodedata

from news_pipeline.statuses import (
    CLEAN_STATUS_CLEANED,
    DEDUPE_STATUS_EXACT_DUPLICATE,
    DEDUPE_STATUS_PENDING,
    DEDUPE_STATUS_UNIQUE,
)
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


logger = get_logger()


def canonicalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFC", text or "")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def compute_clean_hash(text: str) -> str:
    canonical = canonicalize_text(text)
    if not canonical:
        return ""
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def run_exact_dedup():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, clean_text
        FROM articles
        WHERE clean_status = ?
        ORDER BY id
        """,
        (CLEAN_STATUS_CLEANED,),
    )
    articles = cursor.fetchall()

    seen_hashes: dict[str, int] = {}
    unique_count = 0
    duplicate_count = 0

    for row in articles:
        article_id = row["id"]
        clean_hash = compute_clean_hash(row["clean_text"])

        if not clean_hash:
            cursor.execute(
                """
                UPDATE articles
                SET clean_hash = NULL,
                    dedupe_status = ?,
                    is_duplicate = 0,
                    duplicate_of_id = NULL
                WHERE id = ?
                """,
                (DEDUPE_STATUS_PENDING, article_id),
            )
            continue

        if clean_hash in seen_hashes:
            duplicate_count += 1
            cursor.execute(
                """
                UPDATE articles
                SET clean_hash = ?,
                    dedupe_status = ?,
                    is_duplicate = 1,
                    duplicate_of_id = ?
                WHERE id = ?
                """,
                (
                    clean_hash,
                    DEDUPE_STATUS_EXACT_DUPLICATE,
                    seen_hashes[clean_hash],
                    article_id,
                ),
            )
        else:
            seen_hashes[clean_hash] = article_id
            unique_count += 1
            cursor.execute(
                """
                UPDATE articles
                SET clean_hash = ?,
                    dedupe_status = ?,
                    is_duplicate = 0,
                    duplicate_of_id = NULL
                WHERE id = ?
                """,
                (clean_hash, DEDUPE_STATUS_UNIQUE, article_id),
            )

    conn.commit()
    conn.close()

    logger.info("=== Exact Deduplication Complete ===")
    logger.info("Unique articles: %s | Exact duplicates: %s", unique_count, duplicate_count)

    return {
        "unique_articles": unique_count,
        "exact_duplicates": duplicate_count,
    }
