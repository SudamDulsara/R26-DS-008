import hashlib
import re
import unicodedata
from time import perf_counter

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


def run_exact_dedup(
    *,
    update_unchanged: bool = False,
    reuse_stored_hashes: bool = True,
):
    started = perf_counter()
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, source, clean_hash, dedupe_status,
               is_duplicate, duplicate_of_id
        FROM articles INDEXED BY idx_articles_dedup_scan
        WHERE clean_status = ?
        ORDER BY id
        """,
        (CLEAN_STATUS_CLEANED,),
    )
    articles = cursor.fetchall()
    article_ids_to_hash = [
        int(row["id"])
        for row in articles
        if (
            not reuse_stored_hashes
            or not row["clean_hash"]
            or row["dedupe_status"]
            not in {DEDUPE_STATUS_UNIQUE, DEDUPE_STATUS_EXACT_DUPLICATE}
        )
    ]
    clean_text_by_id = {}
    if article_ids_to_hash:
        for offset in range(0, len(article_ids_to_hash), 500):
            batch = article_ids_to_hash[offset : offset + 500]
            placeholders = ",".join("?" for _ in batch)
            clean_text_by_id.update(
                {
                    int(row["id"]): row["clean_text"]
                    for row in cursor.execute(
                        f"""
                        SELECT id, clean_text
                        FROM articles
                        WHERE id IN ({placeholders})
                        """,
                        batch,
                    )
                }
            )

    seen_hashes: dict[str, int] = {}
    unique_count = 0
    duplicate_count = 0
    unhashable_count = 0
    skips_by_source = {}
    updates = []
    reused_hashes = 0
    computed_hashes = 0
    hashing_started = perf_counter()

    for row in articles:
        article_id = row["id"]
        can_reuse_hash = bool(
            reuse_stored_hashes
            and row["clean_hash"]
            and row["dedupe_status"]
            in {DEDUPE_STATUS_UNIQUE, DEDUPE_STATUS_EXACT_DUPLICATE}
        )
        if can_reuse_hash:
            clean_hash = str(row["clean_hash"])
            reused_hashes += 1
        else:
            clean_hash = compute_clean_hash(clean_text_by_id.get(article_id))
            computed_hashes += 1

        if not clean_hash:
            unhashable_count += 1
            source_skips = skips_by_source.setdefault(
                row["source"] or "unknown",
                {},
            )
            source_skips["unhashable_article"] = (
                source_skips.get("unhashable_article", 0) + 1
            )
            expected = (None, DEDUPE_STATUS_PENDING, 0, None)
        elif clean_hash in seen_hashes:
            duplicate_count += 1
            source_skips = skips_by_source.setdefault(
                row["source"] or "unknown",
                {},
            )
            source_skips["exact_duplicate"] = (
                source_skips.get("exact_duplicate", 0) + 1
            )
            expected = (
                clean_hash,
                DEDUPE_STATUS_EXACT_DUPLICATE,
                1,
                seen_hashes[clean_hash],
            )
        else:
            seen_hashes[clean_hash] = article_id
            unique_count += 1
            expected = (
                clean_hash,
                DEDUPE_STATUS_UNIQUE,
                0,
                None,
            )

        current = (
            row["clean_hash"],
            row["dedupe_status"],
            int(row["is_duplicate"] or 0),
            row["duplicate_of_id"],
        )
        if update_unchanged or current != expected:
            updates.append((*expected, article_id))

    hashing_seconds = perf_counter() - hashing_started
    write_started = perf_counter()
    cursor.executemany(
        """
        UPDATE articles
        SET clean_hash = ?,
            dedupe_status = ?,
            is_duplicate = ?,
            duplicate_of_id = ?
        WHERE id = ?
        """,
        updates,
    )
    conn.commit()
    conn.close()
    write_seconds = perf_counter() - write_started
    total_seconds = perf_counter() - started

    logger.info("=== Exact Deduplication Complete ===")
    logger.info("Unique articles: %s | Exact duplicates: %s", unique_count, duplicate_count)
    logger.info(
        "Deduplication writes: %s changed | %s unchanged in %.3fs",
        len(updates),
        len(articles) - len(updates),
        total_seconds,
    )

    return {
        "unique_articles": unique_count,
        "exact_duplicates": duplicate_count,
        "unhashable_articles": unhashable_count,
        "scanned_articles": len(articles),
        "reused_hashes": reused_hashes,
        "computed_hashes": computed_hashes,
        "updated_articles": len(updates),
        "unchanged_articles": len(articles) - len(updates),
        "hashing_seconds": round(hashing_seconds, 6),
        "write_seconds": round(write_seconds, 6),
        "total_seconds": round(total_seconds, 6),
        "skips_by_source": skips_by_source,
    }
