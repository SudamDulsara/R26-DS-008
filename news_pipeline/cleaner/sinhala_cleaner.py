import json
import re
import sqlite3
import unicodedata
from datetime import datetime
from urllib.parse import urlsplit

from news_pipeline.config import load_config
from news_pipeline.statuses import (
    CLEAN_STATUS_CLEANED,
    CLEAN_STATUS_PENDING,
    CLEAN_STATUS_QUALITY_QUARANTINE,
    CLEAN_STATUS_RETRYABLE_EXTRACTION,
    CLEAN_STATUS_UNSUPPORTED_MEDIA,
    URL_ERROR_ARTICLE_BODY_INCOMPLETE,
    URL_STATUS_EXHAUSTED,
    URL_STATUS_FETCH_FAILED,
)
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


sqlite3.register_adapter(datetime, lambda value: value.isoformat())

logger = get_logger()

SINHALA_START = "\u0D80"
SINHALA_END = "\u0DFF"
NAVIGATION_MARKERS = frozenset(
    {
        "toggle navigation",
        "home",
        "archive",
        "contact us",
        "web gossip",
        "cartoon",
        "most viewed stories",
        "most viewed video stories",
    }
)


def normalize_unicode(text):
    return unicodedata.normalize("NFC", text)


def remove_noise(text):
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"\S+@\S+", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = "".join(char for char in text if char.isprintable() or char == "\n")
    return text.strip()


def calculate_sinhala_purity(text):
    if not text:
        return 0.0

    sinhala_chars = [char for char in text if SINHALA_START <= char <= SINHALA_END]
    total_chars = [char for char in text if not char.isspace()]

    if not total_chars:
        return 0.0

    return len(sinhala_chars) / len(total_chars)


def clean_article(raw_text):
    if not raw_text:
        return "", 0.0

    text = normalize_unicode(raw_text)
    text = remove_noise(text)
    purity = calculate_sinhala_purity(text)
    return text, purity


def assess_article_quality(
    *,
    text: str,
    title: str,
    purity: float,
    purity_threshold: float,
    min_article_length: int,
    url: str = "",
) -> tuple[str, list[str]]:
    """Classify structure and content; language purity is advisory only."""
    flags: list[str] = []
    if is_structurally_unsupported_media(url):
        return CLEAN_STATUS_UNSUPPORTED_MEDIA, [
            "unsupported_image_only_media"
        ]
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    lowered_lines = {line.lower() for line in lines}
    marker_count = len(lowered_lines & NAVIGATION_MARKERS)
    sinhala_letters = sum(SINHALA_START <= char <= SINHALA_END for char in text)
    body_without_title = text
    if title:
        body_without_title = body_without_title.replace(title.strip(), "", 1).strip()
    body_sinhala_letters = sum(
        SINHALA_START <= char <= SINHALA_END for char in body_without_title
    )
    sentence_marks = sum(text.count(mark) for mark in (".", "?", "!", "।"))

    if purity < purity_threshold:
        flags.append("low_sinhala_purity_advisory")
    if "january 1, 1970" in text.lower():
        flags.append("invalid_default_date")
    if marker_count >= 3:
        flags.append("navigation_dominated")
    if len(text) < min_article_length:
        flags.append("article_too_short")
    if sinhala_letters < 35 or body_sinhala_letters < 25:
        flags.append("insufficient_sinhala_article_body")
    if marker_count >= 3 and sentence_marks == 0:
        flags.append("no_article_sentences")

    retryable_flags = {
        "invalid_default_date",
        "navigation_dominated",
        "article_too_short",
        "no_article_sentences",
    }
    if retryable_flags & set(flags):
        return CLEAN_STATUS_RETRYABLE_EXTRACTION, flags
    if "insufficient_sinhala_article_body" in flags:
        return CLEAN_STATUS_QUALITY_QUARANTINE, flags
    return CLEAN_STATUS_CLEANED, flags


def is_structurally_unsupported_media(url: str) -> bool:
    """Identify publisher routes whose payload is a visual, not an article."""
    path_segments = {
        segment.lower()
        for segment in urlsplit(url or "").path.split("/")
        if segment
    }
    return bool(path_segments & {"cartoon", "cartoons"})


def run_cleaner(*, article_ids: set[int] | None = None):
    config = load_config()
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) AS count FROM articles")
    logger.info("Total articles in DB: %s", cursor.fetchone()["count"])

    if article_ids:
        placeholders = ",".join("?" for _ in article_ids)
        cursor.execute(
            f"""
            SELECT id, source, title, raw_text, url
            FROM articles
            WHERE id IN ({placeholders})
            ORDER BY id
            """,
            tuple(sorted(article_ids)),
        )
    else:
        cursor.execute(
            """
            SELECT id, source, title, raw_text, url
            FROM articles
            WHERE clean_status = ?
               OR (
                    clean_status != ?
                    AND (
                        LOWER(url) LIKE '%/cartoon/%'
                        OR LOWER(url) LIKE '%/cartoons/%'
                    )
               )
            ORDER BY id
            """,
            (CLEAN_STATUS_PENDING, CLEAN_STATUS_UNSUPPORTED_MEDIA),
        )
    articles = cursor.fetchall()

    logger.info("=== Sinhala Cleaner Started ===")
    logger.info("Found %s articles to clean", len(articles))
    logger.info("")

    passed = 0
    failed = 0
    retryable = 0
    quarantined = 0
    unsupported_media = 0
    failures_by_source = {}
    skips_by_source = {}

    try:
        for row in articles:
            article_id = row["id"]
            source = row["source"]
            title = row["title"] or ""
            raw_text = row["raw_text"]

            clean_text, purity = clean_article(raw_text)
            purity_percent = round(purity * 100, 2)
            cleaned_at = datetime.now().isoformat(timespec="seconds")

            status, quality_flags = assess_article_quality(
                text=clean_text,
                title=title,
                purity=purity,
                purity_threshold=config.purity_threshold,
                min_article_length=config.min_article_length,
                url=row["url"],
            )
            if status == CLEAN_STATUS_UNSUPPORTED_MEDIA:
                cursor.execute(
                    """
                    UPDATE articles
                    SET clean_text = ?, sinhala_purity = ?, clean_status = ?,
                        quality_flags = ?, cleaned_at = ?
                    WHERE id = ?
                    """,
                    (
                        clean_text,
                        purity,
                        CLEAN_STATUS_UNSUPPORTED_MEDIA,
                        json.dumps(quality_flags, ensure_ascii=False),
                        cleaned_at,
                        article_id,
                    ),
                )
                logger.info("[%s] UNSUPPORTED MEDIA - image-only route", source)
                unsupported_media += 1
                source_skips = skips_by_source.setdefault(source, {})
                source_skips["unsupported_image_only_media"] = (
                    source_skips.get("unsupported_image_only_media", 0) + 1
                )
            elif status == CLEAN_STATUS_CLEANED:
                cursor.execute(
                    """
                    UPDATE articles
                    SET clean_text = ?,
                        sinhala_purity = ?,
                        clean_status = ?,
                        quality_flags = ?,
                        cleaned_at = ?
                    WHERE id = ?
                    """,
                    (
                        clean_text,
                        purity,
                        CLEAN_STATUS_CLEANED,
                        json.dumps(quality_flags, ensure_ascii=False),
                        cleaned_at,
                        article_id,
                    ),
                )
                logger.info(
                    "[%s] PASSED - Purity: %s%%%s",
                    source,
                    purity_percent,
                    " (advisory warning retained)" if quality_flags else "",
                )
                passed += 1
            elif status == CLEAN_STATUS_RETRYABLE_EXTRACTION:
                cursor.execute(
                    """
                    UPDATE articles
                    SET clean_text = ?,
                        sinhala_purity = ?,
                        clean_status = ?,
                        quality_flags = ?,
                        cleaned_at = ?
                    WHERE id = ?
                    """,
                    (
                        clean_text,
                        purity,
                        CLEAN_STATUS_RETRYABLE_EXTRACTION,
                        json.dumps(quality_flags, ensure_ascii=False),
                        cleaned_at,
                        article_id,
                    ),
                )
                discovered = cursor.execute(
                    "SELECT id, fetch_attempts FROM discovered_urls WHERE url = (SELECT url FROM articles WHERE id = ?)",
                    (article_id,),
                ).fetchone()
                if discovered is not None:
                    attempts = int(discovered["fetch_attempts"] or 0)
                    next_status = (
                        URL_STATUS_EXHAUSTED
                        if attempts >= config.max_retries
                        else URL_STATUS_FETCH_FAILED
                    )
                    cursor.execute(
                        """
                        UPDATE discovered_urls
                        SET status = ?, fetched = 0,
                            last_error = ?, last_error_code = ?
                        WHERE id = ?
                        """,
                        (
                            next_status,
                            "Cleaner detected an incomplete article body",
                            URL_ERROR_ARTICLE_BODY_INCOMPLETE,
                            discovered["id"],
                        ),
                    )
                logger.warning(
                    "[%s] RETRYABLE EXTRACTION - %s",
                    source,
                    ", ".join(quality_flags),
                )
                failed += 1
                retryable += 1
                source_failures = failures_by_source.setdefault(source, {})
                source_failures["article_body_incomplete"] = (
                    source_failures.get("article_body_incomplete", 0) + 1
                )
            else:
                cursor.execute(
                    """
                    UPDATE articles
                    SET clean_text = ?, sinhala_purity = ?, clean_status = ?,
                        quality_flags = ?, cleaned_at = ?
                    WHERE id = ?
                    """,
                    (
                        clean_text,
                        purity,
                        CLEAN_STATUS_QUALITY_QUARANTINE,
                        json.dumps(quality_flags, ensure_ascii=False),
                        cleaned_at,
                        article_id,
                    ),
                )
                logger.warning(
                    "[%s] QUALITY QUARANTINE - %s",
                    source,
                    ", ".join(quality_flags),
                )
                failed += 1
                quarantined += 1
                source_failures = failures_by_source.setdefault(source, {})
                source_failures["quality_quarantine"] = (
                    source_failures.get("quality_quarantine", 0) + 1
                )

        connection.commit()
    finally:
        connection.close()

    logger.info("")
    logger.info("=== Cleaning Complete ===")
    if passed + failed + unsupported_media > 0:
        logger.info(
            "Passed: %s | Failed: %s | Unsupported media: %s",
            passed,
            failed,
            unsupported_media,
        )
        if passed + failed > 0:
            logger.info(
                "Overall article quality acceptance rate: %s%%",
                round((passed / (passed + failed)) * 100, 1),
            )
    else:
        logger.info("No articles to process.")

    return {
        "cleaned_articles": passed,
        "rejected_articles": failed,
        "retryable_extraction_articles": retryable,
        "quality_quarantined_articles": quarantined,
        "unsupported_media_articles": unsupported_media,
        "failures_by_source": failures_by_source,
        "skips_by_source": skips_by_source,
    }


def run_cleaner_with_recovery(*, fresh_discovered_at: str | None = None):
    """Feed structural cleaner failures back into extraction in this run."""
    initial = run_cleaner()
    if initial["retryable_extraction_articles"] == 0:
        return initial
    from news_pipeline.extractor.article_extractor import (
        extract_articles_with_recovery,
    )

    extraction = extract_articles_with_recovery(
        fresh_discovered_at=fresh_discovered_at,
    )
    followup = run_cleaner()
    remaining_extraction_failures = int(extraction["fetch_failures"])
    failures_by_source = {}
    skips_by_source = {}
    for result in (initial, extraction, followup):
        for source, reason_counts in result.get("failures_by_source", {}).items():
            combined = failures_by_source.setdefault(source, {})
            for reason, count in reason_counts.items():
                combined[reason] = combined.get(reason, 0) + int(count)
        for source, reason_counts in result.get("skips_by_source", {}).items():
            combined = skips_by_source.setdefault(source, {})
            for reason, count in reason_counts.items():
                combined[reason] = combined.get(reason, 0) + int(count)
    return {
        "cleaned_articles": initial["cleaned_articles"] + followup["cleaned_articles"],
        "rejected_articles": (
            initial["quality_quarantined_articles"]
            + remaining_extraction_failures
            + followup["rejected_articles"]
        ),
        "retryable_extraction_articles": (
            remaining_extraction_failures
            + followup["retryable_extraction_articles"]
        ),
        "quality_quarantined_articles": (
            initial["quality_quarantined_articles"]
            + followup["quality_quarantined_articles"]
        ),
        "unsupported_media_articles": (
            initial.get("unsupported_media_articles", 0)
            + followup.get("unsupported_media_articles", 0)
        ),
        "same_run_extraction_recovery": extraction,
        "failures_by_source": failures_by_source,
        "skips_by_source": skips_by_source,
    }


if __name__ == "__main__":
    run_cleaner()
