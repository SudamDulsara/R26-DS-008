import hashlib
import json
import sqlite3
import time
from datetime import datetime
from urllib.parse import urlparse

from news_pipeline.config import load_config
from news_pipeline.statuses import (
    CLEAN_STATUS_PENDING,
    DEDUPE_STATUS_PENDING,
    URL_STATUS_DISCOVERED,
    URL_STATUS_EXHAUSTED,
    URL_STATUS_EXTRACTED,
    URL_STATUS_FETCH_FAILED,
    URL_STATUS_REJECTED,
)
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


sqlite3.register_adapter(datetime, lambda value: value.isoformat())

_scraper = None
logger = get_logger()


def _get_scraper():
    global _scraper
    if _scraper is None:
        try:
            import cloudscraper
        except ModuleNotFoundError:
            return None
        _scraper = cloudscraper.create_scraper()
    return _scraper


def extract_category_from_url(url, source):
    if source == "Dinamina":
        parts = [part for part in urlparse(url).path.split("/") if part]
        if len(parts) >= 4:
            return parts[3]
    return ""


def fetch_article(url):
    config = load_config()
    try:
        import requests
        import trafilatura
    except ModuleNotFoundError as exc:
        return {
            "ok": False,
            "error": f"Missing dependency: {exc.name}",
            "status_code": None,
            "html": None,
            "extracted_json": None,
        }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    for attempt in range(2):
        try:
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 403:
                scraper = _get_scraper()
                if scraper is not None:
                    logger.info("    HTTP 403 - retrying with cloudscraper...")
                    response = scraper.get(url, timeout=20)

            if response.status_code != 200:
                return {
                    "ok": False,
                    "error": f"HTTP {response.status_code}",
                    "status_code": response.status_code,
                    "html": None,
                    "extracted_json": None,
                }

            return {
                "ok": True,
                "error": None,
                "status_code": response.status_code,
                "html": response.text,
                "extracted_json": trafilatura.extract(
                    response.text,
                    output_format="json",
                ),
            }
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            if attempt == 0:
                logger.warning(
                    "    Network error (%s), retrying in %ss...",
                    exc.__class__.__name__,
                    config.retry_delay_seconds,
                )
                time.sleep(config.retry_delay_seconds)
            else:
                return {
                    "ok": False,
                    "error": f"Network error: {exc}",
                    "status_code": None,
                    "html": None,
                    "extracted_json": None,
                }
        except Exception as exc:
            return {
                "ok": False,
                "error": f"Unexpected error: {exc}",
                "status_code": None,
                "html": None,
                "extracted_json": None,
            }

    return {
        "ok": False,
        "error": "Unknown fetch error",
        "status_code": None,
        "html": None,
        "extracted_json": None,
    }


def _upsert_article(cursor, payload):
    cursor.execute(
        """
        INSERT INTO articles (
            url,
            source,
            title,
            author,
            published_date,
            category,
            raw_html,
            raw_text,
            content_hash,
            crawl_timestamp,
            clean_text,
            sinhala_purity,
            clean_hash,
            is_duplicate,
            duplicate_of_id,
            clean_status,
            dedupe_status,
            quality_flags,
            cleaned_at,
            exported_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0, NULL, ?, ?, '[]', NULL, NULL)
        ON CONFLICT(url) DO UPDATE SET
            source = excluded.source,
            title = excluded.title,
            author = excluded.author,
            published_date = excluded.published_date,
            category = excluded.category,
            raw_html = excluded.raw_html,
            raw_text = excluded.raw_text,
            content_hash = excluded.content_hash,
            crawl_timestamp = excluded.crawl_timestamp,
            clean_text = NULL,
            sinhala_purity = NULL,
            clean_hash = NULL,
            is_duplicate = 0,
            duplicate_of_id = NULL,
            clean_status = excluded.clean_status,
            dedupe_status = excluded.dedupe_status,
            quality_flags = '[]',
            cleaned_at = NULL,
            exported_at = NULL
        """,
        payload,
    )


def extract_articles():
    config = load_config()
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT id, url, source, fetch_attempts, rss_title, rss_published
        FROM discovered_urls
        WHERE status IN (?, ?)
          AND fetch_attempts < ?
        ORDER BY id
        """,
        (
            URL_STATUS_DISCOVERED,
            URL_STATUS_FETCH_FAILED,
            config.max_retries,
        ),
    )
    urls = cursor.fetchall()

    logger.info("=== Article Extraction Started ===")
    logger.info("Found %s pending URLs", len(urls))
    logger.info("")

    success = 0
    failed = 0
    rejected = 0

    try:
        for row in urls:
            url_id = row["id"]
            url = row["url"]
            source = row["source"]
            fetch_attempts = row["fetch_attempts"] or 0
            rss_title = row["rss_title"] or ""
            rss_published = row["rss_published"] or ""

            attempt_number = fetch_attempts + 1
            logger.info(
                "[%s] Fetching (attempt %s/%s): %s...",
                source,
                attempt_number,
                config.max_retries,
                url[:60],
            )

            fetch_result = fetch_article(url)
            now = datetime.now().isoformat(timespec="seconds")
            next_attempts = fetch_attempts + 1

            if not fetch_result["ok"]:
                failed += 1
                exhausted = next_attempts >= config.max_retries
                status = URL_STATUS_EXHAUSTED if exhausted else URL_STATUS_FETCH_FAILED
                cursor.execute(
                    """
                    UPDATE discovered_urls
                    SET status = ?,
                        fetch_attempts = ?,
                        last_error = ?,
                        last_attempted_at = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        next_attempts,
                        fetch_result["error"],
                        now,
                        url_id,
                    ),
                )
                if exhausted:
                    logger.warning("    Failed - %s (exhausted retries)", fetch_result["error"])
                else:
                    logger.warning("    Failed - %s", fetch_result["error"])
                continue

            extracted_json = fetch_result["extracted_json"]
            if not extracted_json:
                rejected += 1
                cursor.execute(
                    """
                    UPDATE discovered_urls
                    SET status = ?,
                        fetch_attempts = ?,
                        last_error = ?,
                        last_attempted_at = ?
                    WHERE id = ?
                    """,
                    (
                        URL_STATUS_REJECTED,
                        next_attempts,
                        "Extractor returned no article JSON",
                        now,
                        url_id,
                    ),
                )
                logger.warning("    Rejected - extractor returned no article JSON")
                continue

            try:
                data = json.loads(extracted_json)
            except json.JSONDecodeError:
                rejected += 1
                cursor.execute(
                    """
                    UPDATE discovered_urls
                    SET status = ?,
                        fetch_attempts = ?,
                        last_error = ?,
                        last_attempted_at = ?
                    WHERE id = ?
                    """,
                    (
                        URL_STATUS_REJECTED,
                        next_attempts,
                        "JSON parse error",
                        now,
                        url_id,
                    ),
                )
                logger.warning("    Rejected - JSON parse error")
                continue

            raw_text = (data.get("text") or "").strip()
            if len(raw_text) < config.min_article_length:
                rejected += 1
                cursor.execute(
                    """
                    UPDATE discovered_urls
                    SET status = ?,
                        fetch_attempts = ?,
                        last_error = ?,
                        last_attempted_at = ?
                    WHERE id = ?
                    """,
                    (
                        URL_STATUS_REJECTED,
                        next_attempts,
                        f"Article text too short (< {config.min_article_length} chars)",
                        now,
                        url_id,
                    ),
                )
                logger.warning("    Rejected - text too short")
                continue

            title = (data.get("title") or "").strip() or rss_title
            author = (data.get("author") or "").strip()
            published_date = (data.get("date") or "").strip() or rss_published
            category = extract_category_from_url(url, source)
            content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

            _upsert_article(
                cursor,
                (
                    url,
                    source,
                    title,
                    author,
                    published_date,
                    category,
                    fetch_result["html"],
                    raw_text,
                    content_hash,
                    now,
                    CLEAN_STATUS_PENDING,
                    DEDUPE_STATUS_PENDING,
                ),
            )

            cursor.execute(
                """
                UPDATE discovered_urls
                SET status = ?,
                    fetch_attempts = ?,
                    last_error = NULL,
                    last_attempted_at = ?,
                    fetched_at = ?,
                    fetched = 1
                WHERE id = ?
                """,
                (
                    URL_STATUS_EXTRACTED,
                    next_attempts,
                    now,
                    now,
                    url_id,
                ),
            )

            success += 1
            logger.info("    Saved - %s chars | Title: %s", len(raw_text), title[:60])

        connection.commit()
    finally:
        connection.close()

    logger.info("")
    logger.info("=== Extraction Complete ===")
    logger.info("Success: %s | Failed: %s | Rejected: %s", success, failed, rejected)

    return {
        "extracted_articles": success,
        "fetch_failures": failed,
        "rejected_articles": rejected,
    }


if __name__ == "__main__":
    extract_articles()
