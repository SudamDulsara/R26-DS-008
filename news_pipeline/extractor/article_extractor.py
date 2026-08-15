import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from news_pipeline.config import load_config
from news_pipeline.extractor.metadata_extractor import (
    compute_text_hash,
    extract_category_from_url as metadata_category_from_url,
    extract_metadata,
    final_metadata_flags,
)
from news_pipeline.statuses import (
    CLEAN_STATUS_PENDING,
    DEDUPE_STATUS_PENDING,
    URL_ERROR_ARTICLE_TOO_SHORT,
    URL_ERROR_EMPTY_EXTRACTION,
    URL_ERROR_HTTP_RETRYABLE,
    URL_ERROR_HTTP_TERMINAL,
    URL_ERROR_INVALID_EXTRACTION_JSON,
    URL_ERROR_MISSING_DEPENDENCY,
    URL_ERROR_NETWORK,
    URL_ERROR_UNEXPECTED,
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

RETRYABLE_HTTP_STATUSES = frozenset(
    {408, 425, 429, 500, 502, 503, 504}
)


def classify_http_failure(status_code: int) -> tuple[str, bool]:
    if status_code in RETRYABLE_HTTP_STATUSES:
        return URL_ERROR_HTTP_RETRYABLE, True
    return URL_ERROR_HTTP_TERMINAL, False


def failed_attempt_status(
    *,
    retryable: bool,
    completed_attempts: int,
    max_retries: int,
) -> str:
    if completed_attempts <= 0:
        raise ValueError("completed_attempts must be positive")
    if max_retries <= 0:
        raise ValueError("max_retries must be positive")
    if not retryable:
        return URL_STATUS_REJECTED
    if completed_attempts >= max_retries:
        return URL_STATUS_EXHAUSTED
    return URL_STATUS_FETCH_FAILED


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
    return metadata_category_from_url(url, source)


def fetch_article(url):
    config = load_config()
    try:
        import requests
        import trafilatura
    except ModuleNotFoundError as exc:
        return {
            "ok": False,
            "error": f"Missing dependency: {exc.name}",
            "error_code": URL_ERROR_MISSING_DEPENDENCY,
            "retryable": False,
            "fatal": True,
            "status_code": None,
            "request_attempts": 0,
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

    request_attempts = 0
    for attempt in range(2):
        try:
            request_attempts += 1
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 403:
                scraper = _get_scraper()
                if scraper is not None:
                    logger.info("    HTTP 403 - retrying with cloudscraper...")
                    request_attempts += 1
                    response = scraper.get(url, timeout=20)

            if response.status_code != 200:
                error_code, retryable = classify_http_failure(
                    response.status_code
                )
                return {
                    "ok": False,
                    "error": f"HTTP {response.status_code}",
                    "error_code": error_code,
                    "retryable": retryable,
                    "fatal": False,
                    "status_code": response.status_code,
                    "request_attempts": request_attempts,
                    "html": None,
                    "extracted_json": None,
                }

            return {
                "ok": True,
                "error": None,
                "error_code": None,
                "retryable": False,
                "fatal": False,
                "status_code": response.status_code,
                "request_attempts": request_attempts,
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
                    "error_code": URL_ERROR_NETWORK,
                    "retryable": True,
                    "fatal": False,
                    "status_code": None,
                    "request_attempts": request_attempts,
                    "html": None,
                    "extracted_json": None,
                }
        except Exception as exc:
            return {
                "ok": False,
                "error": f"Unexpected error: {exc}",
                "error_code": URL_ERROR_UNEXPECTED,
                "retryable": True,
                "fatal": False,
                "status_code": None,
                "request_attempts": request_attempts,
                "html": None,
                "extracted_json": None,
            }

    return {
        "ok": False,
        "error": "Unknown fetch error",
        "error_code": URL_ERROR_UNEXPECTED,
        "retryable": True,
        "fatal": False,
        "status_code": None,
        "request_attempts": request_attempts,
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
            title_source,
            author,
            author_source,
            published_date,
            published_date_source,
            category,
            category_source,
            raw_html,
            raw_text,
            content_hash,
            metadata_flags,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0, NULL, ?, ?, '[]', NULL, NULL)
        ON CONFLICT(url) DO UPDATE SET
            source = excluded.source,
            title = excluded.title,
            title_source = excluded.title_source,
            author = excluded.author,
            author_source = excluded.author_source,
            published_date = excluded.published_date,
            published_date_source = excluded.published_date_source,
            category = excluded.category,
            category_source = excluded.category_source,
            raw_html = excluded.raw_html,
            raw_text = excluded.raw_text,
            content_hash = excluded.content_hash,
            metadata_flags = excluded.metadata_flags,
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


def extract_articles(
    *,
    limit: int | None = None,
    min_id: int | None = None,
    workers: int = 1,
    commit_every: int = 25,
):
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")
    if min_id is not None and min_id <= 0:
        raise ValueError("min_id must be positive when provided")
    if workers <= 0:
        raise ValueError("workers must be positive")
    if commit_every <= 0:
        raise ValueError("commit_every must be positive")

    config = load_config()
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT id, url, source, fetch_attempts, rss_title, rss_published
        FROM discovered_urls
        WHERE status IN (?, ?)
          AND fetch_attempts < ?
          AND (? IS NULL OR id >= ?)
        ORDER BY id
        """,
        (
            URL_STATUS_DISCOVERED,
            URL_STATUS_FETCH_FAILED,
            config.max_retries,
            min_id,
            min_id,
        ),
    )
    urls = cursor.fetchall()
    if limit is not None:
        urls = urls[:limit]

    logger.info("=== Article Extraction Started ===")
    logger.info("Found %s pending URLs", len(urls))
    logger.info(
        "Fetch workers: %s | Commit interval: %s",
        workers,
        commit_every,
    )
    logger.info("")

    success = 0
    failed = 0
    rejected = 0
    failures_by_source = {}

    def record_source_failure(source_name, reason):
        source_failures = failures_by_source.setdefault(source_name, {})
        source_failures[reason] = source_failures.get(reason, 0) + 1

    executor = None
    try:
        if workers == 1:
            work_items = (
                (row, None)
                for row in urls
            )
        else:
            executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="article-fetch",
            )
            futures = [
                executor.submit(fetch_article, row["url"])
                for row in urls
            ]
            work_items = zip(
                urls,
                futures,
            )

        for item_number, (row, future) in enumerate(
            work_items,
            start=1,
        ):
            if (
                item_number > 1
                and (item_number - 1) % commit_every == 0
            ):
                connection.commit()

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

            fetch_result = (
                fetch_article(url)
                if future is None
                else future.result()
            )
            now = datetime.now().isoformat(timespec="seconds")
            next_attempts = fetch_attempts + 1

            if not fetch_result["ok"]:
                if fetch_result.get("fatal"):
                    raise RuntimeError(fetch_result["error"])
                failed += 1
                status = failed_attempt_status(
                    retryable=bool(fetch_result["retryable"]),
                    completed_attempts=next_attempts,
                    max_retries=config.max_retries,
                )
                cursor.execute(
                    """
                    UPDATE discovered_urls
                    SET status = ?,
                        fetch_attempts = ?,
                        last_error = ?,
                        last_error_code = ?,
                        last_http_status = ?,
                        last_request_attempts = ?,
                        last_attempted_at = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        next_attempts,
                        fetch_result["error"],
                        fetch_result["error_code"],
                        fetch_result["status_code"],
                        fetch_result["request_attempts"],
                        now,
                        url_id,
                    ),
                )
                if status == URL_STATUS_EXHAUSTED:
                    record_source_failure(source, "exhausted")
                    logger.warning("    Failed - %s (exhausted retries)", fetch_result["error"])
                elif status == URL_STATUS_REJECTED:
                    failed -= 1
                    rejected += 1
                    record_source_failure(source, "terminal_rejection")
                    logger.warning(
                        "    Rejected - %s (terminal failure)",
                        fetch_result["error"],
                    )
                else:
                    record_source_failure(source, "retryable_fetch_failure")
                    logger.warning("    Failed - %s", fetch_result["error"])
                continue

            extracted_json = fetch_result["extracted_json"]
            if not extracted_json:
                failed += 1
                status = failed_attempt_status(
                    retryable=True,
                    completed_attempts=next_attempts,
                    max_retries=config.max_retries,
                )
                cursor.execute(
                    """
                    UPDATE discovered_urls
                    SET status = ?,
                        fetch_attempts = ?,
                        last_error = ?,
                        last_error_code = ?,
                        last_http_status = ?,
                        last_request_attempts = ?,
                        last_attempted_at = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        next_attempts,
                        "Extractor returned no article JSON",
                        URL_ERROR_EMPTY_EXTRACTION,
                        fetch_result["status_code"],
                        fetch_result["request_attempts"],
                        now,
                        url_id,
                    ),
                )
                logger.warning(
                    "    Failed - extractor returned no article JSON%s",
                    " (exhausted retries)"
                    if status == URL_STATUS_EXHAUSTED
                    else "",
                )
                record_source_failure(
                    source,
                    (
                        "exhausted"
                        if status == URL_STATUS_EXHAUSTED
                        else "empty_extraction"
                    ),
                )
                continue

            try:
                data = json.loads(extracted_json)
            except json.JSONDecodeError:
                failed += 1
                status = failed_attempt_status(
                    retryable=True,
                    completed_attempts=next_attempts,
                    max_retries=config.max_retries,
                )
                cursor.execute(
                    """
                    UPDATE discovered_urls
                    SET status = ?,
                        fetch_attempts = ?,
                        last_error = ?,
                        last_error_code = ?,
                        last_http_status = ?,
                        last_request_attempts = ?,
                        last_attempted_at = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        next_attempts,
                        "JSON parse error",
                        URL_ERROR_INVALID_EXTRACTION_JSON,
                        fetch_result["status_code"],
                        fetch_result["request_attempts"],
                        now,
                        url_id,
                    ),
                )
                logger.warning(
                    "    Failed - JSON parse error%s",
                    " (exhausted retries)"
                    if status == URL_STATUS_EXHAUSTED
                    else "",
                )
                record_source_failure(
                    source,
                    (
                        "exhausted"
                        if status == URL_STATUS_EXHAUSTED
                        else "invalid_extraction_json"
                    ),
                )
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
                        last_error_code = ?,
                        last_http_status = ?,
                        last_request_attempts = ?,
                        last_attempted_at = ?
                    WHERE id = ?
                    """,
                    (
                        URL_STATUS_REJECTED,
                        next_attempts,
                        f"Article text too short (< {config.min_article_length} chars)",
                        URL_ERROR_ARTICLE_TOO_SHORT,
                        fetch_result["status_code"],
                        fetch_result["request_attempts"],
                        now,
                        url_id,
                    ),
                )
                logger.warning("    Rejected - text too short")
                record_source_failure(source, "article_too_short")
                continue

            content_hash = compute_text_hash(raw_text)
            metadata = extract_metadata(
                url=url,
                source=source,
                trafilatura_data=data,
                html=fetch_result["html"] or "",
                rss_title=rss_title,
                rss_published=rss_published,
            )
            metadata_flags = final_metadata_flags(
                metadata.title,
                metadata.published_date,
                content_hash,
                metadata.metadata_flags,
            )

            _upsert_article(
                cursor,
                (
                    url,
                    source,
                    metadata.title,
                    metadata.title_source,
                    metadata.author,
                    metadata.author_source,
                    metadata.published_date,
                    metadata.published_date_source,
                    metadata.category,
                    metadata.category_source,
                    fetch_result["html"],
                    raw_text,
                    content_hash,
                    json.dumps(metadata_flags, ensure_ascii=False),
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
                    last_error_code = NULL,
                    last_http_status = ?,
                    last_request_attempts = ?,
                    last_attempted_at = ?,
                    fetched_at = ?,
                    fetched = 1
                WHERE id = ?
                """,
                (
                    URL_STATUS_EXTRACTED,
                    next_attempts,
                    fetch_result["status_code"],
                    fetch_result["request_attempts"],
                    now,
                    now,
                    url_id,
                ),
            )

            success += 1
            logger.info(
                "    Saved - %s chars | Title: %s",
                len(raw_text),
                metadata.title[:60],
            )

        connection.commit()
    finally:
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        connection.close()

    logger.info("")
    logger.info("=== Extraction Complete ===")
    logger.info("Success: %s | Failed: %s | Rejected: %s", success, failed, rejected)

    return {
        "extracted_articles": success,
        "fetch_failures": failed,
        "rejected_articles": rejected,
        "failures_by_source": failures_by_source,
    }


if __name__ == "__main__":
    extract_articles()
