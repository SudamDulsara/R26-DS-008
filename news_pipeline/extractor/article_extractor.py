import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import urljoin

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
    URL_ERROR_ARTICLE_BODY_INCOMPLETE,
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

BOILERPLATE_MARKERS = frozenset(
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


def _body_quality(text: str, title: str, min_length: int) -> tuple[bool, list[str]]:
    normalized = "\n".join(
        line.strip() for line in (text or "").splitlines() if line.strip()
    )
    flags = []
    if len(normalized) < min_length:
        flags.append("article_too_short")
    lowered_lines = {line.lower() for line in normalized.splitlines()}
    marker_count = len(lowered_lines & BOILERPLATE_MARKERS)
    sinhala_letters = sum("\u0D80" <= char <= "\u0DFF" for char in normalized)
    body_without_title = normalized
    if title:
        body_without_title = body_without_title.replace(title.strip(), "", 1).strip()
    body_sinhala_letters = sum(
        "\u0D80" <= char <= "\u0DFF" for char in body_without_title
    )
    sentence_marks = sum(normalized.count(mark) for mark in (".", "?", "!", "।"))
    if "january 1, 1970" in normalized.lower():
        flags.append("invalid_default_date")
    if marker_count >= 3:
        flags.append("navigation_dominated")
    if sinhala_letters < 35 or body_sinhala_letters < 25:
        flags.append("insufficient_article_body")
    if marker_count >= 3 and sentence_marks == 0:
        flags.append("no_article_sentences")
    return not flags, flags


def _html_extraction_candidates(html: str) -> list[tuple[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except ModuleNotFoundError:
        return []
    soup = BeautifulSoup(html or "", "html.parser")
    candidates: list[tuple[str, str]] = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            value = json.loads(script.string or script.get_text())
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        values = value if isinstance(value, list) else [value]
        for item in values:
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            if isinstance(graph, list):
                values.extend(node for node in graph if isinstance(node, dict))
            body = item.get("articleBody")
            if isinstance(body, str) and body.strip():
                candidates.append(("json_ld_article_body", body.strip()))
    selectors = (
        ".news-content",
        "article .entry-content",
        ".entry-content",
        ".post-content",
        ".article-content",
        "article",
    )
    seen = set()
    for selector in selectors:
        for node in soup.select(selector):
            value = "\n".join(
                part.strip()
                for part in node.get_text("\n", strip=True).splitlines()
                if part.strip()
            )
            if value and value not in seen:
                seen.add(value)
                candidates.append((f"html_selector:{selector}", value))
    return candidates


def _best_extraction(
    *,
    html: str,
    extracted_json: str | None,
    rss_title: str,
    rss_summary: str,
    min_length: int,
) -> tuple[dict | None, str | None, list[str]]:
    base_data = {}
    candidates: list[tuple[str, str]] = []
    if extracted_json:
        try:
            value = json.loads(extracted_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            value = None
        if isinstance(value, dict):
            base_data = value
            if str(value.get("text") or "").strip():
                candidates.append(("trafilatura", str(value["text"]).strip()))
    candidates.extend(_html_extraction_candidates(html))
    if len(rss_summary.strip()) >= max(800, min_length * 4):
        candidates.append(("rss_summary", rss_summary.strip()))

    best = None
    diagnostics: list[str] = []
    for method, text in candidates:
        valid, flags = _body_quality(text, rss_title, min_length)
        if valid:
            priority = (
                3
                if method in {"trafilatura", "json_ld_article_body"}
                else 2
                if method != "rss_summary" and method != "html_selector:article"
                else 1
            )
            candidate = (priority, len(text), method, text)
            if best is None or candidate[:2] > best[:2]:
                best = candidate
        else:
            diagnostics.extend(flags)
    if best is None:
        return None, None, sorted(set(diagnostics or ["empty_extraction"]))
    data = dict(base_data)
    data["text"] = best[3]
    return data, best[2], []


def _alternate_document_urls(html: str, original_url: str) -> list[str]:
    try:
        from bs4 import BeautifulSoup
    except ModuleNotFoundError:
        return []
    soup = BeautifulSoup(html or "", "html.parser")
    urls = []
    for rel in ("amphtml", "canonical"):
        node = soup.select_one(f'link[rel~="{rel}"][href]')
        if node is None:
            continue
        candidate = urljoin(
            original_url,
            str(node.get("href") or "").strip(),
        )
        if candidate and candidate != original_url and candidate not in urls:
            urls.append(candidate)
    return urls


def fetch_article(url, *, rss_title="", rss_summary=""):
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

            extracted_json = trafilatura.extract(
                response.text,
                output_format="json",
            )
            data, method, diagnostics = _best_extraction(
                html=response.text,
                extracted_json=extracted_json,
                rss_title=rss_title,
                rss_summary=rss_summary,
                min_length=config.min_article_length,
            )
            selected_html = response.text
            if data is None:
                for alternate_url in _alternate_document_urls(response.text, url)[:2]:
                    request_attempts += 1
                    alternate = requests.get(alternate_url, headers=headers, timeout=15)
                    if alternate.status_code != 200:
                        continue
                    alternate_json = trafilatura.extract(
                        alternate.text,
                        output_format="json",
                    )
                    data, method, alternate_diagnostics = _best_extraction(
                        html=alternate.text,
                        extracted_json=alternate_json,
                        rss_title=rss_title,
                        rss_summary=rss_summary,
                        min_length=config.min_article_length,
                    )
                    diagnostics.extend(alternate_diagnostics)
                    if data is not None:
                        selected_html = alternate.text
                        method = f"alternate:{method}"
                        break
            return {
                "ok": True,
                "error": None,
                "error_code": None,
                "retryable": False,
                "fatal": False,
                "status_code": response.status_code,
                "request_attempts": request_attempts,
                "html": selected_html,
                "extracted_json": (
                    json.dumps(data, ensure_ascii=False) if data is not None else None
                ),
                "extraction_method": method,
                "extraction_diagnostics": sorted(set(diagnostics)),
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
            exported_at,
            extraction_method
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0, NULL, ?, ?, '[]', NULL, NULL, ?)
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
            exported_at = NULL,
            extraction_method = excluded.extraction_method
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
        SELECT id, url, source, fetch_attempts, rss_title, rss_published,
               rss_summary
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
    processed_url_ids = []

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
                executor.submit(
                    fetch_article,
                    row["url"],
                    rss_title=row["rss_title"] or "",
                    rss_summary=row["rss_summary"] or "",
                )
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
            rss_summary = row["rss_summary"] or ""
            processed_url_ids.append(int(url_id))

            attempt_number = fetch_attempts + 1
            logger.info(
                "[%s] Fetching (attempt %s/%s): %s...",
                source,
                attempt_number,
                config.max_retries,
                url[:60],
            )

            fetch_result = (
                fetch_article(
                    url,
                    rss_title=rss_title,
                    rss_summary=rss_summary,
                )
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
                        "Article body unavailable after alternate extraction",
                        URL_ERROR_ARTICLE_BODY_INCOMPLETE,
                        fetch_result["status_code"],
                        fetch_result["request_attempts"],
                        now,
                        url_id,
                    ),
                )
                logger.warning(
                    "    Failed - article body unavailable%s",
                    " (exhausted retries)"
                    if status == URL_STATUS_EXHAUSTED
                    else "",
                )
                record_source_failure(
                    source,
                    (
                        "exhausted"
                        if status == URL_STATUS_EXHAUSTED
                        else "article_body_incomplete"
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
                    fetch_result.get("extraction_method") or "trafilatura",
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
        "_processed_url_ids": processed_url_ids,
    }


def extract_articles_with_recovery(*, fresh_discovered_at: str | None = None):
    """Run bounded recovery passes inside the ordinary extraction stage."""
    config = load_config()
    pass_results = []
    processed_ids: set[int] = set()
    for pass_number in range(config.extraction_recovery_passes):
        result = extract_articles(workers=config.extraction_workers)
        pass_results.append(result)
        processed_ids.update(int(value) for value in result.get("_processed_url_ids", []))
        if result["fetch_failures"] == 0:
            break
        if pass_number + 1 < config.extraction_recovery_passes:
            logger.info(
                "Retrying %s recoverable extraction failures in the same run",
                result["fetch_failures"],
            )
            if config.retry_delay_seconds > 0:
                time.sleep(config.retry_delay_seconds)

    if not processed_ids:
        return {
            "extracted_articles": 0,
            "fetch_failures": 0,
            "rejected_articles": 0,
            "recovered_articles": 0,
            "recovery_passes": len(pass_results),
            "failures_by_source": {},
            "fresh_urls_attempted": 0,
            "fresh_extracted_articles": 0,
            "fresh_fetch_failures": 0,
            "fresh_rejected_articles": 0,
            "historical_retry_urls_attempted": 0,
            "historical_retry_extracted_articles": 0,
            "historical_retry_fetch_failures": 0,
            "historical_retry_rejected_articles": 0,
            "fresh_failures_by_source": {},
            "historical_retry_failures_by_source": {},
        }
    connection = get_connection()
    try:
        placeholders = ",".join("?" for _ in processed_ids)
        rows = connection.execute(
            f"""
            SELECT source, status, last_error_code, discovered_at
            FROM discovered_urls
            WHERE id IN ({placeholders})
            """,
            tuple(sorted(processed_ids)),
        ).fetchall()
    finally:
        connection.close()
    extracted = sum(row["status"] == URL_STATUS_EXTRACTED for row in rows)
    rejected = sum(row["status"] == URL_STATUS_REJECTED for row in rows)
    failed_rows = [
        row
        for row in rows
        if row["status"] in {URL_STATUS_FETCH_FAILED, URL_STATUS_EXHAUSTED}
    ]
    failures_by_source = {}
    for row in failed_rows:
        source_failures = failures_by_source.setdefault(row["source"], {})
        reason = row["last_error_code"] or "unknown_failure"
        source_failures[reason] = source_failures.get(reason, 0) + 1
    first_successes = int(pass_results[0]["extracted_articles"])
    origin_counts = _summarize_extraction_origins(
        rows,
        fresh_discovered_at=fresh_discovered_at,
    )
    return {
        "extracted_articles": extracted,
        "fetch_failures": len(failed_rows),
        "rejected_articles": rejected,
        "recovered_articles": max(0, extracted - first_successes),
        "recovery_passes": len(pass_results),
        "request_attempts": sum(
            len(result.get("_processed_url_ids", [])) for result in pass_results
        ),
        "failures_by_source": failures_by_source,
        **origin_counts,
    }


def _summarize_extraction_origins(
    rows,
    *,
    fresh_discovered_at: str | None,
) -> dict:
    summaries = {
        "fresh": {
            "urls_attempted": 0,
            "extracted_articles": 0,
            "fetch_failures": 0,
            "rejected_articles": 0,
            "failures_by_source": {},
        },
        "historical_retry": {
            "urls_attempted": 0,
            "extracted_articles": 0,
            "fetch_failures": 0,
            "rejected_articles": 0,
            "failures_by_source": {},
        },
    }
    for row in rows:
        origin = (
            "fresh"
            if fresh_discovered_at is not None
            and str(row["discovered_at"]) == fresh_discovered_at
            else "historical_retry"
        )
        summary = summaries[origin]
        summary["urls_attempted"] += 1
        status = row["status"]
        if status == URL_STATUS_EXTRACTED:
            summary["extracted_articles"] += 1
        elif status == URL_STATUS_REJECTED:
            summary["rejected_articles"] += 1
        elif status in {URL_STATUS_FETCH_FAILED, URL_STATUS_EXHAUSTED}:
            summary["fetch_failures"] += 1
            source_failures = summary["failures_by_source"].setdefault(
                row["source"],
                {},
            )
            reason = row["last_error_code"] or "unknown_failure"
            source_failures[reason] = source_failures.get(reason, 0) + 1

    return {
        f"{origin}_{name}": value
        for origin, summary in summaries.items()
        for name, value in summary.items()
    }


if __name__ == "__main__":
    extract_articles()
