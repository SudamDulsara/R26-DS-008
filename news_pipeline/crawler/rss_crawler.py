from datetime import datetime
import sqlite3
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from news_pipeline.config import load_config
from news_pipeline.statuses import URL_STATUS_DISCOVERED
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


sqlite3.register_adapter(datetime, lambda value: value.isoformat())

logger = get_logger()


def build_feed_page_urls(rss_url: str, feed_pages: int) -> list[str]:
    if feed_pages < 1:
        raise ValueError("feed_pages must be at least 1")
    urls = [rss_url]
    path = urlsplit(rss_url).path.rstrip("/").lower()
    if feed_pages == 1 or not path.endswith("/feed"):
        return urls
    split = urlsplit(rss_url)
    base_query = dict(parse_qsl(split.query, keep_blank_values=True))
    for page in range(2, feed_pages + 1):
        query = {**base_query, "paged": str(page)}
        urls.append(
            urlunsplit(
                (
                    split.scheme,
                    split.netloc,
                    split.path,
                    urlencode(query),
                    split.fragment,
                )
            )
        )
    return urls


def discover_urls(
    source_name,
    rss_url,
    connection,
    log_label=None,
    telemetry=None,
):
    def record_failure(reason):
        if telemetry is None:
            return
        source_failures = telemetry.setdefault(source_name, {})
        source_failures[reason] = source_failures.get(reason, 0) + 1

    try:
        import feedparser
    except ModuleNotFoundError as exc:
        logger.error("Missing dependency: %s", exc.name)
        record_failure("missing_dependency")
        return 0

    logger.info("")
    label = log_label or source_name
    logger.info("[%s] Fetching RSS feed...", label)
    feed = feedparser.parse(rss_url)

    if feed.bozo:
        logger.warning("[%s] Feed may have issues - %s", label, feed.bozo_exception)
        if not feed.entries:
            record_failure("feed_parse_error")

    new_count = 0
    cursor = connection.cursor()

    for entry in feed.entries:
        url = entry.get("link", "").strip()
        if not url and entry.get("links"):
            url = entry.links[0].get("href", "").strip()
        if not url:
            continue

        rss_title = entry.get("title", "").strip() or None
        rss_published = (
            entry.get("published", "").strip()
            or entry.get("updated", "").strip()
            or None
        )

        try:
            cursor.execute("SELECT 1 FROM discovered_urls WHERE url = ?", (url,))
            existed = cursor.fetchone() is not None
            cursor.execute(
                """
                INSERT INTO discovered_urls
                    (url, source, discovered_at, rss_title, rss_published, status, fetched)
                VALUES (?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(url) DO UPDATE SET
                    rss_title = COALESCE(discovered_urls.rss_title, excluded.rss_title),
                    rss_published = COALESCE(
                        discovered_urls.rss_published,
                        excluded.rss_published
                    )
                """,
                (
                    url,
                    source_name,
                    datetime.now().isoformat(timespec="seconds"),
                    rss_title,
                    rss_published,
                    URL_STATUS_DISCOVERED,
                ),
            )

            if not existed:
                new_count += 1

        except sqlite3.Error as exc:
            logger.error("[%s] DB error for %s: %s", label, url, exc)
            record_failure("database_error")

    logger.info("[%s] Done - %s new URLs saved.", label, new_count)
    return new_count


def run_discovery(feed_pages: int = 1):
    config = load_config()
    connection = get_connection()

    logger.info("=== URL Discovery Started ===")
    total = 0
    failures_by_source = {}

    try:
        for source_name, rss_url in config.news_sources.items():
            feed_urls = build_feed_page_urls(rss_url, feed_pages)
            for page_number, feed_url in enumerate(feed_urls, start=1):
                log_label = (
                    source_name
                    if len(feed_urls) == 1
                    else f"{source_name} feed page {page_number}"
                )
                total += discover_urls(
                    source_name,
                    feed_url,
                    connection,
                    log_label=log_label,
                    telemetry=failures_by_source,
                )
        connection.commit()
    finally:
        connection.close()

    logger.info("")
    logger.info("=== Discovery Complete - %s total new URLs found ===", total)
    return {
        "new_urls": total,
        "sources_checked": len(config.news_sources),
        "feed_pages_requested": feed_pages,
        "failures_by_source": failures_by_source,
    }


if __name__ == "__main__":
    run_discovery()
