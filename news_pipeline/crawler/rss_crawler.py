from datetime import datetime
import sqlite3

from news_pipeline.config import load_config
from news_pipeline.statuses import URL_STATUS_DISCOVERED
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


sqlite3.register_adapter(datetime, lambda value: value.isoformat())

logger = get_logger()


def discover_urls(source_name, rss_url, connection):
    try:
        import feedparser
    except ModuleNotFoundError as exc:
        logger.error("Missing dependency: %s", exc.name)
        return 0

    logger.info("")
    logger.info("[%s] Fetching RSS feed...", source_name)
    feed = feedparser.parse(rss_url)

    if feed.bozo:
        logger.warning("[%s] Feed may have issues - %s", source_name, feed.bozo_exception)

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
            logger.error("[%s] DB error for %s: %s", source_name, url, exc)

    logger.info("[%s] Done - %s new URLs saved.", source_name, new_count)
    return new_count


def run_discovery():
    config = load_config()
    connection = get_connection()

    logger.info("=== URL Discovery Started ===")
    total = 0

    try:
        for source_name, rss_url in config.news_sources.items():
            total += discover_urls(source_name, rss_url, connection)
        connection.commit()
    finally:
        connection.close()

    logger.info("")
    logger.info("=== Discovery Complete - %s total new URLs found ===", total)
    return {
        "new_urls": total,
        "sources_checked": len(config.news_sources),
    }


if __name__ == "__main__":
    run_discovery()
