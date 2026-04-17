import feedparser
import sqlite3
from datetime import datetime

sqlite3.register_adapter(datetime, lambda d: d.isoformat())

from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger

logger = get_logger()

NEWS_SOURCES = {
    "Mawbima":           "https://www.mawbima.lk/feed",
    "Divaina":           "https://www.divaina.lk/feed",
    "Silumina":          "https://www.silumina.lk/feed",
    "BBC Sinhala":       "https://www.bbc.com/sinhala/index.xml",
    "Ada Derana Sinhala": "https://sinhala.adaderana.lk/rsshotnews.php",
    "Anidda":            "https://www.anidda.lk/feed",
    "NethnewsLk":        "https://www.nethnews.lk/feed",
    "Navaliya":          "https://www.navaliya.lk/feed",
    "Dinamina":          "https://www.dinamina.lk/feed",
}

def discover_urls(source_name, rss_url):
    logger.info(f"\n[{source_name}] Fetching RSS feed...")
    feed = feedparser.parse(rss_url)

    if feed.bozo:
        logger.warning(f"[{source_name}] Feed may have issues — {feed.bozo_exception}")

    new_count = 0
    conn = get_connection()
    try:
        cursor = conn.cursor()

        for entry in feed.entries:
            url = entry.get("link", "").strip()
            if not url and entry.get("links"):
                url = entry.links[0].get("href", "").strip()
            if not url:
                continue

            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO discovered_urls (url, source, discovered_at, fetched)
                    VALUES (?, ?, ?, 0)
                ''', (url, source_name, datetime.now()))

                if cursor.rowcount > 0:
                    new_count += 1

            except sqlite3.Error as e:
                logger.error(f"[{source_name}] DB error for {url}: {e}")

        conn.commit()
    finally:
        conn.close()
    logger.info(f"[{source_name}] Done — {new_count} new URLs saved.")
    return new_count

def run_discovery():
    logger.info("=== URL Discovery Started ===")
    total = 0
    for source_name, rss_url in NEWS_SOURCES.items():
        total += discover_urls(source_name, rss_url)
    logger.info(f"\n=== Discovery Complete — {total} total new URLs found ===")

if __name__ == "__main__":
    run_discovery()