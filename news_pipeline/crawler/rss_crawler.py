import feedparser
import sqlite3
import sys
import os
from datetime import datetime

sqlite3.register_adapter(datetime, lambda d: d.isoformat())

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from news_pipeline.storage.database import get_connection

NEWS_SOURCES = {
    "Mawbima": "https://www.mawbima.lk/feed",
    "Divaina": "https://www.divaina.lk/feed",
    "Silumina": "https://www.silumina.lk/feed",
    "BBC Sinhala": "https://www.bbc.com/sinhala/index.xml",
}

def discover_urls(source_name, rss_url):
    print(f"\n[{source_name}] Fetching RSS feed...")
    feed = feedparser.parse(rss_url)

    if feed.bozo:
        print(f"[{source_name}] WARNING: Feed may have issues — {feed.bozo_exception}")

    new_count = 0
    conn = get_connection()
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
            print(f"[{source_name}] DB error for {url}: {e}")

    conn.commit()
    conn.close()
    print(f"[{source_name}] Done — {new_count} new URLs saved.")
    return new_count

def run_discovery():
    print("=== URL Discovery Started ===")
    total = 0
    for source_name, rss_url in NEWS_SOURCES.items():
        total += discover_urls(source_name, rss_url)
    print(f"\n=== Discovery Complete — {total} total new URLs found ===")

if __name__ == "__main__":
    run_discovery()