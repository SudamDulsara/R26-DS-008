import trafilatura
import sqlite3
import requests
import cloudscraper
from datetime import datetime
from urllib.parse import urlparse

_scraper = cloudscraper.create_scraper()

sqlite3.register_adapter(datetime, lambda d: d.isoformat())

from news_pipeline.storage.database import get_connection

def extract_category_from_url(url, source):
    if source == 'Dinamina':
        parts = [p for p in urlparse(url).path.split('/') if p]
        if len(parts) >= 4:
            return parts[3]
    return ''

def fetch_article(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 403:
            print(f"    HTTP 403 — retrying with cloudscraper...")
            response = _scraper.get(url, timeout=20)

        if response.status_code != 200:
            print(f"    HTTP {response.status_code}")
            return None

        result = trafilatura.extract(
            response.text,
            output_format='json'
        )
        return result
    except Exception as e:
        print(f"    Error fetching {url}: {e}")
        return None

def extract_articles():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, url, source FROM discovered_urls
        WHERE fetched = 0
    ''')
    urls = cursor.fetchall()
    conn.close()

    print(f"=== Article Extraction Started ===")
    print(f"Found {len(urls)} unfetched URLs\n")

    success = 0
    failed = 0

    for row in urls:
        url_id = row['id']
        url = row['url']
        source = row['source']

        print(f"[{source}] Fetching: {url[:60]}...")

        result = fetch_article(url)

        conn = get_connection()
        cursor = conn.cursor()

        if result:
            import json
            try:
                data = json.loads(result)
                raw_text = data.get('text', '')

                if not raw_text or len(raw_text.strip()) < 50:
                    print(f"    Skipped — text too short or empty")
                    failed += 1
                else:
                    title = data.get('title', '')
                    author = data.get('author', '')
                    date = data.get('date', '')
                    category = extract_category_from_url(url, source)

                    cursor.execute('''
                        INSERT OR IGNORE INTO articles
                        (url, source, title, author, published_date, category, raw_text, crawl_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (url, source, title, author, date, category, raw_text, datetime.now()))

                    print(f"    Saved — {len(raw_text)} chars | Title: {title[:50]}")
                    success += 1

            except json.JSONDecodeError:
                print(f"    Failed — JSON parse error")
                failed += 1
        else:
            print(f"    Failed — could not fetch")
            failed += 1

        cursor.execute('''
            UPDATE discovered_urls SET fetched = 1 WHERE id = ?
        ''', (url_id,))

        conn.commit()
        conn.close()

    print(f"\n=== Extraction Complete ===")
    print(f"Success: {success} | Failed: {failed}")

if __name__ == "__main__":
    extract_articles()