import json
import hashlib
import trafilatura
import sqlite3
import requests
import cloudscraper
import time
from datetime import datetime
from urllib.parse import urlparse

MAX_RETRIES = 3 
RETRY_DELAY = 3 

_scraper = cloudscraper.create_scraper()

sqlite3.register_adapter(datetime, lambda d: d.isoformat())

from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger

logger = get_logger()

def extract_category_from_url(url, source):
    if source == 'Dinamina':
        parts = [p for p in urlparse(url).path.split('/') if p]
        if len(parts) >= 4:
            return parts[3]
    return ''

def fetch_article(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for attempt in range(2):  
        try:
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 403:
                logger.info(f"    HTTP 403 — retrying with cloudscraper...")
                response = _scraper.get(url, timeout=20)

            if response.status_code != 200:
                logger.warning(f"    HTTP {response.status_code}")
                return None  

            return trafilatura.extract(response.text, output_format='json')

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == 0:
                logger.warning(f"    Network error ({e.__class__.__name__}), retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"    Error fetching {url}: {e}")
        except Exception as e:
            logger.error(f"    Error fetching {url}: {e}")
            return None
    return None

def extract_articles():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, url, source, fetch_attempts FROM discovered_urls
        WHERE fetched = 0 AND fetch_attempts < ?
    ''', (MAX_RETRIES,))
    urls = cursor.fetchall()
    conn.close()

    logger.info(f"=== Article Extraction Started ===")
    logger.info(f"Found {len(urls)} unfetched URLs\n")

    success = 0
    failed = 0

    for row in urls:
        url_id = row['id']
        url = row['url']
        source = row['source']
        fetch_attempts = row['fetch_attempts']

        attempt_label = f" (attempt {fetch_attempts + 1}/{MAX_RETRIES})" if fetch_attempts > 0 else ""
        logger.info(f"[{source}] Fetching{attempt_label}: {url[:60]}...")

        result = fetch_article(url)

        conn = get_connection()
        cursor = conn.cursor()

        fetch_failed = False

        if result:
            try:
                data = json.loads(result)
                raw_text = data.get('text', '')

                if not raw_text or len(raw_text.strip()) < 50:
                    logger.warning(f"    Skipped — text too short or empty")
                    failed += 1
                else:
                    title = data.get('title', '')
                    author = data.get('author', '')
                    date = data.get('date', '')
                    category = extract_category_from_url(url, source)
                    content_hash = hashlib.sha256(raw_text.encode('utf-8')).hexdigest()

                    cursor.execute('''
                        INSERT OR IGNORE INTO articles
                        (url, source, title, author, published_date, category, raw_text, content_hash, crawl_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (url, source, title, author, date, category, raw_text, content_hash, datetime.now()))

                    logger.info(f"    Saved — {len(raw_text)} chars | Title: {title[:50]}")
                    success += 1

            except json.JSONDecodeError:
                logger.error(f"    Failed — JSON parse error")
                failed += 1
        else:
            logger.warning(f"    Failed — could not fetch")
            failed += 1
            fetch_failed = True

        new_attempts = fetch_attempts + 1
        if fetch_failed:
            exhausted = new_attempts >= MAX_RETRIES
            cursor.execute('''
                UPDATE discovered_urls SET fetch_attempts = ?, fetched = ? WHERE id = ?
            ''', (new_attempts, 1 if exhausted else 0, url_id))
            if exhausted:
                logger.warning(f"    Giving up after {MAX_RETRIES} attempts")
            else:
                logger.info(f"    Will retry ({new_attempts}/{MAX_RETRIES} attempts used)")
        else:
            cursor.execute(
                'UPDATE discovered_urls SET fetched = 1, fetch_attempts = ? WHERE id = ?',
                (new_attempts, url_id)
            )

        conn.commit()
        conn.close()

    logger.info(f"\n=== Extraction Complete ===")
    logger.info(f"Success: {success} | Failed: {failed}")

if __name__ == "__main__":
    extract_articles()