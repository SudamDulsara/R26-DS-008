import trafilatura
import sqlite3
import sys
import os
import requests
from datetime import datetime

sqlite3.register_adapter(datetime, lambda d: d.isoformat())

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from news_pipeline.storage.database import get_connection

def fetch_article(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        response = requests.get(url, headers=headers, timeout=15)
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
                    lines = raw_text.strip().split('\n')
                    title = lines[0].strip() if len(lines) > 0 else ''
                    date = lines[1].strip() if len(lines) > 1 else ''
                    body = '\n'.join(lines[2:]).strip()

                    cursor.execute('''
                        INSERT OR IGNORE INTO articles 
                        (url, source, title, author, published_date, category, raw_text, crawl_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (url, source, title, '', date, '', body, datetime.now()))

                    print(f"    Saved — {len(body)} chars | Title: {title[:50]}")
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