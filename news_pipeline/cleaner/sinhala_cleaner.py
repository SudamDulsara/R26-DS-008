import unicodedata
import re
import sqlite3
from datetime import datetime

sqlite3.register_adapter(datetime, lambda d: d.isoformat())

from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger

logger = get_logger()

SINHALA_START = '\u0D80'
SINHALA_END = '\u0DFF'

PURITY_THRESHOLD = 0.85

def normalize_unicode(text):
    return unicodedata.normalize('NFC', text)

def remove_noise(text):
    text = re.sub(r'http\S+|www\S+', '', text)
    text = re.sub(r'\S+@\S+', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    text = ''.join(c for c in text if c.isprintable() or c == '\n')
    return text.strip()

def calculate_sinhala_purity(text):
    if not text or len(text) == 0:
        return 0.0
    sinhala_chars = [c for c in text if SINHALA_START <= c <= SINHALA_END]
    total_chars = [c for c in text if not c.isspace()]
    if len(total_chars) == 0:
        return 0.0
    return len(sinhala_chars) / len(total_chars)

def clean_article(raw_text):
    if not raw_text:
        return None, 0.0
    text = normalize_unicode(raw_text)
    text = remove_noise(text)
    purity = calculate_sinhala_purity(text)
    return text, purity

def run_cleaner():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM articles')
    logger.info(f"Total articles in DB: {cursor.fetchone()[0]}")

    cursor.execute('''
        SELECT id, url, source, raw_text FROM articles
        WHERE clean_text IS NULL
    ''')
    articles = cursor.fetchall()
    conn.close()

    logger.info(f"=== Sinhala Cleaner Started ===")
    logger.info(f"Found {len(articles)} articles to clean\n")

    passed = 0
    failed = 0

    for row in articles:
        article_id = row['id']
        source = row['source']
        raw_text = row['raw_text']

        clean_text, purity = clean_article(raw_text)
        purity_percent = round(purity * 100, 2)

        conn = get_connection()
        cursor = conn.cursor()

        if purity >= PURITY_THRESHOLD:
            cursor.execute('''
                UPDATE articles
                SET clean_text = ?, sinhala_purity = ?
                WHERE id = ?
            ''', (clean_text, purity, article_id))
            logger.info(f"[{source}] PASSED — Purity: {purity_percent}%")
            passed += 1
        else:
            cursor.execute('''
                UPDATE articles
                SET clean_text = NULL, sinhala_purity = ?
                WHERE id = ?
            ''', (purity, article_id))
            logger.warning(f"[{source}] FAILED — Purity: {purity_percent}% (below {PURITY_THRESHOLD*100}%)")
            failed += 1

        conn.commit()
        conn.close()

    logger.info(f"\n=== Cleaning Complete ===")
    if passed + failed > 0:
        logger.info(f"Passed: {passed} | Failed: {failed}")
        logger.info(f"Overall purity pass rate: {round(passed/(passed+failed)*100, 1)}%")
    else:
        logger.info("No articles to process.")

if __name__ == "__main__":
    run_cleaner()