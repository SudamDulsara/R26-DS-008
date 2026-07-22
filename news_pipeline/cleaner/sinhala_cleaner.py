import json
import re
import sqlite3
import unicodedata
from datetime import datetime

from news_pipeline.config import load_config
from news_pipeline.statuses import (
    CLEAN_STATUS_CLEANED,
    CLEAN_STATUS_PENDING,
    CLEAN_STATUS_REJECTED,
)
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


sqlite3.register_adapter(datetime, lambda value: value.isoformat())

logger = get_logger()

SINHALA_START = "\u0D80"
SINHALA_END = "\u0DFF"


def normalize_unicode(text):
    return unicodedata.normalize("NFC", text)


def remove_noise(text):
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"\S+@\S+", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = "".join(char for char in text if char.isprintable() or char == "\n")
    return text.strip()


def calculate_sinhala_purity(text):
    if not text:
        return 0.0

    sinhala_chars = [char for char in text if SINHALA_START <= char <= SINHALA_END]
    total_chars = [char for char in text if not char.isspace()]

    if not total_chars:
        return 0.0

    return len(sinhala_chars) / len(total_chars)


def clean_article(raw_text):
    if not raw_text:
        return "", 0.0

    text = normalize_unicode(raw_text)
    text = remove_noise(text)
    purity = calculate_sinhala_purity(text)
    return text, purity


def _flags_for_rejection(reason: str):
    return json.dumps([reason], ensure_ascii=False)


def run_cleaner():
    config = load_config()
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) AS count FROM articles")
    logger.info("Total articles in DB: %s", cursor.fetchone()["count"])

    cursor.execute(
        """
        SELECT id, source, raw_text
        FROM articles
        WHERE clean_status = ?
        ORDER BY id
        """,
        (CLEAN_STATUS_PENDING,),
    )
    articles = cursor.fetchall()

    logger.info("=== Sinhala Cleaner Started ===")
    logger.info("Found %s articles to clean", len(articles))
    logger.info("")

    passed = 0
    failed = 0

    try:
        for row in articles:
            article_id = row["id"]
            source = row["source"]
            raw_text = row["raw_text"]

            clean_text, purity = clean_article(raw_text)
            purity_percent = round(purity * 100, 2)
            cleaned_at = datetime.now().isoformat(timespec="seconds")

            if purity >= config.purity_threshold:
                cursor.execute(
                    """
                    UPDATE articles
                    SET clean_text = ?,
                        sinhala_purity = ?,
                        clean_status = ?,
                        quality_flags = '[]',
                        cleaned_at = ?
                    WHERE id = ?
                    """,
                    (
                        clean_text,
                        purity,
                        CLEAN_STATUS_CLEANED,
                        cleaned_at,
                        article_id,
                    ),
                )
                logger.info("[%s] PASSED - Purity: %s%%", source, purity_percent)
                passed += 1
            else:
                cursor.execute(
                    """
                    UPDATE articles
                    SET clean_text = ?,
                        sinhala_purity = ?,
                        clean_status = ?,
                        quality_flags = ?,
                        cleaned_at = ?
                    WHERE id = ?
                    """,
                    (
                        clean_text,
                        purity,
                        CLEAN_STATUS_REJECTED,
                        _flags_for_rejection("low_sinhala_purity"),
                        cleaned_at,
                        article_id,
                    ),
                )
                logger.warning(
                    "[%s] FAILED - Purity: %s%% (below %s%%)",
                    source,
                    purity_percent,
                    config.purity_threshold * 100,
                )
                failed += 1

        connection.commit()
    finally:
        connection.close()

    logger.info("")
    logger.info("=== Cleaning Complete ===")
    if passed + failed > 0:
        logger.info("Passed: %s | Failed: %s", passed, failed)
        logger.info(
            "Overall purity pass rate: %s%%",
            round((passed / (passed + failed)) * 100, 1),
        )
    else:
        logger.info("No articles to process.")

    return {
        "cleaned_articles": passed,
        "rejected_articles": failed,
    }


if __name__ == "__main__":
    run_cleaner()
