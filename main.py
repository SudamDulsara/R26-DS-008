import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from news_pipeline.storage.logger import setup_logger
from news_pipeline.crawler.rss_crawler import run_discovery
from news_pipeline.extractor.article_extractor import extract_articles
from news_pipeline.cleaner.sinhala_cleaner import run_cleaner

if __name__ == "__main__":
    log = setup_logger()

    log.info("=" * 50)
    log.info("STEP 1: Crawl — RSS URL Discovery")
    log.info("=" * 50)
    run_discovery()

    log.info("\n" + "=" * 50)
    log.info("STEP 2: Extract — Article Content")
    log.info("=" * 50)
    extract_articles()

    log.info("\n" + "=" * 50)
    log.info("STEP 3: Clean — Sinhala Text Cleaning")
    log.info("=" * 50)
    run_cleaner()

    log.info("\n=== Pipeline Complete ===")
