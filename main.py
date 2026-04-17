import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from news_pipeline.crawler.rss_crawler import run_discovery
from news_pipeline.extractor.article_extractor import extract_articles
from news_pipeline.cleaner.sinhala_cleaner import run_cleaner

if __name__ == "__main__":
    print("=" * 50)
    print("STEP 1: Crawl — RSS URL Discovery")
    print("=" * 50)
    run_discovery()

    print("\n" + "=" * 50)
    print("STEP 2: Extract — Article Content")
    print("=" * 50)
    extract_articles()

    print("\n" + "=" * 50)
    print("STEP 3: Clean — Sinhala Text Cleaning")
    print("=" * 50)
    run_cleaner()

    print("\n=== Pipeline Complete ===")
