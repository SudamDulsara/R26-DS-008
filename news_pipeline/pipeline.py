from news_pipeline.cleaner.sinhala_cleaner import run_cleaner
from news_pipeline.crawler.rss_crawler import run_discovery
from news_pipeline.dataset.exporter import export_snapshot
from news_pipeline.deduplicator.exact_deduper import run_exact_dedup
from news_pipeline.extractor.article_extractor import extract_articles
from news_pipeline.storage.database import (
    finish_pipeline_run,
    initialize_db,
    start_pipeline_run,
)
from news_pipeline.storage.logger import setup_logger


def _log_step(logger, step_name: str):
    logger.info("=" * 50)
    logger.info(step_name)
    logger.info("=" * 50)


def run_pipeline():
    initialize_db()
    logger = setup_logger()
    run_id = start_pipeline_run()
    stats = {}

    try:
        _log_step(logger, "STEP 1: Crawl - RSS URL Discovery")
        stats["discovery"] = run_discovery()

        logger.info("")
        _log_step(logger, "STEP 2: Extract - Article Content")
        stats["extraction"] = extract_articles()

        logger.info("")
        _log_step(logger, "STEP 3: Clean - Sinhala Text Cleaning")
        stats["cleaning"] = run_cleaner()

        logger.info("")
        _log_step(logger, "STEP 4: Deduplicate - Exact Match Pass")
        stats["deduplication"] = run_exact_dedup()

        logger.info("")
        _log_step(logger, "STEP 5: Export - Versioned Snapshot")
        stats["export"] = export_snapshot()

        finish_pipeline_run(run_id, "completed", stats)
        logger.info("")
        logger.info("=== Pipeline Complete ===")
        return stats
    except Exception as exc:
        finish_pipeline_run(run_id, "failed", stats, note=str(exc))
        logger.exception("Pipeline failed")
        raise
