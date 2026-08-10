from dataclasses import replace
from pathlib import Path

from news_pipeline.cleaner.sinhala_cleaner import run_cleaner
from news_pipeline.clustering.event_clusterer import run_event_clustering
from news_pipeline.config import load_config
from news_pipeline.crawler.rss_crawler import run_discovery
from news_pipeline.dataset.exporter import export_snapshot
from news_pipeline.deduplicator.exact_deduper import run_exact_dedup
from news_pipeline.extractor.article_extractor import extract_articles
from news_pipeline.observability import (
    PipelineRunMetrics,
    write_pipeline_health_report,
)
from news_pipeline.run_lock import pipeline_run_lock
from news_pipeline.storage.database import (
    finish_pipeline_run,
    initialize_db,
    start_pipeline_run,
)
from news_pipeline.storage.logger import setup_logger
from news_pipeline.unification.production import run_gpt_unification


def _log_step(logger, step_name: str):
    logger.info("=" * 50)
    logger.info(step_name)
    logger.info("=" * 50)


def _active_unification_config(config):
    if not config.gpt_only_publication_enabled:
        return config
    return replace(
        config,
        gpt_prompt_version=(
            config.gpt_only_publication_prompt_version
        ),
    )


def _run_pipeline_locked(*, config, no_gpt: bool = False):
    initialize_db()
    logger = setup_logger()
    run_id = start_pipeline_run()
    stats = {}
    metrics = PipelineRunMetrics()

    try:
        _log_step(logger, "STEP 1: Crawl - RSS URL Discovery")
        stats["discovery"] = metrics.run("discovery", run_discovery)

        logger.info("")
        _log_step(logger, "STEP 2: Extract - Article Content")
        stats["extraction"] = metrics.run(
            "extraction",
            extract_articles,
        )

        logger.info("")
        _log_step(logger, "STEP 3: Clean - Sinhala Text Cleaning")
        stats["cleaning"] = metrics.run("cleaning", run_cleaner)

        logger.info("")
        _log_step(logger, "STEP 4: Deduplicate - Exact Match Pass")
        stats["deduplication"] = metrics.run(
            "deduplication",
            run_exact_dedup,
        )

        logger.info("")
        _log_step(logger, "STEP 5: Cluster - Same-Event Story Groups")
        stats["clustering"] = metrics.run(
            "clustering",
            run_event_clustering,
        )

        logger.info("")
        _log_step(
            logger,
            "STEP 6: Unify - Generate or Reuse Unified Stories",
        )
        unification_kwargs = {
            "no_gpt": no_gpt,
            "config": _active_unification_config(config),
        }
        changed_story_keys = stats["clustering"].get(
            "changed_story_keys"
        )
        if (
            changed_story_keys
            and not config.gpt_only_publication_enabled
        ):
            unification_kwargs["cluster_keys"] = changed_story_keys
        stats["unification"] = metrics.run(
            "unification",
            run_gpt_unification,
            **unification_kwargs,
        )

        logger.info("")
        _log_step(logger, "STEP 7: Export - Versioned Snapshot")
        stats["export"] = metrics.run(
            "export",
            export_snapshot,
            gpt_shadow=bool(
                stats["unification"].get("shadow_mode", False)
            ),
        )

        stats["run_metrics"] = metrics.finish("completed")
        snapshot_dir = stats["export"].get("snapshot_dir")
        if snapshot_dir:
            stats["run_health"] = write_pipeline_health_report(
                run_id=run_id,
                stats=stats,
                output_dir=Path(snapshot_dir),
            )
        finish_pipeline_run(run_id, "completed", stats)
        logger.info("")
        logger.info("=== Pipeline Complete ===")
        return stats
    except Exception as exc:
        stats["run_metrics"] = metrics.finish("failed")
        finish_pipeline_run(run_id, "failed", stats, note=str(exc))
        logger.exception("Pipeline failed")
        raise


def run_pipeline(*, no_gpt: bool = False):
    config = load_config()
    with pipeline_run_lock(config.db_path):
        return _run_pipeline_locked(config=config, no_gpt=no_gpt)
