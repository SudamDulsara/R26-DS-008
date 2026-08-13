import argparse

from news_pipeline.storage.database import initialize_db
from news_pipeline.storage.logger import setup_logger


def build_parser():
    parser = argparse.ArgumentParser(description="Sinhala news dataset pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Initialize the SQLite database")
    status_parser = subparsers.add_parser(
        "status",
        help="Show local pipeline, snapshot, and publication health",
    )
    status_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the status contract as JSON",
    )
    discover_parser = subparsers.add_parser(
        "discover",
        help="Discover article URLs from RSS feeds",
    )
    discover_parser.add_argument(
        "--feed-pages",
        type=int,
        default=1,
        help=(
            "WordPress feed pages to inspect per source (default: 1); "
            "non-WordPress feeds remain single-page"
        ),
    )
    extract_parser = subparsers.add_parser(
        "extract",
        help="Fetch and extract article content",
    )
    extract_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum pending URLs to process in this run",
    )
    extract_parser.add_argument(
        "--min-id",
        type=int,
        help=(
            "Process only discovered URL rows at or above this ID; useful "
            "for a bounded newly discovered batch"
        ),
    )
    extract_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Concurrent network fetches (default: 1)",
    )
    extract_parser.add_argument(
        "--commit-every",
        type=int,
        default=25,
        help="Persist progress after this many URLs (default: 25)",
    )

    metadata_parser = subparsers.add_parser(
        "improve-metadata",
        help="Backfill article metadata from stored HTML, RSS, and URL fallbacks",
    )
    metadata_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing metadata when a stronger extracted value is available",
    )

    subparsers.add_parser("clean", help="Clean extracted Sinhala text")
    subparsers.add_parser("dedupe", help="Run exact duplicate detection")

    cluster_parser = subparsers.add_parser(
        "cluster",
        help="Cluster near-duplicate articles into same-event story groups",
    )
    cluster_parser.add_argument(
        "--model",
        help=(
            "Embedding model name. Use 'hashing' for a lightweight offline "
            "smoke test."
        ),
    )
    cluster_parser.add_argument(
        "--model-revision",
        help=(
            "Exact model commit/tag to load. The configured E5 commit is used "
            "when --model is not overridden."
        ),
    )
    cluster_parser.add_argument(
        "--threshold",
        type=float,
        help="Cosine similarity threshold for linking article pairs",
    )
    cluster_parser.add_argument(
        "--representative-threshold",
        type=float,
        help=(
            "Minimum direct similarity between every cluster member and its "
            "representative. Defaults to the configured link threshold."
        ),
    )
    cluster_parser.add_argument(
        "--cohesion-threshold",
        type=float,
        help=(
            "Fallback threshold for a borderline member that is similar to "
            "every accepted cluster member."
        ),
    )

    unify_parser = subparsers.add_parser(
        "unify",
        help="Generate or reuse persisted GPT unified stories",
    )
    unify_parser.add_argument(
        "--no-gpt",
        action="store_true",
        help="Run offline using cached GPT or evidence-safe source fallback",
    )
    unify_parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate even when an accepted cache entry exists",
    )
    unify_parser.add_argument(
        "--revalidate-only",
        action="store_true",
        help=(
            "Re-run local validation on persisted GPT outputs without "
            "making API calls"
        ),
    )
    unify_parser.add_argument(
        "--cache-only",
        action="store_true",
        help=(
            "Verify persisted cache coverage without constructing an API "
            "client or making requests"
        ),
    )
    export_parser = subparsers.add_parser(
        "export",
        help="Export a versioned dataset snapshot",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Run the full end-to-end pipeline",
    )
    run_parser.add_argument(
        "--no-gpt",
        action="store_true",
        help="Complete the pipeline without making GPT API calls",
    )

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        from news_pipeline.pipeline import run_pipeline

        return run_pipeline(no_gpt=args.no_gpt)

    initialize_db()
    setup_logger()

    if args.command == "init-db":
        return initialize_db()
    if args.command == "status":
        import json

        from news_pipeline.operational_status import (
            build_pipeline_status,
            format_pipeline_status,
        )

        status = build_pipeline_status()
        print(
            json.dumps(status, ensure_ascii=False, indent=2)
            if args.json
            else format_pipeline_status(status)
        )
        return status
    if args.command == "discover":
        from news_pipeline.crawler.rss_crawler import run_discovery

        return run_discovery(feed_pages=args.feed_pages)
    if args.command == "extract":
        from news_pipeline.extractor.article_extractor import extract_articles

        return extract_articles(
            limit=args.limit,
            min_id=args.min_id,
            workers=args.workers,
            commit_every=args.commit_every,
        )
    if args.command == "improve-metadata":
        from news_pipeline.extractor.metadata_backfill import run_metadata_backfill

        return run_metadata_backfill(overwrite=args.overwrite)
    if args.command == "clean":
        from news_pipeline.cleaner.sinhala_cleaner import run_cleaner

        return run_cleaner()
    if args.command == "dedupe":
        from news_pipeline.deduplicator.exact_deduper import run_exact_dedup

        return run_exact_dedup()
    if args.command == "cluster":
        from news_pipeline.clustering.event_clusterer import run_event_clustering

        return run_event_clustering(
            model_name=args.model,
            model_revision=args.model_revision,
            similarity_threshold=args.threshold,
            representative_threshold=args.representative_threshold,
            cohesion_threshold=args.cohesion_threshold,
        )
    if args.command == "unify":
        from news_pipeline.unification.production import (
            revalidate_cached_unification,
            run_gpt_unification,
            verify_unification_cache,
        )

        selected_safe_modes = sum(
            bool(value)
            for value in (
                args.revalidate_only,
                args.cache_only,
                args.no_gpt,
            )
        )
        if selected_safe_modes > 1:
            parser.error(
                "--revalidate-only, --cache-only, and --no-gpt "
                "cannot be combined"
            )
        if args.cache_only:
            if args.force:
                parser.error(
                    "--cache-only cannot be combined with --force"
                )
            return verify_unification_cache()
        if args.revalidate_only:
            if args.force:
                parser.error(
                    "--revalidate-only cannot be combined with --force"
                )
            return revalidate_cached_unification()
        return run_gpt_unification(
            no_gpt=args.no_gpt,
            force=args.force,
        )
    if args.command == "export":
        from news_pipeline.dataset.exporter import export_snapshot

        return export_snapshot()
    parser.error(f"Unknown command: {args.command}")
