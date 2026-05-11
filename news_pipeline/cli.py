import argparse
from pathlib import Path

from news_pipeline.storage.database import initialize_db
from news_pipeline.storage.logger import setup_logger


def build_parser():
    parser = argparse.ArgumentParser(description="Sinhala news dataset pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Initialize the SQLite database")
    subparsers.add_parser("discover", help="Discover article URLs from RSS feeds")
    subparsers.add_parser("extract", help="Fetch and extract article content")
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
        "--threshold",
        type=float,
        help="Cosine similarity threshold for linking article pairs",
    )
    subparsers.add_parser("export", help="Export a versioned dataset snapshot")
    review_parser = subparsers.add_parser(
        "review-clusters",
        help="Export a human-review file for current story clusters",
    )
    review_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for review CSV/JSONL files",
    )
    review_parser.add_argument(
        "--snippet-chars",
        type=int,
        default=280,
        help="Maximum characters to include per article snippet",
    )
    subparsers.add_parser("run", help="Run the full end-to-end pipeline")

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    initialize_db()
    setup_logger()

    if args.command == "init-db":
        return initialize_db()
    if args.command == "discover":
        from news_pipeline.crawler.rss_crawler import run_discovery

        return run_discovery()
    if args.command == "extract":
        from news_pipeline.extractor.article_extractor import extract_articles

        return extract_articles()
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
            similarity_threshold=args.threshold,
        )
    if args.command == "export":
        from news_pipeline.dataset.exporter import export_snapshot

        return export_snapshot()
    if args.command == "review-clusters":
        from news_pipeline.evaluation.cluster_review import generate_cluster_review

        return generate_cluster_review(
            output_dir=args.output_dir,
            max_snippet_chars=args.snippet_chars,
        )
    if args.command == "run":
        from news_pipeline.pipeline import run_pipeline

        return run_pipeline()

    parser.error(f"Unknown command: {args.command}")
