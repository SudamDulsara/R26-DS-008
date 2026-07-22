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
            "every accepted cluster member. Defaults to 0.02 below the "
            "configured representative threshold."
        ),
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
    evaluation_parser = subparsers.add_parser(
        "evaluate-clusters",
        help="Calculate metrics from a reviewed cluster CSV file",
    )
    evaluation_parser.add_argument(
        "--review-file",
        type=Path,
        help=(
            "Reviewed CSV to evaluate. Defaults to "
            "data/reviews/cluster_reviewed.csv"
        ),
    )
    evaluation_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for the generated JSON and Markdown metrics reports",
    )
    pair_parser = subparsers.add_parser(
        "build-pair-benchmark",
        help="Build a balanced pair-labeling draft from a reviewed cluster CSV",
    )
    pair_parser.add_argument(
        "--review-file",
        type=Path,
        required=True,
        help="Fully reviewed cluster CSV used as pair-label provenance",
    )
    pair_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for the pair-review CSV and manifest",
    )
    pair_parser.add_argument(
        "--target-size",
        type=int,
        default=150,
        help="Total benchmark pairs to select (default: 150)",
    )
    pair_parser.add_argument(
        "--positive-fraction",
        type=float,
        default=0.5,
        help="Fraction pre-labeled from trusted same-event clusters (default: 0.5)",
    )
    pair_parser.add_argument(
        "--model",
        help="Embedding model used for hard-pair ranking",
    )
    pair_parser.add_argument(
        "--model-revision",
        help="Exact embedding model commit/tag used for hard-pair ranking",
    )
    pair_parser.add_argument(
        "--snippet-chars",
        type=int,
        default=220,
        help="Maximum characters in each article snippet",
    )
    comparison_parser = subparsers.add_parser(
        "compare-pair-models",
        help="Score a labeled pair benchmark with one or more embedding models",
    )
    comparison_parser.add_argument(
        "--review-file",
        type=Path,
        required=True,
        help="Fully labeled pair benchmark CSV",
    )
    comparison_parser.add_argument(
        "--model",
        action="append",
        dest="models",
        help="Model to compare; repeat for multiple models",
    )
    comparison_parser.add_argument(
        "--revision",
        action="append",
        default=[],
        metavar="MODEL=REVISION",
        help="Optional exact revision for a selected model; repeat as needed",
    )
    comparison_parser.add_argument(
        "--threshold",
        type=float,
        help="Operational threshold to evaluate (default: configured threshold)",
    )
    comparison_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for comparison reports and scored-pair CSV files",
    )
    cluster_comparison_parser = subparsers.add_parser(
        "compare-cluster-runs",
        help="Compare cluster memberships in baseline and candidate databases",
    )
    cluster_comparison_parser.add_argument(
        "--baseline-db",
        type=Path,
        required=True,
        help="Baseline SQLite database",
    )
    cluster_comparison_parser.add_argument(
        "--candidate-db",
        type=Path,
        required=True,
        help="Candidate SQLite database",
    )
    cluster_comparison_parser.add_argument(
        "--baseline-review-file",
        type=Path,
        help="Optional labeled baseline cluster-review CSV",
    )
    cluster_comparison_parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory for comparison and focused-review artifacts",
    )
    cluster_comparison_parser.add_argument(
        "--snippet-chars",
        type=int,
        default=280,
        help="Maximum characters in each article snippet",
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
    if args.command == "export":
        from news_pipeline.dataset.exporter import export_snapshot

        return export_snapshot()
    if args.command == "review-clusters":
        from news_pipeline.evaluation.cluster_review import generate_cluster_review

        return generate_cluster_review(
            output_dir=args.output_dir,
            max_snippet_chars=args.snippet_chars,
        )
    if args.command == "evaluate-clusters":
        from news_pipeline.evaluation.cluster_metrics import (
            evaluate_reviewed_clusters,
        )

        return evaluate_reviewed_clusters(
            review_path=args.review_file,
            output_dir=args.output_dir,
        )
    if args.command == "build-pair-benchmark":
        from news_pipeline.evaluation.pair_benchmark import (
            generate_pair_benchmark,
        )

        return generate_pair_benchmark(
            review_path=args.review_file,
            output_dir=args.output_dir,
            target_size=args.target_size,
            positive_fraction=args.positive_fraction,
            model_name=args.model,
            model_revision=args.model_revision,
            max_snippet_chars=args.snippet_chars,
        )
    if args.command == "compare-pair-models":
        from news_pipeline.evaluation.pair_metrics import (
            compare_pair_models,
            parse_revision_overrides,
        )

        return compare_pair_models(
            review_path=args.review_file,
            model_names=args.models,
            revision_by_model=parse_revision_overrides(args.revision),
            output_dir=args.output_dir,
            operational_threshold=args.threshold,
        )
    if args.command == "compare-cluster-runs":
        from news_pipeline.evaluation.cluster_comparison import (
            compare_cluster_databases,
        )

        return compare_cluster_databases(
            baseline_db_path=args.baseline_db,
            candidate_db_path=args.candidate_db,
            baseline_review_path=args.baseline_review_file,
            output_dir=args.output_dir,
            max_snippet_chars=args.snippet_chars,
        )
    if args.command == "run":
        from news_pipeline.pipeline import run_pipeline

        return run_pipeline()

    parser.error(f"Unknown command: {args.command}")
