import csv
import json
import re
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from pathlib import Path
from typing import Optional

from news_pipeline.clustering.embedder import cosine_similarity, create_embedder
from news_pipeline.clustering.text import (
    build_similarity_text,
    lexical_overlap,
    parse_article_datetime,
)
from news_pipeline.config import load_config
from news_pipeline.evaluation.cluster_review import make_snippet
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


logger = get_logger()

PAIR_LABEL_HELP = "same_event | different_event | unsure"
DECIDED_CLUSTER_LABELS = {"same_event", "mixed_related", "wrong_cluster"}


@dataclass(frozen=True)
class ReviewedCluster:
    cluster_id: str
    review_label: str
    article_ids: tuple[int, ...]


@dataclass(frozen=True)
class BenchmarkArticle:
    id: int
    source: str
    title: str
    url: str
    published_date: str
    crawl_timestamp: str
    clean_text: str
    event_time: Optional[datetime]
    similarity_text: str


@dataclass(frozen=True)
class PairCandidate:
    left_id: int
    right_id: int
    left_cluster_id: str
    right_cluster_id: str
    selection_bucket: str
    similarity_score: float
    lexical_overlap: float
    hours_apart: Optional[float]


def generate_pair_benchmark(
    review_path: Path,
    output_dir: Optional[Path] = None,
    target_size: int = 150,
    positive_fraction: float = 0.5,
    model_name: Optional[str] = None,
    model_revision: Optional[str] = None,
    max_snippet_chars: int = 220,
):
    _validate_options(target_size, positive_fraction, max_snippet_chars)
    config = load_config()
    selected_model = model_name or config.cluster_model_name
    selected_revision = model_revision
    if selected_revision is None and selected_model == config.cluster_model_name:
        selected_revision = config.cluster_model_revision

    clusters = read_reviewed_clusters(review_path)
    article_ids = sorted(
        {article_id for cluster in clusters for article_id in cluster.article_ids}
    )
    articles = load_benchmark_articles(article_ids, config.cluster_lead_char_limit)
    missing_ids = sorted(set(article_ids) - set(articles))
    if missing_ids:
        raise ValueError(
            "Reviewed article IDs are missing from the database: "
            + ", ".join(map(str, missing_ids))
        )

    embedder = create_embedder(selected_model, selected_revision)
    ordered_articles = [articles[article_id] for article_id in article_ids]
    vectors = embedder.encode(
        [article.similarity_text for article in ordered_articles],
        batch_size=config.cluster_batch_size,
    )
    embeddings = {
        article.id: vector
        for article, vector in zip(ordered_articles, vectors)
        if vector
    }
    records = build_pair_benchmark_records(
        clusters=clusters,
        articles=articles,
        embeddings=embeddings,
        target_size=target_size,
        positive_fraction=positive_fraction,
        window_hours=config.cluster_window_hours,
        model_name=embedder.model_name,
        model_revision=embedder.model_revision,
        max_snippet_chars=max_snippet_chars,
    )

    generated_at = datetime.now().astimezone()
    timestamp = generated_at.strftime("%Y-%m-%d_%H-%M-%S")
    benchmark_dir = output_dir or config.reviews_dir / "pair_benchmark"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    csv_path = benchmark_dir / f"pair_benchmark_review_{timestamp}.csv"
    manifest_path = benchmark_dir / f"pair_benchmark_manifest_{timestamp}.json"
    _write_csv(csv_path, records)

    label_counts = _count_values(records, "review_label")
    bucket_counts = _count_values(records, "selection_bucket")
    manifest = {
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "source_cluster_review": str(review_path.resolve()),
        "model_name": embedder.model_name,
        "model_revision": embedder.model_revision,
        "target_size": target_size,
        "pair_count": len(records),
        "prelabeled_same_event_pairs": label_counts.get("same_event", 0),
        "pairs_needing_review": label_counts.get("", 0),
        "selection_bucket_counts": bucket_counts,
        "cluster_window_hours": config.cluster_window_hours,
        "csv_path": str(csv_path.resolve()),
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    logger.info("=== Pair Benchmark Draft Complete ===")
    logger.info("Pairs: %s", len(records))
    logger.info(
        "Trusted same-event labels: %s | Pairs needing review: %s",
        manifest["prelabeled_same_event_pairs"],
        manifest["pairs_needing_review"],
    )
    logger.info("Model: %s@%s", embedder.model_name, embedder.model_revision)
    logger.info("CSV: %s", csv_path)
    logger.info("Manifest: %s", manifest_path)

    return {
        **manifest,
        "csv_path": str(csv_path),
        "manifest_path": str(manifest_path),
    }


def read_reviewed_clusters(review_path: Path) -> list[ReviewedCluster]:
    if not review_path.exists():
        raise FileNotFoundError(f"Reviewed cluster file not found: {review_path}")

    with review_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"cluster_id", "review_label", "member_article_ids"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(
                "Reviewed cluster file is missing required columns: "
                + ", ".join(missing)
            )
        rows = list(reader)

    clusters = []
    seen_cluster_ids = set()
    article_to_cluster = {}
    for row_number, row in enumerate(rows, 2):
        cluster_id = str(row.get("cluster_id") or "").strip()
        label = str(row.get("review_label") or "").strip().lower()
        if not cluster_id:
            raise ValueError(f"Missing cluster_id on CSV row {row_number}")
        if cluster_id in seen_cluster_ids:
            raise ValueError(f"Duplicate cluster_id in review file: {cluster_id}")
        if label not in DECIDED_CLUSTER_LABELS:
            raise ValueError(
                f"Cluster {cluster_id} needs a decided review label before "
                "building a pair benchmark"
            )
        article_ids = tuple(
            int(value)
            for value in re.findall(r"\d+", row.get("member_article_ids") or "")
        )
        if len(article_ids) < 2:
            raise ValueError(
                f"Cluster {cluster_id} has fewer than two member article IDs"
            )
        for article_id in article_ids:
            previous_cluster = article_to_cluster.get(article_id)
            if previous_cluster is not None:
                raise ValueError(
                    f"Article {article_id} appears in clusters "
                    f"{previous_cluster} and {cluster_id}"
                )
            article_to_cluster[article_id] = cluster_id
        seen_cluster_ids.add(cluster_id)
        clusters.append(ReviewedCluster(cluster_id, label, article_ids))

    return sorted(clusters, key=lambda cluster: _sortable_id(cluster.cluster_id))


def build_pair_benchmark_records(
    clusters: list[ReviewedCluster],
    articles: dict[int, BenchmarkArticle],
    embeddings: dict[int, list[float]],
    target_size: int,
    positive_fraction: float,
    window_hours: int,
    model_name: str,
    model_revision: str,
    max_snippet_chars: int = 220,
) -> list[dict]:
    _validate_options(target_size, positive_fraction, max_snippet_chars)
    cluster_by_article = {
        article_id: cluster
        for cluster in clusters
        for article_id in cluster.article_ids
    }
    positive_candidates = []
    review_candidates = []

    for cluster in clusters:
        for left_id, right_id in combinations(sorted(cluster.article_ids), 2):
            candidate = _make_candidate(
                left_id,
                right_id,
                cluster,
                cluster,
                (
                    "trusted_same_event"
                    if cluster.review_label == "same_event"
                    else "mixed_cluster_internal"
                ),
                articles,
                embeddings,
                window_hours,
            )
            if candidate is None:
                continue
            if cluster.review_label == "same_event":
                positive_candidates.append(candidate)
            else:
                review_candidates.append(candidate)

    article_ids = sorted(cluster_by_article)
    for left_id, right_id in combinations(article_ids, 2):
        left_cluster = cluster_by_article[left_id]
        right_cluster = cluster_by_article[right_id]
        if left_cluster.cluster_id == right_cluster.cluster_id:
            continue
        candidate = _make_candidate(
            left_id,
            right_id,
            left_cluster,
            right_cluster,
            "cross_cluster_hard_candidate",
            articles,
            embeddings,
            window_hours,
        )
        if candidate is not None:
            review_candidates.append(candidate)

    positive_target = round(target_size * positive_fraction)
    selected_positive = _round_robin_positive_pairs(
        positive_candidates,
        positive_target,
    )
    review_target = target_size - len(selected_positive)
    selected_review = _select_review_pairs(review_candidates, review_target)
    if len(selected_positive) + len(selected_review) < target_size:
        raise ValueError(
            "Not enough eligible reviewed-cluster pairs to build the requested "
            f"benchmark size of {target_size}"
        )

    selected = [
        (candidate, "same_event", "trusted_cluster_review")
        for candidate in selected_positive
    ] + [
        (candidate, "", "human_review_required")
        for candidate in selected_review
    ]
    return [
        _candidate_record(
            candidate,
            review_label,
            label_source,
            articles,
            cluster_by_article,
            model_name,
            model_revision,
            max_snippet_chars,
        )
        for candidate, review_label, label_source in selected
    ]


def load_benchmark_articles(
    article_ids: list[int],
    lead_char_limit: int,
) -> dict[int, BenchmarkArticle]:
    if not article_ids:
        return {}
    placeholders = ",".join("?" for _ in article_ids)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT
            id,
            source,
            title,
            url,
            published_date,
            crawl_timestamp,
            clean_text
        FROM articles
        WHERE id IN ({placeholders})
        ORDER BY id
        """,
        article_ids,
    )
    rows = cursor.fetchall()
    conn.close()

    articles = {}
    for row in rows:
        title = row["title"] or ""
        clean_text = row["clean_text"] or ""
        articles[row["id"]] = BenchmarkArticle(
            id=row["id"],
            source=row["source"] or "",
            title=title,
            url=row["url"] or "",
            published_date=row["published_date"] or "",
            crawl_timestamp=row["crawl_timestamp"] or "",
            clean_text=clean_text,
            event_time=parse_article_datetime(
                row["published_date"],
                row["crawl_timestamp"],
            ),
            similarity_text=build_similarity_text(
                title,
                clean_text,
                lead_char_limit,
            ),
        )
    return articles


def _make_candidate(
    left_id: int,
    right_id: int,
    left_cluster: ReviewedCluster,
    right_cluster: ReviewedCluster,
    selection_bucket: str,
    articles: dict[int, BenchmarkArticle],
    embeddings: dict[int, list[float]],
    window_hours: int,
) -> Optional[PairCandidate]:
    left = articles[left_id]
    right = articles[right_id]
    if left.source == right.source:
        return None
    hours_apart = _hours_between(left.event_time, right.event_time)
    if hours_apart is not None and hours_apart > window_hours:
        return None
    left_embedding = embeddings.get(left_id)
    right_embedding = embeddings.get(right_id)
    if not left_embedding or not right_embedding:
        return None
    return PairCandidate(
        left_id=min(left_id, right_id),
        right_id=max(left_id, right_id),
        left_cluster_id=left_cluster.cluster_id,
        right_cluster_id=right_cluster.cluster_id,
        selection_bucket=selection_bucket,
        similarity_score=cosine_similarity(left_embedding, right_embedding),
        lexical_overlap=lexical_overlap(
            left.similarity_text,
            right.similarity_text,
        ),
        hours_apart=hours_apart,
    )


def _round_robin_positive_pairs(
    candidates: list[PairCandidate],
    target: int,
) -> list[PairCandidate]:
    queues = defaultdict(list)
    for candidate in candidates:
        queues[candidate.left_cluster_id].append(candidate)
    ordered_queues = {
        cluster_id: deque(
            sorted(
                cluster_candidates,
                key=lambda item: (
                    item.similarity_score,
                    item.left_id,
                    item.right_id,
                ),
            )
        )
        for cluster_id, cluster_candidates in queues.items()
    }
    cluster_ids = sorted(ordered_queues, key=_sortable_id)
    selected = []
    while len(selected) < target:
        added = False
        for cluster_id in cluster_ids:
            queue = ordered_queues[cluster_id]
            if queue and len(selected) < target:
                selected.append(queue.popleft())
                added = True
        if not added:
            break
    return selected


def _select_review_pairs(
    candidates: list[PairCandidate],
    target: int,
) -> list[PairCandidate]:
    internal = sorted(
        (
            candidate
            for candidate in candidates
            if candidate.selection_bucket == "mixed_cluster_internal"
        ),
        key=_hard_negative_rank,
    )
    cross_cluster = sorted(
        (
            candidate
            for candidate in candidates
            if candidate.selection_bucket == "cross_cluster_hard_candidate"
        ),
        key=_hard_negative_rank,
    )
    internal_target = min(len(internal), target // 2)
    selected = internal[:internal_target]
    selected.extend(cross_cluster[: target - len(selected)])
    if len(selected) < target:
        remaining = target - len(selected)
        selected.extend(
            internal[internal_target : internal_target + remaining]
        )
    return selected[:target]


def _hard_negative_rank(candidate: PairCandidate):
    return (-candidate.similarity_score, candidate.left_id, candidate.right_id)


def _candidate_record(
    candidate: PairCandidate,
    review_label: str,
    label_source: str,
    articles: dict[int, BenchmarkArticle],
    cluster_by_article: dict[int, ReviewedCluster],
    model_name: str,
    model_revision: str,
    max_snippet_chars: int,
) -> dict:
    left = articles[candidate.left_id]
    right = articles[candidate.right_id]
    left_cluster = cluster_by_article[candidate.left_id]
    right_cluster = cluster_by_article[candidate.right_id]
    return {
        "pair_id": f"pair_{candidate.left_id}_{candidate.right_id}",
        "left_article_id": candidate.left_id,
        "right_article_id": candidate.right_id,
        "review_label": review_label,
        "review_label_help": PAIR_LABEL_HELP,
        "review_notes": "",
        "suggested_label": (
            "same_event" if review_label == "same_event" else "different_event"
        ),
        "label_source": label_source,
        "selection_bucket": candidate.selection_bucket,
        "baseline_model_name": model_name,
        "baseline_model_revision": model_revision,
        "baseline_similarity_score": round(candidate.similarity_score, 6),
        "lexical_overlap": round(candidate.lexical_overlap, 6),
        "hours_apart": (
            "" if candidate.hours_apart is None else round(candidate.hours_apart, 3)
        ),
        "left_cluster_id": candidate.left_cluster_id,
        "right_cluster_id": candidate.right_cluster_id,
        "left_cluster_label": left_cluster.review_label,
        "right_cluster_label": right_cluster.review_label,
        "left_source": left.source,
        "right_source": right.source,
        "left_title": left.title,
        "right_title": right.title,
        "left_published_date": left.published_date,
        "right_published_date": right.published_date,
        "left_url": left.url,
        "right_url": right.url,
        "left_snippet": make_snippet(left.clean_text, max_snippet_chars),
        "right_snippet": make_snippet(right.clean_text, max_snippet_chars),
    }


def _write_csv(path: Path, records: list[dict]):
    if not records:
        raise ValueError("Cannot write an empty pair benchmark")
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def _count_values(records: list[dict], key: str) -> dict[str, int]:
    counts = defaultdict(int)
    for record in records:
        counts[str(record.get(key) or "")] += 1
    return dict(sorted(counts.items()))


def _hours_between(
    left_time: Optional[datetime],
    right_time: Optional[datetime],
) -> Optional[float]:
    if left_time is None or right_time is None:
        return None
    return abs((right_time - left_time).total_seconds()) / 3600


def _sortable_id(value: str):
    try:
        return 0, int(value)
    except ValueError:
        return 1, value


def _validate_options(
    target_size: int,
    positive_fraction: float,
    max_snippet_chars: int,
):
    if target_size < 2:
        raise ValueError("target_size must be at least 2")
    if not 0.0 < positive_fraction < 1.0:
        raise ValueError("positive_fraction must be between 0 and 1")
    if max_snippet_chars < 20:
        raise ValueError("max_snippet_chars must be at least 20")
