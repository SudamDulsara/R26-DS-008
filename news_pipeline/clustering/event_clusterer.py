import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from news_pipeline.clustering.candidate_builder import CandidatePair, build_candidate_pairs
from news_pipeline.clustering.embedder import cosine_similarity, create_embedder
from news_pipeline.clustering.text import (
    ClusterArticle,
    build_similarity_text,
    parse_article_datetime,
)
from news_pipeline.config import load_config
from news_pipeline.statuses import CLEAN_STATUS_CLEANED, DEDUPE_STATUS_UNIQUE
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


logger = get_logger()


@dataclass(frozen=True)
class ScoredPair:
    left_id: int
    right_id: int
    similarity_score: float
    lexical_overlap: float
    hours_apart: Optional[float]


@dataclass(frozen=True)
class StoryCluster:
    article_ids: list[int]
    representative_article_id: int
    confidence: float
    member_scores: dict[int, float]


class UnionFind:
    def __init__(self, values: list[int]):
        self.parent = {value: value for value in values}

    def find(self, value: int) -> int:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: int, right: int):
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def run_event_clustering(
    model_name: Optional[str] = None,
    similarity_threshold: Optional[float] = None,
):
    config = load_config()
    selected_model = model_name or config.cluster_model_name
    threshold = (
        similarity_threshold
        if similarity_threshold is not None
        else config.cluster_similarity_threshold
    )

    conn = get_connection()
    cursor = conn.cursor()

    articles = _load_cluster_articles(cursor, config.cluster_lead_char_limit)
    logger.info("=== Same-Event Clustering Started ===")
    logger.info("Eligible unique cleaned articles: %s", len(articles))
    logger.info("Model: %s | Threshold: %s", selected_model, threshold)

    if len(articles) < config.cluster_min_articles:
        _replace_clusters(cursor)
        conn.commit()
        conn.close()
        logger.info("Not enough articles for clustering.")
        return {
            "eligible_articles": len(articles),
            "candidate_pairs": 0,
            "linked_pairs": 0,
            "story_clusters": 0,
            "clustered_articles": 0,
            "model_name": selected_model,
            "similarity_threshold": threshold,
        }

    candidates = build_candidate_pairs(
        articles=articles,
        window_hours=config.cluster_window_hours,
        allow_same_source_pairs=config.cluster_allow_same_source_pairs,
        min_lexical_overlap=config.cluster_min_lexical_overlap,
    )
    logger.info("Candidate pairs: %s", len(candidates))

    article_by_id = {article.id: article for article in articles}
    embedder = create_embedder(selected_model)
    embeddings = _embed_articles(articles, embedder, config.cluster_batch_size)
    scored_pairs = _score_pairs(candidates, embeddings)
    linked_pairs = [
        pair for pair in scored_pairs if pair.similarity_score >= threshold
    ]
    clusters = _build_story_clusters(
        articles,
        linked_pairs,
        min_articles=config.cluster_min_articles,
    )

    _replace_clusters(cursor)
    _persist_clusters(
        cursor=cursor,
        clusters=clusters,
        article_by_id=article_by_id,
        model_name=embedder.model_name,
        text_variant="title_lead",
        similarity_threshold=threshold,
    )
    conn.commit()
    conn.close()

    clustered_article_ids = {
        article_id for cluster in clusters for article_id in cluster.article_ids
    }

    logger.info("Linked pairs over threshold: %s", len(linked_pairs))
    logger.info("Story clusters created: %s", len(clusters))
    logger.info("Clustered articles: %s", len(clustered_article_ids))

    return {
        "eligible_articles": len(articles),
        "candidate_pairs": len(candidates),
        "linked_pairs": len(linked_pairs),
        "story_clusters": len(clusters),
        "clustered_articles": len(clustered_article_ids),
        "model_name": embedder.model_name,
        "similarity_threshold": threshold,
    }


def _load_cluster_articles(cursor, lead_char_limit: int) -> list[ClusterArticle]:
    cursor.execute(
        """
        SELECT
            id,
            source,
            title,
            published_date,
            clean_text,
            crawl_timestamp
        FROM articles
        WHERE clean_status = ?
          AND dedupe_status = ?
          AND clean_text IS NOT NULL
          AND TRIM(clean_text) != ''
        ORDER BY id
        """,
        (CLEAN_STATUS_CLEANED, DEDUPE_STATUS_UNIQUE),
    )

    articles: list[ClusterArticle] = []
    for row in cursor.fetchall():
        clean_text = row["clean_text"] or ""
        title = row["title"] or ""
        similarity_text = build_similarity_text(title, clean_text, lead_char_limit)
        if not similarity_text:
            continue

        articles.append(
            ClusterArticle(
                id=row["id"],
                source=row["source"] or "",
                title=title,
                published_date=row["published_date"] or "",
                crawl_timestamp=row["crawl_timestamp"] or "",
                clean_text=clean_text,
                event_time=parse_article_datetime(
                    row["published_date"],
                    row["crawl_timestamp"],
                ),
                similarity_text=similarity_text,
            )
        )

    return articles


def _embed_articles(articles, embedder, batch_size: int) -> dict[int, list[float]]:
    texts = [article.similarity_text for article in articles]
    vectors = embedder.encode(texts, batch_size=batch_size)
    return {
        article.id: vector
        for article, vector in zip(articles, vectors)
        if vector
    }


def _score_pairs(
    candidates: list[CandidatePair],
    embeddings: dict[int, list[float]],
) -> list[ScoredPair]:
    scored_pairs: list[ScoredPair] = []
    for candidate in candidates:
        left_embedding = embeddings.get(candidate.left_id)
        right_embedding = embeddings.get(candidate.right_id)
        if not left_embedding or not right_embedding:
            continue

        scored_pairs.append(
            ScoredPair(
                left_id=candidate.left_id,
                right_id=candidate.right_id,
                similarity_score=cosine_similarity(left_embedding, right_embedding),
                lexical_overlap=candidate.lexical_overlap,
                hours_apart=candidate.hours_apart,
            )
        )

    return scored_pairs


def _build_story_clusters(
    articles: list[ClusterArticle],
    linked_pairs: list[ScoredPair],
    min_articles: int,
) -> list[StoryCluster]:
    if not linked_pairs:
        return []

    article_ids = [article.id for article in articles]
    article_by_id = {article.id: article for article in articles}
    pair_scores = {
        frozenset((pair.left_id, pair.right_id)): pair.similarity_score
        for pair in linked_pairs
    }
    union_find = UnionFind(article_ids)

    for pair in linked_pairs:
        union_find.union(pair.left_id, pair.right_id)

    grouped_ids: dict[int, list[int]] = defaultdict(list)
    for article_id in article_ids:
        grouped_ids[union_find.find(article_id)].append(article_id)

    clusters: list[StoryCluster] = []
    for grouped_article_ids in grouped_ids.values():
        if len(grouped_article_ids) < min_articles:
            continue

        representative_id = _choose_representative(
            grouped_article_ids,
            pair_scores,
            article_by_id,
        )
        member_scores = _member_scores_to_representative(
            grouped_article_ids,
            representative_id,
            pair_scores,
        )
        confidence = _cluster_confidence(grouped_article_ids, pair_scores)
        clusters.append(
            StoryCluster(
                article_ids=sorted(grouped_article_ids),
                representative_article_id=representative_id,
                confidence=confidence,
                member_scores=member_scores,
            )
        )

    return sorted(clusters, key=lambda cluster: cluster.article_ids)


def _choose_representative(
    article_ids: list[int],
    pair_scores: dict[frozenset[int], float],
    article_by_id: dict[int, ClusterArticle],
) -> int:
    article_id_set = set(article_ids)

    def rank(article_id: int):
        scores = [
            score
            for key, score in pair_scores.items()
            if article_id in key and key.issubset(article_id_set)
        ]
        edge_count = len(scores)
        average_score = sum(scores) / len(scores) if scores else 0.0
        text_length = len(article_by_id[article_id].clean_text)
        return edge_count, average_score, text_length, -article_id

    return max(article_ids, key=rank)


def _member_scores_to_representative(
    article_ids: list[int],
    representative_article_id: int,
    pair_scores: dict[frozenset[int], float],
) -> dict[int, float]:
    member_scores = {}
    for article_id in article_ids:
        if article_id == representative_article_id:
            member_scores[article_id] = 1.0
        else:
            member_scores[article_id] = pair_scores.get(
                frozenset((representative_article_id, article_id)),
                0.0,
            )
    return member_scores


def _cluster_confidence(
    article_ids: list[int],
    pair_scores: dict[frozenset[int], float],
) -> float:
    article_id_set = set(article_ids)
    scores = [
        score
        for key, score in pair_scores.items()
        if key.issubset(article_id_set)
    ]
    if not scores:
        return 0.0
    return round(sum(scores) / len(scores), 4)


def _replace_clusters(cursor):
    cursor.execute("DELETE FROM story_cluster_members")
    cursor.execute("DELETE FROM story_clusters")


def _persist_clusters(
    cursor,
    clusters: list[StoryCluster],
    article_by_id: dict[int, ClusterArticle],
    model_name: str,
    text_variant: str,
    similarity_threshold: float,
):
    created_at = datetime.now().isoformat(timespec="seconds")
    for cluster in clusters:
        member_articles = [article_by_id[article_id] for article_id in cluster.article_ids]
        event_times = [
            article.event_time
            for article in member_articles
            if article.event_time is not None
        ]
        cluster_key = _cluster_key(cluster.article_ids, model_name, similarity_threshold)
        source_count = len({article.source for article in member_articles})

        cursor.execute(
            """
            INSERT INTO story_clusters (
                cluster_key,
                representative_article_id,
                model_name,
                text_variant,
                similarity_threshold,
                event_date_start,
                event_date_end,
                article_count,
                source_count,
                confidence,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cluster_key,
                cluster.representative_article_id,
                model_name,
                text_variant,
                similarity_threshold,
                min(event_times) if event_times else None,
                max(event_times) if event_times else None,
                len(cluster.article_ids),
                source_count,
                cluster.confidence,
                created_at,
            ),
        )
        cluster_id = cursor.lastrowid
        cursor.executemany(
            """
            INSERT INTO story_cluster_members (
                cluster_id,
                article_id,
                similarity_score,
                is_representative
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    cluster_id,
                    article_id,
                    round(cluster.member_scores.get(article_id, 0.0), 4),
                    1 if article_id == cluster.representative_article_id else 0,
                )
                for article_id in cluster.article_ids
            ],
        )


def _cluster_key(
    article_ids: list[int],
    model_name: str,
    similarity_threshold: float,
) -> str:
    raw_key = f"{model_name}|{similarity_threshold}|{','.join(map(str, article_ids))}"
    digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:16]
    return f"story_{digest}"
