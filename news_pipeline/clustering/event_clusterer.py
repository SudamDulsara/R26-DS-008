import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from news_pipeline.clustering.candidate_builder import (
    CandidatePair,
    build_candidate_pairs,
)
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


@dataclass(frozen=True)
class ClusterBuildResult:
    clusters: list[StoryCluster]
    initial_components: int
    changed_components: int
    split_components: int
    unclustered_articles: int
    cohesion_fallback_members: int


CLUSTERING_STATUS_CLUSTERED = "clustered"
CLUSTERING_STATUS_UNCLUSTERED = "unclustered"
CLUSTERING_STATUS_INELIGIBLE = "ineligible"
CLUSTERING_STATUS_BASELINE_CLUSTERED = "baseline_clustered"
CLUSTERING_STATUS_BASELINE_UNCLUSTERED = "baseline_unclustered"


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
    model_revision: Optional[str] = None,
    similarity_threshold: Optional[float] = None,
    representative_threshold: Optional[float] = None,
    cohesion_threshold: Optional[float] = None,
):
    config = load_config()
    selected_model = model_name or config.cluster_model_name
    selected_revision = model_revision
    if selected_revision is None and selected_model == config.cluster_model_name:
        selected_revision = config.cluster_model_revision
    threshold = (
        similarity_threshold
        if similarity_threshold is not None
        else config.cluster_similarity_threshold
    )
    representative_cutoff = (
        representative_threshold
        if representative_threshold is not None
        else config.cluster_representative_threshold
    )
    cohesion_cutoff = (
        cohesion_threshold
        if cohesion_threshold is not None
        else config.cluster_cohesion_threshold
    )
    _validate_similarity_threshold("similarity_threshold", threshold)
    _validate_similarity_threshold(
        "representative_threshold",
        representative_cutoff,
    )
    _validate_similarity_threshold("cohesion_threshold", cohesion_cutoff)
    if cohesion_cutoff > representative_cutoff:
        raise ValueError(
            "cohesion_threshold cannot exceed representative_threshold"
        )

    conn = get_connection()
    cursor = conn.cursor()
    existing_cluster_keys = _load_existing_cluster_keys(cursor)
    clusters_by_key, current_cluster_key_by_article = (
        _load_current_cluster_memberships(cursor)
    )
    states = _load_clustering_states(cursor)
    articles = _load_cluster_articles(cursor, config.cluster_lead_char_limit)
    article_by_id = {article.id: article for article in articles}
    eligible_article_ids = set(article_by_id)
    contract = _clustering_contract(
        model_name=selected_model,
        model_revision=selected_revision,
        similarity_threshold=threshold,
        representative_threshold=representative_cutoff,
        cohesion_threshold=cohesion_cutoff,
        window_hours=config.cluster_window_hours,
        allow_same_source_pairs=config.cluster_allow_same_source_pairs,
        min_lexical_overlap=config.cluster_min_lexical_overlap,
        lead_char_limit=config.cluster_lead_char_limit,
        min_articles=config.cluster_min_articles,
    )
    fingerprints = {
        article_id: _article_state_fingerprint(article, contract)
        for article_id, article in article_by_id.items()
    }

    logger.info("=== Incremental Same-Event Clustering Started ===")
    logger.info("Eligible unique cleaned articles: %s", len(articles))
    logger.info(
        "Model: %s@%s | Link threshold: %s | Representative threshold: %s | "
        "Cohesion threshold: %s",
        selected_model,
        selected_revision or "latest",
        threshold,
        representative_cutoff,
        cohesion_cutoff,
    )

    def current_counts():
        story_clusters = cursor.execute(
            "SELECT COUNT(*) AS count FROM story_clusters"
        ).fetchone()["count"]
        clustered_articles = cursor.execute(
            "SELECT COUNT(DISTINCT article_id) AS count "
            "FROM story_cluster_members"
        ).fetchone()["count"]
        return int(story_clusters), int(clustered_articles)

    def base_stats(*, baseline_initialized=False, incremental_noop=False):
        story_clusters, clustered_articles = current_counts()
        return {
            "eligible_articles": len(articles),
            "changed_articles": 0,
            "retired_articles": 0,
            "affected_articles": 0,
            "candidate_pairs": 0,
            "linked_pairs": 0,
            "story_clusters": story_clusters,
            "clustered_articles": clustered_articles,
            "clusters_replaced": 0,
            "preserved_clusters": story_clusters,
            "initial_story_components": 0,
            "representative_changed_components": 0,
            "representative_split_components": 0,
            "representative_unclustered_articles": 0,
            "cohesion_fallback_members": 0,
            "stable_cluster_keys_reused": 0,
            "baseline_initialized": baseline_initialized,
            "incremental_noop": incremental_noop,
            "embedding_articles": 0,
            "changed_story_keys": [],
            "transition_batch_id": None,
            "cluster_transition_counts": {},
            "model_name": selected_model,
            "model_revision": selected_revision,
            "similarity_threshold": threshold,
            "representative_threshold": representative_cutoff,
            "cohesion_threshold": cohesion_cutoff,
        }

    if not states and clusters_by_key:
        _persist_clustering_states(
            cursor,
            article_ids=eligible_article_ids,
            fingerprints=fingerprints,
            cluster_key_by_article=current_cluster_key_by_article,
        )
        legacy_ineligible_ids = (
            set(current_cluster_key_by_article) - eligible_article_ids
        )
        _persist_clustering_states(
            cursor,
            article_ids=legacy_ineligible_ids,
            fingerprints={},
            cluster_key_by_article={},
            status_override=CLUSTERING_STATUS_INELIGIBLE,
        )
        conn.commit()
        result = base_stats(
            baseline_initialized=True,
            incremental_noop=True,
        )
        conn.close()
        logger.info(
            "Initialized incremental state from %s existing clusters; "
            "embedding calls: 0",
            len(clusters_by_key),
        )
        return result

    baseline_state_ids = {
        article_id
        for article_id, state in states.items()
        if (
            article_id in eligible_article_ids
            and state["clustering_status"]
            in {
                CLUSTERING_STATUS_BASELINE_CLUSTERED,
                CLUSTERING_STATUS_BASELINE_UNCLUSTERED,
            }
        )
    }
    baseline_initialized = bool(baseline_state_ids)
    if baseline_state_ids:
        _persist_clustering_states(
            cursor,
            article_ids=baseline_state_ids,
            fingerprints=fingerprints,
            cluster_key_by_article=current_cluster_key_by_article,
        )
        conn.commit()
        states = _load_clustering_states(cursor)

    changed_article_ids = {
        article_id
        for article_id in eligible_article_ids
        if (
            article_id not in states
            or states[article_id]["input_fingerprint_sha256"]
            != fingerprints[article_id]
            or states[article_id]["clustering_status"]
            == CLUSTERING_STATUS_INELIGIBLE
        )
    }
    retired_article_ids = {
        article_id
        for article_id, state in states.items()
        if (
            article_id not in eligible_article_ids
            and state["clustering_status"] != CLUSTERING_STATUS_INELIGIBLE
        )
    } | (
        set(current_cluster_key_by_article) - eligible_article_ids
    )

    if not changed_article_ids and not retired_article_ids:
        result = base_stats(
            baseline_initialized=baseline_initialized,
            incremental_noop=True,
        )
        conn.close()
        logger.info("No clustering input changes; embedding calls: 0")
        return result

    potential_pairs = build_candidate_pairs(
        articles=articles,
        window_hours=config.cluster_window_hours,
        allow_same_source_pairs=config.cluster_allow_same_source_pairs,
        min_lexical_overlap=config.cluster_min_lexical_overlap,
    )
    affected_article_ids = _select_affected_article_ids(
        eligible_article_ids=eligible_article_ids,
        changed_article_ids=changed_article_ids,
        retired_article_ids=retired_article_ids,
        candidate_pairs=potential_pairs,
        clusters_by_key=clusters_by_key,
        cluster_key_by_article=current_cluster_key_by_article,
    )
    affected_all_ids = affected_article_ids | retired_article_ids
    affected_cluster_keys = {
        cluster_key
        for article_id in affected_all_ids
        if (
            cluster_key := current_cluster_key_by_article.get(article_id)
        )
        is not None
    }
    affected_cluster_ids = sorted(
        int(clusters_by_key[key]["id"])
        for key in affected_cluster_keys
    )
    old_affected_memberships = {
        key: set(clusters_by_key[key]["article_ids"])
        for key in affected_cluster_keys
    }
    affected_articles = [
        article_by_id[article_id]
        for article_id in sorted(affected_article_ids)
    ]
    candidates = [
        pair
        for pair in potential_pairs
        if (
            pair.left_id in affected_article_ids
            and pair.right_id in affected_article_ids
        )
    ]

    embedder = None
    linked_pairs = []
    build_result = ClusterBuildResult([], 0, 0, 0, 0, 0)
    if len(affected_articles) >= config.cluster_min_articles:
        embedder = create_embedder(selected_model, selected_revision)
        embeddings = _embed_articles(
            affected_articles,
            embedder,
            config.cluster_batch_size,
        )
        scored_pairs = _score_pairs(candidates, embeddings)
        linked_pairs = [
            pair
            for pair in scored_pairs
            if pair.similarity_score >= threshold
        ]
        build_result = _build_story_clusters(
            affected_articles,
            linked_pairs,
            embeddings=embeddings,
            min_articles=config.cluster_min_articles,
            representative_threshold=representative_cutoff,
            cohesion_threshold=cohesion_cutoff,
        )

    resolved_model_name = (
        embedder.model_name if embedder is not None else selected_model
    )
    resolved_model_revision = (
        embedder.model_revision if embedder is not None else selected_revision
    )
    _delete_affected_clusters_and_states(
        cursor,
        affected_article_ids=affected_all_ids,
        affected_cluster_ids=affected_cluster_ids,
    )
    stable_cluster_keys_reused = _persist_clusters(
        cursor=cursor,
        clusters=build_result.clusters,
        article_by_id=article_by_id,
        model_name=resolved_model_name,
        model_revision=resolved_model_revision,
        text_variant="title_lead",
        similarity_threshold=threshold,
        representative_threshold=representative_cutoff,
        cohesion_threshold=cohesion_cutoff,
        existing_cluster_keys=existing_cluster_keys,
    )
    new_cluster_key_by_article = {}
    new_memberships_by_key = {}
    for cluster in build_result.clusters:
        membership = tuple(sorted(cluster.article_ids))
        cluster_key = existing_cluster_keys.get(membership) or _cluster_key(
            cluster.article_ids,
            resolved_model_name,
            resolved_model_revision,
            threshold,
            representative_cutoff,
            cohesion_cutoff,
        )
        new_memberships_by_key[cluster_key] = set(cluster.article_ids)
        for article_id in cluster.article_ids:
            new_cluster_key_by_article[article_id] = cluster_key
    _persist_clustering_states(
        cursor,
        article_ids=affected_article_ids,
        fingerprints=fingerprints,
        cluster_key_by_article=new_cluster_key_by_article,
    )
    _persist_clustering_states(
        cursor,
        article_ids=retired_article_ids,
        fingerprints={},
        cluster_key_by_article={},
        status_override=CLUSTERING_STATUS_INELIGIBLE,
    )
    transitions = _build_cluster_transitions(
        old_affected_memberships,
        new_memberships_by_key,
    )
    transition_batch_id = None
    transition_counts = {}
    if transitions:
        transition_batch_id, transition_counts = (
            _persist_cluster_transitions(
                cursor,
                transitions=transitions,
            )
        )
    conn.commit()
    story_clusters, clustered_articles = current_counts()
    conn.close()

    logger.info("Affected articles embedded: %s", len(affected_articles))
    logger.info("Candidate pairs in affected window: %s", len(candidates))
    logger.info("Linked pairs over threshold: %s", len(linked_pairs))
    logger.info(
        "Replaced %s clusters and preserved %s unrelated clusters",
        len(affected_cluster_ids),
        len(clusters_by_key) - len(affected_cluster_ids),
    )

    return {
        "eligible_articles": len(articles),
        "changed_articles": len(changed_article_ids),
        "retired_articles": len(retired_article_ids),
        "affected_articles": len(affected_article_ids),
        "candidate_pairs": len(candidates),
        "linked_pairs": len(linked_pairs),
        "story_clusters": story_clusters,
        "clustered_articles": clustered_articles,
        "clusters_replaced": len(affected_cluster_ids),
        "preserved_clusters": (
            len(clusters_by_key) - len(affected_cluster_ids)
        ),
        "initial_story_components": build_result.initial_components,
        "representative_changed_components": (
            build_result.changed_components
        ),
        "representative_split_components": build_result.split_components,
        "representative_unclustered_articles": (
            build_result.unclustered_articles
        ),
        "cohesion_fallback_members": (
            build_result.cohesion_fallback_members
        ),
        "stable_cluster_keys_reused": stable_cluster_keys_reused,
        "baseline_initialized": baseline_initialized,
        "incremental_noop": False,
        "embedding_articles": len(affected_articles),
        "changed_story_keys": sorted(new_memberships_by_key),
        "transition_batch_id": transition_batch_id,
        "cluster_transition_counts": transition_counts,
        "model_name": resolved_model_name,
        "model_revision": resolved_model_revision,
        "similarity_threshold": threshold,
        "representative_threshold": representative_cutoff,
        "cohesion_threshold": cohesion_cutoff,
    }


def _clustering_contract(
    *,
    model_name: str,
    model_revision: Optional[str],
    similarity_threshold: float,
    representative_threshold: float,
    cohesion_threshold: float,
    window_hours: int,
    allow_same_source_pairs: bool,
    min_lexical_overlap: float,
    lead_char_limit: int,
    min_articles: int,
) -> dict:
    return {
        "model_name": model_name,
        "model_revision": model_revision,
        "similarity_threshold": similarity_threshold,
        "representative_threshold": representative_threshold,
        "cohesion_threshold": cohesion_threshold,
        "window_hours": window_hours,
        "allow_same_source_pairs": allow_same_source_pairs,
        "min_lexical_overlap": min_lexical_overlap,
        "lead_char_limit": lead_char_limit,
        "min_articles": min_articles,
        "text_variant": "title_lead",
    }


def _article_state_fingerprint(
    article: ClusterArticle,
    contract: dict,
) -> str:
    payload = {
        "source": article.source,
        "title": article.title,
        "published_date": article.published_date,
        "crawl_timestamp": article.crawl_timestamp,
        "clean_text": article.clean_text,
        "similarity_text": article.similarity_text,
        "contract": contract,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_clustering_states(cursor) -> dict[int, dict]:
    return {
        int(row["article_id"]): dict(row)
        for row in cursor.execute(
            """
            SELECT
                article_id,
                input_fingerprint_sha256,
                clustering_status,
                cluster_key,
                processed_at
            FROM clustering_article_state
            """
        )
    }


def _load_current_cluster_memberships(cursor):
    clusters_by_key: dict[str, dict] = {}
    cluster_key_by_article: dict[int, str] = {}
    for row in cursor.execute(
        """
        SELECT
            clusters.id AS cluster_id,
            clusters.cluster_key,
            members.article_id
        FROM story_clusters AS clusters
        JOIN story_cluster_members AS members
          ON members.cluster_id = clusters.id
        ORDER BY clusters.id, members.article_id
        """
    ):
        cluster_key = str(row["cluster_key"])
        cluster = clusters_by_key.setdefault(
            cluster_key,
            {
                "id": int(row["cluster_id"]),
                "article_ids": set(),
            },
        )
        article_id = int(row["article_id"])
        cluster["article_ids"].add(article_id)
        cluster_key_by_article[article_id] = cluster_key
    return clusters_by_key, cluster_key_by_article


def _select_affected_article_ids(
    *,
    eligible_article_ids: set[int],
    changed_article_ids: set[int],
    retired_article_ids: set[int],
    candidate_pairs: list[CandidatePair],
    clusters_by_key: dict[str, dict],
    cluster_key_by_article: dict[int, str],
) -> set[int]:
    directly_related = set(changed_article_ids)
    for pair in candidate_pairs:
        if pair.left_id in changed_article_ids:
            directly_related.add(pair.right_id)
        if pair.right_id in changed_article_ids:
            directly_related.add(pair.left_id)

    affected = directly_related | retired_article_ids
    cluster_seed_ids = set(affected)
    for article_id in cluster_seed_ids:
        cluster_key = cluster_key_by_article.get(article_id)
        if cluster_key is not None:
            affected.update(
                clusters_by_key[cluster_key]["article_ids"]
            )
    return affected & eligible_article_ids


def _delete_affected_clusters_and_states(
    cursor,
    *,
    affected_article_ids: set[int],
    affected_cluster_ids: list[int],
):
    if affected_article_ids:
        cursor.executemany(
            "DELETE FROM clustering_article_state WHERE article_id = ?",
            [(article_id,) for article_id in sorted(affected_article_ids)],
        )
    if affected_cluster_ids:
        cursor.executemany(
            "DELETE FROM story_cluster_members WHERE cluster_id = ?",
            [(cluster_id,) for cluster_id in affected_cluster_ids],
        )
        cursor.executemany(
            "DELETE FROM story_clusters WHERE id = ?",
            [(cluster_id,) for cluster_id in affected_cluster_ids],
        )


def _persist_clustering_states(
    cursor,
    *,
    article_ids: set[int],
    fingerprints: dict[int, str],
    cluster_key_by_article: dict[int, str],
    status_override: Optional[str] = None,
):
    processed_at = datetime.now().isoformat(timespec="seconds")
    rows = []
    for article_id in sorted(article_ids):
        cluster_key = cluster_key_by_article.get(article_id)
        status = status_override or (
            CLUSTERING_STATUS_CLUSTERED
            if cluster_key is not None
            else CLUSTERING_STATUS_UNCLUSTERED
        )
        rows.append(
            (
                article_id,
                fingerprints.get(article_id),
                status,
                cluster_key if status == CLUSTERING_STATUS_CLUSTERED else None,
                processed_at,
            )
        )
    cursor.executemany(
        """
        INSERT INTO clustering_article_state (
            article_id,
            input_fingerprint_sha256,
            clustering_status,
            cluster_key,
            processed_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(article_id) DO UPDATE SET
            input_fingerprint_sha256 = excluded.input_fingerprint_sha256,
            clustering_status = excluded.clustering_status,
            cluster_key = excluded.cluster_key,
            processed_at = excluded.processed_at
        """,
        rows,
    )


def _build_cluster_transitions(
    old_memberships: dict[str, set[int]],
    new_memberships: dict[str, set[int]],
) -> list[dict]:
    old_neighbors = {
        old_key: {
            new_key
            for new_key, new_ids in new_memberships.items()
            if old_ids & new_ids
        }
        for old_key, old_ids in old_memberships.items()
    }
    new_neighbors = {
        new_key: {
            old_key
            for old_key, old_ids in old_memberships.items()
            if new_ids & old_ids
        }
        for new_key, new_ids in new_memberships.items()
    }
    transitions = []
    for old_key, old_ids in sorted(old_memberships.items()):
        if not old_neighbors[old_key]:
            transitions.append(
                {
                    "transition_type": "retired",
                    "old_cluster_key": old_key,
                    "new_cluster_key": None,
                    "overlap_article_count": 0,
                    "old_article_ids": sorted(old_ids),
                    "new_article_ids": [],
                }
            )
            continue
        for new_key in sorted(old_neighbors[old_key]):
            new_ids = new_memberships[new_key]
            old_degree = len(old_neighbors[old_key])
            new_degree = len(new_neighbors[new_key])
            if old_degree > 1 and new_degree > 1:
                transition_type = "split_merge"
            elif old_degree > 1:
                transition_type = "split"
            elif new_degree > 1:
                transition_type = "merge"
            elif old_ids == new_ids:
                transition_type = "unchanged"
            elif old_ids < new_ids:
                transition_type = "expanded"
            elif new_ids < old_ids:
                transition_type = "contracted"
            else:
                transition_type = "reshaped"
            transitions.append(
                {
                    "transition_type": transition_type,
                    "old_cluster_key": old_key,
                    "new_cluster_key": new_key,
                    "overlap_article_count": len(old_ids & new_ids),
                    "old_article_ids": sorted(old_ids),
                    "new_article_ids": sorted(new_ids),
                }
            )
    for new_key, new_ids in sorted(new_memberships.items()):
        if new_neighbors[new_key]:
            continue
        transitions.append(
            {
                "transition_type": "created",
                "old_cluster_key": None,
                "new_cluster_key": new_key,
                "overlap_article_count": 0,
                "old_article_ids": [],
                "new_article_ids": sorted(new_ids),
            }
        )
    return transitions


def _persist_cluster_transitions(
    cursor,
    *,
    transitions: list[dict],
) -> tuple[str, dict[str, int]]:
    created_at = datetime.now().isoformat(timespec="microseconds")
    transition_payload = json.dumps(
        transitions,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(
        transition_payload.encode("utf-8")
    ).hexdigest()[:12]
    transition_batch_id = (
        "cluster_transition_"
        + datetime.now().strftime("%Y%m%dT%H%M%S%f")
        + "_"
        + digest
    )
    cursor.executemany(
        """
        INSERT INTO story_cluster_transitions (
            transition_batch_id,
            transition_type,
            old_cluster_key,
            new_cluster_key,
            overlap_article_count,
            old_article_ids_json,
            new_article_ids_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                transition_batch_id,
                transition["transition_type"],
                transition["old_cluster_key"],
                transition["new_cluster_key"],
                transition["overlap_article_count"],
                json.dumps(
                    transition["old_article_ids"],
                    separators=(",", ":"),
                ),
                json.dumps(
                    transition["new_article_ids"],
                    separators=(",", ":"),
                ),
                created_at,
            )
            for transition in transitions
        ],
    )
    counts: dict[str, int] = {}
    for transition in transitions:
        transition_type = str(transition["transition_type"])
        counts[transition_type] = counts.get(transition_type, 0) + 1
    return transition_batch_id, counts


def _validate_similarity_threshold(name: str, value: float):
    if not -1.0 <= value <= 1.0:
        raise ValueError(f"{name} must be between -1.0 and 1.0; got {value}")


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
    embeddings: dict[int, list[float]],
    min_articles: int,
    representative_threshold: float,
    cohesion_threshold: float,
) -> ClusterBuildResult:
    if not linked_pairs:
        return ClusterBuildResult([], 0, 0, 0, 0, 0)

    article_ids = [article.id for article in articles]
    article_by_id = {article.id: article for article in articles}
    pair_scores = {
        frozenset((pair.left_id, pair.right_id)): pair.similarity_score
        for pair in linked_pairs
    }
    initial_components = [
        component
        for component in _connected_components(article_ids, pair_scores)
        if len(component) >= min_articles
    ]

    clusters: list[StoryCluster] = []
    changed_components = 0
    split_components = 0
    cohesion_fallback_members = 0
    initial_component_article_ids = set()
    for component in initial_components:
        initial_component_article_ids.update(component)
        component_clusters, component_fallback_members = (
            _partition_component_by_representative(
                component,
                pair_scores,
                article_by_id,
                embeddings,
                min_articles,
                representative_threshold,
                cohesion_threshold,
            )
        )
        clusters.extend(component_clusters)
        cohesion_fallback_members += component_fallback_members

        unchanged = (
            len(component_clusters) == 1
            and set(component_clusters[0].article_ids) == set(component)
        )
        if not unchanged:
            changed_components += 1
        if len(component_clusters) > 1:
            split_components += 1

    clustered_article_ids = {
        article_id for cluster in clusters for article_id in cluster.article_ids
    }
    return ClusterBuildResult(
        clusters=sorted(clusters, key=lambda cluster: cluster.article_ids),
        initial_components=len(initial_components),
        changed_components=changed_components,
        split_components=split_components,
        unclustered_articles=len(
            initial_component_article_ids - clustered_article_ids
        ),
        cohesion_fallback_members=cohesion_fallback_members,
    )


def _connected_components(
    article_ids: list[int],
    pair_scores: dict[frozenset[int], float],
) -> list[list[int]]:
    if not article_ids:
        return []

    article_id_set = set(article_ids)
    union_find = UnionFind(article_ids)
    for pair_key in pair_scores:
        if len(pair_key) != 2 or not pair_key.issubset(article_id_set):
            continue
        left_id, right_id = tuple(pair_key)
        union_find.union(left_id, right_id)

    grouped_ids: dict[int, list[int]] = defaultdict(list)
    for article_id in article_ids:
        grouped_ids[union_find.find(article_id)].append(article_id)

    return sorted(
        (sorted(group) for group in grouped_ids.values()),
        key=lambda group: group,
    )


def _partition_component_by_representative(
    article_ids: list[int],
    pair_scores: dict[frozenset[int], float],
    article_by_id: dict[int, ClusterArticle],
    embeddings: dict[int, list[float]],
    min_articles: int,
    representative_threshold: float,
    cohesion_threshold: float,
) -> tuple[list[StoryCluster], int]:
    pending_groups = [sorted(article_ids)]
    clusters: list[StoryCluster] = []
    cohesion_fallback_members = 0

    while pending_groups:
        group = pending_groups.pop(0)
        if len(group) < min_articles:
            continue

        representative_id = _choose_representative(
            group,
            pair_scores,
            article_by_id,
        )
        member_scores = _member_scores_to_representative(
            group,
            representative_id,
            embeddings,
        )
        accepted_ids = [
            article_id
            for article_id in group
            if member_scores[article_id] >= representative_threshold
        ]
        accepted_ids, fallback_ids = _add_cohesive_borderline_members(
            group,
            accepted_ids,
            member_scores,
            embeddings,
            cohesion_threshold,
        )

        if len(accepted_ids) >= min_articles:
            accepted_scores = {
                article_id: member_scores[article_id]
                for article_id in accepted_ids
            }
            clusters.append(
                StoryCluster(
                    article_ids=sorted(accepted_ids),
                    representative_article_id=representative_id,
                    confidence=_cluster_confidence(
                        accepted_scores,
                        representative_id,
                    ),
                    member_scores=accepted_scores,
                )
            )
            cohesion_fallback_members += len(fallback_ids)
            remaining_ids = [
                article_id
                for article_id in group
                if article_id not in accepted_scores
            ]
        else:
            remaining_ids = [
                article_id
                for article_id in group
                if article_id != representative_id
            ]

        pending_groups.extend(
            component
            for component in _connected_components(remaining_ids, pair_scores)
            if len(component) >= min_articles
        )

    return clusters, cohesion_fallback_members


def _add_cohesive_borderline_members(
    article_ids: list[int],
    accepted_ids: list[int],
    representative_scores: dict[int, float],
    embeddings: dict[int, list[float]],
    cohesion_threshold: float,
) -> tuple[list[int], list[int]]:
    accepted = list(accepted_ids)
    accepted_set = set(accepted_ids)
    fallback_ids = []
    candidates = sorted(
        (
            article_id
            for article_id in article_ids
            if article_id not in accepted_set
            and representative_scores[article_id] >= cohesion_threshold
        ),
        key=lambda article_id: (-representative_scores[article_id], article_id),
    )

    for candidate_id in candidates:
        candidate_embedding = embeddings.get(candidate_id)
        if not candidate_embedding:
            continue
        is_cohesive = all(
            cosine_similarity(candidate_embedding, embeddings.get(member_id, []))
            >= cohesion_threshold
            for member_id in accepted
        )
        if is_cohesive:
            accepted.append(candidate_id)
            fallback_ids.append(candidate_id)

    return accepted, fallback_ids


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
    embeddings: dict[int, list[float]],
) -> dict[int, float]:
    representative_embedding = embeddings.get(representative_article_id)
    member_scores = {}
    for article_id in article_ids:
        if article_id == representative_article_id:
            member_scores[article_id] = 1.0
        else:
            member_embedding = embeddings.get(article_id)
            member_scores[article_id] = (
                cosine_similarity(representative_embedding, member_embedding)
                if representative_embedding and member_embedding
                else 0.0
            )
    return member_scores


def _cluster_confidence(
    member_scores: dict[int, float],
    representative_article_id: int,
) -> float:
    scores = [
        score
        for article_id, score in member_scores.items()
        if article_id != representative_article_id
    ]
    if not scores:
        return 0.0
    return round(sum(scores) / len(scores), 4)


def _replace_clusters(cursor):
    cursor.execute("DELETE FROM story_cluster_members")
    cursor.execute("DELETE FROM story_clusters")


def _load_existing_cluster_keys(cursor) -> dict[tuple[int, ...], str]:
    rows = cursor.execute(
        """
        SELECT
            clusters.cluster_key,
            members.article_id
        FROM story_clusters AS clusters
        JOIN story_cluster_members AS members
          ON members.cluster_id = clusters.id
        ORDER BY clusters.id, members.article_id
        """
    ).fetchall()
    members_by_key: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        members_by_key[str(row["cluster_key"])].append(
            int(row["article_id"])
        )
    keys_by_membership: dict[tuple[int, ...], str] = {}
    duplicate_memberships: set[tuple[int, ...]] = set()
    for cluster_key, article_ids in members_by_key.items():
        membership = tuple(sorted(article_ids))
        if membership in keys_by_membership:
            duplicate_memberships.add(membership)
            continue
        keys_by_membership[membership] = cluster_key
    for membership in duplicate_memberships:
        keys_by_membership.pop(membership, None)
    return keys_by_membership


def _persist_clusters(
    cursor,
    clusters: list[StoryCluster],
    article_by_id: dict[int, ClusterArticle],
    model_name: str,
    model_revision: str,
    text_variant: str,
    similarity_threshold: float,
    representative_threshold: float,
    cohesion_threshold: float,
    existing_cluster_keys: Optional[dict[tuple[int, ...], str]] = None,
) -> int:
    created_at = datetime.now().isoformat(timespec="seconds")
    reused_keys = 0
    existing_cluster_keys = existing_cluster_keys or {}
    for cluster in clusters:
        member_articles = [
            article_by_id[article_id] for article_id in cluster.article_ids
        ]
        event_times = [
            article.event_time
            for article in member_articles
            if article.event_time is not None
        ]
        membership = tuple(sorted(cluster.article_ids))
        cluster_key = existing_cluster_keys.get(membership)
        if cluster_key is not None:
            reused_keys += 1
        else:
            cluster_key = _cluster_key(
                cluster.article_ids,
                model_name,
                model_revision,
                similarity_threshold,
                representative_threshold,
                cohesion_threshold,
            )
        source_count = len({article.source for article in member_articles})

        cursor.execute(
            """
            INSERT INTO story_clusters (
                cluster_key,
                representative_article_id,
                model_name,
                model_revision,
                text_variant,
                similarity_threshold,
                representative_threshold,
                cohesion_threshold,
                event_date_start,
                event_date_end,
                article_count,
                source_count,
                confidence,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cluster_key,
                cluster.representative_article_id,
                model_name,
                model_revision,
                text_variant,
                similarity_threshold,
                representative_threshold,
                cohesion_threshold,
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
    return reused_keys


def _cluster_key(
    article_ids: list[int],
    model_name: str,
    model_revision: str,
    similarity_threshold: float,
    representative_threshold: float,
    cohesion_threshold: float,
) -> str:
    raw_key = (
        f"{model_name}@{model_revision}|{similarity_threshold}|"
        f"{representative_threshold}|"
        f"{cohesion_threshold}|"
        f"{','.join(map(str, article_ids))}"
    )
    digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:16]
    return f"story_{digest}"
