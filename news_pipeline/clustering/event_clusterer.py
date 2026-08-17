import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Optional

from news_pipeline.clustering.candidate_builder import (
    CandidatePair,
    build_candidate_pairs,
)
from news_pipeline.clustering.embedding_cache import (
    load_cached_vectors,
    persist_cached_vectors,
)
from news_pipeline.clustering.embedder import (
    cosine_similarity,
    create_embedder,
    embedding_input_fingerprint,
)
from news_pipeline.clustering.semantic_constraints import (
    load_active_different_event_pairs,
)
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
    force_article_ids: Optional[set[int]] = None,
    use_embedding_cache: bool = True,
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
    different_event_pairs = load_active_different_event_pairs(
        cursor,
        article_by_id=article_by_id,
    )
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
    singleton_clusters_backfilled = 0

    logger.info("=== Incremental Same-Event Clustering Started ===")
    logger.info("Clustering setup:")
    logger.info("  Eligible cleaned articles: %s", len(articles))
    logger.info(
        "  Embedding model: %s@%s",
        selected_model,
        selected_revision or "latest",
    )
    logger.info(
        "  Similarity thresholds: direct link >= %s | representative >= %s | "
        "all-member cohesion >= %s",
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
            "semantic_constraint_pairs_removed": 0,
            "bridge_pairs_removed": 0,
            "story_clusters": story_clusters,
            "clustered_articles": clustered_articles,
            "clusters_replaced": 0,
            "preserved_clusters": story_clusters,
            "initial_story_components": 0,
            "representative_changed_components": 0,
            "representative_split_components": 0,
            "representative_unclustered_articles": 0,
            "singleton_articles": 0,
            "cohesion_fallback_members": 0,
            "singleton_clusters_backfilled": (
                singleton_clusters_backfilled
            ),
            "stable_cluster_keys_reused": 0,
            "baseline_initialized": baseline_initialized,
            "incremental_noop": incremental_noop,
            "embedding_articles": 0,
            "embedding_cache_enabled": bool(use_embedding_cache),
            "embedding_cache_hits": 0,
            "embedding_cache_misses": 0,
            "embedding_encoded_vectors": 0,
            "embedding_cache_lookup_seconds": 0.0,
            "embedding_model_load_seconds": 0.0,
            "embedding_encoding_seconds": 0.0,
            "embedding_cache_write_seconds": 0.0,
            "embedding_total_seconds": 0.0,
            "changed_story_keys": [],
            "transition_batch_id": None,
            "cluster_transition_counts": {},
            "model_name": selected_model,
            "model_revision": selected_revision,
            "similarity_threshold": threshold,
            "representative_threshold": representative_cutoff,
            "cohesion_threshold": cohesion_cutoff,
        }

    initialized_from_existing_clusters = False
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
        states = _load_clustering_states(cursor)
        initialized_from_existing_clusters = True
        logger.info(
            "Initialized incremental state from %s existing clusters",
            len(clusters_by_key),
        )

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
    baseline_initialized = bool(
        baseline_state_ids or initialized_from_existing_clusters
    )
    if baseline_state_ids:
        _persist_clustering_states(
            cursor,
            article_ids=baseline_state_ids,
            fingerprints=fingerprints,
            cluster_key_by_article=current_cluster_key_by_article,
        )
        conn.commit()
        states = _load_clustering_states(cursor)

    stable_unclustered_ids = {
        article_id
        for article_id in eligible_article_ids
        if (
            article_id not in current_cluster_key_by_article
            and article_id in states
            and states[article_id]["input_fingerprint_sha256"]
            == fingerprints[article_id]
            and states[article_id]["clustering_status"]
            in {
                CLUSTERING_STATUS_UNCLUSTERED,
                CLUSTERING_STATUS_BASELINE_UNCLUSTERED,
            }
        )
    }
    if stable_unclustered_ids:
        singleton_clusters = _singleton_story_clusters(
            stable_unclustered_ids
        )
        _persist_clusters(
            cursor=cursor,
            clusters=singleton_clusters,
            article_by_id=article_by_id,
            model_name=selected_model,
            model_revision=selected_revision,
            text_variant="single_source_passthrough",
            similarity_threshold=threshold,
            representative_threshold=representative_cutoff,
            cohesion_threshold=cohesion_cutoff,
            existing_cluster_keys=existing_cluster_keys,
        )
        conn.commit()
        existing_cluster_keys = _load_existing_cluster_keys(cursor)
        clusters_by_key, current_cluster_key_by_article = (
            _load_current_cluster_memberships(cursor)
        )
        _persist_clustering_states(
            cursor,
            article_ids=stable_unclustered_ids,
            fingerprints=fingerprints,
            cluster_key_by_article=current_cluster_key_by_article,
        )
        conn.commit()
        states = _load_clustering_states(cursor)
        singleton_clusters_backfilled = len(stable_unclustered_ids)
        logger.info(
            "Backfilled %s single-source story groups without embeddings",
            singleton_clusters_backfilled,
        )

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
    } | (set(force_article_ids or set()) & eligible_article_ids)
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
        logger.info("Clustering summary:")
        logger.info("  No new, changed, or retired clustering inputs.")
        logger.info("  Embeddings generated: 0")
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
    embedding_metrics = _empty_embedding_metrics(use_embedding_cache)
    linked_pairs = []
    build_result = ClusterBuildResult([], 0, 0, 0, 0, 0)
    if len(affected_articles) >= config.cluster_min_articles:
        embeddings, embedder, embedding_metrics = _embed_articles_with_cache(
            affected_articles,
            cursor=cursor,
            model_name=selected_model,
            model_revision=selected_revision,
            batch_size=config.cluster_batch_size,
            use_embedding_cache=use_embedding_cache,
        )
        scored_pairs = _score_pairs(candidates, embeddings)
        linked_pairs = [
            pair
            for pair in scored_pairs
            if pair.similarity_score >= threshold
        ]
        linked_before_constraints = len(linked_pairs)
        linked_pairs = [
            pair
            for pair in linked_pairs
            if (
                min(pair.left_id, pair.right_id),
                max(pair.left_id, pair.right_id),
            )
            not in different_event_pairs
        ]
        direct_constraint_pairs_removed = (
            linked_before_constraints - len(linked_pairs)
        )
        linked_pairs, component_constraint_pairs_removed = (
            _prevent_incompatible_components(
                linked_pairs=linked_pairs,
                incompatible_pairs=different_event_pairs,
            )
        )
        semantic_constraint_pairs_removed = (
            direct_constraint_pairs_removed
            + component_constraint_pairs_removed
        )
        linked_pairs, bridge_pairs_removed = _prevent_bridge_merges(
            linked_pairs=linked_pairs,
            changed_article_ids=changed_article_ids,
            clusters_by_key=clusters_by_key,
            cluster_key_by_article=current_cluster_key_by_article,
            embeddings=embeddings,
            representative_threshold=representative_cutoff,
            incompatible_pairs=different_event_pairs,
        )
        build_result = _build_story_clusters(
            affected_articles,
            linked_pairs,
            embeddings=embeddings,
            min_articles=config.cluster_min_articles,
            representative_threshold=representative_cutoff,
            cohesion_threshold=cohesion_cutoff,
        )
    elif affected_articles:
        semantic_constraint_pairs_removed = 0
        bridge_pairs_removed = 0
        build_result = _build_story_clusters(
            affected_articles,
            [],
            embeddings={},
            min_articles=config.cluster_min_articles,
            representative_threshold=representative_cutoff,
            cohesion_threshold=cohesion_cutoff,
        )

    else:
        semantic_constraint_pairs_removed = 0
        bridge_pairs_removed = 0

    resolved_model_name = str(
        embedding_metrics.get("resolved_model_name") or selected_model
    )
    resolved_model_revision = (
        embedding_metrics.get("resolved_model_revision") or selected_revision
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
        text_variant="title_lead_embedding_candidates_v2",
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

    logger.info("Clustering summary:")
    logger.info(
        "  Articles reconsidered: %s (changed articles and nearby context)",
        len(affected_articles),
    )
    logger.info(
        "  Embeddings: %s reused, %s cache misses, %s newly encoded in %.3fs",
        embedding_metrics["embedding_cache_hits"],
        embedding_metrics["embedding_cache_misses"],
        embedding_metrics["embedding_encoded_vectors"],
        embedding_metrics["embedding_total_seconds"],
    )
    logger.info(
        "  Candidate comparisons: %s (article pairs inside the configured window)",
        len(candidates),
    )
    logger.info(
        "  Same-event links retained: %s (after thresholds and safeguards)",
        len(linked_pairs),
    )
    if build_result.unclustered_articles:
        logger.info(
            "  Standalone stories: %s (not safely grouped with another article)",
            build_result.unclustered_articles,
        )
    if bridge_pairs_removed:
        logger.info(
            "  Ambiguous bridge links removed: %s",
            bridge_pairs_removed,
        )
    if semantic_constraint_pairs_removed:
        logger.info(
            "  Previously audited different-event links removed: %s",
            semantic_constraint_pairs_removed,
        )
    logger.info(
        "  Previously stored groups replaced: %s | Unrelated groups kept: %s",
        len(affected_cluster_ids),
        len(clusters_by_key) - len(affected_cluster_ids),
    )
    logger.info(
        "  Final story groups: %s covering %s eligible articles",
        story_clusters,
        clustered_articles,
    )

    return {
        "eligible_articles": len(articles),
        "changed_articles": len(changed_article_ids),
        "retired_articles": len(retired_article_ids),
        "affected_articles": len(affected_article_ids),
        "candidate_pairs": len(candidates),
        "linked_pairs": len(linked_pairs),
        "semantic_constraint_pairs_removed": (
            semantic_constraint_pairs_removed
        ),
        "bridge_pairs_removed": bridge_pairs_removed,
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
        "singleton_articles": build_result.unclustered_articles,
        "cohesion_fallback_members": (
            build_result.cohesion_fallback_members
        ),
        "singleton_clusters_backfilled": singleton_clusters_backfilled,
        "stable_cluster_keys_reused": stable_cluster_keys_reused,
        "baseline_initialized": baseline_initialized,
        "incremental_noop": False,
        "embedding_articles": len(affected_articles),
        **{
            key: value
            for key, value in embedding_metrics.items()
            if not key.startswith("resolved_")
        },
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
            clusters.representative_article_id,
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
                "representative_article_id": (
                    int(row["representative_article_id"])
                    if row["representative_article_id"] is not None
                    else int(row["article_id"])
                ),
                "article_ids": set(),
            },
        )
        article_id = int(row["article_id"])
        cluster["article_ids"].add(article_id)
        cluster_key_by_article[article_id] = cluster_key
    return clusters_by_key, cluster_key_by_article


def _prevent_bridge_merges(
    *,
    linked_pairs: list[ScoredPair],
    changed_article_ids: set[int],
    clusters_by_key: dict[str, dict],
    cluster_key_by_article: dict[int, str],
    embeddings: dict[int, list[float]],
    representative_threshold: float,
    incompatible_pairs: Optional[set[tuple[int, int]]] = None,
) -> tuple[list[ScoredPair], int]:
    """Stop one new broad article from collapsing unrelated old stories."""
    removed: set[tuple[int, int]] = set()
    for changed_id in sorted(changed_article_ids):
        links_by_old_cluster: dict[str, list[ScoredPair]] = defaultdict(list)
        for pair in linked_pairs:
            if changed_id not in {pair.left_id, pair.right_id}:
                continue
            neighbor_id = (
                pair.right_id if pair.left_id == changed_id else pair.left_id
            )
            old_key = cluster_key_by_article.get(neighbor_id)
            if old_key is not None:
                links_by_old_cluster[old_key].append(pair)
        if len(links_by_old_cluster) < 2:
            continue

        representatives = [
            int(clusters_by_key[key]["representative_article_id"])
            for key in sorted(links_by_old_cluster)
        ]
        incompatible_pairs = incompatible_pairs or set()
        representatives_are_compatible = all(
            (
                min(left_id, right_id),
                max(left_id, right_id),
            )
            not in incompatible_pairs
            and cosine_similarity(
                embeddings.get(left_id, []),
                embeddings.get(right_id, []),
            )
            >= representative_threshold
            for index, left_id in enumerate(representatives)
            for right_id in representatives[index + 1 :]
        )
        if representatives_are_compatible:
            continue

        strongest_key = max(
            links_by_old_cluster,
            key=lambda key: (
                max(
                    pair.similarity_score
                    for pair in links_by_old_cluster[key]
                ),
                key,
            ),
        )
        for key, pairs in links_by_old_cluster.items():
            if key == strongest_key:
                continue
            removed.update(
                (min(pair.left_id, pair.right_id), max(pair.left_id, pair.right_id))
                for pair in pairs
            )

    if not removed:
        return linked_pairs, 0
    retained = [
        pair
        for pair in linked_pairs
        if (min(pair.left_id, pair.right_id), max(pair.left_id, pair.right_id))
        not in removed
    ]
    return retained, len(linked_pairs) - len(retained)


def _prevent_incompatible_components(
    *,
    linked_pairs: list[ScoredPair],
    incompatible_pairs: set[tuple[int, int]],
) -> tuple[list[ScoredPair], int]:
    """Keep audited different-event articles out of the same component.

    Removing only the direct link is insufficient because an otherwise
    unreviewed article can bridge two audited-incompatible groups. Links are
    considered strongest-first so a new article joins the best-supported
    compatible component without needlessly forcing it to a singleton.
    """
    if not linked_pairs or not incompatible_pairs:
        return linked_pairs, 0

    article_ids = sorted(
        {
            article_id
            for pair in linked_pairs
            for article_id in (pair.left_id, pair.right_id)
        }
    )
    union_find = UnionFind(article_ids)
    members_by_root = {article_id: {article_id} for article_id in article_ids}
    retained: list[ScoredPair] = []
    removed = 0

    for pair in sorted(
        linked_pairs,
        key=lambda item: (
            -item.similarity_score,
            min(item.left_id, item.right_id),
            max(item.left_id, item.right_id),
        ),
    ):
        left_root = union_find.find(pair.left_id)
        right_root = union_find.find(pair.right_id)
        if left_root == right_root:
            retained.append(pair)
            continue

        left_members = members_by_root[left_root]
        right_members = members_by_root[right_root]
        would_violate = any(
            (min(left_id, right_id), max(left_id, right_id))
            in incompatible_pairs
            for left_id in left_members
            for right_id in right_members
        )
        if would_violate:
            removed += 1
            continue

        union_find.union(left_root, right_root)
        merged_root = union_find.find(left_root)
        retired_root = (
            right_root if merged_root == left_root else left_root
        )
        members_by_root[merged_root] = left_members | right_members
        members_by_root.pop(retired_root, None)
        retained.append(pair)

    retained_ids = {
        (min(pair.left_id, pair.right_id), max(pair.left_id, pair.right_id))
        for pair in retained
    }
    return (
        [
            pair
            for pair in linked_pairs
            if (min(pair.left_id, pair.right_id), max(pair.left_id, pair.right_id))
            in retained_ids
        ],
        removed,
    )


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


def _empty_embedding_metrics(use_embedding_cache: bool) -> dict:
    return {
        "embedding_cache_enabled": bool(use_embedding_cache),
        "embedding_cache_hits": 0,
        "embedding_cache_misses": 0,
        "embedding_encoded_vectors": 0,
        "embedding_cache_lookup_seconds": 0.0,
        "embedding_model_load_seconds": 0.0,
        "embedding_encoding_seconds": 0.0,
        "embedding_cache_write_seconds": 0.0,
        "embedding_total_seconds": 0.0,
        "resolved_model_name": None,
        "resolved_model_revision": None,
    }


def _embed_articles_with_cache(
    articles,
    *,
    cursor,
    model_name: str,
    model_revision: Optional[str],
    batch_size: int,
    use_embedding_cache: bool,
):
    started = perf_counter()
    metrics = _empty_embedding_metrics(use_embedding_cache)
    fingerprints = [
        embedding_input_fingerprint(model_name, article.similarity_text)
        for article in articles
    ]
    cached_by_fingerprint: dict[str, list[float]] = {}
    resolved_revision = model_revision
    embedder = None

    if resolved_revision is None:
        model_load_started = perf_counter()
        embedder = create_embedder(model_name, model_revision)
        metrics["embedding_model_load_seconds"] = round(
            perf_counter() - model_load_started,
            6,
        )
        model_name = embedder.model_name
        resolved_revision = embedder.model_revision

    if use_embedding_cache and resolved_revision:
        lookup_started = perf_counter()
        cached_by_fingerprint = load_cached_vectors(
            cursor,
            input_fingerprints=fingerprints,
            model_name=model_name,
            model_revision=str(resolved_revision),
        )
        metrics["embedding_cache_lookup_seconds"] = round(
            perf_counter() - lookup_started,
            6,
        )

    missing_indexes = [
        index
        for index, fingerprint in enumerate(fingerprints)
        if fingerprint not in cached_by_fingerprint
    ]
    metrics["embedding_cache_hits"] = len(articles) - len(missing_indexes)
    metrics["embedding_cache_misses"] = len(missing_indexes)

    if missing_indexes:
        if embedder is None:
            model_load_started = perf_counter()
            embedder = create_embedder(model_name, resolved_revision)
            metrics["embedding_model_load_seconds"] = round(
                perf_counter() - model_load_started,
                6,
            )
        actual_model_name = embedder.model_name
        actual_revision = embedder.model_revision
        if (
            cached_by_fingerprint
            and (
                actual_model_name != model_name
                or actual_revision != resolved_revision
            )
        ):
            raise RuntimeError(
                "embedding model resolved differently from the pinned cache key"
            )
        model_name = actual_model_name
        resolved_revision = actual_revision
        texts = [articles[index].similarity_text for index in missing_indexes]
        encoding_started = perf_counter()
        vectors = embedder.encode(texts, batch_size=batch_size)
        metrics["embedding_encoding_seconds"] = round(
            perf_counter() - encoding_started,
            6,
        )
        encoded_by_index = {
            index: vector
            for index, vector in zip(missing_indexes, vectors)
            if vector
        }
        metrics["embedding_encoded_vectors"] = len(encoded_by_index)
        if use_embedding_cache and resolved_revision:
            write_started = perf_counter()
            persist_cached_vectors(
                cursor,
                vectors_by_fingerprint={
                    fingerprints[index]: vector
                    for index, vector in encoded_by_index.items()
                },
                model_name=model_name,
                model_revision=str(resolved_revision),
            )
            metrics["embedding_cache_write_seconds"] = round(
                perf_counter() - write_started,
                6,
            )
    else:
        encoded_by_index = {}

    embeddings = {
        article.id: (
            cached_by_fingerprint.get(fingerprints[index])
            or encoded_by_index.get(index)
        )
        for index, article in enumerate(articles)
        if (
            cached_by_fingerprint.get(fingerprints[index])
            or encoded_by_index.get(index)
        )
    }
    metrics["resolved_model_name"] = model_name
    metrics["resolved_model_revision"] = resolved_revision
    metrics["embedding_total_seconds"] = round(perf_counter() - started, 6)
    return embeddings, embedder, metrics


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
        singleton_clusters = _singleton_story_clusters(
            article.id for article in articles
        )
        return ClusterBuildResult(
            singleton_clusters,
            0,
            0,
            0,
            len(singleton_clusters),
            0,
        )

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
    for component in initial_components:
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
    singleton_article_ids = set(article_ids) - clustered_article_ids
    clusters.extend(_singleton_story_clusters(singleton_article_ids))
    guarded_singleton_count = sum(
        len(cluster.article_ids) == 1 for cluster in clusters
    )
    return ClusterBuildResult(
        clusters=sorted(clusters, key=lambda cluster: cluster.article_ids),
        initial_components=len(initial_components),
        changed_components=changed_components,
        split_components=split_components,
        unclustered_articles=guarded_singleton_count,
        cohesion_fallback_members=cohesion_fallback_members,
    )


def _singleton_story_clusters(article_ids) -> list[StoryCluster]:
    return [
        StoryCluster(
            article_ids=[article_id],
            representative_article_id=article_id,
            confidence=1.0,
            member_scores={article_id: 1.0},
        )
        for article_id in sorted(article_ids)
    ]


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
