from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence

from news_pipeline.clustering.event_clusterer import (
    _build_cluster_transitions,
    _cluster_key,
    _persist_cluster_transitions,
)
from news_pipeline.clustering.semantic_constraints import (
    persist_different_event_constraints,
)


@dataclass(frozen=True)
class SemanticPartitionResult:
    old_cluster_key: str
    new_cluster_keys: tuple[str, ...]
    multi_article_cluster_keys: tuple[str, ...]
    group_count: int
    singleton_count: int


def validate_semantic_partition(
    article_ids: Iterable[int],
    groups: Sequence[Sequence[int]],
) -> tuple[tuple[int, ...], ...]:
    """Validate an exhaustive, non-overlapping, genuinely smaller partition."""
    expected = {int(article_id) for article_id in article_ids}
    if not expected:
        raise ValueError("semantic partition source must not be empty")
    if len(groups) < 2:
        raise ValueError("semantic partition must contain at least two groups")

    normalized: list[tuple[int, ...]] = []
    flattened: list[int] = []
    for group in groups:
        values = tuple(sorted(int(article_id) for article_id in group))
        if not values:
            raise ValueError("semantic partition groups must not be empty")
        if len(values) != len(set(values)):
            raise ValueError("semantic partition group contains duplicates")
        normalized.append(values)
        flattened.extend(values)

    if len(flattened) != len(set(flattened)):
        raise ValueError("semantic partition groups overlap")
    if set(flattened) != expected:
        raise ValueError("semantic partition must cover the source exactly")
    if any(len(group) >= len(expected) for group in normalized):
        raise ValueError("semantic partition must make every group smaller")
    return tuple(sorted(normalized))


def apply_semantic_partition(
    connection,
    *,
    cluster_key: str,
    groups: Sequence[Sequence[int]],
    audit_version: str,
) -> SemanticPartitionResult:
    """Atomically replace one candidate with audited same-event groups."""
    savepoint = "semantic_partition_apply"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        result = _apply_semantic_partition(
            connection,
            cluster_key=cluster_key,
            groups=groups,
            audit_version=audit_version,
        )
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        connection.commit()
        return result
    except Exception:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise


def _apply_semantic_partition(
    connection,
    *,
    cluster_key: str,
    groups: Sequence[Sequence[int]],
    audit_version: str,
) -> SemanticPartitionResult:
    """Replace one embedding cluster with model-audited same-event groups."""
    cluster = connection.execute(
        "SELECT * FROM story_clusters WHERE cluster_key = ?",
        (cluster_key,),
    ).fetchone()
    if cluster is None:
        raise ValueError(f"semantic partition cluster not found: {cluster_key}")
    member_rows = connection.execute(
        """
        SELECT article_id, similarity_score, is_representative
        FROM story_cluster_members
        WHERE cluster_id = ?
        ORDER BY article_id
        """,
        (int(cluster["id"]),),
    ).fetchall()
    member_ids = {int(row["article_id"]) for row in member_rows}
    normalized = validate_semantic_partition(member_ids, groups)
    member_by_id = {int(row["article_id"]): row for row in member_rows}
    created_at = datetime.now().isoformat(timespec="seconds")

    persist_different_event_constraints(
        connection,
        groups=normalized,
        audit_version=audit_version,
        source_cluster_key=cluster_key,
        created_at=created_at,
    )

    old_representative = int(cluster["representative_article_id"])
    model_name = str(cluster["model_name"])
    model_revision = str(cluster["model_revision"] or "")
    similarity_threshold = float(cluster["similarity_threshold"])
    representative_threshold = float(
        cluster["representative_threshold"]
        if cluster["representative_threshold"] is not None
        else similarity_threshold
    )
    cohesion_threshold = float(
        cluster["cohesion_threshold"]
        if cluster["cohesion_threshold"] is not None
        else similarity_threshold
    )

    connection.execute(
        "DELETE FROM final_story_publication_states WHERE cluster_id = ?",
        (int(cluster["id"]),),
    )
    connection.execute(
        "DELETE FROM final_unified_stories WHERE cluster_id = ?",
        (int(cluster["id"]),),
    )
    connection.execute(
        """
        UPDATE clustering_article_state
        SET cluster_key = NULL
        WHERE cluster_key = ?
        """,
        (cluster_key,),
    )
    connection.execute(
        "DELETE FROM story_cluster_members WHERE cluster_id = ?",
        (int(cluster["id"]),),
    )
    connection.execute(
        "DELETE FROM story_clusters WHERE id = ?",
        (int(cluster["id"]),),
    )

    new_memberships: dict[str, set[int]] = {}
    multi_article_keys: list[str] = []
    for group in normalized:
        representative = (
            old_representative
            if old_representative in group
            else max(
                group,
                key=lambda article_id: (
                    float(member_by_id[article_id]["similarity_score"] or 0.0),
                    -article_id,
                ),
            )
        )
        new_key = _cluster_key(
            list(group),
            model_name,
            model_revision,
            similarity_threshold,
            representative_threshold,
            cohesion_threshold,
        )
        source_count = int(
            connection.execute(
                """
                SELECT COUNT(DISTINCT source) AS count
                FROM articles
                WHERE id IN ({})
                """.format(",".join("?" for _ in group)),
                tuple(group),
            ).fetchone()["count"]
        )
        event_dates = connection.execute(
            """
            SELECT MIN(COALESCE(published_date, crawl_timestamp)) AS start,
                   MAX(COALESCE(published_date, crawl_timestamp)) AS end
            FROM articles
            WHERE id IN ({})
            """.format(",".join("?" for _ in group)),
            tuple(group),
        ).fetchone()
        member_scores = {
            article_id: float(
                member_by_id[article_id]["similarity_score"] or 0.0
            )
            for article_id in group
        }
        confidence = (
            sum(member_scores.values()) / len(member_scores)
            if member_scores
            else 1.0
        )
        cursor = connection.execute(
            """
            INSERT INTO story_clusters (
                cluster_key, representative_article_id, model_name,
                model_revision, text_variant, similarity_threshold,
                representative_threshold, cohesion_threshold,
                event_date_start, event_date_end, article_count,
                source_count, confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_key,
                representative,
                model_name,
                model_revision,
                "title_lead_semantic_audit_v1",
                similarity_threshold,
                representative_threshold,
                cohesion_threshold,
                event_dates["start"],
                event_dates["end"],
                len(group),
                source_count,
                confidence,
                created_at,
            ),
        )
        cluster_id = int(cursor.lastrowid)
        connection.executemany(
            """
            INSERT INTO story_cluster_members (
                cluster_id, article_id, similarity_score, is_representative
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (
                    cluster_id,
                    article_id,
                    member_scores[article_id],
                    1 if article_id == representative else 0,
                )
                for article_id in group
            ],
        )
        connection.executemany(
            """
            UPDATE clustering_article_state
            SET clustering_status = 'clustered', cluster_key = ?,
                processed_at = ?
            WHERE article_id = ?
            """,
            [(new_key, created_at, article_id) for article_id in group],
        )
        new_memberships[new_key] = set(group)
        if len(group) >= 2:
            multi_article_keys.append(new_key)

    transitions = _build_cluster_transitions(
        {cluster_key: member_ids},
        new_memberships,
    )
    if transitions:
        _persist_cluster_transitions(
            connection.cursor(),
            transitions=transitions,
        )
    return SemanticPartitionResult(
        old_cluster_key=cluster_key,
        new_cluster_keys=tuple(sorted(new_memberships)),
        multi_article_cluster_keys=tuple(sorted(multi_article_keys)),
        group_count=len(normalized),
        singleton_count=sum(len(group) == 1 for group in normalized),
    )
