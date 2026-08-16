from __future__ import annotations

import hashlib
from datetime import datetime
from itertools import combinations
from typing import Optional, Sequence


def semantic_content_sha256(*, title: str, clean_text: str) -> str:
    payload = f"{str(title or '').strip()}\n{str(clean_text or '').strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_active_different_event_pairs(
    cursor,
    *,
    article_by_id,
) -> set[tuple[int, int]]:
    active: set[tuple[int, int]] = set()
    rows = cursor.execute(
        """
        SELECT left_article_id, right_article_id,
               left_content_sha256, right_content_sha256
        FROM semantic_pair_constraints
        WHERE decision = 'different_event'
        """
    ).fetchall()
    for row in rows:
        left_id = int(row["left_article_id"])
        right_id = int(row["right_article_id"])
        left = article_by_id.get(left_id)
        right = article_by_id.get(right_id)
        if left is None or right is None:
            continue
        left_sha = semantic_content_sha256(
            title=left.title,
            clean_text=left.clean_text,
        )
        right_sha = semantic_content_sha256(
            title=right.title,
            clean_text=right.clean_text,
        )
        if (
            left_sha == str(row["left_content_sha256"])
            and right_sha == str(row["right_content_sha256"])
        ):
            active.add((left_id, right_id))
    return active


def persist_different_event_constraints(
    connection,
    *,
    groups: Sequence[Sequence[int]],
    audit_version: str,
    source_cluster_key: str,
    created_at: Optional[str] = None,
) -> int:
    normalized = tuple(
        tuple(sorted(int(value) for value in group)) for group in groups
    )
    member_ids = {article_id for group in normalized for article_id in group}
    if not member_ids:
        return 0
    articles = {
        int(row["id"]): row
        for row in connection.execute(
            """
            SELECT id, title, clean_text
            FROM articles
            WHERE id IN ({})
            """.format(",".join("?" for _ in member_ids)),
            tuple(sorted(member_ids)),
        )
    }
    if set(articles) != member_ids:
        raise ValueError("semantic constraint article is missing")
    group_index = {
        article_id: index
        for index, group in enumerate(normalized)
        for article_id in group
    }
    timestamp = created_at or datetime.now().isoformat(timespec="seconds")
    rows = []
    for left_id, right_id in combinations(sorted(member_ids), 2):
        if group_index[left_id] == group_index[right_id]:
            continue
        left = articles[left_id]
        right = articles[right_id]
        rows.append(
            (
                left_id,
                right_id,
                semantic_content_sha256(
                    title=left["title"],
                    clean_text=left["clean_text"],
                ),
                semantic_content_sha256(
                    title=right["title"],
                    clean_text=right["clean_text"],
                ),
                "different_event",
                audit_version,
                source_cluster_key,
                timestamp,
            )
        )
    connection.executemany(
        """
        INSERT INTO semantic_pair_constraints (
            left_article_id, right_article_id,
            left_content_sha256, right_content_sha256,
            decision, audit_version, source_cluster_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(left_article_id, right_article_id) DO UPDATE SET
            left_content_sha256 = excluded.left_content_sha256,
            right_content_sha256 = excluded.right_content_sha256,
            decision = excluded.decision,
            audit_version = excluded.audit_version,
            source_cluster_key = excluded.source_cluster_key,
            created_at = excluded.created_at
        """,
        rows,
    )
    return len(rows)
