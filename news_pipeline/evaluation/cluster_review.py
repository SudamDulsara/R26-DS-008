import csv
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from news_pipeline.config import load_config
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger


logger = get_logger()

REVIEW_LABEL_HELP = "same_event | mixed_related | wrong_cluster | unsure"


def make_snippet(text: str, max_chars: int = 280) -> str:
    normalized = re.sub(r"\s+", " ", text or "").strip()
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3].rstrip() + "..."


def build_cluster_flags(cluster: dict) -> list[str]:
    flags = []
    if cluster["article_count"] >= 10:
        flags.append("large_cluster")
    if cluster["source_count"] <= 1:
        flags.append("single_source")
    if cluster["confidence"] is not None and cluster["confidence"] < 0.93:
        flags.append("borderline_confidence")
    return flags


def build_member_flags(
    members: list[dict],
    representative_threshold: Optional[float] = None,
) -> list[str]:
    flags = []
    has_indirect_member = any(
        not member.get("is_representative")
        and float(member.get("similarity_score") or 0.0) == 0.0
        for member in members
    )
    if has_indirect_member:
        flags.append("indirect_graph_member")
    has_cohesion_fallback_member = (
        representative_threshold is not None
        and any(
            not member.get("is_representative")
            and 0.0 < float(member.get("similarity_score") or 0.0)
            < representative_threshold
            for member in members
        )
    )
    if has_cohesion_fallback_member:
        flags.append("cohesion_fallback_member")
    return flags


def generate_cluster_review(
    output_dir: Optional[Path] = None,
    max_snippet_chars: int = 280,
):
    config = load_config()
    review_dir = output_dir or config.reviews_dir
    review_dir.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_path = review_dir / f"cluster_review_{generated_at}.csv"
    jsonl_path = review_dir / f"cluster_review_{generated_at}.jsonl"

    conn = get_connection()
    cursor = conn.cursor()

    clusters = _load_clusters(cursor)
    members_by_cluster = _load_members_by_cluster(cursor)
    conn.close()

    review_records = [
        _build_review_record(
            cluster=dict(cluster),
            members=[dict(member) for member in members_by_cluster[cluster["id"]]],
            max_snippet_chars=max_snippet_chars,
        )
        for cluster in clusters
    ]

    _write_jsonl(jsonl_path, review_records)
    _write_csv(csv_path, review_records)

    suspicious_count = sum(1 for record in review_records if record["flags"])

    logger.info("=== Cluster Review Export Complete ===")
    logger.info("Review clusters: %s", len(review_records))
    logger.info("Flagged clusters: %s", suspicious_count)
    logger.info("CSV: %s", csv_path)
    logger.info("JSONL: %s", jsonl_path)

    return {
        "clusters": len(review_records),
        "flagged_clusters": suspicious_count,
        "csv_path": str(csv_path),
        "jsonl_path": str(jsonl_path),
    }


def _load_clusters(cursor):
    cursor.execute(
        """
        SELECT
            id,
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
        FROM story_clusters
        ORDER BY article_count DESC, confidence ASC, id
        """
    )
    return cursor.fetchall()


def _load_members_by_cluster(cursor):
    cursor.execute(
        """
        SELECT
            members.cluster_id,
            members.article_id,
            members.similarity_score,
            members.is_representative,
            articles.url,
            articles.source,
            articles.title,
            articles.published_date,
            articles.clean_text
        FROM story_cluster_members AS members
        JOIN articles ON articles.id = members.article_id
        ORDER BY members.cluster_id, members.is_representative DESC, members.article_id
        """
    )

    members_by_cluster = defaultdict(list)
    for row in cursor.fetchall():
        members_by_cluster[row["cluster_id"]].append(row)
    return members_by_cluster


def _build_review_record(cluster: dict, members: list[dict], max_snippet_chars: int):
    representative = next(
        (member for member in members if member["is_representative"]),
        members[0] if members else {},
    )
    flags = build_cluster_flags(cluster) + build_member_flags(
        members,
        cluster.get("representative_threshold"),
    )

    return {
        "cluster_id": cluster["id"],
        "cluster_key": cluster["cluster_key"],
        "review_label": "",
        "review_label_help": REVIEW_LABEL_HELP,
        "review_notes": "",
        "flags": flags,
        "model_name": cluster["model_name"],
        "model_revision": cluster.get("model_revision") or "",
        "text_variant": cluster["text_variant"],
        "similarity_threshold": cluster["similarity_threshold"],
        "representative_threshold": cluster["representative_threshold"],
        "cohesion_threshold": cluster["cohesion_threshold"],
        "confidence": cluster["confidence"],
        "article_count": cluster["article_count"],
        "source_count": cluster["source_count"],
        "event_date_start": cluster["event_date_start"],
        "event_date_end": cluster["event_date_end"],
        "representative": _member_summary(representative, max_snippet_chars),
        "members": [
            _member_summary(member, max_snippet_chars)
            for member in members
        ],
    }


def _member_summary(member: dict, max_snippet_chars: int):
    return {
        "article_id": member.get("article_id"),
        "source": member.get("source", ""),
        "title": member.get("title", ""),
        "url": member.get("url", ""),
        "published_date": member.get("published_date", ""),
        "similarity_score": member.get("similarity_score"),
        "is_representative": bool(member.get("is_representative")),
        "snippet": make_snippet(member.get("clean_text", ""), max_snippet_chars),
    }


def _write_jsonl(path: Path, records: list[dict]):
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_csv(path: Path, records: list[dict]):
    fieldnames = [
        "cluster_id",
        "cluster_key",
        "review_label",
        "review_label_help",
        "review_notes",
        "flags",
        "model_name",
        "model_revision",
        "similarity_threshold",
        "representative_threshold",
        "cohesion_threshold",
        "confidence",
        "article_count",
        "source_count",
        "event_date_start",
        "event_date_end",
        "representative_article_id",
        "representative_source",
        "representative_title",
        "representative_url",
        "representative_snippet",
        "member_article_ids",
        "member_sources",
        "member_similarity_scores",
        "member_titles",
        "member_urls",
        "member_snippets",
    ]

    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            representative = record["representative"]
            members = record["members"]
            writer.writerow(
                {
                    "cluster_id": record["cluster_id"],
                    "cluster_key": record["cluster_key"],
                    "review_label": record["review_label"],
                    "review_label_help": record["review_label_help"],
                    "review_notes": record["review_notes"],
                    "flags": "; ".join(record["flags"]),
                    "model_name": record["model_name"],
                    "model_revision": record["model_revision"],
                    "similarity_threshold": record["similarity_threshold"],
                    "representative_threshold": record[
                        "representative_threshold"
                    ],
                    "cohesion_threshold": record["cohesion_threshold"],
                    "confidence": record["confidence"],
                    "article_count": record["article_count"],
                    "source_count": record["source_count"],
                    "event_date_start": record["event_date_start"],
                    "event_date_end": record["event_date_end"],
                    "representative_article_id": representative.get("article_id"),
                    "representative_source": representative.get("source"),
                    "representative_title": representative.get("title"),
                    "representative_url": representative.get("url"),
                    "representative_snippet": representative.get("snippet"),
                    "member_article_ids": _join_member_values(members, "article_id"),
                    "member_sources": _join_member_values(members, "source"),
                    "member_similarity_scores": _join_member_values(
                        members,
                        "similarity_score",
                    ),
                    "member_titles": _join_member_values(members, "title"),
                    "member_urls": _join_member_values(members, "url"),
                    "member_snippets": _join_member_values(members, "snippet"),
                }
            )


def _join_member_values(members: list[dict], key: str) -> str:
    values = []
    for member in members:
        value = member.get(key)
        values.append("" if value is None else str(value))
    return "\n---\n".join(values)
