import csv
import json
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from news_pipeline.evaluation.cluster_review import (
    REVIEW_LABEL_HELP,
    build_cluster_flags,
    build_member_flags,
    make_snippet,
)
from news_pipeline.storage.logger import get_logger


logger = get_logger()


def compare_cluster_databases(
    baseline_db_path: Path,
    candidate_db_path: Path,
    output_dir: Path,
    baseline_review_path: Optional[Path] = None,
    max_snippet_chars: int = 280,
):
    if max_snippet_chars < 20:
        raise ValueError("max_snippet_chars must be at least 20")
    baseline_clusters = load_cluster_records(baseline_db_path)
    candidate_clusters = load_cluster_records(candidate_db_path)
    baseline_memberships = {
        cluster_id: frozenset(record["member_article_ids"])
        for cluster_id, record in baseline_clusters.items()
    }
    candidate_memberships = {
        cluster_id: frozenset(record["member_article_ids"])
        for cluster_id, record in candidate_clusters.items()
    }
    comparison = compare_membership_sets(
        baseline_memberships,
        candidate_memberships,
    )
    baseline_reviews = _read_baseline_reviews(baseline_review_path)

    review_records = []
    for change in comparison["candidate_changes"]:
        candidate = candidate_clusters[change["candidate_cluster_id"]]
        review_records.append(
            _build_focused_review_record(
                candidate,
                change,
                baseline_memberships,
                baseline_reviews,
                max_snippet_chars,
            )
        )

    generated_at = datetime.now().astimezone()
    timestamp = generated_at.strftime("%Y-%m-%d_%H-%M-%S")
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"changed_cluster_review_{timestamp}.csv"
    json_path = output_dir / f"cluster_comparison_{timestamp}.json"
    markdown_path = output_dir / f"cluster_comparison_{timestamp}.md"
    _write_focused_csv(csv_path, review_records)

    report = {
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "baseline_database": str(baseline_db_path.resolve()),
        "candidate_database": str(candidate_db_path.resolve()),
        "baseline_review_file": (
            str(baseline_review_path.resolve())
            if baseline_review_path is not None
            else None
        ),
        **comparison,
        "focused_review_csv": str(csv_path.resolve()),
    }
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_render_markdown(report), encoding="utf-8")

    logger.info("=== Cluster Run Comparison Complete ===")
    logger.info(
        "Unchanged clusters: %s | Changed candidate clusters: %s",
        comparison["summary"]["unchanged_clusters"],
        comparison["summary"]["changed_candidate_clusters"],
    )
    logger.info(
        "Newly clustered articles: %s | No longer clustered: %s",
        comparison["summary"]["newly_clustered_articles"],
        comparison["summary"]["no_longer_clustered_articles"],
    )
    logger.info("Focused review: %s", csv_path)
    logger.info("Markdown: %s", markdown_path)

    return {
        **report,
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
        "csv_path": str(csv_path),
    }


def load_cluster_records(db_path: Path) -> dict[int, dict]:
    if not db_path.exists():
        raise FileNotFoundError(f"Cluster database not found: {db_path}")
    connection = sqlite3.connect(
        f"file:{db_path.resolve().as_posix()}?mode=ro",
        uri=True,
    )
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
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
        ORDER BY id
        """
    )
    clusters = {row["id"]: dict(row) for row in cursor.fetchall()}
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
        ORDER BY members.cluster_id, members.is_representative DESC,
                 members.article_id
        """
    )
    members_by_cluster = defaultdict(list)
    for row in cursor.fetchall():
        members_by_cluster[row["cluster_id"]].append(dict(row))
    connection.close()

    for cluster_id, cluster in clusters.items():
        members = members_by_cluster[cluster_id]
        cluster["members"] = members
        cluster["member_article_ids"] = [
            member["article_id"] for member in members
        ]
    return clusters


def compare_membership_sets(
    baseline_memberships: dict[int, frozenset[int]],
    candidate_memberships: dict[int, frozenset[int]],
) -> dict:
    baseline_by_members = {
        members: cluster_id for cluster_id, members in baseline_memberships.items()
    }
    candidate_by_members = {
        members: cluster_id for cluster_id, members in candidate_memberships.items()
    }
    unchanged_candidate_ids = sorted(
        cluster_id
        for cluster_id, members in candidate_memberships.items()
        if members in baseline_by_members
    )
    changed_baseline_ids = sorted(
        cluster_id
        for cluster_id, members in baseline_memberships.items()
        if members not in candidate_by_members
    )
    candidate_changes = []
    for candidate_id, candidate_members in sorted(candidate_memberships.items()):
        if candidate_members in baseline_by_members:
            continue
        overlaps = [
            {
                "baseline_cluster_id": baseline_id,
                "overlap_article_ids": sorted(candidate_members & baseline_members),
                "baseline_member_article_ids": sorted(baseline_members),
            }
            for baseline_id, baseline_members in sorted(baseline_memberships.items())
            if candidate_members & baseline_members
        ]
        overlapping_members = set().union(
            *(
                set(overlap["baseline_member_article_ids"])
                for overlap in overlaps
            )
        ) if overlaps else set()
        all_baseline_articles = set().union(
            *(set(members) for members in baseline_memberships.values())
        ) if baseline_memberships else set()
        candidate_changes.append(
            {
                "candidate_cluster_id": candidate_id,
                "change_type": _change_type(candidate_members, overlaps),
                "candidate_member_article_ids": sorted(candidate_members),
                "overlapping_baseline_clusters": overlaps,
                "newly_clustered_article_ids": sorted(
                    candidate_members - all_baseline_articles
                ),
                "removed_overlap_article_ids": sorted(
                    overlapping_members - candidate_members
                ),
            }
        )

    baseline_articles = set().union(
        *(set(members) for members in baseline_memberships.values())
    ) if baseline_memberships else set()
    candidate_articles = set().union(
        *(set(members) for members in candidate_memberships.values())
    ) if candidate_memberships else set()
    return {
        "summary": {
            "baseline_clusters": len(baseline_memberships),
            "candidate_clusters": len(candidate_memberships),
            "unchanged_clusters": len(unchanged_candidate_ids),
            "changed_baseline_clusters": len(changed_baseline_ids),
            "changed_candidate_clusters": len(candidate_changes),
            "baseline_clustered_articles": len(baseline_articles),
            "candidate_clustered_articles": len(candidate_articles),
            "newly_clustered_articles": len(candidate_articles - baseline_articles),
            "no_longer_clustered_articles": len(
                baseline_articles - candidate_articles
            ),
        },
        "unchanged_candidate_cluster_ids": unchanged_candidate_ids,
        "changed_baseline_cluster_ids": changed_baseline_ids,
        "newly_clustered_article_ids": sorted(candidate_articles - baseline_articles),
        "no_longer_clustered_article_ids": sorted(
            baseline_articles - candidate_articles
        ),
        "candidate_changes": candidate_changes,
    }


def _change_type(candidate_members: frozenset[int], overlaps: list[dict]) -> str:
    if not overlaps:
        return "new_cluster"
    baseline_sets = [
        set(overlap["baseline_member_article_ids"]) for overlap in overlaps
    ]
    if len(overlaps) > 1:
        return (
            "merged_clusters"
            if set(candidate_members) == set().union(*baseline_sets)
            else "reconfigured_clusters"
        )
    baseline_members = baseline_sets[0]
    if baseline_members < set(candidate_members):
        return "expanded_cluster"
    if set(candidate_members) < baseline_members:
        return "contracted_cluster"
    return "reconfigured_cluster"


def _build_focused_review_record(
    cluster: dict,
    change: dict,
    baseline_memberships: dict[int, frozenset[int]],
    baseline_reviews: dict[str, dict],
    max_snippet_chars: int,
) -> dict:
    members = cluster["members"]
    representative = next(
        (member for member in members if member["is_representative"]),
        members[0] if members else {},
    )
    baseline_cluster_by_article = {
        article_id: cluster_id
        for cluster_id, article_ids in baseline_memberships.items()
        for article_id in article_ids
    }
    overlap_ids = [
        overlap["baseline_cluster_id"]
        for overlap in change["overlapping_baseline_clusters"]
    ]
    overlap_summary = []
    baseline_label_summary = []
    for overlap in change["overlapping_baseline_clusters"]:
        baseline_id = overlap["baseline_cluster_id"]
        review = baseline_reviews.get(str(baseline_id), {})
        label = review.get("review_label") or "not_recorded"
        baseline_label_summary.append(f"{baseline_id}:{label}")
        overlap_summary.append(
            f"{baseline_id} ({label}) overlap "
            f"{','.join(map(str, overlap['overlap_article_ids']))}; baseline "
            f"{','.join(map(str, overlap['baseline_member_article_ids']))}"
        )
    flags = build_cluster_flags(cluster) + build_member_flags(
        members,
        cluster.get("representative_threshold"),
    )
    return {
        "cluster_id": cluster["id"],
        "cluster_key": cluster["cluster_key"],
        "change_type": change["change_type"],
        "review_label": "",
        "review_label_help": REVIEW_LABEL_HELP,
        "review_notes": "",
        "baseline_cluster_ids": "; ".join(map(str, overlap_ids)),
        "baseline_review_labels": "; ".join(baseline_label_summary),
        "baseline_overlap_summary": "\n".join(overlap_summary),
        "newly_clustered_article_ids": "; ".join(
            map(str, change["newly_clustered_article_ids"])
        ),
        "removed_overlap_article_ids": "; ".join(
            map(str, change["removed_overlap_article_ids"])
        ),
        "flags": "; ".join(flags),
        "model_name": cluster["model_name"],
        "model_revision": cluster.get("model_revision") or "",
        "similarity_threshold": cluster["similarity_threshold"],
        "representative_threshold": cluster["representative_threshold"],
        "cohesion_threshold": cluster["cohesion_threshold"],
        "confidence": cluster["confidence"],
        "article_count": cluster["article_count"],
        "source_count": cluster["source_count"],
        "event_date_start": cluster["event_date_start"],
        "event_date_end": cluster["event_date_end"],
        "representative_article_id": representative.get("article_id"),
        "representative_source": representative.get("source", ""),
        "representative_title": representative.get("title", ""),
        "representative_url": representative.get("url", ""),
        "representative_snippet": make_snippet(
            representative.get("clean_text", ""),
            max_snippet_chars,
        ),
        "member_article_ids": _join(members, "article_id"),
        "member_baseline_cluster_ids": "\n---\n".join(
            str(baseline_cluster_by_article.get(member["article_id"], ""))
            for member in members
        ),
        "member_sources": _join(members, "source"),
        "member_similarity_scores": _join(members, "similarity_score"),
        "member_titles": _join(members, "title"),
        "member_urls": _join(members, "url"),
        "member_snippets": "\n---\n".join(
            make_snippet(member.get("clean_text", ""), max_snippet_chars)
            for member in members
        ),
    }


def _read_baseline_reviews(path: Optional[Path]) -> dict[str, dict]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"Baseline review file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    reviews = {}
    for row in rows:
        cluster_id = str(row.get("cluster_id") or "").strip()
        if cluster_id:
            reviews[cluster_id] = row
    return reviews


def _write_focused_csv(path: Path, records: list[dict]):
    fieldnames = [
        "cluster_id",
        "cluster_key",
        "change_type",
        "review_label",
        "review_label_help",
        "review_notes",
        "baseline_cluster_ids",
        "baseline_review_labels",
        "baseline_overlap_summary",
        "newly_clustered_article_ids",
        "removed_overlap_article_ids",
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
        "member_baseline_cluster_ids",
        "member_sources",
        "member_similarity_scores",
        "member_titles",
        "member_urls",
        "member_snippets",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def _render_markdown(report: dict) -> str:
    summary = report["summary"]
    lines = [
        "# Cluster Run Comparison",
        "",
        f"Generated: {report['generated_at']}",
        "",
        "## Summary",
        "",
        "| Metric | Baseline | Candidate |",
        "| --- | ---: | ---: |",
        (
            f"| Story clusters | {summary['baseline_clusters']} | "
            f"{summary['candidate_clusters']} |"
        ),
        (
            f"| Clustered articles | {summary['baseline_clustered_articles']} | "
            f"{summary['candidate_clustered_articles']} |"
        ),
        "",
        f"- Unchanged membership sets: {summary['unchanged_clusters']}",
        f"- Changed baseline clusters: {summary['changed_baseline_clusters']}",
        f"- Changed candidate clusters: {summary['changed_candidate_clusters']}",
        f"- Newly clustered articles: {summary['newly_clustered_articles']}",
        f"- No-longer-clustered articles: {summary['no_longer_clustered_articles']}",
        "",
        "## Candidate changes",
        "",
        "| Candidate cluster | Change | Members | Baseline overlaps | Newly clustered |",
        "| ---: | --- | --- | --- | --- |",
    ]
    for change in report["candidate_changes"]:
        overlap_ids = ", ".join(
            str(overlap["baseline_cluster_id"])
            for overlap in change["overlapping_baseline_clusters"]
        ) or "none"
        members = ", ".join(map(str, change["candidate_member_article_ids"]))
        new_ids = ", ".join(map(str, change["newly_clustered_article_ids"])) or "none"
        lines.append(
            f"| {change['candidate_cluster_id']} | {change['change_type']} | "
            f"{members} | {overlap_ids} | {new_ids} |"
        )
    lines.extend(
        [
            "",
            f"Focused review CSV: `{report['focused_review_csv']}`",
            "",
        ]
    )
    return "\n".join(lines)


def _join(members: list[dict], key: str) -> str:
    return "\n---\n".join(
        "" if member.get(key) is None else str(member.get(key))
        for member in members
    )
