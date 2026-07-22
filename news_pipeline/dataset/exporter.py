import json
from collections import defaultdict
from datetime import datetime

from news_pipeline.config import load_config
from news_pipeline.statuses import CLEAN_STATUS_CLEANED, DEDUPE_STATUS_UNIQUE
from news_pipeline.storage.database import get_connection
from news_pipeline.storage.logger import get_logger
from news_pipeline.unification.contract import (
    UNIFIED_STORY_SCHEMA_VERSION,
    unified_story_contract_metadata,
    validate_unified_story_record,
)
from news_pipeline.unification.evidence import build_sentence_evidence
from news_pipeline.unification.near_duplicates import (
    group_near_duplicate_evidence,
)
from news_pipeline.unification.selector import select_extractive_story
from news_pipeline.unification.titles import build_display_title


logger = get_logger()


def _write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_unified_story_rows(
    cluster_rows: list[dict],
    cluster_member_rows: list[dict],
    article_records_by_id: dict[int, dict],
) -> list[dict]:
    members_by_cluster = defaultdict(list)
    for member in cluster_member_rows:
        members_by_cluster[member["cluster_id"]].append(member)

    unified_stories = []
    for cluster in cluster_rows:
        members = members_by_cluster.get(cluster["id"], [])
        representative_id = cluster.get("representative_article_id")
        representative = article_records_by_id.get(representative_id)
        representative_member = next(
            (
                member
                for member in members
                if member["article_id"] == representative_id
            ),
            None,
        )

        if representative is not None:
            representative_article = {
                "article_id": representative_id,
                "url": representative.get("url"),
                "source": representative.get("source"),
                "title": representative.get("title"),
                "author": representative.get("author"),
                "published_date": representative.get("published_date"),
                "category": representative.get("category"),
            }
            title = representative.get("title")
            clean_text = representative.get("clean_text")
        elif representative_member is not None:
            representative_article = {
                "article_id": representative_id,
                "url": representative_member.get("url"),
                "source": representative_member.get("source"),
                "title": representative_member.get("title"),
                "author": None,
                "published_date": representative_member.get("published_date"),
                "category": None,
            }
            title = representative_member.get("title")
            clean_text = None
        else:
            representative_article = None
            title = None
            clean_text = None

        extractive_story, sentence_evidence = _build_extractive_story(
            members,
            article_records_by_id,
        )
        display_title = build_display_title(
            title,
            extractive_story["unified_sentences"],
            representative_clean_text=clean_text,
            source_title_source=(
                representative.get("title_source")
                if representative is not None
                else None
            ),
            source_sentence_candidates=sentence_evidence,
        )
        if extractive_story["unified_text"]:
            unification_status = "generated"
        elif clean_text:
            unification_status = "fallback_only"
        else:
            unification_status = "unavailable"

        story = (
            {
                "story_id": cluster["cluster_key"],
                "cluster_id": cluster["id"],
                "title": title,
                "display_title": display_title["text"],
                "display_title_provenance": {
                    key: value
                    for key, value in display_title.items()
                    if key != "text"
                },
                "clean_text": clean_text,
                "unified_text": extractive_story["unified_text"],
                "unified_sentences": extractive_story["unified_sentences"],
                "has_conflict_flags": extractive_story["has_conflict_flags"],
                "conflict_flag_count": extractive_story[
                    "conflict_flag_count"
                ],
                "conflict_detection_scope": extractive_story[
                    "conflict_detection_scope"
                ],
                "conflict_flags": extractive_story["conflict_flags"],
                "unification": {
                    "version": UNIFIED_STORY_SCHEMA_VERSION,
                    "status": unification_status,
                    "selection_method": extractive_story[
                        "selection_method"
                    ],
                    "selection_config": extractive_story[
                        "selection_config"
                    ],
                    "candidate_group_count": extractive_story[
                        "candidate_group_count"
                    ],
                    "eligible_group_count": extractive_story[
                        "eligible_group_count"
                    ],
                    "selected_sentence_count": extractive_story[
                        "selected_sentence_count"
                    ],
                    "character_count": extractive_story["character_count"],
                    "lead_anchor_used": extractive_story[
                        "lead_anchor_used"
                    ],
                    "lead_anchor_sentence_id": extractive_story[
                        "lead_anchor_sentence_id"
                    ],
                    "suppressed_orphan_speaker_introduction_count": (
                        extractive_story[
                            "suppressed_orphan_speaker_introduction_count"
                        ]
                    ),
                    "suppressed_orphan_speaker_introductions": (
                        extractive_story[
                            "suppressed_orphan_speaker_introductions"
                        ]
                    ),
                    "suppressed_orphan_context_sentence_count": (
                        extractive_story[
                            "suppressed_orphan_context_sentence_count"
                        ]
                    ),
                    "suppressed_orphan_context_sentences": (
                        extractive_story[
                            "suppressed_orphan_context_sentences"
                        ]
                    ),
                    "suppressed_heading_fragment_count": (
                        extractive_story[
                            "suppressed_heading_fragment_count"
                        ]
                    ),
                    "suppressed_heading_fragments": extractive_story[
                        "suppressed_heading_fragments"
                    ],
                    "suppressed_relevance_sentence_count": extractive_story[
                        "suppressed_relevance_sentence_count"
                    ],
                    "suppressed_relevance_sentences": extractive_story[
                        "suppressed_relevance_sentences"
                    ],
                    "suppressed_residual_repetition_count": (
                        extractive_story[
                            "suppressed_residual_repetition_count"
                        ]
                    ),
                    "suppressed_residual_repetitions": extractive_story[
                        "suppressed_residual_repetitions"
                    ],
                    "fallback": {
                        "method": "representative_article_v1",
                        "content_field": "clean_text",
                        "available": bool(clean_text),
                    },
                },
                "representative_article": representative_article,
                "event_date_start": cluster.get("event_date_start"),
                "event_date_end": cluster.get("event_date_end"),
                "article_count": cluster["article_count"],
                "source_count": cluster["source_count"],
                "confidence": cluster["confidence"],
                "clustering": {
                    "model_name": cluster["model_name"],
                    "model_revision": cluster.get("model_revision"),
                    "text_variant": cluster["text_variant"],
                    "similarity_threshold": cluster["similarity_threshold"],
                    "representative_threshold": cluster.get(
                        "representative_threshold"
                    ),
                    "cohesion_threshold": cluster.get("cohesion_threshold"),
                    "created_at": cluster.get("created_at"),
                },
                "source_articles": [
                    {
                        "article_id": member["article_id"],
                        "url": member.get("url"),
                        "source": member.get("source"),
                        "title": member.get("title"),
                        "published_date": member.get("published_date"),
                        "similarity_score": member.get("similarity_score"),
                        "is_representative": bool(member.get("is_representative")),
                    }
                    for member in members
                ],
            }
        )
        validate_unified_story_record(story)
        unified_stories.append(story)
    return unified_stories


def _build_extractive_story(
    members: list[dict],
    article_records_by_id: dict[int, dict],
) -> tuple[dict, list[dict]]:
    article_records = []
    for member in members:
        article = article_records_by_id.get(member["article_id"])
        if article is None or not article.get("clean_text"):
            continue
        article_records.append(
            {
                **article,
                "article_id": member["article_id"],
                "is_representative": bool(member.get("is_representative")),
            }
        )

    sentence_evidence = build_sentence_evidence(article_records)
    evidence_groups = group_near_duplicate_evidence(sentence_evidence)
    return select_extractive_story(evidence_groups), sentence_evidence


def export_snapshot():
    config = load_config()
    config.snapshots_dir.mkdir(parents=True, exist_ok=True)

    snapshot_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_dir = config.snapshots_dir / snapshot_name
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            id,
            url,
            source,
            title,
            title_source,
            author,
            author_source,
            published_date,
            published_date_source,
            category,
            category_source,
            clean_text,
            raw_text,
            sinhala_purity,
            content_hash,
            clean_hash,
            metadata_flags,
            crawl_timestamp
        FROM articles
        WHERE clean_status = ?
          AND dedupe_status = ?
        ORDER BY id
        """,
        (CLEAN_STATUS_CLEANED, DEDUPE_STATUS_UNIQUE),
    )
    rows = cursor.fetchall()

    fulltext_rows = []
    metadata_rows = []
    article_records_by_id = {}
    exported_ids = []

    for row in rows:
        record = dict(row)
        article_id = record.pop("id")
        raw_text = record.pop("raw_text", "")
        clean_text = record.get("clean_text", "")
        article_records_by_id[article_id] = record.copy()

        fulltext_rows.append(record)
        metadata_rows.append(
            {
                key: value
                for key, value in record.items()
                if key not in {"clean_text"}
            }
            | {
                "clean_text_length": len(clean_text or ""),
                "raw_text_length": len(raw_text or ""),
            }
        )
        exported_ids.append(article_id)

    generated_at = datetime.now().isoformat(timespec="seconds")

    cursor.execute("SELECT COUNT(*) AS count FROM discovered_urls")
    total_urls = cursor.fetchone()["count"]
    cursor.execute("SELECT status, COUNT(*) AS count FROM discovered_urls GROUP BY status")
    url_status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}

    cursor.execute("SELECT COUNT(*) AS count FROM articles")
    total_articles = cursor.fetchone()["count"]
    cursor.execute(
        "SELECT clean_status, COUNT(*) AS count FROM articles GROUP BY clean_status"
    )
    clean_status_counts = {
        row["clean_status"]: row["count"] for row in cursor.fetchall()
    }
    cursor.execute(
        "SELECT dedupe_status, COUNT(*) AS count FROM articles GROUP BY dedupe_status"
    )
    dedupe_status_counts = {
        row["dedupe_status"]: row["count"] for row in cursor.fetchall()
    }
    cursor.execute("SELECT source, COUNT(*) AS count FROM articles GROUP BY source")
    articles_by_source = {row["source"]: row["count"] for row in cursor.fetchall()}

    cursor.execute(
        """
        SELECT
            SUM(CASE WHEN title IS NULL OR TRIM(title) = '' THEN 1 ELSE 0 END)
                AS missing_title,
            SUM(CASE WHEN published_date IS NULL OR TRIM(published_date) = '' THEN 1 ELSE 0 END)
                AS missing_published_date,
            SUM(CASE WHEN content_hash IS NULL OR TRIM(content_hash) = '' THEN 1 ELSE 0 END)
                AS missing_content_hash
        FROM articles
        """
    )
    metadata_missing_counts = dict(cursor.fetchone())
    cursor.execute(
        """
        SELECT COALESCE(NULLIF(title_source, ''), 'unknown') AS source, COUNT(*) AS count
        FROM articles
        GROUP BY COALESCE(NULLIF(title_source, ''), 'unknown')
        """
    )
    title_source_counts = {row["source"]: row["count"] for row in cursor.fetchall()}
    cursor.execute(
        """
        SELECT COALESCE(NULLIF(published_date_source, ''), 'unknown') AS source,
               COUNT(*) AS count
        FROM articles
        GROUP BY COALESCE(NULLIF(published_date_source, ''), 'unknown')
        """
    )
    published_date_source_counts = {
        row["source"]: row["count"] for row in cursor.fetchall()
    }

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
    cluster_rows = [dict(row) for row in cursor.fetchall()]

    cursor.execute(
        """
        SELECT
            members.cluster_id,
            clusters.cluster_key,
            members.article_id,
            members.similarity_score,
            members.is_representative,
            articles.url,
            articles.source,
            articles.title,
            articles.published_date
        FROM story_cluster_members AS members
        JOIN story_clusters AS clusters ON clusters.id = members.cluster_id
        JOIN articles ON articles.id = members.article_id
        ORDER BY members.cluster_id, members.is_representative DESC, members.article_id
        """
    )
    cluster_member_rows = [dict(row) for row in cursor.fetchall()]
    unified_story_rows = build_unified_story_rows(
        cluster_rows,
        cluster_member_rows,
        article_records_by_id,
    )
    unified_stories_with_extractive_text = sum(
        bool(story["unified_text"]) for story in unified_story_rows
    )
    unified_stories_with_conflict_flags = sum(
        story["has_conflict_flags"] for story in unified_story_rows
    )
    unified_story_conflict_flags = sum(
        story["conflict_flag_count"] for story in unified_story_rows
    )
    unified_stories_with_display_title = sum(
        bool(story["display_title"]) for story in unified_story_rows
    )
    unified_stories_using_display_title_fallback = sum(
        story["display_title_provenance"]["fallback_used"]
        for story in unified_story_rows
    )
    unified_stories_using_concise_display_title_fallback = sum(
        story["display_title_provenance"]["method"]
        in {
            "extractive_source_heading",
            "extractive_source_lead_sentence",
        }
        for story in unified_story_rows
    )

    if exported_ids:
        cursor.executemany(
            "UPDATE articles SET exported_at = ? WHERE id = ?",
            [(generated_at, article_id) for article_id in exported_ids],
        )

    conn.commit()
    conn.close()

    fulltext_path = snapshot_dir / "dataset_fulltext.jsonl"
    metadata_path = snapshot_dir / "dataset_metadata.jsonl"
    clusters_path = snapshot_dir / "story_clusters.jsonl"
    cluster_members_path = snapshot_dir / "story_cluster_members.jsonl"
    unified_stories_path = snapshot_dir / "unified_stories.jsonl"
    report_path = snapshot_dir / "report.json"

    _write_jsonl(fulltext_path, fulltext_rows)
    _write_jsonl(metadata_path, metadata_rows)
    _write_jsonl(clusters_path, cluster_rows)
    _write_jsonl(cluster_members_path, cluster_member_rows)
    _write_jsonl(unified_stories_path, unified_story_rows)

    report = {
        "generated_at": generated_at,
        "snapshot_name": snapshot_name,
        "contracts": {
            "unified_stories": unified_story_contract_metadata(),
        },
        "paths": {
            "fulltext": str(fulltext_path),
            "metadata": str(metadata_path),
            "story_clusters": str(clusters_path),
            "story_cluster_members": str(cluster_members_path),
            "unified_stories": str(unified_stories_path),
        },
        "counts": {
            "discovered_urls": total_urls,
            "articles_total": total_articles,
            "exported_unique_articles": len(fulltext_rows),
            "story_clusters": len(cluster_rows),
            "story_cluster_members": len(cluster_member_rows),
            "unified_stories": len(unified_story_rows),
            "unified_stories_with_extractive_text": (
                unified_stories_with_extractive_text
            ),
            "unified_stories_with_conflict_flags": (
                unified_stories_with_conflict_flags
            ),
            "unified_story_conflict_flags": unified_story_conflict_flags,
            "unified_stories_with_display_title": (
                unified_stories_with_display_title
            ),
            "unified_stories_using_display_title_fallback": (
                unified_stories_using_display_title_fallback
            ),
            "unified_stories_using_concise_display_title_fallback": (
                unified_stories_using_concise_display_title_fallback
            ),
        },
        "url_status_counts": url_status_counts,
        "clean_status_counts": clean_status_counts,
        "dedupe_status_counts": dedupe_status_counts,
        "articles_by_source": articles_by_source,
        "metadata_missing_counts": metadata_missing_counts,
        "title_source_counts": title_source_counts,
        "published_date_source_counts": published_date_source_counts,
    }

    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    logger.info("=== Snapshot Export Complete ===")
    logger.info("Exported %s unique cleaned articles", len(fulltext_rows))
    logger.info("Exported %s story clusters", len(cluster_rows))
    logger.info("Exported %s unified stories", len(unified_story_rows))
    logger.info("Snapshot directory: %s", snapshot_dir)

    return {
        "snapshot_dir": str(snapshot_dir),
        "fulltext_path": str(fulltext_path),
        "metadata_path": str(metadata_path),
        "clusters_path": str(clusters_path),
        "cluster_members_path": str(cluster_members_path),
        "unified_stories_path": str(unified_stories_path),
        "report_path": str(report_path),
        "unified_story_contract": unified_story_contract_metadata(),
        "exported_unique_articles": len(fulltext_rows),
        "story_clusters": len(cluster_rows),
        "unified_stories": len(unified_story_rows),
        "unified_stories_with_extractive_text": (
            unified_stories_with_extractive_text
        ),
        "unified_stories_with_conflict_flags": (
            unified_stories_with_conflict_flags
        ),
        "unified_story_conflict_flags": unified_story_conflict_flags,
        "unified_stories_with_display_title": (
            unified_stories_with_display_title
        ),
        "unified_stories_using_display_title_fallback": (
            unified_stories_using_display_title_fallback
        ),
        "unified_stories_using_concise_display_title_fallback": (
            unified_stories_using_concise_display_title_fallback
        ),
    }
