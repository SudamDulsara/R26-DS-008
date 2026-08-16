from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from news_pipeline.config import PipelineConfig, load_config
from news_pipeline.storage.database import get_connection


PIPELINE_STATUS_VERSION = "pipeline_operational_status_v3"


def _counts_by(connection, table: str, column: str) -> dict[str, int]:
    return {
        str(row["value"] or "unknown"): int(row["count"])
        for row in connection.execute(
            f"""
            SELECT {column} AS value, COUNT(*) AS count
            FROM {table}
            GROUP BY {column}
            ORDER BY {column}
            """
        )
    }


def _latest_run(connection) -> Optional[dict[str, Any]]:
    row = connection.execute(
        """
        SELECT
            id, started_at, finished_at, status, note,
            snapshot_path, is_latest_success
        FROM pipeline_runs
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row is not None else None


def _latest_success(connection) -> Optional[dict[str, Any]]:
    row = connection.execute(
        """
        SELECT id, finished_at, snapshot_path
        FROM pipeline_runs
        WHERE is_latest_success = 1
          AND status = 'completed'
          AND snapshot_path IS NOT NULL
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    result = dict(row)
    result["snapshot_exists"] = Path(
        result["snapshot_path"]
    ).exists()
    return result


def build_pipeline_status(
    *,
    config: Optional[PipelineConfig] = None,
) -> dict[str, Any]:
    selected_config = config or load_config()
    connection = get_connection(selected_config)
    try:
        latest_run = _latest_run(connection)
        latest_success = _latest_success(connection)
        current_publication_path = selected_config.data_dir / "current"
        status = {
            "status_version": PIPELINE_STATUS_VERSION,
            "generated_at": datetime.now().astimezone().isoformat(
                timespec="seconds"
            ),
            "database_path": str(selected_config.db_path),
            "configuration": {
                "gpt_generation_enabled": selected_config.gpt_enabled,
                "gpt_shadow_mode": selected_config.gpt_shadow_mode,
                "gpt_only_publication_enabled": (
                    selected_config.gpt_only_publication_enabled
                ),
                "primary_unification_contract": (
                    "final_gpt_only_publication"
                    if selected_config.gpt_only_publication_enabled
                    else "unified_stories_v3"
                ),
                "hybrid_prompt_version": (
                    selected_config.gpt_prompt_version
                ),
                "gpt_only_publication_prompt_version": (
                    selected_config.gpt_only_publication_prompt_version
                ),
                "gpt_model": selected_config.gpt_model,
                "autonomous_audit_enabled": (
                    selected_config.gpt_autonomous_audit_enabled
                ),
                "autonomous_audit_policy_mode": (
                    selected_config.gpt_audit_policy_mode
                ),
                "low_risk_audit_sample_rate": (
                    selected_config.gpt_low_risk_audit_sample_rate
                ),
                "autonomous_audit_model": (
                    selected_config.gpt_audit_model
                ),
                "autonomous_complex_audit_model": (
                    selected_config.gpt_audit_complex_model
                ),
                "gpt_max_clusters_per_run": (
                    selected_config.gpt_max_clusters_per_run
                ),
            },
            "latest_run": latest_run,
            "latest_successful_snapshot": latest_success,
            "current_publication": {
                "path": str(current_publication_path),
                "exists": current_publication_path.is_dir(),
                "manifest_exists": (
                    current_publication_path
                    / "final_publication_manifest.json"
                ).is_file(),
            },
            "counts": {
                "discovered_urls": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM discovered_urls"
                    ).fetchone()[0]
                ),
                "url_statuses": _counts_by(
                    connection,
                    "discovered_urls",
                    "status",
                ),
                "articles": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM articles"
                    ).fetchone()[0]
                ),
                "article_clean_statuses": _counts_by(
                    connection,
                    "articles",
                    "clean_status",
                ),
                "article_dedupe_statuses": _counts_by(
                    connection,
                    "articles",
                    "dedupe_status",
                ),
                "story_clusters": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM story_clusters"
                    ).fetchone()[0]
                ),
                "unified_story_versions": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM unified_story_versions"
                    ).fetchone()[0]
                ),
                "gpt_review_queue_statuses": _counts_by(
                    connection,
                    "gpt_unification_review_queue",
                    "queue_status",
                ),
                "unreviewed_gpt_queue_rows": int(
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM gpt_unification_review_queue
                        WHERE queue_status = 'pending_review'
                          AND review_decision IS NULL
                        """
                    ).fetchone()[0]
                ),
                "final_publication_statuses": _counts_by(
                    connection,
                    "final_story_publication_states",
                    "publication_status",
                ),
                "final_unified_stories": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM final_unified_stories"
                    ).fetchone()[0]
                ),
                "evidence_safe_fallback_stories": int(
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM final_story_publication_states
                        WHERE reason_codes_json LIKE
                            '%\"evidence_safe_fallback\"%'
                        """
                    ).fetchone()[0]
                ),
            },
        }
    finally:
        connection.close()
    status["attention"] = {
        "latest_run_failed": bool(
            latest_run and latest_run["status"] == "failed"
        ),
        "pending_gpt_reviews": status["counts"][
            "final_publication_statuses"
        ].get("pending_review", 0),
        "manual_review_required_items": (
            0
            if selected_config.gpt_autonomous_audit_enabled
            else status["counts"]["unreviewed_gpt_queue_rows"]
        ),
        "autonomous_recovery_queue_rows": (
            status["counts"]["unreviewed_gpt_queue_rows"]
            if selected_config.gpt_autonomous_audit_enabled
            else 0
        ),
        "autonomous_safe_fallbacks": status["counts"][
            "evidence_safe_fallback_stories"
        ],
        "unavailable_final_stories": status["counts"][
            "final_publication_statuses"
        ].get("unavailable", 0),
        "rejected_final_stories": status["counts"][
            "final_publication_statuses"
        ].get("rejected", 0),
        "latest_snapshot_missing": bool(
            latest_success
            and not latest_success["snapshot_exists"]
        ),
    }
    return status


def format_pipeline_status(status: dict[str, Any]) -> str:
    latest = status.get("latest_run") or {}
    snapshot = status.get("latest_successful_snapshot") or {}
    current = status.get("current_publication") or {}
    counts = status["counts"]
    attention = status["attention"]
    configuration = status["configuration"]
    lines = [
        "Pipeline status",
        f"  Latest run: {latest.get('status', 'none')} "
        f"(id={latest.get('id', 'none')})",
        "  Current publication: "
        f"{current.get('path', 'none')} "
        f"(exists={str(bool(current.get('exists'))).lower()})",
        "  Latest successful run output: "
        f"{snapshot.get('snapshot_path', 'none')}",
        f"  URLs: {counts['discovered_urls']} "
        f"{json.dumps(counts['url_statuses'], sort_keys=True)}",
        f"  Articles: {counts['articles']} "
        f"{json.dumps(counts['article_clean_statuses'], sort_keys=True)}",
        f"  Story clusters: {counts['story_clusters']}",
        f"  Final stories: {counts['final_unified_stories']} "
        f"{json.dumps(counts['final_publication_statuses'], sort_keys=True)}",
        "  GPT generation enabled: "
        f"{str(configuration['gpt_generation_enabled']).lower()}",
        "  GPT-only publication enabled: "
        f"{str(configuration['gpt_only_publication_enabled']).lower()}",
        "  Autonomous GPT audit enabled: "
        f"{str(configuration['autonomous_audit_enabled']).lower()}",
        "  Primary unification contract: "
        f"{configuration['primary_unification_contract']}",
        "  Attention: "
        f"{json.dumps(attention, sort_keys=True)}",
    ]
    return "\n".join(lines)
