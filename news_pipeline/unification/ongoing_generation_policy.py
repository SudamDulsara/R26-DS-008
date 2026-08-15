from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from news_pipeline.config import PipelineConfig
from news_pipeline.storage.database import get_connection
from news_pipeline.unification.gpt_preflight import (
    DEFAULT_PROVIDER_FRAMING_TOKEN_ALLOWANCE,
    MODEL_PRICING,
    OfflineRequestSizePreflight,
)


POLICY_VERSION = "autonomous_generation_budget_policy_v2"


@dataclass(frozen=True)
class OngoingPolicyGate:
    policy_dir: Path
    approval_sha256: str
    cluster_count: int
    day_actual_cost_usd: Decimal
    month_actual_cost_usd: Decimal
    maximum_cost_per_day_usd: Decimal
    preflight: OfflineRequestSizePreflight

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_version": POLICY_VERSION,
            "configuration_sha256": self.approval_sha256,
            "cluster_count": self.cluster_count,
            "day_actual_cost_usd": format(self.day_actual_cost_usd, "f"),
            "month_actual_cost_usd": format(
                self.month_actual_cost_usd,
                "f",
            ),
            "maximum_cost_per_day_usd": format(
                self.maximum_cost_per_day_usd,
                "f",
            ),
            "input_token_count_calls": 0,
            "automatic_retries": 0,
        }


def policy_directory(config: PipelineConfig) -> Path:
    return config.reviews_dir / "autonomous_generation_policy"


def _actual_costs(config: PipelineConfig) -> tuple[Decimal, Decimal]:
    now = datetime.now()
    day_prefix = now.strftime("%Y-%m-%d")
    month_prefix = now.strftime("%Y-%m")
    connection = get_connection(config)
    try:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE
                    WHEN primary_estimated_cost_usd IS NULL
                         AND substr(created_at, 1, 10) = ?
                    THEN CAST(estimated_cost_usd AS REAL)
                    WHEN primary_estimated_cost_usd IS NOT NULL
                         AND substr(created_at, 1, 10) = ?
                    THEN CAST(primary_estimated_cost_usd AS REAL)
                    ELSE 0 END), 0)
                + COALESCE(SUM(CASE
                    WHEN substr(autonomous_audit_created_at, 1, 10) = ?
                    THEN CAST(autonomous_audit_estimated_cost_usd AS REAL)
                    ELSE 0 END), 0) AS day_cost,
                COALESCE(SUM(CASE
                    WHEN primary_estimated_cost_usd IS NULL
                         AND substr(created_at, 1, 7) = ?
                    THEN CAST(estimated_cost_usd AS REAL)
                    WHEN primary_estimated_cost_usd IS NOT NULL
                         AND substr(created_at, 1, 7) = ?
                    THEN CAST(primary_estimated_cost_usd AS REAL)
                    ELSE 0 END), 0)
                + COALESCE(SUM(CASE
                    WHEN substr(autonomous_audit_created_at, 1, 7) = ?
                    THEN CAST(autonomous_audit_estimated_cost_usd AS REAL)
                    ELSE 0 END), 0) AS month_cost
            FROM unified_story_versions
            WHERE response_id IS NOT NULL
              AND estimated_cost_usd IS NOT NULL
            """,
            (
                day_prefix,
                day_prefix,
                day_prefix,
                month_prefix,
                month_prefix,
                month_prefix,
            ),
        ).fetchone()
    finally:
        connection.close()
    return Decimal(str(row["day_cost"])), Decimal(str(row["month_cost"]))


def _configuration_sha256(config: PipelineConfig) -> str:
    payload = {
        "policy_version": POLICY_VERSION,
        "primary_model": config.gpt_model,
        "audit_enabled": config.gpt_autonomous_audit_enabled,
        "audit_model": config.gpt_audit_model,
        "audit_complex_model": config.gpt_audit_complex_model,
        "max_clusters_per_run": config.gpt_max_clusters_per_run,
        "max_cost_per_story_usd": config.gpt_max_cost_per_story_usd,
        "max_cost_per_run_usd": config.gpt_max_cost_per_run_usd,
        "max_cost_per_day_usd": config.gpt_max_cost_per_day_usd,
        "max_cost_per_month_usd": config.gpt_max_cost_per_month_usd,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def enforce_ongoing_generation_policy(
    *,
    config: PipelineConfig,
    selected_cluster_count: int,
) -> OngoingPolicyGate:
    """Apply reusable configured limits before autonomous provider calls."""
    configured_models = {
        config.gpt_model,
        config.gpt_audit_model,
        config.gpt_audit_complex_model,
    }
    if (
        not config.gpt_enabled
        or not config.gpt_only_publication_enabled
        or config.gpt_max_retries != 0
        or not configured_models <= set(MODEL_PRICING)
    ):
        raise ValueError("live GPT configuration is not safely supported")
    if not 0 <= selected_cluster_count <= config.gpt_max_clusters_per_run:
        raise ValueError("changed-cluster run exceeds the configured cap")

    day_cost, month_cost = _actual_costs(config)
    run_cap = Decimal(str(config.gpt_max_cost_per_run_usd))
    day_cap = Decimal(str(config.gpt_max_cost_per_day_usd))
    month_cap = Decimal(str(config.gpt_max_cost_per_month_usd))
    if day_cost + run_cap > day_cap:
        raise ValueError("daily autonomous GPT budget would be exceeded")
    if month_cost + run_cap > month_cap:
        raise ValueError("monthly autonomous GPT budget would be exceeded")

    return OngoingPolicyGate(
        policy_dir=policy_directory(config),
        approval_sha256=_configuration_sha256(config),
        cluster_count=selected_cluster_count,
        day_actual_cost_usd=day_cost,
        month_actual_cost_usd=month_cost,
        maximum_cost_per_day_usd=day_cap,
        preflight=OfflineRequestSizePreflight(
            max_cost_per_story_usd=config.gpt_max_cost_per_story_usd,
            max_cost_per_run_usd=config.gpt_max_cost_per_run_usd,
            provider_framing_token_allowance=(
                DEFAULT_PROVIDER_FRAMING_TOKEN_ALLOWANCE
            ),
        ),
    )
