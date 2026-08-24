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
    token_usage_cost_usd,
)


POLICY_VERSION = "autonomous_generation_budget_policy_v3"


@dataclass(frozen=True)
class OngoingPolicyGate:
    policy_dir: Path
    approval_sha256: str
    cluster_count: int
    day_actual_cost_usd: Decimal
    month_actual_cost_usd: Decimal
    maximum_cost_per_day_usd: Decimal
    effective_run_cap_usd: Decimal
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
            "effective_run_cap_usd": format(
                self.effective_run_cap_usd,
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
    day_cost = Decimal("0")
    month_cost = Decimal("0")
    connection = get_connection(config)
    try:
        rows = connection.execute(
            """
            SELECT created_at, model_name, input_tokens, output_tokens,
                   primary_model_name, primary_input_tokens,
                   primary_output_tokens, autonomous_audit_created_at,
                   autonomous_audit_model, autonomous_audit_input_tokens,
                   autonomous_audit_output_tokens
            FROM unified_story_versions
            WHERE response_id IS NOT NULL
            """,
        ).fetchall()
    finally:
        connection.close()

    def add_usage(
        *,
        created_at: Any,
        model: Any,
        input_tokens: Any,
        output_tokens: Any,
    ) -> None:
        nonlocal day_cost, month_cost
        if not created_at or not model:
            return
        try:
            cost = token_usage_cost_usd(
                model_name=str(model),
                input_tokens=int(input_tokens or 0),
                output_tokens=int(output_tokens or 0),
            )
        except (TypeError, ValueError):
            return
        if cost is None:
            return
        timestamp = str(created_at)
        if timestamp.startswith(month_prefix):
            month_cost += cost
        if timestamp.startswith(day_prefix):
            day_cost += cost

    for row in rows:
        if row["primary_model_name"]:
            add_usage(
                created_at=row["created_at"],
                model=row["primary_model_name"],
                input_tokens=row["primary_input_tokens"],
                output_tokens=row["primary_output_tokens"],
            )
        else:
            add_usage(
                created_at=row["created_at"],
                model=row["model_name"],
                input_tokens=row["input_tokens"],
                output_tokens=row["output_tokens"],
            )
        add_usage(
            created_at=row["autonomous_audit_created_at"],
            model=row["autonomous_audit_model"],
            input_tokens=row["autonomous_audit_input_tokens"],
            output_tokens=row["autonomous_audit_output_tokens"],
        )
    return day_cost, month_cost


def _configuration_sha256(config: PipelineConfig) -> str:
    payload = {
        "policy_version": POLICY_VERSION,
        "primary_model": config.gpt_model,
        "audit_enabled": config.gpt_autonomous_audit_enabled,
        "audit_policy_mode": config.gpt_audit_policy_mode,
        "low_risk_audit_sample_rate": (
            config.gpt_low_risk_audit_sample_rate
        ),
        "audit_circuit_min_evaluated": (
            config.gpt_audit_circuit_min_evaluated
        ),
        "audit_circuit_max_material_rate": (
            config.gpt_audit_circuit_max_material_rate
        ),
        "audit_high_risk_article_count": (
            config.gpt_audit_high_risk_article_count
        ),
        "audit_high_risk_source_count": (
            config.gpt_audit_high_risk_source_count
        ),
        "audit_high_risk_evidence_chars": (
            config.gpt_audit_high_risk_evidence_chars
        ),
        "audit_medium_risk_evidence_chars": (
            config.gpt_audit_medium_risk_evidence_chars
        ),
        "audit_model": config.gpt_audit_model,
        "audit_complex_model": config.gpt_audit_complex_model,
        "max_clusters_per_run": config.gpt_max_clusters_per_run,
        "max_cost_per_story_usd": config.gpt_max_cost_per_story_usd,
        "max_cost_per_run_usd": config.gpt_max_cost_per_run_usd,
        "max_cost_per_day_usd": config.gpt_max_cost_per_day_usd,
        "max_cost_per_month_usd": config.gpt_max_cost_per_month_usd,
        "concurrent_unification_enabled": (
            config.gpt_concurrent_unification_enabled
        ),
        "unification_workers": config.gpt_unification_workers,
        "evidence_aliases_enabled": config.gpt_evidence_aliases_enabled,
        "audit_prompt_cache_enabled": (
            config.gpt_audit_prompt_cache_enabled
        ),
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
    effective_run_cap = max(
        Decimal("0"),
        min(
            run_cap,
            day_cap - day_cost,
            month_cap - month_cost,
        ),
    )

    return OngoingPolicyGate(
        policy_dir=policy_directory(config),
        approval_sha256=_configuration_sha256(config),
        cluster_count=selected_cluster_count,
        day_actual_cost_usd=day_cost,
        month_actual_cost_usd=month_cost,
        maximum_cost_per_day_usd=day_cap,
        effective_run_cap_usd=effective_run_cap,
        preflight=OfflineRequestSizePreflight(
            max_cost_per_story_usd=config.gpt_max_cost_per_story_usd,
            max_cost_per_run_usd=effective_run_cap,
            provider_framing_token_allowance=(
                DEFAULT_PROVIDER_FRAMING_TOKEN_ALLOWANCE
            ),
        ),
    )
