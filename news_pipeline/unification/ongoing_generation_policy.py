from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Union

from news_pipeline.config import PipelineConfig
from news_pipeline.evaluation.gpt_only_backfill import (
    _read_json,
    _sha256,
    _write_json,
)
from news_pipeline.storage.database import get_connection
from news_pipeline.unification.gpt_contract import GPT_PROMPT_VERSION_V2_8
from news_pipeline.unification.gpt_preflight import (
    DEFAULT_PROVIDER_FRAMING_TOKEN_ALLOWANCE,
    OfflineRequestSizePreflight,
)


POLICY_VERSION = "gpt_only_ongoing_generation_policy_v1"
POLICY_APPROVAL_VERSION = "gpt_only_ongoing_generation_policy_approval_v1"
POLICY_DIRECTORY_NAME = "ongoing_generation_policy_2026-07-31_16-59"
POLICY_MODEL = "gpt-5.6-luna"
POLICY_REASONING_EFFORT = "low"
POLICY_MAX_OUTPUT_TOKENS = 4096
POLICY_MAX_CLUSTERS_PER_RUN = 25
POLICY_MAX_COST_PER_STORY_USD = Decimal("0.25")
POLICY_MAX_COST_PER_RUN_USD = Decimal("1.00")
POLICY_MAX_COST_PER_DAY_USD = Decimal("2.00")
POLICY_MAX_COST_PER_MONTH_USD = Decimal("20.00")


@dataclass(frozen=True)
class OngoingPolicyGate:
    policy_dir: Path
    approval_sha256: str
    cluster_count: int
    day_actual_cost_usd: Decimal
    month_actual_cost_usd: Decimal
    preflight: OfflineRequestSizePreflight

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_version": POLICY_VERSION,
            "approval_sha256": self.approval_sha256,
            "cluster_count": self.cluster_count,
            "maximum_clusters_per_run": POLICY_MAX_CLUSTERS_PER_RUN,
            "day_actual_cost_usd": format(
                self.day_actual_cost_usd,
                "f",
            ),
            "month_actual_cost_usd": format(
                self.month_actual_cost_usd,
                "f",
            ),
            "maximum_cost_per_story_usd": format(
                POLICY_MAX_COST_PER_STORY_USD,
                ".2f",
            ),
            "maximum_cost_per_run_usd": format(
                POLICY_MAX_COST_PER_RUN_USD,
                ".2f",
            ),
            "maximum_cost_per_day_usd": format(
                POLICY_MAX_COST_PER_DAY_USD,
                ".2f",
            ),
            "maximum_cost_per_month_usd": format(
                POLICY_MAX_COST_PER_MONTH_USD,
                ".2f",
            ),
            "input_token_count_calls": 0,
            "automatic_retries": 0,
        }


def policy_directory(config: PipelineConfig) -> Path:
    return (
        config.reviews_dir
        / "phase4_gpt_only"
        / POLICY_DIRECTORY_NAME
    )


def record_ongoing_generation_policy_approval(
    *,
    policy_dir: Union[str, Path],
    approval_statement: str,
) -> dict[str, Any]:
    """Bind exact approval to the frozen proposal without network calls."""
    selected_dir = Path(policy_dir)
    proposal_path = selected_dir / "policy_proposal.json"
    markdown_path = selected_dir / "policy_proposal.md"
    approval_path = selected_dir / "generation_policy_approval.json"
    proposal = _read_json(proposal_path)
    expected = str(proposal.get("approval_statement") or "")
    if approval_statement.strip() != expected:
        raise ValueError(
            "approval statement does not exactly match the frozen ongoing "
            "generation policy"
        )
    generation = proposal.get("generation") or {}
    budget = proposal.get("budget") or {}
    publication = proposal.get("publication") or {}
    operations = proposal.get("operations") or {}
    if (
        proposal.get("policy_version") != POLICY_VERSION
        or proposal.get("status") != "awaiting_explicit_policy_approval"
        or proposal.get("egress_destination") != "api.openai.com"
        or generation.get("model") != POLICY_MODEL
        or generation.get("prompt_version")
        != GPT_PROMPT_VERSION_V2_8
        or generation.get("reasoning_effort")
        != POLICY_REASONING_EFFORT
        or generation.get("max_output_tokens_per_story")
        != POLICY_MAX_OUTPUT_TOKENS
        or generation.get("automatic_retries") != 0
        or generation.get("token_count_calls") != 0
        or generation.get("only_new_or_changed_clusters") is not True
        or generation.get("maximum_clusters_per_run")
        != POLICY_MAX_CLUSTERS_PER_RUN
        or generation.get("one_generation_call_per_cluster") is not True
        or generation.get("unchanged_cache_hits_are_never_resent")
        is not True
        or str(budget.get("maximum_cost_per_story_usd")) != "0.25"
        or str(budget.get("maximum_cost_per_run_usd")) != "1.00"
        or str(budget.get("maximum_cost_per_day_usd")) != "2.00"
        or str(budget.get("maximum_cost_per_calendar_month_usd"))
        != "20.00"
        or budget.get("offline_request_bound_required_before_each_call")
        is not True
        or budget.get("fail_closed_if_any_limit_would_be_exceeded")
        is not True
        or publication.get("gpt_is_only_story_writer") is not True
        or publication.get("deterministic_story_substitution") is not False
        or operations.get("hosted_persistence_scheduling_retention_and_soak_are_out_of_scope")
        is not True
        or not markdown_path.is_file()
    ):
        raise ValueError("frozen ongoing generation policy is invalid")
    if approval_path.exists():
        existing = _read_json(approval_path)
        if existing.get("approval_statement") != expected:
            raise ValueError("a different ongoing policy approval is recorded")
        return {
            "approval_path": str(approval_path),
            "approval_sha256": _sha256(approval_path),
            "recorded": False,
            "network_calls_made": 0,
        }
    approval = {
        "approval_version": POLICY_APPROVAL_VERSION,
        "policy_version": POLICY_VERSION,
        "approved_at": datetime.now().astimezone().isoformat(
            timespec="seconds"
        ),
        "approval_statement": expected,
        "frozen_artifacts": {
            "policy_proposal_json_sha256": _sha256(proposal_path),
            "policy_proposal_markdown_sha256": _sha256(markdown_path),
        },
        "status": "approved_pending_runtime_egress_permission",
        "network_calls_made": 0,
        "token_count_calls_made": 0,
        "generation_calls_made": 0,
    }
    _write_json(approval_path, approval)
    return {
        "approval_path": str(approval_path),
        "approval_sha256": _sha256(approval_path),
        "recorded": True,
        "network_calls_made": 0,
    }


def _actual_costs(config: PipelineConfig) -> tuple[Decimal, Decimal]:
    now = datetime.now()
    day_prefix = now.strftime("%Y-%m-%d")
    month_prefix = now.strftime("%Y-%m")
    connection = get_connection(config)
    try:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(
                    CASE WHEN substr(created_at, 1, 10) = ?
                         THEN CAST(estimated_cost_usd AS REAL) ELSE 0 END
                ), 0) AS day_cost,
                COALESCE(SUM(
                    CASE WHEN substr(created_at, 1, 7) = ?
                         THEN CAST(estimated_cost_usd AS REAL) ELSE 0 END
                ), 0) AS month_cost
            FROM unified_story_versions
            WHERE prompt_version = ?
              AND response_id IS NOT NULL
              AND estimated_cost_usd IS NOT NULL
            """,
            (day_prefix, month_prefix, GPT_PROMPT_VERSION_V2_8),
        ).fetchone()
    finally:
        connection.close()
    return Decimal(str(row["day_cost"])), Decimal(str(row["month_cost"]))


def enforce_ongoing_generation_policy(
    *,
    config: PipelineConfig,
    selected_cluster_count: int,
    policy_dir: Optional[Union[str, Path]] = None,
) -> OngoingPolicyGate:
    """Fail closed before any ordinary changed-cluster provider request."""
    selected_dir = (
        Path(policy_dir) if policy_dir is not None else policy_directory(config)
    )
    proposal_path = selected_dir / "policy_proposal.json"
    markdown_path = selected_dir / "policy_proposal.md"
    approval_path = selected_dir / "generation_policy_approval.json"
    proposal = _read_json(proposal_path)
    approval = _read_json(approval_path)
    frozen = approval.get("frozen_artifacts") or {}
    if (
        approval.get("approval_version") != POLICY_APPROVAL_VERSION
        or approval.get("policy_version") != POLICY_VERSION
        or approval.get("approval_statement")
        != proposal.get("approval_statement")
        or approval.get("status")
        != "approved_pending_runtime_egress_permission"
        or frozen.get("policy_proposal_json_sha256")
        != _sha256(proposal_path)
        or frozen.get("policy_proposal_markdown_sha256")
        != _sha256(markdown_path)
        or approval.get("network_calls_made") != 0
        or approval.get("token_count_calls_made") != 0
        or approval.get("generation_calls_made") != 0
    ):
        raise ValueError("ongoing GPT-only policy approval is invalid")
    if (
        not config.gpt_enabled
        or not config.gpt_only_publication_enabled
        or config.gpt_model != POLICY_MODEL
        or config.gpt_prompt_version != GPT_PROMPT_VERSION_V2_8
        or config.gpt_reasoning_effort != POLICY_REASONING_EFFORT
        or config.gpt_max_output_tokens != POLICY_MAX_OUTPUT_TOKENS
        or config.gpt_max_retries != 0
        or Decimal(str(config.gpt_max_cost_per_story_usd))
        > POLICY_MAX_COST_PER_STORY_USD
        or Decimal(str(config.gpt_max_cost_per_run_usd))
        > POLICY_MAX_COST_PER_RUN_USD
    ):
        raise ValueError("live GPT configuration exceeds the approved policy")
    if (
        selected_cluster_count < 0
        or selected_cluster_count > POLICY_MAX_CLUSTERS_PER_RUN
    ):
        raise ValueError("changed-cluster run exceeds the approved 25-cluster cap")
    day_cost, month_cost = _actual_costs(config)
    run_cap = Decimal(str(config.gpt_max_cost_per_run_usd))
    if day_cost + run_cap > POLICY_MAX_COST_PER_DAY_USD:
        raise ValueError("daily ongoing GPT budget reservation would be exceeded")
    if month_cost + run_cap > POLICY_MAX_COST_PER_MONTH_USD:
        raise ValueError("monthly ongoing GPT budget reservation would be exceeded")
    return OngoingPolicyGate(
        policy_dir=selected_dir,
        approval_sha256=_sha256(approval_path),
        cluster_count=selected_cluster_count,
        day_actual_cost_usd=day_cost,
        month_actual_cost_usd=month_cost,
        preflight=OfflineRequestSizePreflight(
            max_cost_per_story_usd=config.gpt_max_cost_per_story_usd,
            max_cost_per_run_usd=config.gpt_max_cost_per_run_usd,
            provider_framing_token_allowance=(
                DEFAULT_PROVIDER_FRAMING_TOKEN_ALLOWANCE
            ),
        ),
    )
