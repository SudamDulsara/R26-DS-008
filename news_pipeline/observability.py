from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from time import perf_counter
from typing import Any, Callable, Mapping


RUN_METRICS_VERSION = "pipeline_run_metrics_v1"
RUN_HEALTH_REPORT_VERSION = "pipeline_run_health_v3"


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _integer(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _decimal_string(value: Any) -> str:
    try:
        return format(Decimal(str(value or "0")), "f")
    except (InvalidOperation, TypeError, ValueError):
        return "0"


def _numeric_counts(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in result.items()
        if (
            not isinstance(value, bool)
            and isinstance(value, (int, float))
        )
    }


def _nested_counts(
    result: Mapping[str, Any],
    key: str,
) -> dict[str, Any]:
    value = result.get(key)
    if not isinstance(value, Mapping):
        return {}
    normalized = {}
    for group, counts in value.items():
        if isinstance(counts, Mapping):
            normalized[str(group)] = {
                str(name): _integer(count)
                for name, count in counts.items()
            }
        else:
            normalized[str(group)] = _integer(counts)
    return normalized


def _stage_counts(
    stage_name: str,
    result: Mapping[str, Any],
) -> dict[str, int]:
    if stage_name == "discovery":
        failures = sum(
            sum(
                _integer(value)
                for value in counts.values()
            )
            if isinstance(counts, Mapping)
            else _integer(counts)
            for counts in _nested_counts(
                result,
                "failures_by_source",
            ).values()
        )
        return {
            "input": _integer(result.get("sources_checked")),
            "output": _integer(result.get("new_urls")),
            "skipped": 0,
            "failed": failures,
        }
    if stage_name == "extraction":
        output = _integer(result.get("extracted_articles"))
        failed = (
            _integer(result.get("fetch_failures"))
            + _integer(result.get("rejected_articles"))
        )
        return {
            "input": output + failed,
            "output": output,
            "skipped": 0,
            "failed": failed,
        }
    if stage_name == "cleaning":
        output = _integer(result.get("cleaned_articles"))
        failed = _integer(result.get("rejected_articles"))
        skipped = _integer(result.get("unsupported_media_articles"))
        return {
            "input": output + failed + skipped,
            "output": output,
            "skipped": skipped,
            "failed": failed,
        }
    if stage_name == "deduplication":
        output = _integer(result.get("unique_articles"))
        skipped = (
            _integer(result.get("exact_duplicates"))
            + _integer(result.get("unhashable_articles"))
        )
        return {
            "input": output + skipped,
            "output": output,
            "skipped": skipped,
            "failed": 0,
        }
    if stage_name == "clustering":
        inputs = _integer(result.get("eligible_articles"))
        affected = _integer(result.get("affected_articles"))
        return {
            "input": inputs,
            "output": _integer(result.get("story_clusters")),
            "skipped": max(inputs - affected, 0),
            # Articles that do not join a multi-source component are now
            # deliberately materialized as singleton stories. They are a
            # successful routing outcome, not clustering failures.
            "failed": 0,
        }
    if stage_name == "unification":
        budget_deferred = _integer(result.get("budget_deferred"))
        return {
            "input": _integer(result.get("clusters_seen")),
            "output": _integer(result.get("accepted")),
            "skipped": (
                _integer(result.get("cache_hits")) + budget_deferred
            ),
            # Budget deferrals are safe, retryable outcomes rather than bad
            # provider results. Invalid inputs are already included in the
            # current-run fallback count and must not be double-counted.
            "failed": max(
                _integer(result.get("fallbacks")) - budget_deferred,
                0,
            ),
        }
    if stage_name == "export":
        final = result.get("final_gpt_only_publication")
        final_counts = (
            final.get("counts", {})
            if isinstance(final, Mapping)
            else {}
        )
        return {
            "input": _integer(result.get("exported_unique_articles")),
            "output": _integer(
                final_counts.get(
                    "final_unified_stories",
                    result.get("unified_stories"),
                )
            ),
            "skipped": _integer(
                final_counts.get("explicit_nonpublishable_states")
            ),
            "failed": 0,
        }
    return {
        "input": 0,
        "output": 0,
        "skipped": 0,
        "failed": 0,
    }


class PipelineRunMetrics:
    def __init__(self) -> None:
        self._started_clock = perf_counter()
        self.data: dict[str, Any] = {
            "metrics_version": RUN_METRICS_VERSION,
            "started_at": _now(),
            "finished_at": None,
            "duration_seconds": None,
            "status": "running",
            "failed_stage": None,
            "stages": {},
            "gpt_usage": {
                "generation_calls": 0,
                "audit_calls": 0,
                "provider_calls": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "estimated_cost_usd": "0",
            },
        }

    def run(
        self,
        stage_name: str,
        function: Callable[..., Mapping[str, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        started_at = _now()
        started_clock = perf_counter()
        try:
            result = function(*args, **kwargs)
            if not isinstance(result, Mapping):
                raise TypeError(
                    f"{stage_name} stage returned non-mapping stats"
                )
        except Exception as exc:
            self.data["stages"][stage_name] = {
                "status": "failed",
                "started_at": started_at,
                "finished_at": _now(),
                "duration_seconds": round(
                    perf_counter() - started_clock,
                    6,
                ),
                "counts": {
                    "input": 0,
                    "output": 0,
                    "skipped": 0,
                    "failed": 1,
                },
                "raw_counts": {},
                "failures_by_source": {},
                "skips_by_source": {},
                "error_type": type(exc).__name__,
            }
            self.data["failed_stage"] = stage_name
            raise
        normalized = dict(result)
        self.data["stages"][stage_name] = {
            "status": "completed",
            "started_at": started_at,
            "finished_at": _now(),
            "duration_seconds": round(
                perf_counter() - started_clock,
                6,
            ),
            "counts": _stage_counts(stage_name, normalized),
            "raw_counts": _numeric_counts(normalized),
            "failures_by_source": _nested_counts(
                normalized,
                "failures_by_source",
            ),
            "skips_by_source": _nested_counts(
                normalized,
                "skips_by_source",
            ),
            "error_type": None,
        }
        if stage_name == "unification":
            self.data["gpt_usage"] = {
                "generation_calls": _integer(
                    normalized.get("generation_calls")
                ),
                "audit_calls": _integer(
                    normalized.get("audit_calls")
                ),
                "provider_calls": _integer(
                    normalized.get("provider_calls")
                ),
                "input_tokens": _integer(
                    normalized.get("input_tokens")
                ),
                "output_tokens": _integer(
                    normalized.get("output_tokens")
                ),
                "total_tokens": _integer(
                    normalized.get("total_tokens")
                ),
                "estimated_cost_usd": _decimal_string(
                    normalized.get("estimated_cost_usd")
                ),
            }
        return result

    def finish(self, status: str) -> dict[str, Any]:
        self.data["status"] = status
        self.data["finished_at"] = _now()
        self.data["duration_seconds"] = round(
            perf_counter() - self._started_clock,
            6,
        )
        return self.data


def _atomic_write(path: Path, content: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def write_pipeline_health_report(
    *,
    run_id: int,
    stats: Mapping[str, Any],
    output_dir: Path,
) -> dict[str, str]:
    metrics = stats.get("run_metrics")
    if not isinstance(metrics, Mapping):
        raise ValueError("run metrics are required for a health report")
    output_dir.mkdir(parents=True, exist_ok=True)
    export = stats.get("export")
    export = export if isinstance(export, Mapping) else {}
    final = export.get("final_gpt_only_publication")
    final = final if isinstance(final, Mapping) else {}
    publication_counts = final.get("counts")
    publication_counts = (
        publication_counts
        if isinstance(publication_counts, Mapping)
        else {}
    )
    publication_reconciliation = final.get("reconciliation")
    publication_reconciliation = (
        publication_reconciliation
        if isinstance(publication_reconciliation, Mapping)
        else {}
    )
    unification = stats.get("unification")
    unification = unification if isinstance(unification, Mapping) else {}
    clustering = stats.get("clustering")
    clustering = clustering if isinstance(clustering, Mapping) else {}
    extraction = stats.get("extraction")
    extraction = extraction if isinstance(extraction, Mapping) else {}
    payload = {
        "report_version": RUN_HEALTH_REPORT_VERSION,
        "run_id": int(run_id),
        "status": metrics.get("status"),
        "started_at": metrics.get("started_at"),
        "finished_at": metrics.get("finished_at"),
        "duration_seconds": metrics.get("duration_seconds"),
        "failed_stage": metrics.get("failed_stage"),
        "gpt_usage": metrics.get("gpt_usage"),
        "stages": metrics.get("stages"),
        "clustering_embedding_cache": {
            "enabled": bool(clustering.get("embedding_cache_enabled", True)),
            "hits": _integer(clustering.get("embedding_cache_hits")),
            "misses": _integer(clustering.get("embedding_cache_misses")),
            "encoded_vectors": _integer(
                clustering.get("embedding_encoded_vectors")
            ),
            "lookup_seconds": float(
                clustering.get("embedding_cache_lookup_seconds") or 0.0
            ),
            "model_load_seconds": float(
                clustering.get("embedding_model_load_seconds") or 0.0
            ),
            "encoding_seconds": float(
                clustering.get("embedding_encoding_seconds") or 0.0
            ),
            "write_seconds": float(
                clustering.get("embedding_cache_write_seconds") or 0.0
            ),
            "total_seconds": float(
                clustering.get("embedding_total_seconds") or 0.0
            ),
            "model_name": clustering.get("model_name"),
            "model_revision": clustering.get("model_revision"),
        },
        "extraction_outcomes": {
            "fresh": {
                "urls_attempted": _integer(
                    extraction.get("fresh_urls_attempted")
                ),
                "extracted_articles": _integer(
                    extraction.get("fresh_extracted_articles")
                ),
                "fetch_failures": _integer(
                    extraction.get("fresh_fetch_failures")
                ),
                "rejected_articles": _integer(
                    extraction.get("fresh_rejected_articles")
                ),
                "failures_by_source": dict(
                    extraction.get("fresh_failures_by_source", {})
                ),
            },
            "historical_retries": {
                "urls_attempted": _integer(
                    extraction.get("historical_retry_urls_attempted")
                ),
                "extracted_articles": _integer(
                    extraction.get("historical_retry_extracted_articles")
                ),
                "fetch_failures": _integer(
                    extraction.get("historical_retry_fetch_failures")
                ),
                "rejected_articles": _integer(
                    extraction.get("historical_retry_rejected_articles")
                ),
                "failures_by_source": dict(
                    extraction.get(
                        "historical_retry_failures_by_source",
                        {},
                    )
                ),
            },
        },
        "unification_outcomes": {
            "current_run_fallbacks": _integer(
                unification.get("fallbacks")
            ),
            "current_run_fallback_reasons": dict(
                unification.get("fallback_reasons", {})
            ),
            "budget_deferred": _integer(
                unification.get("budget_deferred")
            ),
            "provider_failed": _integer(
                unification.get("provider_failed")
            ),
            "audit_provider_failed": _integer(
                unification.get("audit_provider_failed")
            ),
            "audit_rejected": _integer(
                unification.get("audit_rejected")
            ),
            "cached_historical_fallbacks": _integer(
                unification.get("cached_fallbacks")
            ),
            "cached_historical_fallback_reasons": dict(
                unification.get("cached_fallback_reasons", {})
            ),
            "audit_budget_safe_routes": _integer(
                unification.get("audit_budget_safe_routes")
            ),
            "audit_policy_mode": str(
                unification.get("audit_policy_mode") or "all"
            ),
            "audit_policy_effective_mode": str(
                unification.get("audit_policy_effective_mode") or "all"
            ),
            "audit_circuit_breaker": dict(
                unification.get("audit_circuit_breaker", {})
            ),
            "audit_risk_tiers": dict(
                unification.get("audit_risk_tiers", {})
            ),
            "audit_policy_would_skip": _integer(
                unification.get("audit_policy_would_skip")
            ),
            "audit_policy_sampled": _integer(
                unification.get("audit_policy_sampled")
            ),
            "audits_skipped_low_risk": _integer(
                unification.get("audits_skipped_low_risk")
            ),
            "shadow_avoidable_audit_calls": _integer(
                unification.get("shadow_avoidable_audit_calls")
            ),
            "shadow_avoidable_audit_cost_usd": _decimal_string(
                unification.get("shadow_avoidable_audit_cost_usd")
            ),
            "audit_change_levels": dict(
                unification.get("audit_change_levels", {})
            ),
            "audit_change_levels_by_risk": dict(
                unification.get("audit_change_levels_by_risk", {})
            ),
            "semantic_partitions_applied": _integer(
                unification.get("semantic_partitions_applied")
            ),
            "semantic_partition_groups": _integer(
                unification.get("semantic_partition_groups")
            ),
            "semantic_partition_multi_groups": _integer(
                unification.get("semantic_partition_multi_groups")
            ),
            "semantic_partition_singletons": _integer(
                unification.get("semantic_partition_singletons")
            ),
        },
        "publication": {
            "counts": dict(publication_counts),
            "reconciliation": dict(publication_reconciliation),
        },
        "snapshot_dir": (
            export.get("snapshot_dir")
        ),
    }
    json_path = output_dir / "run_health.json"
    markdown_path = output_dir / "run_health.md"
    _atomic_write(
        json_path,
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    lines = [
        "# Pipeline Run Health",
        "",
        f"- Run ID: `{run_id}`",
        f"- Status: `{payload['status']}`",
        f"- Duration: `{payload['duration_seconds']}` seconds",
        f"- Failed stage: `{payload['failed_stage'] or 'none'}`",
        "",
        "| Stage | Status | Seconds | Input | Output | Skipped | Failed |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    stages = payload["stages"]
    if isinstance(stages, Mapping):
        for stage_name, stage in stages.items():
            if not isinstance(stage, Mapping):
                continue
            counts = stage.get("counts")
            counts = counts if isinstance(counts, Mapping) else {}
            lines.append(
                "| {name} | {status} | {duration} | {input} | "
                "{output} | {skipped} | {failed} |".format(
                    name=stage_name,
                    status=stage.get("status", "unknown"),
                    duration=stage.get("duration_seconds", 0),
                    input=counts.get("input", 0),
                    output=counts.get("output", 0),
                    skipped=counts.get("skipped", 0),
                    failed=counts.get("failed", 0),
                )
            )
    gpt = payload["gpt_usage"]
    gpt = gpt if isinstance(gpt, Mapping) else {}
    embedding_cache = payload["clustering_embedding_cache"]
    extraction_outcomes = payload["extraction_outcomes"]
    fresh_extraction = extraction_outcomes["fresh"]
    historical_extraction = extraction_outcomes["historical_retries"]
    outcomes = payload["unification_outcomes"]
    publication = payload["publication"]
    publication_counts = publication["counts"]
    reconciliation = publication["reconciliation"]
    reconciliation_ok = bool(reconciliation) and all(
        bool(value) for value in reconciliation.values()
    )
    lines.extend(
        [
            "",
            "## Extraction outcomes",
            "",
            f"- Fresh URLs attempted: `{fresh_extraction['urls_attempted']}`",
            f"- Fresh articles extracted: "
            f"`{fresh_extraction['extracted_articles']}`",
            f"- Fresh fetch failures: `{fresh_extraction['fetch_failures']}`",
            f"- Fresh terminal rejections: "
            f"`{fresh_extraction['rejected_articles']}`",
            f"- Historical retry URLs attempted: "
            f"`{historical_extraction['urls_attempted']}`",
            f"- Historical retries recovered: "
            f"`{historical_extraction['extracted_articles']}`",
            f"- Historical retry failures: "
            f"`{historical_extraction['fetch_failures']}`",
            f"- Historical retry terminal rejections: "
            f"`{historical_extraction['rejected_articles']}`",
            "",
            "## Clustering embedding cache",
            "",
            f"- Enabled: `{str(embedding_cache['enabled']).lower()}`",
            f"- Cache hits: `{embedding_cache['hits']}`",
            f"- Cache misses: `{embedding_cache['misses']}`",
            f"- Encoded vectors: `{embedding_cache['encoded_vectors']}`",
            f"- Cache lookup seconds: `{embedding_cache['lookup_seconds']}`",
            f"- Model load seconds: `{embedding_cache['model_load_seconds']}`",
            f"- Encoding seconds: `{embedding_cache['encoding_seconds']}`",
            f"- Cache write seconds: `{embedding_cache['write_seconds']}`",
            f"- Total embedding seconds: `{embedding_cache['total_seconds']}`",
            f"- Model snapshot: `{embedding_cache['model_name']}@"
            f"{embedding_cache['model_revision']}`",
            "",
            "## GPT usage for this run",
            "",
            f"- Generation calls: `{gpt.get('generation_calls', 0)}`",
            f"- Audit calls: `{gpt.get('audit_calls', 0)}`",
            f"- Total provider calls: `{gpt.get('provider_calls', 0)}`",
            f"- Input tokens: `{gpt.get('input_tokens', 0)}`",
            f"- Output tokens: `{gpt.get('output_tokens', 0)}`",
            f"- Total tokens: `{gpt.get('total_tokens', 0)}`",
            f"- Estimated cost USD: "
            f"`{gpt.get('estimated_cost_usd', '0')}`",
            "",
            "## Unification outcomes for this run",
            "",
            f"- New fallback decisions: "
            f"`{outcomes.get('current_run_fallbacks', 0)}`",
            f"- Safely deferred by budget: "
            f"`{outcomes.get('budget_deferred', 0)}`",
            f"- Primary provider failures: "
            f"`{outcomes.get('provider_failed', 0)}`",
            f"- Audit provider failures: "
            f"`{outcomes.get('audit_provider_failed', 0)}`",
            f"- Audit model rejections: "
            f"`{outcomes.get('audit_rejected', 0)}`",
            f"- Historical cached fallback states encountered: "
            f"`{outcomes.get('cached_historical_fallbacks', 0)}`",
            f"- Budget-safe Luna audit routes: "
            f"`{outcomes.get('audit_budget_safe_routes', 0)}`",
            f"- Audit policy mode: "
            f"`{outcomes.get('audit_policy_mode', 'all')}`",
            f"- Effective audit policy mode: "
            f"`{outcomes.get('audit_policy_effective_mode', 'all')}`",
            f"- Audit circuit breaker: "
            f"`{json.dumps(outcomes.get('audit_circuit_breaker', {}), sort_keys=True)}`",
            f"- Audit risk tiers: "
            f"`{json.dumps(outcomes.get('audit_risk_tiers', {}), sort_keys=True)}`",
            f"- Candidates the risk policy would skip: "
            f"`{outcomes.get('audit_policy_would_skip', 0)}`",
            f"- Stable low-risk quality samples: "
            f"`{outcomes.get('audit_policy_sampled', 0)}`",
            f"- Audits actually skipped as low risk: "
            f"`{outcomes.get('audits_skipped_low_risk', 0)}`",
            f"- Shadow audits potentially avoidable: "
            f"`{outcomes.get('shadow_avoidable_audit_calls', 0)}`",
            f"- Shadow avoidable audit cost USD: "
            f"`{outcomes.get('shadow_avoidable_audit_cost_usd', '0')}`",
            f"- Model-reported audit change levels: "
            f"`{json.dumps(outcomes.get('audit_change_levels', {}), sort_keys=True)}`",
            f"- Embedding clusters semantically partitioned: "
            f"`{outcomes.get('semantic_partitions_applied', 0)}`",
            f"- Resulting same-event groups: "
            f"`{outcomes.get('semantic_partition_groups', 0)}` "
            f"(`{outcomes.get('semantic_partition_multi_groups', 0)}` "
            f"multi-article, "
            f"`{outcomes.get('semantic_partition_singletons', 0)}` "
            f"necessary singleton)",
            "",
            "## Current publication outcomes",
            "",
            f"- Published stories: "
            f"`{publication_counts.get('final_unified_stories', 0)}`",
            f"- GPT-unified stories: "
            f"`{publication_counts.get('gpt_unified_stories', 0)}`",
            f"- Singleton passthrough stories: "
            f"`{publication_counts.get('singleton_passthrough_stories', 0)}`",
            f"- Evidence-safe fallback stories: "
            f"`{publication_counts.get('evidence_safe_fallback_stories', 0)}`",
            f"- Published source articles: "
            f"`{publication_counts.get('final_story_sources', 0)}`",
            f"- Explicit nonpublishable states: "
            f"`{publication_counts.get('explicit_nonpublishable_states', 0)}`",
            f"- Publication reconciliation passed: "
            f"`{str(reconciliation_ok).lower()}`",
            "",
        ]
    )
    _atomic_write(markdown_path, "\n".join(lines))
    return {
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }
