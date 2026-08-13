from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from time import perf_counter
from typing import Any, Callable, Mapping


RUN_METRICS_VERSION = "pipeline_run_metrics_v1"
RUN_HEALTH_REPORT_VERSION = "pipeline_run_health_v1"


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
        return {
            "input": output + failed,
            "output": output,
            "skipped": 0,
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
        return {
            "input": _integer(result.get("clusters_seen")),
            "output": _integer(result.get("accepted")),
            "skipped": _integer(result.get("cache_hits")),
            "failed": (
                _integer(result.get("fallbacks"))
                + _integer(result.get("invalid_inputs"))
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
        "snapshot_dir": (
            stats.get("export", {}).get("snapshot_dir")
            if isinstance(stats.get("export"), Mapping)
            else None
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
    lines.extend(
        [
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
        ]
    )
    _atomic_write(markdown_path, "\n".join(lines))
    return {
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }
