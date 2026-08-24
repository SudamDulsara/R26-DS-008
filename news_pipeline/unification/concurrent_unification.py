from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from typing import Any, Mapping, Optional, Sequence

from pydantic import ValidationError

from news_pipeline.config import PipelineConfig
from news_pipeline.storage.database import get_connection
from news_pipeline.unification.evidence_aliases import (
    EvidenceAliases,
    alias_provider_request,
    build_evidence_aliases,
    expand_provider_result,
)
from news_pipeline.unification.gpt_preflight import (
    response_usage_cost_usd,
)
from news_pipeline.unification.openai_adapter import (
    AdapterResult,
    AsyncOpenAIResponsesAdapter,
    StructuredResponseRequest,
)


CONCURRENT_UNIFICATION_VERSION = "bounded_async_rolling_single_writer_v3"
PROVIDER_LOOKAHEAD_MULTIPLIER = 2


def _request_key(request: StructuredResponseRequest) -> str:
    return json.dumps(
        {
            "model": request.model,
            "instructions": request.instructions,
            "input": request.input,
            "text_format": (
                request.text_format.__module__ + "." + request.text_format.__qualname__
            ),
            "max_output_tokens": request.max_output_tokens,
            "reasoning_effort": request.reasoning_effort,
            "text_verbosity": request.text_verbosity,
            "prompt_cache_key": request.prompt_cache_key,
            "prompt_cache_options": request.prompt_cache_options,
            "explicit_developer_cache_breakpoint": (
                request.explicit_developer_cache_breakpoint
            ),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


@dataclass
class ProviderRecord:
    cluster_key: str
    phase: str
    request: StructuredResponseRequest
    queued_at: float
    started_at: float = 0.0
    completed_at: float = 0.0
    parse_validation_seconds: float = 0.0
    alias_expansions: int = 0
    direct_rate_limit_count: int = 0
    result: Optional[AdapterResult] = None
    error: Optional[BaseException] = None

    def metrics(self, coordinator_wait_seconds: float) -> dict[str, Any]:
        response = self.result.response if self.result is not None else None
        usage = _field(response, "usage")
        details = _field(usage, "input_tokens_details")
        return {
            "cluster_key": self.cluster_key,
            "phase": self.phase,
            "queue_seconds": max(0.0, self.started_at - self.queued_at),
            "provider_seconds": (
                float(self.result.provider_seconds)
                if self.result is not None and self.result.provider_seconds
                else max(0.0, self.completed_at - self.started_at)
            ),
            "parse_validation_seconds": self.parse_validation_seconds,
            "coordinator_wait_seconds": coordinator_wait_seconds,
            "cache_read_tokens": int(_field(details, "cached_tokens", 0) or 0),
            "cache_write_tokens": int(
                _field(details, "cache_write_tokens", 0) or 0
            ),
            "alias_expansions": self.alias_expansions,
            "retry_count": (
                int(self.result.retry_count)
                if self.result is not None
                else max(0, int(getattr(self.error, "attempts", 1)) - 1)
            ),
            "rate_limit_count": (
                int(self.result.rate_limit_count)
                if self.result is not None
                else max(
                    self.direct_rate_limit_count,
                    int(getattr(self.error, "rate_limit_count", 0)),
                )
            ),
        }


@dataclass
class CandidatePlan:
    candidate: tuple[Any, ...]
    aliases: EvidenceAliases
    route: Any = None
    used_budget_safe_route: bool = False
    audit_capacity_report: Any = None
    primary_report: Any = None
    primary_record: Optional[ProviderRecord] = None
    primary_values: Optional[dict[str, Any]] = None
    audit_request: Optional[StructuredResponseRequest] = None
    audit_report: Any = None
    audit_record: Optional[ProviderRecord] = None

    @property
    def cluster_key(self) -> str:
        return str(self.candidate[0]["cluster_key"])


class ReplayPreflight:
    def __init__(self) -> None:
        self._fits: dict[str, deque[Optional[StructuredResponseRequest]]] = (
            defaultdict(deque)
        )
        self._evaluations: dict[str, deque[Any]] = defaultdict(deque)
        self._condition = threading.Condition()
        self._finished = False
        self._failure: Optional[BaseException] = None

    def record_fit(
        self,
        request: StructuredResponseRequest,
        fitted: Optional[StructuredResponseRequest],
    ) -> None:
        with self._condition:
            self._fits[_request_key(request)].append(fitted)
            self._condition.notify_all()

    def record_evaluation(
        self, request: StructuredResponseRequest, report: Any
    ) -> None:
        with self._condition:
            self._evaluations[_request_key(request)].append(report)
            self._condition.notify_all()

    def finish(self, failure: Optional[BaseException] = None) -> None:
        with self._condition:
            self._failure = failure
            self._finished = True
            self._condition.notify_all()

    def _next(self, queues: dict[str, deque[Any]], key: str, kind: str) -> Any:
        with self._condition:
            while not queues[key] and not self._finished:
                self._condition.wait()
            if queues[key]:
                return queues[key].popleft()
            if self._failure is not None:
                raise RuntimeError("provider prefetch failed") from self._failure
            raise AssertionError(f"missing replay {kind}")

    def fit_request_to_budget(
        self,
        request: StructuredResponseRequest,
        *,
        minimum_output_tokens: int = 1024,
    ) -> Optional[StructuredResponseRequest]:
        del minimum_output_tokens
        return self._next(
            self._fits,
            _request_key(request),
            "fit decision",
        )

    def evaluate(self, request: StructuredResponseRequest) -> Any:
        return self._next(
            self._evaluations,
            _request_key(request),
            "budget decision",
        )

    def release(self, report: Any) -> None:
        del report

    def settle(self, report: Any, actual_cost_usd: Any) -> None:
        del report, actual_cost_usd


class ReplayGenerator:
    def __init__(self, *, buffer_limit: int) -> None:
        self._records: dict[str, deque[ProviderRecord]] = defaultdict(deque)
        self._condition = threading.Condition()
        self._finished = False
        self._failure: Optional[BaseException] = None
        self._cancelled = False
        self.buffer_limit = max(1, int(buffer_limit))
        self._buffered_records = 0
        self.maximum_buffered_records = 0
        self.replays_before_provider_pass_complete = 0
        self.call_metrics: list[dict[str, Any]] = []

    def publish(self, record: ProviderRecord) -> None:
        with self._condition:
            while (
                self._buffered_records >= self.buffer_limit
                and not self._cancelled
            ):
                self._condition.wait()
            if self._cancelled:
                raise RuntimeError("provider replay was cancelled")
            self._records[_request_key(record.request)].append(record)
            self._buffered_records += 1
            self.maximum_buffered_records = max(
                self.maximum_buffered_records,
                self._buffered_records,
            )
            self._condition.notify_all()

    def cancel(self) -> None:
        with self._condition:
            self._cancelled = True
            self._condition.notify_all()

    def finish(self, failure: Optional[BaseException] = None) -> None:
        with self._condition:
            self._failure = failure
            self._finished = True
            self._condition.notify_all()

    def generate(self, request: StructuredResponseRequest) -> AdapterResult:
        key = _request_key(request)
        with self._condition:
            while not self._records[key] and not self._finished:
                self._condition.wait()
            if not self._records[key]:
                if self._failure is not None:
                    raise RuntimeError("provider prefetch failed") from self._failure
                raise AssertionError("missing replay provider result")
            if not self._finished:
                self.replays_before_provider_pass_complete += 1
            record = self._records[key].popleft()
            self._buffered_records -= 1
            self._condition.notify_all()
        coordinator_wait = max(0.0, time.perf_counter() - record.completed_at)
        self.call_metrics.append(record.metrics(coordinator_wait))
        if record.error is not None:
            raise record.error
        if record.result is None:
            raise AssertionError("replay result is absent")
        return record.result


class CentralBudgetManager:
    """The coordinator is the only owner of mutable preflight budget state."""

    def __init__(self, preflight: Any, replay: ReplayPreflight) -> None:
        self.preflight = preflight
        self.replay = replay
        self.reservations = 0
        self.settlements = 0
        self.releases = 0

    def _wire(
        self,
        request: StructuredResponseRequest,
        aliases: EvidenceAliases,
        enabled: bool,
    ) -> StructuredResponseRequest:
        return alias_provider_request(request, aliases) if enabled else request

    def fit(
        self,
        request: StructuredResponseRequest,
        *,
        aliases: EvidenceAliases,
        aliases_enabled: bool,
        minimum_output_tokens: int,
    ) -> Optional[StructuredResponseRequest]:
        wire = self._wire(request, aliases, aliases_enabled)
        if hasattr(self.preflight, "fit_request_to_budget"):
            fitted_wire = self.preflight.fit_request_to_budget(
                wire, minimum_output_tokens=minimum_output_tokens
            )
        else:
            fitted_wire = wire
        fitted = (
            None
            if fitted_wire is None
            else replace(request, max_output_tokens=fitted_wire.max_output_tokens)
        )
        self.replay.record_fit(request, fitted)
        return fitted

    def reserve(
        self,
        request: StructuredResponseRequest,
        *,
        aliases: EvidenceAliases,
        aliases_enabled: bool,
    ) -> Any:
        report = self.preflight.evaluate(
            self._wire(request, aliases, aliases_enabled)
        )
        self.replay.record_evaluation(request, report)
        if report.should_generate:
            self.reservations += 1
        return report

    def reserve_capacity(
        self,
        request: StructuredResponseRequest,
        *,
        aliases: EvidenceAliases,
        aliases_enabled: bool,
    ) -> Any:
        if not hasattr(self.preflight, "release"):
            self.replay.record_evaluation(request, None)
            return None
        return self.reserve(
            request,
            aliases=aliases,
            aliases_enabled=aliases_enabled,
        )

    def settle(self, report: Any, result: AdapterResult, model: str) -> None:
        if hasattr(self.preflight, "settle"):
            self.preflight.settle(
                report, response_usage_cost_usd(result, model)
            )
        self.settlements += 1

    def release(self, report: Any) -> None:
        if report is not None and hasattr(self.preflight, "release"):
            self.preflight.release(report)
            self.releases += 1


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


async def _provider_call(
    *,
    generator: Any,
    semaphore: asyncio.Semaphore,
    plan: CandidatePlan,
    phase: str,
    request: StructuredResponseRequest,
    aliases_enabled: bool,
) -> ProviderRecord:
    record = ProviderRecord(
        cluster_key=plan.cluster_key,
        phase=phase,
        request=request,
        queued_at=time.perf_counter(),
    )
    async with semaphore:
        record.started_at = time.perf_counter()
        wire_request = (
            alias_provider_request(request, plan.aliases)
            if aliases_enabled
            else request
        )
        try:
            if hasattr(generator, "generate_async"):
                result = await generator.generate_async(wire_request)
            else:
                result = await asyncio.to_thread(generator.generate, wire_request)
            if aliases_enabled:
                result, record.alias_expansions = expand_provider_result(
                    result, plan.aliases
                )
            record.result = result
        except Exception as error:
            record.error = error
            if type(error).__name__ == "RateLimitError":
                record.direct_rate_limit_count = 1
        record.completed_at = time.perf_counter()
    return record


def _selected_provider_candidates(
    *,
    config: PipelineConfig,
    force: bool,
    cluster_keys: Optional[Sequence[str]],
    correction_requirements_by_story: Optional[Mapping[str, str]],
    provider_candidates_remaining: int,
) -> tuple[list[tuple[Any, ...]], dict[str, str]]:
    from news_pipeline.unification import production as prod

    connection = get_connection(config)
    try:
        candidates = prod._select_gpt_generation_candidates(
            connection, cluster_keys
        )
        selected_requirements = {
            str(key): str(value).strip()
            for key, value in (correction_requirements_by_story or {}).items()
        }
        candidate_keys = {str(candidate[0]["cluster_key"]) for candidate in candidates}
        if correction_requirements_by_story is not None and (
            any(not key or not value for key, value in selected_requirements.items())
            or set(selected_requirements) != candidate_keys
        ):
            raise ValueError(
                "correction requirements must exactly cover selected stories"
            )
        provider_candidates: list[tuple[Any, ...]] = []
        for cluster, members, article_records in candidates:
            try:
                identity = prod.build_generation_identity(
                    cluster=cluster,
                    members=members,
                    article_records_by_id=article_records,
                    config=config,
                    correction_requirements=selected_requirements.get(
                        str(cluster["cluster_key"])
                    ),
                )
            except (ValueError, ValidationError):
                continue
            cached = prod.load_cached_version(
                connection, identity.request_fingerprint_sha256
            )
            reusable = (
                not force
                and not prod._cached_candidate_requires_autonomous_audit(
                    cached, config=config
                )
                and (
                    prod.version_is_deployable_gpt(cached)
                    or prod.version_is_pending_validator_warning(cached)
                    or (
                        cached is not None
                        and (
                            cached["generation_status"]
                            == prod.GENERATION_STATUS_FALLBACK
                            or prod.human_review_blocks_version(cached)
                        )
                        and cached.get("response_id")
                    )
                )
            )
            if not reusable:
                provider_candidates.append(
                    (cluster, members, article_records, identity, cached, False)
                )
    finally:
        connection.close()
    provider_candidates.sort(
        key=lambda candidate: (
            str(candidate[0].get("event_date_end") or ""),
            int(candidate[0]["article_count"]),
            int(candidate[0]["id"]),
        ),
        reverse=True,
    )
    limit = len(provider_candidates)
    if config.gpt_only_publication_enabled:
        limit = min(
            limit,
            config.gpt_max_clusters_per_run,
            provider_candidates_remaining,
        )
    return provider_candidates[:limit], selected_requirements


async def _prefetch_pass(
    *,
    plans: list[CandidatePlan],
    generator: Any,
    budget: CentralBudgetManager,
    config: PipelineConfig,
    audit_policy_config: PipelineConfig,
    force: bool,
    worker_limit: int,
    record_sink: Optional[Any] = None,
) -> tuple[list[ProviderRecord], list[dict[str, int]], int, int]:
    from news_pipeline.unification import production as prod

    records: list[ProviderRecord] = []
    backpressure_events: list[dict[str, int]] = []
    effective_workers = min(config.gpt_unification_workers, worker_limit)
    minimum_effective_workers = effective_workers
    start = 0
    while start < len(plans):
        window_record_start = len(records)
        # Reserve in deterministic candidate order, but keep one bounded
        # worker-set queued behind the active calls. A fast peer can then
        # release its provider slot to the next primary while a slow earlier
        # call is still running. The lookahead never exceeds the existing
        # cross-thread provider-record buffer bound.
        provider_lookahead = (
            effective_workers * PROVIDER_LOOKAHEAD_MULTIPLIER
        )
        window = plans[start : start + provider_lookahead]
        start += len(window)
        semaphore = asyncio.Semaphore(effective_workers)
        primary_tasks: list[tuple[CandidatePlan, asyncio.Task[ProviderRecord]]] = []
        for plan in window:
            cluster, members, _articles, identity, cached, _ = plan.candidate
            audit_only_cached = bool(
                not force
                and cached is not None
                and prod._cached_candidate_requires_autonomous_audit(
                    cached, config=config
                )
            )
            plan.route = prod.classify_audit_route(
                cluster=cluster, members=members, config=config
            )
            reserve_audit = bool(
                config.gpt_autonomous_audit_enabled
                and not audit_only_cached
                and (
                    audit_policy_config.gpt_audit_policy_mode in {"all", "shadow"}
                    or plan.route.risk_tier != "low"
                )
            )
            if reserve_audit:
                capacity = prod._build_audit_capacity_probe(
                    identity=identity, route=plan.route, config=config
                )
                fitted = budget.fit(
                    capacity,
                    aliases=plan.aliases,
                    aliases_enabled=config.gpt_evidence_aliases_enabled,
                    minimum_output_tokens=prod.AUDIT_CAPACITY_MIN_OUTPUT_TOKENS,
                )
                if fitted is None:
                    safe_route = prod._budget_safe_audit_route(
                        plan.route, config=config
                    )
                    if safe_route is not None:
                        safe_capacity = prod._build_audit_capacity_probe(
                            identity=identity, route=safe_route, config=config
                        )
                        fitted = budget.fit(
                            safe_capacity,
                            aliases=plan.aliases,
                            aliases_enabled=config.gpt_evidence_aliases_enabled,
                            minimum_output_tokens=prod.AUDIT_CAPACITY_MIN_OUTPUT_TOKENS,
                        )
                        if fitted is not None:
                            plan.route = safe_route
                            plan.used_budget_safe_route = True
                if fitted is None:
                    continue
                plan.audit_capacity_report = budget.reserve_capacity(
                    fitted,
                    aliases=plan.aliases,
                    aliases_enabled=config.gpt_evidence_aliases_enabled,
                )
                if (
                    plan.audit_capacity_report is not None
                    and not plan.audit_capacity_report.should_generate
                ):
                    continue
            if audit_only_cached:
                plan.primary_values = dict(cached)
                continue
            plan.primary_report = budget.reserve(
                identity.request,
                aliases=plan.aliases,
                aliases_enabled=config.gpt_evidence_aliases_enabled,
            )
            if not plan.primary_report.should_generate:
                budget.release(plan.audit_capacity_report)
                continue
            task = asyncio.create_task(
                _provider_call(
                    generator=generator,
                    semaphore=semaphore,
                    plan=plan,
                    phase="primary",
                    request=identity.request,
                    aliases_enabled=config.gpt_evidence_aliases_enabled,
                )
            )
            primary_tasks.append((plan, task))

        primary_task_by_key = {
            plan.cluster_key: task for plan, task in primary_tasks
        }
        audit_tasks: list[tuple[CandidatePlan, asyncio.Task[ProviderRecord]]] = []
        for plan in window:
            cluster, _members, _articles, identity, cached, _ = plan.candidate
            task = primary_task_by_key.get(plan.cluster_key)
            if task is not None:
                record = await task
                plan.primary_record = record
                records.append(record)
                if record.error is not None or record.result is None:
                    if record_sink is not None:
                        record_sink(record)
                    budget.release(plan.primary_report)
                    budget.release(plan.audit_capacity_report)
                    continue
                budget.settle(
                    plan.primary_report,
                    record.result,
                    identity.request.model,
                )
                started = time.perf_counter()
                plan.primary_values = prod._interpret_generation(
                    base=prod._base_version_values(cluster=cluster, identity=identity),
                    identity=identity,
                    preflight=plan.primary_report,
                    generation=record.result,
                )
                record.parse_validation_seconds = time.perf_counter() - started
                if record_sink is not None:
                    record_sink(record)
            if plan.primary_values is None or not config.gpt_autonomous_audit_enabled:
                continue
            audit_only_cached = bool(
                not force
                and cached is not None
                and prod._cached_candidate_requires_autonomous_audit(
                    cached, config=config
                )
            )
            decision = prod.decide_autonomous_audit(
                route=plan.route,
                primary=plan.primary_values,
                request_fingerprint_sha256=identity.request_fingerprint_sha256,
                config=audit_policy_config,
                force_audit=audit_only_cached,
            )
            if not decision.should_audit:
                continue
            try:
                candidate_payload = json.loads(
                    str(plan.primary_values.get("output_json") or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                candidate_payload = {}
            try:
                validation_payload = json.loads(
                    str(plan.primary_values.get("validation_json") or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                validation_payload = {}
            request = prod.build_autonomous_audit_request(
                contract_input=identity.contract_input,
                candidate=candidate_payload,
                validation=validation_payload,
                route=plan.route,
                config=config,
            )
            budget.release(plan.audit_capacity_report)
            fitted = budget.fit(
                request,
                aliases=plan.aliases,
                aliases_enabled=config.gpt_evidence_aliases_enabled,
                minimum_output_tokens=prod.AUDIT_CAPACITY_MIN_OUTPUT_TOKENS,
            )
            if fitted is None:
                safe_route = prod._budget_safe_audit_route(plan.route, config=config)
                if safe_route is not None:
                    safe_request = prod.build_autonomous_audit_request(
                        contract_input=identity.contract_input,
                        candidate=candidate_payload,
                        validation=validation_payload,
                        route=safe_route,
                        config=config,
                    )
                    fitted = budget.fit(
                        safe_request,
                        aliases=plan.aliases,
                        aliases_enabled=config.gpt_evidence_aliases_enabled,
                        minimum_output_tokens=prod.AUDIT_CAPACITY_MIN_OUTPUT_TOKENS,
                    )
                    if fitted is not None:
                        request = fitted
                        plan.route = safe_route
                if fitted is None:
                    fitted = request
            else:
                request = fitted
            plan.audit_request = request
            plan.audit_report = budget.reserve(
                request,
                aliases=plan.aliases,
                aliases_enabled=config.gpt_evidence_aliases_enabled,
            )
            if not plan.audit_report.should_generate:
                continue
            audit_tasks.append(
                (
                    plan,
                    asyncio.create_task(
                        _provider_call(
                            generator=generator,
                            semaphore=semaphore,
                            plan=plan,
                            phase="audit",
                            request=request,
                            aliases_enabled=config.gpt_evidence_aliases_enabled,
                        )
                    ),
                )
            )
        for plan, task in audit_tasks:
            record = await task
            plan.audit_record = record
            records.append(record)
            if record.error is not None or record.result is None:
                if record_sink is not None:
                    record_sink(record)
                budget.release(plan.audit_report)
                continue
            budget.settle(plan.audit_report, record.result, plan.audit_request.model)
            started = time.perf_counter()
            try:
                assessment = prod._parse_autonomous_audit_response(
                    record.result.response
                )
                if assessment.corrected_story is not None:
                    identity = plan.candidate[3]
                    audit_base = prod._base_version_values(
                        cluster=plan.candidate[0], identity=identity
                    )
                    audit_base.update(
                        {
                            "model_name": plan.route.model,
                            "reasoning_effort": plan.route.reasoning_effort,
                            "max_output_tokens": plan.audit_request.max_output_tokens,
                        }
                    )
                    prod._interpret_generation(
                        base=audit_base,
                        identity=identity,
                        preflight=plan.audit_report,
                        generation=record.result,
                        structured_output_override=assessment.corrected_story,
                    )
            except Exception:
                pass
            record.parse_validation_seconds = time.perf_counter() - started
            if record_sink is not None:
                record_sink(record)
        window_rate_limits = sum(
            int(record.metrics(0.0)["rate_limit_count"])
            for record in records[window_record_start:]
        )
        if window_rate_limits and effective_workers > 1:
            previous_workers = effective_workers
            effective_workers = max(1, effective_workers // 2)
            minimum_effective_workers = min(
                minimum_effective_workers, effective_workers
            )
            backpressure_events.append(
                {
                    "after_candidate_count": start,
                    "rate_limits": window_rate_limits,
                    "previous_workers": previous_workers,
                    "effective_workers": effective_workers,
                }
            )
    return (
        records,
        backpressure_events,
        minimum_effective_workers,
        effective_workers,
    )


def run_concurrent_gpt_unification(
    *,
    no_gpt: bool,
    force: bool,
    config: PipelineConfig,
    generator: Any,
    preflight: Any,
    cluster_keys: Optional[Sequence[str]],
    correction_requirements_by_story: Optional[Mapping[str, str]],
    provider_candidates_remaining: Optional[int],
) -> dict[str, Any]:
    from news_pipeline.unification import production as prod
    from news_pipeline.unification.ongoing_generation_policy import (
        enforce_ongoing_generation_policy,
    )

    if no_gpt or not config.gpt_enabled or (
        not config.openai_api_key and generator is None
    ):
        return prod.run_gpt_unification(
            no_gpt=no_gpt,
            force=force,
            config=config,
            generator=generator,
            preflight=preflight,
            cluster_keys=cluster_keys,
            correction_requirements_by_story=correction_requirements_by_story,
            _provider_candidates_remaining=provider_candidates_remaining,
            _concurrent_internal=True,
        )
    remaining = (
        config.gpt_max_clusters_per_run
        if provider_candidates_remaining is None
        else max(0, int(provider_candidates_remaining))
    )
    candidates, _requirements = _selected_provider_candidates(
        config=config,
        force=force,
        cluster_keys=cluster_keys,
        correction_requirements_by_story=correction_requirements_by_story,
        provider_candidates_remaining=remaining,
    )
    if generator is None:
        policy = enforce_ongoing_generation_policy(
            config=config, selected_cluster_count=len(candidates)
        )
        preflight = preflight or policy.preflight
        generator = AsyncOpenAIResponsesAdapter.from_config(config)
        policy_payload = policy.to_dict()
    else:
        policy_payload = None
    if preflight is None:
        preflight = prod.GPTPreflight.from_config(config)

    all_stats: Optional[dict[str, Any]] = None
    all_call_metrics: list[dict[str, Any]] = []
    persistence_metrics: list[dict[str, Any]] = []
    maximum_in_flight = 0
    minimum_effective_workers = config.gpt_unification_workers
    final_effective_workers = config.gpt_unification_workers
    adaptive_worker_limit = config.gpt_unification_workers
    backpressure_events: list[dict[str, int]] = []
    budget_totals = {"reservations": 0, "settlements": 0, "releases": 0}
    maximum_buffered_provider_records = 0
    streaming_replays = 0
    streaming_replays_before_provider_pass_complete = 0
    pending_keys = cluster_keys
    pending_requirements = correction_requirements_by_story
    while remaining > 0:
        candidates, _ = _selected_provider_candidates(
            config=config,
            force=force if all_stats is None else False,
            cluster_keys=pending_keys,
            correction_requirements_by_story=pending_requirements,
            provider_candidates_remaining=remaining,
        )
        replay_preflight = ReplayPreflight()
        budget = CentralBudgetManager(preflight, replay_preflight)
        connection = get_connection(config)
        try:
            breaker = prod._audit_circuit_breaker_status(connection, config=config)
        finally:
            connection.close()
        audit_config = replace(
            config, gpt_audit_policy_mode=str(breaker["effective_mode"])
        )
        plans = [
            CandidatePlan(
                candidate=candidate,
                aliases=build_evidence_aliases(candidate[3].contract_input),
            )
            for candidate in candidates
        ]
        replay_generator = ReplayGenerator(
            buffer_limit=config.gpt_unification_workers * 2
        )
        producer_result: dict[str, Any] = {}

        def produce_provider_records() -> None:
            failure: Optional[BaseException] = None
            try:
                producer_result["value"] = asyncio.run(
                    _prefetch_pass(
                        plans=plans,
                        generator=generator,
                        budget=budget,
                        config=config,
                        audit_policy_config=audit_config,
                        force=force if all_stats is None else False,
                        worker_limit=adaptive_worker_limit,
                        record_sink=replay_generator.publish,
                    )
                )
            except BaseException as error:
                failure = error
                producer_result["failure"] = error
            finally:
                replay_preflight.finish(failure)
                replay_generator.finish(failure)

        producer = threading.Thread(
            target=produce_provider_records,
            name="news-pipeline-provider-prefetch",
        )
        producer.start()
        persistence_start = len(persistence_metrics)
        token = prod._PERSISTENCE_TIMINGS.set(persistence_metrics)
        try:
            pass_stats = prod.run_gpt_unification(
                no_gpt=False,
                force=force if all_stats is None else False,
                config=config,
                generator=replay_generator,
                preflight=replay_preflight,
                cluster_keys=pending_keys,
                correction_requirements_by_story=pending_requirements,
                _provider_candidates_remaining=remaining,
                _concurrent_internal=True,
                _disable_followups=True,
                _provider_replay_order=[plan.cluster_key for plan in plans],
            )
        except BaseException:
            replay_generator.cancel()
            raise
        finally:
            prod._PERSISTENCE_TIMINGS.reset(token)
            producer.join()
        if "failure" in producer_result:
            raise RuntimeError("provider prefetch failed") from producer_result[
                "failure"
            ]
        (
            records,
            pass_backpressure_events,
            pass_minimum_effective_workers,
            pass_final_effective_workers,
        ) = producer_result["value"]
        backpressure_events.extend(pass_backpressure_events)
        minimum_effective_workers = min(
            minimum_effective_workers, pass_minimum_effective_workers
        )
        final_effective_workers = pass_final_effective_workers
        adaptive_worker_limit = pass_final_effective_workers
        events = sorted(
            [
                (record.started_at, 1)
                for record in records
                if record.started_at
            ]
            + [
                (record.completed_at, -1)
                for record in records
                if record.completed_at
            ],
            key=lambda event: (event[0], event[1]),
        )
        in_flight = 0
        for _timestamp, delta in events:
            in_flight += delta
            maximum_in_flight = max(maximum_in_flight, in_flight)
        persistence_by_cluster: dict[str, float] = defaultdict(float)
        for item in persistence_metrics[persistence_start:]:
            persistence_by_cluster[str(item.get("cluster_key") or "")] += float(
                item.get("persistence_seconds") or 0.0
            )
        last_call_by_cluster: dict[str, dict[str, Any]] = {}
        for item in replay_generator.call_metrics:
            item["persistence_seconds"] = 0.0
            last_call_by_cluster[str(item["cluster_key"])] = item
        for key, seconds in persistence_by_cluster.items():
            if key in last_call_by_cluster:
                last_call_by_cluster[key]["persistence_seconds"] = seconds
        all_call_metrics.extend(replay_generator.call_metrics)
        maximum_buffered_provider_records = max(
            maximum_buffered_provider_records,
            replay_generator.maximum_buffered_records,
        )
        streaming_replays += len(replay_generator.call_metrics)
        streaming_replays_before_provider_pass_complete += (
            replay_generator.replays_before_provider_pass_complete
        )
        for key in budget_totals:
            budget_totals[key] += int(getattr(budget, key))
        all_stats = (
            pass_stats
            if all_stats is None
            else prod._merge_unification_pass_stats(all_stats, pass_stats)
        )
        remaining = max(
            0,
            remaining - int(pass_stats.get("provider_request_candidates") or 0),
        )
        followups = tuple(
            sorted(set(pass_stats.get("semantic_partition_followup_keys") or []))
        )
        if not followups or remaining <= 0:
            break
        pending_keys = followups
        pending_requirements = None
        force = False
    if all_stats is None:
        all_stats = prod.run_gpt_unification(
            no_gpt=False,
            force=force,
            config=config,
            generator=generator,
            preflight=preflight,
            cluster_keys=cluster_keys,
            correction_requirements_by_story=correction_requirements_by_story,
            _provider_candidates_remaining=remaining,
            _concurrent_internal=True,
        )
    if policy_payload is not None:
        all_stats["ongoing_generation_policy"] = policy_payload
    all_stats["concurrent_unification"] = {
        "version": CONCURRENT_UNIFICATION_VERSION,
        "enabled": True,
        "configured_workers": config.gpt_unification_workers,
        "deterministic_streaming_commit": True,
        "streaming_replays": streaming_replays,
        "streaming_replays_before_provider_pass_complete": (
            streaming_replays_before_provider_pass_complete
        ),
        "maximum_buffered_provider_records": maximum_buffered_provider_records,
        "provider_record_buffer_limit": config.gpt_unification_workers * 2,
        "provider_lookahead_limit": (
            config.gpt_unification_workers * PROVIDER_LOOKAHEAD_MULTIPLIER
        ),
        "maximum_in_flight": maximum_in_flight,
        "minimum_effective_workers": minimum_effective_workers,
        "final_effective_workers": final_effective_workers,
        "backpressure_events": backpressure_events,
        "provider_call_metrics": all_call_metrics,
        "persistence_metrics": persistence_metrics,
        "budget": budget_totals,
        "cache_read_tokens": sum(
            int(item["cache_read_tokens"]) for item in all_call_metrics
        ),
        "cache_write_tokens": sum(
            int(item["cache_write_tokens"]) for item in all_call_metrics
        ),
        "alias_expansions": sum(
            int(item["alias_expansions"]) for item in all_call_metrics
        ),
        "retries": sum(int(item["retry_count"]) for item in all_call_metrics),
        "rate_limits": sum(
            int(item["rate_limit_count"]) for item in all_call_metrics
        ),
    }
    return all_stats
