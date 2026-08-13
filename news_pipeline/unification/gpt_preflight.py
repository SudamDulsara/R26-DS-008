from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Optional, Protocol

from news_pipeline.config import PipelineConfig
from news_pipeline.unification.openai_adapter import (
    OpenAIConfigurationError,
    StructuredResponseRequest,
)


TOKENS_PER_MILLION = Decimal("1000000")
GPT_PRICING_VERSION = "openai_model_pages_2026-08-13"
GPT_PRICING_SOURCE_URL = "https://developers.openai.com/api/docs/models"
OFFLINE_INPUT_BOUND_VERSION = "utf8_request_bytes_plus_framing_v1"
DEFAULT_PROVIDER_FRAMING_TOKEN_ALLOWANCE = 1024
LONG_CONTEXT_INPUT_TOKEN_THRESHOLD = 272_000
CACHE_WRITE_INPUT_PRICE_MULTIPLIER = Decimal("1.25")
LONG_CONTEXT_INPUT_PRICE_MULTIPLIER = Decimal("2")
LONG_CONTEXT_OUTPUT_PRICE_MULTIPLIER = Decimal("1.5")


@dataclass(frozen=True)
class ModelPricing:
    model: str
    input_usd_per_million_tokens: Decimal
    output_usd_per_million_tokens: Decimal

    def __post_init__(self) -> None:
        if not self.model.strip():
            raise ValueError("pricing model must not be blank")
        if (
            not self.input_usd_per_million_tokens.is_finite()
            or self.input_usd_per_million_tokens < 0
        ):
            raise ValueError("input token price must be finite and nonnegative")
        if (
            not self.output_usd_per_million_tokens.is_finite()
            or self.output_usd_per_million_tokens < 0
        ):
            raise ValueError("output token price must be finite and nonnegative")

    def estimate(
        self,
        *,
        input_tokens: int,
        max_output_tokens: int,
    ) -> TokenCostEstimate:
        if input_tokens < 0:
            raise ValueError("input token count must not be negative")
        if max_output_tokens <= 0:
            raise ValueError("maximum output tokens must be greater than zero")

        input_cost = (
            Decimal(input_tokens)
            * self.input_usd_per_million_tokens
            / TOKENS_PER_MILLION
        )
        maximum_output_cost = (
            Decimal(max_output_tokens)
            * self.output_usd_per_million_tokens
            / TOKENS_PER_MILLION
        )
        return TokenCostEstimate(
            input_tokens=input_tokens,
            max_output_tokens=max_output_tokens,
            input_cost_usd=input_cost,
            max_output_cost_usd=maximum_output_cost,
        )


@dataclass(frozen=True)
class TokenCostEstimate:
    input_tokens: int
    max_output_tokens: int
    input_cost_usd: Decimal
    max_output_cost_usd: Decimal

    @property
    def max_total_cost_usd(self) -> Decimal:
        return self.input_cost_usd + self.max_output_cost_usd


MODEL_PRICING: Mapping[str, ModelPricing] = MappingProxyType(
    {
        "gpt-5.6-luna": ModelPricing(
            model="gpt-5.6-luna",
            input_usd_per_million_tokens=Decimal("0.20"),
            output_usd_per_million_tokens=Decimal("1.20"),
        ),
        "gpt-5.6-terra": ModelPricing(
            model="gpt-5.6-terra",
            input_usd_per_million_tokens=Decimal("2.00"),
            output_usd_per_million_tokens=Decimal("12.00"),
        ),
    }
)


class InputTokenCounter(Protocol):
    def count(self, request: StructuredResponseRequest) -> int:
        ...


class StructuredResponseGenerator(Protocol):
    def generate(self, request: StructuredResponseRequest) -> Any:
        ...


class InputTokensResource(Protocol):
    def count(self, **kwargs: Any) -> Any:
        ...


class ResponsesWithInputTokens(Protocol):
    input_tokens: InputTokensResource


class TokenCountClient(Protocol):
    responses: ResponsesWithInputTokens


class TokenCountClientFactory(Protocol):
    def __call__(self, **kwargs: Any) -> TokenCountClient:
        ...


TextFormatConverter = Callable[[type[Any]], Mapping[str, Any]]


def _sdk_text_format_converter(text_format: type[Any]) -> Mapping[str, Any]:
    # responses.parse uses this SDK helper internally. Reusing it makes the
    # count request carry the exact strict schema that generation will carry.
    from openai.lib._parsing._responses import type_to_text_format_param

    return type_to_text_format_param(text_format)


def _field_value(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(field_name)
    return getattr(value, field_name, None)


class OpenAIInputTokenCounter:
    def __init__(
        self,
        client: TokenCountClient,
        *,
        text_format_converter: TextFormatConverter = (
            _sdk_text_format_converter
        ),
    ) -> None:
        self._client = client
        self._text_format_converter = text_format_converter

    @classmethod
    def from_config(
        cls,
        config: PipelineConfig,
        *,
        client_factory: Optional[TokenCountClientFactory] = None,
        text_format_converter: TextFormatConverter = (
            _sdk_text_format_converter
        ),
    ) -> OpenAIInputTokenCounter:
        if not config.gpt_enabled:
            raise OpenAIConfigurationError(
                "GPT generation is disabled by configuration"
            )
        if not config.openai_api_key:
            raise OpenAIConfigurationError(
                "OPENAI_API_KEY is required when GPT generation is enabled"
            )

        if client_factory is None:
            from openai import OpenAI

            client_factory = OpenAI

        client = client_factory(
            api_key=config.openai_api_key,
            max_retries=0,
            timeout=config.gpt_timeout_seconds,
        )
        return cls(
            client,
            text_format_converter=text_format_converter,
        )

    def count(self, request: StructuredResponseRequest) -> int:
        text = {
            "format": self._text_format_converter(request.text_format),
        }
        if request.text_verbosity is not None:
            text["verbosity"] = request.text_verbosity
        response = self._client.responses.input_tokens.count(
            model=request.model,
            instructions=request.instructions,
            input=request.input,
            reasoning={"effort": request.reasoning_effort},
            text=text,
        )
        input_tokens = _field_value(response, "input_tokens")
        if (
            isinstance(input_tokens, bool)
            or not isinstance(input_tokens, int)
            or input_tokens < 0
        ):
            raise ValueError(
                "OpenAI token-count response did not contain a valid "
                "input_tokens value"
            )
        return input_tokens


class PreflightDecision(str, Enum):
    GENERATE = "generate"
    USE_V2_FALLBACK = "use_v2_fallback"


class PreflightReason(str, Enum):
    WITHIN_BUDGET = "within_budget"
    UNKNOWN_MODEL_PRICING = "unknown_model_pricing"
    TOKEN_COUNT_FAILED = "token_count_failed"
    PER_STORY_BUDGET_EXCEEDED = "per_story_budget_exceeded"
    PER_RUN_BUDGET_EXCEEDED = "per_run_budget_exceeded"


@dataclass(frozen=True)
class GPTPreflightReport:
    model: str
    pricing_version: str
    decision: PreflightDecision
    reason: PreflightReason
    per_story_budget_usd: Decimal
    per_run_budget_usd: Decimal
    run_reserved_cost_before_usd: Decimal
    run_reserved_cost_after_usd: Decimal
    estimate: Optional[TokenCostEstimate] = None
    input_token_count_method: str = (
        "openai_responses_input_tokens_count"
    )
    input_tokens_exact: bool = True

    @property
    def should_generate(self) -> bool:
        return self.decision is PreflightDecision.GENERATE

    def to_dict(self) -> dict[str, Any]:
        estimate = self.estimate
        return {
            "model": self.model,
            "pricing_version": self.pricing_version,
            "pricing_source_url": GPT_PRICING_SOURCE_URL,
            "input_token_count_method": self.input_token_count_method,
            "input_tokens_exact": self.input_tokens_exact,
            "decision": self.decision.value,
            "reason": self.reason.value,
            "per_story_budget_usd": _decimal_text(
                self.per_story_budget_usd
            ),
            "per_run_budget_usd": _decimal_text(
                self.per_run_budget_usd
            ),
            "run_reserved_cost_before_usd": _decimal_text(
                self.run_reserved_cost_before_usd
            ),
            "run_reserved_cost_after_usd": _decimal_text(
                self.run_reserved_cost_after_usd
            ),
            "input_tokens": (
                estimate.input_tokens if estimate is not None else None
            ),
            "max_output_tokens": (
                estimate.max_output_tokens if estimate is not None else None
            ),
            "estimated_input_cost_usd": (
                _decimal_text(estimate.input_cost_usd)
                if estimate is not None
                else None
            ),
            "estimated_max_output_cost_usd": (
                _decimal_text(estimate.max_output_cost_usd)
                if estimate is not None
                else None
            ),
            "estimated_max_total_cost_usd": (
                _decimal_text(estimate.max_total_cost_usd)
                if estimate is not None
                else None
            ),
        }


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _budget_decimal(value: Any, *, name: str) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite() or result < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return result


class GPTPreflight:
    """Run-scoped, fail-closed token and worst-case cost preflight."""

    def __init__(
        self,
        token_counter: InputTokenCounter,
        *,
        max_cost_per_story_usd: Any,
        max_cost_per_run_usd: Any,
        pricing: Mapping[str, ModelPricing] = MODEL_PRICING,
    ) -> None:
        self._token_counter = token_counter
        self._max_cost_per_story_usd = _budget_decimal(
            max_cost_per_story_usd,
            name="per-story budget",
        )
        self._max_cost_per_run_usd = _budget_decimal(
            max_cost_per_run_usd,
            name="per-run budget",
        )
        self._pricing = dict(pricing)
        self._run_reserved_cost_usd = Decimal("0")

    @classmethod
    def from_config(
        cls,
        config: PipelineConfig,
        *,
        client_factory: Optional[TokenCountClientFactory] = None,
        text_format_converter: TextFormatConverter = (
            _sdk_text_format_converter
        ),
    ) -> GPTPreflight:
        counter = OpenAIInputTokenCounter.from_config(
            config,
            client_factory=client_factory,
            text_format_converter=text_format_converter,
        )
        return cls(
            counter,
            max_cost_per_story_usd=config.gpt_max_cost_per_story_usd,
            max_cost_per_run_usd=config.gpt_max_cost_per_run_usd,
        )

    @property
    def run_reserved_cost_usd(self) -> Decimal:
        return self._run_reserved_cost_usd

    def evaluate(
        self,
        request: StructuredResponseRequest,
    ) -> GPTPreflightReport:
        reserved_before = self._run_reserved_cost_usd
        model_pricing = self._pricing.get(request.model)
        if model_pricing is None:
            return self._fallback_report(
                request=request,
                reason=PreflightReason.UNKNOWN_MODEL_PRICING,
                reserved_before=reserved_before,
            )

        try:
            input_tokens = self._token_counter.count(request)
            if (
                isinstance(input_tokens, bool)
                or not isinstance(input_tokens, int)
                or input_tokens < 0
            ):
                raise ValueError("invalid input token count")
        except Exception:
            # Do not propagate or retain exception text: a provider error can
            # contain request data or credentials. The reason code is enough.
            return self._fallback_report(
                request=request,
                reason=PreflightReason.TOKEN_COUNT_FAILED,
                reserved_before=reserved_before,
            )

        estimate = model_pricing.estimate(
            input_tokens=input_tokens,
            max_output_tokens=request.max_output_tokens,
        )
        if estimate.max_total_cost_usd > self._max_cost_per_story_usd:
            return self._fallback_report(
                request=request,
                reason=PreflightReason.PER_STORY_BUDGET_EXCEEDED,
                reserved_before=reserved_before,
                estimate=estimate,
            )

        projected_run_cost = (
            reserved_before + estimate.max_total_cost_usd
        )
        if projected_run_cost > self._max_cost_per_run_usd:
            return self._fallback_report(
                request=request,
                reason=PreflightReason.PER_RUN_BUDGET_EXCEEDED,
                reserved_before=reserved_before,
                estimate=estimate,
            )

        self._run_reserved_cost_usd = projected_run_cost
        return GPTPreflightReport(
            model=request.model,
            pricing_version=GPT_PRICING_VERSION,
            decision=PreflightDecision.GENERATE,
            reason=PreflightReason.WITHIN_BUDGET,
            per_story_budget_usd=self._max_cost_per_story_usd,
            per_run_budget_usd=self._max_cost_per_run_usd,
            run_reserved_cost_before_usd=reserved_before,
            run_reserved_cost_after_usd=projected_run_cost,
            estimate=estimate,
        )

    def _fallback_report(
        self,
        *,
        request: StructuredResponseRequest,
        reason: PreflightReason,
        reserved_before: Decimal,
        estimate: Optional[TokenCostEstimate] = None,
    ) -> GPTPreflightReport:
        return GPTPreflightReport(
            model=request.model,
            pricing_version=GPT_PRICING_VERSION,
            decision=PreflightDecision.USE_V2_FALLBACK,
            reason=reason,
            per_story_budget_usd=self._max_cost_per_story_usd,
            per_run_budget_usd=self._max_cost_per_run_usd,
            run_reserved_cost_before_usd=reserved_before,
            run_reserved_cost_after_usd=reserved_before,
            estimate=estimate,
        )


def request_input_token_upper_bound(
    request: StructuredResponseRequest,
    *,
    provider_framing_token_allowance: int = (
        DEFAULT_PROVIDER_FRAMING_TOKEN_ALLOWANCE
    ),
    text_format_converter: TextFormatConverter = (
        _sdk_text_format_converter
    ),
) -> int:
    """Bound input tokens locally without a provider token-count call."""
    if (
        isinstance(provider_framing_token_allowance, bool)
        or not isinstance(provider_framing_token_allowance, int)
        or provider_framing_token_allowance < 0
    ):
        raise ValueError(
            "provider framing token allowance must be a nonnegative integer"
        )
    text = {
        "format": text_format_converter(request.text_format),
    }
    if request.text_verbosity is not None:
        text["verbosity"] = request.text_verbosity
    payload = {
        "model": request.model,
        "instructions": request.instructions,
        "input": request.input,
        "text": text,
        "max_output_tokens": request.max_output_tokens,
        "reasoning": {"effort": request.reasoning_effort},
    }
    supplied_utf8_bytes = len(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return supplied_utf8_bytes + provider_framing_token_allowance


class OfflineRequestSizePreflight:
    """No-network preflight using a defensive request-size upper bound."""

    def __init__(
        self,
        *,
        max_cost_per_story_usd: Any,
        max_cost_per_run_usd: Any,
        pricing: Mapping[str, ModelPricing] = MODEL_PRICING,
        provider_framing_token_allowance: int = (
            DEFAULT_PROVIDER_FRAMING_TOKEN_ALLOWANCE
        ),
        text_format_converter: TextFormatConverter = (
            _sdk_text_format_converter
        ),
    ) -> None:
        self._max_cost_per_story_usd = _budget_decimal(
            max_cost_per_story_usd,
            name="per-story budget",
        )
        self._max_cost_per_run_usd = _budget_decimal(
            max_cost_per_run_usd,
            name="per-run budget",
        )
        if (
            isinstance(provider_framing_token_allowance, bool)
            or not isinstance(provider_framing_token_allowance, int)
            or provider_framing_token_allowance < 0
        ):
            raise ValueError(
                "provider framing token allowance must be a nonnegative "
                "integer"
            )
        self._pricing = dict(pricing)
        self._provider_framing_token_allowance = (
            provider_framing_token_allowance
        )
        self._text_format_converter = text_format_converter
        self._run_reserved_cost_usd = Decimal("0")

    @property
    def run_reserved_cost_usd(self) -> Decimal:
        return self._run_reserved_cost_usd

    def evaluate(
        self,
        request: StructuredResponseRequest,
    ) -> GPTPreflightReport:
        reserved_before = self._run_reserved_cost_usd
        model_pricing = self._pricing.get(request.model)
        if model_pricing is None:
            return self._fallback_report(
                request=request,
                reason=PreflightReason.UNKNOWN_MODEL_PRICING,
                reserved_before=reserved_before,
            )

        input_token_upper_bound = request_input_token_upper_bound(
            request,
            provider_framing_token_allowance=(
                self._provider_framing_token_allowance
            ),
            text_format_converter=self._text_format_converter,
        )
        input_multiplier = CACHE_WRITE_INPUT_PRICE_MULTIPLIER
        output_multiplier = Decimal("1")
        if (
            input_token_upper_bound
            > LONG_CONTEXT_INPUT_TOKEN_THRESHOLD
        ):
            input_multiplier = max(
                input_multiplier,
                LONG_CONTEXT_INPUT_PRICE_MULTIPLIER,
            )
            output_multiplier = LONG_CONTEXT_OUTPUT_PRICE_MULTIPLIER

        estimate = TokenCostEstimate(
            input_tokens=input_token_upper_bound,
            max_output_tokens=request.max_output_tokens,
            input_cost_usd=(
                Decimal(input_token_upper_bound)
                * model_pricing.input_usd_per_million_tokens
                * input_multiplier
                / TOKENS_PER_MILLION
            ),
            max_output_cost_usd=(
                Decimal(request.max_output_tokens)
                * model_pricing.output_usd_per_million_tokens
                * output_multiplier
                / TOKENS_PER_MILLION
            ),
        )
        if estimate.max_total_cost_usd > self._max_cost_per_story_usd:
            return self._fallback_report(
                request=request,
                reason=PreflightReason.PER_STORY_BUDGET_EXCEEDED,
                reserved_before=reserved_before,
                estimate=estimate,
            )

        projected_run_cost = reserved_before + estimate.max_total_cost_usd
        if projected_run_cost > self._max_cost_per_run_usd:
            return self._fallback_report(
                request=request,
                reason=PreflightReason.PER_RUN_BUDGET_EXCEEDED,
                reserved_before=reserved_before,
                estimate=estimate,
            )

        self._run_reserved_cost_usd = projected_run_cost
        return GPTPreflightReport(
            model=request.model,
            pricing_version=GPT_PRICING_VERSION,
            decision=PreflightDecision.GENERATE,
            reason=PreflightReason.WITHIN_BUDGET,
            per_story_budget_usd=self._max_cost_per_story_usd,
            per_run_budget_usd=self._max_cost_per_run_usd,
            run_reserved_cost_before_usd=reserved_before,
            run_reserved_cost_after_usd=projected_run_cost,
            estimate=estimate,
            input_token_count_method=OFFLINE_INPUT_BOUND_VERSION,
            input_tokens_exact=False,
        )

    def _fallback_report(
        self,
        *,
        request: StructuredResponseRequest,
        reason: PreflightReason,
        reserved_before: Decimal,
        estimate: Optional[TokenCostEstimate] = None,
    ) -> GPTPreflightReport:
        return GPTPreflightReport(
            model=request.model,
            pricing_version=GPT_PRICING_VERSION,
            decision=PreflightDecision.USE_V2_FALLBACK,
            reason=reason,
            per_story_budget_usd=self._max_cost_per_story_usd,
            per_run_budget_usd=self._max_cost_per_run_usd,
            run_reserved_cost_before_usd=reserved_before,
            run_reserved_cost_after_usd=reserved_before,
            estimate=estimate,
            input_token_count_method=OFFLINE_INPUT_BOUND_VERSION,
            input_tokens_exact=False,
        )

@dataclass(frozen=True)
class PreflightedGenerationResult:
    preflight: GPTPreflightReport
    generation: Optional[Any]

    @property
    def used_v2_fallback(self) -> bool:
        return not self.preflight.should_generate


class PreflightedGPTGenerator:
    """Guarantees that generation cannot bypass the run-scoped preflight."""

    def __init__(
        self,
        preflight: GPTPreflight,
        generator: StructuredResponseGenerator,
    ) -> None:
        self._preflight = preflight
        self._generator = generator

    def generate(
        self,
        request: StructuredResponseRequest,
    ) -> PreflightedGenerationResult:
        report = self._preflight.evaluate(request)
        if not report.should_generate:
            return PreflightedGenerationResult(
                preflight=report,
                generation=None,
            )

        return PreflightedGenerationResult(
            preflight=report,
            generation=self._generator.generate(request),
        )
