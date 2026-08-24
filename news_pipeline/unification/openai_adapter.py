from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Protocol

from news_pipeline.config import PipelineConfig


DEFAULT_RETRY_BASE_DELAY_SECONDS = 1.0


class StructuredResponsesResource(Protocol):
    def create(
        self,
        *,
        model: str,
        instructions: str,
        input: Any,
        text: Mapping[str, Any],
        max_output_tokens: int,
        reasoning: Mapping[str, str],
    ) -> Any:
        ...


class OpenAIClient(Protocol):
    responses: StructuredResponsesResource


class ClientFactory(Protocol):
    def __call__(self, **kwargs: Any) -> OpenAIClient:
        ...


TextFormatConverter = Callable[[type[Any]], Mapping[str, Any]]


def _sdk_text_format_converter(
    text_format: type[Any],
) -> Mapping[str, Any]:
    from openai.lib._parsing._responses import type_to_text_format_param

    return type_to_text_format_param(text_format)


class AdapterOutcome(str, Enum):
    SUCCESS = "success"
    REFUSAL = "refusal"


@dataclass(frozen=True)
class StructuredResponseRequest:
    model: str
    instructions: str
    input: Any
    text_format: type[Any]
    max_output_tokens: int
    reasoning_effort: str = "low"
    text_verbosity: Optional[str] = None
    prompt_cache_key: Optional[str] = None
    prompt_cache_options: Optional[Mapping[str, str]] = None
    explicit_developer_cache_breakpoint: bool = False

    def __post_init__(self) -> None:
        if not self.model.strip():
            raise ValueError("model must not be blank")
        if not self.instructions.strip():
            raise ValueError("instructions must not be blank")
        if self.max_output_tokens <= 0:
            raise ValueError("max_output_tokens must be greater than zero")
        if self.reasoning_effort not in {
            "none",
            "low",
            "medium",
            "high",
            "xhigh",
            "max",
        }:
            raise ValueError("reasoning_effort is not supported")
        if self.text_verbosity not in {None, "low", "medium", "high"}:
            raise ValueError("text_verbosity is not supported")
        if self.prompt_cache_key is not None and not self.prompt_cache_key.strip():
            raise ValueError("prompt_cache_key must not be blank")
        if self.explicit_developer_cache_breakpoint and (
            not self.prompt_cache_key or self.prompt_cache_options is None
        ):
            raise ValueError(
                "an explicit cache breakpoint requires a key and options"
            )


@dataclass(frozen=True)
class AdapterResult:
    outcome: AdapterOutcome
    response: Any
    attempts: int
    refusal: Optional[str] = None
    provider_seconds: float = 0.0
    retry_count: int = 0
    rate_limit_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.outcome is AdapterOutcome.SUCCESS


class OpenAIAdapterError(RuntimeError):
    pass


class OpenAIConfigurationError(OpenAIAdapterError):
    pass


class OpenAIRetryExhaustedError(OpenAIAdapterError):
    def __init__(
        self,
        attempts: int,
        last_error: Exception,
        *,
        rate_limit_count: int = 0,
    ) -> None:
        super().__init__(
            f"OpenAI request failed after {attempts} bounded attempts"
        )
        self.attempts = attempts
        self.last_error = last_error
        self.rate_limit_count = rate_limit_count


def _is_retryable_openai_error(error: Exception) -> bool:
    from openai import (
        APIConnectionError,
        APITimeoutError,
        InternalServerError,
        RateLimitError,
    )

    return isinstance(
        error,
        (
            APIConnectionError,
            APITimeoutError,
            InternalServerError,
            RateLimitError,
        ),
    )


def _field_value(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(field_name)
    return getattr(value, field_name, None)


def _extract_refusal(response: Any) -> Optional[str]:
    direct_refusal = _field_value(response, "refusal")
    if direct_refusal:
        return str(direct_refusal).strip()

    output_items = _field_value(response, "output") or ()
    for output_item in output_items:
        content_parts = _field_value(output_item, "content") or ()
        for content_part in content_parts:
            if _field_value(content_part, "type") != "refusal":
                continue
            refusal = _field_value(content_part, "refusal")
            if refusal:
                return str(refusal).strip()
            return "The model refused the structured response request."
    return None


class OpenAIResponsesAdapter:
    def __init__(
        self,
        client: OpenAIClient,
        *,
        max_retries: int,
        retry_base_delay_seconds: float = DEFAULT_RETRY_BASE_DELAY_SECONDS,
        is_retryable: Callable[[Exception], bool] = _is_retryable_openai_error,
        sleep: Callable[[float], None] = time.sleep,
        text_format_converter: TextFormatConverter = (
            _sdk_text_format_converter
        ),
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must not be negative")
        if retry_base_delay_seconds < 0:
            raise ValueError("retry_base_delay_seconds must not be negative")

        self._client = client
        self._max_retries = max_retries
        self._retry_base_delay_seconds = retry_base_delay_seconds
        self._is_retryable = is_retryable
        self._sleep = sleep
        self._text_format_converter = text_format_converter

    @classmethod
    def from_config(
        cls,
        config: PipelineConfig,
        *,
        client_factory: Optional[ClientFactory] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> OpenAIResponsesAdapter:
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
            max_retries=config.gpt_max_retries,
            sleep=sleep,
        )

    def generate(self, request: StructuredResponseRequest) -> AdapterResult:
        maximum_attempts = self._max_retries + 1

        rate_limit_count = 0
        started = time.perf_counter()
        for attempt in range(1, maximum_attempts + 1):
            try:
                text = {
                    "format": self._text_format_converter(
                        request.text_format
                    )
                }
                if request.text_verbosity is not None:
                    text["verbosity"] = request.text_verbosity
                response = self._client.responses.create(
                    **_create_parameters(request, text=text)
                )
            except Exception as error:
                if type(error).__name__ == "RateLimitError":
                    rate_limit_count += 1
                if not self._is_retryable(error):
                    raise
                if attempt >= maximum_attempts:
                    raise OpenAIRetryExhaustedError(
                        attempts=attempt,
                        last_error=error,
                        rate_limit_count=rate_limit_count,
                    ) from error

                delay_seconds = self._retry_base_delay_seconds * (
                    2 ** (attempt - 1)
                )
                self._sleep(delay_seconds)
                continue

            refusal = _extract_refusal(response)
            if refusal is not None:
                return AdapterResult(
                    outcome=AdapterOutcome.REFUSAL,
                    response=response,
                    attempts=attempt,
                    refusal=refusal,
                    provider_seconds=time.perf_counter() - started,
                    retry_count=attempt - 1,
                    rate_limit_count=rate_limit_count,
                )
            return AdapterResult(
                outcome=AdapterOutcome.SUCCESS,
                response=response,
                attempts=attempt,
                provider_seconds=time.perf_counter() - started,
                retry_count=attempt - 1,
                rate_limit_count=rate_limit_count,
            )

        raise AssertionError("bounded retry loop ended without a result")


def _create_parameters(
    request: StructuredResponseRequest,
    *,
    text: Mapping[str, Any],
) -> dict[str, Any]:
    parameters: dict[str, Any] = {
        "model": request.model,
        "text": text,
        "max_output_tokens": request.max_output_tokens,
        "reasoning": {"effort": request.reasoning_effort},
    }
    if request.explicit_developer_cache_breakpoint:
        parameters["input"] = [
            {
                "type": "message",
                "role": "developer",
                "content": [
                    {
                        "type": "input_text",
                        "text": request.instructions,
                        "prompt_cache_breakpoint": {"mode": "explicit"},
                    }
                ],
            },
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": request.input}],
            },
        ]
    else:
        parameters["instructions"] = request.instructions
        parameters["input"] = request.input
    if request.prompt_cache_key is not None:
        parameters["prompt_cache_key"] = request.prompt_cache_key
    if request.prompt_cache_options is not None:
        parameters["prompt_cache_options"] = dict(request.prompt_cache_options)
    return parameters


class AsyncOpenAIResponsesAdapter(OpenAIResponsesAdapter):
    """Async Responses adapter with the same bounded retry contract."""

    @classmethod
    def from_config(
        cls,
        config: PipelineConfig,
        *,
        client_factory: Optional[ClientFactory] = None,
    ) -> "AsyncOpenAIResponsesAdapter":
        if not config.gpt_enabled:
            raise OpenAIConfigurationError(
                "GPT generation is disabled by configuration"
            )
        if not config.openai_api_key:
            raise OpenAIConfigurationError(
                "OPENAI_API_KEY is required when GPT generation is enabled"
            )
        if client_factory is None:
            from openai import AsyncOpenAI

            client_factory = AsyncOpenAI
        client = client_factory(
            api_key=config.openai_api_key,
            max_retries=0,
            timeout=config.gpt_timeout_seconds,
        )
        return cls(client, max_retries=config.gpt_max_retries)

    async def generate_async(
        self, request: StructuredResponseRequest
    ) -> AdapterResult:
        maximum_attempts = self._max_retries + 1
        rate_limit_count = 0
        started = time.perf_counter()
        for attempt in range(1, maximum_attempts + 1):
            try:
                text = {"format": self._text_format_converter(request.text_format)}
                if request.text_verbosity is not None:
                    text["verbosity"] = request.text_verbosity
                response = await self._client.responses.create(
                    **_create_parameters(request, text=text)
                )
            except Exception as error:
                if type(error).__name__ == "RateLimitError":
                    rate_limit_count += 1
                if not self._is_retryable(error):
                    raise
                if attempt >= maximum_attempts:
                    raise OpenAIRetryExhaustedError(
                        attempt,
                        error,
                        rate_limit_count=rate_limit_count,
                    ) from error
                delay_seconds = self._retry_base_delay_seconds * (
                    2 ** (attempt - 1)
                )
                await asyncio.sleep(delay_seconds)
                continue
            refusal = _extract_refusal(response)
            return AdapterResult(
                outcome=(
                    AdapterOutcome.REFUSAL
                    if refusal is not None
                    else AdapterOutcome.SUCCESS
                ),
                response=response,
                attempts=attempt,
                refusal=refusal,
                provider_seconds=time.perf_counter() - started,
                retry_count=attempt - 1,
                rate_limit_count=rate_limit_count,
            )
        raise AssertionError("bounded retry loop ended without a result")
