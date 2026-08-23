from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any, Mapping

from news_pipeline.unification.gpt_contract import (
    GPTUnifiedStoryInputV2,
    UNTRUSTED_SOURCE_DATA_BEGIN,
    UNTRUSTED_SOURCE_DATA_END,
)
from news_pipeline.unification.openai_adapter import (
    AdapterResult,
    StructuredResponseRequest,
)


EVIDENCE_ALIAS_VERSION = "request_local_evidence_aliases_v1"
EVIDENCE_ALIAS_NOTE = (
    "Evidence identifiers in this request are short request-local aliases "
    "such as e1 and e2. Copy only those exact aliases into "
    "evidence_span_ids. Local code expands them to the complete canonical "
    "evidence hashes before provenance or publication validation."
)


@dataclass(frozen=True)
class EvidenceAliases:
    full_to_short: dict[str, str]
    short_to_full: dict[str, str]


@dataclass(frozen=True)
class ExpandedProviderResponse:
    """Override parsed text while preserving every other SDK response field."""

    original: Any
    output_text: str
    output_parsed: None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self.original, name)


def build_evidence_aliases(
    contract_input: GPTUnifiedStoryInputV2,
) -> EvidenceAliases:
    full_ids = [
        span.evidence_span_id
        for article in contract_input.articles
        for span in article.evidence_spans
    ]
    if len(full_ids) != len(set(full_ids)):
        raise ValueError("canonical evidence span IDs must be unique")
    full_to_short = {
        full_id: f"e{index}"
        for index, full_id in enumerate(full_ids, start=1)
    }
    return EvidenceAliases(
        full_to_short=full_to_short,
        short_to_full={short: full for full, short in full_to_short.items()},
    )


def _replace_exact_values(value: Any, replacements: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        return replacements.get(value, value)
    if isinstance(value, list):
        return [_replace_exact_values(child, replacements) for child in value]
    if isinstance(value, Mapping):
        return {
            str(key): _replace_exact_values(child, replacements)
            for key, child in value.items()
        }
    return value


def alias_provider_request(
    request: StructuredResponseRequest,
    aliases: EvidenceAliases,
) -> StructuredResponseRequest:
    """Replace only exact evidence IDs in one provider-wire request."""
    if UNTRUSTED_SOURCE_DATA_BEGIN in request.input:
        prefix, remainder = request.input.split(
            f"{UNTRUSTED_SOURCE_DATA_BEGIN}\n", 1
        )
        payload_text, suffix = remainder.split(
            f"\n{UNTRUSTED_SOURCE_DATA_END}", 1
        )
        payload = json.loads(payload_text)
        wire_input = (
            prefix
            + UNTRUSTED_SOURCE_DATA_BEGIN
            + "\n"
            + json.dumps(
                _replace_exact_values(payload, aliases.full_to_short),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            + UNTRUSTED_SOURCE_DATA_END
            + suffix
        )
    else:
        prefix, payload_text = request.input.split("\n", 1)
        payload = json.loads(payload_text)
        wire_input = prefix + "\n" + json.dumps(
            _replace_exact_values(payload, aliases.full_to_short),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    return replace(
        request,
        instructions=request.instructions + "\n\n" + EVIDENCE_ALIAS_NOTE,
        input=wire_input,
    )


def expand_provider_result(
    result: AdapterResult,
    aliases: EvidenceAliases,
) -> tuple[AdapterResult, int]:
    """Expand response aliases before any production interpretation."""
    response = result.response
    output_text = (
        response.get("output_text")
        if isinstance(response, Mapping)
        else getattr(response, "output_text", None)
    )
    if not isinstance(output_text, str) or not output_text.strip():
        return result, 0
    try:
        payload = json.loads(output_text)
    except (TypeError, ValueError, json.JSONDecodeError):
        # Preserve the original provider response so the existing production
        # parser assigns the same local validation outcome as the legacy path.
        return result, 0
    expanded, expansion_count = _expand_evidence_id_fields(
        payload, aliases.short_to_full
    )
    if not expansion_count:
        return result, 0
    expanded_text = json.dumps(
        expanded,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if isinstance(response, Mapping):
        response = dict(response)
        response["output_text"] = expanded_text
        response["output_parsed"] = None
    else:
        # OpenAI SDK Response.output_text is a computed read-only property, so
        # model_copy(update={"output_text": ...}) does not change what callers
        # read. A transparent wrapper safely overrides only the parsed fields.
        response = ExpandedProviderResponse(response, expanded_text)
    return replace(result, response=response), expansion_count


def _expand_evidence_id_fields(
    value: Any,
    replacements: Mapping[str, str],
) -> tuple[Any, int]:
    if isinstance(value, list):
        expanded_items = []
        count = 0
        for child in value:
            expanded, child_count = _expand_evidence_id_fields(
                child, replacements
            )
            expanded_items.append(expanded)
            count += child_count
        return expanded_items, count
    if isinstance(value, Mapping):
        expanded_mapping: dict[str, Any] = {}
        count = 0
        for key, child in value.items():
            normalized_key = str(key)
            if normalized_key == "evidence_span_ids" and isinstance(child, list):
                expanded_ids = []
                for evidence_id in child:
                    if (
                        isinstance(evidence_id, str)
                        and evidence_id in replacements
                    ):
                        expanded_ids.append(replacements[evidence_id])
                        count += 1
                    else:
                        expanded_ids.append(evidence_id)
                expanded_mapping[normalized_key] = expanded_ids
                continue
            expanded, child_count = _expand_evidence_id_fields(
                child, replacements
            )
            expanded_mapping[normalized_key] = expanded
            count += child_count
        return expanded_mapping, count
    return value, 0
