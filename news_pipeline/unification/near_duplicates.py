import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Optional

from news_pipeline.unification.sentences import TOKEN_PATTERN, normalize_sentence


NUMBER_PATTERN = re.compile(r"\d+(?:[.,:/-]\d+)*")
DATE_PATTERN = re.compile(r"^(\d{4})[./-](\d{1,2})[./-](\d{1,2})$")
THOUSANDS_PATTERN = re.compile(r"^\d{1,3}(?:,\d{3})+(?:\.\d+)?$")


@dataclass(frozen=True)
class NearDuplicateThresholds:
    min_tokens: int = 5
    min_shared_tokens: int = 5
    min_containment: float = 0.80
    min_jaccard: float = 0.55
    min_sequence_ratio: float = 0.82
    sequence_min_containment: float = 0.65
    sequence_min_jaccard: float = 0.45

    def __post_init__(self):
        if self.min_tokens < 1:
            raise ValueError("min_tokens must be at least 1")
        if self.min_shared_tokens < 1:
            raise ValueError("min_shared_tokens must be at least 1")
        for name in (
            "min_containment",
            "min_jaccard",
            "min_sequence_ratio",
            "sequence_min_containment",
            "sequence_min_jaccard",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")


def calculate_sentence_similarity(left: str, right: str) -> dict:
    left_normalized = normalize_sentence(left).casefold()
    right_normalized = normalize_sentence(right).casefold()
    left_tokens = _token_set(left_normalized)
    right_tokens = _token_set(right_normalized)
    shared_tokens = left_tokens & right_tokens
    union_tokens = left_tokens | right_tokens
    minimum_token_count = min(len(left_tokens), len(right_tokens))
    containment = (
        len(shared_tokens) / minimum_token_count if minimum_token_count else 0.0
    )
    jaccard = len(shared_tokens) / len(union_tokens) if union_tokens else 0.0
    sequence_ratio = SequenceMatcher(
        None,
        left_normalized,
        right_normalized,
        autojunk=False,
    ).ratio()
    left_numbers = extract_number_facts(left_normalized)
    right_numbers = extract_number_facts(right_normalized)
    numeric_conflict = bool(
        left_numbers
        and right_numbers
        and not (
            left_numbers <= right_numbers
            or right_numbers <= left_numbers
        )
    )

    return {
        "left_token_count": len(left_tokens),
        "right_token_count": len(right_tokens),
        "shared_token_count": len(shared_tokens),
        "token_containment": round(containment, 6),
        "token_jaccard": round(jaccard, 6),
        "sequence_ratio": round(sequence_ratio, 6),
        "left_numbers": sorted(left_numbers),
        "right_numbers": sorted(right_numbers),
        "numeric_conflict": numeric_conflict,
    }


def is_near_duplicate(
    similarity: dict,
    thresholds: Optional[NearDuplicateThresholds] = None,
) -> bool:
    thresholds = thresholds or NearDuplicateThresholds()
    if similarity["numeric_conflict"]:
        return False
    if min(
        similarity["left_token_count"],
        similarity["right_token_count"],
    ) < thresholds.min_tokens:
        return False
    if similarity["shared_token_count"] < thresholds.min_shared_tokens:
        return False

    lexical_match = (
        similarity["token_containment"] >= thresholds.min_containment
        and similarity["token_jaccard"] >= thresholds.min_jaccard
    )
    sequence_match = (
        similarity["sequence_ratio"] >= thresholds.min_sequence_ratio
        and similarity["token_containment"]
        >= thresholds.sequence_min_containment
        and similarity["token_jaccard"] >= thresholds.sequence_min_jaccard
    )
    return lexical_match or sequence_match


def group_near_duplicate_evidence(
    evidence_records: list[dict],
    thresholds: Optional[NearDuplicateThresholds] = None,
) -> list[dict]:
    thresholds = thresholds or NearDuplicateThresholds()
    _validate_evidence(evidence_records)
    groups = []

    for evidence in evidence_records:
        compatible_groups = []
        for group_index, group in enumerate(groups):
            similarities = [
                calculate_sentence_similarity(evidence["text"], member["text"])
                for member in group
            ]
            if all(
                is_near_duplicate(similarity, thresholds)
                for similarity in similarities
            ):
                compatible_groups.append(
                    (
                        min(_similarity_strength(item) for item in similarities),
                        group_index,
                    )
                )

        if compatible_groups:
            _, best_group_index = max(
                compatible_groups,
                key=lambda item: (item[0], -item[1]),
            )
            groups[best_group_index].append(evidence)
        else:
            groups.append([evidence])

    return [_build_group_record(group) for group in groups]


def _build_group_record(evidence_group: list[dict]) -> dict:
    anchor = evidence_group[0]
    article_ids = {
        support["article_id"]
        for evidence in evidence_group
        for support in evidence.get("supporting_articles", [])
    }
    variants = []
    for evidence in evidence_group:
        similarity = calculate_sentence_similarity(
            anchor["text"],
            evidence["text"],
        )
        variants.append(
            {
                **evidence,
                "similarity_to_anchor": similarity,
            }
        )

    sentence_ids = sorted(evidence["sentence_id"] for evidence in evidence_group)
    return {
        "group_id": _group_id(sentence_ids),
        "anchor_sentence_id": anchor["sentence_id"],
        "variant_count": len(variants),
        "support_count": len(article_ids),
        "occurrence_count": sum(
            evidence.get("occurrence_count", 0) for evidence in evidence_group
        ),
        "is_near_duplicate_group": len(variants) > 1,
        "variants": variants,
    }


def _token_set(text: str) -> set[str]:
    return {match.group(0) for match in TOKEN_PATTERN.finditer(text)}


def extract_number_facts(text: str) -> set[str]:
    facts = set()
    for raw_value in NUMBER_PATTERN.findall(text):
        date_match = DATE_PATTERN.fullmatch(raw_value)
        if date_match:
            facts.update(_normalize_integer(part) for part in date_match.groups())
            continue
        if THOUSANDS_PATTERN.fullmatch(raw_value):
            facts.add(_normalize_decimal(raw_value.replace(",", "")))
            continue
        if ":" in raw_value:
            facts.add(
                ":".join(
                    _normalize_integer(part) for part in raw_value.split(":")
                )
            )
            continue
        if "/" in raw_value or "-" in raw_value:
            facts.update(
                _normalize_integer(part)
                for part in re.split(r"[/-]", raw_value)
            )
            continue
        facts.add(_normalize_decimal(raw_value.replace(",", "")))
    return facts


def _normalize_decimal(value: str) -> str:
    try:
        normalized = Decimal(value).normalize()
    except InvalidOperation:
        return value
    return format(normalized, "f")


def _normalize_integer(value: str) -> str:
    try:
        return str(int(value))
    except ValueError:
        return value


def _similarity_strength(similarity: dict) -> float:
    lexical_strength = (
        similarity["token_containment"] + similarity["token_jaccard"]
    ) / 2
    return max(lexical_strength, similarity["sequence_ratio"])


def _group_id(sentence_ids: list[str]) -> str:
    digest_input = "|".join(sentence_ids)
    digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()[:24]
    return f"sentence_group_{digest}"


def _validate_evidence(evidence_records: list[dict]):
    seen_sentence_ids = set()
    for evidence in evidence_records:
        sentence_id = evidence.get("sentence_id")
        if not sentence_id:
            raise ValueError("Every evidence record must include sentence_id")
        if sentence_id in seen_sentence_ids:
            raise ValueError(f"Duplicate sentence_id: {sentence_id}")
        seen_sentence_ids.add(sentence_id)
        if not normalize_sentence(evidence.get("text") or ""):
            raise ValueError(f"Evidence record {sentence_id} has no text")
