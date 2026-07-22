import hashlib
import re
from itertools import combinations
from typing import Optional

from news_pipeline.unification.near_duplicates import (
    NearDuplicateThresholds,
    calculate_sentence_similarity,
    extract_number_facts,
    is_near_duplicate,
)
from news_pipeline.unification.sentences import TOKEN_PATTERN, normalize_sentence


NUMERIC_DATE_PATTERN = re.compile(
    r"\b(?:"
    r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})"
    r"|"
    r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})"
    r")\b"
)
MONTHS = {
    "ජනවාරි": 1,
    "පෙබරවාරි": 2,
    "මාර්තු": 3,
    "අප්‍රේල්": 4,
    "අප්රේල්": 4,
    "මැයි": 5,
    "ජූනි": 6,
    "ජුනි": 6,
    "ජූලි": 7,
    "ජුලි": 7,
    "අගෝස්තු": 8,
    "සැප්තැම්බර්": 9,
    "ඔක්තෝබර්": 10,
    "නොවැම්බර්": 11,
    "දෙසැම්බර්": 12,
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
NEGATION_MARKERS = frozenset(
    {
        "නැත",
        "නැහැ",
        "නැති",
        "නොවේ",
        "නොවන",
        "නොමැත",
        "නොමැති",
        "නොහැකි",
        "නොකළ",
        "නොකර",
        "නොගත්",
        "නොදෙන",
        "නොදුන්",
        "ප්‍රතික්ෂේප",
        "ප්රතික්ෂේප",
        "අසත්‍ය",
        "අසත්ය",
        "not",
        "denied",
        "denies",
        "false",
        "without",
    }
)


def detect_potential_conflicts(
    sentence_records: list[dict],
    thresholds: Optional[NearDuplicateThresholds] = None,
) -> list[dict]:
    thresholds = thresholds or NearDuplicateThresholds()
    _validate_sentence_records(sentence_records)
    flags = []
    prepared_records = [_prepare_record(record) for record in sentence_records]

    for left_prepared, right_prepared in combinations(prepared_records, 2):
        left = left_prepared["record"]
        right = right_prepared["record"]
        numbers_conflict = _fact_sets_conflict(
            left_prepared["numbers"],
            right_prepared["numbers"],
        )
        polarity_conflict = bool(left_prepared["polarity"]) != bool(
            right_prepared["polarity"]
        )
        if not numbers_conflict and not polarity_conflict:
            continue
        if not _could_be_related(left_prepared, right_prepared, thresholds):
            continue

        similarity = calculate_sentence_similarity(left["text"], right["text"])
        related_similarity = {**similarity, "numeric_conflict": False}
        if not is_near_duplicate(related_similarity, thresholds):
            continue

        left_dates = left_prepared["dates"]
        right_dates = right_prepared["dates"]
        dates_conflict = _fact_sets_conflict(
            left_dates,
            right_dates,
        )
        if similarity["numeric_conflict"]:
            conflict_type = "date_mismatch" if dates_conflict else "numeric_mismatch"
            flags.append(
                _build_flag(
                    conflict_type,
                    left,
                    right,
                    similarity,
                    {
                        "left_numbers": similarity["left_numbers"],
                        "right_numbers": similarity["right_numbers"],
                        "left_dates": sorted(left_dates),
                        "right_dates": sorted(right_dates),
                    },
                )
            )

        left_polarity = left_prepared["polarity"]
        right_polarity = right_prepared["polarity"]
        if polarity_conflict:
            flags.append(
                _build_flag(
                    "polarity_mismatch",
                    left,
                    right,
                    similarity,
                    {
                        "left_markers": sorted(left_polarity),
                        "right_markers": sorted(right_polarity),
                    },
                )
            )

    return flags


def extract_date_facts(text: str) -> set[str]:
    normalized = normalize_sentence(text).casefold()
    facts = set()
    for match in NUMERIC_DATE_PATTERN.finditer(normalized):
        if match.group(1):
            year, month, day = match.group(1), match.group(2), match.group(3)
        else:
            day, month, year = match.group(4), match.group(5), match.group(6)
        facts.add(_date_fact(year, month, day))

    tokens = [match.group(0) for match in TOKEN_PATTERN.finditer(normalized)]
    for index, token in enumerate(tokens):
        month = MONTHS.get(token)
        if month is None:
            continue
        nearby = tokens[max(0, index - 2) : index] + tokens[index + 1 : index + 3]
        numeric_tokens = [item for item in nearby if item.isdigit()]
        day = next(
            (item for item in numeric_tokens if 1 <= int(item) <= 31),
            None,
        )
        year = next(
            (item for item in numeric_tokens if len(item) == 4),
            None,
        )
        if day is not None:
            facts.add(_date_fact(year, str(month), day))
    return facts


def extract_polarity_markers(text: str) -> set[str]:
    normalized = normalize_sentence(text).casefold()
    tokens = {match.group(0) for match in TOKEN_PATTERN.finditer(normalized)}
    return {
        token
        for token in tokens
        if any(token == marker or token.startswith(marker) for marker in NEGATION_MARKERS)
    }


def _prepare_record(record: dict) -> dict:
    normalized = normalize_sentence(record["text"]).casefold()
    return {
        "record": record,
        "tokens": {
            match.group(0) for match in TOKEN_PATTERN.finditer(normalized)
        },
        "numbers": extract_number_facts(normalized),
        "dates": extract_date_facts(normalized),
        "polarity": extract_polarity_markers(normalized),
    }


def _fact_sets_conflict(left: set[str], right: set[str]) -> bool:
    return bool(
        left
        and right
        and not (left <= right or right <= left)
    )


def _could_be_related(
    left: dict,
    right: dict,
    thresholds: NearDuplicateThresholds,
) -> bool:
    left_tokens = left["tokens"]
    right_tokens = right["tokens"]
    minimum_size = min(len(left_tokens), len(right_tokens))
    if minimum_size < thresholds.min_tokens:
        return False
    shared_count = len(left_tokens & right_tokens)
    if shared_count < thresholds.min_shared_tokens:
        return False
    containment = shared_count / minimum_size
    union_size = len(left_tokens | right_tokens)
    jaccard = shared_count / union_size if union_size else 0.0
    return (
        containment >= thresholds.min_containment
        and jaccard >= thresholds.min_jaccard
    ) or (
        containment >= thresholds.sequence_min_containment
        and jaccard >= thresholds.sequence_min_jaccard
    )


def _build_flag(
    conflict_type: str,
    left: dict,
    right: dict,
    similarity: dict,
    facts: dict,
) -> dict:
    sentence_ids = sorted([left["sentence_id"], right["sentence_id"]])
    return {
        "conflict_id": _conflict_id(conflict_type, sentence_ids),
        "type": conflict_type,
        "status": "potential_review",
        "sentence_ids": sentence_ids,
        "left": _sentence_reference(left),
        "right": _sentence_reference(right),
        "facts": facts,
        "similarity": {
            "shared_token_count": similarity["shared_token_count"],
            "token_containment": similarity["token_containment"],
            "token_jaccard": similarity["token_jaccard"],
            "sequence_ratio": similarity["sequence_ratio"],
        },
    }


def _sentence_reference(record: dict) -> dict:
    return {
        "group_id": record.get("group_id"),
        "sentence_id": record["sentence_id"],
        "text": record["text"],
        "supporting_articles": record.get("supporting_articles", []),
    }


def _date_fact(year: Optional[str], month: str, day: str) -> str:
    year_value = year or "unknown"
    return f"{year_value}-{int(month):02d}-{int(day):02d}"


def _conflict_id(conflict_type: str, sentence_ids: list[str]) -> str:
    digest_input = f"{conflict_type}|{'|'.join(sentence_ids)}"
    digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()[:24]
    return f"conflict_{digest}"


def _validate_sentence_records(sentence_records: list[dict]):
    seen_sentence_ids = set()
    for record in sentence_records:
        sentence_id = record.get("sentence_id")
        if not sentence_id:
            raise ValueError("Every sentence record must include sentence_id")
        if sentence_id in seen_sentence_ids:
            raise ValueError(f"Duplicate sentence_id: {sentence_id}")
        seen_sentence_ids.add(sentence_id)
        if not normalize_sentence(record.get("text") or ""):
            raise ValueError(f"Sentence record {sentence_id} has no text")
