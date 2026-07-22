from dataclasses import asdict, dataclass
from typing import Optional

from news_pipeline.unification.conflicts import detect_potential_conflicts
from news_pipeline.unification.near_duplicates import (
    calculate_sentence_similarity,
)
from news_pipeline.unification.sentences import (
    CLOSING_PUNCTUATION,
    HARD_TERMINATORS,
    SINHALA_END,
    SINHALA_START,
    TOKEN_PATTERN,
    normalize_sentence,
)
from news_pipeline.unification.titles import invalid_title_reasons


SPEAKER_INTRODUCTION_ENDINGS = (
    "මෙසේ ප්‍රකාශ කළහ",
    "මෙසේ ප්රකාශ කළහ",
    "මෙසේ ප්‍රකාශ කළේය",
    "මෙසේ ප්රකාශ කළේය",
    "මෙසේ ප්‍රකාශ කළාය",
    "මෙසේ ප්රකාශ කළාය",
    "මෙසේ පැවසීය",
    "මෙසේ පැවසූහ",
    "මෙසේ සඳහන් කළේය",
    "මෙසේ සඳහන් කළාය",
    "මෙසේ අදහස් දැක්වීය",
    "මෙසේ අදහස් දැක්වූහ",
)
CONTEXT_DEPENDENT_PREFIXES = (
    "ඒ අතරින් ",
    "ඉදිරියේදී එම ",
)
DOCUMENT_REFERENCE_PREFIXES = (
    "රාජ්ය පරිපාලන චක්රලේඛ අංක ",
    "රාජ්‍ය පරිපාලන චක්‍රලේඛ අංක ",
)
COMPETING_EVENT_ENTITY_GROUPS = (
    ("ලිට්රෝ", "ලාෆ්ස්"),
)


@dataclass(frozen=True)
class ExtractiveSelectionConfig:
    max_sentences: int = 5
    max_chars: int = 1200
    min_sentence_chars: int = 35
    max_sentence_chars: int = 450
    max_redundancy: float = 0.70
    redundancy_penalty: float = 0.25
    min_selection_score: float = 0.30
    anchor_representative_lead: bool = True

    def __post_init__(self):
        if self.max_sentences < 1:
            raise ValueError("max_sentences must be at least 1")
        if self.max_chars < 1:
            raise ValueError("max_chars must be at least 1")
        if self.min_sentence_chars < 1:
            raise ValueError("min_sentence_chars must be at least 1")
        if self.max_sentence_chars < self.min_sentence_chars:
            raise ValueError(
                "max_sentence_chars cannot be below min_sentence_chars"
            )
        if not 0.0 <= self.max_redundancy <= 1.0:
            raise ValueError("max_redundancy must be between 0 and 1")
        if not 0.0 <= self.redundancy_penalty <= 1.0:
            raise ValueError("redundancy_penalty must be between 0 and 1")
        if not 0.0 <= self.min_selection_score <= 1.0:
            raise ValueError("min_selection_score must be between 0 and 1")


def select_extractive_story(
    evidence_groups: list[dict],
    config: Optional[ExtractiveSelectionConfig] = None,
) -> dict:
    config = config or ExtractiveSelectionConfig()
    _validate_groups(evidence_groups)
    candidates = []
    for group_index, group in enumerate(evidence_groups):
        candidate = _build_candidate(group, group_index, config)
        if candidate is not None:
            candidates.append(candidate)

    selected = []
    remaining = candidates.copy()
    current_chars = 0
    lead_anchor = None
    if config.anchor_representative_lead:
        lead_anchor = _pop_representative_lead(remaining, config)
    if lead_anchor is not None:
        lead_anchor["selection_score"] = lead_anchor["base_score"]
        lead_anchor["redundancy_to_selected"] = 0.0
        lead_anchor["selection_reason"] = "representative_lead"
        selected.append(lead_anchor)
        current_chars = len(lead_anchor["variant"]["text"])

    while remaining and len(selected) < config.max_sentences:
        scored_candidates = []
        for candidate_index, candidate in enumerate(remaining):
            separator_chars = 1 if selected else 0
            if (
                current_chars
                + separator_chars
                + len(candidate["variant"]["text"])
                > config.max_chars
            ):
                continue
            redundancy = _maximum_redundancy(candidate, selected)
            if redundancy >= config.max_redundancy:
                continue
            selection_score = (
                candidate["base_score"]
                - config.redundancy_penalty * redundancy
            )
            if selection_score < config.min_selection_score:
                continue
            scored_candidates.append(
                (
                    selection_score,
                    candidate["base_score"],
                    -candidate["group_index"],
                    candidate_index,
                    redundancy,
                )
            )

        if not scored_candidates:
            break
        (
            selection_score,
            _,
            _,
            best_candidate_index,
            redundancy,
        ) = max(scored_candidates)
        best = remaining.pop(best_candidate_index)
        best["selection_score"] = round(selection_score, 6)
        best["redundancy_to_selected"] = round(redundancy, 6)
        best["selection_reason"] = "mmr"
        selected.append(best)
        current_chars += (1 if current_chars else 0) + len(best["variant"]["text"])

    selected, suppressed_speaker_introductions = (
        _suppress_orphan_speaker_introductions(selected)
    )
    selected, suppressed_context_sentences = (
        _suppress_orphan_context_sentences(selected)
    )
    selected, suppressed_heading_fragments = _suppress_heading_fragments(
        selected
    )
    selected, suppressed_relevance_sentences = _suppress_relevance_sentences(
        selected
    )
    selected, suppressed_residual_repetitions = (
        _suppress_residual_repetitions(selected)
    )
    selected_records = [
        _selection_record(candidate, rank)
        for rank, candidate in enumerate(selected, 1)
    ]
    unified_sentences = sorted(selected_records, key=_narrative_order_key)
    for output_position, record in enumerate(unified_sentences, 1):
        record["output_position"] = output_position
    conflict_candidates = [
        {
            "group_id": candidate["group"]["group_id"],
            "sentence_id": candidate["variant"]["sentence_id"],
            "text": candidate["variant"]["text"],
            "supporting_articles": candidate["variant"].get(
                "supporting_articles", []
            ),
        }
        for candidate in candidates
    ]
    conflict_flags = detect_potential_conflicts(conflict_candidates)
    selected_sentence_ids = {
        record["sentence_id"] for record in unified_sentences
    }
    conflict_ids_by_sentence = {
        sentence_id: [] for sentence_id in selected_sentence_ids
    }
    for flag in conflict_flags:
        for side in ("left", "right"):
            sentence_id = flag[side]["sentence_id"]
            flag[side]["selected_for_unified_text"] = (
                sentence_id in selected_sentence_ids
            )
            if sentence_id in conflict_ids_by_sentence:
                conflict_ids_by_sentence[sentence_id].append(flag["conflict_id"])
    for record in unified_sentences:
        record["conflict_flag_ids"] = conflict_ids_by_sentence[
            record["sentence_id"]
        ]
    unified_text = "\n".join(record["text"] for record in unified_sentences)
    return {
        "selection_method": "extractive_lead_mmr_v6",
        "selection_config": asdict(config),
        "candidate_group_count": len(evidence_groups),
        "eligible_group_count": len(candidates),
        "selected_sentence_count": len(unified_sentences),
        "character_count": len(unified_text),
        "lead_anchor_used": lead_anchor is not None,
        "lead_anchor_sentence_id": (
            lead_anchor["variant"]["sentence_id"]
            if lead_anchor is not None
            else None
        ),
        "suppressed_orphan_speaker_introduction_count": len(
            suppressed_speaker_introductions
        ),
        "suppressed_orphan_speaker_introductions": [
            _suppressed_speaker_introduction_record(candidate)
            for candidate in suppressed_speaker_introductions
        ],
        "suppressed_orphan_context_sentence_count": len(
            suppressed_context_sentences
        ),
        "suppressed_orphan_context_sentences": [
            _suppressed_candidate_record(
                candidate,
                "missing_immediately_preceding_source_sentence",
            )
            for candidate in suppressed_context_sentences
        ],
        "suppressed_heading_fragment_count": len(
            suppressed_heading_fragments
        ),
        "suppressed_heading_fragments": [
            _suppressed_candidate_record(candidate, reason)
            for candidate, reason in suppressed_heading_fragments
        ],
        "suppressed_relevance_sentence_count": len(
            suppressed_relevance_sentences
        ),
        "suppressed_relevance_sentences": [
            _suppressed_relevance_record(candidate, reason, details)
            for candidate, reason, details in suppressed_relevance_sentences
        ],
        "suppressed_residual_repetition_count": len(
            suppressed_residual_repetitions
        ),
        "suppressed_residual_repetitions": [
            _suppressed_repetition_record(
                candidate,
                reason,
                earlier_candidate,
                similarity,
            )
            for (
                candidate,
                reason,
                earlier_candidate,
                similarity,
            ) in suppressed_residual_repetitions
        ],
        "has_conflict_flags": bool(conflict_flags),
        "conflict_flag_count": len(conflict_flags),
        "conflict_detection_scope": [
            "numeric_mismatch",
            "date_mismatch",
            "polarity_mismatch",
        ],
        "conflict_flags": conflict_flags,
        "unified_text": unified_text,
        "unified_sentences": unified_sentences,
    }


def _pop_representative_lead(
    candidates: list[dict],
    config: ExtractiveSelectionConfig,
) -> Optional[dict]:
    compatible = []
    for candidate_index, candidate in enumerate(candidates):
        text = candidate["variant"]["text"]
        if len(text) > config.max_chars:
            continue
        if _is_speaker_introduction(text):
            continue
        positions = [
            sentence_index
            for support in candidate["variant"].get(
                "supporting_articles", []
            )
            if support.get("is_representative")
            for sentence_index in support.get("sentence_indexes", [])
        ]
        if not positions:
            continue
        compatible.append(
            (
                min(positions),
                -candidate["base_score"],
                candidate["group_index"],
                candidate_index,
            )
        )

    if not compatible:
        return None
    *_, candidate_index = min(compatible)
    return candidates.pop(candidate_index)


def _suppress_orphan_speaker_introductions(
    selected: list[dict],
) -> tuple[list[dict], list[dict]]:
    kept = []
    suppressed = []
    for candidate in selected:
        if not _is_speaker_introduction(candidate["variant"]["text"]):
            kept.append(candidate)
            continue
        support_positions = {
            (support.get("article_id"), sentence_index)
            for support in candidate["variant"].get(
                "supporting_articles", []
            )
            for sentence_index in support.get("sentence_indexes", [])
        }
        other_selected_positions = {
            (support.get("article_id"), sentence_index)
            for other_candidate in selected
            if other_candidate is not candidate
            for support in other_candidate["variant"].get(
                "supporting_articles", []
            )
            for sentence_index in support.get("sentence_indexes", [])
        }
        has_following_sentence = any(
            (article_id, sentence_index + 1) in other_selected_positions
            for article_id, sentence_index in support_positions
        )
        if has_following_sentence:
            kept.append(candidate)
        else:
            suppressed.append(candidate)
    return kept, suppressed


def _is_speaker_introduction(text: str) -> bool:
    stripped = normalize_sentence(text).rstrip()
    while stripped and (
        stripped[-1] == "."
        or stripped[-1] in HARD_TERMINATORS
        or stripped[-1] in CLOSING_PUNCTUATION
    ):
        stripped = stripped[:-1].rstrip()
    return stripped.endswith(SPEAKER_INTRODUCTION_ENDINGS)


def _suppress_orphan_context_sentences(
    selected: list[dict],
) -> tuple[list[dict], list[dict]]:
    kept = []
    suppressed = []
    for candidate in selected:
        text = normalize_sentence(candidate["variant"]["text"])
        if not text.startswith(CONTEXT_DEPENDENT_PREFIXES):
            kept.append(candidate)
            continue
        support_positions = _candidate_support_positions(candidate)
        other_selected_positions = {
            position
            for other_candidate in selected
            if other_candidate is not candidate
            for position in _candidate_support_positions(other_candidate)
        }
        has_preceding_sentence = any(
            (article_id, sentence_index - 1) in other_selected_positions
            for article_id, sentence_index in support_positions
        )
        if has_preceding_sentence:
            kept.append(candidate)
        else:
            suppressed.append(candidate)
    return kept, suppressed


def _suppress_heading_fragments(
    selected: list[dict],
) -> tuple[list[dict], list[tuple[dict, str]]]:
    kept = []
    suppressed = []
    for candidate in selected:
        reason = _heading_fragment_reason(candidate)
        if reason is None:
            kept.append(candidate)
        else:
            suppressed.append((candidate, reason))
    return kept, suppressed


def _heading_fragment_reason(candidate: dict) -> Optional[str]:
    text = normalize_sentence(candidate["variant"]["text"])
    if "_" in text and len(text) <= 120:
        return "malformed_heading_fragment"

    text_key = _title_key(text)
    if len(text_key) < 25:
        return None
    for support in candidate["variant"].get("supporting_articles", []):
        title_key = _title_key(support.get("title") or "")
        if title_key != text_key and title_key.startswith(text_key):
            return "embedded_source_title_prefix"
    return None


def _suppress_relevance_sentences(
    selected: list[dict],
) -> tuple[list[dict], list[tuple[dict, str, dict]]]:
    kept = []
    suppressed = []
    for candidate in selected:
        match = _relevance_suppression_match(candidate, selected)
        if match is None:
            kept.append(candidate)
        else:
            reason, details = match
            suppressed.append((candidate, reason, details))
    return kept, suppressed


def _relevance_suppression_match(
    candidate: dict,
    selected: list[dict],
) -> Optional[tuple[str, dict]]:
    document_match = _orphan_document_reference_match(candidate, selected)
    if document_match is not None:
        return document_match

    low_confidence_match = _low_confidence_unanchored_match(candidate)
    if low_confidence_match is not None:
        return low_confidence_match

    return _competing_entity_match(candidate, selected)


def _orphan_document_reference_match(
    candidate: dict,
    selected: list[dict],
) -> Optional[tuple[str, dict]]:
    text = normalize_sentence(candidate["variant"]["text"])
    if not text.startswith(DOCUMENT_REFERENCE_PREFIXES):
        return None

    support_positions = _candidate_support_positions(candidate)
    other_selected_positions = {
        position
        for other_candidate in selected
        if other_candidate is not candidate
        for position in _candidate_support_positions(other_candidate)
    }
    has_preceding_sentence = any(
        (article_id, sentence_index - 1) in other_selected_positions
        for article_id, sentence_index in support_positions
    )
    if has_preceding_sentence:
        return None
    return (
        "missing_immediately_preceding_document_context",
        {
            "support_positions": sorted(support_positions),
            "required_preceding_positions": sorted(
                (article_id, sentence_index - 1)
                for article_id, sentence_index in support_positions
            ),
        },
    )


def _low_confidence_unanchored_match(
    candidate: dict,
) -> Optional[tuple[str, dict]]:
    if candidate.get("selection_reason") == "representative_lead":
        return None
    if candidate["base_score"] >= 0.45:
        return None
    if candidate.get("redundancy_to_selected", 0.0) >= 0.15:
        return None
    if candidate["group"].get("support_count", 0) != 1:
        return None
    if not any(
        SINHALA_START <= character <= SINHALA_END
        for character in candidate["variant"]["text"]
    ):
        return None

    support = candidate["variant"].get("supporting_articles", [])
    if not support or any(item.get("is_representative") for item in support):
        return None
    title_checks = [
        {
            "article_id": item.get("article_id"),
            "title": item.get("title") or "",
            "invalid_title_reasons": invalid_title_reasons(
                item.get("title")
            ),
        }
        for item in support
    ]
    if not all(item["invalid_title_reasons"] for item in title_checks):
        return None
    return (
        "low_confidence_unanchored_invalid_title_source",
        {
            "base_score": candidate["base_score"],
            "redundancy_to_selected": candidate.get(
                "redundancy_to_selected", 0.0
            ),
            "source_title_checks": title_checks,
        },
    )


def _competing_entity_match(
    candidate: dict,
    selected: list[dict],
) -> Optional[tuple[str, dict]]:
    representative_leads = [
        item
        for item in selected
        if item.get("selection_reason") == "representative_lead"
    ]
    if candidate in representative_leads:
        return None
    anchor_text = " ".join(
        normalize_sentence(item["variant"]["text"]).casefold()
        for item in representative_leads
    )
    candidate_text = normalize_sentence(
        candidate["variant"]["text"]
    ).casefold()
    for entity_group in COMPETING_EVENT_ENTITY_GROUPS:
        anchor_entities = [
            entity for entity in entity_group if entity in anchor_text
        ]
        candidate_entities = [
            entity for entity in entity_group if entity in candidate_text
        ]
        if (
            len(anchor_entities) == 1
            and candidate_entities
            and anchor_entities[0] not in candidate_entities
        ):
            return (
                "competing_entity_event_switch",
                {
                    "anchor_entity": anchor_entities[0],
                    "candidate_entities": candidate_entities,
                },
            )
    return None


def _suppress_residual_repetitions(
    selected: list[dict],
) -> tuple[list[dict], list[tuple[dict, str, dict, dict]]]:
    kept = []
    suppressed = []
    for candidate in selected:
        match = _residual_repetition_match(candidate, kept)
        if match is None:
            kept.append(candidate)
        else:
            reason, earlier_candidate, similarity = match
            suppressed.append(
                (candidate, reason, earlier_candidate, similarity)
            )
    return kept, suppressed


def _residual_repetition_match(
    candidate: dict,
    earlier_candidates: list[dict],
) -> Optional[tuple[str, dict, dict]]:
    for earlier_candidate in earlier_candidates:
        similarity = calculate_sentence_similarity(
            earlier_candidate["variant"]["text"],
            candidate["variant"]["text"],
        )
        same_number_facts = (
            similarity["left_numbers"] == similarity["right_numbers"]
        )
        low_confidence_paraphrase = (
            candidate["base_score"] < 0.45
            and similarity["shared_token_count"] >= 7
            and similarity["sequence_ratio"] >= 0.50
            and same_number_facts
        )
        if low_confidence_paraphrase:
            return (
                "low_confidence_paraphrase",
                earlier_candidate,
                similarity,
            )

        repeated_numeric_claim = (
            bool(similarity["left_numbers"])
            and same_number_facts
            and similarity["shared_token_count"] >= 10
            and similarity["sequence_ratio"] >= 0.55
        )
        if repeated_numeric_claim:
            return (
                "repeated_numeric_claim",
                earlier_candidate,
                similarity,
            )
    return None


def _candidate_support_positions(candidate: dict) -> set[tuple]:
    return {
        (support.get("article_id"), sentence_index)
        for support in candidate["variant"].get("supporting_articles", [])
        for sentence_index in support.get("sentence_indexes", [])
    }


def _suppressed_speaker_introduction_record(candidate: dict) -> dict:
    return _suppressed_candidate_record(
        candidate,
        "missing_immediately_following_source_sentence",
    )


def _suppressed_candidate_record(candidate: dict, reason: str) -> dict:
    variant = candidate["variant"]
    return {
        "group_id": candidate["group"]["group_id"],
        "sentence_id": variant["sentence_id"],
        "text": variant["text"],
        "suppression_reason": reason,
        "selection_reason": candidate.get("selection_reason", "mmr"),
        "selection_score": candidate["selection_score"],
        "supporting_articles": variant.get("supporting_articles", []),
    }


def _suppressed_repetition_record(
    candidate: dict,
    reason: str,
    earlier_candidate: dict,
    similarity: dict,
) -> dict:
    record = _suppressed_candidate_record(candidate, reason)
    record.update(
        {
            "repeats_sentence_id": earlier_candidate["variant"][
                "sentence_id"
            ],
            "repeats_text": earlier_candidate["variant"]["text"],
            "similarity": similarity,
        }
    )
    return record


def _suppressed_relevance_record(
    candidate: dict,
    reason: str,
    details: dict,
) -> dict:
    record = _suppressed_candidate_record(candidate, reason)
    record["relevance_evidence"] = details
    return record


def _build_candidate(
    group: dict,
    group_index: int,
    config: ExtractiveSelectionConfig,
) -> Optional[dict]:
    eligible_variants = [
        variant
        for variant in group["variants"]
        if _is_eligible_variant(variant, config)
    ]
    if not eligible_variants:
        return None

    variant = max(eligible_variants, key=_variant_priority)
    all_support = _unique_group_support(group)
    source_count = len(
        {
            support.get("source")
            for support in all_support
            if support.get("source")
        }
    )
    has_representative_support = any(
        support.get("is_representative") for support in all_support
    )
    representative_support = [
        support for support in all_support if support.get("is_representative")
    ]
    position_support = representative_support or all_support
    earliest_position = min(
        (
            index
            for support in position_support
            for index in support.get("sentence_indexes", [])
        ),
        default=999,
    )
    support_score = min(source_count, 3) / 3
    representative_score = 1.0 if has_representative_support else 0.0
    position_score = 1 / (1 + 0.2 * earliest_position)
    length_score = _length_score(len(variant["text"]), config)
    base_score = (
        0.35 * support_score
        + 0.30 * representative_score
        + 0.20 * position_score
        + 0.15 * length_score
    )
    return {
        "group": group,
        "group_index": group_index,
        "variant": variant,
        "tokens": _token_set(variant["text"]),
        "character_ngrams": _character_ngrams(variant["text"]),
        "base_score": round(base_score, 6),
    }


def _is_eligible_variant(
    variant: dict,
    config: ExtractiveSelectionConfig,
) -> bool:
    text = normalize_sentence(variant.get("text") or "")
    if not config.min_sentence_chars <= len(text) <= config.max_sentence_chars:
        return False
    if not _has_terminal_punctuation(text):
        return False
    return not _matches_supporting_title(text, variant)


def _variant_priority(variant: dict) -> tuple:
    support = variant.get("supporting_articles", [])
    representative_support = [item for item in support if item.get("is_representative")]
    position_support = representative_support or support
    earliest_position = min(
        (
            index
            for item in position_support
            for index in item.get("sentence_indexes", [])
        ),
        default=999,
    )
    source_count = len(
        {item.get("source") for item in support if item.get("source")}
    )
    return (
        bool(representative_support),
        source_count,
        variant.get("support_count", 0),
        -earliest_position,
        -len(variant.get("text") or ""),
        variant.get("sentence_id") or "",
    )


def _length_score(length: int, config: ExtractiveSelectionConfig) -> float:
    if length <= 120:
        return min(1.0, length / 120)
    if length <= 260:
        return 1.0
    available_drop = max(1, config.max_sentence_chars - 260)
    return max(0.4, 1.0 - 0.6 * ((length - 260) / available_drop))


def _maximum_redundancy(candidate: dict, selected: list[dict]) -> float:
    if not selected:
        return 0.0
    selected_tokens = set().union(*(item["tokens"] for item in selected))
    token_coverage = (
        len(candidate["tokens"] & selected_tokens) / len(candidate["tokens"])
        if candidate["tokens"]
        else 0.0
    )
    selected_ngrams = set().union(
        *(item["character_ngrams"] for item in selected)
    )
    character_coverage = (
        len(candidate["character_ngrams"] & selected_ngrams)
        / len(candidate["character_ngrams"])
        if candidate["character_ngrams"]
        else 0.0
    )
    return max(token_coverage, character_coverage)


def _token_set(text: str) -> set[str]:
    normalized = normalize_sentence(text).casefold()
    return {match.group(0) for match in TOKEN_PATTERN.finditer(normalized)}


def _character_ngrams(text: str, size: int = 3) -> set[str]:
    normalized = normalize_sentence(text).casefold()
    content = "".join(
        char
        for char in normalized
        if char.isalnum() or "\u0D80" <= char <= "\u0DFF"
    )
    return {
        content[index : index + size]
        for index in range(max(0, len(content) - size + 1))
    }


def _has_terminal_punctuation(text: str) -> bool:
    stripped = text.rstrip()
    while stripped and stripped[-1] in CLOSING_PUNCTUATION:
        stripped = stripped[:-1].rstrip()
    return bool(stripped) and (
        stripped[-1] == "." or stripped[-1] in HARD_TERMINATORS
    )


def _matches_supporting_title(text: str, variant: dict) -> bool:
    text_key = _title_key(text)
    return any(
        title and _title_key(title) == text_key
        for title in (
            support.get("title")
            for support in variant.get("supporting_articles", [])
        )
    )


def _title_key(text: str) -> str:
    normalized = normalize_sentence(text).casefold()
    return normalized.rstrip(' .!?।෴"\'”’»)]}')


def _unique_group_support(group: dict) -> list[dict]:
    support_by_article = {}
    for variant in group["variants"]:
        for support in variant.get("supporting_articles", []):
            support_by_article.setdefault(support["article_id"], support)
    return list(support_by_article.values())


def _selection_record(candidate: dict, rank: int) -> dict:
    group = candidate["group"]
    variant = candidate["variant"]
    return {
        "selection_rank": rank,
        "group_id": group["group_id"],
        "sentence_id": variant["sentence_id"],
        "text": variant["text"],
        "selection_reason": candidate.get("selection_reason", "mmr"),
        "selection_score": candidate["selection_score"],
        "base_score": candidate["base_score"],
        "redundancy_to_selected": candidate["redundancy_to_selected"],
        "exact_support_count": variant.get("support_count", 0),
        "group_support_count": group.get("support_count", 0),
        "group_variant_count": group.get("variant_count", 0),
        "supporting_articles": variant.get("supporting_articles", []),
        "near_duplicate_variants": [
            {
                "sentence_id": item["sentence_id"],
                "text": item["text"],
                "supporting_articles": item.get("supporting_articles", []),
            }
            for item in group["variants"]
            if item["sentence_id"] != variant["sentence_id"]
        ],
    }


def _narrative_order_key(record: dict) -> tuple:
    support = record.get("supporting_articles", [])
    representative_support = [item for item in support if item.get("is_representative")]
    position_support = representative_support or support
    earliest_position = min(
        (
            index
            for item in position_support
            for index in item.get("sentence_indexes", [])
        ),
        default=999,
    )
    earliest_date = min(
        (
            item.get("published_date")
            for item in position_support
            if item.get("published_date")
        ),
        default="",
    )
    return (
        0 if representative_support else 1,
        earliest_position,
        earliest_date,
        record["selection_rank"],
    )


def _validate_groups(evidence_groups: list[dict]):
    seen_group_ids = set()
    for group in evidence_groups:
        group_id = group.get("group_id")
        if not group_id:
            raise ValueError("Every evidence group must include group_id")
        if group_id in seen_group_ids:
            raise ValueError(f"Duplicate group_id: {group_id}")
        seen_group_ids.add(group_id)
        if not group.get("variants"):
            raise ValueError(f"Evidence group {group_id} has no variants")
