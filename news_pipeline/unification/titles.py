import re
from typing import Optional

from news_pipeline.unification.sentences import (
    CLOSING_PUNCTUATION,
    HARD_TERMINATORS,
    SINHALA_END,
    SINHALA_START,
    TOKEN_PATTERN,
    extract_usable_sentences,
    normalize_sentence,
)


FILENAME_TITLE_PATTERN = re.compile(
    r"(?i)^(?:news|article|index|default)(?:\.[a-z0-9]{1,8})?(?:\?.*)?$"
)
GENERIC_FILE_PATTERN = re.compile(
    r"(?i)^\S+\.(?:php|html?|aspx?|jsp)(?:\?.*)?$"
)
OPAQUE_IDENTIFIER_PATTERN = re.compile(r"(?i)^[a-z][a-z0-9_-]{7,31}$")
URL_TITLE_PATTERN = re.compile(r"(?i)^(?:https?://|www\.)")
CONTENT_HEADING_PREFIX = re.compile(r"(?i)^reading\s*:\s*")
CONTEXTUAL_SENTENCE_PREFIXES = (
    "ඒ ",
    "එම ",
    "මෙම ",
    "අදාළ ",
    "ඔහු ",
    "ඇය ",
    "මෙලෙස ",
    "මේ ",
)
MAX_DISPLAY_TITLE_CHARS = 180
MIN_CONCISE_TITLE_CHARS = 25
MIN_TITLE_RELEVANCE = 0.25


def build_display_title(
    source_title: Optional[str],
    unified_sentences: list[dict],
    representative_clean_text: Optional[str] = None,
    source_title_source: Optional[str] = None,
    source_sentence_candidates: Optional[list[dict]] = None,
) -> dict:
    reasons = invalid_title_reasons(
        source_title,
        reference_text=representative_clean_text,
    )
    if not reasons:
        return {
            "text": normalize_sentence(source_title or ""),
            "method": "representative_source_title",
            "fallback_used": False,
            "source_title": source_title,
            "source_title_source": source_title_source,
            "invalid_source_title_reasons": [],
            "sentence_id": None,
            "supporting_articles": [],
            "source_text": None,
            "transformations": [],
        }

    ordered_sentences = sorted(
        unified_sentences,
        key=lambda sentence: (
            sentence.get("output_position", 999),
            sentence.get("selection_rank", 999),
        ),
    )
    if ordered_sentences:
        lead = ordered_sentences[0]
        concise_candidate = None
        if len(lead["text"]) > MAX_DISPLAY_TITLE_CHARS:
            concise_candidate = _select_concise_source_title(
                lead["text"],
                source_sentence_candidates or [],
            )
        if concise_candidate is not None:
            return {
                "text": concise_candidate["text"],
                "method": concise_candidate["method"],
                "fallback_used": True,
                "source_title": source_title,
                "source_title_source": source_title_source,
                "invalid_source_title_reasons": reasons,
                "sentence_id": concise_candidate["sentence_id"],
                "supporting_articles": concise_candidate[
                    "supporting_articles"
                ],
                "source_text": concise_candidate["source_text"],
                "transformations": concise_candidate["transformations"],
            }
        return {
            "text": lead["text"],
            "method": "extractive_lead_sentence",
            "fallback_used": True,
            "source_title": source_title,
            "source_title_source": source_title_source,
            "invalid_source_title_reasons": reasons,
            "sentence_id": lead.get("sentence_id"),
            "supporting_articles": lead.get("supporting_articles", []),
            "source_text": lead["text"],
            "transformations": [],
        }

    fallback_sentences = extract_usable_sentences(
        representative_clean_text or "",
    )
    if fallback_sentences:
        return {
            "text": fallback_sentences[0],
            "method": "representative_clean_text_sentence",
            "fallback_used": True,
            "source_title": source_title,
            "source_title_source": source_title_source,
            "invalid_source_title_reasons": reasons,
            "sentence_id": None,
            "supporting_articles": [],
            "source_text": fallback_sentences[0],
            "transformations": [],
        }

    return {
        "text": None,
        "method": "unavailable",
        "fallback_used": True,
        "source_title": source_title,
        "source_title_source": source_title_source,
        "invalid_source_title_reasons": reasons,
        "sentence_id": None,
        "supporting_articles": [],
        "source_text": None,
        "transformations": [],
    }


def _select_concise_source_title(
    current_lead: str,
    candidates: list[dict],
) -> Optional[dict]:
    heading_candidates = []
    lead_candidates = []
    for candidate in candidates:
        source_text = normalize_sentence(candidate.get("text") or "")
        display_text, transformations = _clean_source_heading(source_text)
        if not _is_concise_candidate(display_text, current_lead):
            continue

        supporting_articles = candidate.get("supporting_articles", [])
        source_positions = [
            sentence_index
            for support in supporting_articles
            for sentence_index in support.get("sentence_indexes", [])
        ]
        is_source_heading = bool(transformations) or (
            not _has_terminal_punctuation(source_text)
            and 0 in source_positions
        )
        is_source_lead = (
            _has_terminal_punctuation(source_text)
            and 0 in source_positions
        )
        if not is_source_heading and not is_source_lead:
            continue

        relevance = _token_coverage(display_text, current_lead)
        if relevance < MIN_TITLE_RELEVANCE:
            continue
        record = {
            "text": display_text,
            "method": (
                "extractive_source_heading"
                if is_source_heading
                else "extractive_source_lead_sentence"
            ),
            "sentence_id": candidate.get("sentence_id"),
            "supporting_articles": supporting_articles,
            "source_text": source_text,
            "transformations": transformations,
            "relevance": relevance,
            "has_representative_support": any(
                support.get("is_representative")
                for support in supporting_articles
            ),
            "support_count": candidate.get("support_count", 0),
        }
        if is_source_heading:
            heading_candidates.append(record)
        else:
            lead_candidates.append(record)

    pool = heading_candidates or lead_candidates
    if not pool:
        return None
    return max(
        pool,
        key=lambda candidate: (
            candidate["relevance"],
            candidate["has_representative_support"],
            candidate["support_count"],
            -len(candidate["text"]),
            candidate["sentence_id"] or "",
        ),
    )


def _clean_source_heading(text: str) -> tuple[str, list[str]]:
    if CONTENT_HEADING_PREFIX.match(text):
        return (
            CONTENT_HEADING_PREFIX.sub("", text).strip(),
            ["removed_reading_prefix"],
        )
    return text, []


def _is_concise_candidate(candidate: str, current_lead: str) -> bool:
    if not MIN_CONCISE_TITLE_CHARS <= len(candidate) <= MAX_DISPLAY_TITLE_CHARS:
        return False
    if len(candidate) >= len(current_lead):
        return False
    if not _has_sinhala(candidate):
        return False
    if len(_tokens(candidate)) < 4:
        return False
    if candidate.startswith(CONTEXTUAL_SENTENCE_PREFIXES):
        return False
    return not invalid_title_reasons(candidate, reference_text=current_lead)


def _token_coverage(candidate: str, reference: str) -> float:
    candidate_tokens = set(_tokens(candidate))
    reference_tokens = set(_tokens(reference))
    if not candidate_tokens:
        return 0.0
    return len(candidate_tokens & reference_tokens) / len(candidate_tokens)


def _has_terminal_punctuation(text: str) -> bool:
    stripped = text.rstrip()
    while stripped and stripped[-1] in CLOSING_PUNCTUATION:
        stripped = stripped[:-1].rstrip()
    return bool(stripped) and (
        stripped[-1] == "." or stripped[-1] in HARD_TERMINATORS
    )


def invalid_title_reasons(
    title: Optional[str],
    reference_text: Optional[str] = None,
) -> list[str]:
    normalized = normalize_sentence(title or "")
    if not normalized:
        return ["missing"]

    reasons = []
    if URL_TITLE_PATTERN.match(normalized):
        reasons.append("url_like")
    if (
        FILENAME_TITLE_PATTERN.match(normalized)
        or GENERIC_FILE_PATTERN.match(normalized)
    ):
        reasons.append("filename_like")
    if (
        OPAQUE_IDENTIFIER_PATTERN.match(normalized)
        and any(character.isdigit() for character in normalized)
    ):
        reasons.append("opaque_identifier")
    if _looks_visibly_truncated(normalized, reference_text or ""):
        reasons.append("visibly_truncated")
    return reasons


def _looks_visibly_truncated(title: str, reference_text: str) -> bool:
    title_tokens = _tokens(title)
    if len(title_tokens) < 3:
        return False
    final_token = title_tokens[-1]
    if not _has_sinhala(final_token) or len(final_token) > 2:
        return False

    reference_tokens = _tokens(reference_text)
    if final_token in reference_tokens:
        return False
    return any(
        len(token) > len(final_token) and token.startswith(final_token)
        for token in reference_tokens
    )


def _tokens(text: str) -> list[str]:
    normalized = normalize_sentence(text).casefold()
    return [match.group(0) for match in TOKEN_PATTERN.finditer(normalized)]


def _has_sinhala(text: str) -> bool:
    return any(SINHALA_START <= character <= SINHALA_END for character in text)
