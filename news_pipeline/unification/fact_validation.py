from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Optional

from news_pipeline.unification.fact_normalization import (
    ISO_DATE_PATTERN,
    normalize_number as _normalize_number,
    normalize_text as _normalize_text,
    numeric_literals,
    sinhala_decimal_values,
    sinhala_entity_aliases,
    sinhala_number_word_values,
    time_values,
)

from news_pipeline.unification.gpt_contract import (
    GPT_PROMPT_VERSION_V2_7,
    GPT_PROMPT_VERSION_V2_8,
    GPT_PROMPT_VERSION_V2_9,
    GPT_PROMPT_VERSION_V2_10,
    GPTUnifiedStoryInputV2,
    GPTResolvedUnifiedStoryV2,
)


LATIN_NAME_PATTERN = re.compile(
    r"\b(?:[A-Z][A-Za-z.'’-]{1,})(?:\s+[A-Z][A-Za-z.'’-]{1,})+\b"
    r"|\b[A-Z]{2,}\b"
)
SINHALA_TOKEN_PATTERN = re.compile(r"[\u0D80-\u0DFF]+")
DEVANAGARI_TOKEN_PATTERN = re.compile(r"[\u0900-\u097F]+")
WORD_TOKEN_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)

CLAIM_PROJECTION_MIN_COVERAGE = 0.70
CLAIM_PROJECTION_MIN_TOKENS = 20
CLAIM_PROJECTION_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "by",
        "for",
        "from",
        "in",
        "is",
        "of",
        "on",
        "or",
        "that",
        "the",
        "to",
        "was",
        "were",
        "with",
        "අතර",
        "අනුව",
        "ඇත",
        "ඇති",
        "එම",
        "කර",
        "කරයි",
        "කරන",
        "කළ",
        "කිරීමට",
        "කෙරේ",
        "තිබේ",
        "තුළ",
        "නමුත්",
        "පවසා",
        "පසු",
        "පිළිබඳ",
        "බව",
        "ලදී",
        "ලෙස",
        "වන",
        "වූ",
        "වී",
        "වේ",
        "වෙත",
        "විසින්",
        "සඳහා",
        "සඳහන්",
        "සම්බන්ධයෙන්",
        "සහ",
        "සිට",
        "හෝ",
        "යන",
        "මෙම",
        "තවත්",
    }
)

MONTH_TOKENS = {
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
    "ජුලි",
    "ජනවාරි",
    "පෙබරවාරි",
    "මාර්තු",
    "අප්‍රේල්",
    "මැයි",
    "ජූනි",
    "ජූලි",
    "අගෝස්තු",
    "සැප්තැම්බර්",
    "ඔක්තෝබර්",
    "නොවැම්බර්",
    "දෙසැම්බර්",
}
MONTH_TOKEN_NUMBERS = {
    token: month
    for month, tokens in enumerate(
        (
            ("january", "ජනවාරි", "ජන"),
            ("february", "පෙබරවාරි", "පෙබ"),
            ("march", "මාර්තු", "මාර්"),
            ("april", "අප්‍රේල්", "අප්‍රේ"),
            ("may", "මැයි"),
            ("june", "ජූනි"),
            ("july", "ජූලි", "ජුලි"),
            ("august", "අගෝස්තු", "අගෝ"),
            ("september", "සැප්තැම්බර්", "සැප්"),
            ("october", "ඔක්තෝබර්", "ඔක්"),
            ("november", "නොවැම්බර්", "නොවැ"),
            ("december", "දෙසැම්බර්", "දෙසැ"),
        ),
        start=1,
    )
    for token in tokens
}
SINHALA_ENTITY_MARKERS = {
    "මහතා",
    "මහත්මිය",
    "හිමි",
    "තෙරණුවෝ",
}

PERSON_COUNT_PATTERN = re.compile(
    r"(?<!\d)([0-9\u0DE6-\u0DEF]+(?:,[0-9\u0DE6-\u0DEF]+)*)\s*"
    r"(?:"
    r"\u0daf\u0dd9\u0db1\u0dd9\u0d9a\u0dd4"
    r"|\u0daf\u0dd9\u0db1\u0dd9\u0d9a\u0dca"
    r"|\u0daf\u0dd9\u0db1\u0d9a\u0dd4"
    r"|\u0daf\u0dd9\u0db1\u0dcf"
    r"|people|persons|officers|inmates|prisoners"
    r")",
    re.IGNORECASE,
)
DEATH_MARKERS = tuple(
    _marker.casefold()
    for _marker in (
        "\u0db8\u0dd2\u0dba\u0d9c\u0ddc\u0dc3\u0dca",
        "\u0db8\u0dd2\u0dba\u0d9c\u0dd2\u0dba",
        "\u0db8\u0dbb\u0dab",
        "\u0d9d\u0dcf\u0dad\u0db1\u0dba",
        "dead",
        "deaths",
        "killed",
    )
)
DEATH_BREAKDOWN_MARKERS = tuple(
    _marker.casefold()
    for _marker in (
        (
            "\u0db8\u0dd2\u0dba\u0d9c\u0dd2\u0dba "
            "\u0d85\u0dba \u0d85\u0dad\u0dbb"
        ),
        (
            "\u0db8\u0dd2\u0dba\u0d9c\u0dd2\u0dba "
            "\u0db4\u0dd2\u0dbb\u0dd2\u0dc3 \u0d85\u0dad\u0dbb"
        ),
        "among the dead",
        "among those killed",
    )
)


@dataclass(frozen=True)
class FactShapeIssue:
    code: str
    location: str
    value: str


@dataclass(frozen=True)
class FactShapeValidationReport:
    issues: tuple[FactShapeIssue, ...]

    @property
    def accepted(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict:
        return {
            "accepted": self.accepted,
            "issues": [
                {
                    "code": issue.code,
                    "location": issue.location,
                    "value": issue.value,
                }
                for issue in self.issues
            ],
        }


@dataclass(frozen=True)
class ClaimProjectionIssue:
    code: str
    location: str
    coverage_ratio: float
    minimum_coverage_ratio: float
    matched_token_count: int
    claim_token_count: int


@dataclass(frozen=True)
class ClaimProjectionValidationReport:
    applied: bool
    coverage_ratio: Optional[float]
    matched_token_count: int
    claim_token_count: int
    issues: tuple[ClaimProjectionIssue, ...]

    @property
    def accepted(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict:
        return {
            "applied": self.applied,
            "accepted": self.accepted,
            "coverage_ratio": self.coverage_ratio,
            "minimum_coverage_ratio": CLAIM_PROJECTION_MIN_COVERAGE,
            "matched_token_count": self.matched_token_count,
            "claim_token_count": self.claim_token_count,
            "issues": [
                {
                    "code": issue.code,
                    "location": issue.location,
                    "coverage_ratio": issue.coverage_ratio,
                    "minimum_coverage_ratio": (
                        issue.minimum_coverage_ratio
                    ),
                    "matched_token_count": (
                        issue.matched_token_count
                    ),
                    "claim_token_count": issue.claim_token_count,
                }
                for issue in self.issues
            ],
        }


@dataclass(frozen=True)
class SemanticSupportIssue:
    code: str
    location: str
    article_id: int
    evidence_sentence_index: int
    antecedent_sentence_indexes: tuple[int, ...]
    alternate_numeric_anchors: tuple[str, ...]


@dataclass(frozen=True)
class SemanticSupportValidationReport:
    issues: tuple[SemanticSupportIssue, ...]

    @property
    def accepted(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict:
        return {
            "accepted": self.accepted,
            "issues": [
                {
                    "code": issue.code,
                    "location": issue.location,
                    "article_id": issue.article_id,
                    "evidence_sentence_index": (
                        issue.evidence_sentence_index
                    ),
                    "antecedent_sentence_indexes": list(
                        issue.antecedent_sentence_indexes
                    ),
                    "alternate_numeric_anchors": list(
                        issue.alternate_numeric_anchors
                    ),
                }
                for issue in self.issues
            ],
        }


@dataclass(frozen=True)
class NumericalConflictCoverageIssue:
    code: str
    location: str
    stated_total: int
    component_counts: tuple[int, ...]
    component_sum: int


@dataclass(frozen=True)
class NumericalConflictCoverageValidationReport:
    applied: bool
    issues: tuple[NumericalConflictCoverageIssue, ...]

    @property
    def accepted(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict:
        return {
            "applied": self.applied,
            "accepted": self.accepted,
            "issues": [
                {
                    "code": issue.code,
                    "location": issue.location,
                    "stated_total": issue.stated_total,
                    "component_counts": list(issue.component_counts),
                    "component_sum": issue.component_sum,
                }
                for issue in self.issues
            ],
        }


def _projection_tokens(value: str) -> set[str]:
    normalized = _normalize_text(value)
    return {
        token
        for token in WORD_TOKEN_PATTERN.findall(normalized)
        if (
            len(token) >= 2
            and token not in CLAIM_PROJECTION_STOPWORDS
        )
    }


def validate_claim_projection(
    resolved: GPTResolvedUnifiedStoryV2,
    contract_input: Optional[GPTUnifiedStoryInputV2] = None,
) -> ClaimProjectionValidationReport:
    """Detect dense structured claims missing from publishable narrative.

    The v2.7 development review exposed a large, deterministic separation:
    its one major omission projected 40.2% of unique informative claim tokens
    into the story, while every non-major v2.7 candidate projected at least
    87.3%. The deliberately conservative 70% boundary is fail-closed and
    applies to v2.7 and later experimental prompt contracts, preserving all
    earlier reviewed contracts.
    """
    if (
        contract_input is not None
        and contract_input.prompt_version
        not in {
            GPT_PROMPT_VERSION_V2_7,
            GPT_PROMPT_VERSION_V2_8,
            GPT_PROMPT_VERSION_V2_9,
            GPT_PROMPT_VERSION_V2_10,
        }
    ):
        return ClaimProjectionValidationReport(
            applied=False,
            coverage_ratio=None,
            matched_token_count=0,
            claim_token_count=0,
            issues=(),
        )

    claim_tokens = _projection_tokens(
        " ".join(claim.claim_text for claim in resolved.claims)
    )
    if len(claim_tokens) < CLAIM_PROJECTION_MIN_TOKENS:
        return ClaimProjectionValidationReport(
            applied=True,
            coverage_ratio=1.0,
            matched_token_count=len(claim_tokens),
            claim_token_count=len(claim_tokens),
            issues=(),
        )

    story_tokens = _projection_tokens(resolved.unified_story)
    matched_tokens = claim_tokens & story_tokens
    coverage_ratio = len(matched_tokens) / len(claim_tokens)
    rounded_ratio = round(coverage_ratio, 6)
    issues = ()
    if coverage_ratio < CLAIM_PROJECTION_MIN_COVERAGE:
        issues = (
            ClaimProjectionIssue(
                code="insufficient_claim_projection",
                location="unified_story",
                coverage_ratio=rounded_ratio,
                minimum_coverage_ratio=(
                    CLAIM_PROJECTION_MIN_COVERAGE
                ),
                matched_token_count=len(matched_tokens),
                claim_token_count=len(claim_tokens),
            ),
        )
    return ClaimProjectionValidationReport(
        applied=True,
        coverage_ratio=rounded_ratio,
        matched_token_count=len(matched_tokens),
        claim_token_count=len(claim_tokens),
        issues=issues,
    )


def _numbers(value: str) -> set[str]:
    return numeric_literals(value)


def _person_counts(value: str) -> tuple[int, ...]:
    return tuple(
        int(_normalize_number(match.group(1)))
        for match in PERSON_COUNT_PATTERN.finditer(value)
    )


def _has_marker(value: str, markers: tuple[str, ...]) -> bool:
    normalized = _normalize_text(value)
    return any(marker in normalized for marker in markers)


def _story_sentences(value: str) -> list[str]:
    return [
        sentence.strip()
        for sentence in re.split(r"[\n.!?\u0964]+", value)
        if sentence.strip()
    ]


def validate_numerical_conflict_coverage(
    resolved: GPTResolvedUnifiedStoryV2,
    contract_input: Optional[GPTUnifiedStoryInputV2],
) -> NumericalConflictCoverageValidationReport:
    """Quarantine a measured unacknowledged death-count overflow pattern.

    This deliberately narrow v2.8+ warning looks only for a stated death total
    followed by an "among the dead" person-count breakdown whose components
    sum above that total. Smaller partial breakdowns are not treated as
    conflicts. A conflict record that names the total and either every
    component or their sum satisfies the gate.
    """
    if (
        contract_input is None
        or contract_input.prompt_version
        not in {
            GPT_PROMPT_VERSION_V2_8,
            GPT_PROMPT_VERSION_V2_9,
            GPT_PROMPT_VERSION_V2_10,
        }
    ):
        return NumericalConflictCoverageValidationReport(
            applied=False,
            issues=(),
        )

    totals: list[int] = []
    breakdowns: list[tuple[int, tuple[int, ...]]] = []
    for sentence_index, sentence in enumerate(
        _story_sentences(resolved.unified_story)
    ):
        counts = _person_counts(sentence)
        if not counts:
            continue
        if _has_marker(sentence, DEATH_BREAKDOWN_MARKERS):
            if len(counts) >= 2:
                breakdowns.append((sentence_index, counts))
            continue
        if _has_marker(sentence, DEATH_MARKERS):
            # The lead person count is the sentence-level reported total.
            # Later counts commonly describe causes or other subdivisions.
            totals.append(counts[0])

    if not totals or not breakdowns:
        return NumericalConflictCoverageValidationReport(
            applied=True,
            issues=(),
        )

    stated_total = max(totals)
    conflict_number_sets = [
        _numbers(conflict.description)
        for conflict in resolved.conflicts_or_uncertainties
    ]
    issues = []
    for sentence_index, component_counts in breakdowns:
        component_sum = sum(component_counts)
        if component_sum <= stated_total:
            continue
        total_token = str(stated_total)
        component_tokens = {str(value) for value in component_counts}
        sum_token = str(component_sum)
        acknowledged = any(
            total_token in numbers
            and (
                component_tokens <= numbers
                or sum_token in numbers
            )
            for numbers in conflict_number_sets
        )
        if acknowledged:
            continue
        issues.append(
            NumericalConflictCoverageIssue(
                code="unacknowledged_death_count_overflow",
                location=f"unified_story.sentences[{sentence_index}]",
                stated_total=stated_total,
                component_counts=component_counts,
                component_sum=component_sum,
            )
        )
    return NumericalConflictCoverageValidationReport(
        applied=True,
        issues=tuple(issues),
    )


AMBIGUOUS_REFERENCE_MARKERS = tuple(
    _normalize_text(marker)
    for marker in (
        "මේ සම්බන්ධයෙන්",
        "ඒ සම්බන්ධයෙන්",
        "එම කටයුත්ත",
        "එම සිදුවීම",
        "අදාළව",
        "මේ බව",
        "this matter",
        "that matter",
        "this incident",
        "that incident",
        "related to it",
    )
)


def _contains_ambiguous_reference(value: str) -> bool:
    normalized = _normalize_text(value)
    return any(
        marker in normalized
        for marker in AMBIGUOUS_REFERENCE_MARKERS
    )


def validate_semantic_support_strands(
    resolved: GPTResolvedUnifiedStoryV2,
    contract_input: Optional[GPTUnifiedStoryInputV2],
    *,
    antecedent_window: int = 3,
) -> SemanticSupportValidationReport:
    """Fail closed on ambiguous cross-strand numeric coreference.

    Multi-topic source articles can contain a vague phrase such as "this
    matter" after a separately quantified topic. Exact evidence resolution
    proves that the cited sentence exists, but not that the vague antecedent
    was attached to the correct topic. This gate detects the measured Phase 4
    pattern: a claim/evidence pair uses ambiguous coreference while its recent
    source context contains a material numeric anchor that belongs neither to
    the article title nor to the claim and cited evidence.

    The rule is deliberately narrow and must remain fail-closed until fresh
    precision measurement is complete.
    """
    if contract_input is None:
        return SemanticSupportValidationReport(issues=())
    article_by_id = {
        article.article_id: article
        for article in contract_input.articles
    }
    issues = []
    seen = set()
    for claim_index, claim in enumerate(resolved.claims):
        claim_is_ambiguous = _contains_ambiguous_reference(
            claim.claim_text
        )
        for evidence in claim.evidence:
            if not (
                claim_is_ambiguous
                or _contains_ambiguous_reference(evidence.excerpt)
            ):
                continue
            article = article_by_id.get(evidence.article_id)
            if article is None:
                continue
            local_support_text = " ".join(
                (claim.claim_text, evidence.excerpt)
            )
            # A locally explicit number already identifies the strand; the
            # measured failure had only a vague reference to a differently
            # quantified preceding topic.
            if _numbers(local_support_text):
                continue
            # Shared informative title vocabulary is another conservative
            # strand anchor. The fresh holdout showed that vague connective
            # language is common inside otherwise explicit same-topic
            # passages (for example, a tariff claim in a tariff article).
            if _projection_tokens(article.title or "") & _projection_tokens(
                local_support_text
            ):
                continue
            start = max(0, evidence.sentence_index - antecedent_window)
            preceding_spans = article.evidence_spans[
                start:evidence.sentence_index
            ]
            if not preceding_spans:
                continue
            prior_numbers = {
                number
                for span in preceding_spans
                for number in _numbers(span.text)
            }
            supported_numbers = _numbers(
                " ".join(
                    (
                        article.title or "",
                        local_support_text,
                    )
                )
            )
            alternate_numbers = tuple(
                sorted(prior_numbers - supported_numbers)
            )
            if not alternate_numbers:
                continue
            key = (
                claim_index,
                evidence.article_id,
                evidence.sentence_index,
                alternate_numbers,
            )
            if key in seen:
                continue
            seen.add(key)
            issues.append(
                SemanticSupportIssue(
                    code="ambiguous_cross_strand_coreference",
                    location=f"claims[{claim_index}].claim_text",
                    article_id=evidence.article_id,
                    evidence_sentence_index=evidence.sentence_index,
                    antecedent_sentence_indexes=tuple(
                        span.sentence_index for span in preceding_spans
                    ),
                    alternate_numeric_anchors=alternate_numbers,
                )
            )
    return SemanticSupportValidationReport(issues=tuple(issues))


def _months(value: str) -> dict[str, int]:
    normalized = _normalize_text(value)
    months = {
        _normalize_text(token): month
        for token, month in MONTH_TOKEN_NUMBERS.items()
        if re.search(
            rf"(?<!\w){re.escape(_normalize_text(token))}(?!\w)",
            normalized,
        )
    }
    for match in ISO_DATE_PATTERN.finditer(normalized):
        months[match.group(0)] = int(match.group(1))
    return months


def _latin_names(value: str) -> set[str]:
    return {
        _normalize_text(match.group(0))
        for match in LATIN_NAME_PATTERN.finditer(value)
    }


def _sinhala_entity_names(value: str) -> set[str]:
    tokens = SINHALA_TOKEN_PATTERN.findall(
        _normalize_text(value)
    )
    candidates = set()
    for index, token in enumerate(tokens):
        if token not in SINHALA_ENTITY_MARKERS or index == 0:
            continue
        # Check the identifying token, not the honorific itself. Sources
        # commonly include or omit "මහතා" while referring to the same person.
        candidates.add(_normalize_text(tokens[index - 1]))
    return candidates


def _unsupported_values(
    generated_text: str,
    evidence_text: str,
) -> Iterable[tuple[str, str]]:
    evidence_normalized = _normalize_text(evidence_text)

    for token in sorted(set(DEVANAGARI_TOKEN_PATTERN.findall(generated_text))):
        yield "unexpected_devanagari_token", token

    evidence_numbers = (
        _numbers(evidence_text)
        | sinhala_decimal_values(evidence_text)
        | sinhala_number_word_values(evidence_text)
    )
    generated_times = time_values(generated_text)
    evidence_time_identities = set(time_values(evidence_text).values())
    unsupported_numbers = _numbers(generated_text) - evidence_numbers
    for number in sorted(unsupported_numbers):
        if generated_times.get(number) in evidence_time_identities:
            continue
        yield "unsupported_number", number

    evidence_months = set(_months(evidence_text).values())
    for token, month in sorted(_months(generated_text).items()):
        if month not in evidence_months:
            yield "unsupported_date_token", token

    for name in sorted(_latin_names(generated_text)):
        if name not in evidence_normalized:
            yield "unsupported_named_entity", name

    evidence_sinhala_tokens = {
        _normalize_text(token)
        for token in SINHALA_TOKEN_PATTERN.findall(evidence_text)
    }
    for name in sorted(_sinhala_entity_names(generated_text)):
        aliases = sinhala_entity_aliases(name)
        if not any(
            alias in evidence_normalized
            or alias in evidence_sinhala_tokens
            for alias in aliases
        ):
            yield "unsupported_named_entity", name


def validate_resolved_fact_shapes(
    resolved: GPTResolvedUnifiedStoryV2,
    contract_input: Optional[GPTUnifiedStoryInputV2] = None,
) -> FactShapeValidationReport:
    issues = []
    article_context = {
        article.article_id: " ".join(
            value
            for value in (
                article.publisher,
                article.title,
                article.published_date,
                *(
                    span.text
                    for span in article.evidence_spans
                ),
            )
            if value
        )
        for article in (contract_input.articles if contract_input else [])
    }
    all_evidence_text = " ".join(
        evidence.excerpt
        for claim in resolved.claims
        for evidence in claim.evidence
    )
    all_evidence_text += " " + " ".join(
        evidence.excerpt
        for conflict in resolved.conflicts_or_uncertainties
        for evidence in conflict.evidence
    )
    all_evidence_text += " " + " ".join(article_context.values())

    for claim_index, claim in enumerate(resolved.claims):
        evidence_text = " ".join(
            evidence.excerpt for evidence in claim.evidence
        )
        evidence_text += " " + " ".join(article_context.values())
        for code, value in _unsupported_values(
            claim.claim_text,
            evidence_text,
        ):
            issues.append(
                FactShapeIssue(
                    code=code,
                    location=f"claims[{claim_index}].claim_text",
                    value=value,
                )
            )

    for conflict_index, conflict in enumerate(
        resolved.conflicts_or_uncertainties
    ):
        evidence_text = " ".join(
            evidence.excerpt for evidence in conflict.evidence
        )
        evidence_text += " " + " ".join(article_context.values())
        for code, value in _unsupported_values(
            conflict.description,
            evidence_text,
        ):
            issues.append(
                FactShapeIssue(
                    code=code,
                    location=(
                        "conflicts_or_uncertainties"
                        f"[{conflict_index}].description"
                    ),
                    value=value,
                )
            )

    for field_name, generated_text in (
        ("display_title", resolved.display_title),
        ("unified_story", resolved.unified_story),
    ):
        for code, value in _unsupported_values(
            generated_text,
            all_evidence_text,
        ):
            issues.append(
                FactShapeIssue(
                    code=code,
                    location=field_name,
                    value=value,
                )
            )

    return FactShapeValidationReport(issues=tuple(issues))
