"""
Sources for the negation inventory:

- De Abrew (1981), "The Syntax and Semantics of Negation in Sinhala",
  Cornell PhD dissertation.
- Peiris (2018), "A Comparative Analysis of Canonical Clausal Negation
  in English and Sinhala", IRCHSS 2018.
- Standard Sinhala grammar references for the productive prefix නො-.
"""

from __future__ import annotations

import re


# Standalone negation words (clausal negation).
# The most common form is නැත (formal / written) with variants නැහැ (spoken)
# and නෑ (colloquial contraction). නොවේ / නොවෙයි negate copular clauses
# ("is not"). These are matched as whole tokens.
_STANDALONE_NEGATIONS: frozenset[str] = frozenset({
    "නැත",     # formal negation of existence / did not
    "නැහැ",    # spoken variant
    "නෑ",      # colloquial contraction
    "නොවේ",    # is not (copular negation)
    "නොවෙයි",  # is not (variant)
    "නැති",    # non-existent / without
    "නොව",     # is not (bare stem, occasionally standalone)
})

# Productive negation prefix නො- (roughly "un-" / "non-" / "not").
# Attached to verb stems and some adjectives:
#   කළ ("did") → නොකළ ("did not")
#   කරන ("does") → නොකරන ("does not")
#   දකින ("sees") → නොදකින ("does not see")
#
# We detect this by looking for tokens that START with නො but are NOT one
# of the standalone forms above (which happen to also start with නො).
# The prefix is productive, so we can't enumerate every combination — we
# instead check the shape.
_NEGATION_PREFIX = "නො"

# The standalone words that START with නො must not double-count as
# prefixed forms. Any token in this set is handled by the standalone check.
_PREFIX_EXCLUSIONS: frozenset[str] = frozenset({
    w for w in _STANDALONE_NEGATIONS if w.startswith(_NEGATION_PREFIX)
})

# Sinhala token splitter: whitespace + basic punctuation.
_TOKEN_SPLIT = re.compile(r"[\s\.,!\?;:\(\)\[\]\{\}\"'\u2018\u2019\u201C\u201D—–…]+")


def _tokenize(text: str) -> list[str]:
    """Split text into non-empty tokens on whitespace and punctuation."""
    return [tok for tok in _TOKEN_SPLIT.split(text) if tok]


def has_negation(text: str) -> bool:
    """
    Return True if the text contains any Sinhala negation marker.

    Positive cases (returns True):
        "ලංකා කණ්ඩායම ජයග්‍රහණය ලබා ගත්තේ නැත"        (standalone නැත)
        "එය නොවේ"                                     (copular නොවේ)
        "ඔහු එතන නොසිටියේය"                          (prefixed නො-)

    Negative cases (returns False):
        "ලංකා කණ්ඩායම ජයග්‍රහණය ලබා ගත්තේය"            (no markers)
        "නොවැම්බර් මාසය"                              (නොවැම්බර් = "November",
                                                       not a prefix — but this
                                                       IS a false positive with
                                                       the current heuristic;
                                                       documented limitation)
    """
    tokens = _tokenize(text)
    for tok in tokens:
        # Case 1: exact match of a standalone negation word.
        if tok in _STANDALONE_NEGATIONS:
            return True
        # Case 2: token starts with නො but isn't already a standalone form.
        # Guard against very short tokens (just "නො" alone is standalone).
        if (
            tok.startswith(_NEGATION_PREFIX)
            and tok not in _PREFIX_EXCLUSIONS
            and len(tok) > len(_NEGATION_PREFIX) + 1
        ):
            return True
    return False


def polarity_matches(text_a: str, text_b: str) -> bool:
    """
    True iff both texts have the SAME negation polarity.

    Used by the semantic-overlap stage: two texts with matching content but
    OPPOSITE polarity are not duplicates. Only same-polarity pairs pass the
    duplicate check.
    """
    return has_negation(text_a) == has_negation(text_b)
