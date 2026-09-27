"""
quality/negation_detector.py
----------------------------
Detects the negation polarity of a Sinhala text.

Why this exists
---------------
Sentence-embedding models like LaBSE are trained on paraphrase similarity.
They are structurally weak at negation: two sentences that differ only by
"X did Y" vs "X did NOT Y" often score >0.85 similar because 90%+ of the
words are identical and the embedding space wasn't trained to be sensitive
to that specific perturbation.

This is a documented limitation of sentence encoders (see e.g. SICK-R
and STS-B analyses across sentence-BERT variants). For a corpus quality
pipeline, this matters: two documents making opposite claims are NOT
duplicates and should not be treated as such.

The polarity check runs after sentence-embedding similarity has already
identified a candidate near-duplicate. If the two documents don't agree
on whether they contain Sinhala negation markers, we do not treat them
as duplicates regardless of similarity score.

What this is NOT
----------------
- It is NOT a full negation-scope analyzer. It doesn't tell you what
  is being negated, or handle sentences with multiple clauses where
  one is negated and one isn't. It's a coarse binary check.
- It does NOT catch semantic inversions without negation markers, such
  as antonym flips ("won" vs "lost") or lexical negation ("dry" vs
  "wet"). The proper solution for those is a Natural Language Inference
  re-ranker, planned as future work.
- It does NOT use context. "The article did NOT say X" is treated as
  negated, even if X itself is a positive claim being reported.

For the current prototype this is sufficient: the primary failure mode
we observed is direct clausal negation of matched content, and this
handles that case cleanly.

Sources for the negation inventory
----------------------------------
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
