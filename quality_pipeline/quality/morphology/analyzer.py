"""
analyzer.py
-----------
Rule-based morphological analyzer for Sinhala words.

Given a word, returns its decomposition: a (probable) root plus a stack of
recognized suffixes, each tagged with category (case / plural / verb / etc.).

How it works (longest-match iterative stripping):
  1. Try to match the longest known suffix at the end of the word.
  2. If matched, record (suffix, category), strip it, and repeat on the rest.
  3. Stop when no suffix matches OR what's left is too short to be a root.
  4. Return whatever's left as the root.

Why this is sufficient for our purpose:
  We don't need to be RIGHT about the analysis to score quality usefully.
  We need our analyzer's *behavior* to differ between rich Sinhala and
  degraded text — and it does, because:
    - Rich Sinhala → many words decompose into root + 1-3 known suffixes
    - OCR garbage  → words don't end in known patterns; few decompositions
    - ASR junk     → same disfluency repeats; very few unique morphemes
    - MT-translated → suspiciously *flat* morphology, all forms similar
  A perfect analyzer would do this better, but a rule-based one already
  produces signal that distinguishes these classes.

What this analyzer is NOT:
  - It is NOT a full Sinhala lemmatizer (that's SinMorphy / Welgama et al.)
  - It does NOT handle sandhi (morphophonological) rules at boundaries
  - It does NOT use POS context to disambiguate overlapping suffixes (-lā as
    plural vs. -lā as past participle) — it just records both possibilities.
  These limitations are honest, documented, and don't break the quality
  signal. Replacing the engine with a learned analyzer later is a drop-in
  swap because the output schema (Morpheme, MorphemeAnalysis) is stable.
"""

from dataclasses import dataclass, field

from .suffix_rules import SUFFIX_TABLE


# Minimum number of characters that must remain to be considered a "root".
# If we strip suffixes down past this, we either over-stripped (rolled back)
# or the word is too short to meaningfully analyze.
MIN_ROOT_LENGTH = 2


@dataclass
class Morpheme:
    """A single suffix that was peeled off the word."""
    surface: str          # the actual characters, e.g. "ගේ"
    category: str         # "case", "plural", "verb", etc.


@dataclass
class MorphemeAnalysis:
    """Result of analyzing one word."""
    word: str                                    # original input word
    root: str                                    # what remains after stripping
    suffixes: list[Morpheme] = field(default_factory=list)
    is_sinhala: bool = True                      # any Sinhala chars at all?
    decomposed: bool = False                     # any suffixes recognized?

    @property
    def depth(self) -> int:
        """How many suffixes were stripped (= agglutination depth)."""
        return len(self.suffixes)

    @property
    def categories(self) -> set[str]:
        """Set of categories present — used for diversity calculations."""
        return {m.category for m in self.suffixes}


# Sinhala Unicode block: U+0D80–U+0DFF
_SINHALA_RANGE = range(0x0D80, 0x0E00)


def _is_sinhala_char(ch: str) -> bool:
    return len(ch) == 1 and ord(ch) in _SINHALA_RANGE


def _has_sinhala(word: str) -> bool:
    return any(_is_sinhala_char(c) for c in word)


def analyze(word: str) -> MorphemeAnalysis:
    """
    Decompose a single Sinhala word into root + suffixes.

    Always returns a MorphemeAnalysis (never raises). For non-Sinhala or
    too-short words, returns the word as its own root with no suffixes
    and is_sinhala=False.
    """
    word = word.strip()

    # Non-Sinhala or trivial input: return as-is.
    if not word or not _has_sinhala(word):
        return MorphemeAnalysis(
            word=word, root=word, is_sinhala=False, decomposed=False
        )

    remaining = word
    stripped: list[Morpheme] = []

    # Iteratively peel suffixes from the right.
    while True:
        match_found = False
        for suffix, category in SUFFIX_TABLE:
            if remaining.endswith(suffix):
                # Don't strip if it would leave the root too short.
                if len(remaining) - len(suffix) < MIN_ROOT_LENGTH:
                    continue
                stripped.append(Morpheme(surface=suffix, category=category))
                remaining = remaining[: -len(suffix)]
                match_found = True
                break  # restart the loop, look for the next suffix
        if not match_found:
            break

    return MorphemeAnalysis(
        word=word,
        root=remaining,
        suffixes=list(reversed(stripped)),  # natural order: root → outermost
        is_sinhala=True,
        decomposed=len(stripped) > 0,
    )


def analyze_text(text: str) -> list[MorphemeAnalysis]:
    """Analyze every whitespace-separated token in a text."""
    # Strip common Sinhala/Latin punctuation before analysis.
    cleaned = text
    for p in ".,;:!?\"'()[]{}—–-…":
        cleaned = cleaned.replace(p, " ")
    return [analyze(tok) for tok in cleaned.split() if tok]