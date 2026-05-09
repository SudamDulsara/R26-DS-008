"""
STAGE 1: Structural Grapheme Refinement
========================================
Fixes broken Sinhala grapheme structures produced by OCR engines.

Rules applied:
  1. Post-base vowel sign spaces removed
  2. Kombuwa (ෙ) reordered / spaces fixed
  3. Al-lakuna (්) attached to consonant
  4. Orphaned vowel signs removed
  5. Multiple spaces collapsed
  6. Word boundary reconstruction using Sinhala word dictionary + MuRIL model
"""

import re

# ─────────────────────────────────────────────────────────────
#  Safe import of word boundary module
#  If MuRIL is not downloaded yet or word_boundary.py is missing,
#  rules 1-5 still run — Rule 6 is skipped with a clear message
# ─────────────────────────────────────────────────────────────

try:
    from pipeline.word_boundary import reconstruct_words
    WORD_BOUNDARY_AVAILABLE = True
    print("[Stage 1] word_boundary module loaded ✅")
except Exception as e:
    print(f"[Stage 1] word_boundary import failed: {e}")
    print("[Stage 1] Rules 1-5 will run. Rule 6 (MuRIL word reconstruction) skipped.")
    WORD_BOUNDARY_AVAILABLE = False


# ─────────────────────────────────────────────────────────────
#  Sinhala Unicode definitions
# ─────────────────────────────────────────────────────────────

SINHALA_CONSONANT_RANGE = (0x0D9A, 0x0DC6)
SINHALA_BLOCK           = (0x0D80, 0x0DFF)

POST_BASE_VOWEL_SIGNS = (
    "\u0DCF\u0DD0\u0DD1\u0DD2\u0DD3"
    "\u0DD4\u0DD6\u0DD8\u0DDF\u0DF2\u0DF3\u0DCA"
)
PRE_BASE_VOWEL_SIGNS  = "\u0DD9\u0DDA\u0DDB"
KOMBUWA_COMBOS        = "\u0DDC\u0DDD\u0DDE"
ALL_VOWEL_SIGNS       = POST_BASE_VOWEL_SIGNS + PRE_BASE_VOWEL_SIGNS + KOMBUWA_COMBOS


# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────

def _is_sinhala(ch: str) -> bool:
    return SINHALA_BLOCK[0] <= ord(ch) <= SINHALA_BLOCK[1]

def _is_vowel_sign(ch: str) -> bool:
    return ch in ALL_VOWEL_SIGNS


# ─────────────────────────────────────────────────────────────
#  Rule 1 — Post-base vowel sign spaces
# ─────────────────────────────────────────────────────────────

def fix_post_base_spaces(text: str) -> str:
    """
    Remove spaces that OCR inserts between a Sinhala character
    and its following post-base vowel sign.
    e.g. ව  ත  ු  ර  →  වතුර
    """
    return re.sub(
        r'([\u0D80-\u0DFF]) {1,3}(?=[\u0DCF-\u0DDF\u0DF2\u0DF3\u0DCA])',
        r'\1', text
    )


# ─────────────────────────────────────────────────────────────
#  Rule 2 — Kombuwa reorder / spaces
# ─────────────────────────────────────────────────────────────

def fix_pre_base_kombuwa(text: str) -> str:
    """
    Fix kombuwa (ෙ ේ ෛ) that OCR places before its consonant
    or separates with a space.
    e.g. ෙ  ම  →  මෙ
    """
    # Case A: consonant + space + kombuwa → remove space
    text = re.sub(
        r'([\u0D9A-\u0DC6]) {1,3}([\u0DD9\u0DDA\u0DDB])',
        r'\1\2', text
    )
    # Case B: kombuwa at start of token → reorder
    text = re.sub(
        r'(^|(?<=\s))([\u0DD9\u0DDA\u0DDB]) {0,3}([\u0D9A-\u0DC6])',
        lambda m: m.group(1) + m.group(3) + m.group(2),
        text
    )
    return text


# ─────────────────────────────────────────────────────────────
#  Rule 3 — Al-lakuna spaces
# ─────────────────────────────────────────────────────────────

def fix_al_lakuna_spaces(text: str) -> str:
    """
    Remove space between a consonant and al-lakuna (්).
    e.g. ශ ් ර  →  ශ්ර
    """
    return re.sub(
        r'([\u0D9A-\u0DC6]) {1,3}(\u0DCA)',
        r'\1\2', text
    )


# ─────────────────────────────────────────────────────────────
#  Rule 4 — Remove orphaned vowel signs
# ─────────────────────────────────────────────────────────────

def remove_orphaned_signs(text: str) -> str:
    """
    Remove vowel signs that have no valid Sinhala base consonant
    before them — these are pure OCR noise.
    """
    chars  = list(text)
    result = []
    for i, ch in enumerate(chars):
        if _is_vowel_sign(ch):
            prev = next(
                (chars[j] for j in range(i - 1, -1, -1) if chars[j] != " "),
                None
            )
            if prev and _is_sinhala(prev):
                result.append(ch)
            # else: orphaned — drop silently
        else:
            result.append(ch)
    return "".join(result)


# ─────────────────────────────────────────────────────────────
#  Rule 5 — Normalise multiple spaces
# ─────────────────────────────────────────────────────────────

def normalise_spaces(text: str) -> str:
    """Collapse multiple consecutive spaces into a single space."""
    return re.sub(r'  +', ' ', text).strip()


# ─────────────────────────────────────────────────────────────
#  Main Stage 1 entry point
# ─────────────────────────────────────────────────────────────

def stage1_grapheme_refinement(raw_ocr_text: str) -> dict:
    """
    Apply all grapheme-refinement rules in sequence.

    Rules 1-5: Script-aware structural fixes (always run)
    Rule  6:   MuRIL + dictionary word boundary reconstruction
               (runs only if word_boundary.py is available)

    Returns a dict with:
      - 'output'              : final refined text
      - 'steps'               : [(label, text)] — input and final output
      - 'changed'             : bool
      - 'word_boundary_used'  : bool — whether Rule 6 ran
    """
    text = raw_ocr_text

    # ── Rules 1-5: structural grapheme fixes ──────────────────
    text = fix_post_base_spaces(text)
    text = fix_pre_base_kombuwa(text)
    text = fix_al_lakuna_spaces(text)
    text = remove_orphaned_signs(text)
    text = normalise_spaces(text)

    # ── Rule 6: MuRIL word boundary reconstruction ────────────
    # Disabled: context-free dictionary merges produce valid but wrong words
    # that are invisible to Groq downstream. Groq handles merging better
    # using full sentence context.
    word_boundary_used = False
    print("[Stage 1] Rule 6 skipped — word boundary merging delegated to Groq (Stage 2/3)")

    steps = [
        ("🔴 Raw OCR Input",            raw_ocr_text),
        ("✅ After Grapheme Correction", text),
    ]

    return {
        "output":             text,
        "steps":              steps,
        "changed":            text != raw_ocr_text,
        "word_boundary_used": word_boundary_used,
    }