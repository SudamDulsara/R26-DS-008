"""
STAGE 4: Syntactic Restoration
================================
Restores punctuation, spacing, and sentence boundaries
damaged during OCR extraction.

Rules applied:
  1. Broken lines mid-sentence   → join with space
  2. Sinhala danda (।) restore   → normalise pipe | to ।
  3. Space before punctuation    → remove it
  4. Space after punctuation     → add it
  5. Multiple spaces             → collapse to single space
  6. Missing sentence-end mark   → add full stop if absent
"""

import re


#  Individual restoration rules

def _fix_newline_breaks(text: str) -> str:
    """
    OCR often breaks a single sentence across multiple lines.
    Join lines that don't end with sentence-ending punctuation.
    """
    lines = text.split('\n')
    joined = []
    buffer = ""
    sentence_enders = {'.', '!', '?', '।', '|', '\u0964', '\u0965'}

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if buffer:
                joined.append(buffer.strip())
                buffer = ""
            joined.append("")
            continue

        buffer = (buffer + " " + stripped).strip() if buffer else stripped

        if stripped[-1] in sentence_enders:
            joined.append(buffer.strip())
            buffer = ""

    if buffer:
        joined.append(buffer.strip())

    return '\n'.join(joined)


def _normalise_sinhala_danda(text: str) -> str:
    """
    Sinhala uses danda (।, U+0964) as a sentence ender.
    OCR often produces a pipe | instead. Normalise it.
    """
    return re.sub(
        r'([\u0D80-\u0DFF])\s*\|\s*([\u0D80-\u0DFF])',
        r'\1। \2',
        text
    )


def _fix_space_before_punctuation(text: str) -> str:
    """Remove space immediately before , . ! ? ; : )"""
    return re.sub(r'\s+([,\.!\?;:\)])', r'\1', text)


def _fix_space_after_punctuation(text: str) -> str:
    """Ensure a space follows , . ! ? ; : ) when next char is not a space."""
    return re.sub(r'([,\.!\?;:\)])\s*(?=[^\s])', r'\1 ', text)


def _fix_multiple_spaces(text: str) -> str:
    """Collapse multiple consecutive spaces into one."""
    return re.sub(r' {2,}', ' ', text)


def _add_missing_sentence_end(text: str) -> str:
    """
    If the text ends with a Sinhala character and has no
    sentence-ending punctuation, add a full stop.
    """
    stripped = text.rstrip()
    sentence_enders = {'.', '!', '?', '।', '\u0964', '\u0965'}
    if stripped and stripped[-1] not in sentence_enders:
        if re.search(r'[\u0D80-\u0DFF]', stripped):
            return stripped + '.'
    return text



#  Stage 4 entry point

def stage4_syntactic_restoration(text: str) -> dict:
    """
    Apply all syntactic restoration rules in sequence.

    Returns a dict with:
      - 'output'  : restored text
      - 'steps'   : list of (label, text) for UI display
      - 'changed' : bool
    """
    steps = [("🔴 Input to Stage 4", text)]

    t = text

    # Rule 1 disabled: LLM in Stage 3 handles sentence joining with full context.
    # Applying it here merges intentional line breaks in poetry/verse content.
    steps.append(("Rule 1 — Broken lines join (skipped)", t))

    t = _normalise_sinhala_danda(t)
    steps.append(("Rule 2 — Sinhala danda (।) normalised", t))

    t = _fix_space_before_punctuation(t)
    steps.append(("Rule 3 — Spaces before punctuation removed", t))

    t = _fix_space_after_punctuation(t)
    steps.append(("Rule 4 — Spaces after punctuation added", t))

    t = _fix_multiple_spaces(t)
    steps.append(("Rule 5 — Multiple spaces collapsed", t))

    t = _add_missing_sentence_end(t)
    steps.append(("Rule 6 — Missing sentence-end punctuation added", t))

    return {
        "output": t,
        "steps": steps,
        "changed": t != text,
    }