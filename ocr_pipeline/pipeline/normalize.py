"""
Unicode Normalisation for Sinhala Text
=======================================
One canonical form, applied to every string this project compares, stores,
or trains on.

WHY THIS MODULE EXISTS
----------------------
The same visible Sinhala text can be encoded as different codepoint
sequences. Four Sinhala vowel signs have canonical decompositions:

    ේ  U+0DDA  =  U+0DD9 + U+0DCA
    ො  U+0DDC  =  U+0DD9 + U+0DCF
    ෝ  U+0DDD  =  U+0DDC + U+0DCA   (so also U+0DD9 + U+0DCF + U+0DCA)
    ෞ  U+0DDE  =  U+0DD9 + U+0DDF

Different keyboards, input methods and OCR engines emit different forms.
Python compares them as unequal strings, so jiwer counts them as character
errors — meaning CER would measure *how the text was typed* rather than how
well the pipeline corrected it. Composing to NFC makes the two forms
identical.

THE GOVERNING RULE
------------------
This module canonicalises ENCODING. It never corrects ERRORS.

Anything that could plausibly be a genuine OCR mistake must survive
normalisation untouched, or the evaluation stops being able to see it.
That is why, deliberately:

  - runs of spaces are NOT collapsed   (Stage 1 exists to fix those)
  - the pipe/danda confusion is NOT fixed   (Stage 3 exists to fix that)
  - no character substitution of any kind happens here

If you ever feel tempted to "clean up" something in this file, that thing
belongs in a pipeline stage instead, where its effect is measured.

USED BY
-------
  - the evaluator, on both hypothesis and reference before scoring
  - gold-standard text as it is entered
  - training pair construction and the corruption script
  - anything that writes text to the database

Applying it in some places but not others is worse than not applying it at
all, because the mismatch is invisible in the output and shows up only as
inflated error rates.
"""

import unicodedata

# ─────────────────────────────────────────────────────────────
#  Policy constants — the decisions, in one reviewable place
# ─────────────────────────────────────────────────────────────

#: Composed form. Merges the decomposed kombuwa sequences shown above.
NORMALISATION_FORM = "NFC"

#: Semantically significant in Sinhala — PRESERVED, never stripped.
#: ZWNJ (U+200C) suppresses a ligature that would otherwise form;
#: ZWJ (U+200D) forms the bandi akuru (touching-letter) conjuncts.
#: Removing either changes what the text says, so both survive
#: normalisation. Stage 2's _restore_zwnj() exists because the LLM drops
#: ZWNJ; that is a pipeline concern, not a normalisation one.
MEANINGFUL_INVISIBLES = frozenset({
    "‌",  # ZERO WIDTH NON-JOINER
    "‍",  # ZERO WIDTH JOINER
})

#: Carry no meaning in Sinhala text and arrive via copy-paste, editors and
#: web pages. Removed, because leaving them in would block NFC composition
#: and register as phantom character errors.
NOISE_INVISIBLES = frozenset({
    "﻿",  # ZERO WIDTH NO-BREAK SPACE / BOM
    "​",  # ZERO WIDTH SPACE
    "‎",  # LEFT-TO-RIGHT MARK
    "‏",  # RIGHT-TO-LEFT MARK
    "⁠",  # WORD JOINER
    "­",  # SOFT HYPHEN
})

#: Spacing characters that are typographically distinct but textually a
#: plain space. Mapped to U+0020 so a non-breaking space typed in Word and
#: a normal space from OCR compare equal.
SPACE_VARIANTS = frozenset({
    " ",  # NO-BREAK SPACE
    " ", " ", " ", " ", " ", " ",
    " ", " ", " ", " ", " ",  # EN/EM/THIN etc.
    " ",  # NARROW NO-BREAK SPACE
    " ",  # MEDIUM MATHEMATICAL SPACE
    "　",  # IDEOGRAPHIC SPACE
    "\t",      # TAB
})


# ─────────────────────────────────────────────────────────────
#  The one function
# ─────────────────────────────────────────────────────────────

def normalize(text: str) -> str:
    """
    Return the canonical encoding of `text`.

    Steps, in an order that matters:
      1. Line endings   → \\n          (CRLF and CR both appear in OCR output)
      2. Space variants → U+0020
      3. Noise invisibles removed      (before composing, so they cannot
                                        block a canonical composition)
      4. Unicode NFC                   (composes the kombuwa sequences)
      5. Trailing whitespace stripped per line
      6. Leading/trailing blank lines stripped

    Line breaks *within* the text are preserved — they are meaningful in
    verse, and joining them is Stage 2's job, not this function's.

    Idempotent: normalize(normalize(x)) == normalize(x).
    """
    if not text:
        return ""

    # 1. Line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # 2 & 3. Character-level pass
    out = []
    for ch in text:
        if ch in NOISE_INVISIBLES:
            continue
        out.append(" " if ch in SPACE_VARIANTS else ch)
    text = "".join(out)

    # 4. Compose
    text = unicodedata.normalize(NORMALISATION_FORM, text)

    # 5. Trailing whitespace per line (invisible, never a meaningful
    #    difference to a reader, and a common artifact of hand-typing)
    text = "\n".join(line.rstrip(" ") for line in text.split("\n"))

    # 6. Blank lines at either end
    return text.strip("\n ")


# ─────────────────────────────────────────────────────────────
#  Diagnostics — for verifying gold-standard text by hand
# ─────────────────────────────────────────────────────────────

def inspect_invisibles(text: str) -> list:
    """
    List every invisible/format character in `text`, with its position.

    Useful when checking hand-typed gold references: invisible characters
    are impossible to see in an editor but change every metric. Returns a
    list of (index, 'U+XXXX', name, kept_or_removed) tuples.
    """
    found = []
    for i, ch in enumerate(text):
        if ch in MEANINGFUL_INVISIBLES:
            verdict = "kept (meaningful)"
        elif ch in NOISE_INVISIBLES:
            verdict = "removed (noise)"
        elif ch in SPACE_VARIANTS and ch != " ":
            verdict = "mapped to plain space"
        elif unicodedata.category(ch) == "Cf":
            verdict = "kept (unrecognised format char — review this)"
        else:
            continue
        found.append((i, f"U+{ord(ch):04X}", unicodedata.name(ch, "?"), verdict))
    return found


def describe_changes(text: str) -> list:
    """
    Explain what normalize() would change about `text`, as human-readable
    lines. Returns an empty list when the text is already canonical.

    Intended for spot-checking a sample of the evaluation set rather than
    for use in the pipeline.
    """
    changes = []
    normalised = normalize(text)
    if normalised == text:
        return changes

    if "\r" in text:
        changes.append("line endings normalised to \\n")

    composed = unicodedata.normalize("NFC", text)
    if composed != text:
        # Report the codepoint count that composition removed. A positional
        # diff would be misleading here: composing shifts every index after
        # the first change, so it reports far more differences than occurred.
        merged = len(text) - len(composed)
        detail = f"{merged} codepoint(s) merged" if merged else "reordered"
        changes.append(f"decomposed Sinhala vowel signs composed to NFC ({detail})")

    noise = [c for c in text if c in NOISE_INVISIBLES]
    if noise:
        names = ", ".join(sorted({f"U+{ord(c):04X}" for c in noise}))
        changes.append(f"{len(noise)} noise invisible(s) removed ({names})")

    variants = [c for c in text if c in SPACE_VARIANTS]
    if variants:
        changes.append(f"{len(variants)} space variant(s) mapped to U+0020")

    if any(line != line.rstrip(" ") for line in text.split("\n")):
        changes.append("trailing spaces stripped from line ends")

    if text.strip("\n ") != text:
        changes.append("leading/trailing blank lines stripped")

    return changes


# ─────────────────────────────────────────────────────────────
#  Self-check — run `python -m pipeline.normalize` to verify
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    # The case this module exists for: same word, two encodings.
    decomposed = "කොලඹ"   # ක + ෙ + ා + ල + ඹ
    composed = "කොලඹ"           # ක + ො + ල + ඹ

    print("Sinhala normalisation self-check")
    print("=" * 52)
    print(f"decomposed : {decomposed!r}")
    print(f"composed   : {composed!r}")
    print(f"equal as typed?      {decomposed == composed}")
    print(f"equal normalised?    {normalize(decomposed) == normalize(composed)}")
    print()

    # ZWNJ must survive; a BOM must not.
    with_zwnj = "﻿ම්‌ම"
    result = normalize(with_zwnj)
    print(f"ZWNJ preserved?      {chr(0x200C) in result}")
    print(f"BOM removed?         {chr(0xFEFF) not in result}")
    print()

    # Spaces are NOT collapsed — that is Stage 1's measurable job.
    spaced = "ක  ො ල  ඹ"
    print(f"spacing preserved?   {normalize(spaced) == spaced}")
    print(f"idempotent?          {normalize(normalize(spaced)) == normalize(spaced)}")
    print()

    messy = "﻿කොලඹ ම්‌ම  \r\n"
    print("describe_changes() on a messy sample:")
    for line in describe_changes(messy):
        print(f"  - {line}")
