"""
linguistic/normalizer.py
------------------------
Stage 1, minimal version: Unicode normalization.

Why this matters for Sinhala specifically
-----------------------------------------
Sinhala characters can be encoded multiple ways in Unicode. The same visible
character (say, ක් with a virama) may appear as:
  - a single precomposed codepoint, OR
  - a base character + combining mark

These look identical on screen but are DIFFERENT strings to a computer.
If we don't normalize, deduplication will miss obvious duplicates, and the
tokenizer will learn two different tokens for what a human reads as one letter.

NFC (Canonical Composition) is the standard choice: it composes characters
into their shortest canonical form. It's deterministic, lossless, and every
downstream NLP tool expects it.

This is a  tiny stage. Everything fancy comes later.
"""

import unicodedata

from ..schema import Document
from ..stages.base import Stage


class UnicodeNormalizer(Stage):
    """Normalize text to Unicode NFC form and strip zero-width junk."""

    name = "linguistic.unicode_normalize"

    # Invisible characters that sneak in from copy-paste, OCR, and scraped HTML.
    # They confuse tokenizers and break deduplication without any visible effect.
    #
    # IMPORTANT: we do NOT strip U+200D (ZWJ) because Sinhala uses it
    # legitimately for rakaransaya ("ශ්‍ර") and yansaya ("ශ්‍ය") ligatures.
    # Stripping it changes the visible character. U+200C (ZWNJ) is kept for
    # the same reason — safer to leave alone until we have a Sinhala-aware rule.
    INVISIBLE_CHARS = {
        "\u200b",  # zero-width space (safe to strip)
        "\ufeff",  # BOM / zero-width no-break space (safe to strip)
    }

    def _process(self, doc: Document) -> Document:
        original = doc.text

        # Step 1: NFC normalize
        normalized = unicodedata.normalize("NFC", original)

        # Step 2: strip invisible characters
        cleaned = "".join(c for c in normalized if c not in self.INVISIBLE_CHARS)

        # Step 3: collapse runs of whitespace (common OCR/scrape artifact)
        cleaned = " ".join(cleaned.split())

        # Record what we did so downstream stages/debugging can see it
        doc.text = cleaned
        doc.linguistic["unicode_normalized"] = True
        doc.linguistic["chars_removed"] = len(original) - len(cleaned)

        return doc