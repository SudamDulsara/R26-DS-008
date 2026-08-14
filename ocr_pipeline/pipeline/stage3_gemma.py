"""
STAGE 3: Contextual Linguistic Recovery via OpenRouter (Gemma 4 31B)
=====================================================================
Sends OCR-extracted Sinhala text to Gemma 4 31B via OpenRouter.

Requires: pip install openai
          OPENROUTER_API_KEY — key at openrouter.ai
"""

import os
import re
from openai import OpenAI

ZWNJ = '‌'

# Single source of truth for the model. app.py imports MODEL_LABEL for its
# UI text so the interface can never drift from the model actually called.
MODEL_ID    = "google/gemma-4-31b-it"
MODEL_LABEL = "Gemma 4 31B"


SYSTEM_PROMPT = """\
You are a Sinhala language expert specializing in OCR post-correction.
Fix character-level errors in Sinhala text that was extracted by an OCR engine.

Common OCR errors in Sinhala:
- Visually similar character confusion: ල/ළ, ණ/න, ය/ව, ශ/ෂ, ද/ධ, ත/ථ, ස/ශ, බ/භ, ට/ඨ, ක/ඛ, ඉ/න, එ/ච
- Missing or broken diacritical marks (vowel signs attached to consonants)
- Spurious spaces inserted inside a single word
- Corrupted words that should be inferred from context (e.g. medical terms, common phrases)
- Wrong words that are valid Sinhala but don't fit the sentence context

Strict rules:
1. Return ONLY the corrected Sinhala text — no explanation, no commentary
2. Do NOT translate — keep the text in Sinhala
3. Preserve all punctuation, line breaks, and spacing between words
4. Preserve invisible Unicode characters exactly as they appear, including Zero-Width Non-Joiner (U+200C) after ් characters
5. Use full sentence context to decide correct word forms
6. If the text looks correct already, return it unchanged exactly"""


def _restore_zwnj(original: str, refined: str) -> str:
    """
    Re-insert ZWNJ characters that the LLM silently dropped.
    Builds a set of word stems that carried ZWNJ in the original,
    then re-appends ZWNJ to any matching word token in the refined text.
    Uses re.sub on non-whitespace tokens so all newlines and spacing are preserved.
    """
    zwnj_stems = {w[:-1] for w in original.split() if w.endswith(ZWNJ)}
    if not zwnj_stems:
        return refined

    def maybe_restore(match: re.Match) -> str:
        word = match.group(0)
        if word in zwnj_stems:
            return word + ZWNJ
        return word

    return re.sub(r'\S+', maybe_restore, refined)


def _get_client():
    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        return None
    return OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)


def _word_diff(original: str, corrected: str) -> list:
    orig_words = original.split()
    corr_words = corrected.split()
    return [(o, c) for o, c in zip(orig_words, corr_words) if o != c]


def stage3_gemini_contextual_recovery(text: str) -> dict:
    """
    Send text to Gemma 4 31B via OpenRouter for Sinhala OCR correction.

    Returns dict: {output, steps, corrections, changed}
    """
    steps = [(f"🔴 Input to Contextual Recovery ({MODEL_LABEL})", text)]

    client = _get_client()
    if client is None:
        steps.append(("⚠️ Stage 3 skipped — OPENROUTER_API_KEY not set", text))
        return {"output": text, "steps": steps, "corrections": [], "changed": False}

    try:
        response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": f"Correct the following Sinhala OCR text:\n\n{text}"},
            ],
            temperature=0.1,
        )

        corrected   = response.choices[0].message.content.strip()
        corrected   = _restore_zwnj(text, corrected)
        corrections = _word_diff(text, corrected)

        steps.append((f"✅ After {MODEL_LABEL} Contextual Recovery", corrected))

        for wrong, right in corrections:
            print(f"[Stage 3] '{wrong}' → '{right}'")

        return {
            "output":      corrected,
            "steps":       steps,
            "corrections": corrections,
            "changed":     corrected != text,
        }

    except Exception as e:
        print(f"[Stage 3] API call failed: {e}")
        steps.append((f"❌ Stage 3 failed — {e}", text))
        return {"output": text, "steps": steps, "corrections": [], "changed": False}
