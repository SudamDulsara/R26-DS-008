"""
STAGE 3: Contextual Linguistic Recovery via OpenRouter (Gemma 4 31B)
=====================================================================
Sends OCR-extracted Sinhala text to Gemma 4 31B via OpenRouter.

Requires: pip install openai
          OPENROUTER_API_KEY — key at openrouter.ai
"""

import os
from openai import OpenAI


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
4. Use full sentence context to decide correct word forms
5. If the text looks correct already, return it unchanged exactly"""


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
    steps = [("🔴 Input to Stage 3 (Gemma 4 31B)", text)]

    client = _get_client()
    if client is None:
        steps.append(("⚠️ Stage 3 skipped — OPENROUTER_API_KEY not set", text))
        return {"output": text, "steps": steps, "corrections": [], "changed": False}

    try:
        response = client.chat.completions.create(
            model="google/gemma-4-31b-it",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": f"Correct the following Sinhala OCR text:\n\n{text}"},
            ],
            temperature=0.1,
        )

        corrected   = response.choices[0].message.content.strip()
        corrections = _word_diff(text, corrected)

        steps.append(("✅ After Gemma 4 31B Contextual Recovery", corrected))

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
