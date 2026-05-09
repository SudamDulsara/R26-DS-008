"""
frontend/components/word_breakdown.py
-------------------------------------
Renders the per-word morphological breakdown table.

This is the "killer slide" of the demo — for each token in the input, it
shows the identified root, the suffix stack with category labels, and
whether the analyzer succeeded in decomposing the word.

It's what makes the score *explainable* rather than just *displayable*.
"""

import pandas as pd
import streamlit as st

from quality_pipeline.schema import Document


# Pretty labels and short codes for category tags shown next to each suffix.
_CATEGORY_TAG = {
    "case":     "CASE",
    "plural":   "PL",
    "definite": "DEF",
    "verb":     "VERB",
    "clitic":   "CLT",
}


def _format_suffixes(suffixes: list) -> str:
    """Render suffix stack as 'සුf₁[CASE] + සුf₂[CLT]'. Empty if none."""
    if not suffixes:
        return "—"
    return " + ".join(f"{s}[{_CATEGORY_TAG.get(c, c.upper())}]" for s, c in suffixes)


def _status(word: dict) -> str:
    if not word["is_sinhala"]:
        return "non-Sinhala"
    return "decomposed" if word["decomposed"] else "no suffix found"


def render_word_breakdown(doc: Document) -> None:
    """Per-word table of decompositions, with summary stats above."""
    morph = doc.quality.get("morphology", {})
    per_word = morph.get("per_word", [])

    if not per_word:
        st.info("No words to analyze.")
        return

    df = pd.DataFrame([
        {
            "Word":     w["word"],
            "Root":     w["root"],
            "Suffixes": _format_suffixes(w["suffixes"]),
            "Status":   _status(w),
        }
        for w in per_word
    ])

    # Quick summary above the table.
    total = len(per_word)
    decomposed = sum(1 for w in per_word if w["decomposed"])
    non_sinhala = sum(1 for w in per_word if not w["is_sinhala"])

    cols = st.columns(3)
    cols[0].metric("Total tokens", total)
    cols[1].metric("Decomposed", f"{decomposed} ({100*decomposed/total:.0f}%)" if total else "0")
    cols[2].metric("Non-Sinhala", non_sinhala)

    # The table. Streamlit's dataframe gives sortable columns for free.
    st.dataframe(df, use_container_width=True, hide_index=True, height=380)

    # Top-suffixes summary, useful for pointing out repetition in degraded text.
    top = morph.get("features", {}).get("top_suffixes", [])
    if top:
        st.caption("Most frequent suffixes: " + ", ".join(f"{s} ({n})" for s, n in top))