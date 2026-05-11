"""
frontend/components/stage_details.py
------------------------------------
Renders a per-stage breakdown of what the pipeline did to the document.

This makes the pipeline's internal work *visible* — without it, the
normalizer and deduplicator stages run silently and the user only sees
their effect on the morphology scorer's output. With this panel, the
user can point at exactly what each stage changed.
"""

import streamlit as st

from quality_pipeline.schema import Document


# Friendly names for invisible characters that may appear in raw text.
_INVISIBLE_NAMES = {
    "\u200b": "ZWSP (U+200B)",       # zero-width space — stripped
    "\u200c": "ZWNJ (U+200C)",       # zero-width non-joiner — preserved (Sinhala)
    "\u200d": "ZWJ (U+200D)",        # zero-width joiner — preserved (Sinhala ligatures)
    "\ufeff": "BOM (U+FEFF)",        # byte-order mark — stripped
}


def _describe_invisibles(text: str) -> list[tuple[str, int]]:
    """Return list of (friendly_name, count) for invisible chars present."""
    found = []
    for ch, name in _INVISIBLE_NAMES.items():
        count = text.count(ch)
        if count:
            found.append((name, count))
    return found


def _highlight_invisibles(text: str) -> str:
    """Replace invisible chars with visible markers for display."""
    return (
        text
        .replace("\u200b", "⟨ZWSP⟩")
        .replace("\u200c", "⟨ZWNJ⟩")
        .replace("\u200d", "⟨ZWJ⟩")
        .replace("\ufeff", "⟨BOM⟩")
    )


def render_stage_details(doc: Document) -> None:
    """Show what each pipeline stage did to the document."""
    st.markdown("### Pipeline stage details")
    st.caption(
        "What each stage actually changed. Useful for verifying the "
        "normalizer's Sinhala-aware behavior and seeing the dedup hash."
    )

    # ---- Stage 1: Unicode normalization ----
    with st.expander("Stage 1 — Unicode normalization", expanded=True):
        linguistic = doc.linguistic
        chars_removed = linguistic.get("chars_removed", 0)

        cols = st.columns(2)
        cols[0].metric("Characters removed", chars_removed)
        cols[1].metric(
            "Length change",
            f"{len(doc.raw_text)} → {len(doc.text)}",
        )

        # Show raw vs cleaned side by side, with invisibles made visible.
        raw_display = _highlight_invisibles(doc.raw_text)
        clean_display = _highlight_invisibles(doc.text)

        col_raw, col_clean = st.columns(2)
        with col_raw:
            st.markdown("**Raw input** (invisibles marked)")
            st.code(raw_display, language=None)
        with col_clean:
            st.markdown("**After normalization**")
            st.code(clean_display, language=None)

        # Itemise which invisible chars were in the raw input.
        raw_invisibles = _describe_invisibles(doc.raw_text)
        clean_invisibles = _describe_invisibles(doc.text)

        if raw_invisibles:
            st.markdown("**Invisible characters in raw text:**")
            for name, count in raw_invisibles:
                # Note which were kept vs stripped.
                kept = "ZWJ" in name or "ZWNJ" in name
                status = "✓ preserved (Sinhala ligature)" if kept else "✗ stripped"
                st.markdown(f"- {name} × {count} — {status}")
        else:
            st.caption("No invisible characters detected in raw input.")

        if clean_invisibles:
            kept_summary = ", ".join(f"{n} ({c})" for n, c in clean_invisibles)
            st.caption(f"Remaining after normalization: {kept_summary}")

        # Whitespace collapse indicator.
        raw_ws_runs = sum(
            1 for i in range(1, len(doc.raw_text))
            if doc.raw_text[i].isspace() and doc.raw_text[i-1].isspace()
        )
        if raw_ws_runs:
            st.caption(f"Whitespace runs collapsed: {raw_ws_runs} consecutive whitespace characters merged.")

    # ---- Stage 2: Exact-hash deduplication ----
    with st.expander("Stage 2 — Exact-hash deduplication", expanded=False):
        quality = doc.quality
        content_hash = quality.get("content_hash", "")
        is_duplicate = quality.get("is_duplicate", False)

        if content_hash:
            st.markdown(f"**Content hash (SHA-256):** `{content_hash[:32]}...`")
            st.caption(
                "Hash is computed on the *cleaned* text, not the raw input. "
                "Two documents that differ only in whitespace or invisible "
                "characters will produce the same hash after normalization."
            )

            if is_duplicate:
                dup_of = quality.get("duplicate_of", "?")
                st.error(f"🔁 This document was flagged as a duplicate of `{dup_of}`.")
            else:
                st.success("✓ No prior document with this hash — new content.")
        else:
            st.info("Deduplication stage did not run on this document.")

    # ---- Stage 3a: Morphology scoring (summary only — full detail elsewhere) ----
    with st.expander("Stage 3a — Morphology scoring summary", expanded=False):
        morph = doc.quality.get("morphology", {})
        features = morph.get("features", {})

        if not features:
            st.info("Morphology stage did not produce features.")
            return

        if not features.get("scoreable", False):
            st.warning(
                f"Document not scoreable: only {features.get('sinhala_words', 0)} "
                f"Sinhala words out of {features.get('total_words', 0)} total."
            )
            return

        st.markdown(f"**Final score:** {morph.get('score', 0):.1f} / 100")
        st.caption(
            "See the *Per-word morphological breakdown* and the bar chart "
            "above for the full feature contributions."
        )