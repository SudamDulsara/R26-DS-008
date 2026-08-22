"""
frontend/components/stage_details.py
------------------------------------
Renders a per-stage breakdown of what the pipeline did to the document.

Adds a Stage 3b (semantic overlap) panel that shows:
  - whether the doc was embedded (or skipped, and why)
  - the best-matching prior document and its similarity score
  - the register-aware decision (no overlap / reject same-source /
    review cross-register)

Note: for single-doc mode, there IS no prior document, so 3b always shows
'no overlap' — that's expected. The panel is most informative in compare
mode and in the new Corpus overlap mode.
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


# ---------- Individual stage renderers ----------

def _render_stage_1(doc: Document) -> None:
    """Stage 1 — Unicode normalization."""
    chars_removed = doc.linguistic.get("chars_removed", 0)

    cols = st.columns(2)
    cols[0].metric("Characters removed", chars_removed)
    cols[1].metric(
        "Length change",
        f"{len(doc.raw_text)} → {len(doc.text)}",
    )

    raw_display = _highlight_invisibles(doc.raw_text)
    clean_display = _highlight_invisibles(doc.text)

    col_raw, col_clean = st.columns(2)
    with col_raw:
        st.markdown("**Raw input** (invisibles marked)")
        st.code(raw_display, language=None)
    with col_clean:
        st.markdown("**After normalization**")
        st.code(clean_display, language=None)

    raw_invisibles = _describe_invisibles(doc.raw_text)
    clean_invisibles = _describe_invisibles(doc.text)

    if raw_invisibles:
        st.markdown("**Invisible characters in raw text:**")
        for name, count in raw_invisibles:
            kept = "ZWJ" in name or "ZWNJ" in name
            status = "✓ preserved (Sinhala ligature)" if kept else "✗ stripped"
            st.markdown(f"- {name} × {count} — {status}")
    else:
        st.caption("No invisible characters detected in raw input.")

    if clean_invisibles:
        kept_summary = ", ".join(f"{n} ({c})" for n, c in clean_invisibles)
        st.caption(f"Remaining after normalization: {kept_summary}")

    raw_ws_runs = sum(
        1 for i in range(1, len(doc.raw_text))
        if doc.raw_text[i].isspace() and doc.raw_text[i-1].isspace()
    )
    if raw_ws_runs:
        st.caption(f"Whitespace runs collapsed: {raw_ws_runs} consecutive whitespace characters merged.")


def _render_stage_2(doc: Document) -> None:
    """Stage 2 — Exact-hash deduplication."""
    quality = doc.quality
    content_hash = quality.get("content_hash", "")
    is_duplicate = quality.get("is_duplicate", False)

    if not content_hash:
        st.info("Deduplication stage did not run on this document.")
        return

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


def _render_stage_3a(doc: Document) -> None:
    """Stage 3a — Morphology scoring summary."""
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


def _render_stage_3b(doc: Document) -> None:
    """Stage 3b — Cross-register semantic overlap."""
    ov = doc.quality.get("semantic_overlap", {})

    if not ov:
        st.info(
            "Semantic overlap stage did not run on this document. "
            "(Not wired into the current pipeline, or the doc was rejected earlier.)"
        )
        return

    # Handle skipped cases first.
    if ov.get("skipped_reason"):
        reason = ov["skipped_reason"]
        if "already_rejected" in reason:
            st.info(
                "Skipped: this document was already rejected by an earlier stage, "
                "so it wasn't embedded or stored."
            )
        elif "too_short" in reason:
            st.warning(
                f"Skipped: document is too short to embed reliably ({reason}). "
                "Very short strings produce unstable embeddings."
            )
        else:
            st.info(f"Skipped: {reason}")
        return

    # Embedded case — show the comparison result.
    threshold = ov.get("threshold", 0.85)
    best_id = ov.get("best_match_id")
    best_src = ov.get("best_match_source")
    best_score = ov.get("best_match_score")
    decision = ov.get("decision")

    st.markdown(f"**Embedded:** yes, compared against all previously-seen documents.")
    st.caption(f"Similarity threshold for flagging overlap: **{threshold}**")

    if best_id is None:
        st.info(
            "No prior documents to compare against. In single-doc mode this is expected — "
            "try compare mode or corpus mode to see Stage 3b in action."
        )
        return

    # Show best-match result and decision.
    cols = st.columns(3)
    cols[0].metric("Best match", best_id)
    cols[1].metric("Source", best_src)
    cols[2].metric("Similarity", f"{best_score:.3f}")

    if decision == "reject_same_source":
        st.error(
            f"🔁 **Near-duplicate rejected.** Similarity {best_score:.3f} ≥ threshold {threshold} "
            f"and both documents come from the same source (`{best_src}`). "
            f"Treated as a semantic duplicate — the corpus doesn't need both."
        )
    elif decision == "review_cross_register":
        st.warning(
            f"⚠️ **Cross-register overlap routed to REVIEW.** Similarity {best_score:.3f} ≥ threshold, "
            f"but the source is different (`{best_src}` → `{doc.source.value}`). "
            f"The same content in a different register may be valuable training data — "
            f"a human should decide, not the pipeline."
        )
    else:  # no_overlap
        st.success(
            f"✓ **No overlap detected.** Best similarity is {best_score:.3f}, "
            f"below the {threshold} threshold. Document is semantically distinct from prior docs."
        )


# ---------- Public entry point ----------

def render_stage_details(doc: Document) -> None:
    """Show what each pipeline stage did to the document."""
    st.markdown("### Pipeline stage details")
    st.caption(
        "What each stage actually changed. Useful for verifying the "
        "normalizer's Sinhala-aware behavior, seeing the dedup hash, and "
        "inspecting the semantic overlap decision."
    )

    with st.expander("Stage 1 — Unicode normalization", expanded=True):
        _render_stage_1(doc)

    with st.expander("Stage 2 — Exact-hash deduplication", expanded=False):
        _render_stage_2(doc)

    with st.expander("Stage 3a — Morphology scoring summary", expanded=False):
        _render_stage_3a(doc)

    with st.expander("Stage 3b — Cross-register semantic overlap", expanded=False):
        _render_stage_3b(doc)