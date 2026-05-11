"""
frontend/app.py
---------------
Streamlit prototype for the Sinhala Quality Pipeline (Component 4).

Run from the repo root with:
    streamlit run quality_pipeline/frontend/app.py

What this page does
-------------------
Two modes selectable from the sidebar:

1. Single Document — paste/select Sinhala text, see the full pipeline run
   (Unicode normalize → exact-hash dedup → morphology score) and inspect
   verdict, score, sub-features, and per-word decomposition.

2. Compare Two Documents — runs both through the same pipeline instance so
   the deduplicator can flag near/exact duplicates between them.

Both modes use *real* upstream-style Document objects flowing through the
*real* QualityPipeline — the frontend builds Documents from the textarea
input and feeds them through the same code path as the production pipeline.
No mock outputs.
"""

import sys
from pathlib import Path

# Make `quality_pipeline` importable when running `streamlit run` from repo root.
# Streamlit runs the script directly, not as a package module, so we need to
# add the repo root to sys.path. This is a one-off acceptable hack for entry
# points; the rest of the app uses normal package imports.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st

from quality_pipeline.linguistic.normalizer import UnicodeNormalizer
from quality_pipeline.pipeline import QualityPipeline
from quality_pipeline.quality.deduplicator import ExactHashDeduplicator
from quality_pipeline.quality.morphology import MorphologyQualityScorer
from quality_pipeline.schema import Document, Source, Verdict

from quality_pipeline.frontend.components.feature_chart import render_feature_chart
from quality_pipeline.frontend.components.score_display import (
    render_score_gauge,
    render_verdict_badge,
)
from quality_pipeline.frontend.components.stage_details import render_stage_details
from quality_pipeline.frontend.components.word_breakdown import render_word_breakdown


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Sinhala Quality Pipeline — Prototype",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Demo examples (curated for the supervisor demo)
# ---------------------------------------------------------------------------
# Each example is chosen to demonstrate a specific behaviour of the scorer.
# Keep these short — they're loaded into the textarea so the demo doesn't
# require typing Sinhala on the spot.

DEMO_EXAMPLES = {
    "📰 Clean news (Sinhala)": (
        "ශ්‍රී ලංකා ක්‍රිකට් කණ්ඩායම අද ජයග්‍රහණය ලබා ගත්තේය. තරගය "
        "කොළඹදී පැවැත්වුණි. ක්‍රීඩකයන් 11 දෙනා හොඳින් ක්‍රීඩා කළහ. "
        "ජනාධිපතිවරයා කණ්ඩායමට සුබපැතුම් එක් කළේය."
    ),
    "🧪 Same news with hidden junk (BOM + ZWSP + extra spaces)": (
        "\ufeffශ්‍රී ලංකා\u200b ක්‍රිකට්   කණ්ඩායම   අද\u200b ජයග්‍රහණය ලබා ගත්තේය. "
        "තරගය කොළඹදී    පැවැත්වුණි. ක්‍රීඩකයන් 11 දෙනා හොඳින් ක්‍රීඩා කළහ. "
        "ජනාධිපතිවරයා කණ්ඩායමට සුබපැතුම් එක් කළේය.\ufeff"
    ),
    "📚 OCR — clean book page": (
        "ශ්‍රී ලංකාවේ ඉතිහාසය පුරාණ රාජවංශ සමයේ සිට පටන් ගනී. "
        "අනුරාධපුරය එහි පළමු අග නගරය විය. නගරයේ සිට රජවරු පාලනය කළහ."
    ),
    "🎙️ ASR — disfluent speech": (
        "umm... ah... හරි... ඊළඟට... අපි... කොහොමද... හරි ඉතින්..."
    ),
    "🗑️ OCR garbage (low quality scan)": (
        "p@ge wi+h l0ts of OCR err0rs and garbage characters mixed in"
    ),
    "🌐 Mixed English / Sinhala": (
        "Mixed English with Sinhala සිංහල and numbers 12345 should also "
        "flow through pipeline for handling code-mixed content."
    ),
    "🔢 Math problem (low morphology by nature)": (
        "ගණිත අභ්‍යාසය: 12 + 8 = 20. 25 - 9 = 16. 6 x 7 = 42."
    ),
}

DEFAULT_KEY_LEFT = "📰 Clean news (Sinhala)"
DEFAULT_KEY_RIGHT = "🗑️ OCR garbage (low quality scan)"


# ---------------------------------------------------------------------------
# Pipeline construction
# ---------------------------------------------------------------------------
# Cached so we don't rebuild the stages on every interaction. The pipeline
# itself is stateful (the deduplicator remembers hashes), so we explicitly
# reset it before each run.

@st.cache_resource
def get_pipeline() -> QualityPipeline:
    return QualityPipeline(stages=[
        UnicodeNormalizer(),
        ExactHashDeduplicator(),
        MorphologyQualityScorer(),
    ])


def reset_pipeline_state(pipeline: QualityPipeline) -> None:
    """Reset stateful stages so each run is independent."""
    for stage in pipeline.stages:
        if hasattr(stage, "reset"):
            stage.reset()


def make_document(doc_id: str, text: str, source: Source = Source.NEWS) -> Document:
    """Build a Document object from raw text — same shape the loader produces."""
    return Document(
        doc_id=doc_id,
        source=source,
        raw_text=text,
        text=text,
        source_metadata={"origin": "frontend_input"},
    )


def finalize_verdict(doc: Document) -> None:
    """
    Promote PENDING → ACCEPT (placeholder, same logic as test_pipeline.py).
    Mirrors the production pipeline's behaviour so the frontend matches the
    Excel output for the same input.
    """
    if doc.verdict == Verdict.PENDING:
        doc.verdict = Verdict.ACCEPT
        doc.verdict_reasons.append("tentative_accept:no_rejection_signals")


# ---------------------------------------------------------------------------
# Sidebar — mode selection + about
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Pipeline Controls")

    mode = st.radio(
        "Mode",
        ["Single Document", "Compare Two Documents"],
        help="Single: full pipeline on one document. Compare: also runs the "
             "deduplicator across both documents to detect duplicates.",
    )

    st.markdown("---")
    st.markdown("### About")
    st.markdown(
        "**Component 4** — Sinhala NLP Engine and Unified Quality Pipeline.\n\n"
        "Pipeline stages currently active:\n"
        "1. **Unicode normalization** (NFC + invisible-char cleanup)\n"
        "2. **Exact-hash deduplication** (SHA-256 of cleaned text)\n"
        "3. **Morphology-aware quality scoring** ← *novelty*"
    )
    st.caption("All processing runs through the real pipeline — no mock outputs.")


# ---------------------------------------------------------------------------
# Main page header
# ---------------------------------------------------------------------------
st.title("🇱🇰 Sinhala Quality Pipeline — Prototype")
st.caption(
    "Paste Sinhala text or pick a demo example. The full pipeline runs end-to-end "
    "and shows the verdict, score breakdown, and per-word morphological analysis."
)


# ---------------------------------------------------------------------------
# Helpers — render one document's full result block
# ---------------------------------------------------------------------------
def render_document_result(doc: Document, header: str | None = None) -> None:
    """Render verdict + gauge + features + word breakdown for one document."""
    if header:
        st.subheader(header)

    render_verdict_badge(doc)

    # Two-column layout: gauge on left, features on right.
    gauge_col, features_col = st.columns([1, 1.3])
    with gauge_col:
        render_score_gauge(doc)
    with features_col:
        render_feature_chart(doc)

    with st.expander("🔍 Per-word morphological breakdown", expanded=False):
        render_word_breakdown(doc)

    with st.expander("⚙️ Pipeline stage details (Unicode, dedup, scoring)", expanded=False):
        render_stage_details(doc)


# ---------------------------------------------------------------------------
# MODE 1 — Single Document
# ---------------------------------------------------------------------------
if mode == "Single Document":
    st.markdown("### Input")

    example_key = st.selectbox(
        "Pick a demo example (or paste your own below):",
        list(DEMO_EXAMPLES.keys()),
        index=0,
    )
    text = st.text_area(
        "Sinhala text",
        value=DEMO_EXAMPLES[example_key],
        height=140,
        label_visibility="collapsed",
    )

    if st.button("Analyze", type="primary"):
        if not text.strip():
            st.warning("Please enter some text.")
        else:
            pipeline = get_pipeline()
            reset_pipeline_state(pipeline)

            doc = pipeline.run(make_document("frontend_001", text))
            finalize_verdict(doc)

            st.markdown("---")
            st.markdown("### Result")
            render_document_result(doc)


# ---------------------------------------------------------------------------
# MODE 2 — Compare Two Documents
# ---------------------------------------------------------------------------
else:
    st.markdown("### Input")
    st.caption(
        "Both documents flow through the same pipeline instance, so the "
        "deduplicator catches if Document B is a duplicate of Document A."
    )

    left, right = st.columns(2)

    with left:
        st.markdown("**Document A**")
        key_a = st.selectbox(
            "Pick example:",
            list(DEMO_EXAMPLES.keys()),
            index=list(DEMO_EXAMPLES.keys()).index(DEFAULT_KEY_LEFT),
            key="example_a",
        )
        text_a = st.text_area(
            "Document A text",
            value=DEMO_EXAMPLES[key_a],
            height=180,
            key="text_a",
            label_visibility="collapsed",
        )

    with right:
        st.markdown("**Document B**")
        key_b = st.selectbox(
            "Pick example:",
            list(DEMO_EXAMPLES.keys()),
            index=list(DEMO_EXAMPLES.keys()).index(DEFAULT_KEY_RIGHT),
            key="example_b",
        )
        text_b = st.text_area(
            "Document B text",
            value=DEMO_EXAMPLES[key_b],
            height=180,
            key="text_b",
            label_visibility="collapsed",
        )

    if st.button("Analyze Both", type="primary"):
        if not text_a.strip() or not text_b.strip():
            st.warning("Please enter text for both documents.")
        else:
            pipeline = get_pipeline()
            reset_pipeline_state(pipeline)

            doc_a = pipeline.run(make_document("frontend_A", text_a))
            doc_b = pipeline.run(make_document("frontend_B", text_b))
            finalize_verdict(doc_a)
            finalize_verdict(doc_b)

            st.markdown("---")

            # Highlight if B was caught as a duplicate of A.
            if doc_b.quality.get("is_duplicate") and doc_b.quality.get("duplicate_of") == doc_a.doc_id:
                st.error(
                    "🔁 **Document B was detected as an exact duplicate of "
                    "Document A** by the dedup stage (after normalization). "
                    "Try pasting the same text with different whitespace or "
                    "BOM characters into both — normalization + dedup catches that."
                )

            res_left, res_right = st.columns(2)
            with res_left:
                render_document_result(doc_a, header="Document A — Result")
            with res_right:
                render_document_result(doc_b, header="Document B — Result")