"""
frontend/app.py
---------------
Streamlit prototype for the Sinhala Quality Pipeline (Component 4).

Run from the repo root with:
    streamlit run quality_pipeline/frontend/app.py

Three modes selectable from the sidebar:

1. Single Document — pipeline runs on one document; verdict, score, and
   per-word breakdown. Stage 3b will show "no overlap" since there are
   no prior docs to compare against — that's expected.

2. Compare Two Documents — runs A then B through the SAME pipeline
   instance. Stage 2 catches byte-identical dupes; Stage 3b catches
   semantic ones. If they're same-source it's REJECT; if different-source
   it's REVIEW (cross-register overlap — the register-aware novelty).

3. Corpus overlap — add 3-6 documents from different sources, watch
   Stage 3b decide which are same-source dupes vs cross-register overlap.
   This is the mode that actually demonstrates the second novelty end-to-end.

Note: first pipeline run downloads LaBSE (~470MB). Subsequent runs use
the local cache and are fast.
"""

import sys
from pathlib import Path

# Make `quality_pipeline` importable when running `streamlit run` from repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st

from quality_pipeline.linguistic.normalizer import UnicodeNormalizer
from quality_pipeline.pipeline import QualityPipeline
from quality_pipeline.quality.deduplicator import ExactHashDeduplicator
from quality_pipeline.quality.morphology.scorer import MorphologyQualityScorer
from quality_pipeline.quality.semantic_overlap import CrossRegisterSemanticOverlap
from quality_pipeline.schema import Document, Source, Verdict

from quality_pipeline.frontend.components.corpus_overlap import render_corpus_overlap
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
# Demo examples
# ---------------------------------------------------------------------------
DEMO_EXAMPLES = {
    "📰 Clean news (Sinhala)": (
        "ශ්‍රී ලංකා ක්‍රිකට් කණ්ඩායම අද ජයග්‍රහණය ලබා ගත්තේය. තරගය "
        "කොළඹදී පැවැත්වුණි. ක්‍රීඩකයන් 11 දෙනා හොඳින් ක්‍රීඩා කළහ. "
        "ජනාධිපතිවරයා කණ්ඩායමට සුබපැතුම් එක් කළේය."
    ),
    "📰 Same news, paraphrased": (
        "ලංකා කණ්ඩායම අද කොළඹදී පැවති තරගයෙන් ජය ලැබීය. "
        "ක්‍රීඩකයන් සියල්ල හොඳින් ක්‍රීඩා කළහ. ජනාධිපතිවරයා ජයග්‍රාහී "
        "කණ්ඩායමට සුබපැතුම් පිරිනැමීය."
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
    "🎙️ ASR — same match, spoken register": (
        "අද කොළඹදී තිබ්බ තරගේ ලංකා කණ්ඩායම දිනුවා. ක්‍රීඩකයෝ 11ම හොඳට "
        "ක්‍රීඩා කරා. ජනාධිපතිතුමා ඒ අයට සුබපැතුම් කිව්වා."
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

# Which examples represent which sources (for the corpus mode default sources).
EXAMPLE_SOURCE_HINT = {
    "📰 Clean news (Sinhala)": Source.NEWS,
    "📰 Same news, paraphrased": Source.NEWS,
    "🧪 Same news with hidden junk (BOM + ZWSP + extra spaces)": Source.NEWS,
    "📚 OCR — clean book page": Source.OCR,
    "🎙️ ASR — disfluent speech": Source.ASR,
    "🎙️ ASR — same match, spoken register": Source.ASR,
    "🗑️ OCR garbage (low quality scan)": Source.OCR,
    "🌐 Mixed English / Sinhala": Source.NEWS,
    "🔢 Math problem (low morphology by nature)": Source.OCR,
}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
# Cached so we don't rebuild the stages on every interaction. LaBSE download
# happens on first embedding call; the pipeline object holds the loaded model.

@st.cache_resource
def get_pipeline() -> QualityPipeline:
    return QualityPipeline(stages=[
        UnicodeNormalizer(),
        ExactHashDeduplicator(),
        MorphologyQualityScorer(),
        CrossRegisterSemanticOverlap(),
    ])


def reset_pipeline_state(pipeline: QualityPipeline) -> None:
    """Reset stateful stages so each run is independent."""
    for stage in pipeline.stages:
        if hasattr(stage, "reset"):
            stage.reset()


def make_document(doc_id: str, text: str, source: Source = Source.NEWS) -> Document:
    return Document(
        doc_id=doc_id,
        source=source,
        raw_text=text,
        text=text,
        source_metadata={"origin": "frontend_input"},
    )


def finalize_verdict(doc: Document) -> None:
    if doc.verdict == Verdict.PENDING:
        doc.verdict = Verdict.ACCEPT
        doc.verdict_reasons.append("tentative_accept:no_rejection_signals")


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Pipeline Controls")

    mode = st.radio(
        "Mode",
        ["Single Document", "Compare Two Documents", "Corpus overlap"],
        help=(
            "Single: full pipeline on one document.\n"
            "Compare: A vs B through the same pipeline instance.\n"
            "Corpus overlap: 3-6 docs from mixed sources — best mode for "
            "demonstrating the cross-register (Stage 3b) novelty."
        ),
    )

    st.markdown("---")
    st.markdown("### About")
    st.markdown(
        "**Component 4** — Sinhala NLP Engine and Unified Quality Pipeline.\n\n"
        "Pipeline stages currently active:\n"
        "1. **Unicode normalization** (NFC + invisible-char cleanup)\n"
        "2. **Exact-hash deduplication** (SHA-256 of cleaned text)\n"
        "3. **Morphology-aware quality scoring** ← *novelty 1*\n"
        "4. **Cross-register semantic overlap** ← *novelty 2*"
    )
    st.caption("First run downloads LaBSE (~470MB). Cached after that.")


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🇱🇰 Sinhala Quality Pipeline — Prototype")
st.caption(
    "Paste Sinhala text or pick a demo example. The full pipeline runs "
    "end-to-end — no mock outputs."
)


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------
def render_document_result(doc: Document, header: str | None = None) -> None:
    if header:
        st.subheader(header)

    render_verdict_badge(doc)

    gauge_col, features_col = st.columns([1, 1.3])
    with gauge_col:
        render_score_gauge(doc)
    with features_col:
        render_feature_chart(doc)

    with st.expander("🔍 Per-word morphological breakdown", expanded=False):
        render_word_breakdown(doc)

    with st.expander("⚙️ Pipeline stage details (Unicode, dedup, scoring, overlap)", expanded=False):
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
            st.caption(
                "Note: in single-doc mode, Stage 3b has no prior documents to compare "
                "against — expected to show 'no overlap'. Try Corpus mode to see it work."
            )
            render_document_result(doc)


# ---------------------------------------------------------------------------
# MODE 2 — Compare Two Documents
# ---------------------------------------------------------------------------
elif mode == "Compare Two Documents":
    st.markdown("### Input")
    st.caption(
        "Both documents flow through the same pipeline instance. The dedup stage "
        "catches byte-identical B; the semantic-overlap stage catches paraphrases."
    )

    left, right = st.columns(2)

    with left:
        st.markdown("**Document A**")
        key_a = st.selectbox(
            "Pick example:",
            list(DEMO_EXAMPLES.keys()),
            index=0,
            key="example_a",
        )
        src_a = st.selectbox(
            "Source (A):",
            [Source.NEWS.value, Source.OCR.value, Source.ASR.value],
            index=[Source.NEWS.value, Source.OCR.value, Source.ASR.value].index(
                EXAMPLE_SOURCE_HINT.get(key_a, Source.NEWS).value
            ),
            key="src_a",
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
            index=1,  # default to the paraphrased news
            key="example_b",
        )
        src_b = st.selectbox(
            "Source (B):",
            [Source.NEWS.value, Source.OCR.value, Source.ASR.value],
            index=[Source.NEWS.value, Source.OCR.value, Source.ASR.value].index(
                EXAMPLE_SOURCE_HINT.get(key_b, Source.NEWS).value
            ),
            key="src_b",
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

            doc_a = pipeline.run(make_document("frontend_A", text_a, Source(src_a)))
            doc_b = pipeline.run(make_document("frontend_B", text_b, Source(src_b)))
            finalize_verdict(doc_a)
            finalize_verdict(doc_b)

            st.markdown("---")

            # Exact-dup callout (Stage 2)
            if doc_b.quality.get("is_duplicate") and doc_b.quality.get("duplicate_of") == doc_a.doc_id:
                st.error(
                    "🔁 **Document B was detected as an exact duplicate of Document A** "
                    "by the exact-hash dedup stage (after normalization)."
                )

            # Semantic-overlap callout (Stage 3b) — this is the new bit
            ov_b = doc_b.quality.get("semantic_overlap", {})
            if ov_b.get("decision") == "reject_same_source":
                st.error(
                    f"🔁 **Semantic near-duplicate rejected.** Similarity "
                    f"{ov_b['best_match_score']:.3f} ≥ threshold, and both are "
                    f"`{src_a}` → treated as a same-source duplicate."
                )
            elif ov_b.get("decision") == "review_cross_register":
                st.warning(
                    f"⚠️ **Cross-register overlap detected.** Similarity "
                    f"{ov_b['best_match_score']:.3f} ≥ threshold, but sources differ "
                    f"(`{src_a}` → `{src_b}`) → routed to REVIEW, not REJECT. "
                    f"Same content in different registers may be valuable training data."
                )

            res_left, res_right = st.columns(2)
            with res_left:
                render_document_result(doc_a, header="Document A — Result")
            with res_right:
                render_document_result(doc_b, header="Document B — Result")


# ---------------------------------------------------------------------------
# MODE 3 — Corpus overlap (NEW)
# ---------------------------------------------------------------------------
else:
    st.markdown("### Input — build a small corpus")
    st.caption(
        "Add 3-6 documents. Vary the sources to see the register-aware behaviour of Stage 3b. "
        "Same-source semantic dupes are rejected; cross-source overlap is routed to review."
    )

    # Persist the corpus in session state so it survives between clicks.
    if "corpus_entries" not in st.session_state:
        # Preload a demo corpus that shows both novelty behaviours:
        #  - news_1 + news_2 = paraphrases from same source → REJECT
        #  - news_1 + asr_1  = same event, different register → REVIEW
        #  - ocr_1           = distinct content → ACCEPT
        st.session_state.corpus_entries = [
            {"id": "news_1", "source": "news", "text": DEMO_EXAMPLES["📰 Clean news (Sinhala)"]},
            {"id": "news_2", "source": "news", "text": DEMO_EXAMPLES["📰 Same news, paraphrased"]},
            {"id": "asr_1",  "source": "asr",  "text": DEMO_EXAMPLES["🎙️ ASR — same match, spoken register"]},
            {"id": "ocr_1",  "source": "ocr",  "text": DEMO_EXAMPLES["📚 OCR — clean book page"]},
        ]

    # Render editable rows for the corpus.
    st.markdown("**Corpus documents** (edit / add / remove below)")
    new_entries = []
    for i, entry in enumerate(st.session_state.corpus_entries):
        with st.container(border=True):
            cols = st.columns([1, 1, 4, 1])
            with cols[0]:
                doc_id = st.text_input("ID", value=entry["id"], key=f"id_{i}")
            with cols[1]:
                src = st.selectbox(
                    "Source",
                    ["news", "ocr", "asr"],
                    index=["news", "ocr", "asr"].index(entry["source"]),
                    key=f"src_{i}",
                )
            with cols[2]:
                text = st.text_area(
                    "Text", value=entry["text"], height=68, key=f"text_{i}",
                    label_visibility="collapsed",
                )
            with cols[3]:
                remove = st.button("🗑️", key=f"del_{i}", help="Remove this document")
            if not remove:
                new_entries.append({"id": doc_id, "source": src, "text": text})

    st.session_state.corpus_entries = new_entries

    add_col, run_col = st.columns([1, 3])
    with add_col:
        if st.button("+ Add document"):
            st.session_state.corpus_entries.append(
                {"id": f"doc_{len(st.session_state.corpus_entries)+1}",
                 "source": "news", "text": ""}
            )
            st.rerun()
    with run_col:
        run = st.button("Run pipeline on corpus", type="primary")

    if run:
        entries = [e for e in st.session_state.corpus_entries if e["text"].strip()]
        if len(entries) < 2:
            st.warning("Add at least 2 documents to see overlap behaviour.")
        else:
            pipeline = get_pipeline()
            reset_pipeline_state(pipeline)

            docs = [
                make_document(e["id"], e["text"], Source(e["source"]))
                for e in entries
            ]
            processed = pipeline.run_batch(docs)
            for d in processed:
                finalize_verdict(d)

            st.markdown("---")
            render_corpus_overlap(processed)

            # Also let the user drill into any single doc's full detail.
            st.markdown("### Per-document detail")
            selected_id = st.selectbox(
                "Inspect a specific document:",
                [d.doc_id for d in processed],
            )
            selected = next(d for d in processed if d.doc_id == selected_id)
            render_document_result(selected)