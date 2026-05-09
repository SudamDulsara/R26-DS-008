"""
Sinhala OCR Refinement Pipeline — Proof of Concept
====================================================
Run with:  python -m streamlit run app.py
"""

import logging
import os
import warnings
from dotenv import load_dotenv
import streamlit as st
import pandas as pd

load_dotenv()

logging.getLogger("transformers").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

from pipeline.stage1_grapheme  import stage1_grapheme_refinement
from pipeline.stage3_gemma     import stage3_gemini_contextual_recovery
from pipeline.stage4_syntactic import stage4_syntactic_restoration
from pipeline.evaluator        import evaluate
from pipeline.ocr_engine       import run_ocr, check_sinhala_available, INSTALL_GUIDE, TESSERACT_AVAILABLE

# ──────────────────────────────────────────────
#  Page config
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="Sinhala OCR Refinement Pipeline",
    page_icon="🔤",
    layout="wide",
)

# ──────────────────────────────────────────────
#  Sidebar
# ──────────────────────────────────────────────
with st.sidebar:
    st.title("🔤 Pipeline Status")
    st.markdown("---")

    st.markdown("**OCR Engine**")
    if TESSERACT_AVAILABLE:
        sin_ok = check_sinhala_available()
        if sin_ok:
            st.markdown(":green[ Tesseract + Sinhala pack ready]")
        else:
            st.markdown(":orange[⚠️ Tesseract found — Sinhala pack missing]")
    else:
        st.markdown(":red[❌ Tesseract not installed]")

    st.markdown("")
    st.markdown("**HuggingFace API**")
    if os.environ.get("HF_API_KEY", "").strip():
        st.markdown(":green[HuggingFace key loaded from .env]")
    else:
        st.markdown(":orange[⚠️ HF_API_KEY missing in .env — Stage 3 will be skipped]")

    st.markdown("")
    st.markdown("**Refinement Stages**")

    stages = [
        ("Stage 1", "Structural Grapheme Refinement",  " Implemented"),
        ("Stage 2", "Groq Qwen3-32B LLM Recovery",    " Implemented"),
        ("Stage 3", "Syntactic Restoration",           " Implemented"),
    ]
    for stage_id, name, status in stages:
        st.markdown(f"**{stage_id}** — {name}  \n:green[{status}]")
        st.markdown("")

    st.markdown("---")
    st.caption("Prototype v0.4 — Research PoC")
    st.caption("IT22626288 — SLIIT Data Science")

# ──────────────────────────────────────────────
#  Header
# ──────────────────────────────────────────────
st.title("🔤 Sinhala Post-OCR Refinement Pipeline")
st.markdown(
    "**Proof of Concept** | Continuous Dataset Generation Pipeline for Sinhala-Based Model Training"
)
st.markdown("---")

# ──────────────────────────────────────────────
#  Input Mode Selection — only 2 modes now
# ──────────────────────────────────────────────
st.subheader("📥 Input")

input_mode = st.radio(
    "Choose input mode",
    [
        "  Upload image — run through OCR then pipeline",
        "   Type / paste your own OCR text",
    ],
    horizontal=False,
)

raw_ocr_text   = ""
gold_reference = ""
ocr_result     = None

# ══════════════════════════════════════════════
#  MODE 1 — Image Upload + OCR
# ══════════════════════════════════════════════
if "Upload image" in input_mode:

    if not TESSERACT_AVAILABLE:
        st.error("❌ Tesseract is not installed on this machine.")
        with st.expander("📖 How to install Tesseract + Sinhala pack"):
            st.markdown(INSTALL_GUIDE)
        st.stop()

    sinhala_ok = check_sinhala_available()
    if not sinhala_ok:
        st.warning(
            "⚠️ Tesseract is installed but the **Sinhala language pack** is missing. "
            "OCR will run in English mode — results will be inaccurate for Sinhala text."
        )
        with st.expander("📖 How to add Sinhala language pack"):
            st.markdown(INSTALL_GUIDE)

    # Language selector
    col_lang, col_empty = st.columns([2, 5])
    with col_lang:
        lang_choice = st.selectbox(
            "OCR Language",
            ["sin", "sin+eng", "eng"],
            index=0,
            help="'sin' = Sinhala only | 'sin+eng' = mixed Sinhala & English"
        )

    # File uploader
    uploaded_file = st.file_uploader(
        "Upload a scanned Sinhala document image",
        type=["jpg", "jpeg", "png", "bmp", "tiff", "webp"],
        help="Upload a photo or scan of a printed Sinhala document",
    )

    if uploaded_file:
        col_img, col_ocr = st.columns(2)

        with col_img:
            st.markdown("**📄 Uploaded Image**")
            st.image(uploaded_file, width='stretch')

        with col_ocr:
            st.markdown("**🔍 Running Tesseract OCR...**")
            with st.spinner("Extracting text from image..."):
                image_bytes = uploaded_file.read()
                ocr_result  = run_ocr(image_bytes, language=lang_choice)

            if ocr_result["success"]:
                st.success(
                    f"✅ OCR complete — Engine: {ocr_result['engine']} | "
                    f"Language: `{ocr_result['language_used']}`"
                )
                st.markdown("**Raw OCR Output (before refinement):**")
                st.code(ocr_result["raw_text"] or "(no text extracted)", language=None)
                raw_ocr_text = ocr_result["raw_text"]

                if not raw_ocr_text.strip():
                    st.warning(
                        "⚠️ OCR extracted no text. Try a clearer image or check "
                        "that the Sinhala language pack is installed."
                    )
            else:
                st.error(f"❌ OCR failed: {ocr_result['error']}")
                if "not installed" in ocr_result["error"].lower():
                    with st.expander("📖 Installation guide"):
                        st.markdown(INSTALL_GUIDE)

        gold_reference = st.text_area(
            "Gold standard reference (optional — for CER/WER evaluation)",
            height=80,
            placeholder="Type the correct Sinhala text here to measure improvement",
        )

# ══════════════════════════════════════════════
#  MODE 2 — Manual text input
# ══════════════════════════════════════════════
else:
    col1, col2 = st.columns(2)
    with col1:
        raw_ocr_text = st.text_area(
            "Paste raw OCR text (noisy Sinhala)",
            height=120,
            placeholder="e.g.  ක  ො ල  ඹ",
        )
    with col2:
        gold_reference = st.text_area(
            "Gold standard reference (correct Sinhala)",
            height=120,
            placeholder="e.g.  කොලඹ",
        )

# ──────────────────────────────────────────────
#  Run Pipeline Button
# ──────────────────────────────────────────────
st.markdown("---")
run_btn = st.button("▶️  Run Refinement Pipeline", type="primary", width='stretch')

if run_btn and raw_ocr_text.strip():

    st.markdown("---")
    st.subheader("⚙️ Pipeline Execution — Step by Step")

    # ── Stage 1 ──────────────────────────────
    with st.spinner("Running Stage 1 — Grapheme Refinement..."):
        s1 = stage1_grapheme_refinement(raw_ocr_text)

    with st.expander("📌 Stage 1 — Structural Grapheme Refinement  ✅", expanded=True):
        col_in, col_out = st.columns(2)
        with col_in:
            st.markdown(f"**{s1['steps'][0][0]}**")
            st.code(s1["steps"][0][1], language=None)
        with col_out:
            st.markdown(f"**{s1['steps'][-1][0]}**")
            st.code(s1["steps"][-1][1], language=None)
        if s1["changed"]:
            st.success("✅ Stage 1 complete — text modified")
        else:
            st.info("ℹ️ No grapheme errors found")

    # ── Stage 2 (Groq Qwen3-32B) ─────────────
    with st.spinner("Running Stage 2 — Groq Qwen3-32B Contextual Recovery..."):
        s3 = stage3_gemini_contextual_recovery(s1["output"])

    with st.expander("📌 Stage 2 — Gemma 4 31B Contextual Recovery  ✅", expanded=True):
        col_in, col_out = st.columns(2)
        with col_in:
            st.markdown(f"**{s3['steps'][0][0]}**")
            st.code(s3["steps"][0][1], language=None)
        with col_out:
            st.markdown(f"**{s3['steps'][-1][0]}**")
            st.code(s3["steps"][-1][1], language=None)
        if s3["changed"]:
            st.success(f"✅ {len(s3['corrections'])} correction(s) applied by Gemma 4 31B")
            for wrong, correct in s3["corrections"]:
                st.markdown(f"  • `{wrong}` → `{correct}`")
        else:
            st.info("ℹ️ No corrections made by Gemma 4 31B")

    # ── Stage 3 (Syntactic) ───────────────────
    with st.spinner("Running Stage 3 — Syntactic Restoration..."):
        s4 = stage4_syntactic_restoration(s3["output"])

    with st.expander("📌 Stage 3 — Syntactic Restoration  ✅", expanded=True):
        col_in, col_out = st.columns(2)
        with col_in:
            st.markdown(f"**{s4['steps'][0][0]}**")
            st.code(s4["steps"][0][1], language=None)
        with col_out:
            st.markdown(f"**{s4['steps'][-1][0]}**")
            st.code(s4["steps"][-1][1], language=None)
        if s4["changed"]:
            st.success("✅ Syntactic structure restored")
        else:
            st.info("ℹ️ No syntactic issues found")

    final_output = s4["output"]

    # ──────────────────────────────────────────
    #  Final Output
    # ──────────────────────────────────────────
    st.markdown("---")
    st.subheader("📤 Final Output")

    if ocr_result:
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**🔴 Raw OCR Output (Tesseract)**")
            st.code(raw_ocr_text, language=None)
        with col_b:
            st.markdown("**🟢 After Pipeline Refinement**")
            st.code(final_output, language=None)
    else:
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.markdown("**🔴 Raw OCR Input**")
            st.code(raw_ocr_text, language=None)
        with col_b:
            st.markdown("**🟢 Refined Output**")
            st.code(final_output, language=None)
        with col_c:
            st.markdown("**🏆 Gold Standard**")
            if gold_reference.strip():
                st.code(gold_reference, language=None)
            else:
                st.info("No gold reference provided")

    # ──────────────────────────────────────────
    #  Evaluation Metrics
    # ──────────────────────────────────────────
    if gold_reference.strip():
        st.markdown("---")
        st.subheader("📊 Evaluation Metrics")

        metrics = evaluate(raw_ocr_text, final_output, gold_reference)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("CER — Before", f"{metrics['cer_before']*100:.1f}%",
                      help="Character Error Rate of raw OCR vs. gold standard")
        with col2:
            st.metric("CER — After", f"{metrics['cer_after']*100:.1f}%",
                      delta=f"{-round(metrics['cer_improvement']*100, 1):.1f}%",
                      delta_color="inverse")
        with col3:
            st.metric("WER — Before", f"{metrics['wer_before']*100:.1f}%",
                      help="Word Error Rate of raw OCR vs. gold standard")
        with col4:
            st.metric("WER — After", f"{metrics['wer_after']*100:.1f}%",
                      delta=f"{-round(metrics['wer_improvement']*100, 1):.1f}%",
                      delta_color="inverse")

        st.markdown("#### Improvement Summary")
        df = pd.DataFrame({
            "Metric":            ["CER (Character Error Rate)", "WER (Word Error Rate)"],
            "Before Refinement": [f"{metrics['cer_before']*100:.1f}%",  f"{metrics['wer_before']*100:.1f}%"],
            "After Refinement":  [f"{metrics['cer_after']*100:.1f}%",   f"{metrics['wer_after']*100:.1f}%"],
            "Improvement":       [f"↓ {metrics['cer_improvement']*100:.1f}%", f"↓ {metrics['wer_improvement']*100:.1f}%"],
        })
        st.dataframe(df, width='stretch', hide_index=True)

elif run_btn:
    st.warning("Please provide input — upload an image or paste text.")

# ──────────────────────────────────────────────
#  Footer
# ──────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Prototype — Continuous Dataset Generation Pipeline for Sinhala-Based Model Training | "
    "SLIIT B.Sc. (Hons) IT — Data Science | IT22626288"
)