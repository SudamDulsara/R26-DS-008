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
from pipeline.stage3_gemma     import stage3_gemini_contextual_recovery, MODEL_LABEL
from pipeline.stage4_syntactic import stage4_syntactic_restoration
from pipeline.evaluator        import evaluate
from pipeline.ocr_engine       import run_ocr, check_sinhala_available, INSTALL_GUIDE, TESSERACT_AVAILABLE
from pipeline.database         import init_db, save_run, fetch_runs, count_runs
from pipeline.normalize        import normalize

# Identifies which model produced Stage 2's output, so saved runs can be
# compared later when the fine-tuned model is added alongside the API.
CORRECTION_METHOD = "gemma_api"

# Create the SQLite table if it doesn't exist yet (safe on every startup)
try:
    init_db()
    DB_READY = True
except Exception as _db_err:
    print(f"[app] Database init failed: {_db_err}")
    DB_READY = False

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
    st.markdown("**LLM API (OpenRouter)**")
    if os.environ.get("OPENROUTER_API_KEY", "").strip():
        st.markdown(":green[OpenRouter key loaded from .env]")
    else:
        st.markdown(":orange[⚠️ OPENROUTER_API_KEY missing in .env — Stage 2 will be skipped]")

    st.markdown("")
    st.markdown("**Dataset Storage**")
    if DB_READY:
        try:
            st.markdown(f":green[SQLite ready — {count_runs()} run(s) saved]")
        except Exception:
            st.markdown(":orange[⚠️ SQLite table not readable]")
    else:
        st.markdown(":red[❌ SQLite unavailable — runs will not be saved]")

    st.markdown("")
    st.markdown("**Refinement Stages**")

    stages = [
        ("Stage 1", "Structural Grapheme Refinement",              " Implemented"),
        ("Stage 2", f"{MODEL_LABEL} Contextual Recovery",          " Implemented"),
        ("Stage 3", "Syntactic Restoration",                       " Implemented"),
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

raw_ocr_text    = ""
gold_reference  = ""
ocr_result      = None
source_filename = None

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
        source_filename = uploaded_file.name
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

    # Canonicalise the encoding of both inputs before anything else touches
    # them, so every stage and every metric sees one consistent form. This
    # does not correct any OCR error — see pipeline/normalize.py.
    raw_ocr_text   = normalize(raw_ocr_text)
    gold_reference = normalize(gold_reference)

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

    # ── Stage 2 (LLM contextual recovery) ────
    with st.spinner(f"Running Stage 2 — {MODEL_LABEL} Contextual Recovery..."):
        s3 = stage3_gemini_contextual_recovery(s1["output"])

    with st.expander(f"📌 Stage 2 — {MODEL_LABEL} Contextual Recovery  ✅", expanded=True):
        col_in, col_out = st.columns(2)
        with col_in:
            st.markdown(f"**{s3['steps'][0][0]}**")
            st.code(s3["steps"][0][1], language=None)
        with col_out:
            st.markdown(f"**{s3['steps'][-1][0]}**")
            st.code(s3["steps"][-1][1], language=None)
        if s3["changed"]:
            st.success(f"✅ {len(s3['corrections'])} correction(s) applied by {MODEL_LABEL}")
            for wrong, correct in s3["corrections"]:
                st.markdown(f"  • `{wrong}` → `{correct}`")
        else:
            st.info(f"ℹ️ No corrections made by {MODEL_LABEL}")

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

    # ── Persist this run to SQLite ────────────
    # Computed here (not in the render block below) so the run is saved
    # exactly once per button press rather than on every Streamlit rerun.
    run_metrics = evaluate(raw_ocr_text, s4["output"], gold_reference) \
        if gold_reference.strip() else None

    try:
        run_id = save_run(
            raw_text          = raw_ocr_text,
            stage1_output     = s1["output"],
            stage2_output     = s3["output"],
            final_output      = s4["output"],
            correction_method = CORRECTION_METHOD,
            input_mode        = "image" if ocr_result else "text",
            source_filename   = source_filename,
            ocr_language      = (ocr_result or {}).get("language_used"),
            gold_reference    = gold_reference,
            metrics           = run_metrics,
        )
        st.success(f"💾 Run saved to SQLite database — record #{run_id}")
    except Exception as db_err:
        # A storage failure must never break the pipeline output
        st.warning(f"⚠️ Could not save run to database: {db_err}")

    st.session_state['pipeline_results'] = {
        'raw_ocr_text':   raw_ocr_text,
        'gold_reference': gold_reference,
        'ocr_result':     ocr_result,
        'final_output':   s4["output"],
        'metrics':        run_metrics,
    }

elif run_btn:
    st.warning("Please provide input — upload an image or paste text.")

# ──────────────────────────────────────────────
#  Final Output + Download + Metrics
#  Rendered from session_state so download button
#  click does not re-run the pipeline
# ──────────────────────────────────────────────
if 'pipeline_results' in st.session_state:
    res          = st.session_state['pipeline_results']
    final_output = res['final_output']
    _raw         = res['raw_ocr_text']
    _gold        = res['gold_reference']
    _ocr         = res['ocr_result']

    st.markdown("---")
    st.subheader("📤 Final Output")

    if _ocr:
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**🔴 Raw OCR Output (Tesseract)**")
            st.code(_raw, language=None)
        with col_b:
            st.markdown("**🟢 After Pipeline Refinement**")
            st.code(final_output, language=None)
    else:
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.markdown("**🔴 Raw OCR Input**")
            st.code(_raw, language=None)
        with col_b:
            st.markdown("**🟢 Refined Output**")
            st.code(final_output, language=None)
        with col_c:
            st.markdown("**🏆 Gold Standard**")
            if _gold.strip():
                st.code(_gold, language=None)
            else:
                st.info("No gold reference provided")

    st.download_button(
        label="⬇️ Download Refined Output",
        data=final_output.encode("utf-8"),
        file_name="refined_output.txt",
        mime="text/plain",
    )

    if _gold.strip():
        st.markdown("---")
        st.subheader("📊 Evaluation Metrics")

        metrics = res.get('metrics') or evaluate(_raw, final_output, _gold)

        def metric_card(title, subtitle, value, delta=None, delta_inverse=False):
            if delta is not None:
                val = float(delta.replace('%', ''))
                is_good = val < 0 if delta_inverse else val > 0
                color = "#28a745" if is_good else "#dc3545"
                arrow = "&#8595;" if val < 0 else "&#8593;"
                delta_html = f"<p style='color:{color};font-size:0.9em;margin:4px 0 0 0;'>{arrow} {abs(val):.1f}%</p>"
            else:
                delta_html = ""
            st.markdown(f"""
            <div style='text-align:left;'>
                <p style='font-size:0.85em;font-weight:600;color:gray;margin:0;'>{title}</p>
                <p style='font-size:0.75em;color:#9a9a9a;margin:2px 0 6px 0;'>{subtitle}</p>
                <p style='font-size:2em;font-weight:700;margin:0;'>{value}</p>
                {delta_html}
            </div>
            """, unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            metric_card("CER — Before", "Character Error Rate of raw OCR vs. gold standard",
                        f"{metrics['cer_before']*100:.1f}%")
        with col2:
            metric_card("CER — After", "Character Error Rate of refined output vs. gold standard",
                        f"{metrics['cer_after']*100:.1f}%",
                        delta=f"{-round(metrics['cer_improvement']*100, 1):.1f}%", delta_inverse=True)
        with col3:
            metric_card("WER — Before", "Word Error Rate of raw OCR vs. gold standard",
                        f"{metrics['wer_before']*100:.1f}%")
        with col4:
            metric_card("WER — After", "Word Error Rate of refined output vs. gold standard",
                        f"{metrics['wer_after']*100:.1f}%",
                        delta=f"{-round(metrics['wer_improvement']*100, 1):.1f}%", delta_inverse=True)

        st.markdown("#### Improvement Summary")
        df = pd.DataFrame({
            "Metric":            ["CER (Character Error Rate)", "WER (Word Error Rate)"],
            "Before Refinement": [f"{metrics['cer_before']*100:.1f}%",  f"{metrics['wer_before']*100:.1f}%"],
            "After Refinement":  [f"{metrics['cer_after']*100:.1f}%",   f"{metrics['wer_after']*100:.1f}%"],
            "Improvement":       [f"↓ {metrics['cer_improvement']*100:.1f}%", f"↓ {metrics['wer_improvement']*100:.1f}%"],
        })
        st.dataframe(df, width='stretch', hide_index=True)

# ──────────────────────────────────────────────
#  Saved Runs — the accumulated dataset
# ──────────────────────────────────────────────
if DB_READY:
    st.markdown("---")
    try:
        total_runs = count_runs()
    except Exception:
        total_runs = 0

    with st.expander(f"🗄️ Saved Runs — Accumulated Dataset ({total_runs} record(s))"):
        if total_runs == 0:
            st.info(
                "No runs saved yet. Every time you run the pipeline, the raw OCR "
                "text and its corrected version are stored here permanently."
            )
        else:
            all_runs = fetch_runs()

            # Compact view — full text fields go to the CSV export
            summary_cols = [
                "id", "created_at", "input_mode", "correction_method",
                "cer_before", "cer_after", "wer_before", "wer_after",
            ]
            summary_df = pd.DataFrame(all_runs)[summary_cols]
            st.dataframe(summary_df, width='stretch', hide_index=True)

            st.caption(
                "Each record stores the text at every stage (raw → Stage 1 → "
                "Stage 2 → final). Export below for the full contents."
            )

            full_df = pd.DataFrame(all_runs)
            st.download_button(
                label="⬇️ Export full dataset as CSV",
                # utf-8-sig so Sinhala renders correctly when opened in Excel
                data=full_df.to_csv(index=False).encode("utf-8-sig"),
                file_name="sinhala_ocr_dataset.csv",
                mime="text/csv",
            )

# ──────────────────────────────────────────────
#  Footer
# ──────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Prototype — Continuous Dataset Generation Pipeline for Sinhala-Based Model Training | "
    "SLIIT B.Sc. (Hons) IT — Data Science | IT22626288"
)