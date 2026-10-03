"""
frontend/pages/2_Continuous_Pipeline.py
---------------------------------------
Two-button orchestration page:
  1. Run upstream components (subset selectable, live stdout)
  2. Run quality pipeline (incremental, shows new-vs-cumulative)
"""

import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd
import streamlit as st

from quality_pipeline.integration.incremental_state import IncrementalState
from quality_pipeline.integration.run_integration import (
    DEFAULT_INBOX,
    DEFAULT_OUTBOX,
    build_pipeline,
    run_incremental,
)
from quality_pipeline.integration.upstream_runner import (
    default_configs,
    run_one_streaming,
)


st.set_page_config(
    page_title="Continuous Pipeline",
    layout="wide",
)

st.title("🔁 Continuous Data-Gathering Pipeline")
st.caption(
    "Run the three upstream data pipelines, then run the quality pipeline "
    "incrementally on the results. Outputs accumulate over runs."
)


# ---------------------------------------------------------------------------
# Section 1: Run upstream
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("## Step 1 — Run upstream components")
st.caption(
    "Pick which upstream pipelines to run. They execute sequentially. "
    "If one fails, the others still run and you get a report at the end."
)

configs = default_configs(_REPO_ROOT)
config_by_name = {c.name: c for c in configs}

with st.expander("Show the exact commands that will run"):
    for c in configs:
        exe = c.python_exe or "python (current sys.executable)"
        st.markdown(
            f"**{c.name}** — {c.description}\n\n"
            f"- cwd: `{c.cwd}`\n"
            f"- python: `{exe}`\n"
            f"- args: `{' '.join(c.args)}`\n"
            f"- extra env: `{c.env if c.env else '(none)'}`"
        )

chosen = st.multiselect(
    "Components to run (in order):",
    options=[c.name for c in configs],
    default=[c.name for c in configs],
)

if st.button("▶️ Run selected upstream components", type="primary", key="run_upstream"):
    if not chosen:
        st.warning("Pick at least one component.")
    else:
        results = []
        for name in chosen:
            cfg = config_by_name[name]
            with st.status(f"Running: {name}", expanded=True) as status:
                log_area = st.empty()
                lines_buffer: list[str] = []
                final_result = None

                for kind, payload in run_one_streaming(cfg):
                    if kind == "line":
                        lines_buffer.append(payload)
                        log_area.code("\n".join(lines_buffer[-200:]), language=None)
                    elif kind == "result":
                        final_result = payload

                if final_result and final_result.success:
                    status.update(
                        label=f"✅ {name} — done in {final_result.duration_sec:.1f}s",
                        state="complete",
                    )
                else:
                    err = final_result.error_message if final_result else "unknown error"
                    status.update(
                        label=f"❌ {name} — failed ({err})",
                        state="error",
                    )
                results.append(final_result)

        st.markdown("### Upstream run summary")
        ok = [r for r in results if r and r.success]
        bad = [r for r in results if r and not r.success]
        st.markdown(f"**{len(ok)} succeeded, {len(bad)} failed.**")
        if bad:
            for r in bad:
                st.error(f"{r.name}: {r.error_message}")
        st.info(
            "Now check `data/inbox/` for the produced .db files, then run "
            "the quality pipeline below."
        )


# ---------------------------------------------------------------------------
# Section 2: Inbox status
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("## Current `data/inbox/` status")

expected_files = {
    "news":  "news_pipeline.db",
    "ocr":   "ocr_correction_pairs.db",
    "asr":   "videos.db",
}
cols = st.columns(3)
for i, (label, filename) in enumerate(expected_files.items()):
    fpath = DEFAULT_INBOX / filename
    with cols[i]:
        if fpath.exists():
            mtime = pd.Timestamp(fpath.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M")
            size_kb = fpath.stat().st_size / 1024
            st.metric(
                label=f"{label} ({filename})",
                value=f"{size_kb:.1f} KB",
                help=f"Last modified: {mtime}",
            )
        else:
            st.metric(label=f"{label} ({filename})", value="—", help="Not present")


# ---------------------------------------------------------------------------
# Section 3: Run quality pipeline
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("## Step 2 — Run quality pipeline (incremental)")
st.caption(
    "Processes only documents not seen in any prior run. Cumulative "
    "outputs land in `data/outbox/`."
)

force = st.checkbox(
    "Force reprocess (re-run all upstream docs, not just new ones)",
    value=False,
)

if st.button("▶️ Run quality pipeline", type="primary", key="run_quality"):
    with st.status("Running quality pipeline...", expanded=True) as status:
        st.write("Loading LaBSE model (first run downloads ~470MB)...")
        pipeline = build_pipeline()
        st.write("Model ready. Running incremental pass...")

        summary = run_incremental(
            inbox=DEFAULT_INBOX,
            outbox=DEFAULT_OUTBOX,
            pipeline=pipeline,
            force_reprocess=force,
        )

        status.update(label="✅ Quality pipeline complete", state="complete")

    st.markdown("### This run")
    if summary["docs_processed"] == 0:
        st.info("No new documents to process — everything was already seen.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Docs processed", summary["docs_processed"])
        c2.metric("Accepted (new)", summary["verdicts"].get("accepted", 0))
        c3.metric("Review (new)",   summary["verdicts"].get("review", 0))
        c4.metric("Rejected (new)", summary["verdicts"].get("rejected", 0))
        st.caption(f"Run ID: `{summary['run_id']}`")


# ---------------------------------------------------------------------------
# Section 4: Cumulative state
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("## Cumulative corpus state")

verdicts_db = DEFAULT_OUTBOX / "verdicts.db"
training_jsonl = DEFAULT_OUTBOX / "training_corpus.jsonl"

if not verdicts_db.exists():
    st.info("No runs yet. Run the quality pipeline above to see results here.")
else:
    with sqlite3.connect(verdicts_db) as conn:
        totals = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("accepted", "review", "rejected")
        }
        try:
            per_source = pd.read_sql_query(
                "SELECT source, verdict, COUNT(*) AS n FROM ("
                "  SELECT source, verdict FROM accepted "
                "  UNION ALL "
                "  SELECT source, verdict FROM review "
                "  UNION ALL "
                "  SELECT source, verdict FROM rejected"
                ") GROUP BY source, verdict",
                conn,
            )
        except Exception:
            per_source = pd.DataFrame()
        try:
            runs = pd.read_sql_query(
                "SELECT run_id, started_at, docs_processed, accepted, review, rejected "
                "FROM verdict_runs ORDER BY started_at DESC LIMIT 20",
                conn,
            )
        except Exception:
            runs = pd.DataFrame()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Accepted (cum.)", totals["accepted"])
    c2.metric("Review (cum.)",   totals["review"])
    c3.metric("Rejected (cum.)", totals["rejected"])
    if training_jsonl.exists():
        with training_jsonl.open(encoding="utf-8") as f:
            corpus_size = sum(1 for _ in f)
        c4.metric("Training corpus lines", corpus_size)

    if not per_source.empty:
        st.markdown("### Verdicts by source")
        pivot = per_source.pivot(index="source", columns="verdict", values="n").fillna(0).astype(int)
        st.dataframe(pivot, use_container_width=True)

    if not runs.empty:
        st.markdown("### Recent runs")
        st.dataframe(runs, use_container_width=True, hide_index=True)

    state = IncrementalState(verdicts_db)
    seen = state.stats()
    if seen:
        st.caption(
            "Processed doc IDs tracked (won't be re-processed): "
            + ", ".join(f"{src}: {n}" for src, n in seen.items())
        )
