"""
frontend/components/corpus_overlap.py
-------------------------------------
Renders the cross-register semantic overlap results for a small corpus
(3-6 documents processed in one batch).

The single-doc and compare modes can only show pairwise overlap. To
demonstrate the *register-aware* behaviour of Stage 3b — the whole point
of the novelty — we need a mode where you can add several docs from
different sources and see the pipeline decide which are duplicates and
which are cross-register overlaps that need review.

This component takes the already-processed list of Documents (Stage 3b
has run on all of them) and renders:
  - A verdict summary table
  - A similarity heatmap (where the same-source vs cross-source structure
    is visually obvious)
  - Per-decision detail cards
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from quality_pipeline.schema import Document, Verdict


# Verdict → colour, matching the Excel writer for consistency.
_VERDICT_COLOR = {
    Verdict.ACCEPT: "🟢",
    Verdict.REVIEW: "🟡",
    Verdict.REJECT: "🔴",
    Verdict.PENDING: "⚪",
}


def _extract_pairs(docs: list[Document]) -> list[dict]:
    """
    Extract the (source-i, target-j, similarity, decision) records that
    Stage 3b produced. Because Stage 3b compares each doc against ALL
    previously-seen docs, we only get the best-match — not a full
    similarity matrix. That's what the pipeline recorded, so that's what
    we show. A full matrix would require re-encoding, which we don't do.
    """
    records = []
    for i, doc in enumerate(docs):
        ov = doc.quality.get("semantic_overlap", {})
        if not ov or ov.get("skipped_reason"):
            continue
        best_id = ov.get("best_match_id")
        if best_id is None:
            continue
        records.append({
            "target_id": doc.doc_id,
            "target_source": doc.source.value,
            "best_match_id": best_id,
            "best_match_source": ov.get("best_match_source"),
            "score": ov.get("best_match_score"),
            "decision": ov.get("decision"),
            "verdict": doc.verdict.value,
        })
    return records


def render_corpus_overlap(docs: list[Document]) -> None:
    """
    Render Stage 3b results across a small processed corpus.

    Assumes all docs have been through the full pipeline (including Stage 3b).
    """
    if not docs:
        st.info("No documents to show.")
        return

    st.markdown("### Corpus verdict summary")

    # ---- Verdict table ----
    verdict_rows = []
    for d in docs:
        emoji = _VERDICT_COLOR.get(d.verdict, "⚪")
        ov = d.quality.get("semantic_overlap", {})
        reason_short = ""
        if ov.get("decision") == "reject_same_source":
            reason_short = f"→ semantic dup of {ov.get('best_match_id')}"
        elif ov.get("decision") == "review_cross_register":
            reason_short = f"→ cross-register with {ov.get('best_match_id')}"
        elif ov.get("skipped_reason"):
            reason_short = f"(skipped: {ov['skipped_reason']})"
        elif ov.get("decision") == "no_overlap":
            reason_short = "(distinct)"

        verdict_rows.append({
            "Doc ID":   d.doc_id,
            "Source":   d.source.value,
            "Verdict":  f"{emoji} {d.verdict.value}",
            "Stage 3b": reason_short,
            "Text preview": d.text[:60] + ("…" if len(d.text) > 60 else ""),
        })
    st.dataframe(pd.DataFrame(verdict_rows), use_container_width=True, hide_index=True)

    # ---- Pair records + decision cards ----
    pairs = _extract_pairs(docs)
    if not pairs:
        st.info(
            "No semantic-overlap comparisons to show. This happens when only one "
            "document was processed, or when all documents were rejected before "
            "reaching Stage 3b."
        )
        return

    st.markdown("### Stage 3b — pairwise decisions")
    st.caption(
        "Each processed document is compared against all previously-seen documents. "
        "The row below shows each document's *best* prior match."
    )

    # Group by decision type so the register-aware split is visually obvious.
    rejects = [p for p in pairs if p["decision"] == "reject_same_source"]
    reviews = [p for p in pairs if p["decision"] == "review_cross_register"]
    distincts = [p for p in pairs if p["decision"] == "no_overlap"]

    if rejects:
        st.markdown("**🔴 Rejected — same-source semantic duplicates**")
        for p in rejects:
            st.error(
                f"`{p['target_id']}` ({p['target_source']}) matches "
                f"`{p['best_match_id']}` ({p['best_match_source']}) at "
                f"similarity **{p['score']:.3f}** → both from same source, reject."
            )

    if reviews:
        st.markdown("**🟡 Cross-register overlap — routed to REVIEW**")
        for p in reviews:
            st.warning(
                f"`{p['target_id']}` ({p['target_source']}) matches "
                f"`{p['best_match_id']}` ({p['best_match_source']}) at "
                f"similarity **{p['score']:.3f}** → different sources, "
                f"potentially valuable in different registers, human to decide."
            )

    if distincts:
        st.markdown("**🟢 No significant overlap**")
        with st.expander(f"Show {len(distincts)} distinct docs"):
            for p in distincts:
                best_score = p["score"]
                st.markdown(
                    f"- `{p['target_id']}` ({p['target_source']}) — "
                    f"best match `{p['best_match_id']}` at similarity "
                    f"{best_score:.3f} (below threshold)"
                )

    # ---- Similarity chart ----
    # A bar chart of each doc's best-match similarity, coloured by decision.
    # This makes the register-aware threshold split visually obvious.
    st.markdown("### Best-match similarity per document")

    def _colour(decision: str) -> str:
        return {
            "reject_same_source":    "#C62828",   # red
            "review_cross_register": "#F9A825",   # amber
            "no_overlap":            "#2E7D32",   # green
        }.get(decision, "#757575")

    fig = go.Figure(go.Bar(
        x=[p["score"] for p in pairs],
        y=[f"{p['target_id']} ({p['target_source']})" for p in pairs],
        orientation="h",
        marker_color=[_colour(p["decision"]) for p in pairs],
        text=[f"{p['score']:.2f}" for p in pairs],
        textposition="outside",
        cliponaxis=False,
    ))

    # Threshold line — this is the key visual: dots left = distinct, dots right = flagged
    threshold = docs[0].quality.get("semantic_overlap", {}).get("threshold", 0.85)
    fig.add_vline(
        x=threshold, line_dash="dash", line_color="#666",
        annotation_text=f"threshold {threshold}",
        annotation_position="top",
    )

    fig.update_layout(
        height=max(240, 40 * len(pairs)),
        margin=dict(l=20, r=40, t=40, b=20),
        xaxis=dict(range=[0, 1.05], title="Best-match cosine similarity"),
        yaxis=dict(autorange="reversed"),
        plot_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True)