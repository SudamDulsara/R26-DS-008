"""
frontend/components/score_display.py
------------------------------------
Renders the verdict badge and quality-score gauge for a single Document.

Kept in its own module because both single-doc and compare modes use it,
and the Plotly gauge config is fiddly enough to deserve isolation.
"""

import plotly.graph_objects as go
import streamlit as st

from quality_pipeline.schema import Document, Verdict


# Color scheme — matches the Excel writer for visual consistency between the
# spreadsheet artifact and the live frontend.
_VERDICT_COLOR = {
    Verdict.ACCEPT:  "#2E7D32",  # green
    Verdict.REVIEW:  "#F9A825",  # amber
    Verdict.REJECT:  "#C62828",  # red
    Verdict.PENDING: "#757575",  # grey
}

_VERDICT_LABEL = {
    Verdict.ACCEPT:  "✓ ACCEPT",
    Verdict.REVIEW:  "⚠ REVIEW",
    Verdict.REJECT:  "✗ REJECT",
    Verdict.PENDING: "… PENDING",
}


def render_verdict_badge(doc: Document) -> None:
    """Big colored badge announcing the final verdict."""
    color = _VERDICT_COLOR[doc.verdict]
    label = _VERDICT_LABEL[doc.verdict]
    st.markdown(
        f"""
        <div style="
            background-color: {color};
            color: white;
            padding: 18px;
            border-radius: 8px;
            text-align: center;
            font-size: 28px;
            font-weight: bold;
            margin-bottom: 8px;
        ">{label}</div>
        """,
        unsafe_allow_html=True,
    )

    if doc.verdict_reasons:
        st.caption("Reasons: " + " · ".join(doc.verdict_reasons))


def render_score_gauge(doc: Document) -> None:
    """Plotly gauge showing the morphology quality score (0-100)."""
    morph = doc.quality.get("morphology", {})
    score = morph.get("score", 0.0)

    # Bands match the threshold constants in MorphologyQualityScorer.
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        title={"text": "Morphology Quality Score", "font": {"size": 16}},
        number={"font": {"size": 36}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1},
            "bar": {"color": "#1F2937", "thickness": 0.25},
            "steps": [
                {"range": [0, 15],   "color": "#FFCDD2"},  # reject zone
                {"range": [15, 45],  "color": "#FFE0B2"},  # review zone
                {"range": [45, 100], "color": "#C8E6C9"},  # accept zone
            ],
            "threshold": {
                "line": {"color": "#111827", "width": 3},
                "thickness": 0.85,
                "value": score,
            },
        },
    ))
    fig.update_layout(height=240, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig, use_container_width=True)