"""
frontend/components/feature_chart.py
------------------------------------
Renders a horizontal bar chart of the five morphology sub-features that
contribute to the overall score.

Why this matters for the demo: the score gauge alone is opaque. Showing the
sub-features lets you point at *which* dimension of morphological richness
the document scored low/high on, and connect that back to what the document
actually contains.
"""

import plotly.graph_objects as go
import streamlit as st

from quality_pipeline.schema import Document


# Display names + the order they appear in the chart.
# Order chosen to group related features: language presence → parsing success →
# richness → variety.
_FEATURE_DISPLAY = [
    ("sinhala_ratio",        "Sinhala ratio"),
    ("decomposition_rate",   "Decomposition rate"),
    ("agglutination_depth",  "Agglutination depth"),
    ("morpheme_diversity",   "Morpheme diversity"),
    ("category_diversity",   "Category diversity"),
]


def render_feature_chart(doc: Document) -> None:
    """Horizontal bar chart of the five normalized sub-features."""
    morph = doc.quality.get("morphology", {})
    features = morph.get("features", {})

    if not features.get("scoreable", False):
        st.info("Document is too short or contains no Sinhala — no morphology score.")
        return

    labels = [display for _, display in _FEATURE_DISPLAY]
    values = [features.get(key, 0.0) for key, _ in _FEATURE_DISPLAY]

    # Color each bar by its value (red < 0.3, amber < 0.6, green ≥ 0.6).
    # Same thresholds as the gauge bands, applied per-feature.
    colors = [
        "#EF5350" if v < 0.3 else "#FFB74D" if v < 0.6 else "#66BB6A"
        for v in values
    ]

    fig = go.Figure(go.Bar(
        x=values,
        y=labels,
        orientation="h",
        marker_color=colors,
        text=[f"{v:.2f}" for v in values],
        textposition="outside",
        cliponaxis=False,
    ))
    fig.update_layout(
        height=260,
        margin=dict(l=20, r=40, t=20, b=20),
        xaxis=dict(range=[0, 1.1], showgrid=True, gridcolor="#E0E0E0"),
        yaxis=dict(autorange="reversed"),  # first feature on top
        plot_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Surface the discrete diagnostic info that doesn't fit on a 0-1 axis.
    cols = st.columns(3)
    cols[0].metric("Total words", features.get("total_words", 0))
    cols[1].metric("Sinhala words", features.get("sinhala_words", 0))
    cols[2].metric(
        "Categories used",
        f"{len(features.get('categories_used', []))} / 5",
    )

    cats = features.get("categories_used", [])
    if cats:
        st.caption("Categories present: " + ", ".join(cats))