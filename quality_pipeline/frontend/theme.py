"""
frontend/theme.py
-----------------
Shared theming for the Sinhala Quality Pipeline frontend.
Call inject_theme() once at the top of every page (right after
st.set_page_config). The module is purely cosmetic.
"""

import streamlit as st


_CSS = """
<style>

/* -------- Overall layout -------- */
.block-container {
    padding-top: 2rem !important;
    padding-bottom: 4rem !important;
    max-width: 1200px !important;
}

/* -------- Typography -------- */
h1, h2, h3, h4 {
    letter-spacing: -0.015em !important;
    font-weight: 600 !important;
}

h1 {
    font-size: 2.1rem !important;
    margin-bottom: 0.5rem !important;
    background: linear-gradient(135deg, #E0F2F1 0%, #80CBC4 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

/* Every h2 gets a visible teal accent bar on the left */
h2 {
    font-size: 1.5rem !important;
    margin-top: 2.5rem !important;
    margin-bottom: 1rem !important;
    padding-left: 16px !important;
    border-left: 4px solid #26A69A !important;
    line-height: 1.3 !important;
}

/* ...except in the sidebar */
[data-testid="stSidebar"] h2,
[data-testid="stSidebarContent"] h2 {
    padding-left: 0 !important;
    border-left: none !important;
    font-size: 1.2rem !important;
}

h3 {
    font-size: 1.15rem !important;
    margin-top: 1.5rem !important;
    color: #B0BEC5 !important;
}

/* Captions */
[data-testid="stCaptionContainer"] {
    color: #78909C !important;
    font-size: 0.9rem !important;
}

/* -------- Metric widgets (card style) -------- */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(38, 166, 154, 0.04) 0%, rgba(255, 255, 255, 0.02) 100%) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    border-radius: 14px !important;
    padding: 1.1rem 1.4rem !important;
    transition: border-color 0.2s ease, transform 0.15s ease !important;
}
[data-testid="stMetric"]:hover {
    border-color: rgba(38, 166, 154, 0.5) !important;
    transform: translateY(-2px) !important;
}
[data-testid="stMetricLabel"] {
    font-size: 0.78rem !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    color: #90A4AE !important;
    font-weight: 600 !important;
}
[data-testid="stMetricValue"] {
    font-size: 1.8rem !important;
    font-weight: 500 !important;
    margin-top: 0.3rem !important;
    color: #ECEFF1 !important;
}

/* -------- Buttons -------- */
.stButton > button {
    border-radius: 10px !important;
    font-weight: 500 !important;
    letter-spacing: 0.02em !important;
    padding: 0.6rem 1.4rem !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    transition: transform 0.1s ease, box-shadow 0.2s ease, border-color 0.2s ease !important;
}
.stButton > button:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 18px rgba(0, 0, 0, 0.35) !important;
    border-color: rgba(38, 166, 154, 0.4) !important;
}
.stButton > button:active {
    transform: translateY(0) !important;
}

/* Primary buttons — teal gradient */
.stButton > button[kind="primary"],
button[data-testid="baseButton-primary"],
.stButton > button[data-testid="baseButton-primary"] {
    background: linear-gradient(135deg, #26A69A 0%, #00897B 100%) !important;
    color: white !important;
    border: none !important;
    box-shadow: 0 2px 8px rgba(38, 166, 154, 0.3) !important;
}
.stButton > button[kind="primary"]:hover,
button[data-testid="baseButton-primary"]:hover {
    background: linear-gradient(135deg, #2BB5A8 0%, #00968C 100%) !important;
    box-shadow: 0 6px 20px rgba(38, 166, 154, 0.4) !important;
}

/* -------- Multiselect chips (was pink/red) -------- */
[data-baseweb="tag"] {
    background-color: rgba(38, 166, 154, 0.25) !important;
    color: #B2DFDB !important;
    border-radius: 6px !important;
}
[data-baseweb="tag"] span {
    color: #B2DFDB !important;
}

/* -------- Dividers -------- */
hr {
    border: none !important;
    height: 1px !important;
    background: linear-gradient(90deg, transparent, rgba(38, 166, 154, 0.3), transparent) !important;
    margin: 2.5rem 0 !important;
}

/* -------- Dataframes -------- */
[data-testid="stDataFrame"] {
    border-radius: 10px !important;
    overflow: hidden !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
}

/* -------- Sidebar -------- */
[data-testid="stSidebar"] {
    background: rgba(255, 255, 255, 0.02) !important;
    border-right: 1px solid rgba(255, 255, 255, 0.06) !important;
}

/* -------- Expanders -------- */
[data-testid="stExpander"] {
    border-radius: 12px !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
    background: rgba(255, 255, 255, 0.02) !important;
    margin-bottom: 0.75rem !important;
}
[data-testid="stExpander"] summary {
    font-weight: 500 !important;
    padding: 0.6rem 0.75rem !important;
    border-radius: 10px !important;
}
[data-testid="stExpander"] summary:hover {
    background: rgba(38, 166, 154, 0.05) !important;
}

/* -------- Alerts (info / warning / error / success) -------- */
[data-testid="stAlert"] {
    border-radius: 12px !important;
    border-left-width: 4px !important;
    padding: 1rem 1.25rem !important;
}

/* -------- Status widget (used by run buttons) -------- */
[data-testid="stStatusWidget"] {
    border-radius: 12px !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
}

/* -------- Text inputs / text areas / selectboxes -------- */
.stTextInput input,
.stTextArea textarea,
[data-testid="stSelectbox"] > div,
[data-baseweb="select"] > div {
    border-radius: 10px !important;
}
.stTextArea textarea {
    font-family: "Segoe UI", "Noto Sans Sinhala", "Iskoola Pota", sans-serif !important;
    line-height: 1.6 !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
}
.stTextArea textarea:focus {
    border-color: rgba(38, 166, 154, 0.5) !important;
    box-shadow: 0 0 0 2px rgba(38, 166, 154, 0.15) !important;
}

/* -------- Code blocks -------- */
.stCodeBlock, pre, code {
    border-radius: 8px !important;
}
.stCodeBlock {
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
}

/* -------- Checkbox / radio -------- */
[data-testid="stCheckbox"] { padding: 0.3rem 0 !important; }
[data-testid="stRadio"] label { padding: 0.3rem 0 !important; }

/* -------- Tabs (if any used) -------- */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px !important;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0 !important;
    padding: 0.5rem 1rem !important;
}

</style>
"""


def inject_theme() -> None:
    """Apply the custom theme. Call once at the top of every page."""
    st.markdown(_CSS, unsafe_allow_html=True)


def soft_divider() -> None:
    """A subtle gradient divider — nicer than st.markdown('---')."""
    st.markdown(
        '<hr style="border:none;height:1px;'
        'background:linear-gradient(90deg,transparent,rgba(38,166,154,0.3),transparent);'
        'margin:1.75rem 0;">',
        unsafe_allow_html=True,
    )


_PILL_COLORS = {
    "neutral": ("#B0BEC5", "rgba(144,164,174,0.18)"),
    "success": ("#81C784", "rgba(76,175,80,0.18)"),
    "warning": ("#FFB74D", "rgba(255,167,38,0.18)"),
    "danger":  ("#EF9A9A", "rgba(239,83,80,0.18)"),
    "info":    ("#81D4FA", "rgba(79,195,247,0.18)"),
}


def pill(text: str, variant: str = "neutral") -> str:
    """Return HTML for an inline colored pill badge."""
    fg, bg = _PILL_COLORS.get(variant, _PILL_COLORS["neutral"])
    return (
        f'<span style="display:inline-block;padding:0.25rem 0.7rem;'
        f'border-radius:999px;font-size:0.78rem;font-weight:500;'
        f'letter-spacing:0.02em;background:{bg};color:{fg};">{text}</span>'
    )