"""Shared UI helpers for the bank app pages."""

import streamlit as st
import pandas as pd


def show_profile_picker(df: pd.DataFrame, key: str, id_col: str = "CUSTOMER_ID"):
    """Show a selectbox + button to navigate to a customer profile on Customer 360."""
    if id_col not in df.columns:
        return
    ids = df[id_col].dropna().unique().tolist()
    if not ids:
        return
    col1, col2 = st.columns([3, 1])
    with col1:
        selected = st.selectbox(
            "Customer profile:",
            ids,
            key=f"profile_picker_{key}",
            label_visibility="collapsed",
            placeholder="Select a customer to view profile...",
            index=None,
        )
    with col2:
        if st.button("View profile", key=f"profile_btn_{key}", disabled=selected is None):
            st.session_state["_navigate_customer"] = selected
            st.switch_page("pages/customer_360.py")


# ============================================================
# Apple-inspired design system helpers (theme lives in .streamlit/config.toml)
# ============================================================

import plotly.graph_objects as go
import plotly.io as pio

APPLE_COLORS = ["#007AFF", "#34C759", "#FF9F0A", "#AF52DE", "#FF3B30",
                "#5AC8FA", "#FF2D55", "#5856D6", "#A2845E", "#8E8E93"]
STATUS_COLORS = {"PASS": "#34C759", "OK": "#34C759", "GREEN": "#34C759",
                 "WARNING": "#FF9F0A", "YELLOW": "#FF9F0A",
                 "FAIL": "#FF3B30", "RED": "#FF3B30", "CRITICAL": "#FF3B30"}

# Plotly template: transparent background (follows light/dark theme), hairline grid,
# Apple palette and Inter / system font. Registered once and made the default.
pio.templates["apple"] = go.layout.Template(
    layout=dict(
        colorway=APPLE_COLORS,
        font=dict(family="Inter, -apple-system, BlinkMacSystemFont, sans-serif", size=13),
        title=dict(font=dict(size=16), x=0, xanchor="left"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, zeroline=False, linecolor="rgba(142,142,147,0.4)"),
        yaxis=dict(gridcolor="rgba(142,142,147,0.18)", zeroline=False),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        margin=dict(t=48, l=8, r=8, b=8),
        hoverlabel=dict(font=dict(family="Inter, -apple-system, sans-serif")),
    )
)
pio.templates.default = "plotly+apple"


def page_header(title: str, subtitle: str = None, icon: str = None):
    """Large page title with an optional one-line subtitle (Apple-style hero)."""
    st.title(f"{icon} {title}" if icon else title, anchor=False)
    if subtitle:
        st.caption(subtitle)


def section(title: str, subtitle: str = None):
    """Section heading with an optional caption."""
    st.subheader(title, anchor=False)
    if subtitle:
        st.caption(subtitle)


def kpi_row(kpis):
    """Render KPIs on one gap-free row.

    kpis: list of (label, value) or (label, value, delta) or (label, value, delta, delta_color).
    """
    kpis = [k for k in kpis if k is not None]
    if not kpis:
        return
    for col, k in zip(st.columns(len(kpis)), kpis):
        label, value = k[0], k[1]
        delta = k[2] if len(k) > 2 else None
        delta_color = k[3] if len(k) > 3 else "normal"
        col.metric(label, value, delta, delta_color=delta_color, border=True)


def status_badge(status: str) -> str:
    """Markdown badge for a status value (PASS / WARNING / FAIL ...)."""
    color = {"#34C759": "green", "#FF9F0A": "orange", "#FF3B30": "red"}.get(
        STATUS_COLORS.get(str(status).upper()), "gray")
    return f":{color}-badge[{status}]"


def empty_state(message: str, icon: str = ":material/inbox:"):
    """Consistent empty / no-data message."""
    st.info(message, icon=icon)
