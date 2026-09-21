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
