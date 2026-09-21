"""
Synthetic Retail Bank - Modern Multi-Page App
"""

import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="Synthetic Retail Bank",
    page_icon="https://upload.wikimedia.org/wikipedia/commons/thumb/4/4b/Snowflake_Logo.svg/32px-Snowflake_Logo.svg.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Navigation ---

pages = {
    "Banking": [
        st.Page("pages/customer_360.py", title="Customer 360", icon=":material/person_search:"),
        st.Page("pages/portfolio_analytics.py", title="Portfolio Analytics", icon=":material/pie_chart:"),
        st.Page("pages/churn_lifecycle.py", title="Churn & Lifecycle", icon=":material/autorenew:"),
    ],
    "Risk & Compliance": [
        st.Page("pages/risk_compliance.py", title="Risk & Compliance", icon=":material/shield:"),
        st.Page("pages/aml_fraud.py", title="AML & Fraud", icon=":material/warning:"),
        st.Page("pages/lending.py", title="Lending & Loans", icon=":material/account_balance:"),
        st.Page("pages/wealth_management.py", title="Wealth Management", icon=":material/trending_up:"),
        st.Page("pages/lcr_monitoring.py", title="LCR Monitoring", icon=":material/water_drop:"),
    ],
    "AI & Settings": [
        st.Page("pages/ask_ai.py", title="Ask AI", icon=":material/smart_toy:"),
        st.Page("pages/settings.py", title="Settings", icon=":material/settings:"),
    ],
}

nav = st.navigation(pages)

# --- Sidebar ---

with st.sidebar:
    st.markdown("### Synthetic Retail Bank")
    st.caption("Comprehensive Customer Intelligence")
    st.divider()

    from utils.snowflake_connection import get_snowflake_session, test_connection

    try:
        session = get_snowflake_session()
        st.success("Connected to Snowflake", icon=":material/cloud_done:")
        st.caption("AAA_DEV_SYNTHETIC_BANK")
    except Exception as e:
        st.error("Connection failed", icon=":material/cloud_off:")
        st.caption(str(e))

    st.divider()

    st.markdown(
        """<a href="https://ai.snowflake.com/sfseeurope/demo_mdaeppen" target="_blank"
        style="display:inline-flex;align-items:center;gap:6px;padding:8px 16px;
        background:#1a73e8;color:white;border-radius:8px;text-decoration:none;
        font-weight:500;font-size:14px;width:100%;justify-content:center;">
        Open Snowflake CoWork</a>""",
        unsafe_allow_html=True,
    )

    st.divider()

    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    if st.button("Refresh data", width="stretch", icon=":material/refresh:"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.caption("v2.0 | 2026 | Streamlit + Snowflake")

nav.run()
