import streamlit as st
from datetime import datetime
from utils.snowflake_connection import get_snowflake_session, test_connection
from utils.data_loaders import load_data_quality_metrics
from utils.visualizations import plot_data_quality_completeness

st.header("Settings & Data Quality")

tab_quality, tab_settings = st.tabs(["Data Quality", "Settings"])

with tab_quality:
    try:
        metrics = load_data_quality_metrics()
        if metrics:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Records", f"{metrics.get('TOTAL_RECORDS', 0):,.0f}")
            with col2:
                st.metric("Unique Customers", f"{metrics.get('UNIQUE_CUSTOMERS', 0):,.0f}")
            with col3:
                st.metric("Email Completeness", f"{metrics.get('EMAIL_COMPLETENESS', 0):.1f}%")
            with col4:
                st.metric("Phone Completeness", f"{metrics.get('PHONE_COMPLETENESS', 0):.1f}%")

            st.divider()
            fig = plot_data_quality_completeness(metrics)
            st.plotly_chart(fig, width="stretch")

            st.divider()
            st.subheader("Missing Data Analysis")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write("**Missing Email:**", f"{metrics.get('MISSING_EMAIL', 0):,.0f}")
            with col2:
                st.write("**Missing Phone:**", f"{metrics.get('MISSING_PHONE', 0):,.0f}")
            with col3:
                st.write("**Missing DOB:**", f"{metrics.get('MISSING_DOB', 0):,.0f}")

            st.info("""
            **Quality Thresholds:**
            - Excellent: >95% completeness
            - Acceptable: 80-95% completeness
            - Poor: <80% completeness (requires remediation)
            """)
        else:
            st.warning("No data quality metrics available")
    except Exception as e:
        st.error(f"Error loading data quality metrics: {str(e)}")

with tab_settings:
    st.subheader("Data Freshness")
    st.write(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Refresh All Data", width="stretch", icon=":material/refresh:"):
            st.cache_data.clear()
            st.success("Cache cleared!")
            st.rerun()
    with col2:
        if st.button("Clear Error Cache", width="stretch", icon=":material/delete:"):
            st.cache_data.clear()
            st.success("Error cache cleared!")
            st.rerun()

    st.divider()
    st.subheader("Display Preferences")
    st.select_slider("Rows per page", options=[10, 20, 50, 100], value=20)
    st.selectbox("Date format", ["YYYY-MM-DD", "DD/MM/YYYY", "MM/DD/YYYY"])
    st.selectbox("Currency", ["CHF", "EUR", "USD", "GBP"])

    st.divider()
    st.subheader("Snowflake Connection")
    try:
        session = get_snowflake_session()
        current_db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
        current_schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
        current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]
        st.success("Connected to Snowflake", icon=":material/cloud_done:")
        st.write(f"**Database:** {current_db}")
        st.write(f"**Schema:** {current_schema}")
        st.write(f"**User:** {current_user}")
        if st.button("Test Connection", icon=":material/network_check:"):
            with st.spinner("Testing..."):
                if test_connection():
                    st.success("Connection test successful!")
                else:
                    st.error("Connection test failed")
    except Exception as e:
        st.error("Not connected to Snowflake")
        st.code(str(e))
