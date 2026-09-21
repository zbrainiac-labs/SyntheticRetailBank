import streamlit as st
import pandas as pd
from datetime import datetime
from utils.snowflake_connection import get_snowflake_session
from utils.data_loaders import load_customer_360

st.header("Customer 360")

try:
    df_customers = load_customer_360()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Customers", len(df_customers))
    with col2:
        critical_count = len(df_customers[df_customers['OVERALL_RISK_RATING'].isin(['CRITICAL', 'HIGH'])])
        st.metric("Critical / High Risk", critical_count)
    with col3:
        pep_count = len(df_customers[df_customers['EXPOSED_PERSON_MATCH_TYPE'] != 'NO_MATCH'])
        st.metric("PEP Matches", pep_count)
    with col4:
        avg_risk = df_customers['OVERALL_RISK_SCORE'].mean()
        st.metric("Avg Risk Score", f"{avg_risk:.1f}")

    st.divider()

    # Search and filter
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        search_name = st.text_input("Search by Name", placeholder="Enter customer name...")
    with col2:
        search_country = st.selectbox("Country", ["All"] + sorted(df_customers['COUNTRY'].unique().tolist()))
    with col3:
        search_tier = st.multiselect("Account Tier", df_customers['ACCOUNT_TIER'].unique().tolist())
    with col4:
        search_risk = st.selectbox("Risk Level", ["All"] + sorted(df_customers['OVERALL_RISK_RATING'].unique().tolist()))

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        filter_high_risk = st.checkbox("Critical / High Risk")
    with col2:
        filter_pep = st.checkbox("PEP Matches")
    with col3:
        filter_requires_review = st.checkbox("Requires Review")
    with col4:
        filter_anomaly = st.checkbox("Anomaly Flagged")

    # Check for navigation from other pages via session state or query params
    linked_customer = st.session_state.pop("_navigate_customer", None) or st.query_params.get("customer")

    # Apply filters
    df_filtered = df_customers.copy()
    if linked_customer:
        df_filtered = df_filtered[df_filtered['CUSTOMER_ID'] == linked_customer]
    if search_name:
        df_filtered = df_filtered[df_filtered['FULL_NAME'].str.contains(search_name, case=False, na=False)]
    if search_country != "All":
        df_filtered = df_filtered[df_filtered['COUNTRY'] == search_country]
    if search_tier:
        df_filtered = df_filtered[df_filtered['ACCOUNT_TIER'].isin(search_tier)]
    if search_risk != "All":
        df_filtered = df_filtered[df_filtered['OVERALL_RISK_RATING'] == search_risk]
    if filter_high_risk:
        df_filtered = df_filtered[df_filtered['OVERALL_RISK_RATING'].isin(['CRITICAL', 'HIGH'])]
    if filter_pep:
        df_filtered = df_filtered[df_filtered['EXPOSED_PERSON_MATCH_TYPE'] != 'NO_MATCH']
    if filter_requires_review:
        df_filtered = df_filtered[
            (df_filtered['REQUIRES_EXPOSED_PERSON_REVIEW'] == True) |
            (df_filtered['REQUIRES_SANCTIONS_REVIEW'] == True)
        ]
    if filter_anomaly:
        df_filtered = df_filtered[df_filtered['HAS_ANOMALY'] == True]

    st.info(f"Found **{len(df_filtered)}** customers matching your criteria")

    if len(df_filtered) > 0:
        display_cols = [
            'CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'ACCOUNT_TIER',
            'OVERALL_RISK_RATING', 'OVERALL_RISK_SCORE',
            'EXPOSED_PERSON_MATCH_TYPE', 'SANCTIONS_MATCH_TYPE'
        ]
        df_display = df_filtered[display_cols].copy()
        df_display['OVERALL_RISK_SCORE'] = df_display['OVERALL_RISK_SCORE'].round(1)
        st.dataframe(df_display, width="stretch", height=400, hide_index=True)

        st.divider()
        st.subheader("Customer Profile")

        selected_customer_id = st.selectbox(
            "Select a customer to view detailed profile:",
            df_filtered['CUSTOMER_ID'].tolist(),
            format_func=lambda x: f"{x} - {df_filtered[df_filtered['CUSTOMER_ID']==x]['FULL_NAME'].iloc[0]}"
        )

        if selected_customer_id:
            customer = df_filtered[df_filtered['CUSTOMER_ID'] == selected_customer_id].iloc[0]

            with st.expander("Identity & Demographics", expanded=True):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write("**Customer ID:**", customer['CUSTOMER_ID'])
                    st.write("**Full Name:**", customer['FULL_NAME'])
                with col2:
                    st.write("**First Name:**", customer['FIRST_NAME'])
                    st.write("**Family Name:**", customer['FAMILY_NAME'])
                with col3:
                    st.write("**Date of Birth:**", customer['DATE_OF_BIRTH'])
                    st.write("**Onboarding Date:**", customer['ONBOARDING_DATE'])
                with col4:
                    st.write("**Currency:**", customer['REPORTING_CURRENCY'])
                    st.write("**Status:**", customer['CURRENT_STATUS'])

            with st.expander("Contact Information"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write("**Email:**", customer['EMAIL'])
                with col2:
                    st.write("**Phone:**", customer['PHONE'])
                with col3:
                    st.write("**Preferred Method:**", customer['PREFERRED_CONTACT_METHOD'])

            with st.expander("Employment & Financial Profile"):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write("**Employer:**", customer['EMPLOYER'])
                    st.write("**Position:**", customer['POSITION'])
                with col2:
                    st.write("**Employment Type:**", customer['EMPLOYMENT_TYPE'])
                    st.write("**Income Range:**", customer['INCOME_RANGE'])
                with col3:
                    st.write("**Account Tier:**", customer['ACCOUNT_TIER'])
                    st.write("**Credit Score Band:**", customer['CREDIT_SCORE_BAND'])
                with col4:
                    st.write("**Risk Classification:**", customer['RISK_CLASSIFICATION'])

            with st.expander("Address"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write("**Street:**", customer['STREET_ADDRESS'])
                    st.write("**City:**", customer['CITY'])
                with col2:
                    st.write("**State:**", customer['STATE'])
                    st.write("**Zipcode:**", customer['ZIPCODE'])
                with col3:
                    st.write("**Country:**", customer['COUNTRY'])

            with st.expander("Account Portfolio"):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Accounts", customer['TOTAL_ACCOUNTS'])
                with col2:
                    st.metric("Checking", customer['CHECKING_ACCOUNTS'])
                with col3:
                    st.metric("Savings", customer['SAVINGS_ACCOUNTS'])
                with col4:
                    st.metric("Investment", customer['INVESTMENT_ACCOUNTS'])
                st.write("**Account Types:**", customer['ACCOUNT_TYPES'])
                st.write("**Currencies:**", customer['CURRENCIES'])

            with st.expander("Risk & Compliance"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Overall Risk Rating", customer['OVERALL_RISK_RATING'])
                    st.metric("Risk Score", f"{customer['OVERALL_RISK_SCORE']:.1f}")
                with col2:
                    st.write("**PEP Match Type:**", customer['EXPOSED_PERSON_MATCH_TYPE'])
                    st.write("**PEP Accuracy:**", f"{customer['EXPOSED_PERSON_MATCH_ACCURACY_PERCENT']:.0f}%")
                    st.write("**PEP Risk:**", customer['OVERALL_EXPOSED_PERSON_RISK'])
                with col3:
                    st.write("**Sanctions Match:**", customer['SANCTIONS_MATCH_TYPE'])
                    st.write("**Sanctions Accuracy:**", f"{customer['SANCTIONS_MATCH_ACCURACY_PERCENT']:.0f}%")
                    st.write("**Sanctions Risk:**", customer['OVERALL_SANCTIONS_RISK'])

                if customer['HIGH_RISK_CUSTOMER']:
                    st.error("HIGH RISK CUSTOMER - Enhanced monitoring required")
                if customer['REQUIRES_EXPOSED_PERSON_REVIEW']:
                    st.warning("Requires PEP Review")
                if customer['REQUIRES_SANCTIONS_REVIEW']:
                    st.warning("Requires Sanctions Review")
                if customer['HAS_ANOMALY']:
                    st.warning("Anomalous transaction pattern detected")
    else:
        st.warning("No customers found matching your criteria.")

except Exception as e:
    st.error(f"Error loading customer data: {str(e)}")
