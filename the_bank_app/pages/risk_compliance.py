import streamlit as st
import pandas as pd
from datetime import datetime
from utils.data_loaders import (
    load_customer_360, load_high_risk_customers, load_compliance_risk_summary,
    load_sanctions_matches, load_pep_matches, load_kyc_completeness
)
from utils.visualizations import (
    plot_risk_distribution, plot_risk_distribution_excluding_no_risk,
    plot_compliance_risk_heatmap, plot_sanctions_screening_results,
    plot_pep_screening_results
)
from utils.ui_helpers import show_profile_picker

st.header("Risk & Compliance")

tab_overview, tab_sanctions, tab_kyc = st.tabs(["Overview", "Sanctions Control", "KYC Screening"])

# --- Overview ---
with tab_overview:
    try:
        df_customers = load_customer_360()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            critical_high = len(df_customers[df_customers['OVERALL_RISK_RATING'].isin(['CRITICAL', 'HIGH'])])
            st.metric("Critical/High Risk", critical_high)
        with col2:
            pep_review = len(df_customers[df_customers['REQUIRES_EXPOSED_PERSON_REVIEW'] == True])
            st.metric("Requires PEP Review", pep_review)
        with col3:
            sanctions_review = len(df_customers[df_customers['REQUIRES_SANCTIONS_REVIEW'] == True])
            st.metric("Requires Sanctions Review", sanctions_review)
        with col4:
            high_risk_pct = (len(df_customers[df_customers['HIGH_RISK_CUSTOMER'] == True]) / len(df_customers) * 100)
            st.metric("High Risk %", f"{high_risk_pct:.1f}%")

        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            fig_risk_all = plot_risk_distribution(df_customers)
            st.plotly_chart(fig_risk_all, width="stretch", key="risk_all")
        with col2:
            fig_risk_only = plot_risk_distribution_excluding_no_risk(df_customers)
            st.plotly_chart(fig_risk_only, width="stretch", key="risk_excl")

        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            st.write("**PEP Screening Status**")
            pep_counts = df_customers['EXPOSED_PERSON_MATCH_TYPE'].value_counts()
            for match_type, count in pep_counts.items():
                icon = "🔴" if match_type == "EXACT_MATCH" else "🟡" if match_type == "FUZZY_MATCH" else "🟢"
                st.write(f"{icon} {match_type}: **{count}**")
        with col2:
            st.write("**Sanctions Screening Status**")
            sanctions_counts = df_customers['SANCTIONS_MATCH_TYPE'].value_counts()
            for match_type, count in sanctions_counts.items():
                icon = "🔴" if match_type == "EXACT_MATCH" else "🟡" if match_type == "FUZZY_MATCH" else "🟢"
                st.write(f"{icon} {match_type}: **{count}**")

        st.divider()

        st.subheader("High-Risk Customers Requiring Action")
        df_high_risk = load_high_risk_customers()
        if len(df_high_risk) > 0:
            st.warning(f"**{len(df_high_risk)}** customers require immediate compliance review")
            display_cols = [
                'CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'ACCOUNT_TIER',
                'OVERALL_RISK_RATING', 'OVERALL_RISK_SCORE',
                'EXPOSED_PERSON_MATCH_TYPE', 'SANCTIONS_MATCH_TYPE'
            ]
            df_hr = df_high_risk[display_cols].copy()
            df_hr['OVERALL_RISK_SCORE'] = df_hr['OVERALL_RISK_SCORE'].round(1)
            df_hr['ACTION_REQUIRED'] = df_high_risk.apply(
                lambda row: 'PEP Review' if row['REQUIRES_EXPOSED_PERSON_REVIEW'] else
                           'Sanctions Review' if row['REQUIRES_SANCTIONS_REVIEW'] else
                           'Risk Assessment', axis=1
            )
            st.dataframe(df_hr, width="stretch", height=400, hide_index=True)
            show_profile_picker(df_hr, "high_risk")
            csv = df_hr.to_csv(index=False)
            st.download_button("Export High-Risk List (CSV)", csv,
                             file_name=f"high_risk_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
        else:
            st.success("No high-risk customers requiring immediate action")

        st.divider()
        metrics = load_compliance_risk_summary()
        if metrics:
            col1, col2 = st.columns(2)
            with col1:
                fig_risk = plot_risk_distribution(df_customers)
                st.plotly_chart(fig_risk, width="stretch", key="comp_risk_overview")
            with col2:
                fig_heatmap = plot_compliance_risk_heatmap(df_customers)
                st.plotly_chart(fig_heatmap, width="stretch", key="comp_heatmap")

    except Exception as e:
        st.error(f"Error loading risk data: {str(e)}")

# --- Sanctions ---
with tab_sanctions:
    try:
        df_sanctions = load_sanctions_matches()
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Sanctions Matches", len(df_sanctions))
        with col2:
            if len(df_sanctions) > 0 and 'SANCTIONS_MATCH_TYPE' in df_sanctions.columns:
                exact = len(df_sanctions[df_sanctions['SANCTIONS_MATCH_TYPE'] == 'EXACT_MATCH'])
                st.metric("Exact Matches", exact)
        with col3:
            if len(df_sanctions) > 0 and 'REQUIRES_SANCTIONS_REVIEW' in df_sanctions.columns:
                st.metric("Requires Review", int(df_sanctions['REQUIRES_SANCTIONS_REVIEW'].sum()))
        with col4:
            if len(df_sanctions) > 0 and 'OVERALL_SANCTIONS_RISK' in df_sanctions.columns:
                high = len(df_sanctions[df_sanctions['OVERALL_SANCTIONS_RISK'].isin(['CRITICAL', 'HIGH'])])
                st.metric("High Risk", high)

        if len(df_sanctions) > 0:
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                fig_s = plot_sanctions_screening_results(df_sanctions)
                st.plotly_chart(fig_s, width="stretch", key="sanctions_results")
            with col2:
                if 'OVERALL_SANCTIONS_RISK' in df_sanctions.columns:
                    for risk, count in df_sanctions['OVERALL_SANCTIONS_RISK'].value_counts().items():
                        icon = "🔴" if risk in ['CRITICAL', 'HIGH'] else "🟡" if risk == 'MEDIUM' else "🟢"
                        st.write(f"{icon} **{risk}:** {count}")
            st.divider()
            display_cols = [c for c in ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'SANCTIONS_MATCH_TYPE',
                                        'SANCTIONS_MATCH_ACCURACY_PERCENT', 'OVERALL_SANCTIONS_RISK']
                           if c in df_sanctions.columns]
            if display_cols:
                df_s = df_sanctions[display_cols].copy()
                st.dataframe(df_s, width="stretch", height=400, hide_index=True)
                show_profile_picker(df_s, "sanctions")
        else:
            st.success("No sanctions matches found")
    except Exception as e:
        st.error(f"Error loading sanctions data: {str(e)}")

# --- KYC ---
with tab_kyc:
    try:
        df_pep = load_pep_matches()
        df_kyc = load_kyc_completeness()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("PEP Matches", len(df_pep))
        with col2:
            if len(df_pep) > 0 and 'REQUIRES_EXPOSED_PERSON_REVIEW' in df_pep.columns:
                st.metric("Requires PEP Review", int(df_pep['REQUIRES_EXPOSED_PERSON_REVIEW'].sum()))
        with col3:
            if len(df_pep) > 0 and 'EXPOSED_PERSON_MATCH_TYPE' in df_pep.columns:
                exact = len(df_pep[df_pep['EXPOSED_PERSON_MATCH_TYPE'] == 'EXACT_MATCH'])
                st.metric("Exact PEP Matches", exact)
        with col4:
            if len(df_kyc) > 0 and 'TOTAL_CUSTOMERS' in df_kyc.columns:
                st.metric("Total Customers", f"{df_kyc['TOTAL_CUSTOMERS'].sum():,.0f}")

        if len(df_pep) > 0:
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                fig_pep = plot_pep_screening_results(df_pep)
                st.plotly_chart(fig_pep, width="stretch", key="pep_results")
            with col2:
                if 'OVERALL_EXPOSED_PERSON_RISK' in df_pep.columns:
                    for risk, count in df_pep['OVERALL_EXPOSED_PERSON_RISK'].value_counts().items():
                        icon = "🔴" if risk in ['CRITICAL', 'HIGH'] else "🟡" if risk == 'MEDIUM' else "🟢"
                        st.write(f"{icon} **{risk}:** {count}")
            st.divider()
            display_cols = [c for c in ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'EXPOSED_PERSON_MATCH_TYPE',
                                        'EXPOSED_PERSON_MATCH_ACCURACY_PERCENT', 'OVERALL_EXPOSED_PERSON_RISK']
                           if c in df_pep.columns]
            if display_cols:
                df_p = df_pep[display_cols].copy()
                st.dataframe(df_p, width="stretch", height=400, hide_index=True)
                show_profile_picker(df_p, "pep")

        if len(df_kyc) > 0:
            st.divider()
            st.subheader("KYC Data Completeness by Country")
            st.dataframe(df_kyc, width="stretch", height=300, hide_index=True)

    except Exception as e:
        st.error(f"Error loading KYC data: {str(e)}")
