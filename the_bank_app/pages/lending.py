import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from utils.data_loaders import (
    load_loan_portfolio_summary, load_loan_ltv_distribution,
    load_loan_application_funnel, load_loan_affordability_analysis,
    load_loan_compliance_screening, load_loan_customer_summary,
    load_lending_portfolio
)
from utils.visualizations import plot_credit_risk_distribution
from utils.ui_helpers import show_profile_picker

st.header("Lending & Loans")
st.caption("Retail Loans & Mortgages Portfolio Analysis")

with st.spinner("Loading loan portfolio data..."):
    df_portfolio = load_loan_portfolio_summary()
    df_ltv = load_loan_ltv_distribution()
    df_funnel = load_loan_application_funnel()
    df_affordability = load_loan_affordability_analysis()
    df_compliance = load_loan_compliance_screening()

if not df_portfolio.empty:
    total_apps = df_portfolio['LOAN_COUNT'].sum()
    total_amount = df_portfolio['TOTAL_REQUESTED_AMOUNT'].sum() / 1_000_000
    approved_count = df_portfolio[df_portfolio['APPLICATION_STATUS'] == 'APPROVED']['LOAN_COUNT'].sum()
    declined_count = df_portfolio[df_portfolio['APPLICATION_STATUS'] == 'DECLINED']['LOAN_COUNT'].sum()
    review_count = df_portfolio[df_portfolio['APPLICATION_STATUS'] == 'UNDER_REVIEW']['LOAN_COUNT'].sum()
    approval_rate = (approved_count / total_apps * 100) if total_apps > 0 else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Applications", f"{total_apps:,.0f}")
    with col2:
        st.metric("Total Amount", f"CHF {total_amount:,.0f}M")
    with col3:
        st.metric("Approved", f"{approved_count:,.0f}", f"{approval_rate:.1f}%")
    with col4:
        st.metric("Under Review", f"{review_count:,.0f}")
    with col5:
        st.metric("Declined", f"{declined_count:,.0f}")

    st.divider()

    loan_tab1, loan_tab2, loan_tab3, loan_tab4, loan_tab5, loan_tab6 = st.tabs([
        "Portfolio Overview", "LTV Analysis", "Application Funnel",
        "Affordability", "Compliance Screening", "Credit Risk"
    ])

    with loan_tab1:
        st.subheader("Portfolio by Country & Product")
        if not df_portfolio.empty:
            col1, col2 = st.columns(2)
            with col1:
                apps = df_portfolio.groupby(['COUNTRY', 'PRODUCT_TYPE'])['LOAN_COUNT'].sum().reset_index()
                fig = px.bar(apps, x='COUNTRY', y='LOAN_COUNT', color='PRODUCT_TYPE',
                           title='Applications by Country', color_discrete_sequence=px.colors.qualitative.Set2)
                st.plotly_chart(fig, width="stretch")
            with col2:
                amt = df_portfolio.groupby(['COUNTRY', 'PRODUCT_TYPE'])['TOTAL_REQUESTED_AMOUNT'].sum().reset_index()
                amt['AMT_M'] = amt['TOTAL_REQUESTED_AMOUNT'] / 1_000_000
                fig = px.bar(amt, x='COUNTRY', y='AMT_M', color='PRODUCT_TYPE',
                           title='Total Requested Amount (M CHF)', color_discrete_sequence=px.colors.qualitative.Set2)
                st.plotly_chart(fig, width="stretch")

            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                status = df_portfolio.groupby(['COUNTRY', 'APPLICATION_STATUS'])['LOAN_COUNT'].sum().reset_index()
                fig = px.bar(status, x='COUNTRY', y='LOAN_COUNT', color='APPLICATION_STATUS',
                           title='Status by Country', barmode='stack',
                           color_discrete_map={'APPROVED': '#28A745', 'DECLINED': '#DC3545', 'UNDER_REVIEW': '#FFC107'})
                st.plotly_chart(fig, width="stretch")
            with col2:
                avg = df_portfolio.groupby('COUNTRY')['AVG_REQUESTED_AMOUNT'].mean().reset_index()
                fig = px.bar(avg, x='COUNTRY', y='AVG_REQUESTED_AMOUNT', title='Avg Amount by Country',
                           color='AVG_REQUESTED_AMOUNT', color_continuous_scale='Blues')
                st.plotly_chart(fig, width="stretch")

            st.divider()
            st.dataframe(df_portfolio, width="stretch", height=400)

    with loan_tab2:
        st.subheader("Loan-to-Value (LTV) Distribution")
        if not df_ltv.empty:
            fig = px.bar(df_ltv, x='LTV_BUCKET', y='LOAN_COUNT', title='Loans by LTV Bucket',
                       color='AVG_LTV_PCT', color_continuous_scale='RdYlGn_r')
            st.plotly_chart(fig, width="stretch")
            col1, col2 = st.columns(2)
            with col1:
                d = df_ltv.copy()
                d['AMT_M'] = d['TOTAL_LOAN_AMOUNT'] / 1_000_000
                fig = px.bar(d, x='LTV_BUCKET', y='AMT_M', title='Amount by LTV (M CHF)',
                           color='LTV_BUCKET', color_discrete_sequence=px.colors.sequential.Reds)
                st.plotly_chart(fig, width="stretch")
            with col2:
                high = df_ltv[df_ltv['LTV_BUCKET'].isin(['80-90%', '>90%'])]
                if not high.empty:
                    fig = px.pie(high, values='LOAN_COUNT', names='LTV_BUCKET', title='High-Risk LTV (>80%)')
                    st.plotly_chart(fig, width="stretch")
                else:
                    st.success("No high-risk LTV applications (>80%)")
            st.dataframe(df_ltv, width="stretch", height=300)
        else:
            st.warning("No LTV data available")

    with loan_tab3:
        st.subheader("Application Funnel")
        if not df_funnel.empty:
            total = df_funnel['TOTAL_APPLICATIONS'].sum()
            approved = df_funnel['APPROVED_COUNT'].sum()
            declined = df_funnel['DECLINED_COUNT'].sum()
            review = df_funnel['UNDER_REVIEW_COUNT'].sum()
            funnel = pd.DataFrame({'Stage': ['Submitted', 'Under Review', 'Approved'], 'Count': [total, review, approved]})
            fig = px.funnel(funnel, x='Count', y='Stage', title='Application Funnel')
            st.plotly_chart(fig, width="stretch")
            col1, col2 = st.columns(2)
            with col1:
                s = df_funnel.groupby('COUNTRY').agg({'APPROVED_COUNT': 'sum', 'DECLINED_COUNT': 'sum', 'UNDER_REVIEW_COUNT': 'sum'}).reset_index()
                fig = px.bar(s, x='COUNTRY', y=['APPROVED_COUNT', 'UNDER_REVIEW_COUNT', 'DECLINED_COUNT'],
                           title='Status by Country', barmode='stack',
                           color_discrete_map={'APPROVED_COUNT': '#28A745', 'UNDER_REVIEW_COUNT': '#FFC107', 'DECLINED_COUNT': '#DC3545'})
                st.plotly_chart(fig, width="stretch")
            with col2:
                r = df_funnel.groupby('COUNTRY')['APPROVAL_RATE_PCT'].mean().reset_index()
                fig = px.bar(r, x='COUNTRY', y='APPROVAL_RATE_PCT', title='Approval Rate by Country (%)',
                           color='APPROVAL_RATE_PCT', color_continuous_scale='Greens')
                st.plotly_chart(fig, width="stretch")
            st.dataframe(df_funnel, width="stretch", height=400)
        else:
            st.warning("No funnel data available")

    with loan_tab4:
        st.subheader("Affordability Assessment")
        if not df_affordability.empty:
            agg = df_affordability.groupby('COUNTRY').agg({
                'AVG_DTI_RATIO_PCT': 'mean', 'AVG_DSTI_RATIO_PCT': 'mean',
                'AVG_GROSS_INCOME': 'mean', 'PASS_RATE_PCT': 'mean'
            }).reset_index()
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(agg, x='COUNTRY', y='AVG_DTI_RATIO_PCT', title='Avg DTI by Country (%)',
                           color='AVG_DTI_RATIO_PCT', color_continuous_scale='RdYlGn_r')
                fig.add_hline(y=45, line_dash="dash", line_color="red", annotation_text="45% Threshold")
                st.plotly_chart(fig, width="stretch")
            with col2:
                fig = px.bar(agg, x='COUNTRY', y='AVG_DSTI_RATIO_PCT', title='Avg DSTI by Country (%)',
                           color='AVG_DSTI_RATIO_PCT', color_continuous_scale='RdYlGn_r')
                fig.add_hline(y=33.33, line_dash="dash", line_color="orange", annotation_text="Swiss 33% Threshold")
                st.plotly_chart(fig, width="stretch")
            st.dataframe(df_affordability, width="stretch", height=300)
        else:
            st.warning("No affordability data available")

    with loan_tab5:
        st.subheader("Compliance & Risk Screening")
        if not df_compliance.empty:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Flagged", f"{len(df_compliance):,}")
            with col2:
                st.metric("Sanctions Review", f"{int(df_compliance['REQUIRES_SANCTIONS_REVIEW'].sum()):,}")
            with col3:
                st.metric("PEP Review", f"{int(df_compliance['REQUIRES_EXPOSED_PERSON_REVIEW'].sum()):,}")
            with col4:
                st.metric("Vulnerable", f"{int(df_compliance['VULNERABLE_CUSTOMER_FLAG'].sum()):,}")
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                cc = df_compliance['COMPLIANCE_STATUS'].value_counts().reset_index()
                cc.columns = ['Status', 'Count']
                fig = px.pie(cc, values='Count', names='Status', title='By Compliance Status')
                st.plotly_chart(fig, width="stretch")
            with col2:
                rc = df_compliance['OVERALL_RISK_RATING'].value_counts().reset_index()
                rc.columns = ['Risk', 'Count']
                fig = px.bar(rc, x='Risk', y='Count', title='By Risk Rating', color='Risk',
                           color_discrete_map={'CRITICAL': '#DC3545', 'HIGH': '#FF8C00', 'MEDIUM': '#FFC107', 'LOW': '#28A745'})
                st.plotly_chart(fig, width="stretch")
            st.dataframe(df_compliance, width="stretch", height=400)
        else:
            st.info("No applications flagged for compliance review")

    with loan_tab6:
        st.subheader("Credit Risk Overview")
        try:
            df_lending = load_lending_portfolio()
            if len(df_lending) > 0:
                col1, col2 = st.columns(2)
                with col1:
                    if 'CREDIT_SCORE_BAND' in df_lending.columns:
                        df_ws = df_lending[df_lending['CREDIT_SCORE_BAND'].notna()]
                        fig = plot_credit_risk_distribution(df_ws)
                        st.plotly_chart(fig, width="stretch")
                with col2:
                    if 'RISK_CLASSIFICATION' in df_lending.columns:
                        for risk, count in df_lending['RISK_CLASSIFICATION'].value_counts().items():
                            st.write(f"**{risk}:** {count}")
                display_cols = [c for c in ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'CREDIT_SCORE_BAND',
                                            'RISK_CLASSIFICATION', 'ACCOUNT_TIER'] if c in df_lending.columns]
                if display_cols:
                    df_ld = df_lending[display_cols].copy()
                    st.dataframe(df_ld, width="stretch", height=400, hide_index=True)
                    show_profile_picker(df_ld, "lending")
            else:
                st.info("No lending portfolio data available")
        except Exception as e:
            st.error(f"Error: {str(e)}")

else:
    st.warning("No loan portfolio data available. Check data pipeline.")
