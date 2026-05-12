"""
Synthetic Retail Bank - Streamlit Prototype
AAA Synthetic Bank

A comprehensive customer intelligence dashboard providing 360° customer views,
risk assessment, compliance monitoring, and AI-powered natural language queries.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os
import json
import re

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Page configuration
st.set_page_config(
    page_title="Synthetic Retail Bank",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional banking UI
st.markdown("""
    <style>
    .main {
        background-color: #F8F9FA;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 10px 20px;
        background-color: #FFFFFF;
        border-radius: 5px 5px 0px 0px;
        border: 1px solid #dee2e6;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #003366;
        color: white;
    }
    h1 {
        color: #003366;
    }
    .metric-card {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .risk-critical {
        color: #DC3545;
        font-weight: bold;
    }
    .risk-high {
        color: #FF8C00;
        font-weight: bold;
    }
    .risk-medium {
        color: #FFC107;
        font-weight: bold;
    }
    .risk-low {
        color: #28A745;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

# Import utility functions
from utils.snowflake_connection import get_snowflake_session, test_connection
from utils.agent_caller import call_agent_rest_api
from utils.data_loaders import (
    load_customer_360,
    load_high_risk_customers,
    load_risk_distribution,
    load_pep_sanctions_summary,
    # New data loaders
    load_aml_alerts,
    load_aml_metrics,
    load_lending_portfolio,
    load_wealth_portfolios,
    load_advisor_performance,
    load_sanctions_matches,
    load_advisor_capacity,
    load_team_performance,
    load_pep_matches,
    load_kyc_completeness,
    load_data_quality_metrics,
    load_compliance_risk_summary,
    # Loan portfolio data loaders
    load_loan_portfolio_summary,
    load_loan_ltv_distribution,
    load_loan_application_funnel,
    load_loan_affordability_analysis,
    load_loan_compliance_screening,
    load_loan_customer_summary,
    # Lifecycle data loaders
    load_customer_lifecycle,
    load_lifecycle_summary,
    load_high_churn_risk_customers,
    load_premium_at_risk,
    load_dormant_accounts,
    calculate_revenue_at_risk,
    # LCR data loaders
    load_lcr_current_status,
    load_lcr_trend,
    load_hqla_holdings_detail,
    load_deposit_outflows_detail,
    load_lcr_alerts,
    load_lcr_monthly_summary
)
from utils.visualizations import (
    plot_risk_distribution,
    plot_risk_distribution_excluding_no_risk,
    plot_account_tier_distribution,
    plot_geographic_distribution,
    # New visualization functions
    plot_aml_alert_trend,
    plot_credit_risk_distribution,
    plot_advisor_aum_distribution,
    plot_advisor_capacity,
    plot_sanctions_screening_results,
    plot_pep_screening_results,
    plot_data_quality_completeness,
    plot_compliance_risk_heatmap,
    # Lifecycle visualization functions
    plot_lifecycle_stage_distribution,
    plot_churn_probability_distribution,
    plot_lifecycle_revenue,
    plot_churn_risk_by_tier,
    plot_revenue_at_risk_gauge,
    plot_days_inactive_distribution,
    # LCR visualization functions
    plot_lcr_trend,
    plot_hqla_composition,
    plot_hqla_by_asset_type,
    plot_deposit_outflows_by_type,
    plot_lcr_gauge,
    plot_hqla_vs_outflows,
    plot_monthly_compliance_trend
)

# Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/200x60/003366/FFFFFF?text=AAA+Bank", width="stretch")
    st.markdown("### 🏦 Synthetic Retail Bank")
    st.markdown("---")
    
    # Connection status
    st.markdown("#### Connection Status")
    try:
        session = get_snowflake_session()
        st.success("✅ Connected to Snowflake")
        st.caption(f"Database: AAA_DEV_SYNTHETIC_BANK")
        st.caption(f"Schema: CRM_AGG_001")
    except Exception as e:
        st.error("❌ Connection failed")
        st.caption(str(e))
    
    st.markdown("---")
    
    # Data freshness
    st.markdown("#### Data Freshness")
    st.caption(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    
    # Quick filters
    st.markdown("#### Quick Filters")
    show_high_risk_only = st.checkbox("High-Risk Only", value=False)
    show_pep_matches = st.checkbox("PEP Matches", value=False)
    
    st.markdown("---")
    st.caption("Version 1.0.0 | 2025")

# Main header
st.title("🏦 Synthetic Retail Bank")
st.markdown("**Comprehensive Customer Intelligence • Risk Assessment • Compliance Monitoring**")
st.markdown("---")

# Create tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13, tab14, tab15, tab16 = st.tabs([
    "Customer 360°",
    "Risk & Compliance",
    "Portfolio Analytics",
    "Fraud Detection",
    "Churn & Lifecycle",
    "AML Monitoring",
    "Lending Operations",
    "Wealth Management",
    "Sanctions Control",
    "Advisor Management",
    "KYC Screening",
    "Data Quality",
    "LCR Monitoring",
    "Loans Portfolio",
    "Ask AI",
    "Settings"
])

# ============================================================
# TAB 1: Customer 360° Search
# ============================================================
with tab1:
    st.header("Customer 360° Search")
    
    # Load data
    try:
        df_customers = load_customer_360()
        
        # Key metrics at top
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Customers", len(df_customers))
        with col2:
            high_risk_count = len(df_customers[df_customers['HIGH_RISK_CUSTOMER'] == True])
            st.metric("High Risk Customers", high_risk_count)
        with col3:
            pep_count = len(df_customers[df_customers['EXPOSED_PERSON_MATCH_TYPE'] != 'NO_MATCH'])
            st.metric("PEP Matches", pep_count)
        with col4:
            avg_risk = df_customers['OVERALL_RISK_SCORE'].mean()
            st.metric("Avg Risk Score", f"{avg_risk:.1f}")
        
        st.markdown("---")
        
        # Search and filter section
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            search_name = st.text_input("🔍 Search by Name", placeholder="Enter customer name...")
        with col2:
            search_country = st.selectbox("Country", ["All"] + sorted(df_customers['COUNTRY'].unique().tolist()))
        with col3:
            search_tier = st.multiselect("Account Tier", df_customers['ACCOUNT_TIER'].unique().tolist())
        with col4:
            search_risk = st.selectbox("Risk Level", ["All"] + sorted(df_customers['OVERALL_RISK_RATING'].unique().tolist()))
        
        # Quick filters
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            filter_high_risk = st.checkbox("High-Risk Only")
        with col2:
            filter_pep = st.checkbox("PEP Matches")
        with col3:
            filter_requires_review = st.checkbox("Requires Review")
        with col4:
            filter_anomaly = st.checkbox("Anomaly Flagged")
        
        # Apply filters
        df_filtered = df_customers.copy()
        
        if search_name:
            df_filtered = df_filtered[df_filtered['FULL_NAME'].str.contains(search_name, case=False, na=False)]
        
        if search_country != "All":
            df_filtered = df_filtered[df_filtered['COUNTRY'] == search_country]
        
        if search_tier:
            df_filtered = df_filtered[df_filtered['ACCOUNT_TIER'].isin(search_tier)]
        
        if search_risk != "All":
            df_filtered = df_filtered[df_filtered['OVERALL_RISK_RATING'] == search_risk]
        
        if filter_high_risk:
            df_filtered = df_filtered[df_filtered['HIGH_RISK_CUSTOMER'] == True]
        
        if filter_pep:
            df_filtered = df_filtered[df_filtered['EXPOSED_PERSON_MATCH_TYPE'] != 'NO_MATCH']
        
        if filter_requires_review:
            df_filtered = df_filtered[
                (df_filtered['REQUIRES_EXPOSED_PERSON_REVIEW'] == True) |
                (df_filtered['REQUIRES_SANCTIONS_REVIEW'] == True)
            ]
        
        if filter_anomaly:
            df_filtered = df_filtered[df_filtered['HAS_ANOMALY'] == True]
        
        st.info(f"📊 Found **{len(df_filtered)}** customers matching your criteria")
        
        # Display results table
        if len(df_filtered) > 0:
            # Select columns to display
            display_cols = [
                'CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'ACCOUNT_TIER',
                'OVERALL_RISK_RATING', 'OVERALL_RISK_SCORE',
                'EXPOSED_PERSON_MATCH_TYPE', 'SANCTIONS_MATCH_TYPE'
            ]
            
            df_display = df_filtered[display_cols].copy()
            
            # Format display
            df_display['OVERALL_RISK_SCORE'] = df_display['OVERALL_RISK_SCORE'].round(1)
            
            st.dataframe(
                df_display,
                width="stretch",
                height=400,
                hide_index=True
            )
            
            # Customer detail view
            st.markdown("---")
            st.subheader("Customer 360° Profile")
            
            selected_customer_id = st.selectbox(
                "Select a customer to view detailed profile:",
                df_filtered['CUSTOMER_ID'].tolist(),
                format_func=lambda x: f"{x} - {df_filtered[df_filtered['CUSTOMER_ID']==x]['FULL_NAME'].iloc[0]}"
            )
            
            if selected_customer_id:
                customer = df_filtered[df_filtered['CUSTOMER_ID'] == selected_customer_id].iloc[0]
                
                # Create expandable sections
                with st.expander("👤 Identity & Demographics", expanded=True):
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
                        st.write("**Reporting Currency:**", customer['REPORTING_CURRENCY'])
                        st.write("**Current Status:**", customer['CURRENT_STATUS'])
                
                with st.expander("📞 Contact Information"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.write("**Email:**", customer['EMAIL'])
                    with col2:
                        st.write("**Phone:**", customer['PHONE'])
                    with col3:
                        st.write("**Preferred Method:**", customer['PREFERRED_CONTACT_METHOD'])
                
                with st.expander("💼 Employment & Financial Profile"):
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
                
                with st.expander("📍 Address Information"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.write("**Street Address:**", customer['STREET_ADDRESS'])
                        st.write("**City:**", customer['CITY'])
                    with col2:
                        st.write("**State:**", customer['STATE'])
                        st.write("**Zipcode:**", customer['ZIPCODE'])
                    with col3:
                        st.write("**Country:**", customer['COUNTRY'])
                        st.write("**Effective Date:**", customer['ADDRESS_EFFECTIVE_DATE'])
                
                with st.expander("🏦 Account Portfolio"):
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
                
                with st.expander("🛡️ Risk & Compliance"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        risk_color = {
                            'CRITICAL': '🔴',
                            'HIGH': '🟠',
                            'MEDIUM': '🟡',
                            'LOW': '🟢',
                            'NO_RISK': '🔵'
                        }.get(customer['OVERALL_RISK_RATING'], '⚪')
                        st.metric("Overall Risk Rating", f"{risk_color} {customer['OVERALL_RISK_RATING']}")
                        st.metric("Overall Risk Score", f"{customer['OVERALL_RISK_SCORE']:.1f}")
                    with col2:
                        st.write("**PEP Match Type:**", customer['EXPOSED_PERSON_MATCH_TYPE'])
                        st.write("**PEP Match Accuracy:**", f"{customer['EXPOSED_PERSON_MATCH_ACCURACY_PERCENT']:.0f}%")
                        st.write("**PEP Risk:**", customer['OVERALL_EXPOSED_PERSON_RISK'])
                    with col3:
                        st.write("**Sanctions Match:**", customer['SANCTIONS_MATCH_TYPE'])
                        st.write("**Sanctions Accuracy:**", f"{customer['SANCTIONS_MATCH_ACCURACY_PERCENT']:.0f}%")
                        st.write("**Sanctions Risk:**", customer['OVERALL_SANCTIONS_RISK'])
                    
                    # Action flags
                    if customer['HIGH_RISK_CUSTOMER']:
                        st.error("⚠️ **HIGH RISK CUSTOMER** - Enhanced monitoring required")
                    if customer['REQUIRES_EXPOSED_PERSON_REVIEW']:
                        st.warning("⚠️ Requires PEP Review")
                    if customer['REQUIRES_SANCTIONS_REVIEW']:
                        st.warning("⚠️ Requires Sanctions Review")
                    if customer['HAS_ANOMALY']:
                        st.warning("🚩 Anomalous transaction pattern detected")
        
        else:
            st.warning("No customers found matching your criteria. Try adjusting the filters.")
    
    except Exception as e:
        st.error(f"Error loading customer data: {str(e)}")
        st.info("💡 Make sure your Snowflake connection is configured correctly in `.streamlit/secrets.toml`")

# ============================================================
# TAB 2: Risk & Compliance Dashboard
# ============================================================
with tab2:
    st.header("Risk & Compliance Dashboard")
    
    try:
        df_customers = load_customer_360()
        
        # Key compliance metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            critical_high = len(df_customers[df_customers['OVERALL_RISK_RATING'].isin(['CRITICAL', 'HIGH'])])
            st.metric("Critical/High Risk", critical_high, delta=None, delta_color="inverse")
        with col2:
            pep_review = len(df_customers[df_customers['REQUIRES_EXPOSED_PERSON_REVIEW'] == True])
            st.metric("Requires PEP Review", pep_review)
        with col3:
            sanctions_review = len(df_customers[df_customers['REQUIRES_SANCTIONS_REVIEW'] == True])
            st.metric("Requires Sanctions Review", sanctions_review)
        with col4:
            high_risk_pct = (len(df_customers[df_customers['HIGH_RISK_CUSTOMER'] == True]) / len(df_customers) * 100)
            st.metric("High Risk %", f"{high_risk_pct:.1f}%")
        
        st.markdown("---")
        
        # Risk Distribution Visualizations
        st.subheader("Risk Distribution Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_risk_all = plot_risk_distribution(df_customers)
            st.plotly_chart(fig_risk_all, width="stretch", key="risk_distribution_all")
        
        with col2:
            fig_risk_only = plot_risk_distribution_excluding_no_risk(df_customers)
            st.plotly_chart(fig_risk_only, width="stretch", key="risk_distribution_no_risk_excluded")
        
        st.markdown("---")
        
        # PEP & Sanctions Screening
        st.subheader("PEP & Sanctions Screening")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # PEP breakdown
            st.write("**PEP Screening Status**")
            pep_counts = df_customers['EXPOSED_PERSON_MATCH_TYPE'].value_counts()
            for match_type, count in pep_counts.items():
                icon = "🔴" if match_type == "EXACT_MATCH" else "🟡" if match_type == "FUZZY_MATCH" else "🟢"
                st.write(f"{icon} {match_type}: **{count}**")
        
        with col2:
            # Sanctions breakdown
            st.write("**Sanctions Screening Status**")
            sanctions_counts = df_customers['SANCTIONS_MATCH_TYPE'].value_counts()
            for match_type, count in sanctions_counts.items():
                icon = "🔴" if match_type == "EXACT_MATCH" else "🟡" if match_type == "FUZZY_MATCH" else "🟢"
                st.write(f"{icon} {match_type}: **{count}**")
        
        st.markdown("---")
        
        # High-risk customers requiring action
        st.subheader("High-Risk Customers Requiring Immediate Action")
        
        df_high_risk = load_high_risk_customers()
        
        if len(df_high_risk) > 0:
            st.warning(f"**{len(df_high_risk)}** customers require immediate compliance review")
            
            # Display high-risk table
            display_cols = [
                'CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'ACCOUNT_TIER',
                'OVERALL_RISK_RATING', 'OVERALL_RISK_SCORE',
                'EXPOSED_PERSON_MATCH_TYPE', 'SANCTIONS_MATCH_TYPE'
            ]
            
            df_high_risk_display = df_high_risk[display_cols].copy()
            df_high_risk_display['OVERALL_RISK_SCORE'] = df_high_risk_display['OVERALL_RISK_SCORE'].round(1)
            
            # Add action column
            df_high_risk_display['ACTION_REQUIRED'] = df_high_risk.apply(
                lambda row: 'PEP Review' if row['REQUIRES_EXPOSED_PERSON_REVIEW'] else 
                           'Sanctions Review' if row['REQUIRES_SANCTIONS_REVIEW'] else 
                           'Risk Assessment', axis=1
            )
            
            st.dataframe(
                df_high_risk_display,
                width="stretch",
                height=400,
                hide_index=True
            )
            
            # Export button
            col1, col2, col3 = st.columns([1, 1, 4])
            with col1:
                csv = df_high_risk_display.to_csv(index=False)
                st.download_button(
                    label="📥 Export High-Risk List (CSV)",
                    data=csv,
                    file_name=f"high_risk_customers_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            with col2:
                st.download_button(
                    label="📄 Compliance Report (PDF)",
                    data="PDF generation not implemented in prototype",
                    file_name=f"compliance_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    disabled=True
                )
        else:
            st.success("✅ No high-risk customers requiring immediate action")
        
        st.markdown("---")
        
        # Compliance Risk Management Section
        st.subheader("Compliance Risk Management")
        
        metrics = load_compliance_risk_summary()
        
        if metrics:
            # Additional compliance action items
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("PEP Reviews Needed", metrics.get('PEP_REVIEWS_NEEDED', 0))
            with col2:
                st.metric("Sanctions Reviews Needed", metrics.get('SANCTIONS_REVIEWS_NEEDED', 0))
            with col3:
                st.metric("Anomaly Flags", metrics.get('ANOMALY_COUNT', 0))
            
            st.markdown("---")
            
            # Risk visualization with heatmap
            st.subheader("Geographic Risk Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Overall Risk Distribution**")
                fig_risk = plot_risk_distribution(df_customers)
                st.plotly_chart(fig_risk, width="stretch", key="compliance_risk_distribution_overview")
            
            with col2:
                st.write("**Risk by Geography**")
                fig_heatmap = plot_compliance_risk_heatmap(df_customers)
                st.plotly_chart(fig_heatmap, width="stretch", key="compliance_risk_heatmap")
            
            st.markdown("---")
            
            # Risk profile summary
            st.subheader("Risk Profile Summary")
            st.write(f"**Average Risk Score:** {metrics.get('AVG_RISK_SCORE', 0):.2f}")
            st.write(f"**Total Customers:** {metrics.get('TOTAL_CUSTOMERS', 0):,.0f}")
            
            high_risk = metrics.get('HIGH_RISK_COUNT', 0)
            high_risk_pct = (high_risk / max(metrics.get('TOTAL_CUSTOMERS', 1), 1)) * 100
            st.write(f"**High Risk Customers:** {high_risk:,.0f} ({high_risk_pct:.1f}%)")
            
            # Risk appetite framework
            st.info("""
            **Risk Appetite Framework:**
            - 🟢 **Within Appetite:** <5% high-risk customers
            - 🟡 **Near Limit:** 5-10% high-risk customers
            - 🔴 **Exceeds Appetite:** >10% high-risk customers (requires Board escalation)
            """)
    
    except Exception as e:
        st.error(f"Error loading risk data: {str(e)}")

# ============================================================
# TAB 3: Portfolio Analytics
# ============================================================
with tab3:
    st.header("Portfolio Analytics")
    
    try:
        df_customers = load_customer_360()
        
        # Key portfolio metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            avg_accounts = df_customers['TOTAL_ACCOUNTS'].mean()
            st.metric("Avg Accounts per Customer", f"{avg_accounts:.1f}")
        with col2:
            multi_currency = len(df_customers[df_customers['CURRENCIES'].str.contains(',', na=False)])
            st.metric("Multi-Currency Customers", multi_currency)
        with col3:
            premium_count = len(df_customers[df_customers['ACCOUNT_TIER'].isin(['PREMIUM', 'PLATINUM'])])
            st.metric("Premium/Platinum Customers", premium_count)
        with col4:
            investment_holders = len(df_customers[df_customers['INVESTMENT_ACCOUNTS'] > 0])
            st.metric("Investment Account Holders", investment_holders)
        
        st.markdown("---")
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Account Tier Distribution")
            fig_tier = plot_account_tier_distribution(df_customers)
            st.plotly_chart(fig_tier, width="stretch", key="account_tier_distribution")
        
        with col2:
            st.subheader("Geographic Distribution")
            fig_geo = plot_geographic_distribution(df_customers)
            st.plotly_chart(fig_geo, width="stretch", key="geographic_distribution")
        
        st.markdown("---")
        
        # Account holdings analysis
        st.subheader("Account Type Holdings by Tier")
        
        account_summary = df_customers.groupby('ACCOUNT_TIER').agg({
            'TOTAL_ACCOUNTS': 'mean',
            'CHECKING_ACCOUNTS': 'mean',
            'SAVINGS_ACCOUNTS': 'mean',
            'BUSINESS_ACCOUNTS': 'mean',
            'INVESTMENT_ACCOUNTS': 'mean'
        }).round(1)
        
        st.dataframe(account_summary, width="stretch")
        
    except Exception as e:
        st.error(f"Error loading portfolio data: {str(e)}")

# ============================================================
# TAB 4: Fraud Detection
# ============================================================
with tab4:
    st.header("Fraud & Anomaly Detection")
    
    try:
        df_customers = load_customer_360()
        
        # Fraud metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            total_customers = len(df_customers)
            st.metric("Total Customers", total_customers)
        with col2:
            anomalous = len(df_customers[df_customers['HAS_ANOMALY'] == True])
            st.metric("Anomalous Customers", anomalous)
        with col3:
            high_risk_anomaly = len(df_customers[
                (df_customers['HAS_ANOMALY'] == True) & 
                (df_customers['HIGH_RISK_CUSTOMER'] == True)
            ])
            st.metric("High-Risk + Anomaly", high_risk_anomaly)
        with col4:
            anomaly_rate = (anomalous / total_customers * 100) if total_customers > 0 else 0
            st.metric("Anomaly Rate", f"{anomaly_rate:.1f}%")
        
        st.markdown("---")
        
        # Anomaly priority queue
        st.subheader("Anomaly Priority Queue")
        
        df_anomalies = df_customers[df_customers['HAS_ANOMALY'] == True].sort_values('OVERALL_RISK_SCORE', ascending=False)
        
        if len(df_anomalies) > 0:
            st.warning(f"**{len(df_anomalies)}** customers flagged with anomalous transaction patterns")
            
            display_cols = [
                'CUSTOMER_ID', 'FULL_NAME', 'ACCOUNT_TIER', 'COUNTRY',
                'OVERALL_RISK_RATING', 'OVERALL_RISK_SCORE', 'HAS_ANOMALY'
            ]
            
            df_anomaly_display = df_anomalies[display_cols].copy()
            df_anomaly_display['OVERALL_RISK_SCORE'] = df_anomaly_display['OVERALL_RISK_SCORE'].round(1)
            df_anomaly_display['HAS_ANOMALY'] = df_anomaly_display['HAS_ANOMALY'].apply(lambda x: '✓' if x else '')
            
            st.dataframe(
                df_anomaly_display,
                width="stretch",
                height=400,
                hide_index=True
            )
            
            # Export
            csv = df_anomaly_display.to_csv(index=False)
            st.download_button(
                label="📥 Export AML Report (CSV)",
                data=csv,
                file_name=f"anomaly_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.success("✅ No anomalous transaction patterns detected")
    
    except Exception as e:
        st.error(f"Error loading fraud data: {str(e)}")

# ============================================================
# TAB 5: Churn & Lifecycle Management
# ============================================================
with tab5:
    st.header("Churn & Lifecycle Management")
    
    try:
        # Load lifecycle data
        df_lifecycle = load_customer_lifecycle()
        df_summary = load_lifecycle_summary()
        revenue_metrics = calculate_revenue_at_risk()
        
        if len(df_lifecycle) > 0:
            # Key metrics at top
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Customers", len(df_lifecycle))
            with col2:
                if revenue_metrics:
                    at_risk = revenue_metrics.get('AT_RISK_CUSTOMERS', 0)
                    st.metric("At-Risk Customers (>70%)", at_risk, delta_color="inverse")
            with col3:
                if revenue_metrics:
                    revenue_at_risk = revenue_metrics.get('TOTAL_REVENUE_AT_RISK', 0)
                    st.metric("Revenue at Risk", f"CHF {revenue_at_risk:,.0f}K", delta_color="inverse")
            with col4:
                dormant = len(df_lifecycle[df_lifecycle['DAYS_SINCE_LAST_TRANSACTION'] > 180]) if 'DAYS_SINCE_LAST_TRANSACTION' in df_lifecycle.columns else 0
                st.metric("Dormant Accounts (>180d)", dormant, delta_color="inverse")
            
            st.markdown("---")
            
            # Lifecycle stage distribution
            st.subheader("Lifecycle Stage Distribution")
            
            if len(df_summary) > 0:
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    fig_lifecycle = plot_lifecycle_stage_distribution(df_summary)
                    st.plotly_chart(fig_lifecycle, width="stretch", key="lifecycle_stage_distribution")
                
                with col2:
                    st.write("**Stage Summary:**")
                    for _, row in df_summary.iterrows():
                        stage_icon = {
                            'NEW': '🆕',
                            'ACTIVE': '✅',
                            'MATURE': '⭐',
                            'DECLINING': '⚠️',
                            'DORMANT': '😴',
                            'CHURNED': '❌'
                        }.get(row['LIFECYCLE_STAGE'], '•')
                        st.write(f"{stage_icon} **{row['LIFECYCLE_STAGE']}**: {row['CUSTOMER_COUNT']:,.0f}")
            
            st.markdown("---")
            
            # Risk analysis
            st.subheader("Customer Risk Analysis by Lifecycle Stage")
            
            if revenue_metrics:
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("At-Risk Customers", revenue_metrics.get('AT_RISK_CUSTOMERS', 0))
                with col2:
                    st.metric("Critical Risk (>90%)", revenue_metrics.get('CRITICAL_CUSTOMERS', 0), delta_color="inverse")
                with col3:
                    st.metric("High Risk (70-90%)", revenue_metrics.get('HIGH_CUSTOMERS', 0), delta_color="inverse")
            
            st.markdown("---")
            
            # Churn probability analysis
            st.subheader("Churn Probability Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_churn_dist = plot_churn_probability_distribution(df_lifecycle)
                st.plotly_chart(fig_churn_dist, width="stretch", key="churn_probability_distribution")
            
            with col2:
                fig_churn_tier = plot_churn_risk_by_tier(df_lifecycle)
                st.plotly_chart(fig_churn_tier, width="stretch", key="churn_risk_by_tier")
            
            st.markdown("---")
            
            # At-risk premium customers (GOLD/PLATINUM with >70% churn)
            st.subheader("Premium Customers at Risk (GOLD/PLATINUM)")
            
            df_premium_risk = load_premium_at_risk()
            
            if len(df_premium_risk) > 0:
                st.error(f"**{len(df_premium_risk)} premium customers** at high risk of churning (>70% probability)")
                
                display_cols = [col for col in ['CUSTOMER_ID', 'FIRST_NAME', 'FAMILY_NAME', 'ACCOUNT_TIER', 'COUNTRY',
                                                 'CHURN_PROBABILITY', 'DAYS_SINCE_LAST_TRANSACTION', 'EMAIL', 'PHONE'] 
                                if col in df_premium_risk.columns]
                
                if display_cols:
                    df_display = df_premium_risk[display_cols].copy()
                    if 'CHURN_PROBABILITY' in df_display.columns:
                        df_display['CHURN_PROBABILITY'] = df_display['CHURN_PROBABILITY'].round(1)
                    
                    st.dataframe(df_display, width="stretch", height=400, hide_index=True)
                
                # Export button
                csv = df_premium_risk.to_csv(index=False)
                st.download_button(
                    label="📥 Export Premium At-Risk List (CSV)",
                    data=csv,
                    file_name=f"premium_at_risk_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            else:
                st.success("✅ No premium customers currently at high risk of churning")
            
            st.markdown("---")
            
            # Dormant account reactivation
            st.subheader("Dormant Account Reactivation (>180 Days Inactive)")
            
            df_dormant = load_dormant_accounts()
            
            if len(df_dormant) > 0:
                st.warning(f"**{len(df_dormant)} customers** have been inactive for more than 180 days")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_inactive = plot_days_inactive_distribution(df_dormant)
                    st.plotly_chart(fig_inactive, width="stretch", key="days_inactive_distribution")
                
                with col2:
                    st.write("**Dormant Account Statistics:**")
                    if 'DAYS_SINCE_LAST_TRANSACTION' in df_dormant.columns:
                        avg_days = df_dormant['DAYS_SINCE_LAST_TRANSACTION'].mean()
                        max_days = df_dormant['DAYS_SINCE_LAST_TRANSACTION'].max()
                        st.write(f"• **Average Days Inactive:** {avg_days:.0f} days")
                        st.write(f"• **Longest Inactive:** {max_days:.0f} days")
                    
                    
                    if 'ACCOUNT_TIER' in df_dormant.columns:
                        premium_dormant = len(df_dormant[df_dormant['ACCOUNT_TIER'].isin(['GOLD', 'PLATINUM'])])
                        st.write(f"• **Premium Accounts:** {premium_dormant}")
                
                st.markdown("---")
                
                # Dormant accounts table
                display_cols = [col for col in ['CUSTOMER_ID', 'FIRST_NAME', 'FAMILY_NAME', 'ACCOUNT_TIER', 'COUNTRY',
                                                 'DAYS_SINCE_LAST_TRANSACTION', 'LAST_TRANSACTION_DATE',
                                                 'CHURN_PROBABILITY', 'EMAIL', 'PHONE'] 
                                if col in df_dormant.columns]
                
                if display_cols:
                    df_display = df_dormant[display_cols].copy()
                    if 'CHURN_PROBABILITY' in df_display.columns:
                        df_display['CHURN_PROBABILITY'] = df_display['CHURN_PROBABILITY'].round(1)
                    
                    st.dataframe(df_display.head(100), width="stretch", height=400, hide_index=True)
                
                # Export button
                csv = df_dormant.to_csv(index=False)
                st.download_button(
                    label="📥 Export Dormant Accounts (CSV)",
                    data=csv,
                    file_name=f"dormant_accounts_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            else:
                st.success("✅ No dormant accounts requiring reactivation")
            
            st.markdown("---")
            
            # Action recommendations
            st.subheader("Recommended Actions")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.info("""
                **High Churn Risk (>70%)**
                - Immediate outreach to premium customers
                - Personalized retention offers
                - Account review within 7 days
                """)
            
            with col2:
                st.warning("""
                **Dormant Accounts (>180d)**
                - Reactivation campaign
                - Special promotions
                - Product recommendations
                """)
            
            with col3:
                st.success("""
                **Lifecycle Optimization**
                - Move NEW → ACTIVE faster
                - Prevent ACTIVE → DECLINING
                - Reactivate DORMANT customers
                """)
        
        else:
            st.info("💡 **Lifecycle data not available**. Ensure `CRMA_AGG_DT_CUSTOMER_LIFECYCLE` table is deployed.")
            st.markdown("""
            ### Expected Features:
            - **Lifecycle Stage Distribution**: NEW, ACTIVE, MATURE, DECLINING, DORMANT, CHURNED
            - **Churn Probability**: Predictive scoring (0-100%)
            - **At-Risk Customers**: GOLD/PLATINUM customers with >70% churn probability
            - **Dormant Account Reactivation**: Customers inactive >180 days
            - **Revenue at Risk Calculator**: Estimated revenue impact
            
            **Data Source**: `CRMA_AGG_DT_CUSTOMER_LIFECYCLE`
            """)
    
    except Exception as e:
        st.error(f"Error loading lifecycle data: {str(e)}")
        st.info("💡 Ensure the `CRMA_AGG_DT_CUSTOMER_LIFECYCLE` table exists and is accessible.")

# ============================================================
# TAB 6: AML & Transaction Monitoring
# ============================================================
with tab6:
    st.header("AML & Transaction Monitoring")
    
    try:
        # Load AML metrics
        metrics = load_aml_metrics()
        
        # Key metrics at top
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Alerts (90d)", metrics.get('TOTAL_ALERTS', 0))
        with col2:
            st.metric("Unique Customers", metrics.get('UNIQUE_CUSTOMERS', 0))
        with col3:
            st.metric("Anomalous Transactions", metrics.get('ANOMALOUS_TRANSACTIONS', 0))
        with col4:
            if metrics.get('TOTAL_ALERTS', 0) > 0:
                anomaly_rate = (metrics.get('ANOMALOUS_TRANSACTIONS', 0) / metrics.get('TOTAL_ALERTS', 1)) * 100
                st.metric("Anomaly Rate", f"{anomaly_rate:.1f}%")
        
        st.markdown("---")
        
        # Load alert data
        df_alerts = load_aml_alerts()
        
        if len(df_alerts) > 0:
            # Show data info for debugging
            st.caption(f"ℹ️ Loaded {len(df_alerts)} alert records from PAYA_AGG_DT_TRANSACTION_ANOMALIES")
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Alert Trend")
                fig_trend = plot_aml_alert_trend(df_alerts)
                st.plotly_chart(fig_trend, width="stretch", key="aml_alert_trend")
            
            with col2:
                st.subheader("Alert Statistics")
                st.write(f"**Total Alerts:** {len(df_alerts)}")
                if 'CUSTOMER_ID' in df_alerts.columns:
                    unique_customers = df_alerts['CUSTOMER_ID'].nunique()
                    st.write(f"**Unique Customers:** {unique_customers}")
                if 'OVERALL_ANOMALY_CLASSIFICATION' in df_alerts.columns:
                    anomaly_class = df_alerts['OVERALL_ANOMALY_CLASSIFICATION'].value_counts()
                    st.write(f"**Risk Classification:**")
                    for aclass, count in anomaly_class.items():
                        icon = "🔴" if aclass == "CRITICAL" else "🟠" if aclass == "HIGH" else "🟡" if aclass == "MODERATE" else "🟢"
                        st.write(f"  {icon} {aclass}: {count}")
                if 'REQUIRES_IMMEDIATE_REVIEW' in df_alerts.columns:
                    immediate = df_alerts['REQUIRES_IMMEDIATE_REVIEW'].sum()
                    st.write(f"**Immediate Review Required:** {immediate}")
            
            st.markdown("---")
            
            # Alert details table
            st.subheader("Recent Alerts")
            display_cols = [col for col in ['CUSTOMER_ID', 'BOOKING_DATE', 'AMOUNT', 'CURRENCY',
                                             'OVERALL_ANOMALY_CLASSIFICATION', 'COMPOSITE_ANOMALY_SCORE',
                                             'REQUIRES_IMMEDIATE_REVIEW', 'DESCRIPTION'] if col in df_alerts.columns]
            if display_cols:
                st.dataframe(df_alerts[display_cols].head(100), width="stretch", height=400, hide_index=True)
            
            # Export button
            csv = df_alerts.to_csv(index=False)
            st.download_button(
                label="📥 Export AML Alerts (CSV)",
                data=csv,
                file_name=f"aml_alerts_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info("✅ No AML alerts found in the system")
    
    except Exception as e:
        st.error(f"Error loading AML data: {str(e)}")

# ============================================================
# TAB 7: Lending & Credit Operations
# ============================================================
with tab7:
    st.header("Lending & Credit Operations")
    
    try:
        df_lending = load_lending_portfolio()
        
        if len(df_lending) > 0:
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Customers", len(df_lending))
            with col2:
                if 'CREDIT_SCORE_BAND' in df_lending.columns:
                    lending_eligible = len(df_lending[df_lending['CREDIT_SCORE_BAND'].notna()])
                    st.metric("Lending Eligible", lending_eligible)
            with col3:
                if 'CREDIT_SCORE_BAND' in df_lending.columns:
                    high_score = len(df_lending[df_lending['CREDIT_SCORE_BAND'].isin(['EXCELLENT', 'VERY_GOOD'])])
                    st.metric("High Credit Score", high_score)
            with col4:
                if 'RISK_CLASSIFICATION' in df_lending.columns:
                    low_risk = len(df_lending[df_lending['RISK_CLASSIFICATION'] == 'LOW_RISK'])
                    st.metric("Low Risk Customers", low_risk)
            
            st.markdown("---")
            
            # Lending eligibility info
            if 'CREDIT_SCORE_BAND' in df_lending.columns:
                no_score = len(df_lending[df_lending['CREDIT_SCORE_BAND'].isna()])
                if no_score > 0:
                    st.info(f"ℹ️ **{no_score}** customer(s) have no credit score and require credit assessment before lending eligibility")
            
            st.markdown("---")
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Credit Risk Distribution")
                # Filter out NULL credit scores for the visualization
                df_with_scores = df_lending[df_lending['CREDIT_SCORE_BAND'].notna()] if 'CREDIT_SCORE_BAND' in df_lending.columns else df_lending
                fig_credit = plot_credit_risk_distribution(df_with_scores)
                st.plotly_chart(fig_credit, width="stretch", key="credit_risk_distribution")
            
            with col2:
                st.subheader("Risk Classification")
                if 'RISK_CLASSIFICATION' in df_lending.columns:
                    risk_counts = df_lending['RISK_CLASSIFICATION'].value_counts()
                    for risk, count in risk_counts.items():
                        st.write(f"**{risk}:** {count}")
            
            st.markdown("---")
            
            # Portfolio details
            st.subheader("Lending Portfolio")
            display_cols = [col for col in ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'CREDIT_SCORE_BAND', 
                                             'RISK_CLASSIFICATION', 'ACCOUNT_TIER'] if col in df_lending.columns]
            if display_cols:
                st.dataframe(df_lending[display_cols], width="stretch", height=400, hide_index=True)
        else:
            st.info("No lending portfolio data available")
    
    except Exception as e:
        st.error(f"Error loading lending data: {str(e)}")

# ============================================================
# TAB 8: Wealth Management
# ============================================================
with tab8:
    st.header("Wealth Management")
    
    # Load data with error visibility
    df_wealth = load_advisor_performance()
    
    # Debug info
    if df_wealth is not None:
        st.caption(f"🔍 Debug: Loaded {len(df_wealth)} records, Empty: {df_wealth.empty}, Columns: {list(df_wealth.columns) if len(df_wealth) > 0 else 'None'}")
    else:
        st.caption("🔍 Debug: df_wealth is None")
    
    # Check if data loaded
    if df_wealth is not None and len(df_wealth) > 0 and not df_wealth.empty:
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Advisors", len(df_wealth))
            with col2:
                if 'TOTAL_AUM' in df_wealth.columns:
                    total_aum = df_wealth['TOTAL_AUM'].sum()
                    st.metric("Total AUM", f"CHF {total_aum:,.0f}M")
            with col3:
                if 'CLIENT_COUNT' in df_wealth.columns:
                    total_clients = df_wealth['CLIENT_COUNT'].sum()
                    st.metric("Total Clients", f"{total_clients:,.0f}")
            with col4:
                if 'TOTAL_AUM' in df_wealth.columns and 'CLIENT_COUNT' in df_wealth.columns:
                    avg_aum = total_aum / max(total_clients, 1)
                    st.metric("Avg AUM per Client", f"CHF {avg_aum:,.0f}K")
            
            st.markdown("---")
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Top Advisors by AUM")
                fig_aum = plot_advisor_aum_distribution(df_wealth)
                st.plotly_chart(fig_aum, width="stretch", key="advisor_aum_distribution")
            
            with col2:
                st.subheader("Advisor Capacity Analysis")
                fig_capacity = plot_advisor_capacity(df_wealth)
                st.plotly_chart(fig_capacity, width="stretch", key="advisor_capacity_analysis")
            
            st.markdown("---")
            
            # Advisor performance table
            st.subheader("Advisor Performance")
            display_cols = [col for col in ['ADVISOR_ID', 'ADVISOR_NAME', 'CLIENT_COUNT', 'TOTAL_AUM', 
                                             'PERFORMANCE_RATING', 'REGION'] if col in df_wealth.columns]
            if display_cols:
                st.dataframe(df_wealth[display_cols], width="stretch", height=400, hide_index=True)
    else:
        st.info("💡 **Wealth Management data not available**")
        
        # Try to determine if tables exist but are empty
        try:
            session = get_snowflake_session()
            
            # Check if table exists by attempting to query it
            test_query = "SELECT COUNT(*) as cnt FROM EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED LIMIT 1"
            result = session.sql(test_query).collect()
            row_count = result[0]['CNT'] if result else 0
            
            if row_count == 0:
                st.warning("⚠️ **Tables exist but contain no data**")
                st.markdown("""
                The advisor performance tables are deployed in `CRM_AGG_001` schema but are currently empty.
                
                ### Next Steps:
                1. **Load advisor data** into `EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED`
                2. **Refresh the dashboard** to see wealth management analytics
                
                ### Expected Data:
                - Advisor IDs and names
                - Total Assets Under Management (AUM)
                - Client counts
                - Performance ratings
                - Regional/office assignments
                
                **Data Source:** Employee master data from `CRMI_RAW_TB_EMPLOYEE` and client assignments
                """)
            else:
                st.success(f"✅ Found {row_count} advisor records in table")
                st.warning("⚠️ Data exists but failed to load. Check debug info above.")
                st.write("**Possible causes:**")
                st.write("- Column name mismatch in query")
                st.write("- Data type conversion issue")
                st.write("- Query timeout")
                
                # Show actual columns in the table
                try:
                    cols_query = "SELECT * FROM EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED LIMIT 1"
                    sample = session.sql(cols_query).to_pandas()
                    st.write(f"**Table columns:** {list(sample.columns)}")
                except Exception as col_err:
                    st.caption(f"Could not retrieve column names: {str(col_err)}")
        
        except Exception as e:
            # Table doesn't exist or other error
            error_msg = str(e).lower()
            
            if "does not exist" in error_msg or "invalid identifier" in error_msg:
                st.warning("⚠️ **Tables not deployed**")
                st.markdown("""
                The Employee/Advisor tables are not yet deployed in `CRM_AGG_001` schema.
                
                ### Required Tables:
                - `EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED` - Advisor KPIs and performance metrics
                - `EMPA_AGG_DT_PORTFOLIO_BY_ADVISOR` - Portfolio valuations by advisor
                - `EMPA_AGG_DT_TEAM_LEADER_DASHBOARD` - Team-level aggregations
                
                ### Deployment:
                Deploy the Employee Management dynamic tables from your database structure scripts.
                
                **Contact your Data Platform team for assistance.**
                """)
            else:
                st.error(f"❌ **Error accessing wealth management data**")
                st.caption(f"Technical details: {str(e)}")
                st.write("Please check:")
                st.write("- Schema permissions for `CRM_AGG_001`")
                st.write("- Table access privileges")
                st.write("- Snowflake connection settings")

# ============================================================
# TAB 9: Sanctions Control
# ============================================================
with tab9:
    st.header("Sanctions & Embargo Control")
    
    try:
        df_sanctions = load_sanctions_matches()
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Sanctions Matches", len(df_sanctions))
        with col2:
            if len(df_sanctions) > 0 and 'SANCTIONS_MATCH_TYPE' in df_sanctions.columns:
                exact_matches = len(df_sanctions[df_sanctions['SANCTIONS_MATCH_TYPE'] == 'EXACT_MATCH'])
                st.metric("Exact Matches", exact_matches, delta_color="inverse")
        with col3:
            if len(df_sanctions) > 0 and 'REQUIRES_SANCTIONS_REVIEW' in df_sanctions.columns:
                review_needed = df_sanctions['REQUIRES_SANCTIONS_REVIEW'].sum()
                st.metric("Requires Review", review_needed)
        with col4:
            if len(df_sanctions) > 0 and 'OVERALL_SANCTIONS_RISK' in df_sanctions.columns:
                high_risk = len(df_sanctions[df_sanctions['OVERALL_SANCTIONS_RISK'].isin(['CRITICAL', 'HIGH'])])
                st.metric("High Risk", high_risk)
        
        st.markdown("---")
        
        if len(df_sanctions) > 0:
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Sanctions Screening Results")
                fig_sanctions = plot_sanctions_screening_results(df_sanctions)
                st.plotly_chart(fig_sanctions, width="stretch", key="sanctions_screening_results")
            
            with col2:
                st.subheader("Risk Level Distribution")
                if 'OVERALL_SANCTIONS_RISK' in df_sanctions.columns:
                    risk_counts = df_sanctions['OVERALL_SANCTIONS_RISK'].value_counts()
                    for risk, count in risk_counts.items():
                        icon = "🔴" if risk in ['CRITICAL', 'HIGH'] else "🟡" if risk == 'MEDIUM' else "🟢"
                        st.write(f"{icon} **{risk}:** {count}")
            
            st.markdown("---")
            
            # Sanctions matches table
            st.subheader("Sanctions Matches Requiring Action")
            display_cols = [col for col in ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'SANCTIONS_MATCH_TYPE',
                                             'SANCTIONS_MATCH_ACCURACY_PERCENT', 'OVERALL_SANCTIONS_RISK'] 
                            if col in df_sanctions.columns]
            if display_cols:
                st.dataframe(df_sanctions[display_cols], width="stretch", height=400, hide_index=True)
            
            # Export button
            csv = df_sanctions.to_csv(index=False)
            st.download_button(
                label="📥 Export Sanctions Report (CSV)",
                data=csv,
                file_name=f"sanctions_matches_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.success("✅ No sanctions matches found")
    
    except Exception as e:
        st.error(f"Error loading sanctions data: {str(e)}")

# ============================================================
# TAB 10: Advisor & Employee Management
# ============================================================
with tab10:
    st.header("Advisor & Employee Management")
    
    # Load data
    df_capacity = load_advisor_capacity()
    
    # Check if data loaded
    if df_capacity is not None and len(df_capacity) > 0 and not df_capacity.empty:
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Advisors", len(df_capacity))
            with col2:
                if 'WORKLOAD_STATUS' in df_capacity.columns:
                    available = len(df_capacity[df_capacity['WORKLOAD_STATUS'] == 'AVAILABLE'])
                    st.metric("Available Capacity", available)
            with col3:
                if 'WORKLOAD_STATUS' in df_capacity.columns:
                    at_capacity = len(df_capacity[df_capacity['WORKLOAD_STATUS'] == 'AT_CAPACITY'])
                    st.metric("At Capacity", at_capacity, delta_color="inverse")
            with col4:
                if 'AVAILABLE_CAPACITY' in df_capacity.columns:
                    total_capacity = df_capacity['AVAILABLE_CAPACITY'].sum()
                    st.metric("Total Available Slots", int(total_capacity))
            
            st.markdown("---")
            
            # Advisor capacity table
            st.subheader("Advisor Capacity Dashboard")
            display_cols = [col for col in ['EMPLOYEE_ID', 'ADVISOR_NAME', 'TOTAL_CLIENTS', 'AVAILABLE_CAPACITY',
                                             'WORKLOAD_STATUS', 'CAPACITY_UTILIZATION_PCT', 'TOTAL_PORTFOLIO_VALUE', 
                                             'REGION', 'COUNTRY', 'HIGH_RISK_CLIENTS', 'PERFORMANCE_RATING'] 
                            if col in df_capacity.columns]
            if display_cols:
                st.dataframe(df_capacity[display_cols], width="stretch", height=500, hide_index=True)
            
            # Team performance if available
            try:
                df_team = load_team_performance()
                if len(df_team) > 0:
                    st.markdown("---")
                    st.subheader("Team Performance")
                    st.dataframe(df_team, width="stretch", height=300, hide_index=True)
            except:
                pass
    else:
        st.info("💡 **Advisor Management data not available**")
        st.markdown("""
        Same tables as Wealth Management tab are required.
        
        ### Data Status:
        This dashboard uses the same source data as the **💎 Wealth Management** tab.
        
        ### Next Steps:
        1. Check the **Wealth Management** tab for data status
        2. Ensure `EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED` table has data
        3. Refresh this page after loading advisor data
        
        **Schema:** `CRM_AGG_001`
        """)

# ============================================================
# TAB 11: KYC & Customer Screening
# ============================================================
with tab11:
    st.header("KYC & Customer Screening")
    
    try:
        df_pep = load_pep_matches()
        df_kyc = load_kyc_completeness()
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("PEP Matches", len(df_pep))
        with col2:
            if len(df_pep) > 0 and 'REQUIRES_EXPOSED_PERSON_REVIEW' in df_pep.columns:
                review_needed = df_pep['REQUIRES_EXPOSED_PERSON_REVIEW'].sum()
                st.metric("Requires PEP Review", review_needed)
        with col3:
            if len(df_pep) > 0 and 'EXPOSED_PERSON_MATCH_TYPE' in df_pep.columns:
                exact_matches = len(df_pep[df_pep['EXPOSED_PERSON_MATCH_TYPE'] == 'EXACT_MATCH'])
                st.metric("Exact PEP Matches", exact_matches, delta_color="inverse")
        with col4:
            if len(df_kyc) > 0 and 'TOTAL_CUSTOMERS' in df_kyc.columns:
                total_customers = df_kyc['TOTAL_CUSTOMERS'].sum()
                st.metric("Total Customers", f"{total_customers:,.0f}")
        
        st.markdown("---")
        
        # PEP screening results
        if len(df_pep) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("PEP Screening Results")
                fig_pep = plot_pep_screening_results(df_pep)
                st.plotly_chart(fig_pep, width="stretch", key="pep_screening_results")
            
            with col2:
                st.subheader("Risk Level Distribution")
                if 'OVERALL_EXPOSED_PERSON_RISK' in df_pep.columns:
                    risk_counts = df_pep['OVERALL_EXPOSED_PERSON_RISK'].value_counts()
                    for risk, count in risk_counts.items():
                        icon = "🔴" if risk in ['CRITICAL', 'HIGH'] else "🟡" if risk == 'MEDIUM' else "🟢"
                        st.write(f"{icon} **{risk}:** {count}")
            
            st.markdown("---")
            
            # PEP matches table
            st.subheader("PEP Matches Requiring Review")
            display_cols = [col for col in ['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'EXPOSED_PERSON_MATCH_TYPE',
                                             'EXPOSED_PERSON_MATCH_ACCURACY_PERCENT', 'OVERALL_EXPOSED_PERSON_RISK'] 
                            if col in df_pep.columns]
            if display_cols:
                st.dataframe(df_pep[display_cols], width="stretch", height=400, hide_index=True)
        
        # KYC completeness
        if len(df_kyc) > 0:
            st.markdown("---")
            st.subheader("KYC Data Completeness by Country")
            st.dataframe(df_kyc, width="stretch", height=300, hide_index=True)
    
    except Exception as e:
        st.error(f"Error loading KYC screening data: {str(e)}")

# ============================================================
# TAB 12: Data Quality & Controls
# ============================================================
with tab12:
    st.header("Data Quality & Controls")
    
    try:
        metrics = load_data_quality_metrics()
        
        if metrics:
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Records", f"{metrics.get('TOTAL_RECORDS', 0):,.0f}")
            with col2:
                st.metric("Unique Customers", f"{metrics.get('UNIQUE_CUSTOMERS', 0):,.0f}")
            with col3:
                st.metric("Email Completeness", f"{metrics.get('EMAIL_COMPLETENESS', 0):.1f}%")
            with col4:
                st.metric("Phone Completeness", f"{metrics.get('PHONE_COMPLETENESS', 0):.1f}%")
            
            st.markdown("---")
            
            # Data quality gauges
            st.subheader("Data Quality Completeness Metrics")
            fig_quality = plot_data_quality_completeness(metrics)
            st.plotly_chart(fig_quality, width="stretch", key="data_quality_completeness")
            
            st.markdown("---")
            
            # Missing data summary
            st.subheader("Missing Data Analysis")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write("**Missing Email:**", f"{metrics.get('MISSING_EMAIL', 0):,.0f}")
            with col2:
                st.write("**Missing Phone:**", f"{metrics.get('MISSING_PHONE', 0):,.0f}")
            with col3:
                st.write("**Missing DOB:**", f"{metrics.get('MISSING_DOB', 0):,.0f}")
            
            # Quality thresholds
            st.markdown("---")
            st.subheader("Data Quality Standards")
            st.info("""
            **Quality Thresholds:**
            - ✅ **Excellent:** >95% completeness
            - ⚠️ **Acceptable:** 80-95% completeness
            - ❌ **Poor:** <80% completeness (requires remediation)
            """)
        else:
            st.warning("No data quality metrics available")
    
    except Exception as e:
        st.error(f"Error loading data quality metrics: {str(e)}")

# ============================================================
# TAB 13: Ask AI (Natural Language Query)
# ============================================================
# ============================================================
# TAB 13: LCR Monitoring
# ============================================================
with tab13:
    st.header("📊 Liquidity Coverage Ratio (LCR) Monitoring")
    st.markdown("**FINMA LCR Reporting** • Real-time liquidity risk monitoring • Regulatory compliance dashboard")
    
    try:
        # Load current status
        df_current = load_lcr_current_status()
        
        if len(df_current) > 0:
            current_row = df_current.iloc[0]
            
            # Key metrics at top
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                # Color-coded LCR ratio
                lcr_value = current_row['LCR_RATIO']
                if lcr_value >= 105:
                    delta_color = "normal"
                elif lcr_value >= 100:
                    delta_color = "off"
                else:
                    delta_color = "inverse"
                
                st.metric(
                    "Current LCR Ratio",
                    f"{lcr_value:.2f}%",
                    delta=f"{lcr_value - 100:.2f}pp vs. minimum",
                    delta_color=delta_color
                )
            
            with col2:
                st.metric(
                    "Compliance Status",
                    current_row['LCR_STATUS'],
                    help="PASS = ≥100%, WARNING = 95-100%, FAIL = <95%"
                )
            
            with col3:
                # Convert to millions for display
                hqla_millions = current_row['HQLA_TOTAL'] / 1_000_000
                st.metric(
                    "HQLA Total",
                    f"CHF {hqla_millions:,.0f}M",
                    help="High-Quality Liquid Assets (Numerator)"
                )
            
            with col4:
                # Convert to millions for display
                outflow_millions = current_row['OUTFLOW_TOTAL'] / 1_000_000
                st.metric(
                    "Net Outflows",
                    f"CHF {outflow_millions:,.0f}M",
                    help="30-day stressed outflows (Denominator)"
                )
            
            st.markdown("---")
            
            # Alert section - check if LCR is below threshold
            if current_row.get('LCR_RATIO', 100) < 100:
                st.error(f"🚨 **LCR BREACH**: LCR ratio is below 100% regulatory minimum!")
            elif current_row.get('LCR_RATIO', 100) < 105:
                st.warning(f"⚠️ **WARNING**: LCR ratio is below 105% early warning threshold")
            
            # Load alerts
            df_alerts = load_lcr_alerts()
            if len(df_alerts) > 0:
                with st.expander(f"🚨 Active Alerts ({len(df_alerts)})", expanded=True):
                    for idx, alert in df_alerts.iterrows():
                        if alert['ALERT_SEVERITY'] == 'CRITICAL':
                            severity_icon = "🔴"
                            st.error(f"{severity_icon} **{alert['ALERT_TYPE']}**: {alert['ALERT_MESSAGE']}")
                            st.caption(f"→ Action: {alert.get('RECOMMENDED_ACTION', 'N/A')}")
                        elif alert['ALERT_SEVERITY'] == 'HIGH':
                            severity_icon = "🟠"
                            st.warning(f"{severity_icon} **{alert['ALERT_TYPE']}**: {alert['ALERT_MESSAGE']}")
                            st.caption(f"→ Action: {alert.get('RECOMMENDED_ACTION', 'N/A')}")
                        elif alert['ALERT_SEVERITY'] == 'MEDIUM':
                            severity_icon = "🟡"
                            st.warning(f"{severity_icon} **{alert['ALERT_TYPE']}**: {alert['ALERT_MESSAGE']}")
                            st.caption(f"→ Action: {alert.get('RECOMMENDED_ACTION', 'N/A')}")
                        else:
                            severity_icon = "ℹ️"
                            st.info(f"{severity_icon} **{alert['ALERT_TYPE']}**: {alert['ALERT_MESSAGE']}")
                            st.caption(f"→ Action: {alert.get('RECOMMENDED_ACTION', 'N/A')}")
            
            st.markdown("---")
            
            # Create tabs for detailed views
            lcr_tab1, lcr_tab2, lcr_tab3, lcr_tab4, lcr_tab5 = st.tabs([
                "📈 Trend Analysis",
                "💰 HQLA Breakdown",
                "💸 Outflow Analysis",
                "📊 Components",
                "📅 Monthly Summary"
            ])
            
            # Tab 1: Trend Analysis
            with lcr_tab1:
                st.subheader("LCR Trend Analysis (90 Days)")
                
                df_trend = load_lcr_trend(days=90)
                if len(df_trend) > 0:
                    # Display trend chart
                    fig_trend = plot_lcr_trend(df_trend)
                    st.plotly_chart(fig_trend, width='stretch')
                    
                    # Summary statistics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Average LCR", f"{df_trend['LCR_RATIO'].mean():.2f}%")
                    with col2:
                        st.metric("Minimum LCR", f"{df_trend['LCR_RATIO'].min():.2f}%")
                    with col3:
                        st.metric("Maximum LCR", f"{df_trend['LCR_RATIO'].max():.2f}%")
                    with col4:
                        volatility = df_trend['LCR_RATIO'].std()
                        st.metric("Volatility (StdDev)", f"{volatility:.2f}%")
                    
                    # Show data table
                    with st.expander("📋 View Raw Data"):
                        st.dataframe(
                            df_trend[['AS_OF_DATE', 'LCR_RATIO', 'LCR_7D_AVG', 'LCR_30D_AVG', 'LCR_90D_AVG', 'LCR_30D_VOLATILITY', 'LCR_STATUS']].tail(30),
                            hide_index=True
                        )
                else:
                    st.info("No trend data available")
            
            # Tab 2: HQLA Breakdown
            with lcr_tab2:
                st.subheader("High-Quality Liquid Assets (HQLA) Breakdown")
                
                df_hqla = load_hqla_holdings_detail()
                if len(df_hqla) > 0:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # HQLA by regulatory level
                        fig_level = plot_hqla_composition(df_hqla)
                        st.plotly_chart(fig_level, width='stretch')
                        
                        # Level breakdown
                        level_totals = df_hqla.groupby('REGULATORY_LEVEL')['WEIGHTED_VALUE_CHF'].sum()
                        st.markdown("**Level Breakdown:**")
                        for level in ['L1', 'L2A', 'L2B']:
                            if level in level_totals:
                                pct = (level_totals[level] / level_totals.sum()) * 100
                                st.write(f"- **{level}**: CHF {level_totals[level]:,.0f}M ({pct:.1f}%)")
                    
                    with col2:
                        # HQLA by asset type
                        fig_asset = plot_hqla_by_asset_type(df_hqla)
                        st.plotly_chart(fig_asset, width='stretch')
                    
                    # 40% Cap Rule Status
                    if current_row.get('CAP_APPLIED', False):
                        st.warning("⚠️ **40% Cap Rule Applied**: Level 2 assets exceed 2/3 of Level 1 assets")
                    else:
                        st.success("✅ **40% Cap Rule Not Applied**: Level 2 within allowed limits")
                    
                    # Detailed table
                    st.markdown("---")
                    st.markdown("**Detailed Holdings:**")
                    st.dataframe(
                        df_hqla[[
                            'ASSET_TYPE', 'REGULATORY_LEVEL', 'HAIRCUT_FACTOR',
                            'MARKET_VALUE_CHF', 'WEIGHTED_VALUE_CHF', 'HOLDING_COUNT'
                        ]],
                        hide_index=True
                    )
                else:
                    st.info("No HQLA holdings data available")
            
            # Tab 3: Outflow Analysis
            with lcr_tab3:
                st.subheader("Deposit Outflows Analysis")
                
                df_outflows = load_deposit_outflows_detail()
                if len(df_outflows) > 0:
                    # Outflows by type chart
                    fig_outflows = plot_deposit_outflows_by_type(df_outflows)
                    st.plotly_chart(fig_outflows, width='stretch')
                    
                    # Summary by counterparty type
                    st.markdown("**Outflows by Counterparty Type:**")
                    counterparty_totals = df_outflows.groupby('COUNTERPARTY_TYPE').agg({
                        'TOTAL_BALANCE_CHF': 'sum',
                        'TOTAL_OUTFLOW_CHF': 'sum',
                        'ACCOUNT_COUNT': 'sum'
                    }).reset_index()
                    
                    for idx, row in counterparty_totals.iterrows():
                        run_off_pct = (row['TOTAL_OUTFLOW_CHF'] / row['TOTAL_BALANCE_CHF'] * 100) if row['TOTAL_BALANCE_CHF'] > 0 else 0
                        st.write(f"- **{row['COUNTERPARTY_TYPE']}**: CHF {row['TOTAL_OUTFLOW_CHF']:,.0f}M ({run_off_pct:.1f}% run-off rate)")
                    
                    # Detailed table
                    st.markdown("---")
                    st.markdown("**Detailed Outflows:**")
                    st.dataframe(
                        df_outflows[[
                            'DEPOSIT_TYPE', 'COUNTERPARTY_TYPE', 'BASE_RUN_OFF_RATE',
                            'TOTAL_BALANCE_CHF', 'TOTAL_OUTFLOW_CHF', 'ACCOUNT_COUNT'
                        ]],
                        hide_index=True
                    )
                else:
                    st.info("No deposit outflows data available")
            
            # Tab 4: Components Waterfall
            with lcr_tab4:
                st.subheader("LCR Components")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Gauge chart
                    fig_gauge = plot_lcr_gauge(current_row['LCR_RATIO'])
                    st.plotly_chart(fig_gauge, width='stretch')
                
                with col2:
                    # Waterfall chart
                    fig_waterfall = plot_hqla_vs_outflows(df_current)
                    st.plotly_chart(fig_waterfall, width='stretch')
                
                # Component breakdown
                st.markdown("---")
                st.markdown("**LCR Calculation:**")
                st.latex(r"LCR = \frac{HQLA}{Net\ Cash\ Outflows} \times 100\%")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    l1_millions = current_row['L1_TOTAL'] / 1_000_000
                    st.metric("Level 1 Assets", f"CHF {l1_millions:,.0f}M")
                with col2:
                    l2_millions = current_row['L2_CAPPED'] / 1_000_000
                    st.metric("Level 2 Assets (Capped)", f"CHF {l2_millions:,.0f}M")
                with col3:
                    buffer_millions = current_row.get('LCR_BUFFER_CHF', 0) / 1_000_000
                    st.metric("LCR Buffer", f"CHF {buffer_millions:,.0f}M")
            
            # Tab 5: Monthly Summary
            with lcr_tab5:
                st.subheader("Monthly LCR Summary (SNB Reporting)")
                
                df_monthly = load_lcr_monthly_summary()
                if len(df_monthly) > 0:
                    # Monthly trend chart
                    fig_monthly = plot_monthly_compliance_trend(df_monthly)
                    st.plotly_chart(fig_monthly, width='stretch')
                    
                    # Summary table
                    st.markdown("**Monthly Compliance Summary:**")
                    st.dataframe(
                        df_monthly[[
                            'REPORT_MONTH', 'AVG_LCR_RATIO', 'MIN_LCR_RATIO', 'MAX_LCR_RATIO',
                            'DAYS_BELOW_100_PCT', 'DAYS_BELOW_105_PCT', 'COMPLIANCE_STATUS'
                        ]],
                        hide_index=True
                    )
                    
                    # Export button
                    st.markdown("---")
                    if st.button("📄 Export to SNB XML Format"):
                        st.info("SNB XML export functionality will be added in next release")
                else:
                    st.info("No monthly summary data available")
        
        else:
            st.warning("⚠️ No LCR data available. Please ensure the LCR calculation engine is running.")
            st.info("""
            **To enable LCR monitoring:**
            
            1. Deploy LCR database schemas:
               - `structure/360_LIQA_CalculateHQLAandNetCashOutflows.sql`
               - `structure/361_LIQA_BusinessReporting_FINMA_LCR.sql`
               - `structure/750_LCRS_SV_LCR_SEMANTIC_MODELS.sql`
               - `structure/850_LIQUIDITY_RISK_AGENT.sql`
            
            2. Load sample data or connect to production data sources
            
            3. Dynamic tables will auto-refresh every 60 minutes
            """)
    
    except Exception as e:
        st.error(f"❌ Error loading LCR data: {str(e)}")
        st.info("Please ensure LCR tables are deployed and data is available.")

# ============================================================
# TAB 14: Ask AI
# ============================================================
with tab14:
    st.header("Loans Portfolio")
    st.caption("Retail Loans & Mortgages Portfolio Analysis")
    
    # Load loan data
    with st.spinner("Loading loan portfolio data..."):
        df_portfolio = load_loan_portfolio_summary()
        df_ltv = load_loan_ltv_distribution()
        df_funnel = load_loan_application_funnel()
        df_affordability = load_loan_affordability_analysis()
        df_compliance = load_loan_compliance_screening()
        df_customers = load_loan_customer_summary()
    
    # Key metrics
    if not df_portfolio.empty:
        total_apps = df_portfolio['LOAN_COUNT'].sum()
        total_amount = df_portfolio['TOTAL_REQUESTED_AMOUNT'].sum() / 1_000_000  # Convert to millions
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
        
        st.markdown("---")
        
        # Create sub-tabs
        loan_tab1, loan_tab2, loan_tab3, loan_tab4, loan_tab5 = st.tabs([
            "📊 Portfolio Overview",
            "📈 LTV Analysis",
            "🔄 Application Funnel",
            "💰 Affordability",
            "🛡️ Compliance Screening"
        ])
        
        # Tab 1: Portfolio Overview
        with loan_tab1:
            st.subheader("Portfolio by Country & Product")
            
            if not df_portfolio.empty:
                # Portfolio distribution chart
                col1, col2 = st.columns(2)
                
                with col1:
                    # Applications by country - aggregate by country and product
                    apps_by_country = df_portfolio.groupby(['COUNTRY', 'PRODUCT_TYPE'])['LOAN_COUNT'].sum().reset_index()
                    fig_apps = px.bar(
                        apps_by_country,
                        x='COUNTRY',
                        y='LOAN_COUNT',
                        color='PRODUCT_TYPE',
                        title='Applications by Country',
                        labels={'LOAN_COUNT': 'Applications', 'COUNTRY': 'Country'},
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig_apps, width='stretch')
                
                with col2:
                    # Requested amount by country - aggregate by country and product
                    amount_by_country = df_portfolio.groupby(['COUNTRY', 'PRODUCT_TYPE'])['TOTAL_REQUESTED_AMOUNT'].sum().reset_index()
                    amount_by_country['TOTAL_REQUESTED_AMOUNT_M'] = amount_by_country['TOTAL_REQUESTED_AMOUNT'] / 1_000_000
                    fig_amount = px.bar(
                        amount_by_country,
                        x='COUNTRY',
                        y='TOTAL_REQUESTED_AMOUNT_M',
                        color='PRODUCT_TYPE',
                        title='Total Requested Amount by Country (M CHF)',
                        labels={'TOTAL_REQUESTED_AMOUNT_M': 'Amount (M CHF)', 'COUNTRY': 'Country'},
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig_amount, width='stretch')
                
                st.markdown("---")
                
                # Portfolio status distribution
                col1, col2 = st.columns(2)
                
                with col1:
                    # Status distribution by country
                    status_by_country = df_portfolio.groupby(['COUNTRY', 'APPLICATION_STATUS'])['LOAN_COUNT'].sum().reset_index()
                    fig_status = px.bar(
                        status_by_country,
                        x='COUNTRY',
                        y='LOAN_COUNT',
                        color='APPLICATION_STATUS',
                        title='Application Status by Country',
                        labels={'LOAN_COUNT': 'Count', 'COUNTRY': 'Country'},
                        color_discrete_map={
                            'APPROVED': '#28A745',
                            'DECLINED': '#DC3545',
                            'UNDER_REVIEW': '#FFC107'
                        },
                        barmode='stack'
                    )
                    st.plotly_chart(fig_status, width='stretch')
                
                with col2:
                    # Average loan amounts by country
                    avg_amount_by_country = df_portfolio.groupby('COUNTRY')['AVG_REQUESTED_AMOUNT'].mean().reset_index()
                    fig_avg = px.bar(
                        avg_amount_by_country,
                        x='COUNTRY',
                        y='AVG_REQUESTED_AMOUNT',
                        title='Average Requested Amount by Country',
                        labels={'AVG_REQUESTED_AMOUNT': 'Average Amount (CHF)', 'COUNTRY': 'Country'},
                        color='AVG_REQUESTED_AMOUNT',
                        color_continuous_scale='Blues'
                    )
                    st.plotly_chart(fig_avg, width='stretch')
                
                st.markdown("---")
                
                # Portfolio table
                st.subheader("Detailed Portfolio Breakdown")
                st.dataframe(
                    df_portfolio.style.format({
                        'TOTAL_REQUESTED_AMOUNT': '{:,.0f}',
                        'AVG_REQUESTED_AMOUNT': '{:,.0f}'
                    }),
                    width='stretch',
                    height=400
                )
            else:
                st.warning("No portfolio data available")
        
        # Tab 2: LTV Analysis
        with loan_tab2:
            st.subheader("Loan-to-Value (LTV) Distribution")
            
            if not df_ltv.empty:
                # LTV distribution chart
                fig_ltv = px.bar(
                    df_ltv,
                    x='LTV_BUCKET',
                    y='LOAN_COUNT',
                    title='Loan Count by LTV Bucket',
                    labels={'LOAN_COUNT': 'Loans', 'LTV_BUCKET': 'LTV Bucket'},
                    color='AVG_LTV_PCT',
                    color_continuous_scale='RdYlGn_r'
                )
                st.plotly_chart(fig_ltv, width='stretch')
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # LTV amount distribution
                    df_ltv_millions = df_ltv.copy()
                    df_ltv_millions['TOTAL_LOAN_AMOUNT_M'] = df_ltv_millions['TOTAL_LOAN_AMOUNT'] / 1_000_000
                    fig_ltv_amt = px.bar(
                        df_ltv_millions,
                        x='LTV_BUCKET',
                        y='TOTAL_LOAN_AMOUNT_M',
                        title='Total Loan Amount by LTV Bucket (M CHF)',
                        labels={'TOTAL_LOAN_AMOUNT_M': 'Amount (M CHF)', 'LTV_BUCKET': 'LTV Bucket'},
                        color='LTV_BUCKET',
                        color_discrete_sequence=px.colors.sequential.Reds
                    )
                    st.plotly_chart(fig_ltv_amt, width='stretch')
                
                with col2:
                    # High-risk concentration (>80% LTV)
                    high_risk_ltv = df_ltv[df_ltv['LTV_BUCKET'].isin(['80-90%', '>90%'])]
                    if not high_risk_ltv.empty:
                        fig_high_risk = px.pie(
                            high_risk_ltv,
                            values='LOAN_COUNT',
                            names='LTV_BUCKET',
                            title='High-Risk LTV Distribution (>80%)',
                            color_discrete_sequence=px.colors.sequential.Reds
                        )
                        st.plotly_chart(fig_high_risk, width='stretch')
                    else:
                        st.success("✅ No high-risk LTV applications found (>80%)")
                
                st.markdown("---")
                
                # LTV table
                st.subheader("LTV Distribution Details")
                df_ltv_display = df_ltv.copy()
                df_ltv_display['TOTAL_COLLATERAL_VALUE_M'] = df_ltv_display['TOTAL_COLLATERAL_VALUE'] / 1_000_000
                df_ltv_display['TOTAL_LOAN_AMOUNT_M'] = df_ltv_display['TOTAL_LOAN_AMOUNT'] / 1_000_000
                
                st.dataframe(
                    df_ltv_display[['LTV_BUCKET', 'LOAN_COUNT', 'TOTAL_LOAN_AMOUNT_M', 'AVG_LTV_PCT', 'TOTAL_COLLATERAL_VALUE_M', 'PCT_OF_TOTAL_LOANS']].style.format({
                        'TOTAL_LOAN_AMOUNT_M': '{:,.2f}',
                        'AVG_LTV_PCT': '{:.2f}%',
                        'TOTAL_COLLATERAL_VALUE_M': '{:,.2f}',
                        'PCT_OF_TOTAL_LOANS': '{:.2f}%'
                    }),
                    width='stretch',
                    height=400
                )
            else:
                st.warning("No LTV distribution data available")
        
        # Tab 3: Application Funnel
        with loan_tab3:
            st.subheader("Application Status Funnel")
            
            if not df_funnel.empty:
                # Create funnel data
                total_apps = df_funnel['TOTAL_APPLICATIONS'].sum()
                total_approved = df_funnel['APPROVED_COUNT'].sum()
                total_declined = df_funnel['DECLINED_COUNT'].sum()
                total_review = df_funnel['UNDER_REVIEW_COUNT'].sum()
                
                funnel_data = pd.DataFrame({
                    'Stage': ['Submitted', 'Under Review', 'Approved', 'Declined'],
                    'Count': [total_apps, total_review, total_approved, total_declined]
                })
                
                # Funnel chart
                fig_funnel = px.funnel(
                    funnel_data[funnel_data['Stage'].isin(['Submitted', 'Under Review', 'Approved'])],
                    x='Count',
                    y='Stage',
                    title='Application Funnel (All Countries)'
                )
                st.plotly_chart(fig_funnel, width='stretch')
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Status by country
                    status_data = df_funnel.groupby('COUNTRY').agg({
                        'APPROVED_COUNT': 'sum',
                        'DECLINED_COUNT': 'sum',
                        'UNDER_REVIEW_COUNT': 'sum'
                    }).reset_index()
                    
                    fig_status = px.bar(
                        status_data,
                        x='COUNTRY',
                        y=['APPROVED_COUNT', 'UNDER_REVIEW_COUNT', 'DECLINED_COUNT'],
                        title='Applications by Status & Country',
                        labels={'value': 'Count', 'variable': 'Status'},
                        barmode='stack',
                        color_discrete_map={
                            'APPROVED_COUNT': '#28A745',
                            'UNDER_REVIEW_COUNT': '#FFC107',
                            'DECLINED_COUNT': '#DC3545'
                        }
                    )
                    st.plotly_chart(fig_status, width='stretch')
                
                with col2:
                    # Approval rates by country
                    rates_data = df_funnel.groupby('COUNTRY')['APPROVAL_RATE_PCT'].mean().reset_index()
                    fig_rates = px.bar(
                        rates_data,
                        x='COUNTRY',
                        y='APPROVAL_RATE_PCT',
                        title='Average Approval Rate by Country (%)',
                        labels={'APPROVAL_RATE_PCT': 'Approval Rate (%)'},
                        color='APPROVAL_RATE_PCT',
                        color_continuous_scale='Greens'
                    )
                    st.plotly_chart(fig_rates, width='stretch')
                
                st.markdown("---")
                
                # Funnel table
                st.subheader("Application Funnel Details")
                st.dataframe(
                    df_funnel.style.format({
                        'AVG_REQUESTED_AMOUNT': '{:,.0f}',
                        'APPROVAL_RATE_PCT': '{:.2f}%',
                        'DECLINE_RATE_PCT': '{:.2f}%'
                    }),
                    width='stretch',
                    height=400
                )
            else:
                st.warning("No application funnel data available")
        
        # Tab 4: Affordability
        with loan_tab4:
            st.subheader("Affordability Assessment")
            
            if not df_affordability.empty:
                # Aggregate by country for simpler charts
                aff_by_country = df_affordability.groupby('COUNTRY').agg({
                    'ASSESSMENT_COUNT': 'sum',
                    'AVG_DTI_RATIO_PCT': 'mean',
                    'AVG_DSTI_RATIO_PCT': 'mean',
                    'AVG_GROSS_INCOME': 'mean',
                    'AVG_DEBT_OBLIGATIONS': 'mean',
                    'PASS_RATE_PCT': 'mean'
                }).reset_index()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # DTI distribution
                    fig_dti = px.bar(
                        aff_by_country,
                        x='COUNTRY',
                        y='AVG_DTI_RATIO_PCT',
                        title='Average DTI Ratio by Country (%)',
                        labels={'AVG_DTI_RATIO_PCT': 'DTI Ratio (%)'},
                        color='AVG_DTI_RATIO_PCT',
                        color_continuous_scale='RdYlGn_r'
                    )
                    fig_dti.add_hline(y=45, line_dash="dash", line_color="red", annotation_text="45% Threshold")
                    st.plotly_chart(fig_dti, width='stretch')
                
                with col2:
                    # DSTI distribution
                    fig_dsti = px.bar(
                        aff_by_country,
                        x='COUNTRY',
                        y='AVG_DSTI_RATIO_PCT',
                        title='Average DSTI Ratio by Country (%)',
                        labels={'AVG_DSTI_RATIO_PCT': 'DSTI Ratio (%)'},
                        color='AVG_DSTI_RATIO_PCT',
                        color_continuous_scale='RdYlGn_r'
                    )
                    fig_dsti.add_hline(y=33.33, line_dash="dash", line_color="orange", annotation_text="Swiss 33⅓% Threshold")
                    st.plotly_chart(fig_dsti, width='stretch')
                
                # Income vs debt
                col1, col2 = st.columns(2)
                
                with col1:
                    # Average income
                    fig_income = px.bar(
                        aff_by_country,
                        x='COUNTRY',
                        y='AVG_GROSS_INCOME',
                        title='Average Gross Income by Country',
                        labels={'AVG_GROSS_INCOME': 'Gross Income (CHF)'},
                        color='COUNTRY'
                    )
                    st.plotly_chart(fig_income, width='stretch')
                
                with col2:
                    # Pass rates by country
                    fig_pass = px.bar(
                        aff_by_country,
                        x='COUNTRY',
                        y='PASS_RATE_PCT',
                        title='Affordability Pass Rate by Country (%)',
                        labels={'PASS_RATE_PCT': 'Pass Rate (%)'},
                        color='PASS_RATE_PCT',
                        color_continuous_scale='Greens'
                    )
                    st.plotly_chart(fig_pass, width='stretch')
                
                st.markdown("---")
                
                # Affordability table
                st.subheader("Affordability Details by Country & Result")
                st.dataframe(
                    df_affordability.style.format({
                        'AVG_GROSS_INCOME': '{:,.0f}',
                        'AVG_DEBT_OBLIGATIONS': '{:,.0f}',
                        'AVG_DTI_RATIO_PCT': '{:.2f}%',
                        'AVG_DSTI_RATIO_PCT': '{:.2f}%',
                        'PASS_RATE_PCT': '{:.2f}%'
                    }),
                    width='stretch',
                    height=300
                )
            else:
                st.warning("No affordability data available")
        
        # Tab 5: Compliance Screening
        with loan_tab5:
            st.subheader("Compliance & Risk Screening")
            
            if not df_compliance.empty:
                # Compliance metrics
                total_flagged = len(df_compliance)
                sanctions_count = df_compliance['REQUIRES_SANCTIONS_REVIEW'].sum()
                pep_count = df_compliance['REQUIRES_EXPOSED_PERSON_REVIEW'].sum()
                vulnerable_count = df_compliance['VULNERABLE_CUSTOMER_FLAG'].sum()
                high_risk_count = len(df_compliance[df_compliance['OVERALL_RISK_RATING'].isin(['CRITICAL', 'HIGH'])])
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Flagged", f"{total_flagged:,}")
                with col2:
                    st.metric("Sanctions Review", f"{sanctions_count:,}", delta="Critical", delta_color="inverse")
                with col3:
                    st.metric("PEP Review", f"{pep_count:,}", delta="High", delta_color="inverse")
                with col4:
                    st.metric("Vulnerable Customers", f"{vulnerable_count:,}")
                
                st.markdown("---")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Compliance status distribution
                    compliance_counts = df_compliance['COMPLIANCE_STATUS'].value_counts().reset_index()
                    compliance_counts.columns = ['Status', 'Count']
                    fig_compliance = px.pie(
                        compliance_counts,
                        values='Count',
                        names='Status',
                        title='Applications by Compliance Status',
                        color_discrete_sequence=px.colors.sequential.RdBu
                    )
                    st.plotly_chart(fig_compliance, width='stretch')
                
                with col2:
                    # Risk rating distribution
                    risk_counts = df_compliance['OVERALL_RISK_RATING'].value_counts().reset_index()
                    risk_counts.columns = ['Risk Rating', 'Count']
                    fig_risk = px.bar(
                        risk_counts,
                        x='Risk Rating',
                        y='Count',
                        title='Applications by Risk Rating',
                        color='Risk Rating',
                        color_discrete_map={
                            'CRITICAL': '#DC3545',
                            'HIGH': '#FF8C00',
                            'MEDIUM': '#FFC107',
                            'LOW': '#28A745'
                        }
                    )
                    st.plotly_chart(fig_risk, width='stretch')
                
                st.markdown("---")
                
                # Applications requiring review
                st.subheader("Applications Requiring Compliance Review")
                
                # Filter options
                col1, col2, col3 = st.columns(3)
                with col1:
                    filter_compliance = st.multiselect(
                        "Compliance Status",
                        options=df_compliance['COMPLIANCE_STATUS'].unique(),
                        default=df_compliance['COMPLIANCE_STATUS'].unique()
                    )
                with col2:
                    filter_risk = st.multiselect(
                        "Risk Rating",
                        options=df_compliance['OVERALL_RISK_RATING'].unique(),
                        default=df_compliance['OVERALL_RISK_RATING'].unique()
                    )
                with col3:
                    filter_country = st.multiselect(
                        "Country",
                        options=df_compliance['COUNTRY'].unique(),
                        default=df_compliance['COUNTRY'].unique()
                    )
                
                # Apply filters
                df_filtered = df_compliance[
                    (df_compliance['COMPLIANCE_STATUS'].isin(filter_compliance)) &
                    (df_compliance['OVERALL_RISK_RATING'].isin(filter_risk)) &
                    (df_compliance['COUNTRY'].isin(filter_country))
                ]
                
                st.dataframe(
                    df_filtered.style.format({
                        'REQUESTED_AMOUNT': '{:,.0f}'
                    }),
                    width='stretch',
                    height=400
                )
                
                # Export option
                if st.button("📥 Export Compliance Report"):
                    csv = df_filtered.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"loan_compliance_report_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("✅ No applications currently flagged for compliance review")
        
    else:
        st.warning("⚠️ No loan portfolio data available. Please check data pipeline.")

# ============================================================
# TAB 15: Ask AI
# ============================================================
with tab15:
    st.header("Ask AI Anything About Your Customers")
    
    st.info("🤖 **Powered by Snowflake Cortex AI Agent** - Natural language query interface")
    
    # Example queries
    with st.expander("💡 Try these example queries", expanded=True):
        st.markdown("""
        **Customer Intelligence:**
        - "Show me all PLATINUM customers in Switzerland"
        - "How many customers do we have by account tier?"
        - "Find customers who joined in the last 90 days"
        
        **Risk & Compliance:**
        - "Which customers require PEP review?"
        - "Show me all high-risk customers with CRITICAL ratings"
        - "Find customers with exact sanctions matches"
        
        **Portfolio Analysis:**
        - "Which customers have both investment and business accounts?"
        - "Show me multi-currency customers with 3+ currencies"
        - "What's the average number of accounts per customer tier?"
        
        **Fraud Detection:**
        - "Find customers with anomalous transaction patterns"
        - "Show me high-risk customers with fraud indicators"
        """)
    
    # Query input
    user_question = st.text_area(
        "Your question:",
        placeholder="Ask anything about your customers...",
        height=100
    )
    
    col1, col2 = st.columns([1, 5])
    with col1:
        ask_button = st.button("🚀 Ask", type="primary")
    
    if ask_button and user_question:
        with st.spinner("🤖 AI is thinking..."):
            try:
                # Call Snowflake Cortex AI Agent via REST API
                session = get_snowflake_session()
                
                # First, check if agent exists
                st.caption("🔍 Checking if AI agent exists...")
                try:
                    agent_check = session.sql("SHOW AGENTS LIKE 'CRM_Customer_360' IN SCHEMA CRM_AGG_001").collect()
                    if not agent_check:
                        st.warning("⚠️ AI Agent 'CRM_Customer_360' not found in CRM_AGG_001 schema")
                        st.info("💡 Agent needs to be deployed. Deploy with: `structure/810_CRM_INTELLIGENCE_AGENT.sql`")
                        raise Exception("Agent not deployed")
                    else:
                        st.caption(f"✅ Agent found: {agent_check[0][1]}")
                except Exception as check_error:
                    st.warning(f"⚠️ Could not verify agent exists: {str(check_error)}")
                    # Continue anyway
                
                # Agent name: AAA_DEV_SYNTHETIC_BANK.CRM_AGG_001.CRM_Customer_360 (note: mixed case!)
                agent_full_name = "AAA_DEV_SYNTHETIC_BANK.CRM_AGG_001.CRM_Customer_360"
                
                st.caption(f"🚀 Calling agent via REST API: {agent_full_name}")
                result = call_agent_rest_api(session, agent_full_name, user_question, timeout=60)
                
                if result['success']:
                    # Extract key metrics from the response
                    response_text = result['response']
                    
                    # Try to find numbers like "302 customers", "15 accounts", etc.
                    numbers_pattern = r'(\d+)\s+(customers?|accounts?|transactions?|records?)'
                    matches = re.findall(numbers_pattern, response_text, re.IGNORECASE)
                    
                    if matches:
                        st.success("✅ **Query Results**")
                        # Show key metrics at the top
                        cols = st.columns(min(len(matches), 4))
                        for idx, (number, entity) in enumerate(matches[:4]):  # Max 4 metrics
                            with cols[idx]:
                                st.metric(label=entity.capitalize(), value=number)
                        st.markdown("---")
                    else:
                        st.success("✅ **AI Agent Response**")
                    
                    # Display the final answer
                    st.markdown(response_text)
                    
                    # Optional: Show thinking process in expandable section
                    if result.get('thinking'):
                        with st.expander("🧠 View AI Reasoning (Optional)"):
                            st.caption("_This shows how the AI agent analyzed your question and chose the best approach_")
                            st.markdown(result['thinking'])
                else:
                    st.error(f"❌ AI Agent call failed")
                    st.caption(f"Error: {result['error']}")
                    
                    # Show debug info
                    if result.get('raw_stream'):
                        with st.expander("🔍 Debug: Raw Response Stream"):
                            st.code('\n'.join(result['raw_stream'][:30]))
                    
                    # Show thinking if available (for debugging)
                    if result.get('thinking'):
                        with st.expander("🧠 Thinking Process (Debug)"):
                            st.text(f"Thinking length: {len(result['thinking'])} chars")
                            st.text(f"Response length: {len(result['response'])} chars")
                            st.code(result['thinking'][:500])
                    
                    # Provide workaround
                    st.info("💡 **Alternative**: Use the CLI test script")
                    st.code("""python test_agents/test_single_agent.py \\
  "AAA_DEV_SYNTHETIC_BANK.CRM_AGG_001.CRM_Customer_360" \\
  "your question here"
""", language="bash")
                    
            except Exception as e:
                st.error("❌ AI query failed. Using fallback search...")
                st.caption(f"Error: {str(e)}")
                
                # Fallback: Simple keyword search
                try:
                    df_customers = load_customer_360()
                    
                    # Simple keyword matching
                    if "platinum" in user_question.lower():
                        df_result = df_customers[df_customers['ACCOUNT_TIER'] == 'PLATINUM']
                        st.info(f"Found {len(df_result)} PLATINUM customers")
                        st.dataframe(df_result[['CUSTOMER_ID', 'FULL_NAME', 'COUNTRY', 'ACCOUNT_TIER']].head(10))
                    
                    elif "high risk" in user_question.lower() or "high-risk" in user_question.lower():
                        df_result = df_customers[df_customers['HIGH_RISK_CUSTOMER'] == True]
                        st.info(f"Found {len(df_result)} high-risk customers")
                        st.dataframe(df_result[['CUSTOMER_ID', 'FULL_NAME', 'OVERALL_RISK_RATING', 'OVERALL_RISK_SCORE']].head(10))
                    
                    elif "pep" in user_question.lower():
                        df_result = df_customers[df_customers['REQUIRES_EXPOSED_PERSON_REVIEW'] == True]
                        st.info(f"Found {len(df_result)} customers requiring PEP review")
                        st.dataframe(df_result[['CUSTOMER_ID', 'FULL_NAME', 'EXPOSED_PERSON_MATCH_TYPE']].head(10))
                    
                    else:
                        st.warning("Unable to process query. Try using the search in Customer 360° tab.")
                
                except Exception as fallback_error:
                    st.error(f"Fallback search also failed: {str(fallback_error)}")
    
    # Query history
    st.markdown("---")
    st.subheader("Recent Queries")
    st.caption("Query history feature coming soon...")

# ============================================================
# TAB 16: Settings
# ============================================================
with tab16:
    st.header("Settings")
    
    # Data freshness
    st.subheader("Data Freshness")
    st.write(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    st.write(f"**Next Refresh:** Automatic refresh every 60 minutes")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Refresh All Data", width="stretch"):
            st.cache_data.clear()
            st.success("✅ All cache cleared! Reloading...")
            st.rerun()
    with col2:
        if st.button("🔄 Clear Error Cache", width="stretch"):
            st.cache_data.clear()
            st.success("✅ Error cache cleared! Refresh the page.")
            st.rerun()
    
    st.markdown("---")
    
    # Display preferences
    st.subheader("Display Preferences")
    rows_per_page = st.select_slider("Rows per page", options=[10, 20, 50, 100], value=20)
    date_format = st.selectbox("Date format", ["YYYY-MM-DD", "DD/MM/YYYY", "MM/DD/YYYY"])
    currency = st.selectbox("Currency", ["CHF", "EUR", "USD", "GBP"])
    
    st.markdown("---")
    
    # Connection status
    st.subheader("Snowflake Connection Status")
    
    try:
        session = get_snowflake_session()
        current_db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
        current_schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
        current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]
        
        st.success("✅ Connected to Snowflake")
        st.write(f"**Database:** {current_db}")
        st.write(f"**Schema:** {current_schema}")
        st.write(f"**User:** {current_user}")
        
        if st.button("🔍 Test Connection"):
            with st.spinner("Testing connection..."):
                result = test_connection()
                if result:
                    st.success("✅ Connection test successful!")
                else:
                    st.error("❌ Connection test failed")
    
    except Exception as e:
        st.error("❌ Not connected to Snowflake")
        st.code(str(e))
        
        st.info("""
        💡 **To configure Snowflake connection:**
        
        1. Create `.streamlit/secrets.toml` in the app directory
        2. Add your Snowflake credentials:
        
        ```toml
        [snowflake]
        account = "your-account"
        user = "your-username"
        password = "your-password"
        warehouse = "your-warehouse"
        database = "AAA_DEV_SYNTHETIC_BANK"
        schema = "CRM_AGG_001"
        role = "ACCOUNTADMIN"
        ```
        """)

# Footer
st.markdown("---")
st.caption("Synthetic Retail Bank v1.0.0 | AAA Synthetic Bank | Built with Streamlit + Snowflake | 2025")

