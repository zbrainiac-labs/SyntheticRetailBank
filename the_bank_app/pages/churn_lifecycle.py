import streamlit as st
import pandas as pd
from datetime import datetime
from utils.data_loaders import (
    load_customer_lifecycle, load_lifecycle_summary, calculate_revenue_at_risk,
    load_premium_at_risk, load_dormant_accounts
)
from utils.visualizations import (
    plot_lifecycle_stage_distribution, plot_churn_probability_distribution,
    plot_churn_risk_by_tier, plot_days_inactive_distribution
)
from utils.ui_helpers import show_profile_picker

st.header("Churn & Lifecycle")

try:
    df_lifecycle = load_customer_lifecycle()
    df_summary = load_lifecycle_summary()
    revenue_metrics = calculate_revenue_at_risk()

    if len(df_lifecycle) > 0:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Customers", len(df_lifecycle))
        with col2:
            if revenue_metrics:
                st.metric("At-Risk (>70%)", revenue_metrics.get('AT_RISK_CUSTOMERS', 0))
        with col3:
            if revenue_metrics:
                rev = revenue_metrics.get('TOTAL_REVENUE_AT_RISK', 0)
                st.metric("Revenue at Risk", f"CHF {rev:,.0f}K")
        with col4:
            dormant = len(df_lifecycle[df_lifecycle['DAYS_SINCE_LAST_TRANSACTION'] > 180]) if 'DAYS_SINCE_LAST_TRANSACTION' in df_lifecycle.columns else 0
            st.metric("Dormant (>180d)", dormant)

        st.divider()

        if len(df_summary) > 0:
            col1, col2 = st.columns([2, 1])
            with col1:
                fig = plot_lifecycle_stage_distribution(df_summary)
                st.plotly_chart(fig, width="stretch")
            with col2:
                st.write("**Stage Summary:**")
                for _, row in df_summary.iterrows():
                    st.write(f"**{row['LIFECYCLE_STAGE']}**: {row['CUSTOMER_COUNT']:,.0f}")

        st.divider()
        st.subheader("Churn Probability Analysis")
        col1, col2 = st.columns(2)
        with col1:
            fig_dist = plot_churn_probability_distribution(df_lifecycle)
            st.plotly_chart(fig_dist, width="stretch")
        with col2:
            fig_tier = plot_churn_risk_by_tier(df_lifecycle)
            st.plotly_chart(fig_tier, width="stretch")

        st.divider()
        st.subheader("Premium Customers at Risk (GOLD/PLATINUM)")
        df_premium = load_premium_at_risk()
        if len(df_premium) > 0:
            st.error(f"**{len(df_premium)} premium customers** at high risk of churning (>70%)")
            display_cols = [c for c in ['CUSTOMER_ID', 'FIRST_NAME', 'FAMILY_NAME', 'ACCOUNT_TIER', 'COUNTRY',
                                        'CHURN_PROBABILITY', 'DAYS_SINCE_LAST_TRANSACTION', 'EMAIL', 'PHONE']
                          if c in df_premium.columns]
            if display_cols:
                df_disp = df_premium[display_cols].copy()
                if 'CHURN_PROBABILITY' in df_disp.columns:
                    df_disp['CHURN_PROBABILITY'] = df_disp['CHURN_PROBABILITY'].round(1)
                st.dataframe(df_disp, width="stretch", height=400, hide_index=True)
                show_profile_picker(df_disp, "churn")
            csv = df_premium.to_csv(index=False)
            st.download_button("Export Premium At-Risk (CSV)", csv,
                             file_name=f"premium_at_risk_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
        else:
            st.success("No premium customers currently at high risk")

        st.divider()
        st.subheader("Dormant Accounts (>180 Days)")
        df_dormant = load_dormant_accounts()
        if len(df_dormant) > 0:
            st.warning(f"**{len(df_dormant)} customers** inactive for more than 180 days")
            col1, col2 = st.columns(2)
            with col1:
                fig_inactive = plot_days_inactive_distribution(df_dormant)
                st.plotly_chart(fig_inactive, width="stretch")
            with col2:
                if 'DAYS_SINCE_LAST_TRANSACTION' in df_dormant.columns:
                    st.write(f"**Avg Days Inactive:** {df_dormant['DAYS_SINCE_LAST_TRANSACTION'].mean():.0f}")
                    st.write(f"**Longest Inactive:** {df_dormant['DAYS_SINCE_LAST_TRANSACTION'].max():.0f}")
                if 'ACCOUNT_TIER' in df_dormant.columns:
                    premium_dormant = len(df_dormant[df_dormant['ACCOUNT_TIER'].isin(['GOLD', 'PLATINUM'])])
                    st.write(f"**Premium Dormant:** {premium_dormant}")
        else:
            st.success("No dormant accounts")

        st.divider()
        st.subheader("Recommended Actions")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info("**High Churn Risk (>70%)**\n- Immediate outreach\n- Personalized retention offers\n- Account review within 7 days")
        with col2:
            st.warning("**Dormant Accounts (>180d)**\n- Reactivation campaign\n- Special promotions\n- Product recommendations")
        with col3:
            st.success("**Lifecycle Optimization**\n- Move NEW to ACTIVE faster\n- Prevent ACTIVE to DECLINING\n- Reactivate DORMANT")
    else:
        st.info("Lifecycle data not available. Ensure `CRMA_AGG_DT_CUSTOMER_LIFECYCLE` is deployed.")

except Exception as e:
    st.error(f"Error loading lifecycle data: {str(e)}")
