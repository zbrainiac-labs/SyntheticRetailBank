import streamlit as st
from utils.snowflake_connection import get_snowflake_session
from utils.data_loaders import load_advisor_performance, load_advisor_capacity, load_team_performance
from utils.visualizations import plot_advisor_aum_distribution, plot_advisor_capacity
from utils.ui_helpers import page_header

page_header('Wealth Management', 'Advisors, client portfolios and capacity')

tab_wealth, tab_advisor = st.tabs(["Portfolio Overview", "Advisor Management"])

with tab_wealth:
    df_wealth = load_advisor_performance()
    df_capacity = load_advisor_capacity()
    if df_wealth is not None and len(df_wealth) > 0 and not df_wealth.empty:
        # Collect all available KPIs as (label, value) pairs
        kpis = [("Total Advisors", f"{len(df_wealth)}")]
        if 'TOTAL_AUM' in df_wealth.columns:
            kpis.append(("Total AUM", f"CHF {df_wealth['TOTAL_AUM'].sum():,.0f}M"))
        if 'CLIENT_COUNT' in df_wealth.columns:
            kpis.append(("Total Clients", f"{df_wealth['CLIENT_COUNT'].sum():,.0f}"))
        if 'TOTAL_AUM' in df_wealth.columns and 'CLIENT_COUNT' in df_wealth.columns:
            avg = df_wealth['TOTAL_AUM'].sum() / max(df_wealth['CLIENT_COUNT'].sum(), 1)
            kpis.append(("Avg AUM/Client", f"CHF {avg:,.0f}K"))
        if df_capacity is not None and not df_capacity.empty:
            if 'WORKLOAD_STATUS' in df_capacity.columns:
                avail = len(df_capacity[df_capacity['WORKLOAD_STATUS'] == 'AVAILABLE'])
                kpis.append(("Available", f"{avail}"))
                at_cap = len(df_capacity[df_capacity['WORKLOAD_STATUS'] == 'AT_CAPACITY'])
                kpis.append(("At Capacity", f"{at_cap}"))
            if 'AVAILABLE_CAPACITY' in df_capacity.columns:
                kpis.append(("Total Slots", f"{int(df_capacity['AVAILABLE_CAPACITY'].sum())}"))
            if 'CAPACITY_UTILIZATION_PCT' in df_capacity.columns:
                avg_util = df_capacity['CAPACITY_UTILIZATION_PCT'].mean()
                kpis.append(("Avg Util.", f"{avg_util:.0f}%"))
        # Render all KPIs on one line, no gaps
        cols = st.columns(len(kpis))
        for col, (label, value) in zip(cols, kpis):
            col.metric(label, value)

        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            fig_aum = plot_advisor_aum_distribution(df_wealth)
            st.plotly_chart(fig_aum, width="stretch")
        with col2:
            fig_cap = plot_advisor_capacity(df_wealth)
            st.plotly_chart(fig_cap, width="stretch")

        st.divider()
        display_cols = [c for c in ['ADVISOR_ID', 'ADVISOR_NAME', 'CLIENT_COUNT', 'TOTAL_AUM',
                                    'PERFORMANCE_RATING', 'REGION'] if c in df_wealth.columns]
        if display_cols:
            st.dataframe(df_wealth[display_cols], width="stretch", height=400, hide_index=True)
    else:
        st.info("Wealth Management data not available. Check advisor performance tables.")

with tab_advisor:
    df_capacity = load_advisor_capacity()
    if df_capacity is not None and len(df_capacity) > 0 and not df_capacity.empty:
        kpis = [("Total Advisors", f"{len(df_capacity)}")]
        if 'WORKLOAD_STATUS' in df_capacity.columns:
            avail = len(df_capacity[df_capacity['WORKLOAD_STATUS'] == 'AVAILABLE'])
            kpis.append(("Available Capacity", f"{avail}"))
            at_cap = len(df_capacity[df_capacity['WORKLOAD_STATUS'] == 'AT_CAPACITY'])
            kpis.append(("At Capacity", f"{at_cap}"))
        if 'AVAILABLE_CAPACITY' in df_capacity.columns:
            kpis.append(("Total Slots", f"{int(df_capacity['AVAILABLE_CAPACITY'].sum())}"))
        if 'CAPACITY_UTILIZATION_PCT' in df_capacity.columns:
            avg_util = df_capacity['CAPACITY_UTILIZATION_PCT'].mean()
            kpis.append(("Avg Util.", f"{avg_util:.0f}%"))
        cols = st.columns(len(kpis))
        for col, (label, value) in zip(cols, kpis):
            col.metric(label, value)

        st.divider()
        display_cols = [c for c in ['EMPLOYEE_ID', 'ADVISOR_NAME', 'TOTAL_CLIENTS', 'AVAILABLE_CAPACITY',
                                    'WORKLOAD_STATUS', 'CAPACITY_UTILIZATION_PCT', 'TOTAL_PORTFOLIO_VALUE',
                                    'REGION', 'COUNTRY', 'HIGH_RISK_CLIENTS', 'PERFORMANCE_RATING']
                       if c in df_capacity.columns]
        if display_cols:
            st.dataframe(df_capacity[display_cols], width="stretch", height=500, hide_index=True)

        try:
            df_team = load_team_performance()
            if len(df_team) > 0:
                st.divider()
                st.subheader("Team Performance")
                st.dataframe(df_team, width="stretch", height=300, hide_index=True)
        except Exception:
            pass
    else:
        st.info("Advisor data not available. Check `EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED`.")
