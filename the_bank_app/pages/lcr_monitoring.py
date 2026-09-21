import streamlit as st
from utils.data_loaders import (
    load_lcr_current_status, load_lcr_trend, load_hqla_holdings_detail,
    load_deposit_outflows_detail, load_lcr_alerts, load_lcr_monthly_summary
)
from utils.visualizations import (
    plot_lcr_trend, plot_hqla_composition, plot_hqla_by_asset_type,
    plot_deposit_outflows_by_type, plot_lcr_gauge, plot_hqla_vs_outflows,
    plot_monthly_compliance_trend
)

st.header("LCR Monitoring")
st.caption("FINMA LCR Reporting -- Real-time liquidity risk monitoring")

try:
    df_current = load_lcr_current_status()

    if len(df_current) > 0:
        current_row = df_current.iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            lcr = current_row['LCR_RATIO']
            delta_color = "normal" if lcr >= 105 else "off" if lcr >= 100 else "inverse"
            st.metric("Current LCR Ratio", f"{lcr:.2f}%", delta=f"{lcr - 100:.2f}pp vs. min", delta_color=delta_color)
        with col2:
            st.metric("Compliance Status", current_row['LCR_STATUS'])
        with col3:
            st.metric("HQLA Total", f"CHF {current_row['HQLA_TOTAL'] / 1e6:,.0f}M")
        with col4:
            st.metric("Net Outflows", f"CHF {current_row['OUTFLOW_TOTAL'] / 1e6:,.0f}M")

        if current_row.get('LCR_RATIO', 100) < 100:
            st.error("LCR BREACH: Below 100% regulatory minimum!")
        elif current_row.get('LCR_RATIO', 100) < 105:
            st.warning("WARNING: Below 105% early warning threshold")

        df_alerts = load_lcr_alerts()
        if len(df_alerts) > 0:
            with st.expander(f"Active Alerts ({len(df_alerts)})", expanded=True):
                for _, alert in df_alerts.iterrows():
                    sev = alert['ALERT_SEVERITY']
                    msg = f"**{alert['ALERT_TYPE']}**: {alert['ALERT_MESSAGE']}"
                    action = f"Action: {alert.get('RECOMMENDED_ACTION', 'N/A')}"
                    if sev == 'CRITICAL':
                        st.error(msg)
                    elif sev in ('HIGH', 'MEDIUM'):
                        st.warning(msg)
                    else:
                        st.info(msg)
                    st.caption(action)

        st.divider()

        lcr_tab1, lcr_tab2, lcr_tab3, lcr_tab4, lcr_tab5 = st.tabs([
            "Trend Analysis", "HQLA Breakdown", "Outflow Analysis", "Components", "Monthly Summary"
        ])

        with lcr_tab1:
            df_trend = load_lcr_trend()
            if len(df_trend) > 0:
                fig = plot_lcr_trend(df_trend)
                st.plotly_chart(fig, width="stretch")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Average LCR", f"{df_trend['LCR_RATIO'].mean():.2f}%")
                with col2:
                    st.metric("Minimum LCR", f"{df_trend['LCR_RATIO'].min():.2f}%")
                with col3:
                    st.metric("Maximum LCR", f"{df_trend['LCR_RATIO'].max():.2f}%")
                with col4:
                    st.metric("Volatility", f"{df_trend['LCR_RATIO'].std():.2f}%")
                with st.expander("Raw Data"):
                    st.dataframe(df_trend[['AS_OF_DATE', 'LCR_RATIO', 'LCR_7D_AVG', 'LCR_30D_AVG', 'LCR_90D_AVG', 'LCR_STATUS']].tail(30), hide_index=True)
            else:
                st.info("No trend data available")

        with lcr_tab2:
            df_hqla = load_hqla_holdings_detail()
            if len(df_hqla) > 0:
                col1, col2 = st.columns(2)
                with col1:
                    fig = plot_hqla_composition(df_hqla)
                    st.plotly_chart(fig, width="stretch")
                    level_totals = df_hqla.groupby('REGULATORY_LEVEL')['WEIGHTED_VALUE_CHF'].sum()
                    for level in ['L1', 'L2A', 'L2B']:
                        if level in level_totals:
                            pct = (level_totals[level] / level_totals.sum()) * 100
                            st.write(f"**{level}**: CHF {level_totals[level]:,.0f}M ({pct:.1f}%)")
                with col2:
                    fig = plot_hqla_by_asset_type(df_hqla)
                    st.plotly_chart(fig, width="stretch")
                if current_row.get('CAP_APPLIED', False):
                    st.warning("40% Cap Rule Applied: Level 2 exceeds 2/3 of Level 1")
                else:
                    st.success("40% Cap Rule Not Applied: Level 2 within limits")
                st.dataframe(df_hqla[['ASSET_TYPE', 'REGULATORY_LEVEL', 'HAIRCUT_FACTOR', 'MARKET_VALUE_CHF', 'WEIGHTED_VALUE_CHF', 'HOLDING_COUNT']], hide_index=True)

        with lcr_tab3:
            df_out = load_deposit_outflows_detail()
            if len(df_out) > 0:
                fig = plot_deposit_outflows_by_type(df_out)
                st.plotly_chart(fig, width="stretch")
                ct = df_out.groupby('COUNTERPARTY_TYPE').agg({'TOTAL_BALANCE_CHF': 'sum', 'TOTAL_OUTFLOW_CHF': 'sum', 'ACCOUNT_COUNT': 'sum'}).reset_index()
                for _, r in ct.iterrows():
                    rate = (r['TOTAL_OUTFLOW_CHF'] / r['TOTAL_BALANCE_CHF'] * 100) if r['TOTAL_BALANCE_CHF'] > 0 else 0
                    st.write(f"**{r['COUNTERPARTY_TYPE']}**: CHF {r['TOTAL_OUTFLOW_CHF']:,.0f}M ({rate:.1f}% run-off)")
                st.dataframe(df_out[['DEPOSIT_TYPE', 'COUNTERPARTY_TYPE', 'BASE_RUN_OFF_RATE', 'TOTAL_BALANCE_CHF', 'TOTAL_OUTFLOW_CHF', 'ACCOUNT_COUNT']], hide_index=True)

        with lcr_tab4:
            col1, col2 = st.columns(2)
            with col1:
                fig = plot_lcr_gauge(current_row['LCR_RATIO'])
                st.plotly_chart(fig, width="stretch")
            with col2:
                fig = plot_hqla_vs_outflows(df_current)
                st.plotly_chart(fig, width="stretch")
            st.latex(r"LCR = \frac{HQLA}{Net\ Cash\ Outflows} \times 100\%")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Level 1 Assets", f"CHF {current_row['L1_TOTAL'] / 1e6:,.0f}M")
            with col2:
                st.metric("Level 2 (Capped)", f"CHF {current_row['L2_CAPPED'] / 1e6:,.0f}M")
            with col3:
                st.metric("LCR Buffer", f"CHF {current_row.get('LCR_BUFFER_CHF', 0) / 1e6:,.0f}M")

        with lcr_tab5:
            df_monthly = load_lcr_monthly_summary()
            if len(df_monthly) > 0:
                fig = plot_monthly_compliance_trend(df_monthly)
                st.plotly_chart(fig, width="stretch")
                st.dataframe(df_monthly[['REPORT_MONTH', 'AVG_LCR_RATIO', 'MIN_LCR_RATIO', 'MAX_LCR_RATIO',
                                        'DAYS_BELOW_100_PCT', 'DAYS_BELOW_105_PCT', 'COMPLIANCE_STATUS']], hide_index=True)
    else:
        st.warning("No LCR data available. Ensure LCR calculation engine is running.")

except Exception as e:
    st.error(f"Error loading LCR data: {str(e)}")
