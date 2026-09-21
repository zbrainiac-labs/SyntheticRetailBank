import streamlit as st
import pandas as pd
from datetime import datetime
from utils.data_loaders import load_customer_360, load_aml_alerts, load_aml_metrics
from utils.visualizations import plot_aml_alert_trend
from utils.ui_helpers import show_profile_picker

st.header("AML & Fraud Detection")

tab_fraud, tab_aml = st.tabs(["Fraud Detection", "AML Monitoring"])

with tab_fraud:
    try:
        df_customers = load_customer_360()

        col1, col2, col3, col4 = st.columns(4)
        total = len(df_customers)
        anomalous = len(df_customers[df_customers['HAS_ANOMALY'] == True])
        with col1:
            st.metric("Total Customers", total)
        with col2:
            st.metric("Anomalous Customers", anomalous)
        with col3:
            hr_anomaly = len(df_customers[(df_customers['HAS_ANOMALY'] == True) & (df_customers['HIGH_RISK_CUSTOMER'] == True)])
            st.metric("High-Risk + Anomaly", hr_anomaly)
        with col4:
            rate = (anomalous / total * 100) if total > 0 else 0
            st.metric("Anomaly Rate", f"{rate:.1f}%")

        st.divider()
        st.subheader("Anomaly Priority Queue")
        df_anomalies = df_customers[df_customers['HAS_ANOMALY'] == True].sort_values('OVERALL_RISK_SCORE', ascending=False)

        if len(df_anomalies) > 0:
            st.warning(f"**{len(df_anomalies)}** customers flagged with anomalous transaction patterns")
            display_cols = ['CUSTOMER_ID', 'FULL_NAME', 'ACCOUNT_TIER', 'COUNTRY',
                          'OVERALL_RISK_RATING', 'OVERALL_RISK_SCORE', 'HAS_ANOMALY']
            df_disp = df_anomalies[display_cols].copy()
            df_disp['OVERALL_RISK_SCORE'] = df_disp['OVERALL_RISK_SCORE'].round(1)
            st.dataframe(
                df_disp,
                width="stretch", height=400, hide_index=True,
                column_order=['CUSTOMER_ID', 'FULL_NAME', 'ACCOUNT_TIER', 'COUNTRY',
                              'OVERALL_RISK_RATING', 'OVERALL_RISK_SCORE'],
            )
            show_profile_picker(df_disp, "fraud")
            csv = df_disp.to_csv(index=False)
            st.download_button("Export Anomaly Report (CSV)", csv,
                             file_name=f"anomaly_report_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
        else:
            st.success("No anomalous transaction patterns detected")
    except Exception as e:
        st.error(f"Error loading fraud data: {str(e)}")

with tab_aml:
    try:
        metrics = load_aml_metrics()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Alerts (90d)", metrics.get('TOTAL_ALERTS', 0))
        with col2:
            st.metric("Unique Customers", metrics.get('UNIQUE_CUSTOMERS', 0))
        with col3:
            st.metric("Anomalous Transactions", metrics.get('ANOMALOUS_TRANSACTIONS', 0))
        with col4:
            if metrics.get('TOTAL_ALERTS', 0) > 0:
                ar = (metrics.get('ANOMALOUS_TRANSACTIONS', 0) / metrics.get('TOTAL_ALERTS', 1)) * 100
                st.metric("Anomaly Rate", f"{ar:.1f}%")

        st.divider()
        df_alerts = load_aml_alerts()

        if len(df_alerts) > 0:
            col1, col2 = st.columns(2)
            with col1:
                fig_trend = plot_aml_alert_trend(df_alerts)
                st.plotly_chart(fig_trend, width="stretch", key="aml_trend")
            with col2:
                st.write(f"**Total Alerts:** {len(df_alerts)}")
                if 'OVERALL_ANOMALY_CLASSIFICATION' in df_alerts.columns:
                    for aclass, count in df_alerts['OVERALL_ANOMALY_CLASSIFICATION'].value_counts().items():
                        icon = "🔴" if aclass == "CRITICAL" else "🟠" if aclass == "HIGH" else "🟡" if aclass == "MODERATE" else "🟢"
                        st.write(f"  {icon} {aclass}: {count}")
                if 'REQUIRES_IMMEDIATE_REVIEW' in df_alerts.columns:
                    st.write(f"**Immediate Review Required:** {int(df_alerts['REQUIRES_IMMEDIATE_REVIEW'].sum())}")

            st.divider()
            display_cols = [c for c in ['CUSTOMER_ID', 'BOOKING_DATE', 'AMOUNT', 'CURRENCY',
                                        'OVERALL_ANOMALY_CLASSIFICATION', 'COMPOSITE_ANOMALY_SCORE',
                                        'REQUIRES_IMMEDIATE_REVIEW'] if c in df_alerts.columns]
            if display_cols:
                df_aml_disp = df_alerts[display_cols].head(100).copy()
                st.dataframe(df_aml_disp, width="stretch", height=400, hide_index=True)
                show_profile_picker(df_aml_disp, "aml")
            csv = df_alerts.to_csv(index=False)
            st.download_button("Export AML Alerts (CSV)", csv,
                             file_name=f"aml_alerts_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
        else:
            st.info("No AML alerts found")
    except Exception as e:
        st.error(f"Error loading AML data: {str(e)}")
