import { DB, type LoaderDef } from "@/lib/loaders/types";

const C360 = `${DB}.CRM_AGG_V001.CRMA_AGG_VW_CUSTOMER_360_ENRICHED`;
const ANOM = `${DB}.PAY_AGG_V001.PAYA_AGG_DT_TRANSACTION_ANOMALIES`;
/** load_aml_alerts: the 1000 most recent rows of the anomaly table */
const LATEST_ALERTS = `(SELECT * FROM ${ANOM} ORDER BY BOOKING_DATE DESC LIMIT 1000)`;

/** AML & Fraud Detection (port of the_bank_app/pages/aml_fraud.py loaders). */
export const LOADERS_AML: Record<string, LoaderDef> = {
  "aml.fraud-kpis": {
    sql: `SELECT COUNT(*) AS TOTAL_CUSTOMERS,
                 COUNT_IF(HAS_ANOMALY) AS ANOMALOUS_CUSTOMERS,
                 COUNT_IF(HAS_ANOMALY AND HIGH_RISK_CUSTOMER) AS HIGH_RISK_ANOMALY,
                 ROUND(COUNT_IF(HAS_ANOMALY) / NULLIF(COUNT(*), 0) * 100, 1) AS ANOMALY_RATE_PCT
            FROM ${C360}`,
  },
  "aml.anomaly-queue": {
    sql: `SELECT CUSTOMER_ID, FULL_NAME, ACCOUNT_TIER, COUNTRY, OVERALL_RISK_RATING,
                 ROUND(OVERALL_RISK_SCORE, 1) AS OVERALL_RISK_SCORE
            FROM ${C360}
           WHERE HAS_ANOMALY
           ORDER BY OVERALL_RISK_SCORE DESC, CUSTOMER_ID`,
  },
  /** load_aml_metrics: last 90 days relative to CURRENT_DATE (as in Streamlit) */
  "aml.metrics": {
    sql: `SELECT COUNT(*) AS TOTAL_ALERTS, COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
                 COUNT(*) AS ANOMALOUS_TRANSACTIONS
            FROM ${ANOM}
           WHERE BOOKING_DATE >= DATEADD(day, -90, CURRENT_DATE())`,
  },
  /** plot_aml_alert_trend: alerts per booking day over the latest 1000 rows */
  "aml.alert-trend": {
    sql: `SELECT TO_VARCHAR(BOOKING_DATE::DATE) AS ALERT_DATE, COUNT(*) AS ALERT_COUNT
            FROM ${LATEST_ALERTS}
           GROUP BY 1
           ORDER BY 1`,
  },
  "aml.alert-summary": {
    sql: `SELECT OVERALL_ANOMALY_CLASSIFICATION, COUNT(*) AS ALERTS,
                 COUNT_IF(REQUIRES_IMMEDIATE_REVIEW) AS IMMEDIATE_REVIEW
            FROM ${LATEST_ALERTS}
           GROUP BY 1
           ORDER BY ALERTS DESC`,
  },
  "aml.alert-rows": {
    sql: `SELECT CUSTOMER_ID, TO_VARCHAR(BOOKING_DATE) AS BOOKING_DATE, AMOUNT, CURRENCY,
                 OVERALL_ANOMALY_CLASSIFICATION, COMPOSITE_ANOMALY_SCORE, REQUIRES_IMMEDIATE_REVIEW
            FROM ${ANOM}
           ORDER BY ${ANOM}.BOOKING_DATE DESC
           LIMIT 100`,
  },
  /** CSV export as in Streamlit: every column of the latest 1000 alerts (load_aml_alerts). */
  "aml.alert-export": {
    sql: `SELECT * FROM ${LATEST_ALERTS} ORDER BY BOOKING_DATE DESC`,
  },
};
