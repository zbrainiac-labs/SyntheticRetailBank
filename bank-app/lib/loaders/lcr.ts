import { DB, type LoaderDef } from "@/lib/loaders/types";

const AGG = `${DB}.REP_AGG_V001`;
const RAW = `${DB}.REP_RAW_V001`;

/** LCR Monitoring (port of the_bank_app/pages/lcr_monitoring.py loaders). */
export const LCR_LOADERS: Record<string, LoaderDef> = {
  "lcr.scenario-daily": {
    sql: `SELECT TO_VARCHAR(AS_OF_DATE) AS AS_OF_DATE, STRESS_SCENARIO, SCENARIO_NAME, IS_REGULATORY, LCR_RATIO, LCR_STATUS,
                 HQLA_TOTAL, L1_TOTAL, L2A_TOTAL, L2B_TOTAL, L2B_CAPPED, L2_UNCAPPED, L2_CAPPED,
                 L2B_CAP_APPLIED, L2_CAP_APPLIED, OUTFLOW_RETAIL, OUTFLOW_CORP, OUTFLOW_FI,
                 OUTFLOW_FACILITIES, OUTFLOW_TOTAL, INFLOW_NONFIN, INFLOW_FI, INFLOW_TOTAL,
                 INFLOW_CAPPED, INFLOW_CAP_APPLIED, NET_OUTFLOW, LCR_BUFFER_CHF
            FROM ${AGG}.REPP_AGG_DT_LCR_SCENARIO_DAILY
           ORDER BY AS_OF_DATE, STRESS_SCENARIO`,
  },
  "lcr.events": {
    sql: `SELECT EVENT_ID, TO_VARCHAR(EVENT_DATE) AS EVENT_DATE, EVENT_TYPE, DESCRIPTION, COUNTERPARTY_ID, CONTRACT_ID,
                 AMOUNT_CHF, EXPECTED_DIRECTION
            FROM ${RAW}.LIQI_RAW_TB_EVENTS
         QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY INSERT_TIMESTAMP_UTC DESC) = 1
           ORDER BY EVENT_DATE`,
  },
  "lcr.hqla-holdings": {
    sql: `SELECT ASSET_TYPE, REGULATORY_LEVEL, MAX(HAIRCUT_FACTOR) AS HAIRCUT_FACTOR, COUNT(*) AS HOLDING_COUNT,
                 ROUND(SUM(MARKET_VALUE_CHF), 2) AS MARKET_VALUE_CHF, ROUND(SUM(WEIGHTED_VALUE_CHF), 2) AS WEIGHTED_VALUE_CHF,
                 ROUND(AVG(MARKET_VALUE_CHF), 2) AS AVG_HOLDING_SIZE_CHF
            FROM ${AGG}.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL
           WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM ${AGG}.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL)
           GROUP BY ASSET_TYPE, REGULATORY_LEVEL
           ORDER BY MARKET_VALUE_CHF DESC`,
  },
  "lcr.liquidity-gap": {
    sql: `SELECT STRESS_SCENARIO, BUCKET_CODE, BUCKET_NAME, SORT_ORDER, IN_LCR_HORIZON,
                 CONTRACTUAL_INFLOW_CHF, CONTRACTUAL_OUTFLOW_CHF, BEHAVIORAL_OUTFLOW_CHF,
                 COUNTERBALANCING_CHF, NET_GAP_CHF, CUMULATIVE_GAP_CHF
            FROM ${AGG}.REPP_AGG_DT_LCR_LIQUIDITY_GAP
           WHERE AS_OF_DATE = ?::DATE
           ORDER BY STRESS_SCENARIO, SORT_ORDER`,
    params: [{ name: "date", type: "date" }],
  },
  "lcr.cutoff-deltas": {
    sql: `SELECT ASSET_CLASS, TO_VARCHAR(PRIOR_DATE) AS PRIOR_DATE, HQLA_DELTA_CHF, OUTFLOW_DELTA_CHF, INFLOW_DELTA_CHF,
                 LCR_RATIO, LCR_PRIOR, LCR_DELTA_PP
            FROM ${AGG}.REPP_AGG_DT_LCR_CUTOFF_DELTAS
           WHERE AS_OF_DATE = ?::DATE AND COMPARISON_TYPE = ? AND STRESS_SCENARIO = 'BASE'
           ORDER BY ASSET_CLASS`,
    params: [
      { name: "date", type: "date" },
      { name: "comparison", type: "code", default: "DOD" },
    ],
  },
  "lcr.variance-drivers": {
    sql: `SELECT CLIENT_RANK, POSITION_ID, COUNTERPARTY_NAME, PRODUCT_TYPE, ASSET_CLASS, BUSINESS_UNIT,
                 CLIENT_IMPACT_PP, CONTRIBUTION_PP, SETTLEMENT_IMPACT_PP, DELTA_OUTFLOW_CHF, LCR_DELTA_PP
            FROM ${AGG}.REPP_AGG_VW_LCR_VARIANCE_DRIVERS
           WHERE AS_OF_DATE = ?::DATE AND COMPARISON_TYPE = ? AND STRESS_SCENARIO = ?
           ORDER BY CLIENT_RANK`,
    params: [
      { name: "date", type: "date" },
      { name: "comparison", type: "code", default: "DOD" },
      { name: "scenario", type: "code", default: "BASE" },
    ],
  },
  "lcr.drilldown": {
    sql: `SELECT ASSET_CLASS, PRODUCT_GROUP, COUNTERPARTY_NAME, LCR_TREATMENT, BUSINESS_UNIT, CURRENCY,
                 COUNT(*) AS POSITIONS, SUM(EXPOSURE_CHF) AS EXPOSURE_CHF,
                 SUM(WEIGHTED_CHF) AS WEIGHTED_CHF, SUM(LCR_IMPACT_CHF) AS LCR_IMPACT_CHF
            FROM ${AGG}.REPP_AGG_VW_LCR_DRILLDOWN
           WHERE AS_OF_DATE = ?::DATE AND STRESS_SCENARIO = ?
           GROUP BY ALL`,
    params: [
      { name: "date", type: "date" },
      { name: "scenario", type: "code", default: "BASE" },
    ],
    maxRows: 20000,
  },
  "lcr.drilldown-positions": {
    sql: `SELECT POSITION_ID, POSITION_NAME, PRODUCT_TYPE, COUNTERPARTY_NAME, BUSINESS_UNIT, CURRENCY, RATING,
                 EXPOSURE_CHF, WEIGHTED_CHF, LCR_IMPACT_CHF
            FROM ${AGG}.REPP_AGG_VW_LCR_DRILLDOWN
           WHERE AS_OF_DATE = ?::DATE AND STRESS_SCENARIO = ? AND ASSET_CLASS = ?
           ORDER BY ABS(WEIGHTED_CHF) DESC
           LIMIT 500`,
    params: [
      { name: "date", type: "date" },
      { name: "scenario", type: "code", default: "BASE" },
      { name: "assetClass", type: "code" },
    ],
  },
  "lcr.exceptions": {
    sql: `SELECT CONTRACT_ID, SOURCE_SYSTEM, PRODUCT_TYPE, COUNTERPARTY_ID, CURRENCY, AMOUNT_CCY,
                 ERROR_CODE, ERROR_DETAIL, TO_VARCHAR(LOGGED_AT) AS LOGGED_AT
            FROM ${AGG}.REPP_AGG_DT_LCR_EXCEPTIONS
           WHERE SNAPSHOT_DATE = ?::DATE
           ORDER BY ERROR_CODE, CONTRACT_ID`,
    params: [{ name: "date", type: "date" }],
  },
  "lcr.simulate": {
    sql: `CALL ${AGG}.REPP_AGG_SP_LCR_SIMULATE(?, ?)`,
    params: [
      { name: "date", type: "date" },
      { name: "params", type: "json", default: "{}" },
    ],
  },
  "lcr.monthly-summary": {
    sql: `SELECT REPORTING_MONTH AS REPORT_MONTH, TRADING_DAYS, LCR_AVG AS AVG_LCR_RATIO, LCR_MIN AS MIN_LCR_RATIO,
                 LCR_MAX AS MAX_LCR_RATIO, LCR_VOLATILITY, AVG_HQLA_TOTAL, AVG_OUTFLOW_TOTAL,
                 BREACH_DAYS AS DAYS_BELOW_100_PCT, WARNING_DAYS AS DAYS_100_TO_110_PCT, COMPLIANT_DAYS, BREACH_RATE_PCT,
                 CASE WHEN BREACH_DAYS = 0 THEN 'PASS' WHEN BREACH_DAYS <= 3 THEN 'WARNING' ELSE 'FAIL' END AS COMPLIANCE_STATUS
            FROM ${AGG}.REPP_AGG_VW_LCR_MONTHLY_SUMMARY
           ORDER BY REPORTING_MONTH DESC
           LIMIT 12`,
  },
  "lcr.alerts": {
    sql: `SELECT TO_VARCHAR(AS_OF_DATE) AS AS_OF_DATE, LCR_RATIO, LCR_STATUS, SEVERITY,
                 alert_item.value:severity::STRING AS ALERT_SEVERITY, alert_item.value:type::STRING AS ALERT_TYPE,
                 alert_item.value:message::STRING AS ALERT_MESSAGE, alert_item.value:action::STRING AS RECOMMENDED_ACTION,
                 TO_VARCHAR(ALERT_TIMESTAMP) AS ALERT_TIMESTAMP
            FROM ${AGG}.REPP_AGG_VW_LCR_ALERTS, LATERAL FLATTEN(input => ALL_ALERTS) alert_item
           WHERE TOTAL_ALERT_COUNT > 0
           ORDER BY CASE alert_item.value:severity::STRING WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3
                    WHEN 'INFO' THEN 4 ELSE 5 END, AS_OF_DATE DESC`,
  },
};
