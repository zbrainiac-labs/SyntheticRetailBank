/*
 * 361_LIQA_reporting.sql
 * Liquidity reporting: regulatory views and alerts
 */
DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY(
    AS_OF_DATE DATE COMMENT 'Reporting date for LCR calculation (daily COB snapshot). Primary time dimension for regulatory reporting to SNB and intraday liquidity monitoring. Used for time-series compliance tracking and historical breach analysis.',
    BANK_ID VARCHAR(20) COMMENT 'Bank identifier for regulatory reporting (SYNTH-CH-999). Used for multi-entity consolidation in group structures and SNB submission identification. Static identifier for this synthetic bank.',
    L1_TOTAL NUMBER(18,2) COMMENT 'Total Level 1 HQLA value in CHF (0% haircut). Highest quality liquid assets including SNB reserves, cash, and government bonds rated AA- or higher. Core component of liquidity buffer providing maximum regulatory benefit.',
    L2A_TOTAL NUMBER(18,2) COMMENT 'Total Level 2A HQLA value in CHF (15% haircut). Swiss canton bonds and covered bonds. High-quality assets with minor liquidity discount. Used for yield optimization while maintaining strong LCR contribution.',
    L2B_TOTAL NUMBER(18,2) COMMENT 'Total Level 2B HQLA value in CHF (50% haircut). SMI equities and AA- corporate bonds. Lower-quality liquid assets with significant haircut. Subject to 40% cap rule and rebalancing constraints.',
    L2_UNCAPPED NUMBER(18,2) COMMENT 'Total Level 2 assets before 40% cap enforcement (L2A + L2B). Used for cap proximity monitoring and pre-emptive portfolio rebalancing decisions. Gap between uncapped and capped indicates regulatory constraint.',
    L2_CAPPED NUMBER(18,2) COMMENT 'Total Level 2 assets after 40% cap rule (max 2/3 of L1). Final L2 value included in HQLA numerator. When cap applied, excess L2 is discarded and provides no LCR benefit.',
    HQLA_TOTAL NUMBER(18,2) COMMENT 'Final total HQLA stock in CHF (L1 + L2_capped). The numerator in LCR formula: LCR = (HQLA / Outflows) × 100. Must maintain minimum CHF 2B for systemic banks. Board-level metric for liquidity risk appetite.',
    CAP_APPLIED BOOLEAN COMMENT 'Flag indicating 40% Basel III cap was triggered (TRUE = L2 exceeded 2/3 of L1). When TRUE, triggers Treasury alert to rebalance portfolio toward Level 1 assets. Monitored daily by ALM.',
    DISCARDED_L2 NUMBER(18,2) COMMENT 'CHF amount of Level 2 assets excluded due to cap breach. Represents opportunity cost - unutilized liquidity buffer. High values trigger strategic review to optimize portfolio composition.',
    TOTAL_HOLDINGS NUMBER(10,0) COMMENT 'Total number of HQLA securities across all levels. Portfolio complexity metric for operational management and custody arrangements. Used for diversification analysis and concentration risk assessment.',
    OUTFLOW_RETAIL NUMBER(18,2) COMMENT 'Total expected 30-day retail deposit outflows in CHF (3-10% base rates with discounts). Most stable funding source due to deposit insurance and relationship banking. Critical for retail funding strategy.',
    OUTFLOW_CORP NUMBER(18,2) COMMENT 'Total expected 30-day corporate deposit outflows in CHF (25-40% rates). Operational vs non-operational designation drives run-off assumptions. Used for commercial banking funding strategy.',
    OUTFLOW_FI NUMBER(18,2) COMMENT 'Total expected 30-day financial institution deposit outflows in CHF (100% assumption). Most volatile wholesale funding requiring immediate liquidity coverage. Monitored for counterparty concentration limits.',
    OUTFLOW_TOTAL NUMBER(18,2) COMMENT 'Total gross stressed 30-day outflows in CHF incl. facility drawdowns. The LCR denominator is NET_OUTFLOW (outflows minus capped inflows).',
    TOTAL_DEPOSIT_ACCOUNTS NUMBER(10,0) COMMENT 'Total number of active deposit accounts across all counterparty types. Deposit base breadth metric for diversification assessment and operational complexity monitoring.',
    LCR_RATIO NUMBER(8,2) COMMENT 'Liquidity Coverage Ratio: (HQLA_Total / Net_Outflow) × 100. Primary Basel III liquidity metric. Regulatory minimum 100% per FINMA Circular 2015/2. Values over 9000% indicate no stressed outflows (exceptional case).',
    LCR_STATUS VARCHAR(10) COMMENT 'Regulatory compliance status: FAIL (<100%), WARNING (100% to <110% internal buffer), PASS (≥110%). Used for automated alert generation, management escalation, and FINMA breach reporting.',
    SEVERITY VARCHAR(10) COMMENT 'Color-coded severity level for dashboards: RED (<100%), YELLOW (100-110%), GREEN (≥110%). Visual indicator for Treasury operations and executive dashboards.',
    LCR_BUFFER_CHF NUMBER(18,2) COMMENT 'Absolute liquidity buffer in CHF (HQLA_Total - Net_Outflow). Excess HQLA beyond minimum regulatory requirement. Positive buffer provides cushion for market stress; negative indicates breach requiring immediate action.',
    LCR_BUFFER_PCT NUMBER(8,2) COMMENT 'Liquidity buffer as percentage of net outflows ((HQLA - Net_Outflow) / Net_Outflow × 100). Indicates buffer size relative to stressed outflows. Target typically 20-50% buffer for operational flexibility and stress resilience.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp when LCR calculation was executed. Used for data lineage, audit trail, and SLA monitoring. Critical for validating calculation freshness (must be within 60 minutes for intraday reporting).',
    OUTFLOW_FACILITIES NUMBER(18,2) COMMENT 'Drawdown outflows on undrawn committed credit and liquidity facilities (5% retail, 10% corporate credit, 30% corporate liquidity, 40% banks).',
    INFLOW_TOTAL NUMBER(18,2) COMMENT 'Weighted 30-day contractual inflows before the cap (50% non-financial, 100% financial, 0% non-performing).',
    INFLOW_CAPPED NUMBER(18,2) COMMENT 'Inflows after the 75% cap: min(INFLOW_TOTAL, 0.75 x OUTFLOW_TOTAL).',
    NET_OUTFLOW NUMBER(18,2) COMMENT 'Net cash outflows = OUTFLOW_TOTAL - INFLOW_CAPPED. LCR denominator.',
    L2B_CAP_APPLIED BOOLEAN COMMENT 'TRUE when the 15% Level 2B cap (15/85 of L1 + L2A) reduced the HQLA stock.',
    INFLOW_CAP_APPLIED BOOLEAN COMMENT 'TRUE when the 75% inflow cap was binding.'
) COMMENT = 'Daily Liquidity Coverage Ratio (LCR) calculation per FINMA Circular 2015/2. Combines HQLA stock and 30-day net cash outflows to calculate the primary Basel III liquidity metric. Built from the BASE rows of REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC incl. L2B 15% cap, L2 40% cap and 75% inflow cap). Determines status FAIL (<100%), WARNING (100-110%), PASS (>=110%) and the liquidity buffer. Critical metric monitored daily by Treasury, reported monthly to Swiss National Bank (SNB), and used for strategic liquidity planning. Regulatory minimum 100% must be maintained at all times. Updated hourly for real-time compliance monitoring and management escalation.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    AS_OF_DATE, 'SYNTH-CH-999', L1_TOTAL, L2A_TOTAL, L2B_TOTAL, L2_UNCAPPED, L2_CAPPED, HQLA_TOTAL,
    (L2B_CAP_APPLIED OR L2_CAP_APPLIED), (L2_UNCAPPED - L2_CAPPED)::NUMBER(18,2),
    L1_COUNT + L2A_COUNT + L2B_COUNT,
    OUTFLOW_RETAIL, OUTFLOW_CORP, OUTFLOW_FI, OUTFLOW_TOTAL,
    RETAIL_ACCOUNTS + CORP_ACCOUNTS + FI_ACCOUNTS,
    LCR_RATIO, LCR_STATUS, SEVERITY, LCR_BUFFER_CHF,
    ROUND(LCR_BUFFER_CHF / NULLIF(NET_OUTFLOW, 0) * 100, 2),
    CURRENT_TIMESTAMP(),
    OUTFLOW_FACILITIES, INFLOW_TOTAL, INFLOW_CAPPED, NET_OUTFLOW, L2B_CAP_APPLIED, INFLOW_CAP_APPLIED
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY
WHERE STRESS_SCENARIO = 'BASE';

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_TREND(
    AS_OF_DATE DATE COMMENT 'Reporting date for trend analysis (daily COB snapshot). Time dimension for historical trend visualization, volatility monitoring, and predictive analytics. Used for identifying adverse liquidity patterns before breaches occur.',
    LCR_RATIO NUMBER(8,2) COMMENT 'Daily LCR ratio snapshot from REPP_AGG_DT_LCR_DAILY. Point-in-time value used for calculating rolling averages, volatility measures, and trend analysis. Primary metric for time-series charting.',
    LCR_7D_AVG NUMBER(8,2) COMMENT '7-day rolling average LCR ratio. Short-term trend indicator for weekly liquidity patterns and intraweek volatility smoothing. Used for identifying immediate trend reversals and operational issues.',
    LCR_30D_AVG NUMBER(8,2) COMMENT '30-day rolling average LCR ratio. Medium-term trend indicator for monthly compliance averaging and business cycle patterns. Used for SNB monthly submissions and management reporting.',
    LCR_90D_AVG NUMBER(8,2) COMMENT '90-day rolling average LCR ratio. Long-term strategic trend indicator for quarterly performance assessment and seasonal pattern identification. Used for Board reporting and strategic liquidity planning.',
    LCR_30D_VOLATILITY NUMBER(8,2) COMMENT '30-day rolling standard deviation of LCR ratio. Statistical measure of LCR stability and predictability. High volatility (>5) indicates unstable liquidity position requiring investigation. Used for risk appetite monitoring.',
    LCR_30D_MIN NUMBER(8,2) COMMENT 'Minimum LCR ratio in past 30 days. Identifies worst-case liquidity stress point within monthly window. Used for stress testing validation and regulatory buffer adequacy assessment.',
    LCR_30D_MAX NUMBER(8,2) COMMENT 'Maximum LCR ratio in past 30 days. Identifies peak liquidity position within monthly window. Large min-max range indicates high volatility. Used for capacity planning and buffer optimization.',
    LCR_DOD_CHANGE NUMBER(8,2) COMMENT 'Day-over-day LCR ratio change in percentage points. Daily velocity metric for intraday monitoring and sudden movement detection. Absolute changes >10pp trigger high volatility alerts requiring immediate investigation.',
    LCR_STATUS VARCHAR(10) COMMENT 'Daily compliance status (PASS/WARNING/FAIL/N/A). Used for consecutive breach tracking and sustained stress identification. Drives automated escalation workflows and management notifications.',
    SEVERITY VARCHAR(10) COMMENT 'Color-coded severity level (GREEN/YELLOW/RED/GRAY). Visual indicator for dashboard alerts and operational monitoring. RED severity triggers immediate Treasury escalation and breach protocols.',
    CONSECUTIVE_BREACHES_3D NUMBER(3,0) COMMENT 'Count of days below 100% threshold in past 3 days (rolling window). Identifies sustained breach patterns requiring escalated regulatory action. Values ≥3 trigger FINMA notification and remediation plan requirement.',
    HIGH_VOLATILITY_ALERT BOOLEAN COMMENT 'Flag for excessive daily volatility (|LCR_DOD_Change| >10pp). Indicates significant intraday HQLA or deposit movements requiring investigation. Triggers Treasury alert to identify root cause (large withdrawals, asset sales, etc.).',
    SUSTAINED_BREACH_ALERT BOOLEAN COMMENT 'Flag for persistent compliance failure (3+ consecutive days below 100%). Indicates structural liquidity problem requiring immediate corrective action. Triggers executive escalation, FINMA notification, and remediation plan development.',
    CRITICAL_BREACH_ALERT BOOLEAN COMMENT 'Flag for severe regulatory breach (LCR <95%). Indicates critical liquidity crisis requiring emergency measures. Triggers Board notification, regulatory reporting, and potential public disclosure depending on severity and duration.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp when trend analysis was calculated. Used for data lineage, audit trail, and calculation freshness validation. Ensures trend metrics reflect latest daily LCR calculations within 60-minute SLA.'
) COMMENT = 'Rolling trend analysis of LCR ratio with statistical metrics and automated alert detection. Calculates 7/30/90-day moving averages, volatility measures, day-over-day changes, and consecutive breach tracking. Enables proactive liquidity risk management by identifying adverse trends before regulatory breaches occur. Triggers Treasury alerts for high volatility (>10% daily change), sustained breaches (3+ consecutive days below 100%), and critical breaches (<95%). Used for Board reporting, management dashboards, and early warning system integration.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH daily_lcr AS (
    SELECT
        AS_OF_DATE,
        LCR_RATIO,
        HQLA_TOTAL,
        OUTFLOW_TOTAL,
        LCR_STATUS,
        SEVERITY
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
),
rolling_stats AS (
    SELECT
        AS_OF_DATE,
        LCR_RATIO,
        HQLA_TOTAL,
        OUTFLOW_TOTAL,
        LCR_STATUS,
        SEVERITY,
        AVG(LCR_RATIO) OVER (
            ORDER BY AS_OF_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS LCR_7D_AVG,
        AVG(LCR_RATIO) OVER (
            ORDER BY AS_OF_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS LCR_30D_AVG,
        AVG(LCR_RATIO) OVER (
            ORDER BY AS_OF_DATE
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS LCR_90D_AVG,
        STDDEV(LCR_RATIO) OVER (
            ORDER BY AS_OF_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS LCR_30D_VOLATILITY,
        MIN(LCR_RATIO) OVER (
            ORDER BY AS_OF_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS LCR_30D_MIN,
        MAX(LCR_RATIO) OVER (
            ORDER BY AS_OF_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS LCR_30D_MAX,
        LAG(LCR_RATIO, 1) OVER (ORDER BY AS_OF_DATE) AS LCR_PREV_DAY,
        LCR_RATIO - LAG(LCR_RATIO, 1) OVER (ORDER BY AS_OF_DATE) AS LCR_DOD_CHANGE,
        SUM(CASE WHEN LCR_RATIO < 100 THEN 1 ELSE 0 END) OVER (
            ORDER BY AS_OF_DATE
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS CONSECUTIVE_BREACHES_3D
    FROM daily_lcr
)
SELECT
    AS_OF_DATE,
    LCR_RATIO,
    ROUND(LCR_7D_AVG, 2) AS LCR_7D_AVG,
    ROUND(LCR_30D_AVG, 2) AS LCR_30D_AVG,
    ROUND(LCR_90D_AVG, 2) AS LCR_90D_AVG,
    ROUND(LCR_30D_VOLATILITY, 2) AS LCR_30D_VOLATILITY,
    ROUND(LCR_30D_MIN, 2) AS LCR_30D_MIN,
    ROUND(LCR_30D_MAX, 2) AS LCR_30D_MAX,
    ROUND(LCR_DOD_CHANGE, 2) AS LCR_DOD_CHANGE,
    LCR_STATUS,
    SEVERITY,
    CONSECUTIVE_BREACHES_3D,
    CASE WHEN ABS(LCR_DOD_CHANGE) > 10 THEN TRUE ELSE FALSE END AS HIGH_VOLATILITY_ALERT,
    CASE WHEN CONSECUTIVE_BREACHES_3D >= 3 THEN TRUE ELSE FALSE END AS SUSTAINED_BREACH_ALERT,
    CASE WHEN LCR_RATIO < 95 THEN TRUE ELSE FALSE END AS CRITICAL_BREACH_ALERT,
    CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM rolling_stats
ORDER BY AS_OF_DATE DESC;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_MONTHLY_SUMMARY (
    REPORTING_MONTH COMMENT 'First day of the reporting month (monthly SNB / FINMA reporting period).',
    TRADING_DAYS COMMENT 'Number of reporting days in the month.',
    LCR_AVG COMMENT 'Average LCR of the month in percent.',
    LCR_MIN COMMENT 'Minimum LCR of the month in percent.',
    LCR_MAX COMMENT 'Maximum LCR of the month in percent.',
    LCR_VOLATILITY COMMENT 'Standard deviation of the daily LCR within the month.',
    AVG_HQLA_TOTAL COMMENT 'Average HQLA stock of the month in CHF.',
    AVG_OUTFLOW_TOTAL COMMENT 'Average gross stressed outflows of the month in CHF.',
    BREACH_DAYS COMMENT 'Number of FAIL days (LCR < 100%) in the month.',
    WARNING_DAYS COMMENT 'Number of WARNING days (100% to <110%) in the month.',
    COMPLIANT_DAYS COMMENT 'Number of PASS days (>= 110%) in the month.',
    BREACH_RATE_PCT COMMENT 'Share of FAIL days in the month in percent.'
)
    COMMENT = 'Monthly summary of LCR metrics for Swiss National Bank (SNB) regulatory reporting per FINMA Circular 2015/2. Aggregates daily LCR ratios by month with summary statistics (average, min, max, volatility), breach day counts (FAIL/WARNING/PASS), and compliance rates. Used by Compliance team for monthly regulatory submissions, by Treasury for performance reporting, and by Executive Management for Board reporting. Critical for demonstrating sustained compliance with Basel III liquidity requirements and trend analysis over time.'
    AS
SELECT
    DATE_TRUNC('MONTH', AS_OF_DATE) AS REPORTING_MONTH,
    COUNT(*) AS TRADING_DAYS,
    ROUND(AVG(LCR_RATIO), 2) AS LCR_AVG,
    ROUND(MIN(LCR_RATIO), 2) AS LCR_MIN,
    ROUND(MAX(LCR_RATIO), 2) AS LCR_MAX,
    ROUND(STDDEV(LCR_RATIO), 2) AS LCR_VOLATILITY,
    ROUND(AVG(HQLA_TOTAL), 2) AS AVG_HQLA_TOTAL,
    ROUND(AVG(OUTFLOW_TOTAL), 2) AS AVG_OUTFLOW_TOTAL,
    SUM(CASE WHEN LCR_STATUS = 'FAIL' THEN 1 ELSE 0 END) AS BREACH_DAYS,
    SUM(CASE WHEN LCR_STATUS = 'WARNING' THEN 1 ELSE 0 END) AS WARNING_DAYS,
    SUM(CASE WHEN LCR_STATUS = 'PASS' THEN 1 ELSE 0 END) AS COMPLIANT_DAYS,
    ROUND(SUM(CASE WHEN LCR_STATUS = 'FAIL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS BREACH_RATE_PCT
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
GROUP BY DATE_TRUNC('MONTH', AS_OF_DATE)
ORDER BY REPORTING_MONTH DESC;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_MONITORING (
    REPORTING_DATE COMMENT 'Reporting (cut-off) date of the LCR figures (daily COB snapshot).',
    BANK_ID COMMENT 'Regulatory bank identifier used in SNB submissions (SYNTH-CH-999).',
    LCR_RATIO COMMENT 'Liquidity Coverage Ratio in percent (HQLA / net cash outflows x 100).',
    LCR_STATUS COMMENT 'Status band: FAIL (<100%), WARNING (100% to <110%), PASS (>=110%).',
    SEVERITY COMMENT 'Severity / colour of the status (RED, YELLOW, GREEN) or alert severity (CRITICAL, HIGH, MEDIUM, INFO).',
    HQLA_TOTAL COMMENT 'Stock of HQLA in CHF after the L2B and L2 caps (LCR numerator).',
    L1_TOTAL COMMENT 'Level 1 HQLA in CHF (0% haircut).',
    L2A_TOTAL COMMENT 'Level 2A HQLA in CHF after 15% haircut.',
    L2B_TOTAL COMMENT 'Level 2B HQLA in CHF after 50% haircut, before the 15% cap.',
    L2_CAPPED COMMENT 'Level 2 HQLA in CHF after the L2B 15% and L2 40% caps.',
    CAP_APPLIED COMMENT 'TRUE when the L2B 15% or L2 40% cap reduced the HQLA stock.',
    DISCARDED_L2 COMMENT 'Level 2 assets in CHF excluded by the caps.',
    OUTFLOW_TOTAL COMMENT 'Total gross stressed 30-day outflows in CHF incl. facility drawdowns.',
    OUTFLOW_RETAIL COMMENT 'Stressed 30-day retail deposit outflows in CHF.',
    OUTFLOW_CORP COMMENT 'Stressed 30-day corporate / SME deposit outflows in CHF.',
    OUTFLOW_FI COMMENT 'Stressed 30-day financial-institution deposit and interbank borrowing outflows in CHF.',
    LCR_BUFFER_CHF COMMENT 'Liquidity buffer in CHF = HQLA - net cash outflows.',
    LCR_BUFFER_PCT COMMENT 'Liquidity buffer in percent of net cash outflows.',
    LCR_7D_AVG COMMENT '7-day rolling average LCR in percent.',
    LCR_30D_AVG COMMENT '30-day rolling average LCR in percent.',
    LCR_90D_AVG COMMENT '90-day rolling average LCR in percent.',
    LCR_30D_VOLATILITY COMMENT '30-day rolling standard deviation of the LCR.',
    LCR_30D_MIN COMMENT 'Minimum LCR of the last 30 days in percent.',
    LCR_30D_MAX COMMENT 'Maximum LCR of the last 30 days in percent.',
    LCR_DOD_CHANGE COMMENT 'Day-over-day LCR change in percentage points.',
    CONSECUTIVE_BREACHES_3D COMMENT 'Number of days below 100% within the last 3 days.',
    HIGH_VOLATILITY_ALERT COMMENT 'TRUE when the absolute day-over-day change exceeds 10pp.',
    SUSTAINED_BREACH_ALERT COMMENT 'TRUE when the LCR was below 100% on 3 consecutive days.',
    CRITICAL_BREACH_ALERT COMMENT 'TRUE when the LCR is below 95%.',
    L1_HOLDING_COUNT COMMENT 'Number of eligible Level 1 positions on the latest date.',
    L1_GROSS_VALUE COMMENT 'Gross market value of eligible Level 1 positions in CHF.',
    L1_CURRENCIES COMMENT 'Currencies of the eligible Level 1 positions.',
    L2A_HOLDING_COUNT COMMENT 'Number of eligible Level 2A positions on the latest date.',
    L2A_GROSS_VALUE COMMENT 'Gross market value of eligible Level 2A positions in CHF.',
    L2B_HOLDING_COUNT COMMENT 'Number of eligible Level 2B positions on the latest date.',
    L2B_GROSS_VALUE COMMENT 'Gross market value of eligible Level 2B positions in CHF.',
    RETAIL_ACCOUNT_COUNT COMMENT 'Number of active retail deposit accounts on the latest date.',
    RETAIL_CUSTOMER_COUNT COMMENT 'Number of distinct retail depositors on the latest date.',
    RETAIL_BALANCE_CHF COMMENT 'Total active retail deposit balance in CHF.',
    RETAIL_AVG_RUN_OFF_PCT COMMENT 'Average final run-off rate of retail accounts in percent.',
    CORP_ACCOUNT_COUNT COMMENT 'Number of active corporate deposit accounts on the latest date.',
    CORP_CUSTOMER_COUNT COMMENT 'Number of distinct corporate depositors on the latest date.',
    CORP_BALANCE_CHF COMMENT 'Total active corporate deposit balance in CHF.',
    CORP_AVG_RUN_OFF_PCT COMMENT 'Average final run-off rate of corporate accounts in percent.',
    LCR_CALCULATION_TIMESTAMP COMMENT 'UTC timestamp of the underlying LCR calculation.',
    VIEW_QUERY_TIMESTAMP COMMENT 'UTC timestamp when this view was queried.'
)
    COMMENT = 'Consolidated LCR monitoring dashboard view combining all key liquidity metrics in a single row. Integrates latest daily LCR ratio, compliance status, HQLA breakdown (L1/L2A/L2B), deposit outflow analysis (Retail/Corporate/FI), trend metrics (7/30/90-day averages), volatility measures, and automated alert flags. Optimized for Treasury dashboards, executive reporting, and downstream application integration. Single source of truth for current liquidity position and regulatory compliance status. Updated in real-time via underlying dynamic tables for operational decision-making and management escalation.'
    AS
WITH latest_lcr AS (
    SELECT *
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
    QUALIFY ROW_NUMBER() OVER (ORDER BY AS_OF_DATE DESC) = 1
),
latest_trend AS (
    SELECT *
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_TREND
    QUALIFY ROW_NUMBER() OVER (ORDER BY AS_OF_DATE DESC) = 1
),
hqla_breakdown AS (
    SELECT
        h.AS_OF_DATE,
        h.REGULATORY_LEVEL,
        COUNT(*) AS HOLDING_COUNT,
        ROUND(SUM(h.MARKET_VALUE_CHF), 2) AS GROSS_VALUE_CHF,
        ROUND(SUM(h.WEIGHTED_VALUE_CHF), 2) AS WEIGHTED_VALUE_CHF,
        LISTAGG(DISTINCT h.CURRENCY, ', ') WITHIN GROUP (ORDER BY h.CURRENCY) AS CURRENCIES
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL h
    WHERE h.HQLA_ELIGIBLE
      AND h.AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL)
    GROUP BY h.AS_OF_DATE, h.REGULATORY_LEVEL
),
outflow_breakdown AS (
    SELECT
        o.AS_OF_DATE,
        o.COUNTERPARTY_TYPE,
        COUNT(*) AS ACCOUNT_COUNT,
        COUNT(DISTINCT o.CUSTOMER_ID) AS CUSTOMER_COUNT,
        ROUND(SUM(o.BALANCE_CHF), 2) AS TOTAL_BALANCE_CHF,
        ROUND(SUM(o.OUTFLOW_AMOUNT_CHF), 2) AS TOTAL_OUTFLOW_CHF,
        ROUND(AVG(o.FINAL_RUN_OFF_RATE) * 100, 2) AS AVG_RUN_OFF_PCT
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL o
    WHERE o.ACCOUNT_STATUS = 'ACTIVE'
      AND o.AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL)
    GROUP BY o.AS_OF_DATE, o.COUNTERPARTY_TYPE
)
SELECT
    l.AS_OF_DATE AS REPORTING_DATE,
    l.BANK_ID,
    l.LCR_RATIO,
    l.LCR_STATUS,
    l.SEVERITY,
    l.HQLA_TOTAL,
    l.L1_TOTAL,
    l.L2A_TOTAL,
    l.L2B_TOTAL,
    l.L2_CAPPED,
    l.CAP_APPLIED,
    l.DISCARDED_L2,
    l.OUTFLOW_TOTAL,
    l.OUTFLOW_RETAIL,
    l.OUTFLOW_CORP,
    l.OUTFLOW_FI,
    l.LCR_BUFFER_CHF,
    l.LCR_BUFFER_PCT,
    t.LCR_7D_AVG,
    t.LCR_30D_AVG,
    t.LCR_90D_AVG,
    t.LCR_30D_VOLATILITY,
    t.LCR_30D_MIN,
    t.LCR_30D_MAX,
    t.LCR_DOD_CHANGE,
    t.CONSECUTIVE_BREACHES_3D,
    t.HIGH_VOLATILITY_ALERT,
    t.SUSTAINED_BREACH_ALERT,
    t.CRITICAL_BREACH_ALERT,
    (SELECT HOLDING_COUNT FROM hqla_breakdown WHERE REGULATORY_LEVEL = 'L1') AS L1_HOLDING_COUNT,
    (SELECT GROSS_VALUE_CHF FROM hqla_breakdown WHERE REGULATORY_LEVEL = 'L1') AS L1_GROSS_VALUE,
    (SELECT CURRENCIES FROM hqla_breakdown WHERE REGULATORY_LEVEL = 'L1') AS L1_CURRENCIES,
    (SELECT HOLDING_COUNT FROM hqla_breakdown WHERE REGULATORY_LEVEL = 'L2A') AS L2A_HOLDING_COUNT,
    (SELECT GROSS_VALUE_CHF FROM hqla_breakdown WHERE REGULATORY_LEVEL = 'L2A') AS L2A_GROSS_VALUE,
    (SELECT HOLDING_COUNT FROM hqla_breakdown WHERE REGULATORY_LEVEL = 'L2B') AS L2B_HOLDING_COUNT,
    (SELECT GROSS_VALUE_CHF FROM hqla_breakdown WHERE REGULATORY_LEVEL = 'L2B') AS L2B_GROSS_VALUE,
    (SELECT ACCOUNT_COUNT FROM outflow_breakdown WHERE COUNTERPARTY_TYPE = 'RETAIL') AS RETAIL_ACCOUNT_COUNT,
    (SELECT CUSTOMER_COUNT FROM outflow_breakdown WHERE COUNTERPARTY_TYPE = 'RETAIL') AS RETAIL_CUSTOMER_COUNT,
    (SELECT TOTAL_BALANCE_CHF FROM outflow_breakdown WHERE COUNTERPARTY_TYPE = 'RETAIL') AS RETAIL_BALANCE_CHF,
    (SELECT AVG_RUN_OFF_PCT FROM outflow_breakdown WHERE COUNTERPARTY_TYPE = 'RETAIL') AS RETAIL_AVG_RUN_OFF_PCT,
    (SELECT ACCOUNT_COUNT FROM outflow_breakdown WHERE COUNTERPARTY_TYPE = 'CORPORATE') AS CORP_ACCOUNT_COUNT,
    (SELECT CUSTOMER_COUNT FROM outflow_breakdown WHERE COUNTERPARTY_TYPE = 'CORPORATE') AS CORP_CUSTOMER_COUNT,
    (SELECT TOTAL_BALANCE_CHF FROM outflow_breakdown WHERE COUNTERPARTY_TYPE = 'CORPORATE') AS CORP_BALANCE_CHF,
    (SELECT AVG_RUN_OFF_PCT FROM outflow_breakdown WHERE COUNTERPARTY_TYPE = 'CORPORATE') AS CORP_AVG_RUN_OFF_PCT,
    l.CALCULATION_TIMESTAMP AS LCR_CALCULATION_TIMESTAMP,
    CURRENT_TIMESTAMP() AS VIEW_QUERY_TIMESTAMP
FROM latest_lcr l
CROSS JOIN latest_trend t;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_ALERTS (
    AS_OF_DATE COMMENT 'Reporting (cut-off) date of the record (daily COB snapshot).',
    LCR_RATIO COMMENT 'Liquidity Coverage Ratio in percent (HQLA / net cash outflows x 100).',
    LCR_STATUS COMMENT 'Status band: FAIL (<100%), WARNING (100% to <110%), PASS (>=110%).',
    SEVERITY COMMENT 'Severity / colour of the status (RED, YELLOW, GREEN) or alert severity (CRITICAL, HIGH, MEDIUM, INFO).',
    ALL_ALERTS COMMENT 'Array of alert objects (severity, type, message, action) for the latest date.',
    TOTAL_ALERT_COUNT COMMENT 'Number of alerts raised for the latest date.',
    HIGHEST_SEVERITY COMMENT 'Highest alert severity of the latest date (CRITICAL, HIGH, MEDIUM, INFO, NONE).',
    ALERT_TIMESTAMP COMMENT 'UTC timestamp when the alerts were evaluated.'
)
    COMMENT = 'Automated alert generation view for LCR compliance monitoring with structured alert messages and severity classification. Generates real-time alerts for regulatory breaches (LCR <100%), critical breaches (LCR <95%), high volatility (>10% daily change), sustained breaches (3+ consecutive days), and 40% cap violations. Each alert includes severity level (CRITICAL/HIGH/MEDIUM/INFO), alert type, descriptive message, and recommended action. Used by Treasury operations for real-time monitoring, by Compliance for breach documentation, and integrated with notification systems for management escalation. Critical for FINMA reporting obligations and audit trail maintenance.'
    AS
WITH latest_lcr AS (
    SELECT *
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
    QUALIFY ROW_NUMBER() OVER (ORDER BY AS_OF_DATE DESC) = 1
),
latest_trend AS (
    SELECT *
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_TREND
    QUALIFY ROW_NUMBER() OVER (ORDER BY AS_OF_DATE DESC) = 1
),
alerts AS (
    SELECT
        l.AS_OF_DATE,
        l.LCR_RATIO,
        l.LCR_STATUS,
        l.SEVERITY,
        t.LCR_DOD_CHANGE,
        t.CONSECUTIVE_BREACHES_3D,
        CASE
            WHEN l.LCR_RATIO < 95 THEN ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'severity', 'CRITICAL',
                    'type', 'LCR_BREACH_CRITICAL',
                    'message', 'LCR ratio ' || l.LCR_RATIO || '% is critically below 95% threshold',
                    'action', 'Immediate escalation to Treasury management required'
                )
            )
            WHEN l.LCR_RATIO < 100 THEN ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'severity', 'HIGH',
                    'type', 'LCR_BREACH',
                    'message', 'LCR ratio ' || l.LCR_RATIO || '% is below 100% regulatory minimum',
                    'action', 'Notify FINMA within 24 hours, initiate remediation plan'
                )
            )
            WHEN l.LCR_RATIO < 105 AND t.CONSECUTIVE_BREACHES_3D >= 2 THEN ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'severity', 'MEDIUM',
                    'type', 'LCR_WARNING',
                    'message', 'LCR ratio ' || l.LCR_RATIO || '% near threshold for ' || t.CONSECUTIVE_BREACHES_3D || ' days',
                    'action', 'Monitor closely, consider increasing HQLA buffer'
                )
            )
            ELSE ARRAY_CONSTRUCT()
        END AS compliance_alerts,
        CASE
            WHEN ABS(t.LCR_DOD_CHANGE) > 10 THEN ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'severity', 'HIGH',
                    'type', 'HIGH_VOLATILITY',
                    'message', 'LCR ratio changed by ' || t.LCR_DOD_CHANGE || '% in one day',
                    'action', 'Investigate large HQLA or deposit movements'
                )
            )
            WHEN ABS(t.LCR_DOD_CHANGE) > 5 THEN ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'severity', 'MEDIUM',
                    'type', 'MODERATE_VOLATILITY',
                    'message', 'LCR ratio changed by ' || t.LCR_DOD_CHANGE || '% in one day',
                    'action', 'Review daily position changes'
                )
            )
            ELSE ARRAY_CONSTRUCT()
        END AS volatility_alerts,
        CASE
            WHEN l.CAP_APPLIED THEN ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'severity', 'INFO',
                    'type', 'L2_CAP_APPLIED',
                    'message', 'Level 2 assets exceeded 40% cap. Discarded: CHF ' || ROUND(l.DISCARDED_L2 / 1000000, 2) || 'M',
                    'action', 'Consider increasing Level 1 holdings or reducing Level 2'
                )
            )
            ELSE ARRAY_CONSTRUCT()
        END AS cap_alerts
    FROM latest_lcr l
    CROSS JOIN latest_trend t
)
SELECT
    AS_OF_DATE,
    LCR_RATIO,
    LCR_STATUS,
    SEVERITY,
    ARRAY_CAT(ARRAY_CAT(compliance_alerts, volatility_alerts), cap_alerts) AS ALL_ALERTS,
    ARRAY_SIZE(ARRAY_CAT(ARRAY_CAT(compliance_alerts, volatility_alerts), cap_alerts)) AS TOTAL_ALERT_COUNT,
    CASE
        WHEN ARRAY_SIZE(compliance_alerts) > 0 AND GET(compliance_alerts[0], 'severity')::STRING = 'CRITICAL' THEN 'CRITICAL'
        WHEN ARRAY_SIZE(compliance_alerts) > 0 THEN 'HIGH'
        WHEN ARRAY_SIZE(volatility_alerts) > 0 THEN 'MEDIUM'
        WHEN ARRAY_SIZE(cap_alerts) > 0 THEN 'INFO'
        ELSE 'NONE'
    END AS HIGHEST_SEVERITY,
    CURRENT_TIMESTAMP() AS ALERT_TIMESTAMP
FROM alerts;

DEFINE VIEW {{ db }}.{{ rep_agg }}.LCRS_AGG_VW_LCR_CURRENT (
    REPORTING_DATE COMMENT 'Reporting (cut-off) date of the LCR figures (daily COB snapshot).',
    BANK_ID COMMENT 'Regulatory bank identifier used in SNB submissions (SYNTH-CH-999).',
    LCR_RATIO_PCT COMMENT 'Liquidity Coverage Ratio in percent (HQLA / net cash outflows x 100).',
    COMPLIANCE_STATUS COMMENT 'Status band: FAIL (<100%), WARNING (100% to <110%), PASS (>=110%).',
    STATUS_COLOR COMMENT 'Dashboard colour of the status: RED, YELLOW, GREEN.',
    HQLA_TOTAL_CHF COMMENT 'Stock of HQLA in CHF after the L2B and L2 caps (LCR numerator).',
    LEVEL1_ASSETS_CHF COMMENT 'Level 1 HQLA in CHF (0% haircut).',
    LEVEL2A_ASSETS_CHF COMMENT 'Level 2A HQLA in CHF after 15% haircut.',
    LEVEL2B_ASSETS_CHF COMMENT 'Level 2B HQLA in CHF after 50% haircut, before the 15% cap.',
    LEVEL2_ASSETS_AFTER_CAP_CHF COMMENT 'Level 2 HQLA in CHF after the L2B 15% and L2 40% caps.',
    LEVEL2_ASSETS_BEFORE_CAP_CHF COMMENT 'Level 2 HQLA in CHF (L2A + L2B) before any cap.',
    LEVEL1_PCT_OF_HQLA COMMENT 'Share of Level 1 in the HQLA stock in percent.',
    LEVEL2_PCT_OF_HQLA COMMENT 'Share of capped Level 2 in the HQLA stock in percent.',
    IS_40PCT_CAP_APPLIED COMMENT 'TRUE when the L2B 15% or L2 40% cap reduced the HQLA stock.',
    DISCARDED_LEVEL2_CHF COMMENT 'Level 2 assets in CHF excluded by the caps.',
    CAP_BUFFER_PCT COMMENT 'Headroom to the 40% L2 cap in percent of the cap (negative = cap binding).',
    NET_CASH_OUTFLOWS_CHF COMMENT 'Net cash outflows in CHF = gross outflows - inflows capped at 75% (LCR denominator).',
    RETAIL_OUTFLOWS_CHF COMMENT 'Stressed 30-day retail deposit outflows in CHF.',
    CORPORATE_OUTFLOWS_CHF COMMENT 'Stressed 30-day corporate / SME deposit outflows in CHF.',
    FINANCIAL_INST_OUTFLOWS_CHF COMMENT 'Stressed 30-day financial-institution deposit and interbank borrowing outflows in CHF.',
    RETAIL_PCT_OF_OUTFLOWS COMMENT 'Share of retail outflows in total gross outflows in percent.',
    CORPORATE_PCT_OF_OUTFLOWS COMMENT 'Share of corporate outflows in total gross outflows in percent.',
    FI_PCT_OF_OUTFLOWS COMMENT 'Share of financial-institution outflows in total gross outflows in percent.',
    LIQUIDITY_BUFFER_CHF COMMENT 'Liquidity buffer in CHF = HQLA - net cash outflows.',
    BUFFER_PCT_OF_OUTFLOWS COMMENT 'Liquidity buffer in percent of net cash outflows.',
    BUFFER_STRENGTH COMMENT 'Buffer classification: STRONG, ADEQUATE, MODERATE, MINIMAL, BREACH.',
    HQLA_SECURITIES_COUNT COMMENT 'Number of eligible HQLA positions.',
    DEPOSIT_ACCOUNTS_COUNT COMMENT 'Number of deposit and borrowing positions in the calculation.',
    LAST_CALCULATED_UTC COMMENT 'UTC timestamp of the last LCR calculation (dynamic table refresh).',
    CALCULATION_AGE_MINUTES COMMENT 'Minutes since the last LCR calculation.',
    DATA_FRESHNESS COMMENT 'Freshness flag of the calculation: FRESH (<=60 min), RECENT (<=120 min), STALE.'
)
COMMENT = 'Current LCR status - single row with all key metrics for executive dashboard. Returns latest available LCR calculation with compliance status, HQLA composition, outflow breakdown, and buffer analysis. Optimized for fast retrieval and notebook queries.'
AS
SELECT
    AS_OF_DATE AS REPORTING_DATE,
    BANK_ID,

    LCR_RATIO AS LCR_RATIO_PCT,
    LCR_STATUS AS COMPLIANCE_STATUS,
    SEVERITY AS STATUS_COLOR,

    HQLA_TOTAL AS HQLA_TOTAL_CHF,
    L1_TOTAL AS LEVEL1_ASSETS_CHF,
    L2A_TOTAL AS LEVEL2A_ASSETS_CHF,
    L2B_TOTAL AS LEVEL2B_ASSETS_CHF,
    L2_CAPPED AS LEVEL2_ASSETS_AFTER_CAP_CHF,
    L2_UNCAPPED AS LEVEL2_ASSETS_BEFORE_CAP_CHF,

    ROUND(L1_TOTAL / NULLIF(HQLA_TOTAL, 0) * 100, 1) AS LEVEL1_PCT_OF_HQLA,
    ROUND(L2_CAPPED / NULLIF(HQLA_TOTAL, 0) * 100, 1) AS LEVEL2_PCT_OF_HQLA,

    CAP_APPLIED AS IS_40PCT_CAP_APPLIED,
    DISCARDED_L2 AS DISCARDED_LEVEL2_CHF,
    ROUND((2.0 / 3.0 * L1_TOTAL - L2_UNCAPPED) / NULLIF(2.0 / 3.0 * L1_TOTAL, 0) * 100, 1) AS CAP_BUFFER_PCT,  -- Headroom to the 40% L2 cap in % of the cap

    NET_OUTFLOW AS NET_CASH_OUTFLOWS_CHF,
    OUTFLOW_RETAIL AS RETAIL_OUTFLOWS_CHF,
    OUTFLOW_CORP AS CORPORATE_OUTFLOWS_CHF,
    OUTFLOW_FI AS FINANCIAL_INST_OUTFLOWS_CHF,

    ROUND(OUTFLOW_RETAIL / NULLIF(OUTFLOW_TOTAL, 0) * 100, 1) AS RETAIL_PCT_OF_OUTFLOWS,
    ROUND(OUTFLOW_CORP / NULLIF(OUTFLOW_TOTAL, 0) * 100, 1) AS CORPORATE_PCT_OF_OUTFLOWS,
    ROUND(OUTFLOW_FI / NULLIF(OUTFLOW_TOTAL, 0) * 100, 1) AS FI_PCT_OF_OUTFLOWS,

    LCR_BUFFER_CHF AS LIQUIDITY_BUFFER_CHF,
    LCR_BUFFER_PCT AS BUFFER_PCT_OF_OUTFLOWS,
    CASE
        WHEN LCR_BUFFER_PCT >= 50 THEN 'STRONG'
        WHEN LCR_BUFFER_PCT >= 20 THEN 'ADEQUATE'
        WHEN LCR_BUFFER_PCT >= 10 THEN 'MODERATE'
        WHEN LCR_BUFFER_PCT >= 0 THEN 'MINIMAL'
        ELSE 'BREACH'
    END AS BUFFER_STRENGTH,

    TOTAL_HOLDINGS AS HQLA_SECURITIES_COUNT,
    TOTAL_DEPOSIT_ACCOUNTS AS DEPOSIT_ACCOUNTS_COUNT,

    CALCULATION_TIMESTAMP AS LAST_CALCULATED_UTC,
    DATEDIFF(MINUTE, CALCULATION_TIMESTAMP, CURRENT_TIMESTAMP()) AS CALCULATION_AGE_MINUTES,
    CASE
        WHEN DATEDIFF(MINUTE, CALCULATION_TIMESTAMP, CURRENT_TIMESTAMP()) <= 60 THEN 'FRESH'
        WHEN DATEDIFF(MINUTE, CALCULATION_TIMESTAMP, CURRENT_TIMESTAMP()) <= 120 THEN 'RECENT'
        ELSE 'STALE'
    END AS DATA_FRESHNESS

FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY)
;

DEFINE VIEW {{ db }}.{{ rep_agg }}.LCRS_AGG_VW_HQLA_BREAKDOWN (
    REPORTING_DATE COMMENT 'Reporting (cut-off) date of the LCR figures (daily COB snapshot).',
    ASSET_LEVEL COMMENT 'Basel III HQLA level of the asset type (L1, L2A, L2B).',
    ASSET_TYPE COMMENT 'HQLA asset type code (e.g. CASH_SNB, GOVT_BOND_CHF, CANTON_BOND, EQUITY_SMI).',
    MARKET_VALUE_CHF COMMENT 'Gross market value in CHF before regulatory haircuts.',
    MARKET_VALUE_BILLIONS COMMENT 'Gross market value in CHF billions (display convenience).',
    HAIRCUT_PCT COMMENT 'Regulatory haircut in percent (0 for L1, 15 for L2A, 50 for L2B).',
    STANDARD_HAIRCUT_PCT COMMENT 'Standard Basel III haircut of the HQLA level in percent (0, 15, 50).',
    HQLA_VALUE_CHF COMMENT 'Post-haircut HQLA value in CHF before the L2 caps.',
    HQLA_VALUE_BILLIONS COMMENT 'Post-haircut HQLA value in CHF billions (display convenience).',
    HOLDINGS_COUNT COMMENT 'Number of eligible HQLA positions in the group.',
    PCT_OF_TOTAL_HQLA COMMENT 'Share of the group in total HQLA in percent.',
    LEVEL_SORT_ORDER COMMENT 'Display order of the HQLA level (1 = L1, 2 = L2A, 3 = L2B).',
    QUALITY_DESCRIPTION COMMENT 'Business description of the HQLA quality level and its haircut.'
)
COMMENT = 'HQLA composition by level (L1/L2A/L2B) and asset type with market value, haircuts, and percentage of total. Used for portfolio analysis, rebalancing decisions, and notebook queries about asset composition. Includes all HQLA holdings for latest reporting date.'
AS
WITH latest_date AS (
    SELECT MAX(AS_OF_DATE) AS AS_OF_DATE
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_HQLA
),
total_hqla AS (
    SELECT HQLA_TOTAL
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
    WHERE AS_OF_DATE = (SELECT AS_OF_DATE FROM latest_date)
)
SELECT
    h.AS_OF_DATE AS REPORTING_DATE,
    h.HQLA_LEVEL AS ASSET_LEVEL,
    h.ASSET_TYPE,

    SUM(h.MARKET_VALUE_CHF) AS MARKET_VALUE_CHF,
    ROUND(SUM(h.MARKET_VALUE_CHF) / 1e9, 2) AS MARKET_VALUE_BILLIONS,

    AVG(h.HAIRCUT_PCT) AS HAIRCUT_PCT,
    CASE
        WHEN h.HQLA_LEVEL = 'L1' THEN 0
        WHEN h.HQLA_LEVEL = 'L2A' THEN 15
        WHEN h.HQLA_LEVEL = 'L2B' THEN 50
        ELSE NULL
    END AS STANDARD_HAIRCUT_PCT,

    SUM(h.HQLA_VALUE_CHF) AS HQLA_VALUE_CHF,
    ROUND(SUM(h.HQLA_VALUE_CHF) / 1e9, 2) AS HQLA_VALUE_BILLIONS,

    COUNT(*) AS HOLDINGS_COUNT,
    ROUND(SUM(h.HQLA_VALUE_CHF) / (SELECT HQLA_TOTAL FROM total_hqla) * 100, 1) AS PCT_OF_TOTAL_HQLA,

    CASE
        WHEN h.HQLA_LEVEL = 'L1' THEN 1
        WHEN h.HQLA_LEVEL = 'L2A' THEN 2
        WHEN h.HQLA_LEVEL = 'L2B' THEN 3
        ELSE 4
    END AS LEVEL_SORT_ORDER,

    CASE
        WHEN h.HQLA_LEVEL = 'L1' THEN 'Highest Quality (No Haircut)'
        WHEN h.HQLA_LEVEL = 'L2A' THEN 'High Quality (15% Haircut)'
        WHEN h.HQLA_LEVEL = 'L2B' THEN 'Medium Quality (50% Haircut)'
        ELSE 'Unknown'
    END AS QUALITY_DESCRIPTION

FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_HQLA h
CROSS JOIN latest_date ld
WHERE h.AS_OF_DATE = ld.AS_OF_DATE
GROUP BY
    h.AS_OF_DATE,
    h.HQLA_LEVEL,
    h.ASSET_TYPE,
    LEVEL_SORT_ORDER,
    QUALITY_DESCRIPTION

ORDER BY
    LEVEL_SORT_ORDER,
    HQLA_VALUE_CHF DESC
;

DEFINE VIEW {{ db }}.{{ rep_agg }}.LCRS_AGG_VW_OUTFLOW_BREAKDOWN (
    REPORTING_DATE COMMENT 'Reporting (cut-off) date of the LCR figures (daily COB snapshot).',
    COUNTERPARTY_TYPE COMMENT 'Outflow segment / counterparty type (RETAIL, CORPORATE, FINANCIAL_INSTITUTION, COMMITTED_FACILITIES).',
    DEPOSIT_BALANCE_CHF COMMENT 'Gross amount of the outflow segment in CHF (balances, borrowings or undrawn facilities).',
    DEPOSIT_BALANCE_BILLIONS COMMENT 'Gross amount of the outflow segment in CHF billions.',
    AVG_RUN_OFF_RATE_PCT COMMENT 'Average effective run-off rate of the segment in percent.',
    MIN_RUN_OFF_RATE_PCT COMMENT 'Minimum effective run-off rate of the segment in percent.',
    MAX_RUN_OFF_RATE_PCT COMMENT 'Maximum effective run-off rate of the segment in percent.',
    OUTFLOW_AMOUNT_CHF COMMENT 'Stressed 30-day outflow in CHF (balance x run-off rate).',
    OUTFLOW_AMOUNT_BILLIONS COMMENT 'Stressed 30-day outflow in CHF billions (display convenience).',
    ACCOUNT_COUNT COMMENT 'Number of positions (accounts, borrowings, facilities) in the segment.',
    PCT_OF_TOTAL_OUTFLOWS COMMENT 'Share of the segment in total stressed outflows in percent.',
    PCT_OF_TOTAL_DEPOSITS COMMENT 'Share of the segment in the total gross amount in percent.',
    EFFECTIVE_RUN_OFF_PCT COMMENT 'Effective run-off rate of the segment in percent (outflow / gross amount).',
    COUNTERPARTY_CATEGORY COMMENT 'Business label of the outflow segment (Retail, Corporate, Wholesale Funding, Committed Facilities).',
    STABILITY_RATING COMMENT 'Funding stability rating derived from the average run-off rate.',
    DISPLAY_ORDER COMMENT 'Display order of the outflow segment in charts and tables.'
)
COMMENT = 'Deposit outflow breakdown by counterparty type showing balances, run-off rates, and outflow amounts. Used for funding strategy analysis and notebook queries about deposit composition. Includes percentage breakdowns for executive reporting and waterfall chart visualization.'
AS
WITH latest_date AS (
    SELECT MAX(AS_OF_DATE) AS AS_OF_DATE
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW
),
total_outflows AS (
    SELECT OUTFLOW_TOTAL
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
    WHERE AS_OF_DATE = (SELECT AS_OF_DATE FROM latest_date)
)
SELECT
    o.AS_OF_DATE AS REPORTING_DATE,
    o.COUNTERPARTY_TYPE,

    SUM(o.BALANCE_CHF) AS DEPOSIT_BALANCE_CHF,
    ROUND(SUM(o.BALANCE_CHF) / 1e9, 2) AS DEPOSIT_BALANCE_BILLIONS,

    AVG(o.RUN_OFF_RATE) * 100 AS AVG_RUN_OFF_RATE_PCT,
    MIN(o.RUN_OFF_RATE) * 100 AS MIN_RUN_OFF_RATE_PCT,
    MAX(o.RUN_OFF_RATE) * 100 AS MAX_RUN_OFF_RATE_PCT,

    SUM(o.OUTFLOW_AMOUNT_CHF) AS OUTFLOW_AMOUNT_CHF,
    ROUND(SUM(o.OUTFLOW_AMOUNT_CHF) / 1e9, 2) AS OUTFLOW_AMOUNT_BILLIONS,

    COUNT(*) AS ACCOUNT_COUNT,
    ROUND(SUM(o.OUTFLOW_AMOUNT_CHF) / (SELECT OUTFLOW_TOTAL FROM total_outflows) * 100, 1) AS PCT_OF_TOTAL_OUTFLOWS,
    ROUND(SUM(o.BALANCE_CHF) / SUM(SUM(o.BALANCE_CHF)) OVER () * 100, 1) AS PCT_OF_TOTAL_DEPOSITS,

    ROUND(SUM(o.OUTFLOW_AMOUNT_CHF) / NULLIF(SUM(o.BALANCE_CHF), 0) * 100, 1) AS EFFECTIVE_RUN_OFF_PCT,

    CASE
        WHEN o.COUNTERPARTY_TYPE LIKE 'RETAIL%' THEN 'Retail Deposits'
        WHEN o.COUNTERPARTY_TYPE LIKE 'CORPORATE%' THEN 'Corporate Deposits'
        WHEN o.COUNTERPARTY_TYPE = 'FINANCIAL_INSTITUTION' THEN 'Wholesale Funding'
        WHEN o.COUNTERPARTY_TYPE = 'COMMITTED_FACILITIES' THEN 'Committed Facilities'
        ELSE 'Other'
    END AS COUNTERPARTY_CATEGORY,

    CASE
        WHEN AVG(o.RUN_OFF_RATE) * 100 <= 5 THEN 'Very Stable'
        WHEN AVG(o.RUN_OFF_RATE) * 100 <= 15 THEN 'Stable'
        WHEN AVG(o.RUN_OFF_RATE) * 100 <= 30 THEN 'Moderate Risk'
        WHEN AVG(o.RUN_OFF_RATE) * 100 <= 50 THEN 'High Risk'
        ELSE 'Very High Risk'
    END AS STABILITY_RATING,

    CASE
        WHEN o.COUNTERPARTY_TYPE = 'RETAIL' THEN 1
        WHEN o.COUNTERPARTY_TYPE = 'CORPORATE' THEN 2
        WHEN o.COUNTERPARTY_TYPE = 'FINANCIAL_INSTITUTION' THEN 3
        WHEN o.COUNTERPARTY_TYPE = 'COMMITTED_FACILITIES' THEN 4
        ELSE 5
    END AS DISPLAY_ORDER

FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW o
CROSS JOIN latest_date ld
WHERE o.AS_OF_DATE = ld.AS_OF_DATE
GROUP BY
    o.AS_OF_DATE,
    o.COUNTERPARTY_TYPE,
    COUNTERPARTY_CATEGORY,
    DISPLAY_ORDER

ORDER BY
    DISPLAY_ORDER
;

DEFINE VIEW {{ db }}.{{ rep_agg }}.LCRS_AGG_VW_TREND_90DAY (
    REPORTING_DATE COMMENT 'Reporting (cut-off) date of the LCR figures (daily COB snapshot).',
    LCR_RATIO_PCT COMMENT 'Liquidity Coverage Ratio in percent (HQLA / net cash outflows x 100).',
    COMPLIANCE_STATUS COMMENT 'Status band: FAIL (<100%), WARNING (100% to <110%), PASS (>=110%).',
    STATUS_COLOR COMMENT 'Dashboard colour of the status: RED, YELLOW, GREEN.',
    HQLA_TOTAL_CHF COMMENT 'Stock of HQLA in CHF after the L2B and L2 caps (LCR numerator).',
    OUTFLOW_TOTAL_CHF COMMENT 'Total gross stressed 30-day outflows in CHF incl. facility drawdowns.',
    LIQUIDITY_BUFFER_CHF COMMENT 'Liquidity buffer in CHF = HQLA - net cash outflows.',
    MA7_LCR_RATIO COMMENT '7-day moving average of the LCR in percent.',
    MA30_LCR_RATIO COMMENT '30-day moving average of the LCR in percent.',
    DAY_OVER_DAY_CHANGE_PCT COMMENT 'Day-over-day LCR change in percentage points.',
    DAY_OVER_DAY_CHANGE_PERCENT COMMENT 'Day-over-day LCR change relative to the prior LCR in percent.',
    VOLATILITY_RATING COMMENT 'Daily volatility rating of the LCR change (Stable, Moderate, High, Very High).',
    IS_BREACH COMMENT 'TRUE when the LCR is below the 100% regulatory minimum.',
    IS_WARNING COMMENT 'TRUE when the LCR is between 100% and 110% (internal warning band).',
    CONSECUTIVE_BREACH_DAYS_3DAY COMMENT 'Number of days below 100% within the last 3 days.',
    TREND_DIRECTION COMMENT 'Direction of the day-over-day move: Improving, Declining or Stable.',
    DISTANCE_FROM_MINIMUM_PCT COMMENT 'LCR minus the 100% regulatory minimum in percentage points.',
    DISTANCE_FROM_TARGET_PCT COMMENT 'LCR minus the 110% internal PASS threshold in percentage points.',
    LAST_CALCULATED_UTC COMMENT 'UTC timestamp of the last LCR calculation (dynamic table refresh).',
    IS_40PCT_CAP_APPLIED COMMENT 'TRUE when the L2B 15% or L2 40% cap reduced the HQLA stock.'
)
COMMENT = '90-day LCR historical trend with 7-day and 30-day moving averages. Used for volatility analysis, compliance tracking, and notebook queries about historical performance. Includes day-over-day change calculations and breach detection for management escalation.'
AS
SELECT
    AS_OF_DATE AS REPORTING_DATE,

    LCR_RATIO AS LCR_RATIO_PCT,
    LCR_STATUS AS COMPLIANCE_STATUS,
    SEVERITY AS STATUS_COLOR,

    HQLA_TOTAL AS HQLA_TOTAL_CHF,
    OUTFLOW_TOTAL AS OUTFLOW_TOTAL_CHF,
    LCR_BUFFER_CHF AS LIQUIDITY_BUFFER_CHF,

    ROUND(AVG(LCR_RATIO) OVER (
        ORDER BY AS_OF_DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS MA7_LCR_RATIO,

    ROUND(AVG(LCR_RATIO) OVER (
        ORDER BY AS_OF_DATE
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) AS MA30_LCR_RATIO,

    ROUND(LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE), 2) AS DAY_OVER_DAY_CHANGE_PCT,

    ROUND((LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE)) /
          NULLIF(LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE), 0) * 100, 2) AS DAY_OVER_DAY_CHANGE_PERCENT,

    CASE
        WHEN ABS(LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE)) <= 2 THEN 'Stable'
        WHEN ABS(LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE)) <= 5 THEN 'Moderate'
        WHEN ABS(LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE)) <= 10 THEN 'High'
        ELSE 'Very High'
    END AS VOLATILITY_RATING,

    CASE WHEN LCR_RATIO < 100 THEN TRUE ELSE FALSE END AS IS_BREACH,
    CASE WHEN LCR_RATIO >= 100 AND LCR_RATIO < 110 THEN TRUE ELSE FALSE END AS IS_WARNING,

    SUM(CASE WHEN LCR_RATIO < 100 THEN 1 ELSE 0 END) OVER (
        ORDER BY AS_OF_DATE
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS CONSECUTIVE_BREACH_DAYS_3DAY,

    CASE
        WHEN LCR_RATIO > LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE) THEN 'Improving'
        WHEN LCR_RATIO < LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE) THEN 'Declining'
        ELSE 'Stable'
    END AS TREND_DIRECTION,

    ROUND(LCR_RATIO - 100, 1) AS DISTANCE_FROM_MINIMUM_PCT,
    ROUND(LCR_RATIO - 110, 1) AS DISTANCE_FROM_TARGET_PCT,  -- 110% = internal PASS threshold

    CALCULATION_TIMESTAMP AS LAST_CALCULATED_UTC,
    CAP_APPLIED AS IS_40PCT_CAP_APPLIED

FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY

WHERE AS_OF_DATE >= DATEADD(day, -90, CURRENT_DATE())

ORDER BY AS_OF_DATE DESC
;

DEFINE VIEW {{ db }}.{{ rep_agg }}.LCRS_AGG_VW_ALERTS_ACTIVE (
    ALERT_DATE COMMENT 'Reporting date on which the alert was raised.',
    ALERT_TYPE COMMENT 'Alert type (LCR_BREACH_CRITICAL, LCR_BREACH, LCR_WARNING, HIGH_VOLATILITY, MODERATE_VOLATILITY, L2_CAP_APPLIED).',
    SEVERITY COMMENT 'Severity / colour of the status (RED, YELLOW, GREEN) or alert severity (CRITICAL, HIGH, MEDIUM, INFO).',
    DESCRIPTION COMMENT 'Alert message describing the condition.',
    CURRENT_LCR_RATIO COMMENT 'LCR in percent on the alert date.',
    ALERT_THRESHOLD COMMENT 'Threshold that triggered the alert (95, 100 or 66.67); NULL for volatility alerts.',
    RECOMMENDED_ACTION COMMENT 'Recommended action for Treasury / Compliance.',
    SEVERITY_PRIORITY COMMENT 'Sort priority of the severity (1 = CRITICAL ... 4 = INFO).',
    DAYS_SINCE_ALERT COMMENT 'Days between the alert date and today.',
    ALERT_AGE COMMENT 'Alert age bucket: Today, Yesterday, This Week, Older.',
    ALERT_CATEGORY COMMENT 'Alert category: Compliance, Risk, Portfolio or Operational Alert.',
    ESCALATION_REQUIRED COMMENT 'TRUE when an LCR breach has not been resolved within 24 hours.',
    ALERT_GENERATED_UTC COMMENT 'UTC timestamp when the alert was generated.'
)
COMMENT = 'Active LCR compliance alerts filtered for dashboard display. Pre-filtered to show only unresolved alerts requiring attention. Used for notebook queries and real-time monitoring dashboards. Includes severity classification and recommended actions for Treasury escalation.'
AS
WITH flattened_alerts AS (
    SELECT
        a.AS_OF_DATE,
        a.LCR_RATIO,
        a.LCR_STATUS,
        a.SEVERITY AS OVERALL_SEVERITY,
        f.VALUE:type::STRING AS ALERT_TYPE,
        f.VALUE:severity::STRING AS SEVERITY,
        f.VALUE:message::STRING AS ALERT_MESSAGE,
        f.VALUE:action::STRING AS SYSTEM_ACTION,
        a.ALERT_TIMESTAMP
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_ALERTS a,
    LATERAL FLATTEN(input => a.ALL_ALERTS) f
    WHERE ARRAY_SIZE(a.ALL_ALERTS) > 0
)
SELECT
    AS_OF_DATE AS ALERT_DATE,
    ALERT_TYPE,
    SEVERITY,
    ALERT_MESSAGE AS DESCRIPTION,

    LCR_RATIO AS CURRENT_LCR_RATIO,
    CASE
        WHEN ALERT_TYPE = 'LCR_BREACH_CRITICAL' THEN 95.0
        WHEN ALERT_TYPE IN ('LCR_BREACH', 'LCR_WARNING') THEN 100.0
        WHEN ALERT_TYPE IN ('HIGH_VOLATILITY', 'MODERATE_VOLATILITY') THEN NULL
        WHEN ALERT_TYPE = 'L2_CAP_APPLIED' THEN 66.67
        ELSE NULL
    END AS ALERT_THRESHOLD,

    SYSTEM_ACTION AS RECOMMENDED_ACTION,

    CASE
        WHEN SEVERITY = 'CRITICAL' THEN 1
        WHEN SEVERITY = 'HIGH' THEN 2
        WHEN SEVERITY = 'MEDIUM' THEN 3
        WHEN SEVERITY = 'INFO' THEN 4
        ELSE 5
    END AS SEVERITY_PRIORITY,

    DATEDIFF(day, AS_OF_DATE, CURRENT_DATE()) AS DAYS_SINCE_ALERT,
    CASE
        WHEN DATEDIFF(day, AS_OF_DATE, CURRENT_DATE()) = 0 THEN 'Today'
        WHEN DATEDIFF(day, AS_OF_DATE, CURRENT_DATE()) = 1 THEN 'Yesterday'
        WHEN DATEDIFF(day, AS_OF_DATE, CURRENT_DATE()) <= 7 THEN 'This Week'
        ELSE 'Older'
    END AS ALERT_AGE,

    CASE
        WHEN ALERT_TYPE IN ('LCR_BREACH', 'LCR_BREACH_CRITICAL', 'LCR_WARNING') THEN 'Compliance Alert'
        WHEN ALERT_TYPE IN ('HIGH_VOLATILITY', 'MODERATE_VOLATILITY') THEN 'Risk Alert'
        WHEN ALERT_TYPE = 'L2_CAP_APPLIED' THEN 'Portfolio Alert'
        ELSE 'Operational Alert'
    END AS ALERT_CATEGORY,

    CASE
        WHEN ALERT_TYPE = 'LCR_BREACH' AND DATEDIFF(hour, AS_OF_DATE, CURRENT_TIMESTAMP()) > 24
        THEN TRUE
        ELSE FALSE
    END AS ESCALATION_REQUIRED,

    ALERT_TIMESTAMP AS ALERT_GENERATED_UTC

FROM flattened_alerts

WHERE AS_OF_DATE >= DATEADD(day, -30, CURRENT_DATE())  -- Last 30 days only

ORDER BY
    SEVERITY_PRIORITY ASC,  -- CRITICAL first
    AS_OF_DATE DESC         -- Most recent first
;
