/*
 * 500_REPP_reporting.sql
 * Regulatory reporting: risk, compliance, and supervisory views
 */
DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_CUSTOMER_SUMMARY(
    CUSTOMER_ID VARCHAR(30) COMMENT 'Unique customer identifier for relationship management (CUST_XXXXX format)',
    FULL_NAME VARCHAR(201) COMMENT 'Customer full name (First + Last) for reporting and compliance',
    HAS_ANOMALY BOOLEAN COMMENT 'Flag indicating if customer has anomalous behavior patterns',
    ONBOARDING_DATE DATE COMMENT 'Date when customer relationship was established',
    TOTAL_ACCOUNTS NUMBER(10,0) COMMENT 'Number of accounts held by customer',
    CURRENCY_COUNT NUMBER(10,0) COMMENT 'Number of different currencies in customer portfolio',
    ACCOUNT_CURRENCIES VARCHAR(100) COMMENT 'Comma-separated list of all currencies used by customer',
    TOTAL_TRANSACTIONS NUMBER(10,0) COMMENT 'Total number of transactions across all accounts',
    TOTAL_BASE_AMOUNT DECIMAL(28,2) COMMENT 'Total transaction volume in base currency',
    AVG_TRANSACTION_AMOUNT DECIMAL(28,2) COMMENT 'Average transaction size for customer profiling',
    MAX_TRANSACTION_AMOUNT DECIMAL(28,2) COMMENT 'Largest single transaction for risk assessment',
    ANOMALOUS_TRANSACTIONS NUMBER(10,0) COMMENT 'Count of transactions with suspicious patterns'
) COMMENT = 'Customer 360° Profile and Relationship Risk Assessment (CDD): To provide a consolidated, holistic view of a customer by linking personal details with aggregated transactional behavior (volume, value, account diversity).
Compliance/CDD/KYC: Used for ongoing Customer Due Diligence, monitoring relationship health, and flagging customers with anomalous behavior (HAS_ANOMALY). Risk: Identifies risk exposure through multi-currency holdings and maximum transaction size.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    c.CUSTOMER_ID,
    CONCAT(c.FIRST_NAME, ' ', c.FAMILY_NAME) AS FULL_NAME,
    c.HAS_ANOMALY,
    c.ONBOARDING_DATE,
    COUNT(a.ACCOUNT_ID) AS TOTAL_ACCOUNTS,
    COUNT(DISTINCT a.BASE_CURRENCY) AS CURRENCY_COUNT,
    LISTAGG(DISTINCT a.BASE_CURRENCY, ', ') AS ACCOUNT_CURRENCIES,
    COUNT(t.TRANSACTION_ID) AS TOTAL_TRANSACTIONS,
    SUM(t.AMOUNT) AS TOTAL_BASE_AMOUNT,
    AVG(t.AMOUNT) AS AVG_TRANSACTION_AMOUNT,
    MAX(t.AMOUNT) AS MAX_TRANSACTION_AMOUNT,
    COUNT(CASE WHEN t.DESCRIPTION LIKE '%[%]%' THEN 1 END) AS ANOMALOUS_TRANSACTIONS
FROM {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 c
LEFT JOIN {{ db }}.{{ crm_agg }}.ACCA_AGG_DT_ACCOUNTS a ON c.CUSTOMER_ID = a.CUSTOMER_ID
LEFT JOIN {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES t ON c.CUSTOMER_ID = t.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.FAMILY_NAME, c.HAS_ANOMALY, c.ONBOARDING_DATE;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DAILY_TRANSACTION_SUMMARY(
    TRANSACTION_DATE DATE COMMENT 'Business date for daily reporting and trend analysis',
    TRANSACTION_COUNT NUMBER(10,0) COMMENT 'Total daily transaction volume for operational metrics',
    UNIQUE_CUSTOMERS NUMBER(10,0) COMMENT 'Number of active customers per day',
    TOTAL_BASE_AMOUNT DECIMAL(28,2) COMMENT 'Daily transaction value in base currency',
    AVG_BASE_AMOUNT DECIMAL(28,2) COMMENT 'Average transaction size for market analysis',
    INCOMING_COUNT NUMBER(10,0) COMMENT 'Number of incoming/credit transactions',
    OUTGOING_COUNT NUMBER(10,0) COMMENT 'Number of outgoing/debit transactions',
    ANOMALOUS_COUNT NUMBER(10,0) COMMENT 'Daily suspicious transaction count',
    CURRENCY_COUNT NUMBER(10,0) COMMENT 'Number of different currencies traded daily'
) COMMENT = 'Operational Performance and Daily Liquidity Metrics: To monitor the banks daily activity, transaction volume, and flow (incoming vs. outgoing). This tracks the general health of the payment system and customer engagement.
Operations/Liquidity: Monitors daily transaction counts for system capacity planning. Compliance: Tracks daily count of anomalous transactions for Suspicious Activity Monitoring.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    CAST(BOOKING_DATE AS DATE) AS TRANSACTION_DATE,
    COUNT(*) AS TRANSACTION_COUNT,
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    SUM(AMOUNT) AS TOTAL_BASE_AMOUNT,
    AVG(AMOUNT) AS AVG_BASE_AMOUNT,
    COUNT(CASE WHEN AMOUNT > 0 THEN 1 END) AS INCOMING_COUNT,
    COUNT(CASE WHEN AMOUNT < 0 THEN 1 END) AS OUTGOING_COUNT,
    COUNT(CASE WHEN DESCRIPTION LIKE '%[%]%' THEN 1 END) AS ANOMALOUS_COUNT,
    COUNT(DISTINCT CURRENCY) AS CURRENCY_COUNT
FROM {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES
GROUP BY CAST(BOOKING_DATE AS DATE)
ORDER BY TRANSACTION_DATE;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_CURRENCY_EXPOSURE_CURRENT(
    CURRENCY VARCHAR(3) COMMENT 'Foreign currency code (ISO 4217) for exposure analysis',
    TRANSACTION_COUNT NUMBER(10,0) COMMENT 'Number of transactions in this currency',
    TOTAL_ORIGINAL_AMOUNT DECIMAL(28,2) COMMENT 'Total exposure in original currency',
    TOTAL_CHF_AMOUNT DECIMAL(28,2) COMMENT 'Total exposure converted to CHF (placeholder)',
    AVG_FX_RATE DECIMAL(15,6) COMMENT 'Average exchange rate (placeholder for future FX integration)',
    MIN_FX_RATE DECIMAL(15,6) COMMENT 'Minimum exchange rate observed (placeholder)',
    MAX_FX_RATE DECIMAL(15,6) COMMENT 'Maximum exchange rate observed (placeholder)',
    UNIQUE_CUSTOMERS NUMBER(10,0) COMMENT 'Number of customers with exposure to this currency'
) COMMENT = 'Foreign Exchange (FX) Risk Monitoring (Spot): To provide a current, aggregated view of the banks non-base currency transaction exposure. Used to manage the net open position risk across all currencies.
Market Risk: Measures FX exposure for compliance with internal limits and external regulatory requirements (e.g., assessing large single currency positions).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    CURRENCY,
    COUNT(*) AS TRANSACTION_COUNT,
    SUM(AMOUNT) AS TOTAL_ORIGINAL_AMOUNT,
    SUM(AMOUNT) AS TOTAL_CHF_AMOUNT,
    1.0 AS AVG_FX_RATE,
    1.0 AS MIN_FX_RATE,
    1.0 AS MAX_FX_RATE,
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS
FROM {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES
WHERE CURRENCY != 'CHF'
GROUP BY CURRENCY
ORDER BY TOTAL_CHF_AMOUNT DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_CURRENCY_EXPOSURE_HISTORY(
    EXPOSURE_DATE DATE COMMENT 'Business date for time series analysis',
    CURRENCY VARCHAR(3) COMMENT 'Foreign currency for exposure tracking',
    DAILY_TRANSACTION_COUNT NUMBER(10,0) COMMENT 'Daily transaction volume per currency',
    DAILY_TOTAL_AMOUNT DECIMAL(28,2) COMMENT 'Daily total exposure amount',
    DAILY_AVG_AMOUNT DECIMAL(28,2) COMMENT 'Daily average transaction size',
    DAILY_MIN_AMOUNT DECIMAL(28,2) COMMENT 'Smallest transaction of the day',
    DAILY_MAX_AMOUNT DECIMAL(28,2) COMMENT 'Largest transaction of the day',
    DAILY_UNIQUE_CUSTOMERS NUMBER(10,0) COMMENT 'Number of customers active in this currency',
    ROLLING_7D_TRANSACTION_COUNT NUMBER(10,0) COMMENT '7-day rolling transaction volume',
    ROLLING_7D_TOTAL_AMOUNT DECIMAL(28,2) COMMENT '7-day rolling exposure amount',
    ROLLING_7D_AVG_DAILY_AMOUNT DECIMAL(28,2) COMMENT '7-day average daily exposure',
    AMOUNT_30_DAYS_AGO DECIMAL(28,2) COMMENT 'Exposure amount 30 days prior for comparison',
    GROWTH_RATE_30D_PERCENT DECIMAL(8,2) COMMENT '30-day growth rate percentage for trend monitoring',
    DAILY_VOLUME_CATEGORY VARCHAR(20) COMMENT 'Daily transaction volume risk classification',
    DAILY_EXPOSURE_CATEGORY VARCHAR(20) COMMENT 'Daily exposure amount risk classification'
) COMMENT = 'FX Market Trend and Volatility Analysis: To provide a time series of currency exposure, including 7-day rolling totals and 30-day growth rates. This enables sophisticated analysis of market risk trends.
Market Risk/Treasury: Used for analyzing currency volatility, forecasting future liquidity needs, and classifying exposure into high, medium, and low categories for risk appetite adherence.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    CAST(BOOKING_DATE AS DATE) AS EXPOSURE_DATE,                         -- Business date for time series analysis
    CURRENCY,                                                    -- Foreign currency for exposure tracking
    COUNT(*) AS DAILY_TRANSACTION_COUNT,                         -- Daily transaction volume per currency
    SUM(AMOUNT) AS DAILY_TOTAL_AMOUNT,                           -- Daily total exposure amount
    AVG(AMOUNT) AS DAILY_AVG_AMOUNT,                             -- Daily average transaction size
    MIN(AMOUNT) AS DAILY_MIN_AMOUNT,                             -- Smallest transaction of the day
    MAX(AMOUNT) AS DAILY_MAX_AMOUNT,                             -- Largest transaction of the day
    COUNT(DISTINCT CUSTOMER_ID) AS DAILY_UNIQUE_CUSTOMERS,       -- Number of customers active in this currency

    SUM(COUNT(*)) OVER (
        PARTITION BY CURRENCY
        ORDER BY CAST(BOOKING_DATE AS DATE)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ROLLING_7D_TRANSACTION_COUNT,                           -- 7-day rolling transaction volume

    SUM(SUM(AMOUNT)) OVER (
        PARTITION BY CURRENCY
        ORDER BY CAST(BOOKING_DATE AS DATE)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ROLLING_7D_TOTAL_AMOUNT,                                -- 7-day rolling exposure amount

    AVG(SUM(AMOUNT)) OVER (
        PARTITION BY CURRENCY
        ORDER BY CAST(BOOKING_DATE AS DATE)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ROLLING_7D_AVG_DAILY_AMOUNT,                            -- 7-day average daily exposure

    LAG(SUM(AMOUNT), 30) OVER (
        PARTITION BY CURRENCY
        ORDER BY CAST(BOOKING_DATE AS DATE)
    ) AS AMOUNT_30_DAYS_AGO,                                     -- Exposure amount 30 days prior for comparison

    CASE
        WHEN LAG(SUM(AMOUNT), 30) OVER (PARTITION BY CURRENCY ORDER BY CAST(BOOKING_DATE AS DATE)) > 0
        THEN ROUND(
            ((SUM(AMOUNT) - LAG(SUM(AMOUNT), 30) OVER (PARTITION BY CURRENCY ORDER BY CAST(BOOKING_DATE AS DATE))) /
             LAG(SUM(AMOUNT), 30) OVER (PARTITION BY CURRENCY ORDER BY CAST(BOOKING_DATE AS DATE))) * 100, 2
        )
        ELSE NULL
    END AS GROWTH_RATE_30D_PERCENT,                              -- 30-day growth rate percentage for trend monitoring

    CASE
        WHEN COUNT(*) > 100 THEN 'HIGH_VOLUME'
        WHEN COUNT(*) > 50 THEN 'MEDIUM_VOLUME'
        ELSE 'LOW_VOLUME'
    END AS DAILY_VOLUME_CATEGORY,                                -- Daily transaction volume risk classification

    CASE
        WHEN SUM(AMOUNT) > 1000000 THEN 'HIGH_EXPOSURE'
        WHEN SUM(AMOUNT) > 100000 THEN 'MEDIUM_EXPOSURE'
        ELSE 'LOW_EXPOSURE'
    END AS DAILY_EXPOSURE_CATEGORY                               -- Daily exposure amount risk classification

FROM {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES
WHERE CURRENCY != 'CHF'
GROUP BY CAST(BOOKING_DATE AS DATE), CURRENCY
ORDER BY EXPOSURE_DATE DESC, DAILY_TOTAL_AMOUNT DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_CURRENCY_SETTLEMENT_EXPOSURE(
    SETTLEMENT_DATE DATE COMMENT 'Settlement date for liquidity planning',
    CURRENCY VARCHAR(3) COMMENT 'Currency for settlement risk analysis',
    SETTLEMENT_TRANSACTION_COUNT NUMBER(10,0) COMMENT 'Number of transactions settling on this date',
    SETTLEMENT_TOTAL_AMOUNT DECIMAL(28,2) COMMENT 'Total amount settling in this currency',
    AVG_SETTLEMENT_DAYS DECIMAL(8,2) COMMENT 'Average settlement period for operational planning',
    SAME_DAY_SETTLEMENTS NUMBER(10,0) COMMENT 'Immediate settlement transactions',
    T_PLUS_1_SETTLEMENTS NUMBER(10,0) COMMENT 'Next business day settlements',
    T_PLUS_2_3_SETTLEMENTS NUMBER(10,0) COMMENT 'Standard settlement period transactions',
    DELAYED_SETTLEMENTS NUMBER(10,0) COMMENT 'Delayed settlements requiring attention',
    BACKDATED_SETTLEMENTS NUMBER(10,0) COMMENT 'Backdated settlements (compliance risk)',
    SETTLEMENT_RISK_LEVEL VARCHAR(30) COMMENT 'Overall settlement risk classification',
    SETTLEMENT_TIMING_TYPE VARCHAR(30) COMMENT 'Settlement timing pattern for operational planning'
) COMMENT = 'Settlement Timing and Liquidity Risk Analysis: To analyze the timing of fund settlements (VALUE_DATE vs. BOOKING_DATE) for non-base currencies. It focuses on the settlement lag to manage liquidity and operational risk.
Operational Risk/Treasury: Flags transactions with high settlement risk (delayed or backdated) which are critical for operational stability, liquidity planning, and identifying potential non-compliance patterns.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    CAST(VALUE_DATE AS DATE) AS SETTLEMENT_DATE,                         -- Settlement date for liquidity planning
    CURRENCY,                                                    -- Currency for settlement risk analysis
    COUNT(*) AS SETTLEMENT_TRANSACTION_COUNT,                    -- Number of transactions settling on this date
    SUM(AMOUNT) AS SETTLEMENT_TOTAL_AMOUNT,                      -- Total amount settling in this currency
    AVG(DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE)) AS AVG_SETTLEMENT_DAYS, -- Average settlement period for operational planning

    COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) = 0 THEN 1 END) AS SAME_DAY_SETTLEMENTS,     -- Immediate settlement transactions
    COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) = 1 THEN 1 END) AS T_PLUS_1_SETTLEMENTS,     -- Next business day settlements
    COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) BETWEEN 2 AND 3 THEN 1 END) AS T_PLUS_2_3_SETTLEMENTS, -- Standard settlement period
    COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5 THEN 1 END) AS DELAYED_SETTLEMENTS,      -- Delayed settlements requiring attention
    COUNT(CASE WHEN VALUE_DATE < CAST(BOOKING_DATE AS DATE) THEN 1 END) AS BACKDATED_SETTLEMENTS,               -- Backdated settlements (compliance risk)

    CASE
        WHEN COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5 THEN 1 END) > 0
        THEN 'HIGH_SETTLEMENT_RISK'
        WHEN COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 3 THEN 1 END) >
             COUNT(*) * 0.1
        THEN 'MEDIUM_SETTLEMENT_RISK'
        ELSE 'LOW_SETTLEMENT_RISK'
    END AS SETTLEMENT_RISK_LEVEL,                                -- Overall settlement risk classification

    CASE
        WHEN DAYOFWEEK(CAST(VALUE_DATE AS DATE)) IN (1,7) THEN 'WEEKEND_SETTLEMENT'
        ELSE 'WEEKDAY_SETTLEMENT'
    END AS SETTLEMENT_TIMING_TYPE                                -- Settlement timing pattern for operational planning

FROM {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES
WHERE CURRENCY != 'CHF'
GROUP BY CAST(VALUE_DATE AS DATE), CURRENCY
ORDER BY SETTLEMENT_DATE DESC, SETTLEMENT_TOTAL_AMOUNT DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_ANOMALY_ANALYSIS(
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for compliance tracking',
    FULL_NAME VARCHAR(201) COMMENT 'Customer name for investigation reports',
    IS_ANOMALOUS_CUSTOMER BOOLEAN COMMENT 'Customer-level anomaly flag from profiling',
    TOTAL_TRANSACTIONS NUMBER(10,0) COMMENT 'Total transaction count for baseline comparison',
    ANOMALOUS_TRANSACTIONS NUMBER(10,0) COMMENT 'Count of flagged transactions',
    ANOMALY_PERCENTAGE DECIMAL(8,2) COMMENT 'Percentage of anomalous activity',
    ANOMALOUS_AMOUNT DECIMAL(28,2) COMMENT 'Total value of suspicious transactions',
    ANOMALY_TYPES VARCHAR(2000) COMMENT 'Types of anomalies detected for investigation'
) COMMENT = 'Customer-level anomaly analysis for compliance monitoring, AML investigation, and suspicious activity reporting.
Compliance/AML: Directly supports Anti-Money Laundering operations by identifying and prioritizing high-risk customers for further AML investigation and the generation of Suspicious Activity Reports (SARs).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    c.CUSTOMER_ID,                                               -- Customer identifier for compliance tracking
    CONCAT(c.FIRST_NAME, ' ', c.FAMILY_NAME) AS FULL_NAME,       -- Customer name for investigation reports
    c.HAS_ANOMALY AS IS_ANOMALOUS_CUSTOMER,                      -- Customer-level anomaly flag from profiling
    COUNT(t.TRANSACTION_ID) AS TOTAL_TRANSACTIONS,               -- Total transaction count for baseline comparison
    COUNT(CASE WHEN t.DESCRIPTION LIKE '%[%]%' THEN 1 END) AS ANOMALOUS_TRANSACTIONS, -- Count of flagged transactions
    ROUND(COUNT(CASE WHEN t.DESCRIPTION LIKE '%[%]%' THEN 1 END) * 100.0 / COUNT(t.TRANSACTION_ID), 2) AS ANOMALY_PERCENTAGE, -- Percentage of anomalous activity
    SUM(CASE WHEN t.DESCRIPTION LIKE '%[%]%' THEN t.AMOUNT ELSE 0 END) AS ANOMALOUS_AMOUNT, -- Total value of suspicious transactions
    LISTAGG(DISTINCT
        CASE WHEN t.DESCRIPTION LIKE '%[%]%'
        THEN REGEXP_REPLACE(t.DESCRIPTION, '.*\[(.*?)\].*', '\\1')
        END, ', ') AS ANOMALY_TYPES                              -- Types of anomalies detected for investigation
FROM {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 c
LEFT JOIN {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES t ON c.CUSTOMER_ID = t.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.FAMILY_NAME, c.HAS_ANOMALY
HAVING COUNT(t.TRANSACTION_ID) > 0
ORDER BY ANOMALY_PERCENTAGE DESC, ANOMALOUS_AMOUNT DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_HIGH_RISK_PATTERNS(
    TRANSACTION_ID VARCHAR(50) COMMENT 'Unique identifier for each transaction',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for risk profiling',
    BOOKING_DATE TIMESTAMP_NTZ COMMENT 'Date when transaction was booked in system',
    VALUE_DATE DATE COMMENT 'Settlement date for the transaction',
    AMOUNT DECIMAL(28,2) COMMENT 'Transaction amount in original currency',
    CURRENCY VARCHAR(3) COMMENT 'Currency code (ISO 4217) of the transaction',
    DIRECTION VARCHAR(3) COMMENT 'Transaction flow direction (IN/OUT)',
    DESCRIPTION VARCHAR(500) COMMENT 'Transaction description text for analysis',
    RISK_CATEGORY VARCHAR(30) COMMENT 'Primary risk classification for compliance review (HIGH_AMOUNT/ANOMALOUS/OFFSHORE/CRYPTO/etc.)',
    SETTLEMENT_DAYS NUMBER(10,0) COMMENT 'Transaction Surveillance Hot List for Compliance: To create a focused list of individual transactions that breach pre-defined risk thresholds (e.g., high-amount, off-hours, offshore, crypto, backdated settlements).
    Compliance/Sanctions: Serves as the primary feed for transaction surveillance and compliance review. It uses explicit risk categories (HIGH_AMOUNT, ANOMALOUS, OFFSHORE) to expedite the investigation process.'
) TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    TRANSACTION_ID,
    CUSTOMER_ID,
    BOOKING_DATE,
    VALUE_DATE,
    AMOUNT,
    CURRENCY,
    CASE WHEN AMOUNT > 0 THEN 'IN' ELSE 'OUT' END AS DIRECTION,
    DESCRIPTION,
    CASE
        WHEN AMOUNT >= 10000 THEN 'HIGH_AMOUNT'
        WHEN DESCRIPTION LIKE '%[%]%' THEN 'ANOMALOUS'
        WHEN CURRENCY != 'CHF' AND AMOUNT >= 5000 THEN 'HIGH_FX_AMOUNT'
        WHEN COUNTERPARTY_ACCOUNT LIKE 'OFF_SHORE_%' THEN 'OFFSHORE'
        WHEN COUNTERPARTY_ACCOUNT LIKE 'CRYPTO_%' THEN 'CRYPTO'
        WHEN HOUR(BOOKING_DATE) NOT BETWEEN 9 AND 17 THEN 'OFF_HOURS'
        WHEN VALUE_DATE < CAST(BOOKING_DATE AS DATE) THEN 'BACKDATED_SETTLEMENT'
        WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5 THEN 'DELAYED_SETTLEMENT'
        ELSE 'OTHER'
    END AS RISK_CATEGORY,
    DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) AS SETTLEMENT_DAYS
FROM {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES
WHERE
    AMOUNT >= 10000
    OR DESCRIPTION LIKE '%[%]%'
    OR (CURRENCY != 'CHF' AND AMOUNT >= 5000)
    OR COUNTERPARTY_ACCOUNT LIKE 'OFF_SHORE_%'
    OR COUNTERPARTY_ACCOUNT LIKE 'CRYPTO_%'
    OR HOUR(BOOKING_DATE) NOT BETWEEN 9 AND 17
    OR VALUE_DATE < CAST(BOOKING_DATE AS DATE)  -- Backdated settlements
    OR DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5  -- Delayed settlements
ORDER BY AMOUNT DESC, BOOKING_DATE DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_SETTLEMENT_ANALYSIS(
    BOOKING_DATE DATE COMMENT 'Transaction booking date for settlement tracking',
    VALUE_DATE DATE COMMENT 'Actual settlement date for liquidity planning',
    SETTLEMENT_DAYS NUMBER(10,0) COMMENT 'Settlement period for operational analysis',
    TRANSACTION_COUNT NUMBER(10,0) COMMENT 'Number of transactions with this settlement pattern',
    UNIQUE_CUSTOMERS NUMBER(10,0) COMMENT 'Number of customers affected by settlement timing',
    TOTAL_AMOUNT DECIMAL(28,2) COMMENT 'Total value settling with this timing',
    AVG_AMOUNT DECIMAL(28,2) COMMENT 'Average transaction size for this settlement pattern',
    BACKDATED_COUNT NUMBER(10,0) COMMENT 'Backdated settlements (compliance concern)',
    DELAYED_COUNT NUMBER(10,0) COMMENT 'Delayed settlements (operational risk)',
    SAME_DAY_COUNT NUMBER(10,0) COMMENT 'Same-day settlements',
    NEXT_DAY_COUNT NUMBER(10,0) COMMENT 'Next business day settlements',
    T_PLUS_2_3_COUNT NUMBER(10,0) COMMENT 'Standard settlement period transactions'
) COMMENT = 'Aggregated Operational Settlement Analysis: To provide a view of overall settlement efficiency by grouping transactions based on the number of settlement days.
Operations/Liquidity: Used to manage and improve payment processing efficiency, tracking the volume of same-day, next-day, and delayed settlements. Critical for monitoring compliance with real-time or T+X settlement mandates.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    CAST(BOOKING_DATE AS DATE) AS BOOKING_DATE,                          -- Transaction booking date for settlement tracking
    CAST(VALUE_DATE AS DATE) AS VALUE_DATE,                              -- Actual settlement date for liquidity planning
    DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) AS SETTLEMENT_DAYS,  -- Settlement period for operational analysis
    COUNT(*) AS TRANSACTION_COUNT,                               -- Number of transactions with this settlement pattern
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,            -- Number of customers affected by settlement timing
    SUM(AMOUNT) AS TOTAL_AMOUNT,                                 -- Total value settling with this timing
    AVG(AMOUNT) AS AVG_AMOUNT,                                   -- Average transaction size for this settlement pattern
    COUNT(CASE WHEN VALUE_DATE < CAST(BOOKING_DATE AS DATE) THEN 1 END) AS BACKDATED_COUNT,     -- Backdated settlements (compliance concern)
    COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5 THEN 1 END) AS DELAYED_COUNT,  -- Delayed settlements (operational risk)
    COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) = 0 THEN 1 END) AS SAME_DAY_COUNT,  -- Same-day settlements
    COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) = 1 THEN 1 END) AS NEXT_DAY_COUNT,  -- Next business day settlements
    COUNT(CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) BETWEEN 2 AND 3 THEN 1 END) AS T_PLUS_2_3_COUNT -- Standard settlement period
FROM {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES
GROUP BY CAST(BOOKING_DATE AS DATE), CAST(VALUE_DATE AS DATE), DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE)
ORDER BY BOOKING_DATE DESC, SETTLEMENT_DAYS DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIFECYCLE_ANOMALIES(
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier',
    FULL_NAME VARCHAR(201) COMMENT 'Customer full name',
    LIFECYCLE_EVENT_ID VARCHAR(50) COMMENT 'Lifecycle event that triggered review',
    EVENT_TYPE VARCHAR(30) COMMENT 'Type of lifecycle event',
    EVENT_DATE DATE COMMENT 'Date of lifecycle event',
    DAYS_DORMANT_BEFORE_EVENT NUMBER(10,0) COMMENT 'Days customer was dormant before event',
    TRANSACTION_ID VARCHAR(50) COMMENT 'Suspicious transaction ID',
    TRANSACTION_DATE DATE COMMENT 'Date of suspicious transaction',
    TRANSACTION_AMOUNT DECIMAL(15,2) COMMENT 'Transaction amount',
    ANOMALY_SCORE NUMBER(8,2) COMMENT 'Composite anomaly score from PAYA_AGG_DT_TRANSACTION_ANOMALIES',
    ANOMALY_LEVEL VARCHAR(30) COMMENT 'Overall anomaly level (CRITICAL/HIGH/MODERATE)',
    DAYS_BETWEEN_EVENT_AND_TRANSACTION NUMBER(10,0) COMMENT 'Days between lifecycle event and suspicious transaction',
    AML_RISK_LEVEL VARCHAR(30) COMMENT 'AML risk assessment (CRITICAL/HIGH/MEDIUM/LOW)',
    REQUIRES_SAR_FILING BOOLEAN COMMENT 'Flag indicating if Suspicious Activity Report required',
    INVESTIGATION_STATUS VARCHAR(30) COMMENT 'Investigation status (OPEN/UNDER_REVIEW/CLEARED/SAR_FILED)',
    LAST_UPDATED TIMESTAMP_NTZ COMMENT 'Timestamp when record was last refreshed'
) COMMENT = 'Lifecycle event correlation with transaction anomalies for AML detection. Identifies suspicious patterns like dormant accounts suddenly active after reactivation events, address changes followed by high-value transfers, and employment changes with unusual transaction patterns. Used for SAR filing and compliance investigations.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    c.CUSTOMER_ID,
    CONCAT(c.FIRST_NAME, ' ', c.FAMILY_NAME) AS FULL_NAME,

    e.EVENT_ID AS LIFECYCLE_EVENT_ID,
    e.EVENT_TYPE,
    e.EVENT_DATE,

    DATEDIFF(DAY, prev_txn.LAST_TXN_BEFORE_EVENT, e.EVENT_DATE) AS DAYS_DORMANT_BEFORE_EVENT,

    a.TRANSACTION_ID,
    a.BOOKING_DATE::DATE AS TRANSACTION_DATE,
    a.AMOUNT AS TRANSACTION_AMOUNT,
    a.COMPOSITE_ANOMALY_SCORE AS ANOMALY_SCORE,
    a.OVERALL_ANOMALY_CLASSIFICATION AS ANOMALY_LEVEL,

    DATEDIFF(DAY, e.EVENT_DATE, a.BOOKING_DATE::DATE) AS DAYS_BETWEEN_EVENT_AND_TRANSACTION,

    CASE
        WHEN a.OVERALL_ANOMALY_CLASSIFICATION = 'CRITICAL' AND e.EVENT_TYPE IN ('REACTIVATION', 'ADDRESS_CHANGE') THEN 'CRITICAL'
        WHEN a.OVERALL_ANOMALY_CLASSIFICATION IN ('CRITICAL', 'HIGH') AND DATEDIFF(DAY, prev_txn.LAST_TXN_BEFORE_EVENT, e.EVENT_DATE) > 180 THEN 'HIGH'
        WHEN a.OVERALL_ANOMALY_CLASSIFICATION = 'HIGH' THEN 'MEDIUM'
        ELSE 'LOW'
    END AS AML_RISK_LEVEL,

    CASE
        WHEN a.OVERALL_ANOMALY_CLASSIFICATION = 'CRITICAL'
             AND e.EVENT_TYPE IN ('REACTIVATION', 'ADDRESS_CHANGE')
             AND DATEDIFF(DAY, prev_txn.LAST_TXN_BEFORE_EVENT, e.EVENT_DATE) > 180
        THEN TRUE
        ELSE FALSE
    END AS REQUIRES_SAR_FILING,

    'OPEN' AS INVESTIGATION_STATUS,

    CURRENT_TIMESTAMP() AS LAST_UPDATED

FROM {{ db }}.{{ crm_raw }}.CRMI_RAW_TB_CUSTOMER c

INNER JOIN {{ db }}.{{ crm_raw }}.CRMI_RAW_TB_CUSTOMER_EVENT e
    ON c.CUSTOMER_ID = e.CUSTOMER_ID
    AND e.EVENT_TYPE IN ('REACTIVATION', 'ADDRESS_CHANGE', 'EMPLOYMENT_CHANGE')

INNER JOIN {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES a
    ON c.CUSTOMER_ID = a.CUSTOMER_ID
    AND a.BOOKING_DATE::DATE BETWEEN e.EVENT_DATE AND DATEADD(DAY, 30, e.EVENT_DATE)
    AND a.OVERALL_ANOMALY_CLASSIFICATION IN ('CRITICAL', 'HIGH')

LEFT JOIN LATERAL (
    SELECT MAX(t.BOOKING_DATE::DATE) AS LAST_TXN_BEFORE_EVENT
    FROM {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS acc
    INNER JOIN {{ db }}.{{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS t ON acc.ACCOUNT_ID = t.ACCOUNT_ID
    WHERE acc.CUSTOMER_ID = c.CUSTOMER_ID
    AND t.BOOKING_DATE::DATE < e.EVENT_DATE
) prev_txn

WHERE DATEDIFF(DAY, prev_txn.LAST_TXN_BEFORE_EVENT, e.EVENT_DATE) > 30

ORDER BY AML_RISK_LEVEL DESC, ANOMALY_SCORE DESC;
