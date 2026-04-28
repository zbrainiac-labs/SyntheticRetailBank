DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES(
    TRANSACTION_ID VARCHAR(50) COMMENT 'Unique identifier for each payment transaction',
    ACCOUNT_ID VARCHAR(30) COMMENT 'Account identifier for transaction allocation and behavioral analysis',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for risk profiling and relationship management',
    BOOKING_DATE TIMESTAMP_NTZ COMMENT 'Date when transaction was booked in the system',
    VALUE_DATE DATE COMMENT 'Settlement date for the transaction',
    AMOUNT DECIMAL(15,2) COMMENT 'Transaction amount in original currency',
    CURRENCY VARCHAR(3) COMMENT 'Currency code (ISO 4217) of the transaction',
    COUNTERPARTY_ACCOUNT VARCHAR(100) COMMENT 'Counterparty account identifier for relationship analysis',
    DESCRIPTION VARCHAR(500) COMMENT 'Transaction description text for pattern analysis',
    CUSTOMER_TOTAL_TRANSACTIONS NUMBER(10,0) COMMENT 'Total historical transactions for this customer (behavioral baseline)',
    AVG_TRANSACTION_AMOUNT DECIMAL(15,2) COMMENT 'Customer average transaction amount for anomaly scoring',
    MEDIAN_TRANSACTION_AMOUNT DECIMAL(15,2) COMMENT 'Customer median transaction amount for statistical analysis',
    AVG_DAILY_TRANSACTION_COUNT NUMBER(8,2) COMMENT 'Customer average daily transaction frequency',
    AMOUNT_ANOMALY_SCORE NUMBER(8,2) COMMENT 'Z-score indicating how many standard deviations amount deviates from customer norm',
    TIMING_ANOMALY_SCORE NUMBER(8,2) COMMENT 'Z-score for transaction timing deviation from customer patterns',
    AMOUNT_ANOMALY_LEVEL VARCHAR(30) COMMENT 'Classification of amount anomaly (EXTREME/HIGH/MODERATE/NORMAL)',
    TIMING_ANOMALY_LEVEL VARCHAR(30) COMMENT 'Classification of timing anomaly (HIGH/MODERATE/NORMAL)',
    VELOCITY_ANOMALY_LEVEL VARCHAR(30) COMMENT 'Classification of transaction velocity anomaly (HIGH/MODERATE/NORMAL)',
    IS_LARGE_TRANSACTION BOOLEAN COMMENT 'Boolean flag for transactions above customer 95th percentile',
    IS_UNUSUAL_WEEKEND_TRANSACTION BOOLEAN COMMENT 'Boolean flag for weekend transactions from non-weekend customers',
    IS_OFF_HOURS_TRANSACTION BOOLEAN COMMENT 'Boolean flag for transactions outside 6 AM - 10 PM',
    SETTLEMENT_DAYS NUMBER(3,0) COMMENT 'Number of days between booking and settlement dates',
    IS_DELAYED_SETTLEMENT BOOLEAN COMMENT 'Boolean flag for settlements delayed more than 5 days',
    IS_BACKDATED_SETTLEMENT BOOLEAN COMMENT 'Boolean flag for value dates before booking dates (critical risk)',
    COMPOSITE_ANOMALY_SCORE NUMBER(8,2) COMMENT 'Weighted composite score combining all anomaly indicators',
    OVERALL_ANOMALY_CLASSIFICATION VARCHAR(30) COMMENT 'Overall risk classification (CRITICAL/HIGH/MODERATE/NORMAL)',
    REQUIRES_IMMEDIATE_REVIEW BOOLEAN COMMENT 'Boolean flag for transactions requiring immediate investigation',
    REQUIRES_ENHANCED_MONITORING BOOLEAN COMMENT 'Boolean flag for transactions requiring enhanced monitoring',
    TRANSACTIONS_LAST_24H NUMBER(5,0) COMMENT 'Number of transactions in last 24 hours for velocity analysis',
    TRANSACTIONS_LAST_7D NUMBER(5,0) COMMENT 'Number of transactions in last 7 days for pattern analysis',
    TRANSACTION_HOUR NUMBER(2,0) COMMENT 'Hour of day when transaction occurred (0-23)',
    TRANSACTION_DAYOFWEEK NUMBER(1,0) COMMENT 'Day of week when transaction occurred (1=Sunday, 7=Saturday)',
    ANOMALY_ANALYSIS_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Timestamp when anomaly analysis was performed'
) COMMENT = 'Advanced payment transaction anomaly detection system analyzing individual account behavioral patterns. Identifies abnormal transactions based on statistical deviations from account norms across amount, frequency, timing, and counterparty dimensions. Provides risk scoring for fraud detection, compliance monitoring, and operational alerting with comprehensive behavioral analytics.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH customer_behavioral_profile AS (
    SELECT 
        ACCOUNT_ID,

        COUNT(*) as total_transactions,
        AVG(AMOUNT) as avg_transaction_amount,
        STDDEV(AMOUNT) as stddev_transaction_amount,
        MEDIAN(AMOUNT) as median_transaction_amount,
        MIN(AMOUNT) as min_transaction_amount,
        MAX(AMOUNT) as max_transaction_amount,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY AMOUNT) as q1_amount,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY AMOUNT) as q3_amount,

        COUNT(DISTINCT DATE(BOOKING_DATE)) as active_days,
        COUNT(*) / GREATEST(COUNT(DISTINCT DATE(BOOKING_DATE)), 1) as avg_daily_transaction_count,

        AVG(EXTRACT(HOUR FROM BOOKING_DATE)) as avg_transaction_hour,
        STDDEV(EXTRACT(HOUR FROM BOOKING_DATE)) as stddev_transaction_hour,

        COUNT(DISTINCT CURRENCY) as distinct_currencies,
        COUNT(DISTINCT COUNTERPARTY_ACCOUNT) as distinct_counterparties,

        AVG(CASE WHEN EXTRACT(DAYOFWEEK FROM BOOKING_DATE) IN (1,7) THEN 1 ELSE 0 END) as weekend_transaction_ratio,

        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY AMOUNT) as large_transaction_threshold

    FROM {{ db }}.{{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS
    WHERE BOOKING_DATE >= CURRENT_DATE - INTERVAL '450 days' 
      AND BOOKING_DATE < CURRENT_DATE
    GROUP BY ACCOUNT_ID
    HAVING COUNT(*) >= 5 
),

transaction_analysis AS (
    SELECT 
        t.*,
        acc.CUSTOMER_ID, 
        cbp.total_transactions as customer_total_transactions,
        cbp.avg_transaction_amount,
        cbp.stddev_transaction_amount,
        cbp.median_transaction_amount,
        cbp.q1_amount,
        cbp.q3_amount,
        cbp.avg_daily_transaction_count,
        cbp.avg_transaction_hour,
        cbp.stddev_transaction_hour,
        cbp.distinct_currencies as customer_distinct_currencies,
        cbp.distinct_counterparties as customer_distinct_counterparties,
        cbp.weekend_transaction_ratio,
        cbp.large_transaction_threshold,

        CASE 
            WHEN cbp.stddev_transaction_amount > 0 THEN
                ABS(t.AMOUNT - cbp.avg_transaction_amount) / cbp.stddev_transaction_amount
            ELSE 0
        END as amount_z_score,

        CASE 
            WHEN cbp.stddev_transaction_hour > 0 THEN
                ABS(EXTRACT(HOUR FROM t.BOOKING_DATE) - cbp.avg_transaction_hour) / cbp.stddev_transaction_hour
            ELSE 0
        END as timing_z_score,

        EXTRACT(HOUR FROM t.BOOKING_DATE) as transaction_hour,
        EXTRACT(DAYOFWEEK FROM t.BOOKING_DATE) as transaction_dayofweek,

        COUNT(*) OVER (
            PARTITION BY t.ACCOUNT_ID 
            ORDER BY DATEDIFF('DAY', '1970-01-01'::DATE, t.BOOKING_DATE)
            RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
        ) - 1 as transactions_last_24h,

        COUNT(*) OVER (
            PARTITION BY t.ACCOUNT_ID 
            ORDER BY DATEDIFF('DAY', '1970-01-01'::DATE, t.BOOKING_DATE)
            RANGE BETWEEN 7 PRECEDING AND CURRENT ROW
        ) - 1 as transactions_last_7d

    FROM {{ db }}.{{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS t
    LEFT JOIN customer_behavioral_profile cbp ON t.ACCOUNT_ID = cbp.ACCOUNT_ID
    LEFT JOIN {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS acc ON t.ACCOUNT_ID = acc.ACCOUNT_ID
    WHERE t.BOOKING_DATE >= CURRENT_DATE - INTERVAL '120 days' 
)

SELECT 
    TRANSACTION_ID,
    ACCOUNT_ID,
    CUSTOMER_ID,
    BOOKING_DATE,
    VALUE_DATE,
    AMOUNT,
    CURRENCY,
    COUNTERPARTY_ACCOUNT,
    DESCRIPTION,

    customer_total_transactions,
    avg_transaction_amount,
    median_transaction_amount,
    avg_daily_transaction_count,

    ROUND(amount_z_score, 2) as amount_anomaly_score,
    ROUND(timing_z_score, 2) as timing_anomaly_score,

    CASE 
        WHEN amount_z_score >= 3.0 THEN 'EXTREME_AMOUNT_ANOMALY'
        WHEN amount_z_score >= 2.0 THEN 'HIGH_AMOUNT_ANOMALY'
        WHEN amount_z_score >= 1.5 THEN 'MODERATE_AMOUNT_ANOMALY'
        ELSE 'NORMAL_AMOUNT'
    END as amount_anomaly_level,

    CASE 
        WHEN timing_z_score >= 2.0 THEN 'HIGH_TIMING_ANOMALY'
        WHEN timing_z_score >= 1.5 THEN 'MODERATE_TIMING_ANOMALY'
        ELSE 'NORMAL_TIMING'
    END as timing_anomaly_level,

    CASE 
        WHEN transactions_last_24h >= (avg_daily_transaction_count * 5) THEN 'HIGH_VELOCITY_ANOMALY'
        WHEN transactions_last_24h >= (avg_daily_transaction_count * 3) THEN 'MODERATE_VELOCITY_ANOMALY'
        ELSE 'NORMAL_VELOCITY'
    END as velocity_anomaly_level,

    CASE 
        WHEN AMOUNT >= large_transaction_threshold THEN TRUE
        ELSE FALSE
    END as is_large_transaction,

    CASE 
        WHEN transaction_dayofweek IN (1,7) AND weekend_transaction_ratio < 0.1 THEN TRUE
        ELSE FALSE
    END as is_unusual_weekend_transaction,

    CASE 
        WHEN transaction_hour < 6 OR transaction_hour > 22 THEN TRUE
        ELSE FALSE
    END as is_off_hours_transaction,

    DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) as settlement_days,
    CASE 
        WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5 THEN TRUE
        ELSE FALSE
    END as is_delayed_settlement,

    CASE 
        WHEN VALUE_DATE < DATE(BOOKING_DATE) THEN TRUE
        ELSE FALSE
    END as is_backdated_settlement,

    ROUND(
        (amount_z_score * 0.35) + 
        (timing_z_score * 0.2) +  
        (CASE WHEN transactions_last_24h >= (avg_daily_transaction_count * 3) THEN 2.0 ELSE 0 END * 0.25) +
        (CASE WHEN transaction_dayofweek IN (1,7) AND weekend_transaction_ratio < 0.1 THEN 1.0 ELSE 0 END * 0.1) +
        (CASE WHEN DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5 THEN 1.5 ELSE 0 END * 0.1),
        2
    ) as composite_anomaly_score,

    CASE 
        WHEN (
            amount_z_score >= 3.0 OR 
            timing_z_score >= 2.0 OR 
            transactions_last_24h >= (avg_daily_transaction_count * 5) OR
            (AMOUNT >= large_transaction_threshold AND transaction_hour < 6) OR
            VALUE_DATE < DATE(BOOKING_DATE) 
        ) THEN 'CRITICAL_ANOMALY'
        WHEN (
            amount_z_score >= 2.0 OR 
            timing_z_score >= 1.5 OR 
            transactions_last_24h >= (avg_daily_transaction_count * 3) OR
            (transaction_dayofweek IN (1,7) AND weekend_transaction_ratio < 0.1) OR
            DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5 
        ) THEN 'HIGH_ANOMALY'
        WHEN (
            amount_z_score >= 1.5 OR 
            timing_z_score >= 1.0 OR 
            transactions_last_24h >= (avg_daily_transaction_count * 2) OR
            DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 3 
        ) THEN 'MODERATE_ANOMALY'
        ELSE 'NORMAL_BEHAVIOR'
    END as overall_anomaly_classification,

    CASE 
        WHEN amount_z_score >= 3.0 OR transactions_last_24h >= (avg_daily_transaction_count * 5) OR VALUE_DATE < DATE(BOOKING_DATE) THEN TRUE
        ELSE FALSE
    END as requires_immediate_review,

    CASE 
        WHEN amount_z_score >= 2.0 OR timing_z_score >= 2.0 OR transactions_last_24h >= (avg_daily_transaction_count * 3) OR DATEDIFF(DAY, BOOKING_DATE, VALUE_DATE) > 5 THEN TRUE
        ELSE FALSE
    END as requires_enhanced_monitoring,

    transactions_last_24h,
    transactions_last_7d,
    transaction_hour,
    transaction_dayofweek,

    CURRENT_TIMESTAMP() as anomaly_analysis_timestamp

FROM transaction_analysis
WHERE customer_total_transactions IS NOT NULL 
ORDER BY composite_anomaly_score DESC, BOOKING_DATE DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_ACCOUNT_BALANCES(
    ACCOUNT_ID VARCHAR(30) COMMENT 'Unique account identifier for balance tracking',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for relationship management',
    ACCOUNT_TYPE VARCHAR(20) COMMENT 'Type of account (CHECKING/SAVINGS/BUSINESS/INVESTMENT)',
    BASE_CURRENCY VARCHAR(3) COMMENT 'Base currency of the account',
    ACCOUNT_STATUS VARCHAR(20) COMMENT 'Current status of the account (ACTIVE/INACTIVE/CLOSED)',
    CURRENT_BALANCE_BASE DECIMAL(18,2) COMMENT 'Current account balance in base currency (CHF)',
    TOTAL_CREDITS_BASE DECIMAL(18,2) COMMENT 'Total credit transactions in base currency',
    TOTAL_DEBITS_BASE DECIMAL(18,2) COMMENT 'Total debit transactions in base currency',
    CURRENT_BALANCE_BASE_CURRENCY DECIMAL(18,2) COMMENT 'Current balance converted to account base currency using FX rates',
    TOTAL_TRANSACTIONS NUMBER(10,0) COMMENT 'Total number of transactions for this account',
    CREDIT_TRANSACTIONS NUMBER(10,0) COMMENT 'Number of credit (incoming) transactions',
    DEBIT_TRANSACTIONS NUMBER(10,0) COMMENT 'Number of debit (outgoing) transactions',
    AVG_TRANSACTION_AMOUNT_BASE DECIMAL(18,2) COMMENT 'Average transaction amount in base currency',
    MIN_TRANSACTION_AMOUNT_BASE DECIMAL(18,2) COMMENT 'Minimum transaction amount in base currency',
    MAX_TRANSACTION_AMOUNT_BASE DECIMAL(18,2) COMMENT 'Maximum transaction amount in base currency',
    ACTIVITY_LEVEL VARCHAR(20) COMMENT 'Account activity classification (INACTIVE/DORMANT/LOW/MODERATE/HIGH)',
    BALANCE_CATEGORY VARCHAR(20) COMMENT 'Balance classification (OVERDRAWN/ZERO/LOW/MODERATE/HIGH/VERY_HIGH)',
    IS_OVERDRAWN BOOLEAN COMMENT 'Boolean flag for accounts with negative balance below threshold',
    IS_DORMANT BOOLEAN COMMENT 'Boolean flag for accounts with no recent activity but historical transactions',
    HAS_LARGE_RECENT_MOVEMENTS BOOLEAN COMMENT 'Boolean flag for accounts with significant recent balance changes',
    FIRST_TRANSACTION_DATE DATE COMMENT 'Date of first transaction for account age calculation',
    LAST_TRANSACTION_DATE DATE COMMENT 'Date of most recent transaction',
    LAST_VALUE_DATE DATE COMMENT 'Most recent value date for settlement tracking',
    RECENT_TRANSACTIONS_30D NUMBER(10,0) COMMENT 'Number of transactions in last 30 days',
    RECENT_BALANCE_CHANGE_30D_BASE DECIMAL(18,2) COMMENT 'Net balance change in last 30 days (base currency)',
    BALANCE_CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Timestamp when balance calculation was performed'
) COMMENT = 'Real-time account balance calculation system with enhanced FX rate integration. Provides current balances for ALL customer accounts using enhanced exchange rates with analytics from REF_AGG_001.REFA_AGG_DT_FX_RATES_ENHANCED. Shows all accounts including those with zero balances. Uses direct account-to-transaction mapping (no allocation logic needed). Multi-currency conversion, balance tracking, and comprehensive financial reporting.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH all_accounts AS (
    SELECT 
        ACCOUNT_ID,
        ACCOUNT_TYPE,
        BASE_CURRENCY,
        CUSTOMER_ID,
        STATUS AS ACCOUNT_STATUS
    FROM {{ db }}.{{ crm_agg }}.ACCA_AGG_DT_ACCOUNTS
    WHERE IS_ACTIVE = TRUE
),

account_transactions AS (
    SELECT 
        acc.ACCOUNT_ID,
        acc.ACCOUNT_TYPE,
        acc.BASE_CURRENCY,
        acc.CUSTOMER_ID,
        acc.ACCOUNT_STATUS,
        t.TRANSACTION_ID,
        t.BOOKING_DATE,
        t.VALUE_DATE,
        t.AMOUNT,
        t.CURRENCY AS TRANSACTION_CURRENCY,
        t.BASE_AMOUNT,
        t.FX_RATE,
        t.COUNTERPARTY_ACCOUNT,
        t.DESCRIPTION,
        t.BASE_AMOUNT AS allocated_amount_base 

    FROM all_accounts acc
    LEFT JOIN {{ db }}.{{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS t ON acc.ACCOUNT_ID = t.ACCOUNT_ID
        AND t.BOOKING_DATE >= CURRENT_DATE - INTERVAL '450 days' 
),

transaction_base_currency AS (
    SELECT DISTINCT t.BASE_CURRENCY
    FROM account_transactions atd
    INNER JOIN {{ db }}.{{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS t ON atd.TRANSACTION_ID = t.TRANSACTION_ID
    WHERE atd.TRANSACTION_ID IS NOT NULL
    LIMIT 1
),

fx_rates_current AS (
    SELECT 
        fx.FROM_CURRENCY,
        fx.TO_CURRENCY,
        fx.MID_RATE,
        fx.DATE as fx_date,
        fx.SPREAD_PERCENTAGE,
        fx.VOLATILITY_RISK_LEVEL,
        fx.IS_CURRENT_RATE,
        tbc.BASE_CURRENCY
    FROM {{ db }}.{{ ref_agg }}.REFA_AGG_DT_FX_RATES_ENHANCED fx
    CROSS JOIN transaction_base_currency tbc
    WHERE fx.FROM_CURRENCY = tbc.BASE_CURRENCY
      AND fx.IS_CURRENT_RATE = TRUE 
),

account_balance_calculation AS (
    SELECT 
        ACCOUNT_ID,
        ACCOUNT_TYPE,
        BASE_CURRENCY,
        CUSTOMER_ID,
        ACCOUNT_STATUS,

        COUNT(CASE WHEN allocated_amount_base != 0 THEN TRANSACTION_ID END) as total_transactions,
        COUNT(CASE WHEN allocated_amount_base > 0 THEN TRANSACTION_ID END) as credit_transactions,
        COUNT(CASE WHEN allocated_amount_base < 0 THEN TRANSACTION_ID END) as debit_transactions,

        COALESCE(SUM(allocated_amount_base), 0.00) as current_balance_base,
        COALESCE(SUM(CASE WHEN allocated_amount_base > 0 THEN allocated_amount_base ELSE 0 END), 0.00) as total_credits_base,
        COALESCE(SUM(CASE WHEN allocated_amount_base < 0 THEN ABS(allocated_amount_base) ELSE 0 END), 0.00) as total_debits_base,

        COALESCE(AVG(allocated_amount_base), 0.00) as avg_transaction_amount_base,
        COALESCE(MIN(allocated_amount_base), 0.00) as min_transaction_amount_base,
        COALESCE(MAX(allocated_amount_base), 0.00) as max_transaction_amount_base,
        COALESCE(STDDEV(allocated_amount_base), 0.00) as stddev_transaction_amount_base,

        MIN(BOOKING_DATE) as first_transaction_date,
        MAX(BOOKING_DATE) as last_transaction_date,
        MAX(VALUE_DATE) as last_value_date,

        COUNT(CASE WHEN BOOKING_DATE >= CURRENT_DATE - INTERVAL '30 days' AND allocated_amount_base != 0 THEN TRANSACTION_ID END) as recent_transactions_30d,
        COALESCE(SUM(CASE WHEN BOOKING_DATE >= CURRENT_DATE - INTERVAL '30 days' THEN allocated_amount_base ELSE 0 END), 0.00) as recent_balance_change_30d_base

    FROM account_transactions
    GROUP BY ACCOUNT_ID, ACCOUNT_TYPE, BASE_CURRENCY, CUSTOMER_ID, ACCOUNT_STATUS
)

SELECT 
    abc.ACCOUNT_ID,
    abc.CUSTOMER_ID,
    abc.ACCOUNT_TYPE,
    abc.BASE_CURRENCY,
    abc.ACCOUNT_STATUS,

    ROUND(COALESCE(abc.current_balance_base, 0.00), 2) as CURRENT_BALANCE_BASE,
    ROUND(COALESCE(abc.total_credits_base, 0.00), 2) as TOTAL_CREDITS_BASE,
    ROUND(COALESCE(abc.total_debits_base, 0.00), 2) as TOTAL_DEBITS_BASE,

    ROUND(
        CASE 
            WHEN abc.BASE_CURRENCY = fx.BASE_CURRENCY THEN COALESCE(abc.current_balance_base, 0.00) 
            WHEN fx.MID_RATE IS NOT NULL THEN COALESCE(abc.current_balance_base, 0.00) * fx.MID_RATE 
            ELSE COALESCE(abc.current_balance_base, 0.00) 
        END, 2
    ) as CURRENT_BALANCE_BASE_CURRENCY,

    COALESCE(abc.total_transactions, 0) as total_transactions,
    COALESCE(abc.credit_transactions, 0) as credit_transactions,
    COALESCE(abc.debit_transactions, 0) as debit_transactions,
    ROUND(COALESCE(abc.avg_transaction_amount_base, 0.00), 2) as AVG_TRANSACTION_AMOUNT_BASE,
    ROUND(COALESCE(abc.min_transaction_amount_base, 0.00), 2) as MIN_TRANSACTION_AMOUNT_BASE,
    ROUND(COALESCE(abc.max_transaction_amount_base, 0.00), 2) as MAX_TRANSACTION_AMOUNT_BASE,

    CASE 
        WHEN abc.total_transactions = 0 THEN 'INACTIVE'
        WHEN abc.recent_transactions_30d = 0 THEN 'DORMANT'
        WHEN abc.recent_transactions_30d >= 20 THEN 'HIGH_ACTIVITY'
        WHEN abc.recent_transactions_30d >= 5 THEN 'MODERATE_ACTIVITY'
        ELSE 'LOW_ACTIVITY'
    END as ACTIVITY_LEVEL,

    CASE 
        WHEN COALESCE(abc.current_balance_base, 0.00) < 0 THEN 'OVERDRAWN'
        WHEN COALESCE(abc.current_balance_base, 0.00) = 0 THEN 'ZERO_BALANCE'
        WHEN COALESCE(abc.current_balance_base, 0.00) < 900 THEN 'LOW_BALANCE'       
        WHEN COALESCE(abc.current_balance_base, 0.00) < 9000 THEN 'MODERATE_BALANCE' 
        WHEN COALESCE(abc.current_balance_base, 0.00) < 90000 THEN 'HIGH_BALANCE'    
        ELSE 'VERY_HIGH_BALANCE'
    END as BALANCE_CATEGORY,

    CASE WHEN COALESCE(abc.current_balance_base, 0.00) < -900 THEN TRUE ELSE FALSE END as IS_OVERDRAWN, 
    CASE WHEN COALESCE(abc.recent_transactions_30d, 0) = 0 AND COALESCE(abc.total_transactions, 0) > 0 THEN TRUE ELSE FALSE END as IS_DORMANT,
    CASE WHEN ABS(COALESCE(abc.recent_balance_change_30d_base, 0.00)) > 45000 THEN TRUE ELSE FALSE END as HAS_LARGE_RECENT_MOVEMENTS, 

    abc.first_transaction_date,
    abc.last_transaction_date,
    abc.last_value_date,
    COALESCE(abc.recent_transactions_30d, 0) as recent_transactions_30d,
    ROUND(COALESCE(abc.recent_balance_change_30d_base, 0.00), 2) as RECENT_BALANCE_CHANGE_30D_BASE,

    CURRENT_TIMESTAMP() as BALANCE_CALCULATION_TIMESTAMP

FROM account_balance_calculation abc
LEFT JOIN fx_rates_current fx ON fx.TO_CURRENCY = abc.BASE_CURRENCY
ORDER BY abc.current_balance_base DESC, abc.ACCOUNT_ID;

DEFINE DYNAMIC TABLE {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_CUSTOMER_TRANSACTION_SUMMARY(
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for joining to {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360',

    TOTAL_TRANSACTIONS_12M NUMBER(10,0) COMMENT 'Count of all transactions in last 12 months for engagement scoring',
    TOTAL_TRANSACTIONS_ALL_TIME NUMBER(10,0) COMMENT 'Lifetime transaction count since customer onboarding',

    DEBIT_TRANSACTIONS NUMBER(10,0) COMMENT 'Number of debit transactions (spending activity)',
    CREDIT_TRANSACTIONS NUMBER(10,0) COMMENT 'Number of credit transactions (income deposits)',

    LAST_TRANSACTION_DATE DATE COMMENT 'Most recent transaction date for dormancy detection',
    DAYS_SINCE_LAST_TRANSACTION NUMBER(10,0) COMMENT 'Days since last activity (churn indicator)',
    FIRST_TRANSACTION_DATE DATE COMMENT 'First transaction date for customer lifecycle analysis',

    AVG_MONTHLY_TRANSACTIONS NUMBER(10,2) COMMENT 'Average transactions per month (engagement trend)',
    TRANSACTION_MONTHS_ACTIVE NUMBER(10,0) COMMENT 'Number of months with at least 1 transaction',

    IS_DORMANT_TRANSACTIONALLY BOOLEAN COMMENT 'TRUE if no transactions in 180+ days',
    IS_HIGHLY_ACTIVE BOOLEAN COMMENT 'TRUE if > 50 transactions in last month',

    SUMMARY_AS_OF_DATE TIMESTAMP_NTZ COMMENT 'Timestamp when summary was calculated'

) COMMENT = 'Pre-aggregated customer transaction metrics for efficient integration into Customer 360 and employee analytics. Provides engagement scoring, dormancy detection, and churn prediction indicators. Refreshed hourly to match {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 lag.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    acc.CUSTOMER_ID,

    COUNT(CASE WHEN txn.VALUE_DATE >= DATEADD(month, -12, CURRENT_DATE()) THEN 1 END) AS TOTAL_TRANSACTIONS_12M,
    COUNT(*) AS TOTAL_TRANSACTIONS_ALL_TIME,

    COUNT(CASE 
        WHEN txn.VALUE_DATE >= DATEADD(month, -12, CURRENT_DATE()) 
        AND txn.AMOUNT < 0 
        THEN 1 
    END) AS DEBIT_TRANSACTIONS,

    COUNT(CASE 
        WHEN txn.VALUE_DATE >= DATEADD(month, -12, CURRENT_DATE()) 
        AND txn.AMOUNT > 0 
        THEN 1 
    END) AS CREDIT_TRANSACTIONS,

    MAX(txn.VALUE_DATE) AS LAST_TRANSACTION_DATE,
    DATEDIFF(day, MAX(txn.VALUE_DATE), CURRENT_DATE()) AS DAYS_SINCE_LAST_TRANSACTION,
    MIN(txn.VALUE_DATE) AS FIRST_TRANSACTION_DATE,

    ROUND(COUNT(*) / NULLIF(DATEDIFF(month, MIN(txn.VALUE_DATE), CURRENT_DATE()), 0), 2) AS AVG_MONTHLY_TRANSACTIONS,
    COUNT(DISTINCT DATE_TRUNC('month', txn.VALUE_DATE)) AS TRANSACTION_MONTHS_ACTIVE,

    CASE 
        WHEN DATEDIFF(day, MAX(txn.VALUE_DATE), CURRENT_DATE()) >= 180 THEN TRUE 
        ELSE FALSE 
    END AS IS_DORMANT_TRANSACTIONALLY,

    CASE 
        WHEN COUNT(CASE WHEN txn.VALUE_DATE >= DATEADD(month, -1, CURRENT_DATE()) THEN 1 END) > 50 THEN TRUE 
        ELSE FALSE 
    END AS IS_HIGHLY_ACTIVE,

    CURRENT_TIMESTAMP() AS SUMMARY_AS_OF_DATE

FROM {{ db }}.{{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS txn
INNER JOIN {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS acc
    ON txn.ACCOUNT_ID = acc.ACCOUNT_ID
GROUP BY acc.CUSTOMER_ID;
