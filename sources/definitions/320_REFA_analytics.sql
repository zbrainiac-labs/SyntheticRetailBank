DEFINE DYNAMIC TABLE {{ db }}.{{ ref_agg }}.REFA_AGG_DT_FX_RATES_ENHANCED(
    DATE DATE COMMENT 'Date of the FX rate observation for time series analysis',
    FROM_CURRENCY VARCHAR(3) COMMENT 'Source currency code (ISO 4217) for currency conversion',
    TO_CURRENCY VARCHAR(3) COMMENT 'Target currency code (ISO 4217) for currency conversion',
    CURRENCY_PAIR VARCHAR(7) COMMENT 'Currency pair identifier (FROM/TO format) for market analysis',
    MID_RATE NUMBER(15,6) COMMENT 'Mid-market exchange rate (average of bid and ask)',
    BID_RATE NUMBER(15,6) COMMENT 'Bid exchange rate (rate at which bank buys the base currency)',
    ASK_RATE NUMBER(15,6) COMMENT 'Ask exchange rate (rate at which bank sells the base currency)',
    SPREAD_ABSOLUTE NUMBER(15,6) COMMENT 'Absolute spread between bid and ask rates',
    SPREAD_PERCENTAGE NUMBER(8,4) COMMENT 'Percentage spread relative to mid rate for cost analysis',
    DAILY_CHANGE_ABSOLUTE NUMBER(15,6) COMMENT 'Absolute change in mid rate from previous day',
    DAILY_CHANGE_PERCENTAGE NUMBER(8,4) COMMENT 'Percentage change in mid rate from previous day',
    TREND_DIRECTION VARCHAR(15) COMMENT 'Daily trend classification (APPRECIATING/DEPRECIATING/STABLE/NO_PREV_DATA)',
    VOLATILITY_30D NUMBER(15,6) COMMENT '30-day rolling standard deviation of mid rates for risk assessment',
    MOVING_AVG_7D NUMBER(15,6) COMMENT '7-day rolling average of mid rates for trend analysis',
    MIN_RATE_30D NUMBER(15,6) COMMENT 'Minimum mid rate in last 30 days for range analysis',
    MAX_RATE_30D NUMBER(15,6) COMMENT 'Maximum mid rate in last 30 days for range analysis',
    RATE_POSITION_PERCENTAGE NUMBER(5,2) COMMENT 'Current rate position within 30-day range (0%=low, 100%=high)',
    CURRENCY_PAIR_TYPE VARCHAR(15) COMMENT 'Currency pair classification (CHF_BASE/CHF_TARGET/CROSS_CURRENCY)',
    PAIR_CLASSIFICATION VARCHAR(15) COMMENT 'Market classification (MAJOR_PAIR/MINOR_PAIR)',
    SPREAD_RISK_LEVEL VARCHAR(20) COMMENT 'Spread-based risk classification (LOW/MEDIUM/HIGH/UNKNOWN_SPREAD)',
    VOLATILITY_RISK_LEVEL VARCHAR(20) COMMENT 'Volatility-based risk classification (LOW/MEDIUM/HIGH/UNKNOWN_VOLATILITY)',
    IS_CURRENT_RATE BOOLEAN COMMENT 'Boolean flag indicating if this is the most current rate available',
    CREATED_AT TIMESTAMP_NTZ COMMENT 'Original timestamp when rate was created in source system',
    AGGREGATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Timestamp when aggregation processing was performed',
    AGGREGATION_TYPE VARCHAR(25) COMMENT 'Type of aggregation processing applied (ENHANCED_FX_ANALYTICS)'
) COMMENT = 'Enhanced FX rates aggregation with analytics, volatility metrics, and business intelligence. Provides current rates, historical trends, bid/ask spreads, and currency pair analytics for real-time operations, risk management, and regulatory reporting.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH fx_rates_base AS (
    SELECT 
        DATE,
        FROM_CURRENCY,
        TO_CURRENCY,
        MID_RATE,
        BID_RATE,
        ASK_RATE,

        (ASK_RATE - BID_RATE) AS SPREAD_ABSOLUTE,
        CASE 
            WHEN MID_RATE > 0 THEN ROUND(((ASK_RATE - BID_RATE) / MID_RATE) * 100, 4)
            ELSE NULL
        END AS SPREAD_PERCENTAGE,

        CASE 
            WHEN FROM_CURRENCY = 'CHF' THEN 'CHF_BASE'
            WHEN TO_CURRENCY = 'CHF' THEN 'CHF_TARGET'
            ELSE 'CROSS_CURRENCY'
        END AS CURRENCY_PAIR_TYPE,

        CASE 
            WHEN (FROM_CURRENCY IN ('CHF', 'USD', 'EUR', 'GBP') AND TO_CURRENCY IN ('CHF', 'USD', 'EUR', 'GBP'))
            THEN 'MAJOR_PAIR'
            ELSE 'MINOR_PAIR'
        END AS PAIR_CLASSIFICATION,

        LAG(MID_RATE) OVER (PARTITION BY FROM_CURRENCY, TO_CURRENCY ORDER BY DATE) AS PREV_MID_RATE,

        CREATED_AT

    FROM {{ db }}.{{ ref_raw }}.REFI_RAW_TB_FX_RATES
),

fx_rates_with_trends AS (
    SELECT 
        *,

        CASE 
            WHEN PREV_MID_RATE IS NOT NULL AND PREV_MID_RATE > 0
            THEN ROUND(((MID_RATE - PREV_MID_RATE) / PREV_MID_RATE) * 100, 4)
            ELSE NULL
        END AS DAILY_CHANGE_PERCENTAGE,

        CASE 
            WHEN PREV_MID_RATE IS NOT NULL 
            THEN (MID_RATE - PREV_MID_RATE)
            ELSE NULL
        END AS DAILY_CHANGE_ABSOLUTE,

        CASE 
            WHEN PREV_MID_RATE IS NULL THEN 'NO_PREV_DATA'
            WHEN MID_RATE > PREV_MID_RATE THEN 'APPRECIATING'
            WHEN MID_RATE < PREV_MID_RATE THEN 'DEPRECIATING'
            ELSE 'STABLE'
        END AS TREND_DIRECTION,

        STDDEV(MID_RATE) OVER (
            PARTITION BY FROM_CURRENCY, TO_CURRENCY 
            ORDER BY DATE 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS VOLATILITY_30D,

        AVG(MID_RATE) OVER (
            PARTITION BY FROM_CURRENCY, TO_CURRENCY 
            ORDER BY DATE 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS MOVING_AVG_7D,

        MIN(MID_RATE) OVER (
            PARTITION BY FROM_CURRENCY, TO_CURRENCY 
            ORDER BY DATE 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS MIN_RATE_30D,

        MAX(MID_RATE) OVER (
            PARTITION BY FROM_CURRENCY, TO_CURRENCY 
            ORDER BY DATE 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS MAX_RATE_30D

    FROM fx_rates_base
)

SELECT 
    DATE,
    FROM_CURRENCY,
    TO_CURRENCY,
    CONCAT(FROM_CURRENCY, '/', TO_CURRENCY) AS CURRENCY_PAIR,
    MID_RATE,
    BID_RATE,
    ASK_RATE,

    SPREAD_ABSOLUTE,
    SPREAD_PERCENTAGE,

    DAILY_CHANGE_ABSOLUTE,
    DAILY_CHANGE_PERCENTAGE,
    TREND_DIRECTION,

    ROUND(VOLATILITY_30D, 6) AS VOLATILITY_30D,
    ROUND(MOVING_AVG_7D, 6) AS MOVING_AVG_7D,
    MIN_RATE_30D,
    MAX_RATE_30D,

    CASE 
        WHEN MIN_RATE_30D IS NOT NULL AND MAX_RATE_30D IS NOT NULL AND MAX_RATE_30D > MIN_RATE_30D
        THEN ROUND(((MID_RATE - MIN_RATE_30D) / (MAX_RATE_30D - MIN_RATE_30D)) * 100, 2)
        ELSE NULL
    END AS RATE_POSITION_PERCENTAGE,

    CURRENCY_PAIR_TYPE,
    PAIR_CLASSIFICATION,

    CASE 
        WHEN SPREAD_PERCENTAGE IS NULL THEN 'UNKNOWN_SPREAD'
        WHEN SPREAD_PERCENTAGE > 1.0 THEN 'HIGH_SPREAD'
        WHEN SPREAD_PERCENTAGE > 0.5 THEN 'MEDIUM_SPREAD'
        ELSE 'LOW_SPREAD'
    END AS SPREAD_RISK_LEVEL,

    CASE 
        WHEN VOLATILITY_30D IS NULL THEN 'UNKNOWN_VOLATILITY'
        WHEN VOLATILITY_30D > 0.05 THEN 'HIGH_VOLATILITY'
        WHEN VOLATILITY_30D > 0.02 THEN 'MEDIUM_VOLATILITY'
        ELSE 'LOW_VOLATILITY'
    END AS VOLATILITY_RISK_LEVEL,

    CASE 
        WHEN DATE = CURRENT_DATE() THEN TRUE
        WHEN DATE = (SELECT MAX(DATE) FROM {{ db }}.{{ ref_raw }}.REFI_RAW_TB_FX_RATES) THEN TRUE
        ELSE FALSE
    END AS IS_CURRENT_RATE,

    CREATED_AT,
    CURRENT_TIMESTAMP() AS AGGREGATION_TIMESTAMP,
    'ENHANCED_FX_ANALYTICS' AS AGGREGATION_TYPE

FROM fx_rates_with_trends

ORDER BY FROM_CURRENCY, TO_CURRENCY, DATE DESC;
