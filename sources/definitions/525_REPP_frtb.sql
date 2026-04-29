/*
 * 525_REPP_frtb.sql
 * FRTB reporting: market risk and capital requirements
 */
DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_FRTB_RISK_POSITIONS(
    RISK_CLASS VARCHAR(20) COMMENT 'EQUITY, FX, INTEREST_RATE, COMMODITY, CREDIT_SPREAD',
    RISK_BUCKET VARCHAR(30) COMMENT 'Risk bucket within risk class',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier',
    ACCOUNT_ID VARCHAR(30) COMMENT 'Investment account',
    INSTRUMENT_TYPE VARCHAR(30) COMMENT 'Type of instrument',
    INSTRUMENT_NAME VARCHAR(50) COMMENT 'Instrument name/identifier',
    CURRENCY VARCHAR(3) COMMENT 'Trading currency',
    POSITION_VALUE_CHF DECIMAL(28,2) COMMENT 'Position value in CHF',
    DELTA_CHF DECIMAL(28,2) COMMENT 'Delta sensitivity in CHF',
    VEGA_CHF DECIMAL(28,2) COMMENT 'Vega sensitivity in CHF (if applicable)',
    LIQUIDITY_SCORE DECIMAL(3,1) COMMENT 'Liquidity score (1-10)',
    IS_NMRF BOOLEAN COMMENT 'Non-Modellable Risk Factor flag',
    LAST_UPDATED TIMESTAMP_NTZ COMMENT 'Timestamp when calculated'
) COMMENT = 'Consolidated Trading Book Risk Exposure: To aggregate all open positions from the trading book—across equities, fixed income, commodities, and FX—into a unified view, classified by FRTB Risk Class.
FRTB (Fundamental Review of the Trading Book): Provides the granular position data required as the input for calculating regulatory capital under the Standardized Approach (SA). It consolidates risk for reporting and limit monitoring.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    'EQUITY' AS RISK_CLASS,
    'EQUITY_LARGE_CAP' AS RISK_BUCKET,
    p.CUSTOMER_ID,
    p.ACCOUNT_ID,
    'EQUITY' AS INSTRUMENT_TYPE,
    p.SYMBOL AS INSTRUMENT_NAME,
    'CHF' AS CURRENCY,
    p.NET_INVESTMENT_CHF AS POSITION_VALUE_CHF,
    p.NET_INVESTMENT_CHF AS DELTA_CHF,  -- Simplified: equity delta = position value
    NULL AS VEGA_CHF,
    8.0 AS LIQUIDITY_SCORE,  -- Equities generally liquid
    FALSE AS IS_NMRF,
    CURRENT_TIMESTAMP() AS LAST_UPDATED
FROM {{ db }}.{{ eqt_agg }}.EQTA_AGG_DT_PORTFOLIO_POSITIONS p
WHERE p.POSITION_STATUS != 'CLOSED'

UNION ALL

SELECT
    'INTEREST_RATE' AS RISK_CLASS,
    CASE
        WHEN p.ISSUER_TYPE = 'SOVEREIGN' THEN 'IR_SOVEREIGN'
        WHEN p.ISSUER_TYPE = 'CORPORATE' THEN 'IR_CORPORATE'
        ELSE 'IR_OTHER'
    END AS RISK_BUCKET,
    p.CUSTOMER_ID,
    p.ACCOUNT_ID,
    p.INSTRUMENT_TYPE,
    p.ISSUER AS INSTRUMENT_NAME,
    p.CURRENCY,
    p.TOTAL_INVESTMENT_CHF AS POSITION_VALUE_CHF,
    p.TOTAL_DV01_CHF AS DELTA_CHF,  -- DV01 as delta for interest rate risk
    NULL AS VEGA_CHF,
    CASE
        WHEN p.ISSUER_TYPE = 'SOVEREIGN' THEN 9.0
        WHEN p.CREDIT_RATING IN ('AAA', 'AA') THEN 7.0
        ELSE 5.0
    END AS LIQUIDITY_SCORE,
    CASE
        WHEN p.ISSUER_TYPE = 'CORPORATE' AND p.CREDIT_RATING NOT IN ('AAA', 'AA', 'A') THEN TRUE
        ELSE FALSE
    END AS IS_NMRF,
    CURRENT_TIMESTAMP() AS LAST_UPDATED
FROM {{ db }}.{{ fii_agg }}.FIIA_AGG_DT_PORTFOLIO_POSITIONS p
WHERE p.POSITION_STATUS != 'CLOSED'

UNION ALL

SELECT
    'CREDIT_SPREAD' AS RISK_CLASS,
    CASE
        WHEN p.CREDIT_RATING IN ('AAA', 'AA') THEN 'CS_IG_HIGH'
        WHEN p.CREDIT_RATING IN ('A', 'BBB') THEN 'CS_IG_LOW'
        ELSE 'CS_HY'
    END AS RISK_BUCKET,
    p.CUSTOMER_ID,
    p.ACCOUNT_ID,
    p.INSTRUMENT_TYPE,
    p.ISSUER AS INSTRUMENT_NAME,
    p.CURRENCY,
    p.TOTAL_INVESTMENT_CHF AS POSITION_VALUE_CHF,
    p.TOTAL_INVESTMENT_CHF * 0.01 AS DELTA_CHF,  -- Simplified: 1% credit spread sensitivity
    NULL AS VEGA_CHF,
    CASE
        WHEN p.CREDIT_RATING IN ('AAA', 'AA', 'A') THEN 7.0
        ELSE 4.0
    END AS LIQUIDITY_SCORE,
    CASE
        WHEN p.CREDIT_RATING NOT IN ('AAA', 'AA', 'A', 'BBB') THEN TRUE
        ELSE FALSE
    END AS IS_NMRF,
    CURRENT_TIMESTAMP() AS LAST_UPDATED
FROM {{ db }}.{{ fii_agg }}.FIIA_AGG_DT_PORTFOLIO_POSITIONS p
WHERE p.POSITION_STATUS != 'CLOSED'
  AND p.INSTRUMENT_TYPE = 'BOND'
  AND p.ISSUER_TYPE = 'CORPORATE'

UNION ALL

SELECT
    'COMMODITY' AS RISK_CLASS,
    CASE
        WHEN p.COMMODITY_TYPE = 'ENERGY' THEN 'COMM_ENERGY'
        WHEN p.COMMODITY_TYPE = 'PRECIOUS_METAL' THEN 'COMM_PRECIOUS'
        WHEN p.COMMODITY_TYPE = 'BASE_METAL' THEN 'COMM_BASE'
        WHEN p.COMMODITY_TYPE = 'AGRICULTURAL' THEN 'COMM_AGRI'
        ELSE 'COMM_OTHER'
    END AS RISK_BUCKET,
    p.CUSTOMER_ID,
    p.ACCOUNT_ID,
    p.COMMODITY_TYPE AS INSTRUMENT_TYPE,
    p.COMMODITY_NAME AS INSTRUMENT_NAME,
    'CHF' AS CURRENCY,
    p.TOTAL_INVESTMENT_CHF AS POSITION_VALUE_CHF,
    p.TOTAL_DELTA_CHF AS DELTA_CHF,
    NULL AS VEGA_CHF,
    CASE
        WHEN p.COMMODITY_TYPE IN ('ENERGY', 'PRECIOUS_METAL') THEN 7.0
        WHEN p.COMMODITY_TYPE = 'BASE_METAL' THEN 6.0
        ELSE 4.0
    END AS LIQUIDITY_SCORE,
    CASE
        WHEN p.COMMODITY_TYPE = 'AGRICULTURAL' THEN TRUE
        ELSE FALSE
    END AS IS_NMRF,
    CURRENT_TIMESTAMP() AS LAST_UPDATED
FROM {{ db }}.{{ cmd_agg }}.CMDA_AGG_DT_PORTFOLIO_POSITIONS p
WHERE p.POSITION_STATUS != 'CLOSED'

ORDER BY RISK_CLASS, RISK_BUCKET, POSITION_VALUE_CHF DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_FRTB_SENSITIVITIES(
    RISK_CLASS VARCHAR(20) COMMENT 'EQUITY, FX, INTEREST_RATE, COMMODITY, CREDIT_SPREAD',
    RISK_BUCKET VARCHAR(30) COMMENT 'Risk bucket within risk class',
    TOTAL_POSITIONS NUMBER(10,0) COMMENT 'Number of positions',
    LONG_POSITIONS NUMBER(10,0) COMMENT 'Number of long positions',
    SHORT_POSITIONS NUMBER(10,0) COMMENT 'Number of short positions',
    GROSS_DELTA_CHF DECIMAL(28,2) COMMENT 'Gross delta (sum of absolute values)',
    NET_DELTA_CHF DECIMAL(28,2) COMMENT 'Net delta (long - short)',
    GROSS_VEGA_CHF DECIMAL(28,2) COMMENT 'Gross vega (sum of absolute values)',
    NET_VEGA_CHF DECIMAL(28,2) COMMENT 'Net vega (long - short)',
    LARGEST_POSITION_CHF DECIMAL(28,2) COMMENT 'Largest single position value',
    NMRF_POSITIONS NUMBER(10,0) COMMENT 'Number of NMRF positions',
    LAST_UPDATED TIMESTAMP_NTZ COMMENT 'Timestamp when calculated'
) COMMENT = 'Risk Aggregation and Net Exposure Measurement: To calculate and aggregate the Δ (Delta), ν (Vega), and Curvature sensitivities by risk class and bucket. This summarizes the banks exposure to small movements in underlying risk factors.
Market Risk Management: Essential for internal risk control, quantifying the exposure of the trading book to interest rate changes (Δ for bonds), volatility (ν for options/swaps), and non-linear risk (Curvature).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    RISK_CLASS,
    RISK_BUCKET,
    COUNT(*) AS TOTAL_POSITIONS,
    COUNT(CASE WHEN DELTA_CHF > 0 THEN 1 END) AS LONG_POSITIONS,
    COUNT(CASE WHEN DELTA_CHF < 0 THEN 1 END) AS SHORT_POSITIONS,
    SUM(ABS(DELTA_CHF)) AS GROSS_DELTA_CHF,
    SUM(DELTA_CHF) AS NET_DELTA_CHF,
    SUM(ABS(COALESCE(VEGA_CHF, 0))) AS GROSS_VEGA_CHF,
    SUM(COALESCE(VEGA_CHF, 0)) AS NET_VEGA_CHF,
    MAX(ABS(POSITION_VALUE_CHF)) AS LARGEST_POSITION_CHF,
    COUNT(CASE WHEN IS_NMRF THEN 1 END) AS NMRF_POSITIONS,
    CURRENT_TIMESTAMP() AS LAST_UPDATED
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_FRTB_RISK_POSITIONS
GROUP BY RISK_CLASS, RISK_BUCKET
ORDER BY RISK_CLASS, RISK_BUCKET;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_FRTB_CAPITAL_CHARGES(
    RISK_CLASS VARCHAR(20) COMMENT 'EQUITY, FX, INTEREST_RATE, COMMODITY, CREDIT_SPREAD',
    RISK_BUCKET VARCHAR(30) COMMENT 'Risk bucket within risk class',
    GROSS_DELTA_CHF DECIMAL(28,2) COMMENT 'Gross delta sensitivity',
    NET_DELTA_CHF DECIMAL(28,2) COMMENT 'Net delta sensitivity',
    RISK_WEIGHT DECIMAL(8,2) COMMENT 'FRTB risk weight (%)',
    DELTA_CAPITAL_CHARGE_CHF DECIMAL(28,2) COMMENT 'Delta capital charge',
    VEGA_CAPITAL_CHARGE_CHF DECIMAL(28,2) COMMENT 'Vega capital charge',
    CURVATURE_CAPITAL_CHARGE_CHF DECIMAL(28,2) COMMENT 'Curvature capital charge',
    NMRF_ADD_ON_CHF DECIMAL(28,2) COMMENT 'NMRF capital add-on',
    TOTAL_CAPITAL_CHARGE_CHF DECIMAL(28,2) COMMENT 'Total capital charge for bucket',
    LAST_UPDATED TIMESTAMP_NTZ COMMENT 'Timestamp when calculated'
) COMMENT = 'FRTB Regulatory Capital Calculation (Standardized Approach): To compute the actual capital requirement for each risk class and bucket by applying mandated FRTB Risk Weights to the calculated sensitivities.
Basel III/IV Regulatory Compliance: The core output for regulatory reporting. It determines the Total Capital Charge (SA-based RWA), including Delta, Vega, Curvature, and the NMRF Add-On, which must be covered by the banks capital.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    s.RISK_CLASS,
    s.RISK_BUCKET,
    s.GROSS_DELTA_CHF,
    s.NET_DELTA_CHF,

    CASE s.RISK_CLASS
        WHEN 'EQUITY' THEN 25.0
        WHEN 'FX' THEN 15.0
        WHEN 'INTEREST_RATE' THEN
            CASE
                WHEN s.RISK_BUCKET = 'IR_SOVEREIGN' THEN 1.5
                ELSE 3.0
            END
        WHEN 'COMMODITY' THEN
            CASE
                WHEN s.RISK_BUCKET = 'COMM_ENERGY' THEN 30.0
                WHEN s.RISK_BUCKET = 'COMM_PRECIOUS' THEN 20.0
                WHEN s.RISK_BUCKET = 'COMM_BASE' THEN 25.0
                ELSE 35.0  -- Agricultural
            END
        WHEN 'CREDIT_SPREAD' THEN
            CASE
                WHEN s.RISK_BUCKET = 'CS_IG_HIGH' THEN 2.0
                WHEN s.RISK_BUCKET = 'CS_IG_LOW' THEN 3.5
                ELSE 6.0  -- High yield
            END
        ELSE 10.0
    END AS RISK_WEIGHT,

    ROUND(
        s.GROSS_DELTA_CHF *
        CASE s.RISK_CLASS
            WHEN 'EQUITY' THEN 0.25
            WHEN 'FX' THEN 0.15
            WHEN 'INTEREST_RATE' THEN
                CASE WHEN s.RISK_BUCKET = 'IR_SOVEREIGN' THEN 0.015 ELSE 0.03 END
            WHEN 'COMMODITY' THEN
                CASE
                    WHEN s.RISK_BUCKET = 'COMM_ENERGY' THEN 0.30
                    WHEN s.RISK_BUCKET = 'COMM_PRECIOUS' THEN 0.20
                    WHEN s.RISK_BUCKET = 'COMM_BASE' THEN 0.25
                    ELSE 0.35
                END
            WHEN 'CREDIT_SPREAD' THEN
                CASE
                    WHEN s.RISK_BUCKET = 'CS_IG_HIGH' THEN 0.02
                    WHEN s.RISK_BUCKET = 'CS_IG_LOW' THEN 0.035
                    ELSE 0.06
                END
            ELSE 0.10
        END, 2
    ) AS DELTA_CAPITAL_CHARGE_CHF,

    ROUND(s.GROSS_VEGA_CHF * 0.10, 2) AS VEGA_CAPITAL_CHARGE_CHF,

    ROUND(s.GROSS_DELTA_CHF * 0.05, 2) AS CURVATURE_CAPITAL_CHARGE_CHF,

    ROUND(
        CASE WHEN s.NMRF_POSITIONS > 0 THEN s.GROSS_DELTA_CHF * 0.50 ELSE 0 END, 2
    ) AS NMRF_ADD_ON_CHF,

    ROUND(
        (s.GROSS_DELTA_CHF *
         CASE s.RISK_CLASS
             WHEN 'EQUITY' THEN 0.25
             WHEN 'FX' THEN 0.15
             WHEN 'INTEREST_RATE' THEN
                 CASE WHEN s.RISK_BUCKET = 'IR_SOVEREIGN' THEN 0.015 ELSE 0.03 END
             WHEN 'COMMODITY' THEN
                 CASE
                     WHEN s.RISK_BUCKET = 'COMM_ENERGY' THEN 0.30
                     WHEN s.RISK_BUCKET = 'COMM_PRECIOUS' THEN 0.20
                     WHEN s.RISK_BUCKET = 'COMM_BASE' THEN 0.25
                     ELSE 0.35
                 END
             WHEN 'CREDIT_SPREAD' THEN
                 CASE
                     WHEN s.RISK_BUCKET = 'CS_IG_HIGH' THEN 0.02
                     WHEN s.RISK_BUCKET = 'CS_IG_LOW' THEN 0.035
                     ELSE 0.06
                 END
             ELSE 0.10
         END) +
        (s.GROSS_VEGA_CHF * 0.10) +
        (s.GROSS_DELTA_CHF * 0.05) +
        (CASE WHEN s.NMRF_POSITIONS > 0 THEN s.GROSS_DELTA_CHF * 0.50 ELSE 0 END), 2
    ) AS TOTAL_CAPITAL_CHARGE_CHF,

    CURRENT_TIMESTAMP() AS LAST_UPDATED

FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_FRTB_SENSITIVITIES s
ORDER BY TOTAL_CAPITAL_CHARGE_CHF DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_FRTB_NMRF_ANALYSIS(
    RISK_CLASS VARCHAR(20) COMMENT 'EQUITY, FX, INTEREST_RATE, COMMODITY, CREDIT_SPREAD',
    RISK_BUCKET VARCHAR(30) COMMENT 'Risk bucket within risk class',
    INSTRUMENT_NAME VARCHAR(50) COMMENT 'Instrument name/identifier',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier',
    POSITION_VALUE_CHF DECIMAL(28,2) COMMENT 'Position value in CHF',
    DELTA_CHF DECIMAL(28,2) COMMENT 'Delta sensitivity',
    LIQUIDITY_SCORE DECIMAL(3,1) COMMENT 'Liquidity score (1-10)',
    NMRF_REASON VARCHAR(50) COMMENT 'Reason for NMRF classification',
    CAPITAL_ADD_ON_CHF DECIMAL(28,2) COMMENT 'Additional capital requirement',
    LAST_UPDATED TIMESTAMP_NTZ COMMENT 'Timestamp when calculated'
) COMMENT = 'Non-Modellable Risk Factor (NMRF) Identification and Add-On: To explicitly identify and quantify capital add-ons for illiquid or complex trading positions (e.g., high-yield credit, agricultural commodities) where market data is insufficient for internal modeling.
Market Risk / Regulatory Compliance: Directly addresses the FRTB requirement for liquidity-based capital charges. It isolates and calculates the capital add-on based on the position value and liquidity score, ensuring sufficient capital for difficult-to-hedge risks.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    RISK_CLASS,
    RISK_BUCKET,
    INSTRUMENT_NAME,
    CUSTOMER_ID,
    POSITION_VALUE_CHF,
    DELTA_CHF,
    LIQUIDITY_SCORE,

    CASE
        WHEN LIQUIDITY_SCORE < 3 THEN 'HIGHLY_ILLIQUID'
        WHEN LIQUIDITY_SCORE < 5 THEN 'ILLIQUID'
        WHEN RISK_CLASS = 'CREDIT_SPREAD' AND RISK_BUCKET = 'CS_HY' THEN 'HIGH_YIELD_CREDIT'
        WHEN RISK_CLASS = 'COMMODITY' AND RISK_BUCKET = 'COMM_AGRI' THEN 'AGRICULTURAL_COMMODITY'
        ELSE 'OTHER'
    END AS NMRF_REASON,

    ROUND(ABS(POSITION_VALUE_CHF) * 0.50, 2) AS CAPITAL_ADD_ON_CHF,

    CURRENT_TIMESTAMP() AS LAST_UPDATED

FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_FRTB_RISK_POSITIONS
WHERE IS_NMRF = TRUE
ORDER BY CAPITAL_ADD_ON_CHF DESC;
