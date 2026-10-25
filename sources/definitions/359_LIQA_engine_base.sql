/*
 * 359_LIQA_engine_base.sql
 * LCR engine - part 1: shared LCR formula, daily FX, unified positions, DQ exceptions
 * (REQ-ING-03, REQ-MAP-01, REQ-DQ-01, REQ-ENG-04)
 */
DEFINE FUNCTION {{ db }}.{{ rep_agg }}.REPP_AGG_FN_LCR_CALC(
    L1 FLOAT, L2A FLOAT, L2B FLOAT, GROSS_OUTFLOW FLOAT, GROSS_INFLOW FLOAT, INFLOW_CAP_PCT FLOAT
)
RETURNS OBJECT
IMMUTABLE
COMMENT = 'Single shared FINMA/Basel III LCR formula used by the dynamic tables, the scenario simulator and the agent what-if tool. Inputs are post-haircut L1/L2A/L2B values and gross 30-day outflows/inflows in CHF. Applies the L2B cap (15/85 of L1+L2A), the L2 cap (2/3 of L1) and the inflow cap (INFLOW_CAP_PCT of outflows, regulatory 0.75). Returns OBJECT with L2B_CAPPED, L2_CAPPED, HQLA, INFLOW_CAPPED, NET_OUTFLOW, LCR (percent) and cap flags.'
AS
$$
    OBJECT_CONSTRUCT(
        'L2B_CAPPED', LEAST(L2B, 15.0 / 85.0 * (L1 + L2A)),
        'L2_CAPPED', LEAST(L2A + LEAST(L2B, 15.0 / 85.0 * (L1 + L2A)), 2.0 / 3.0 * L1),
        'HQLA', L1 + LEAST(L2A + LEAST(L2B, 15.0 / 85.0 * (L1 + L2A)), 2.0 / 3.0 * L1),
        'INFLOW_CAPPED', LEAST(GROSS_INFLOW, INFLOW_CAP_PCT * GROSS_OUTFLOW),
        'NET_OUTFLOW', GROSS_OUTFLOW - LEAST(GROSS_INFLOW, INFLOW_CAP_PCT * GROSS_OUTFLOW),
        'LCR', IFF(GROSS_OUTFLOW - LEAST(GROSS_INFLOW, INFLOW_CAP_PCT * GROSS_OUTFLOW) > 0,
                   (L1 + LEAST(L2A + LEAST(L2B, 15.0 / 85.0 * (L1 + L2A)), 2.0 / 3.0 * L1))
                   / (GROSS_OUTFLOW - LEAST(GROSS_INFLOW, INFLOW_CAP_PCT * GROSS_OUTFLOW)) * 100, NULL),
        'L2B_CAP_APPLIED', L2B > 15.0 / 85.0 * (L1 + L2A),
        'L2_CAP_APPLIED', L2A + LEAST(L2B, 15.0 / 85.0 * (L1 + L2A)) > 2.0 / 3.0 * L1,
        'INFLOW_CAP_APPLIED', GROSS_INFLOW > INFLOW_CAP_PCT * GROSS_OUTFLOW
    )
$$;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_FX_DAILY(
    AS_OF_DATE DATE COMMENT 'Calendar date of the end-of-day FX rate (one row per calendar day incl. weekends, which carry the last business-day close). Joined by equality to the AS_OF_DATE of LCR snapshots.',
    CURRENCY VARCHAR(3) COMMENT 'ISO 4217 currency converted to CHF (CHF itself has rate 1).',
    RATE_TO_CHF NUMBER(18,8) COMMENT 'End-of-day conversion rate: 1 unit of CURRENCY = RATE_TO_CHF CHF. Cross rate via USD: mid(USD->CHF) / mid(USD->CURRENCY).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh, used for lineage and freshness monitoring.'
) COMMENT = 'Daily end-of-day CCY->CHF conversion rates derived from REFI_RAW_TB_FX_RATES (USD-based quotes crossed via USD). One row per calendar date and currency so the LCR engine converts all positions with a plain equality join (REQ-ING-03). Currencies without a rate on a date are absent and surface as MISSING_FX exceptions.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH usd AS (
    SELECT DATE, TO_CURRENCY AS CURRENCY, MID_RATE
    FROM {{ db }}.{{ ref_raw }}.REFI_RAW_TB_FX_RATES
    WHERE FROM_CURRENCY = 'USD'
),
usd_chf AS (
    SELECT DATE, MID_RATE AS USD_CHF FROM usd WHERE CURRENCY = 'CHF'
)
SELECT c.DATE AS AS_OF_DATE, 'CHF'::VARCHAR(3) AS CURRENCY, 1::NUMBER(18,8) AS RATE_TO_CHF, CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM usd_chf c
UNION ALL
SELECT c.DATE, 'USD', c.USD_CHF::NUMBER(18,8), CURRENT_TIMESTAMP()
FROM usd_chf c
UNION ALL
SELECT u.DATE, u.CURRENCY, (c.USD_CHF / NULLIF(u.MID_RATE, 0))::NUMBER(18,8), CURRENT_TIMESTAMP()
FROM usd u
JOIN usd_chf c ON c.DATE = u.DATE
WHERE u.CURRENCY <> 'CHF';

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_POSITIONS(
    AS_OF_DATE DATE COMMENT 'Snapshot (cut-off) date of the position (daily COB).',
    POSITION_ID VARCHAR(50) COMMENT 'Atomic position key: HOLDING_ID, deposit ACCOUNT_ID or CONTRACT_ID. Lowest level of the drill-down and driver key in variance attribution.',
    SOURCE_SYSTEM VARCHAR(20) COMMENT 'Source feed of the position: HOLDINGS, DEPOSITS or CONTRACTS.',
    PRODUCT_TYPE VARCHAR(50) COMMENT 'Source product code (asset type, deposit type or contract product type).',
    POSITION_NAME VARCHAR(200) COMMENT 'Display name of the position (security name, account id or contract id).',
    COUNTERPARTY_ID VARCHAR(50) COMMENT 'Counterparty / issuer of the position (FK to LIQI_RAW_TB_COUNTERPARTIES).',
    COUNTERPARTY_NAME VARCHAR(200) COMMENT 'Counterparty name from the counterparty master (falls back to the id).',
    COUNTERPARTY_SECTOR VARCHAR(30) COMMENT 'Rule sector used to look up behavioral rules: deposit COUNTERPARTY_TYPE, contract COUNTERPARTY_SECTOR, or ALL for HQLA.',
    BUSINESS_UNIT VARCHAR(30) COMMENT 'Owning business unit (RETAIL, PRIVATE_BANKING, CORPORATE, TREASURY) for filtering (REQ-UI-02).',
    CURRENCY VARCHAR(3) COMMENT 'Original currency of the position (ISO 4217).',
    RATING VARCHAR(10) COMMENT 'Rating of the instrument or counterparty (S&P scale) for rating filters.',
    ASSET_CLASS VARCHAR(40) COMMENT 'Level-1 LCR taxonomy from LIQI_RAW_TB_PRODUCT_MAPPING (e.g. HQLA_L2A, RETAIL_DEPOSITS, LOANS). NULL when unmapped.',
    PRODUCT_GROUP VARCHAR(40) COMMENT 'Level-2 taxonomy (product group) from the product mapping.',
    LCR_TREATMENT VARCHAR(20) COMMENT 'How the position enters the LCR: HQLA, OUTFLOW, INFLOW or FACILITY.',
    HQLA_LEVEL VARCHAR(5) COMMENT 'HQLA level L1 / L2A / L2B for HQLA positions (from LIQI_RAW_TB_HQLA_ELIGIBILITY).',
    AMOUNT_CCY NUMBER(18,2) COMMENT 'Exposure in original currency: market value (holdings), balance (deposits) or drawn outstanding (contracts).',
    FX_RATE_TO_CHF NUMBER(18,8) COMMENT 'End-of-day CCY->CHF rate of AS_OF_DATE (equality join on REPP_AGG_DT_LCR_FX_DAILY). NULL means missing FX.',
    AMOUNT_CHF NUMBER(18,2) COMMENT 'Exposure converted to CHF (AMOUNT_CCY x FX_RATE_TO_CHF).',
    UNDRAWN_CHF NUMBER(18,2) COMMENT 'Undrawn committed facility amount in CHF (contracts only, basis for drawdown outflows).',
    START_DATE DATE COMMENT 'Contract start / last roll-over date (contracts only).',
    MATURITY_DATE DATE COMMENT 'Contractual maturity of the position (bonds, contracts).',
    HAIRCUT_FACTOR NUMBER(6,4) COMMENT 'Regulatory haircut multiplier for HQLA (1.00 L1, 0.85 L2A, 0.50 L2B).',
    RUNOFF_ADJUSTMENT NUMBER(6,4) COMMENT 'Account-level run-off adjustment for deposits: -2pp for 3+ products and -1pp for direct debit (where the deposit type allows relationship discounts), +5pp for tenure under 18 months.',
    IS_PERFORMING BOOLEAN COMMENT 'Performing flag (contracts); non-performing exposures get 0% inflow.',
    INCLUDED_IN_LCR BOOLEAN COMMENT 'TRUE when the position is in scope of the LCR: HQLA-eligible holdings, ACTIVE deposit accounts, all contracts.',
    ERROR_CODE VARCHAR(30) COMMENT 'Data-quality error: UNMAPPED_ASSET_CLASS, MISSING_FX, INVALID_DATE or MISSING_RULE. NULL when the position is valid.',
    ERROR_DETAIL VARCHAR(500) COMMENT 'Human-readable explanation of the data-quality error.',
    DQ_STATUS VARCHAR(10) COMMENT 'OK = position used in calculations; EXCEPTION = quarantined to REPP_AGG_DT_LCR_EXCEPTIONS (REQ-DQ-01).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Unified, CHF-converted position layer of the LCR engine: HQLA holdings, deposits and contracts in one grain (AS_OF_DATE x POSITION_ID) enriched with counterparty, product mapping (REQ-MAP-01), FX (REQ-ING-03) and data-quality validation (REQ-DQ-01). All downstream scenario, attribution and drill-down objects read from here; only DQ_STATUS = OK and INCLUDED_IN_LCR rows enter the LCR.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH cp AS (
    SELECT COUNTERPARTY_ID, COUNTERPARTY_NAME, RATING
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_COUNTERPARTIES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COUNTERPARTY_ID ORDER BY INSERT_TIMESTAMP_UTC DESC) = 1
),
hold AS (
    SELECT h.AS_OF_DATE, h.HOLDING_ID AS POSITION_ID, 'HOLDINGS' AS SOURCE_SYSTEM, h.ASSET_TYPE AS PRODUCT_TYPE,
           h.SECURITY_NAME AS POSITION_NAME, h.ISSUER_ID AS COUNTERPARTY_ID, 'ALL' AS RULE_SECTOR,
           COALESCE(h.BUSINESS_UNIT, 'TREASURY') AS BUSINESS_UNIT, h.CURRENCY, h.CREDIT_RATING AS RATING,
           h.MARKET_VALUE_CCY AS AMOUNT_CCY, 0 AS UNDRAWN_CCY, NULL::DATE AS START_DATE, h.MATURITY_DATE,
           e.HAIRCUT_FACTOR, e.REGULATORY_LEVEL AS ELIG_LEVEL, 0 AS RUNOFF_ADJUSTMENT, TRUE AS IS_PERFORMING,
           (h.HQLA_ELIGIBLE AND COALESCE(e.IS_ACTIVE, FALSE)) AS INCLUDED_IN_LCR
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS h
    LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY e ON e.ASSET_TYPE = h.ASSET_TYPE
),
dep AS (
    SELECT d.AS_OF_DATE, d.ACCOUNT_ID, 'DEPOSITS', d.DEPOSIT_TYPE, d.ACCOUNT_ID,
           COALESCE(d.COUNTERPARTY_ID, d.CUSTOMER_ID), d.COUNTERPARTY_TYPE,
           COALESCE(d.BUSINESS_UNIT, IFF(d.COUNTERPARTY_TYPE = 'RETAIL', 'RETAIL', IFF(d.COUNTERPARTY_TYPE = 'CORPORATE', 'CORPORATE', 'TREASURY'))),
           d.CURRENCY, NULL, d.BALANCE_CCY, 0, NULL::DATE, NULL::DATE, NULL, NULL,
           IFF(dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.PRODUCT_COUNT >= 3, -0.02, 0)
             + IFF(dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.HAS_DIRECT_DEBIT, -0.01, 0)
             + IFF(d.ACCOUNT_TENURE_DAYS < 18 * 30, 0.05, 0),
           TRUE,
           (d.ACCOUNT_STATUS = 'ACTIVE' AND COALESCE(dt.IS_ACTIVE, FALSE))
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES d
    LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES dt ON dt.DEPOSIT_TYPE = d.DEPOSIT_TYPE
),
ctr AS (
    SELECT c.AS_OF_DATE, c.CONTRACT_ID, 'CONTRACTS', c.PRODUCT_TYPE, c.CONTRACT_ID, c.COUNTERPARTY_ID,
           c.COUNTERPARTY_SECTOR, c.BUSINESS_UNIT, c.CURRENCY, c.RATING, c.OUTSTANDING_CCY, c.UNDRAWN_CCY,
           c.START_DATE, c.MATURITY_DATE, NULL, NULL, 0, c.IS_PERFORMING, TRUE
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_CONTRACTS c
),
allpos AS (
    SELECT * FROM hold UNION ALL SELECT * FROM dep UNION ALL SELECT * FROM ctr
)
SELECT
    p.AS_OF_DATE, p.POSITION_ID, p.SOURCE_SYSTEM, p.PRODUCT_TYPE, p.POSITION_NAME, p.COUNTERPARTY_ID,
    COALESCE(cp.COUNTERPARTY_NAME, p.COUNTERPARTY_ID) AS COUNTERPARTY_NAME,
    p.RULE_SECTOR AS COUNTERPARTY_SECTOR, p.BUSINESS_UNIT, p.CURRENCY, COALESCE(p.RATING, cp.RATING) AS RATING,
    m.ASSET_CLASS, m.PRODUCT_GROUP, m.LCR_TREATMENT, COALESCE(p.ELIG_LEVEL, m.HQLA_LEVEL) AS HQLA_LEVEL,
    p.AMOUNT_CCY::NUMBER(18,2) AS AMOUNT_CCY, fx.RATE_TO_CHF AS FX_RATE_TO_CHF,
    (p.AMOUNT_CCY * fx.RATE_TO_CHF)::NUMBER(18,2) AS AMOUNT_CHF,
    (p.UNDRAWN_CCY * fx.RATE_TO_CHF)::NUMBER(18,2) AS UNDRAWN_CHF,
    p.START_DATE, p.MATURITY_DATE, p.HAIRCUT_FACTOR::NUMBER(6,4) AS HAIRCUT_FACTOR,
    p.RUNOFF_ADJUSTMENT::NUMBER(6,4) AS RUNOFF_ADJUSTMENT, p.IS_PERFORMING, p.INCLUDED_IN_LCR,
    CASE
        WHEN m.PRODUCT_TYPE IS NULL THEN 'UNMAPPED_ASSET_CLASS'
        WHEN fx.RATE_TO_CHF IS NULL THEN 'MISSING_FX'
        WHEN p.SOURCE_SYSTEM = 'CONTRACTS' AND (p.MATURITY_DATE IS NULL OR p.START_DATE > p.MATURITY_DATE) THEN 'INVALID_DATE'
        WHEN br.PRODUCT_TYPE IS NULL THEN 'MISSING_RULE'
    END AS ERROR_CODE,
    CASE ERROR_CODE
        WHEN 'UNMAPPED_ASSET_CLASS' THEN 'Product type ' || p.PRODUCT_TYPE || ' (' || p.SOURCE_SYSTEM || ') has no active mapping in LIQI_RAW_TB_PRODUCT_MAPPING'
        WHEN 'MISSING_FX' THEN 'No end-of-day FX rate ' || p.CURRENCY || '->CHF for ' || p.AS_OF_DATE::VARCHAR
        WHEN 'INVALID_DATE' THEN 'Start date ' || COALESCE(p.START_DATE::VARCHAR, 'NULL') || ' is after maturity date ' || COALESCE(p.MATURITY_DATE::VARCHAR, 'NULL')
        WHEN 'MISSING_RULE' THEN 'No BASE behavioral rule for ' || p.PRODUCT_TYPE || ' / ' || p.RULE_SECTOR
    END AS ERROR_DETAIL,
    IFF(ERROR_CODE IS NULL, 'OK', 'EXCEPTION') AS DQ_STATUS,
    CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM allpos p
LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_PRODUCT_MAPPING m
    ON m.PRODUCT_TYPE = p.PRODUCT_TYPE AND m.SOURCE_SYSTEM = p.SOURCE_SYSTEM AND m.IS_ACTIVE
LEFT JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_FX_DAILY fx
    ON fx.AS_OF_DATE = p.AS_OF_DATE AND fx.CURRENCY = p.CURRENCY
LEFT JOIN cp ON cp.COUNTERPARTY_ID = p.COUNTERPARTY_ID
LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_BEHAVIORAL_RULES br
    ON br.PRODUCT_TYPE = p.PRODUCT_TYPE AND br.COUNTERPARTY_SECTOR = p.RULE_SECTOR
   AND br.STRESS_SCENARIO = 'BASE' AND br.IS_ACTIVE;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_EXCEPTIONS(
    SNAPSHOT_DATE DATE COMMENT 'Snapshot date on which the invalid record was received (spec 3.2 LIQUIDITY_EXCEPTIONS).',
    CONTRACT_ID VARCHAR(50) COMMENT 'Quarantined position key (contract id, account id or holding id).',
    SOURCE_SYSTEM VARCHAR(20) COMMENT 'Source feed of the invalid record: HOLDINGS, DEPOSITS or CONTRACTS.',
    PRODUCT_TYPE VARCHAR(50) COMMENT 'Source product code of the invalid record.',
    COUNTERPARTY_ID VARCHAR(50) COMMENT 'Counterparty of the invalid record.',
    CURRENCY VARCHAR(3) COMMENT 'Currency of the invalid record.',
    AMOUNT_CCY NUMBER(18,2) COMMENT 'Exposure in original currency excluded from the LCR because of the error.',
    ERROR_CODE VARCHAR(30) COMMENT 'Error code: UNMAPPED_ASSET_CLASS, MISSING_FX, INVALID_DATE or MISSING_RULE.',
    ERROR_DETAIL VARCHAR(500) COMMENT 'Human-readable explanation used by data stewards to remediate the record.',
    LOGGED_AT TIMESTAMP_NTZ COMMENT 'UTC timestamp when the exception was logged (dynamic table refresh time).'
) COMMENT = 'Quarantine of invalid LCR source records (REQ-DQ-01, spec 3.2). Records that fail mapping, FX or date validation are excluded from all LCR calculations and listed here with an error code and explanation, so the run never fails and stewards can remediate (TC-VAL-03).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT AS_OF_DATE, POSITION_ID, SOURCE_SYSTEM, PRODUCT_TYPE, COUNTERPARTY_ID, CURRENCY, AMOUNT_CCY,
       ERROR_CODE, ERROR_DETAIL, CURRENT_TIMESTAMP()
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_POSITIONS
WHERE DQ_STATUS = 'EXCEPTION';
