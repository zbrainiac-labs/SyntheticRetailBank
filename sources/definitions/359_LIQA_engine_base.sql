/*
 * 359_LIQA_engine_base.sql
 * Shared base of the liquidity engines (LCR 360-363 and DtI 365) - one book, one calculation path:
 *   REPP_AGG_FN_LCR_CALC          shared LCR formula
 *   REPP_AGG_DT_LCR_FX_DAILY      daily CCY->CHF rates (REQ-ING-03)
 *   REPP_AGG_VW_LIQ_CALENDAR      Zurich business-day calendar (both pages, DtI daily view)
 *   REPP_AGG_VW_LIQ_RULES         active behavioral rules (LCR scenarios, ladder, DtI run-off)
 *   REPP_AGG_DT_LIQ_POSITIONS     unified CHF positions of every snapshot (daily window + month-ends) with
 *                                 product mapping, data-quality validation and the DtI attributes (REQ-MAP-01, REQ-DQ-01)
 *   REPP_AGG_DT_LIQ_CASHFLOWS     contractual cashflows of valid contracts in CHF
 *   REPP_AGG_DT_LCR_POSITIONS     daily slice of the position port = the LCR engine input (REQ-ENG-04)
 *   REPP_AGG_DT_LCR_EXCEPTIONS    quarantined records of the daily LCR snapshots
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

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LIQ_CALENDAR(
    CAL_DATE COMMENT 'Calendar date (2025-01-01 to 2060-12-31; covers the redemption dates of long bonds in the collateral pool).',
    IS_BUSINESS_DAY COMMENT 'TRUE for Monday to Friday excluding Zurich bank holidays (LIQI_RAW_TB_DTI_HOLIDAYS, seeded from reference_data/liqi).',
    NEXT_BUSINESS_DAY COMMENT 'First business day on or after CAL_DATE; cash flows on weekends and holidays are rolled to this date in the DtI daily view.'
) COMMENT = 'Shared Zurich business-day calendar of the liquidity book (one calendar for the LCR and DtI pages, the generator reads the same holiday list). Drives the DtI daily view (Tagessicht, UC2 6.3) and the reporting-date checks.'
AS
WITH d AS (
    SELECT DATEADD(day, f.value::INT, '2025-01-01'::DATE) AS CAL_DATE
    FROM LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, 13149)) f
),
b AS (
    SELECT d.CAL_DATE,
           DAYOFWEEKISO(d.CAL_DATE) <= 5 AND h.HOLIDAY_DATE IS NULL AS IS_BUSINESS_DAY
    FROM d
    LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_HOLIDAYS h ON h.HOLIDAY_DATE = d.CAL_DATE
)
SELECT CAL_DATE, IS_BUSINESS_DAY,
       MIN(IFF(IS_BUSINESS_DAY, CAL_DATE, NULL)) OVER (ORDER BY CAL_DATE ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS NEXT_BUSINESS_DAY
FROM b;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LIQ_RULES(
    PRODUCT_TYPE COMMENT 'Product type the rule applies to (deposit type, contract product or HQLA asset type).',
    COUNTERPARTY_SECTOR COMMENT 'Counterparty sector of the rule (ALL for HQLA asset types).',
    STRESS_SCENARIO COMMENT 'Scenario of the rule: BASE (regulatory), BANK_STRESS, MARKET_STRESS, COMBINED.',
    RUNOFF_RATE COMMENT 'Run-off rate of deposits / repayment share of interbank borrowing (NULL when not applicable).',
    INFLOW_RATE COMMENT 'Inflow rate on 30-day performing contractual inflows (NULL when not applicable).',
    DRAWDOWN_RATE COMMENT 'Drawdown rate of undrawn committed facilities (NULL when not applicable).',
    HAIRCUT_ADDON_PCT COMMENT 'Stress add-on to the regulatory HQLA haircut in percentage points.',
    DECAY_PROFILE_DAYS COMMENT 'Days over which a deposit run-off is spread linearly (ladder and DtI projection).',
    DECAY_METHOD COMMENT 'LINEAR or NONE.',
    SIM_CATEGORY COMMENT 'Lever category of the what-if simulator.',
    OUTFLOW_RATE COMMENT 'Outflow rate of the rule: run-off rate, else drawdown rate (deposit run-off and facility drawdown of the DtI projection).'
) COMMENT = 'Active behavioral rules of the liquidity book (single read path for LIQI_RAW_TB_BEHAVIORAL_RULES): LCR scenario positions, maturity ladder, data-quality rule check and DtI run-off / drawdown all use this view.'
AS
SELECT PRODUCT_TYPE, COUNTERPARTY_SECTOR, STRESS_SCENARIO, RUNOFF_RATE, INFLOW_RATE, DRAWDOWN_RATE, HAIRCUT_ADDON_PCT,
       DECAY_PROFILE_DAYS, DECAY_METHOD, SIM_CATEGORY, COALESCE(RUNOFF_RATE, DRAWDOWN_RATE) AS OUTFLOW_RATE
FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_BEHAVIORAL_RULES
WHERE IS_ACTIVE;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS(
    AS_OF_DATE DATE COMMENT 'Snapshot (cut-off) date of the position.',
    POSITION_ID VARCHAR(50) COMMENT 'Atomic position key: HOLDING_ID (= SECURITY_ID of the securities book), deposit ACCOUNT_ID or CONTRACT_ID.',
    SOURCE_SYSTEM VARCHAR(20) COMMENT 'Source feed of the position: HOLDINGS, DEPOSITS or CONTRACTS.',
    PRODUCT_TYPE VARCHAR(50) COMMENT 'Source product code (asset type, deposit type or contract product type).',
    POSITION_NAME VARCHAR(200) COMMENT 'Display name of the position (security name, account id or contract id).',
    COUNTERPARTY_ID VARCHAR(50) COMMENT 'Counterparty / issuer of the position (FK to LIQI_RAW_TB_COUNTERPARTIES).',
    COUNTERPARTY_NAME VARCHAR(200) COMMENT 'Counterparty name from the counterparty master (falls back to the id).',
    COUNTERPARTY_SECTOR VARCHAR(30) COMMENT 'Rule sector used to look up behavioral rules: deposit COUNTERPARTY_TYPE, contract COUNTERPARTY_SECTOR, or ALL for HQLA.',
    BUSINESS_UNIT VARCHAR(30) COMMENT 'Owning business unit (RETAIL, PRIVATE_BANKING, CORPORATE, TREASURY).',
    CURRENCY VARCHAR(3) COMMENT 'Original currency of the position (ISO 4217).',
    RATING VARCHAR(10) COMMENT 'Rating of the instrument or counterparty (S&P scale).',
    ASSET_CLASS VARCHAR(40) COMMENT 'Level-1 LCR taxonomy from LIQI_RAW_TB_PRODUCT_MAPPING. NULL when unmapped.',
    PRODUCT_GROUP VARCHAR(40) COMMENT 'Level-2 taxonomy (product group) from the product mapping.',
    LCR_TREATMENT VARCHAR(20) COMMENT 'How the position enters the LCR: HQLA, OUTFLOW, INFLOW or FACILITY.',
    HQLA_LEVEL VARCHAR(5) COMMENT 'HQLA level L1 / L2A / L2B for HQLA positions (from LIQI_RAW_TB_HQLA_ELIGIBILITY).',
    AMOUNT_CCY NUMBER(18,2) COMMENT 'Exposure in original currency: market value (holdings), balance (deposits) or drawn outstanding (contracts).',
    FX_RATE_TO_CHF NUMBER(18,8) COMMENT 'End-of-day CCY->CHF rate of AS_OF_DATE (REPP_AGG_DT_LCR_FX_DAILY). NULL means missing FX.',
    AMOUNT_CHF NUMBER(18,2) COMMENT 'Exposure converted to CHF (AMOUNT_CCY x FX_RATE_TO_CHF): the single CHF value used by LCR and DtI.',
    UNDRAWN_CHF NUMBER(18,2) COMMENT 'Undrawn committed facility amount in CHF (contracts only, basis for drawdown outflows).',
    START_DATE DATE COMMENT 'Contract start / last roll-over date (contracts only).',
    MATURITY_DATE DATE COMMENT 'Contractual maturity of the position (bonds, contracts).',
    HAIRCUT_FACTOR NUMBER(6,4) COMMENT 'Regulatory haircut multiplier for HQLA (1.00 L1, 0.85 L2A, 0.50 L2B).',
    RUNOFF_ADJUSTMENT NUMBER(6,4) COMMENT 'Account-level run-off adjustment for deposits: -2pp for 3+ products and -1pp for direct debit (where allowed), +5pp for tenure under 18 months.',
    IS_PERFORMING BOOLEAN COMMENT 'Performing flag (contracts); non-performing exposures get 0% inflow.',
    INCLUDED_IN_LCR BOOLEAN COMMENT 'TRUE when the position is in scope of the LCR: HQLA-eligible unencumbered holdings, ACTIVE deposit accounts, all contracts.',
    ERROR_CODE VARCHAR(30) COMMENT 'Data-quality error: UNMAPPED_ASSET_CLASS, MISSING_FX, INVALID_DATE or MISSING_RULE. NULL when the position is valid.',
    ERROR_DETAIL VARCHAR(500) COMMENT 'Human-readable explanation of the data-quality error.',
    DQ_STATUS VARCHAR(10) COMMENT 'OK = position used in the LCR and DtI calculations; EXCEPTION = quarantined in both engines (REQ-DQ-01).',
    SNAPSHOT_SCOPE VARCHAR(10) COMMENT 'DAILY = LCR window snapshot (every calendar day), MONTH_END = DtI month-end history of the same simulation.',
    ENTITY_ID VARCHAR(10) COMMENT 'Legal entity of the position (LE_01..LE_09); central bank and vault cash are booked at LE_01.',
    APPLICATION_ID VARCHAR(10) COMMENT 'DtI source application (HYP, KRE, GEL, WPA, EIN, DER).',
    AKTIV_PASSIV VARCHAR(1) COMMENT 'Balance sheet side: A = asset, P = liability.',
    ACCOUNT_STATUS VARCHAR(20) COMMENT 'Deposit account status (ACTIVE, DORMANT); NULL for holdings and contracts.',
    WLK_CLUSTER VARCHAR(5) COMMENT 'Internal collateral cluster (WLK I..XIb) of holdings; NULL for deposits and contracts.',
    WLK_HAIRCUT NUMBER(6,4) COMMENT 'WLK haircut of the holding (DtI stressed collateral value = AMOUNT_CHF x (1 - WLK_HAIRCUT)).',
    IS_ENCUMBERED BOOLEAN COMMENT 'TRUE when the security is pledged in secured funding: neither LCR HQLA nor free DtI collateral.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Position port of the liquidity book: HQLA holdings (cash + the securities book), deposits and contracts of every snapshot (daily LCR window and DtI month-ends) in one grain (AS_OF_DATE x POSITION_ID), converted to CHF once, mapped to the LCR taxonomy (REQ-MAP-01) and validated (REQ-DQ-01). The LCR engine reads its daily slice (REPP_AGG_DT_LCR_POSITIONS), the DtI engine reads all snapshots; both use only DQ_STATUS = OK, so the same records count on both pages.'
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
           (h.HQLA_ELIGIBLE AND COALESCE(e.IS_ACTIVE, FALSE)) AS INCLUDED_IN_LCR,
           COALESCE(h.SNAPSHOT_SCOPE, 'DAILY') AS SNAPSHOT_SCOPE, h.ENTITY_ID,
           IFF(h.ASSET_TYPE LIKE 'CASH%', 'GEL', 'WPA') AS APPLICATION_ID, 'A' AS AKTIV_PASSIV, NULL AS ACCOUNT_STATUS,
           h.WLK_CLUSTER, h.WLK_HAIRCUT, COALESCE(h.IS_ENCUMBERED, FALSE) AS IS_ENCUMBERED
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
           (d.ACCOUNT_STATUS = 'ACTIVE' AND COALESCE(dt.IS_ACTIVE, FALSE)),
           COALESCE(d.SNAPSHOT_SCOPE, 'DAILY'), d.ENTITY_ID, d.APPLICATION_ID, d.AKTIV_PASSIV, d.ACCOUNT_STATUS,
           NULL, NULL, FALSE
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES d
    LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES dt ON dt.DEPOSIT_TYPE = d.DEPOSIT_TYPE
),
ctr AS (
    SELECT c.AS_OF_DATE, c.CONTRACT_ID, 'CONTRACTS', c.PRODUCT_TYPE, c.CONTRACT_ID, c.COUNTERPARTY_ID,
           c.COUNTERPARTY_SECTOR, c.BUSINESS_UNIT, c.CURRENCY, c.RATING, c.OUTSTANDING_CCY, c.UNDRAWN_CCY,
           c.START_DATE, c.MATURITY_DATE, NULL, NULL, 0, c.IS_PERFORMING, TRUE,
           COALESCE(c.SNAPSHOT_SCOPE, 'DAILY'), c.ENTITY_ID, c.APPLICATION_ID, c.AKTIV_PASSIV, NULL,
           NULL, NULL, FALSE
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
    p.SNAPSHOT_SCOPE::VARCHAR(10) AS SNAPSHOT_SCOPE, p.ENTITY_ID, p.APPLICATION_ID, p.AKTIV_PASSIV, p.ACCOUNT_STATUS,
    p.WLK_CLUSTER, p.WLK_HAIRCUT::NUMBER(6,4) AS WLK_HAIRCUT, p.IS_ENCUMBERED,
    CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM allpos p
LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_PRODUCT_MAPPING m
    ON m.PRODUCT_TYPE = p.PRODUCT_TYPE AND m.SOURCE_SYSTEM = p.SOURCE_SYSTEM AND m.IS_ACTIVE
LEFT JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_FX_DAILY fx
    ON fx.AS_OF_DATE = p.AS_OF_DATE AND fx.CURRENCY = p.CURRENCY
LEFT JOIN cp ON cp.COUNTERPARTY_ID = p.COUNTERPARTY_ID
LEFT JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LIQ_RULES br
    ON br.PRODUCT_TYPE = p.PRODUCT_TYPE AND br.COUNTERPARTY_SECTOR = p.RULE_SECTOR AND br.STRESS_SCENARIO = 'BASE';

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_CASHFLOWS(
    AS_OF_DATE DATE COMMENT 'Snapshot date of the cashflow schedule.',
    CONTRACT_ID VARCHAR(50) COMMENT 'Contract generating the cashflow (POSITION_ID of REPP_AGG_DT_LIQ_POSITIONS).',
    CF_DATE DATE COMMENT 'Contractual payment date.',
    CF_TYPE VARCHAR(20) COMMENT 'PRINCIPAL or INTEREST.',
    DIRECTION VARCHAR(3) COMMENT 'IN = receivable for the bank, OUT = payable.',
    CURRENCY VARCHAR(3) COMMENT 'Currency of the cashflow (= contract currency).',
    AMOUNT_CCY NUMBER(18,2) COMMENT 'Cashflow amount in original currency.',
    FX_RATE_TO_CHF NUMBER(18,8) COMMENT 'End-of-day CCY->CHF rate of the snapshot date (from the position port).',
    AMOUNT_CHF NUMBER(18,2) COMMENT 'Cashflow amount converted to CHF (AMOUNT_CCY x FX_RATE_TO_CHF).',
    SNAPSHOT_SCOPE VARCHAR(10) COMMENT 'DAILY (LCR window) or MONTH_END (DtI history).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Cashflow port of the liquidity book: contractual cashflows of the valid contracts only (DQ_STATUS = OK in REPP_AGG_DT_LIQ_POSITIONS), converted to CHF with the same rate as the contract. Feeds the LCR 30-day flows, the maturity ladder and the DtI deterministic flows.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT c.AS_OF_DATE, c.CONTRACT_ID, c.CF_DATE, c.CF_TYPE, c.DIRECTION, c.CURRENCY, c.AMOUNT_CCY,
       p.FX_RATE_TO_CHF, (c.AMOUNT_CCY * p.FX_RATE_TO_CHF)::NUMBER(18,2) AS AMOUNT_CHF, p.SNAPSHOT_SCOPE,
       CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_CASHFLOWS c
JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS p
    ON p.AS_OF_DATE = c.AS_OF_DATE AND p.POSITION_ID = c.CONTRACT_ID
   AND p.SOURCE_SYSTEM = 'CONTRACTS' AND p.DQ_STATUS = 'OK';

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
) COMMENT = 'Daily LCR window slice of the position port REPP_AGG_DT_LIQ_POSITIONS: HQLA holdings, deposits and contracts in one grain (AS_OF_DATE x POSITION_ID) enriched with counterparty, product mapping (REQ-MAP-01), FX (REQ-ING-03) and data-quality validation (REQ-DQ-01). All downstream LCR scenario, attribution and drill-down objects read from here; only DQ_STATUS = OK and INCLUDED_IN_LCR rows enter the LCR.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT AS_OF_DATE, POSITION_ID, SOURCE_SYSTEM, PRODUCT_TYPE, POSITION_NAME, COUNTERPARTY_ID, COUNTERPARTY_NAME,
       COUNTERPARTY_SECTOR, BUSINESS_UNIT, CURRENCY, RATING, ASSET_CLASS, PRODUCT_GROUP, LCR_TREATMENT, HQLA_LEVEL,
       AMOUNT_CCY, FX_RATE_TO_CHF, AMOUNT_CHF, UNDRAWN_CHF, START_DATE, MATURITY_DATE, HAIRCUT_FACTOR,
       RUNOFF_ADJUSTMENT, IS_PERFORMING, INCLUDED_IN_LCR, ERROR_CODE, ERROR_DETAIL, DQ_STATUS, CALCULATION_TIMESTAMP
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS
WHERE SNAPSHOT_SCOPE = 'DAILY';

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
) COMMENT = 'Quarantine of invalid LCR source records (REQ-DQ-01, spec 3.2). Records that fail mapping, FX or date validation are excluded from all LCR and DtI calculations and listed here with an error code and explanation, so the run never fails and stewards can remediate (TC-VAL-03).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT AS_OF_DATE, POSITION_ID, SOURCE_SYSTEM, PRODUCT_TYPE, COUNTERPARTY_ID, CURRENCY, AMOUNT_CCY,
       ERROR_CODE, ERROR_DETAIL, CURRENT_TIMESTAMP()
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_POSITIONS
WHERE DQ_STATUS = 'EXCEPTION';
