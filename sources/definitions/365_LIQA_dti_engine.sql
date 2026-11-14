/*
 * 365_LIQA_dti_engine.sql
 * DtI / Stichtagsvergleich (UC2) engine on the shared liquidity book.
 *
 *   REPP_AGG_VW_DTI_CALENDAR    alias of the shared REPP_AGG_VW_LIQ_CALENDAR (359)
 *   REPP_AGG_DT_DTI_CASHFLOWS   all signed LST flows per reporting date (single source)
 *   REPP_AGG_DT_DTI_PROJECTION  cumulative balance per day 0..365 and Level I position
 *   REPP_AGG_DT_DTI_SUMMARY     Distance-to-Illiquidity per entity and date
 *   REPP_AGG_FN_DTI_COMPARE     reference vs. comparison date, buckets or business days
 *
 * Inputs come from the shared ports of 359 (positions, cashflows, rules, calendar) and the securities book,
 * so the DtI uses the same records, CHF values and rules as the LCR.
 * Sign convention: + = liquidity available / inflow, - = outflow.
 * Base values use the BASE behavioral rules, stressed values the COMBINED rules;
 * deterministic flows are identical in both (UC2 6.2 Wertefilter).
 */

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_DTI_CALENDAR(
    CAL_DATE COMMENT 'Calendar date (2025-01-01 to 2060-12-31; covers the redemption dates of long bonds in the collateral pool).',
    IS_BUSINESS_DAY COMMENT 'TRUE for Monday to Friday excluding Zurich bank holidays (LIQI_RAW_TB_DTI_HOLIDAYS).',
    NEXT_BUSINESS_DAY COMMENT 'First business day on or after CAL_DATE; cash flows on weekends and holidays are rolled to this date in the DtI daily view.'
) COMMENT = 'Business day calendar of the DtI comparison: alias of the shared liquidity calendar REPP_AGG_VW_LIQ_CALENDAR (Zurich holidays), kept for the DtI app routes and grants. Drives the daily view (Tagessicht, UC2 6.3).'
AS
SELECT CAL_DATE, IS_BUSINESS_DAY, NEXT_BUSINESS_DAY
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LIQ_CALENDAR;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_CASHFLOWS(
    AS_OF_DATE DATE COMMENT 'Reporting date of the LST data set (Stichtag).',
    ENTITY_ID VARCHAR(10) COMMENT 'Legal entity (LE_01..LE_09).',
    POSITION_CODE VARCHAR(50) COMMENT 'Leaf LST position (FK to LIQI_RAW_TB_DTI_LST_POSITIONS); parents are aggregated in the app along the position tree.',
    PRODUCT_CLUSTER VARCHAR(30) COMMENT 'Product cluster (FK to LIQI_RAW_TB_DTI_PRODUCT_CLUSTERS) for the product group pages.',
    APPLICATION_ID VARCHAR(10) COMMENT 'Source application (HYP, KRE, GEL, WPA, EIN, DER) for the application page.',
    AKTIV_PASSIV VARCHAR(1) COMMENT 'Balance sheet side: A = asset, P = liability.',
    PRODUCT_TYPE VARCHAR(50) COMMENT 'Single product (source product / deposit / asset type) for the single product page.',
    SOURCE_ID VARCHAR(50) COMMENT 'Deal level key: CONTRACT_ID for contractual flows, TXN_ID for new loan business, SECURITY_ID for pool securities, HOLDING_ID for cash, DEAL_ID for secured funding, CORRECTION_ID for corrections; NULL for aggregated modelled flows.',
    CURRENCY VARCHAR(3) COMMENT 'Original currency of the position.',
    CF_DATE DATE COMMENT 'Projection date of the flow: cashflow date of contracts, AS_OF_DATE + 1 for pool securities (repo capacity), AS_OF_DATE for cash.',
    BASE_CHF NUMBER(18,2) COMMENT 'Base value in CHF (signed): contractual amount, market value of the collateral pool, BASE behavioral rules for modelled flows.',
    STRESSED_CHF NUMBER(18,2) COMMENT 'Stressed value in CHF (signed): collateral value after WLK haircut, COMBINED behavioral rules; equal to BASE_CHF for deterministic flows.',
    FLOW_KIND VARCHAR(20) COMMENT 'CONTRACTUAL, POOL, RUNOFF, DRAWDOWN, RENEWAL (modelled prolongation of loan repayments), INFLOW, PLANNING or CORRECTION.',
    IS_CORRECTION BOOLEAN COMMENT 'TRUE for manual corrections (LIQI_RAW_TB_DTI_CORRECTIONS), shown as separate lines.',
    IS_DRILLABLE BOOLEAN COMMENT 'FALSE for planning positions without deal level data.',
    DATA_STATUS VARCHAR(20) COMMENT 'FINAL or PRELIMINARY run status of entity and reporting date (LIQI_RAW_TB_DTI_REPORT_STATUS); missing status counts as FINAL.'
) COMMENT = 'Single source of all DtI / LST flows per reporting date (UC2): deterministic cash flows from the contract book, the collateral pool as counterbalancing capacity (free securities of the shared securities book repoable on the next day, central bank cash), modelled deposit run-off and facility drawdowns (BASE vs. COMBINED rules), secured funding, planning positions and manual corrections. Every DtI page, the projection and the comparison function read from here, so all control sums reconcile.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
REFRESH_MODE = FULL
AS
WITH status AS (
    SELECT AS_OF_DATE, ENTITY_ID, DATA_STATUS
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_REPORT_STATUS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY AS_OF_DATE, ENTITY_ID ORDER BY INSERT_TIMESTAMP_UTC DESC) = 1
),
-- Reporting dates = dates with a DtI pool snapshot: Zurich business days of the window plus month-end history
rdates AS (
    SELECT DISTINCT AS_OF_DATE FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURITIES
),
-- Shared liquidity book (position and cashflow ports of 359): daily window + month-end history in one place,
-- valid records only (DQ_STATUS = OK, the same records the LCR uses), amounts already in CHF
ctr_all AS (
    SELECT POSITION_ID AS CONTRACT_ID, AS_OF_DATE, PRODUCT_TYPE, CURRENCY, UNDRAWN_CHF, COUNTERPARTY_SECTOR,
           ENTITY_ID, APPLICATION_ID, AKTIV_PASSIV
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS
    WHERE SOURCE_SYSTEM = 'CONTRACTS' AND DQ_STATUS = 'OK' AND AS_OF_DATE IN (SELECT AS_OF_DATE FROM rdates)
),
cf_all AS (
    SELECT AS_OF_DATE, CONTRACT_ID, CF_DATE, DIRECTION, AMOUNT_CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_CASHFLOWS WHERE AS_OF_DATE IN (SELECT AS_OF_DATE FROM rdates)
),
dep_all AS (
    SELECT AS_OF_DATE, ENTITY_ID, PRODUCT_TYPE AS DEPOSIT_TYPE, COUNTERPARTY_SECTOR AS COUNTERPARTY_TYPE, ACCOUNT_STATUS,
           APPLICATION_ID, AKTIV_PASSIV, CURRENCY, AMOUNT_CHF AS BALANCE_CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS
    WHERE SOURCE_SYSTEM = 'DEPOSITS' AND DQ_STATUS = 'OK' AND AS_OF_DATE IN (SELECT AS_OF_DATE FROM rdates)
),
cash_all AS (
    SELECT POSITION_ID AS HOLDING_ID, AS_OF_DATE, PRODUCT_TYPE AS ASSET_TYPE, CURRENCY, AMOUNT_CHF AS MARKET_VALUE_CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS
    WHERE SOURCE_SYSTEM = 'HOLDINGS' AND PRODUCT_TYPE LIKE 'CASH%' AND DQ_STATUS = 'OK'
      AND AS_OF_DATE IN (SELECT AS_OF_DATE FROM rdates)
),
txn AS (
    SELECT * FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_TRANSACTIONS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TXN_ID, AS_OF_DATE ORDER BY INSERT_TIMESTAMP_UTC DESC) = 1
),
asg AS (
    SELECT SOURCE_SYSTEM, PRODUCT_TYPE, FLOW_KIND, POSITION_CODE, PRODUCT_CLUSTER
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_LST_ASSIGNMENT
),
rules AS (
    SELECT PRODUCT_TYPE, COUNTERPARTY_SECTOR,
           MAX(IFF(STRESS_SCENARIO = 'BASE', OUTFLOW_RATE, NULL)) AS RATE_BASE,
           MAX(IFF(STRESS_SCENARIO = 'COMBINED', OUTFLOW_RATE, NULL)) AS RATE_STRESS,
           MAX(IFF(STRESS_SCENARIO = 'BASE', DECAY_PROFILE_DAYS, NULL)) AS DAYS_BASE,
           MAX(IFF(STRESS_SCENARIO = 'COMBINED', DECAY_PROFILE_DAYS, NULL)) AS DAYS_STRESS
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LIQ_RULES
    GROUP BY 1, 2
),
-- 1. Deterministic contractual cash flows (deal level)
contractual AS (
    SELECT c.AS_OF_DATE, c.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, c.APPLICATION_ID, c.AKTIV_PASSIV,
           c.PRODUCT_TYPE, c.CONTRACT_ID AS SOURCE_ID, c.CURRENCY, f.CF_DATE,
           IFF(f.DIRECTION = 'OUT', -1, 1) * f.AMOUNT_CHF AS AMT
    FROM cf_all f
    JOIN ctr_all c ON c.CONTRACT_ID = f.CONTRACT_ID AND c.AS_OF_DATE = f.AS_OF_DATE
    JOIN asg a ON a.SOURCE_SYSTEM = 'CASHFLOWS' AND a.PRODUCT_TYPE = c.PRODUCT_TYPE AND a.FLOW_KIND = 'CONTRACTUAL'
    WHERE f.CF_DATE > f.AS_OF_DATE
),
-- New loan business (DtI transactions): bullet repayment with interest at maturity
new_loans AS (
    SELECT t.AS_OF_DATE, t.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, t.APPLICATION_ID, t.AKTIV_PASSIV,
           t.PRODUCT_TYPE, t.TXN_ID AS SOURCE_ID, 'CHF' AS CURRENCY, t.END_DATE AS CF_DATE,
           t.AMOUNT_CHF * (1 + t.INTEREST_RATE * DATEDIFF(day, t.BOOK_DATE, t.END_DATE) / 365) AS AMT
    FROM txn t
    JOIN asg a ON a.SOURCE_SYSTEM = 'CASHFLOWS' AND a.PRODUCT_TYPE = t.PRODUCT_TYPE AND a.FLOW_KIND = 'CONTRACTUAL'
    WHERE t.TXN_TYPE = 'NEW_LOAN' AND t.END_DATE > t.AS_OF_DATE
),
-- 2. Collateral pool securities = counterbalancing capacity: every free (unencumbered) L1/L2A/L2B security of
--    the shared securities book can be repoed at the SNB / in the market and is liquid on the next day
--    (rolled to the next business day in the daily view). Base = market value, stressed = collateral value
--    after the WLK haircut. The same rows are the LCR HQLA (before the regulatory haircut).
pool_sec AS (
    SELECT s.AS_OF_DATE, s.ENTITY_ID,
           'POOL_SEC_' || s.HQLA_CATEGORY AS POSITION_CODE,
           CASE WHEN s.WLK_CLUSTER IN ('II', 'III', 'XIa', 'XIb') THEN 'PC_GOV'
                WHEN s.WLK_CLUSTER IN ('IV', 'V') THEN 'PC_PUBLIC' ELSE 'PC_CORP_BOND' END AS PRODUCT_CLUSTER,
           'WPA' AS APPLICATION_ID, 'A' AS AKTIV_PASSIV, 'WLK ' || s.WLK_CLUSTER AS PRODUCT_TYPE,
           s.SECURITY_ID AS SOURCE_ID, s.CURRENCY, DATEADD(day, 1, s.AS_OF_DATE) AS CF_DATE,
           s.MARKET_VALUE_CHF AS BASE_AMT, s.COLLATERAL_VALUE_CHF AS STRESS_AMT
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURITIES s
    WHERE NOT s.IS_ENCUMBERED AND s.HQLA_CATEGORY IN ('L1', 'L2A', 'L2B')
),
-- Central bank and vault cash is booked centrally and allocated to the entities by their current share of the
-- active deposits (D_e / D on the same reporting date). The entities sum to the total cash, no entity can go
-- negative, and an entity-specific withdrawal lowers that entity's share.
dep_ent AS (
    SELECT AS_OF_DATE, ENTITY_ID, SUM(BALANCE_CHF) AS D_E,
           SUM(SUM(BALANCE_CHF)) OVER (PARTITION BY AS_OF_DATE) AS D_TOT
    FROM dep_all
    WHERE ACCOUNT_STATUS = 'ACTIVE'
    GROUP BY 1, 2
),
pool_cash AS (
    SELECT h.AS_OF_DATE, s.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, 'GEL' AS APPLICATION_ID, 'A' AS AKTIV_PASSIV,
           h.ASSET_TYPE AS PRODUCT_TYPE, h.HOLDING_ID AS SOURCE_ID, h.CURRENCY, h.AS_OF_DATE AS CF_DATE,
           h.MARKET_VALUE_CHF * s.D_E / NULLIF(s.D_TOT, 0) AS BASE_AMT,
           h.MARKET_VALUE_CHF * s.D_E / NULLIF(s.D_TOT, 0) AS STRESS_AMT
    FROM cash_all h
    JOIN dep_ent s ON s.AS_OF_DATE = h.AS_OF_DATE
    JOIN asg a ON a.SOURCE_SYSTEM = 'HOLDINGS' AND a.PRODUCT_TYPE = h.ASSET_TYPE AND a.FLOW_KIND = 'POOL'
),
-- 3. Modelled deposit run-off: balance x rate, spread linearly over the decay profile (per scenario).
--    Includes the DtI business-day deposit movements on top of the LCR balances.
dep AS (
    SELECT AS_OF_DATE, ENTITY_ID, DEPOSIT_TYPE, COUNTERPARTY_TYPE, APPLICATION_ID, AKTIV_PASSIV, CURRENCY, SUM(BAL) AS BAL
    FROM (
        SELECT AS_OF_DATE, ENTITY_ID, DEPOSIT_TYPE, COUNTERPARTY_TYPE, APPLICATION_ID, AKTIV_PASSIV, CURRENCY, BALANCE_CHF AS BAL
        FROM dep_all WHERE ACCOUNT_STATUS = 'ACTIVE'
        UNION ALL
        SELECT AS_OF_DATE, ENTITY_ID, PRODUCT_TYPE, COUNTERPARTY_TYPE, APPLICATION_ID, AKTIV_PASSIV, 'CHF', AMOUNT_CHF
        FROM txn WHERE TXN_TYPE = 'DEPOSIT_MOVEMENT'
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
runoff AS (
    SELECT d.AS_OF_DATE, d.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, d.APPLICATION_ID, d.AKTIV_PASSIV,
           d.DEPOSIT_TYPE AS PRODUCT_TYPE, NULL AS SOURCE_ID, d.CURRENCY,
           DATEADD(day, g.value::INT, d.AS_OF_DATE) AS CF_DATE,
           IFF(g.value::INT <= r.DAYS_BASE, -d.BAL * r.RATE_BASE / r.DAYS_BASE, 0) AS BASE_AMT,
           IFF(g.value::INT <= r.DAYS_STRESS, -d.BAL * r.RATE_STRESS / r.DAYS_STRESS, 0) AS STRESS_AMT
    FROM dep d
    JOIN rules r ON r.PRODUCT_TYPE = d.DEPOSIT_TYPE AND r.COUNTERPARTY_SECTOR = d.COUNTERPARTY_TYPE
    JOIN asg a ON a.SOURCE_SYSTEM = 'DEPOSITS' AND a.PRODUCT_TYPE = d.DEPOSIT_TYPE AND a.FLOW_KIND = 'RUNOFF',
    LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(1, GREATEST(r.DAYS_BASE, r.DAYS_STRESS) + 1)) g
),
-- 3b. Loan renewals (modelled, DtI-only): a share of the contractual corporate loan repayments is assumed to be
--     prolonged (franchise lending) and does not come back as cash: 20% base, 60% stressed of each repayment.
renewal AS (
    SELECT f.AS_OF_DATE, f.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, f.APPLICATION_ID, f.AKTIV_PASSIV,
           f.PRODUCT_TYPE, NULL AS SOURCE_ID, 'CHF' AS CURRENCY, f.CF_DATE,
           -0.20 * SUM(f.AMT) AS BASE_AMT, -0.60 * SUM(f.AMT) AS STRESS_AMT
    FROM (SELECT * FROM contractual UNION ALL SELECT * FROM new_loans) f
    JOIN asg a ON a.SOURCE_SYSTEM = 'CASHFLOWS' AND a.PRODUCT_TYPE = f.PRODUCT_TYPE AND a.FLOW_KIND = 'INFLOW'
    WHERE f.AMT > 0
    GROUP BY f.AS_OF_DATE, f.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, f.APPLICATION_ID, f.AKTIV_PASSIV, f.PRODUCT_TYPE, f.CF_DATE
),
-- 4. Facility drawdowns: undrawn x drawdown rate, linear over the first 30 days
fac AS (
    SELECT c.AS_OF_DATE, c.ENTITY_ID, c.PRODUCT_TYPE, c.COUNTERPARTY_SECTOR, c.APPLICATION_ID, c.AKTIV_PASSIV, c.CURRENCY,
           SUM(c.UNDRAWN_CHF) AS UNDRAWN
    FROM ctr_all c
    WHERE c.UNDRAWN_CHF > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
drawdown AS (
    SELECT f.AS_OF_DATE, f.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, f.APPLICATION_ID, f.AKTIV_PASSIV,
           f.PRODUCT_TYPE, NULL AS SOURCE_ID, f.CURRENCY,
           DATEADD(day, g.value::INT, f.AS_OF_DATE) AS CF_DATE,
           -f.UNDRAWN * r.RATE_BASE / 30 AS BASE_AMT,
           -f.UNDRAWN * r.RATE_STRESS / 30 AS STRESS_AMT
    FROM fac f
    JOIN rules r ON r.PRODUCT_TYPE = f.PRODUCT_TYPE AND r.COUNTERPARTY_SECTOR = f.COUNTERPARTY_SECTOR
    JOIN asg a ON a.SOURCE_SYSTEM = 'CONTRACTS' AND a.PRODUCT_TYPE = f.PRODUCT_TYPE AND a.FLOW_KIND = 'DRAWDOWN',
    LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(1, 31)) g
),
-- 5. Secured funding: maturing reverse repos (inflow) and stressed collateral calls on bilateral repos
secured AS (
    SELECT s.AS_OF_DATE, s.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, 'GEL' AS APPLICATION_ID,
           IFF(s.DIRECTION = 'LEND', 'A', 'P') AS AKTIV_PASSIV, s.TRANSACTION_TYPE AS PRODUCT_TYPE, s.DEAL_ID AS SOURCE_ID,
           'CHF' AS CURRENCY,
           IFF(s.TRANSACTION_TYPE = 'REVERSE_REPO', s.MATURITY_DATE, DATEADD(day, 1, s.AS_OF_DATE)) AS CF_DATE,
           IFF(s.TRANSACTION_TYPE = 'REVERSE_REPO', s.NOMINAL_CHF, 0) AS BASE_AMT,
           IFF(s.TRANSACTION_TYPE = 'REVERSE_REPO', s.NOMINAL_CHF, -0.10 * s.NOMINAL_CHF) AS STRESS_AMT
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURED_FUNDING s
    JOIN asg a ON a.SOURCE_SYSTEM = 'SECURED_FUNDING' AND a.PRODUCT_TYPE = s.TRANSACTION_TYPE
),
-- 6. Planning positions (no deal level) and 7. manual corrections
planning AS (
    SELECT p.AS_OF_DATE, p.ENTITY_ID, a.POSITION_CODE, a.PRODUCT_CLUSTER, 'KRE' AS APPLICATION_ID,
           IFF(p.AMOUNT_CHF < 0, 'A', 'P') AS AKTIV_PASSIV, p.PLAN_TYPE AS PRODUCT_TYPE, NULL AS SOURCE_ID, 'CHF' AS CURRENCY,
           p.VALUE_DATE AS CF_DATE, p.AMOUNT_CHF AS AMT
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_PLANNING p
    JOIN asg a ON a.SOURCE_SYSTEM = 'PLANNING' AND a.PRODUCT_TYPE = p.PLAN_TYPE
),
corrections AS (
    SELECT k.AS_OF_DATE, k.ENTITY_ID, k.LST_POSITION AS POSITION_CODE, NULL AS PRODUCT_CLUSTER, NULL AS APPLICATION_ID,
           NULL AS AKTIV_PASSIV, 'CORRECTION' AS PRODUCT_TYPE, k.CORRECTION_ID AS SOURCE_ID, 'CHF' AS CURRENCY,
           k.VALUE_DATE AS CF_DATE, k.AMOUNT_CHF AS AMT
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_CORRECTIONS k
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k.CORRECTION_ID ORDER BY k.INSERT_TIMESTAMP_UTC DESC) = 1
),
flows AS (
    SELECT AS_OF_DATE, ENTITY_ID, POSITION_CODE, PRODUCT_CLUSTER, APPLICATION_ID, AKTIV_PASSIV, PRODUCT_TYPE, SOURCE_ID, CURRENCY,
           CF_DATE, AMT AS BASE_AMT, AMT AS STRESS_AMT, 'CONTRACTUAL' AS FLOW_KIND, FALSE AS IS_CORRECTION, TRUE AS IS_DRILLABLE FROM contractual
    UNION ALL SELECT AS_OF_DATE, ENTITY_ID, POSITION_CODE, PRODUCT_CLUSTER, APPLICATION_ID, AKTIV_PASSIV, PRODUCT_TYPE, SOURCE_ID, CURRENCY,
           CF_DATE, AMT, AMT, 'CONTRACTUAL', FALSE, TRUE FROM new_loans
    UNION ALL SELECT *, 'POOL', FALSE, TRUE FROM pool_sec
    UNION ALL SELECT *, 'POOL', FALSE, TRUE FROM pool_cash
    UNION ALL SELECT *, 'RUNOFF', FALSE, TRUE FROM runoff WHERE BASE_AMT <> 0 OR STRESS_AMT <> 0
    UNION ALL SELECT *, 'DRAWDOWN', FALSE, TRUE FROM drawdown
    UNION ALL SELECT *, 'RENEWAL', FALSE, TRUE FROM renewal
    UNION ALL SELECT *, IFF(PRODUCT_TYPE = 'REVERSE_REPO', 'INFLOW', 'RUNOFF'), FALSE, TRUE FROM secured
    UNION ALL SELECT AS_OF_DATE, ENTITY_ID, POSITION_CODE, PRODUCT_CLUSTER, APPLICATION_ID, AKTIV_PASSIV, PRODUCT_TYPE, SOURCE_ID, CURRENCY,
           -- planned funding (issuance) cannot be realised under stress: stressed value 0, planned lending stays
           CF_DATE, AMT, IFF(PRODUCT_TYPE = 'FUNDING_PLAN', 0, AMT), 'PLANNING', FALSE, FALSE FROM planning
    UNION ALL SELECT AS_OF_DATE, ENTITY_ID, POSITION_CODE, PRODUCT_CLUSTER, APPLICATION_ID, AKTIV_PASSIV, PRODUCT_TYPE, SOURCE_ID, CURRENCY,
           CF_DATE, AMT, AMT, 'CORRECTION', TRUE, TRUE FROM corrections
)
SELECT f.AS_OF_DATE, f.ENTITY_ID, f.POSITION_CODE, f.PRODUCT_CLUSTER, f.APPLICATION_ID, f.AKTIV_PASSIV, f.PRODUCT_TYPE, f.SOURCE_ID,
       f.CURRENCY, f.CF_DATE,
       ROUND(f.BASE_AMT, 2)::NUMBER(18,2) AS BASE_CHF, ROUND(f.STRESS_AMT, 2)::NUMBER(18,2) AS STRESSED_CHF,
       f.FLOW_KIND, f.IS_CORRECTION, f.IS_DRILLABLE, COALESCE(s.DATA_STATUS, 'FINAL') AS DATA_STATUS
FROM flows f
LEFT JOIN status s ON s.AS_OF_DATE = f.AS_OF_DATE AND s.ENTITY_ID = f.ENTITY_ID
WHERE f.AS_OF_DATE IN (SELECT AS_OF_DATE FROM rdates);

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_PROJECTION(
    AS_OF_DATE DATE COMMENT 'Reporting date of the LST data set.',
    ENTITY_ID VARCHAR(10) COMMENT 'Legal entity (LE_01..LE_09). Group values are sums over the selected entities (all columns are additive).',
    DATA_STATUS VARCHAR(20) COMMENT 'FINAL or PRELIMINARY status of the entity run on AS_OF_DATE.',
    DAY_OFFSET INTEGER COMMENT 'Projection day 0..365 relative to AS_OF_DATE.',
    PROJECTION_DATE DATE COMMENT 'AS_OF_DATE + DAY_OFFSET.',
    POOL_BASE_CHF NUMBER(18,2) COMMENT 'Cumulative Collateral Pool (Level I) up to this day, base values.',
    DET_BASE_CHF NUMBER(18,2) COMMENT 'Cumulative Deterministic Cash Flows (Level I), base values.',
    INF_BASE_CHF NUMBER(18,2) COMMENT 'Cumulative modelled Inflows (Level I), base values.',
    OUT_BASE_CHF NUMBER(18,2) COMMENT 'Cumulative modelled Outflows (Level I, negative), base values.',
    TOTAL_BASE_CHF NUMBER(18,2) COMMENT 'Liquiditaetssaldo gesamt (base) = sum of the four Level I positions.',
    POOL_STRESSED_CHF NUMBER(18,2) COMMENT 'Cumulative Collateral Pool, stressed values.',
    DET_STRESSED_CHF NUMBER(18,2) COMMENT 'Cumulative Deterministic Cash Flows, stressed values (equal to base).',
    INF_STRESSED_CHF NUMBER(18,2) COMMENT 'Cumulative modelled Inflows, stressed values.',
    OUT_STRESSED_CHF NUMBER(18,2) COMMENT 'Cumulative modelled Outflows, stressed values.',
    TOTAL_STRESSED_CHF NUMBER(18,2) COMMENT 'Liquiditaetssaldo gesamt (stressed) = sum of the four Level I positions; basis of the DtI.'
) COMMENT = 'LST projection over 365 days per entity and reporting date (UC2 6.1 element D): cumulative liquidity per Level I position and in total, base and stressed. Built from REPP_AGG_DT_DTI_CASHFLOWS on a dense day grid.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
REFRESH_MODE = FULL
AS
WITH lvl AS (
    -- Level I root of every leaf position (tree depth <= 4)
    SELECT p.POSITION_CODE,
           COALESCE(p4.POSITION_CODE, p3.POSITION_CODE, p2.POSITION_CODE, p.POSITION_CODE) AS ROOT
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_LST_POSITIONS p
    LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_LST_POSITIONS p2 ON p2.POSITION_CODE = p.PARENT_CODE
    LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_LST_POSITIONS p3 ON p3.POSITION_CODE = p2.PARENT_CODE
    LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_LST_POSITIONS p4 ON p4.POSITION_CODE = p3.PARENT_CODE
),
daily AS (
    SELECT c.AS_OF_DATE, c.ENTITY_ID, MAX(c.DATA_STATUS) AS DATA_STATUS,
           GREATEST(DATEDIFF(day, c.AS_OF_DATE, c.CF_DATE), 0) AS DAY_OFFSET,
           SUM(IFF(l.ROOT = 'POOL', c.BASE_CHF, 0)) AS POOL_B, SUM(IFF(l.ROOT = 'DET', c.BASE_CHF, 0)) AS DET_B,
           SUM(IFF(l.ROOT = 'INF', c.BASE_CHF, 0)) AS INF_B, SUM(IFF(l.ROOT = 'OUT', c.BASE_CHF, 0)) AS OUT_B,
           SUM(IFF(l.ROOT = 'POOL', c.STRESSED_CHF, 0)) AS POOL_S, SUM(IFF(l.ROOT = 'DET', c.STRESSED_CHF, 0)) AS DET_S,
           SUM(IFF(l.ROOT = 'INF', c.STRESSED_CHF, 0)) AS INF_S, SUM(IFF(l.ROOT = 'OUT', c.STRESSED_CHF, 0)) AS OUT_S
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_CASHFLOWS c
    JOIN lvl l ON l.POSITION_CODE = c.POSITION_CODE
    WHERE c.CF_DATE <= DATEADD(day, 365, c.AS_OF_DATE)
    GROUP BY 1, 2, 4
),
grid AS (
    SELECT k.AS_OF_DATE, k.ENTITY_ID, k.DATA_STATUS, g.value::INT AS DAY_OFFSET
    FROM (SELECT DISTINCT AS_OF_DATE, ENTITY_ID, DATA_STATUS FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_CASHFLOWS) k,
         LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, 366)) g
)
SELECT g.AS_OF_DATE, g.ENTITY_ID, g.DATA_STATUS, g.DAY_OFFSET, DATEADD(day, g.DAY_OFFSET, g.AS_OF_DATE) AS PROJECTION_DATE,
       SUM(COALESCE(d.POOL_B, 0)) OVER (PARTITION BY g.AS_OF_DATE, g.ENTITY_ID ORDER BY g.DAY_OFFSET ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS POOL_BASE_CHF, SUM(COALESCE(d.DET_B, 0)) OVER (PARTITION BY g.AS_OF_DATE, g.ENTITY_ID ORDER BY g.DAY_OFFSET ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DET_BASE_CHF,
       SUM(COALESCE(d.INF_B, 0)) OVER (PARTITION BY g.AS_OF_DATE, g.ENTITY_ID ORDER BY g.DAY_OFFSET ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INF_BASE_CHF, SUM(COALESCE(d.OUT_B, 0)) OVER (PARTITION BY g.AS_OF_DATE, g.ENTITY_ID ORDER BY g.DAY_OFFSET ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS OUT_BASE_CHF,
       POOL_BASE_CHF + DET_BASE_CHF + INF_BASE_CHF + OUT_BASE_CHF AS TOTAL_BASE_CHF,
       SUM(COALESCE(d.POOL_S, 0)) OVER (PARTITION BY g.AS_OF_DATE, g.ENTITY_ID ORDER BY g.DAY_OFFSET ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS POOL_STRESSED_CHF, SUM(COALESCE(d.DET_S, 0)) OVER (PARTITION BY g.AS_OF_DATE, g.ENTITY_ID ORDER BY g.DAY_OFFSET ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DET_STRESSED_CHF,
       SUM(COALESCE(d.INF_S, 0)) OVER (PARTITION BY g.AS_OF_DATE, g.ENTITY_ID ORDER BY g.DAY_OFFSET ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INF_STRESSED_CHF, SUM(COALESCE(d.OUT_S, 0)) OVER (PARTITION BY g.AS_OF_DATE, g.ENTITY_ID ORDER BY g.DAY_OFFSET ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS OUT_STRESSED_CHF,
       POOL_STRESSED_CHF + DET_STRESSED_CHF + INF_STRESSED_CHF + OUT_STRESSED_CHF AS TOTAL_STRESSED_CHF
FROM grid g
LEFT JOIN daily d ON d.AS_OF_DATE = g.AS_OF_DATE AND d.ENTITY_ID = g.ENTITY_ID AND d.DAY_OFFSET = g.DAY_OFFSET;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_SUMMARY(
    AS_OF_DATE DATE COMMENT 'Reporting date of the LST data set.',
    ENTITY_ID VARCHAR(10) COMMENT 'Legal entity (LE_01..LE_09).',
    DATA_STATUS VARCHAR(20) COMMENT 'FINAL or PRELIMINARY status of the entity run.',
    DTI_DAYS_STRESSED INTEGER COMMENT 'Distance-to-Illiquidity in days: first projection day with stressed Liquiditaetssaldo gesamt <= 0; NULL when liquidity stays positive for 365 days (> 365).',
    DTI_DATE_STRESSED DATE COMMENT 'AS_OF_DATE + DTI_DAYS_STRESSED (UC2 6.1 element C).',
    DTI_DAYS_BASE INTEGER COMMENT 'First projection day with base Liquiditaetssaldo gesamt <= 0; NULL when > 365.',
    MIN_TOTAL_STRESSED_CHF NUMBER(18,2) COMMENT 'Lowest stressed cumulative liquidity within 365 days.',
    TOTAL_DAY0_STRESSED_CHF NUMBER(18,2) COMMENT 'Stressed liquidity on day 0 (collateral pool and same-day flows).'
) COMMENT = 'Distance-to-Illiquidity (DtI) per entity and reporting date (UC2 6.1 element C). Group DtI is computed from summed projections of the selected entities.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
REFRESH_MODE = FULL
AS
SELECT AS_OF_DATE, ENTITY_ID, MAX(DATA_STATUS) AS DATA_STATUS,
       MIN(IFF(TOTAL_STRESSED_CHF <= 0, DAY_OFFSET, NULL)) AS DTI_DAYS_STRESSED,
       DATEADD(day, MIN(IFF(TOTAL_STRESSED_CHF <= 0, DAY_OFFSET, NULL)), AS_OF_DATE) AS DTI_DATE_STRESSED,
       MIN(IFF(TOTAL_BASE_CHF <= 0, DAY_OFFSET, NULL)) AS DTI_DAYS_BASE,
       MIN(TOTAL_STRESSED_CHF) AS MIN_TOTAL_STRESSED_CHF,
       MAX(IFF(DAY_OFFSET = 0, TOTAL_STRESSED_CHF, NULL)) AS TOTAL_DAY0_STRESSED_CHF
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_PROJECTION
GROUP BY AS_OF_DATE, ENTITY_ID;

DEFINE FUNCTION {{ db }}.{{ rep_agg }}.REPP_AGG_FN_DTI_COMPARE(
    P_REF DATE, P_COMP DATE, P_ENTITIES ARRAY, P_VALUE_TYPE VARCHAR, P_VIEW VARCHAR, P_INCLUDE_PRELIM BOOLEAN
)
RETURNS TABLE (
    POSITION_CODE VARCHAR, PRODUCT_CLUSTER VARCHAR, APPLICATION_ID VARCHAR, AKTIV_PASSIV VARCHAR, PRODUCT_TYPE VARCHAR,
    PERIOD_KEY VARCHAR, PERIOD_SORT INTEGER, PERIOD_DATE DATE, BUCKET_REF VARCHAR, BUCKET_COMP VARCHAR,
    REF_FROM DATE, REF_TO DATE, COMP_FROM DATE, COMP_TO DATE,
    REF_VALUE NUMBER(18,2), COMP_VALUE NUMBER(18,2), DELTA NUMBER(18,2), IS_INFO_ROW BOOLEAN
)
COMMENT = 'Stichtagsvergleich (UC2): compares the LST flows of P_REF and P_COMP for the selected entities. P_VIEW = BUCKETS: buckets defined relative to each own date (bucket of the reference date vs. the same bucket of the comparison date, both date ranges returned). P_VIEW = DAYS: one row per business day from P_REF for 365 days (flows rolled to the next business day) with the bucket of that day for both dates. Comparison flows dated before P_REF are returned once as info row BEFORE_REF (not part of the delta). P_VALUE_TYPE = BASE or STRESSED; P_INCLUDE_PRELIM = FALSE excludes PRELIMINARY runs. Grain: leaf LST position x product cluster x application x A/P x product x period; all pages aggregate this output, so their control sums reconcile.'
AS
$$
WITH sel AS (
    SELECT c.AS_OF_DATE, c.POSITION_CODE, c.PRODUCT_CLUSTER, c.APPLICATION_ID, c.AKTIV_PASSIV, c.PRODUCT_TYPE,
           cal.NEXT_BUSINESS_DAY AS BUS_DATE, c.CF_DATE,
           IFF(UPPER(P_VALUE_TYPE) = 'BASE', c.BASE_CHF, c.STRESSED_CHF) AS VAL
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_CASHFLOWS c
    JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_VW_DTI_CALENDAR cal ON cal.CAL_DATE = c.CF_DATE
    WHERE c.AS_OF_DATE IN (P_REF, P_COMP)
      AND ARRAY_CONTAINS(c.ENTITY_ID::VARIANT, P_ENTITIES)
      AND (P_INCLUDE_PRELIM OR c.DATA_STATUS = 'FINAL')
),
bounds AS (
    -- Upper bound of every bucket relative to each date; lower bound = previous upper + 1 (W1 starts on the date itself)
    SELECT d.BASE, b.BUCKET_CODE, b.SORT_ORDER,
           COALESCE(CASE b.OFFSET_UNIT WHEN 'DAY' THEN DATEADD(day, b.OFFSET_VALUE, d.BASE)
                                       ELSE ADD_MONTHS(d.BASE, b.OFFSET_VALUE) END, '9999-12-31'::DATE) AS UPPER_DATE
    FROM (SELECT P_REF AS BASE UNION SELECT P_COMP) d
    CROSS JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_BUCKETS b
),
bk AS (
    SELECT BASE, BUCKET_CODE, SORT_ORDER, UPPER_DATE,
           COALESCE(DATEADD(day, 1, LAG(UPPER_DATE) OVER (PARTITION BY BASE ORDER BY SORT_ORDER)), BASE) AS LOWER_DATE
    FROM bounds
),
tagged AS (
    SELECT s.*, b.BUCKET_CODE, b.SORT_ORDER
    FROM sel s
    JOIN bk b ON b.BASE = s.AS_OF_DATE AND s.CF_DATE BETWEEN b.LOWER_DATE AND b.UPPER_DATE
),
agg AS (
    SELECT POSITION_CODE, PRODUCT_CLUSTER, APPLICATION_ID, AKTIV_PASSIV, PRODUCT_TYPE, BUCKET_CODE,
           SUM(IFF(AS_OF_DATE = P_REF, VAL, NULL)) AS REF_VALUE,
           SUM(IFF(AS_OF_DATE = P_COMP, VAL, NULL)) AS COMP_VALUE
    FROM tagged
    GROUP BY 1, 2, 3, 4, 5, 6
),
bucket_rows AS (
    SELECT a.POSITION_CODE, a.PRODUCT_CLUSTER, a.APPLICATION_ID, a.AKTIV_PASSIV, a.PRODUCT_TYPE,
           r.BUCKET_CODE AS PERIOD_KEY, r.SORT_ORDER AS PERIOD_SORT, NULL::DATE AS PERIOD_DATE,
           r.BUCKET_CODE AS BUCKET_REF, r.BUCKET_CODE AS BUCKET_COMP,
           r.LOWER_DATE AS REF_FROM, r.UPPER_DATE AS REF_TO, c.LOWER_DATE AS COMP_FROM, c.UPPER_DATE AS COMP_TO,
           a.REF_VALUE, a.COMP_VALUE
    FROM agg a
    JOIN bk r ON r.BASE = P_REF AND r.BUCKET_CODE = a.BUCKET_CODE
    JOIN bk c ON c.BASE = P_COMP AND c.BUCKET_CODE = a.BUCKET_CODE
    WHERE UPPER(P_VIEW) = 'BUCKETS'
),
day_rows AS (
    SELECT s.POSITION_CODE, s.PRODUCT_CLUSTER, s.APPLICATION_ID, s.AKTIV_PASSIV, s.PRODUCT_TYPE,
           TO_VARCHAR(s.BUS_DATE, 'YYYY-MM-DD') AS PERIOD_KEY, DATEDIFF(day, P_REF, s.BUS_DATE) AS PERIOD_SORT, s.BUS_DATE AS PERIOD_DATE,
           SUM(IFF(s.AS_OF_DATE = P_REF, s.VAL, NULL)) AS REF_VALUE,
           SUM(IFF(s.AS_OF_DATE = P_COMP, s.VAL, NULL)) AS COMP_VALUE
    FROM sel s
    WHERE UPPER(P_VIEW) = 'DAYS' AND s.BUS_DATE BETWEEN P_REF AND DATEADD(day, 365, P_REF)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
day_out AS (
    SELECT d.POSITION_CODE, d.PRODUCT_CLUSTER, d.APPLICATION_ID, d.AKTIV_PASSIV, d.PRODUCT_TYPE,
           d.PERIOD_KEY, d.PERIOD_SORT, d.PERIOD_DATE,
           MAX(IFF(d.PERIOD_DATE BETWEEN r.LOWER_DATE AND r.UPPER_DATE, r.BUCKET_CODE, NULL)) AS BUCKET_REF,
           MAX(IFF(d.PERIOD_DATE BETWEEN c.LOWER_DATE AND c.UPPER_DATE, c.BUCKET_CODE, NULL)) AS BUCKET_COMP,
           d.PERIOD_DATE AS REF_FROM, d.PERIOD_DATE AS REF_TO, d.PERIOD_DATE AS COMP_FROM, d.PERIOD_DATE AS COMP_TO,
           MAX(d.REF_VALUE) AS REF_VALUE, MAX(d.COMP_VALUE) AS COMP_VALUE
    FROM day_rows d
    JOIN bk r ON r.BASE = P_REF
    JOIN bk c ON c.BASE = P_COMP
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
before_ref AS (
    SELECT POSITION_CODE, PRODUCT_CLUSTER, APPLICATION_ID, AKTIV_PASSIV, PRODUCT_TYPE,
           'BEFORE_REF' AS PERIOD_KEY, -1 AS PERIOD_SORT, NULL::DATE AS PERIOD_DATE, NULL AS BUCKET_REF, NULL AS BUCKET_COMP,
           NULL::DATE AS REF_FROM, NULL::DATE AS REF_TO, P_COMP AS COMP_FROM, DATEADD(day, -1, P_REF) AS COMP_TO,
           NULL AS REF_VALUE, SUM(VAL) AS COMP_VALUE
    FROM sel
    WHERE AS_OF_DATE = P_COMP AND IFF(UPPER(P_VIEW) = 'DAYS', BUS_DATE, CF_DATE) < P_REF AND P_COMP < P_REF
    GROUP BY 1, 2, 3, 4, 5
)
SELECT POSITION_CODE, PRODUCT_CLUSTER, APPLICATION_ID, AKTIV_PASSIV, PRODUCT_TYPE, PERIOD_KEY, PERIOD_SORT, PERIOD_DATE,
       BUCKET_REF, BUCKET_COMP, REF_FROM, REF_TO, COMP_FROM, COMP_TO,
       COALESCE(REF_VALUE, 0)::NUMBER(18,2), COALESCE(COMP_VALUE, 0)::NUMBER(18,2),
       (COALESCE(REF_VALUE, 0) - COALESCE(COMP_VALUE, 0))::NUMBER(18,2), FALSE
FROM (SELECT * FROM bucket_rows UNION ALL SELECT * FROM day_out)
WHERE COALESCE(REF_VALUE, 0) <> 0 OR COALESCE(COMP_VALUE, 0) <> 0
UNION ALL
SELECT POSITION_CODE, PRODUCT_CLUSTER, APPLICATION_ID, AKTIV_PASSIV, PRODUCT_TYPE, PERIOD_KEY, PERIOD_SORT, PERIOD_DATE,
       BUCKET_REF, BUCKET_COMP, REF_FROM, REF_TO, COMP_FROM, COMP_TO,
       0::NUMBER(18,2), COALESCE(COMP_VALUE, 0)::NUMBER(18,2), NULL::NUMBER(18,2), TRUE
FROM before_ref
$$;
