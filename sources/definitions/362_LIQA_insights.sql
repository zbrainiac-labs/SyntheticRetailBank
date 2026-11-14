/*
 * 362_LIQA_insights.sql
 * LCR engine - part 3: liquidity gap / maturity ladder, cut-off deltas, exact variance
 * attribution, top variance drivers and drill-down (REQ-ENG-01/02, REQ-ANL-01/02/03, REQ-UI-03).
 */
DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_LIQUIDITY_GAP(
    AS_OF_DATE DATE COMMENT 'Snapshot (cut-off) date of the maturity ladder.',
    STRESS_SCENARIO VARCHAR(20) COMMENT 'Scenario whose behavioral decay profile is applied to non-maturity deposits.',
    BUCKET_CODE VARCHAR(10) COMMENT 'Maturity bucket code (ON, D2_7, D8_30, M1_3, M3_6, M6_12, Y1P).',
    BUCKET_NAME VARCHAR(30) COMMENT 'Display name of the maturity bucket.',
    SORT_ORDER INTEGER COMMENT 'Display order of the bucket.',
    IN_LCR_HORIZON BOOLEAN COMMENT 'TRUE for buckets within the 30-day LCR horizon.',
    CONTRACTUAL_INFLOW_CHF NUMBER(18,2) COMMENT 'Contractual cash inflows (principal + interest) of valid contracts falling into the bucket, in CHF (unweighted).',
    CONTRACTUAL_OUTFLOW_CHF NUMBER(18,2) COMMENT 'Contractual cash outflows (interbank borrowing repayments and interest) falling into the bucket, in CHF.',
    BEHAVIORAL_OUTFLOW_CHF NUMBER(18,2) COMMENT 'Behavioral run-down of non-maturity deposits allocated to the bucket: balance spread linearly over DECAY_PROFILE_DAYS of the scenario rule (REQ-ENG-02).',
    COUNTERBALANCING_CHF NUMBER(18,2) COMMENT 'Post-haircut HQLA (pre-cap) available as counterbalancing capacity, shown in the overnight bucket only.',
    NET_GAP_CHF NUMBER(18,2) COMMENT 'Bucket gap = contractual inflows - contractual outflows - behavioral outflows.',
    CUMULATIVE_GAP_CHF NUMBER(18,2) COMMENT 'Running sum of NET_GAP_CHF plus counterbalancing capacity from the overnight bucket onward; negative values indicate a funding shortfall at that horizon.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Maturity ladder / liquidity gap per date, scenario and standard bucket (REQ-ENG-01, REQ-ENG-02): contractual cashflows from LIQI_RAW_TB_CASHFLOWS plus behavioral decay of non-maturity deposits (linear over the scenario decay horizon), with counterbalancing HQLA and cumulative gap. Feeds the gap chart and the behavioral heatmap.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH b AS (
    SELECT BUCKET_CODE, BUCKET_NAME, FROM_DAY, TO_DAY, SORT_ORDER, IN_LCR_HORIZON
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_MATURITY_BUCKETS
),
s AS (SELECT SCENARIO_CODE FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_SCENARIOS),
cf AS (
    SELECT c.AS_OF_DATE, b.BUCKET_CODE,
           SUM(IFF(c.DIRECTION = 'IN', c.AMOUNT_CCY * p.FX_RATE_TO_CHF, 0)) AS IN_CHF,
           SUM(IFF(c.DIRECTION = 'OUT', c.AMOUNT_CCY * p.FX_RATE_TO_CHF, 0)) AS OUT_CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_CASHFLOWS c
    JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_POSITIONS p
        ON p.AS_OF_DATE = c.AS_OF_DATE AND p.POSITION_ID = c.CONTRACT_ID
       AND p.SOURCE_SYSTEM = 'CONTRACTS' AND p.DQ_STATUS = 'OK'
    JOIN b ON DATEDIFF(day, c.AS_OF_DATE, c.CF_DATE) BETWEEN b.FROM_DAY AND b.TO_DAY
    GROUP BY c.AS_OF_DATE, b.BUCKET_CODE
),
dep AS (
    SELECT sp.AS_OF_DATE, sp.STRESS_SCENARIO, r.DECAY_PROFILE_DAYS, SUM(sp.EXPOSURE_CHF) AS BAL
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS sp
    JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LIQ_RULES r
        ON r.PRODUCT_TYPE = sp.PRODUCT_TYPE AND r.COUNTERPARTY_SECTOR = sp.COUNTERPARTY_SECTOR
       AND r.STRESS_SCENARIO = sp.STRESS_SCENARIO
    WHERE sp.SOURCE_SYSTEM = 'DEPOSITS' AND r.DECAY_METHOD = 'LINEAR' AND r.DECAY_PROFILE_DAYS > 0
    GROUP BY sp.AS_OF_DATE, sp.STRESS_SCENARIO, r.DECAY_PROFILE_DAYS
),
beh AS (
    SELECT d.AS_OF_DATE, d.STRESS_SCENARIO, b.BUCKET_CODE,
           SUM(d.BAL * GREATEST(0, LEAST(b.TO_DAY, d.DECAY_PROFILE_DAYS) - GREATEST(b.FROM_DAY, 1) + 1) / d.DECAY_PROFILE_DAYS) AS BEH_CHF
    FROM dep d CROSS JOIN b
    GROUP BY 1, 2, 3
),
hq AS (
    SELECT AS_OF_DATE, STRESS_SCENARIO, SUM(HQLA_WEIGHTED_CHF) AS HQLA_CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS
    WHERE LCR_TREATMENT = 'HQLA'
    GROUP BY 1, 2
),
grid AS (
    SELECT DISTINCT h.AS_OF_DATE, h.STRESS_SCENARIO, b.*
    FROM hq h CROSS JOIN b
),
g AS (
    SELECT gr.AS_OF_DATE, gr.STRESS_SCENARIO, gr.BUCKET_CODE, gr.BUCKET_NAME, gr.SORT_ORDER, gr.IN_LCR_HORIZON,
           COALESCE(cf.IN_CHF, 0) AS IN_CHF, COALESCE(cf.OUT_CHF, 0) AS OUT_CHF, COALESCE(beh.BEH_CHF, 0) AS BEH_CHF,
           IFF(gr.BUCKET_CODE = 'ON', hq.HQLA_CHF, 0) AS CBC_CHF
    FROM grid gr
    LEFT JOIN cf ON cf.AS_OF_DATE = gr.AS_OF_DATE AND cf.BUCKET_CODE = gr.BUCKET_CODE
    LEFT JOIN beh ON beh.AS_OF_DATE = gr.AS_OF_DATE AND beh.STRESS_SCENARIO = gr.STRESS_SCENARIO AND beh.BUCKET_CODE = gr.BUCKET_CODE
    LEFT JOIN hq ON hq.AS_OF_DATE = gr.AS_OF_DATE AND hq.STRESS_SCENARIO = gr.STRESS_SCENARIO
)
SELECT AS_OF_DATE, STRESS_SCENARIO, BUCKET_CODE, BUCKET_NAME, SORT_ORDER, IN_LCR_HORIZON,
       IN_CHF::NUMBER(18,2), OUT_CHF::NUMBER(18,2), BEH_CHF::NUMBER(18,2), CBC_CHF::NUMBER(18,2),
       (IN_CHF - OUT_CHF - BEH_CHF)::NUMBER(18,2),
       SUM(IN_CHF - OUT_CHF - BEH_CHF + CBC_CHF) OVER (PARTITION BY AS_OF_DATE, STRESS_SCENARIO ORDER BY SORT_ORDER
                                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::NUMBER(18,2),
       CURRENT_TIMESTAMP()
FROM g;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_CUTOFF_DELTAS(
    AS_OF_DATE DATE COMMENT 'Current cut-off date (T).',
    COMPARISON_TYPE VARCHAR(5) COMMENT 'Comparison window: DOD (T vs T-1), WOW (T vs T-7), MOM (T vs T-30) (REQ-ANL-01).',
    PRIOR_DATE DATE COMMENT 'Prior cut-off date compared against.',
    STRESS_SCENARIO VARCHAR(20) COMMENT 'Scenario code: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS or COMBINED.',
    ASSET_CLASS VARCHAR(40) COMMENT 'LCR asset class compared; TOTAL for the whole-bank row carrying the LCR ratio.',
    HQLA_CHF NUMBER(18,2) COMMENT 'Post-haircut HQLA at T (pre-cap for asset-class rows, capped for TOTAL).',
    HQLA_PRIOR_CHF NUMBER(18,2) COMMENT 'Post-haircut HQLA at the prior date.',
    HQLA_DELTA_CHF NUMBER(18,2) COMMENT 'Change in HQLA (T - prior).',
    OUTFLOW_CHF NUMBER(18,2) COMMENT 'Stressed outflows at T.',
    OUTFLOW_PRIOR_CHF NUMBER(18,2) COMMENT 'Stressed outflows at the prior date.',
    OUTFLOW_DELTA_CHF NUMBER(18,2) COMMENT 'Change in stressed outflows (T - prior).',
    INFLOW_CHF NUMBER(18,2) COMMENT 'Weighted inflows at T (capped for TOTAL).',
    INFLOW_PRIOR_CHF NUMBER(18,2) COMMENT 'Weighted inflows at the prior date.',
    INFLOW_DELTA_CHF NUMBER(18,2) COMMENT 'Change in weighted inflows (T - prior).',
    LCR_RATIO NUMBER(8,2) COMMENT 'LCR at T in percent (TOTAL rows only).',
    LCR_PRIOR NUMBER(8,2) COMMENT 'LCR at the prior date in percent (TOTAL rows only).',
    LCR_DELTA_PP NUMBER(8,2) COMMENT 'LCR change in percentage points (TOTAL rows only).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Snapshot comparison of LCR components between cut-off dates (REQ-ANL-01): day-over-day, week-over-week and month-over-month deltas of HQLA, outflows and inflows per asset class, plus a TOTAL row with the LCR change. Calendar-day comparisons (data exists for every calendar day).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH cmp AS (SELECT column1 AS COMPARISON_TYPE, column2 AS LAG_DAYS FROM VALUES ('DOD', 1), ('WOW', 7), ('MOM', 30)),
cls AS (
    SELECT AS_OF_DATE, STRESS_SCENARIO, ASSET_CLASS,
           SUM(HQLA_WEIGHTED_CHF) AS H, SUM(OUTFLOW_CHF) AS O, SUM(INFLOW_CHF) AS I
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT AS_OF_DATE, STRESS_SCENARIO, 'TOTAL', HQLA_TOTAL, OUTFLOW_TOTAL, INFLOW_CAPPED
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY
),
lcr AS (SELECT AS_OF_DATE, STRESS_SCENARIO, LCR_RATIO FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY)
SELECT t.AS_OF_DATE, c.COMPARISON_TYPE, p.AS_OF_DATE, t.STRESS_SCENARIO, t.ASSET_CLASS,
       t.H::NUMBER(18,2), p.H::NUMBER(18,2), (t.H - p.H)::NUMBER(18,2),
       t.O::NUMBER(18,2), p.O::NUMBER(18,2), (t.O - p.O)::NUMBER(18,2),
       t.I::NUMBER(18,2), p.I::NUMBER(18,2), (t.I - p.I)::NUMBER(18,2),
       IFF(t.ASSET_CLASS = 'TOTAL', lt.LCR_RATIO, NULL),
       IFF(t.ASSET_CLASS = 'TOTAL', lp.LCR_RATIO, NULL),
       IFF(t.ASSET_CLASS = 'TOTAL', lt.LCR_RATIO - lp.LCR_RATIO, NULL),
       CURRENT_TIMESTAMP()
FROM cls t
CROSS JOIN cmp c
JOIN cls p ON p.STRESS_SCENARIO = t.STRESS_SCENARIO AND p.ASSET_CLASS = t.ASSET_CLASS
          AND p.AS_OF_DATE = DATEADD(day, -c.LAG_DAYS, t.AS_OF_DATE)
JOIN lcr lt ON lt.AS_OF_DATE = t.AS_OF_DATE AND lt.STRESS_SCENARIO = t.STRESS_SCENARIO
JOIN lcr lp ON lp.AS_OF_DATE = p.AS_OF_DATE AND lp.STRESS_SCENARIO = p.STRESS_SCENARIO;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_VARIANCE_ATTRIBUTION(
    AS_OF_DATE COMMENT 'Current cut-off date (T).',
    PRIOR_DATE COMMENT 'Prior cut-off date (T-1, T-7 or T-30).',
    COMPARISON_TYPE COMMENT 'Comparison window: DOD (T vs T-1), WOW (T vs T-7) or MOM (T vs T-30).',
    STRESS_SCENARIO COMMENT 'Scenario code: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS or COMBINED.',
    DRIVER_TYPE COMMENT 'CLIENT (deposits, loans, facilities, interbank contracts), TREASURY (HQLA holdings) or ADJUSTMENT (effect of the L2 caps and of the 75% inflow cap).',
    POSITION_ID COMMENT 'Driver position (contract / account / holding id); CAP_ADJUSTMENT or INFLOW_CAP_ADJUSTMENT for adjustment lines.',
    POSITION_NAME COMMENT 'Display name of the driver position.',
    SOURCE_SYSTEM COMMENT 'Source feed of the driver position.',
    PRODUCT_TYPE COMMENT 'Product code of the driver position.',
    ASSET_CLASS COMMENT 'LCR asset class of the driver position.',
    PRODUCT_GROUP COMMENT 'Product group of the driver position.',
    COUNTERPARTY_ID COMMENT 'Counterparty of the driver position.',
    COUNTERPARTY_NAME COMMENT 'Client / issuer name of the driver position.',
    BUSINESS_UNIT COMMENT 'Business unit of the driver position.',
    DELTA_HQLA_CHF COMMENT 'Change in post-haircut (pre-cap) HQLA of the position in CHF.',
    DELTA_OUTFLOW_CHF COMMENT 'Change in stressed outflow of the position in CHF.',
    DELTA_INFLOW_CHF COMMENT 'Change in weighted (pre-cap) inflow of the position in CHF.',
    CONTRIBUTION_PP COMMENT 'Exact contribution of the position to the LCR change in percentage points. HQLA: dH/N1; outflow: -H0 x dO/(N0 x N1); inflow: +H0 x dI/(N0 x N1). All contributions incl. adjustment lines sum exactly to LCR_DELTA_PP.',
    LCR_PRIOR COMMENT 'LCR at the prior date in percent (unrounded).',
    LCR_CURRENT COMMENT 'LCR at T in percent (unrounded).',
    LCR_DELTA_PP COMMENT 'Total LCR change in percentage points (unrounded) that the contributions explain.',
    DELTA_EXPOSURE_CHF COMMENT 'Change in exposure of the position in CHF (deposit balance, drawn outstanding or market value).',
    SETTLEMENT_IMPACT_PP COMMENT 'LCR effect of the cash settlement of the client move in percentage points: deposit withdrawals and borrowing repayments reduce SNB reserves, loan repayments increase them (delta cash / N1 x 100). 0 for HQLA and adjustment lines.',
    CLIENT_IMPACT_PP COMMENT 'Funding-adjusted LCR impact of the client move in percentage points = CONTRIBUTION_PP + SETTLEMENT_IMPACT_PP. A CHF 50M corporate withdrawal shows its full negative effect here (cash leaves the bank) although its own outflow line falls. Used to rank root causes.'
) COMMENT = 'Exact, additive attribution of LCR changes to individual positions (REQ-ANL-03, TC-VAL-04). Uses the decomposition dLCR = dH/N1 + H0 x (1/N1 - 1/N0), split by position, with separate lines for the L2 cap and the 75% inflow cap effects, so contributions always sum to the reported LCR change. Filter by AS_OF_DATE, COMPARISON_TYPE and STRESS_SCENARIO.'
AS
WITH cmp AS (SELECT column1 AS COMPARISON_TYPE, column2 AS LAG_DAYS FROM VALUES ('DOD', 1), ('WOW', 7), ('MOM', 30)),
d AS (
    SELECT t.AS_OF_DATE, p.AS_OF_DATE AS PRIOR_DATE, c.COMPARISON_TYPE, t.STRESS_SCENARIO,
           t.HQLA_TOTAL AS H1, p.HQLA_TOTAL AS H0, t.NET_OUTFLOW AS N1, p.NET_OUTFLOW AS N0,
           t.INFLOW_CAPPED AS IC1, p.INFLOW_CAPPED AS IC0
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY t
    CROSS JOIN cmp c
    JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY p
        ON p.STRESS_SCENARIO = t.STRESS_SCENARIO AND p.AS_OF_DATE = DATEADD(day, -c.LAG_DAYS, t.AS_OF_DATE)
    WHERE t.NET_OUTFLOW > 0 AND p.NET_OUTFLOW > 0
),
legs AS (
    SELECT d.AS_OF_DATE, d.COMPARISON_TYPE, d.STRESS_SCENARIO, sp.POSITION_ID, sp.POSITION_NAME, sp.SOURCE_SYSTEM,
           sp.PRODUCT_TYPE, sp.ASSET_CLASS, sp.PRODUCT_GROUP, sp.COUNTERPARTY_ID, sp.COUNTERPARTY_NAME, sp.BUSINESS_UNIT,
           sp.HQLA_WEIGHTED_CHF AS W, sp.OUTFLOW_CHF AS O, sp.INFLOW_CHF AS I, sp.EXPOSURE_CHF AS X
    FROM d JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS sp
        ON sp.AS_OF_DATE = d.AS_OF_DATE AND sp.STRESS_SCENARIO = d.STRESS_SCENARIO
    UNION ALL
    SELECT d.AS_OF_DATE, d.COMPARISON_TYPE, d.STRESS_SCENARIO, sp.POSITION_ID, sp.POSITION_NAME, sp.SOURCE_SYSTEM,
           sp.PRODUCT_TYPE, sp.ASSET_CLASS, sp.PRODUCT_GROUP, sp.COUNTERPARTY_ID, sp.COUNTERPARTY_NAME, sp.BUSINESS_UNIT,
           -sp.HQLA_WEIGHTED_CHF, -sp.OUTFLOW_CHF, -sp.INFLOW_CHF, -sp.EXPOSURE_CHF
    FROM d JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS sp
        ON sp.AS_OF_DATE = d.PRIOR_DATE AND sp.STRESS_SCENARIO = d.STRESS_SCENARIO
),
pos AS (
    SELECT AS_OF_DATE, COMPARISON_TYPE, STRESS_SCENARIO, POSITION_ID,
           ANY_VALUE(POSITION_NAME) AS POSITION_NAME, ANY_VALUE(SOURCE_SYSTEM) AS SOURCE_SYSTEM,
           ANY_VALUE(PRODUCT_TYPE) AS PRODUCT_TYPE, ANY_VALUE(ASSET_CLASS) AS ASSET_CLASS,
           ANY_VALUE(PRODUCT_GROUP) AS PRODUCT_GROUP, ANY_VALUE(COUNTERPARTY_ID) AS COUNTERPARTY_ID,
           ANY_VALUE(COUNTERPARTY_NAME) AS COUNTERPARTY_NAME, ANY_VALUE(BUSINESS_UNIT) AS BUSINESS_UNIT,
           SUM(W) AS DW, SUM(O) AS DO_, SUM(I) AS DI, SUM(X) AS DX
    FROM legs
    GROUP BY AS_OF_DATE, COMPARISON_TYPE, STRESS_SCENARIO, POSITION_ID
    HAVING SUM(W) <> 0 OR SUM(O) <> 0 OR SUM(I) <> 0 OR SUM(X) <> 0
),
tot AS (
    SELECT AS_OF_DATE, COMPARISON_TYPE, STRESS_SCENARIO, SUM(DW) AS SDW, SUM(DI) AS SDI
    FROM pos GROUP BY 1, 2, 3
)
SELECT d.AS_OF_DATE, d.PRIOR_DATE, d.COMPARISON_TYPE, d.STRESS_SCENARIO,
       IFF(p.SOURCE_SYSTEM = 'HOLDINGS', 'TREASURY', 'CLIENT'), p.POSITION_ID, p.POSITION_NAME, p.SOURCE_SYSTEM,
       p.PRODUCT_TYPE, p.ASSET_CLASS, p.PRODUCT_GROUP, p.COUNTERPARTY_ID, p.COUNTERPARTY_NAME, p.BUSINESS_UNIT,
       p.DW, p.DO_, p.DI,
       (p.DW / d.N1 - d.H0 * p.DO_ / (d.N0 * d.N1) + d.H0 * p.DI / (d.N0 * d.N1)) * 100,
       d.H0 / d.N0 * 100, d.H1 / d.N1 * 100, (d.H1 / d.N1 - d.H0 / d.N0) * 100,
       p.DX,
       -- cash settlement: liabilities (deposits, borrowing) move cash with the balance, assets against it
       CASE WHEN p.SOURCE_SYSTEM = 'HOLDINGS' THEN 0
            WHEN p.SOURCE_SYSTEM = 'DEPOSITS' OR p.PRODUCT_TYPE = 'INTERBANK_BORROWING' THEN p.DX
            WHEN p.PRODUCT_TYPE IN ('CREDIT_FACILITY', 'LIQUIDITY_FACILITY') THEN 0
            ELSE -p.DX END / d.N1 * 100 AS SETTLE_PP,
       (p.DW / d.N1 - d.H0 * p.DO_ / (d.N0 * d.N1) + d.H0 * p.DI / (d.N0 * d.N1)) * 100 + SETTLE_PP
FROM pos p
JOIN d ON d.AS_OF_DATE = p.AS_OF_DATE AND d.COMPARISON_TYPE = p.COMPARISON_TYPE AND d.STRESS_SCENARIO = p.STRESS_SCENARIO
UNION ALL
SELECT d.AS_OF_DATE, d.PRIOR_DATE, d.COMPARISON_TYPE, d.STRESS_SCENARIO, 'ADJUSTMENT', 'CAP_ADJUSTMENT',
       'Effect of L2B 15% / L2 40% caps', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       (d.H1 - d.H0) - COALESCE(t.SDW, 0), 0, 0,
       ((d.H1 - d.H0) - COALESCE(t.SDW, 0)) / d.N1 * 100,
       d.H0 / d.N0 * 100, d.H1 / d.N1 * 100, (d.H1 / d.N1 - d.H0 / d.N0) * 100,
       0, 0, ((d.H1 - d.H0) - COALESCE(t.SDW, 0)) / d.N1 * 100
FROM d LEFT JOIN tot t ON t.AS_OF_DATE = d.AS_OF_DATE AND t.COMPARISON_TYPE = d.COMPARISON_TYPE AND t.STRESS_SCENARIO = d.STRESS_SCENARIO
UNION ALL
SELECT d.AS_OF_DATE, d.PRIOR_DATE, d.COMPARISON_TYPE, d.STRESS_SCENARIO, 'ADJUSTMENT', 'INFLOW_CAP_ADJUSTMENT',
       'Effect of 75% inflow cap', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       0, 0, (d.IC1 - d.IC0) - COALESCE(t.SDI, 0),
       d.H0 * ((d.IC1 - d.IC0) - COALESCE(t.SDI, 0)) / (d.N0 * d.N1) * 100,
       d.H0 / d.N0 * 100, d.H1 / d.N1 * 100, (d.H1 / d.N1 - d.H0 / d.N0) * 100,
       0, 0, d.H0 * ((d.IC1 - d.IC0) - COALESCE(t.SDI, 0)) / (d.N0 * d.N1) * 100
FROM d LEFT JOIN tot t ON t.AS_OF_DATE = d.AS_OF_DATE AND t.COMPARISON_TYPE = d.COMPARISON_TYPE AND t.STRESS_SCENARIO = d.STRESS_SCENARIO;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_VARIANCE_DRIVERS(
    AS_OF_DATE COMMENT 'Current cut-off date (T).',
    PRIOR_DATE COMMENT 'Prior cut-off date the current date is compared against.',
    COMPARISON_TYPE COMMENT 'Comparison window: DOD (T vs T-1), WOW (T vs T-7) or MOM (T vs T-30).',
    STRESS_SCENARIO COMMENT 'Scenario code: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS or COMBINED.',
    CLIENT_RANK COMMENT 'Rank of the driver among CLIENT positions by absolute funding-adjusted impact CLIENT_IMPACT_PP (1 = largest).',
    POSITION_ID COMMENT 'Driver contract / account id.',
    POSITION_NAME COMMENT 'Display name of the driver position.',
    PRODUCT_TYPE COMMENT 'Product code of the driver.',
    ASSET_CLASS COMMENT 'LCR asset class of the driver.',
    COUNTERPARTY_ID COMMENT 'Counterparty of the driver.',
    COUNTERPARTY_NAME COMMENT 'Client name of the driver (REQ-UI-03).',
    BUSINESS_UNIT COMMENT 'Business unit of the driver.',
    DELTA_OUTFLOW_CHF COMMENT 'Change in stressed outflow of the driver in CHF.',
    DELTA_INFLOW_CHF COMMENT 'Change in weighted inflow of the driver in CHF.',
    CONTRIBUTION_PP COMMENT 'Exact contribution of the driver to the LCR change in percentage points.',
    SHARE_OF_DELTA_PCT COMMENT 'Funding-adjusted impact (CLIENT_IMPACT_PP) as a share of the total LCR change in percent.',
    LCR_DELTA_PP COMMENT 'Total LCR change in percentage points.',
    CLIENT_IMPACT_PP COMMENT 'Funding-adjusted LCR impact in percentage points (exact contribution plus the cash settlement of the client move). Negative = the client move lowered the LCR. Use this to explain root causes.',
    SETTLEMENT_IMPACT_PP COMMENT 'Part of CLIENT_IMPACT_PP caused by the cash settlement (SNB reserves) of the client move, in percentage points.'
) COMMENT = 'Top-10 client variance drivers per cut-off comparison and scenario (REQ-UI-03, REQ-ANL-03): the client positions (deposits, loans, facilities, interbank contracts) with the largest absolute exact contribution to the LCR change, with contract id and client name. Treasury HQLA moves and cap adjustments are excluded so the root cause (e.g. a client withdrawal) ranks first.'
AS
SELECT AS_OF_DATE, PRIOR_DATE, COMPARISON_TYPE, STRESS_SCENARIO,
       ROW_NUMBER() OVER (PARTITION BY AS_OF_DATE, COMPARISON_TYPE, STRESS_SCENARIO ORDER BY ABS(CLIENT_IMPACT_PP) DESC),
       POSITION_ID, POSITION_NAME, PRODUCT_TYPE, ASSET_CLASS, COUNTERPARTY_ID, COUNTERPARTY_NAME, BUSINESS_UNIT,
       DELTA_OUTFLOW_CHF, DELTA_INFLOW_CHF, CONTRIBUTION_PP,
       CLIENT_IMPACT_PP / NULLIF(LCR_DELTA_PP, 0) * 100, LCR_DELTA_PP, CLIENT_IMPACT_PP, SETTLEMENT_IMPACT_PP
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_VARIANCE_ATTRIBUTION
WHERE DRIVER_TYPE = 'CLIENT'
QUALIFY ROW_NUMBER() OVER (PARTITION BY AS_OF_DATE, COMPARISON_TYPE, STRESS_SCENARIO ORDER BY ABS(CLIENT_IMPACT_PP) DESC) <= 10;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_DRILLDOWN(
    AS_OF_DATE COMMENT 'Snapshot (cut-off) date.',
    STRESS_SCENARIO COMMENT 'Scenario code: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS or COMBINED.',
    ASSET_CLASS COMMENT 'Drill-down level 1: LCR asset class (REQ-ANL-02).',
    PRODUCT_GROUP COMMENT 'Drill-down level 2: product group.',
    PRODUCT_TYPE COMMENT 'Drill-down level 3: source product code.',
    COUNTERPARTY_ID COMMENT 'Drill-down level 4: counterparty id.',
    COUNTERPARTY_NAME COMMENT 'Drill-down level 4: counterparty name.',
    POSITION_ID COMMENT 'Drill-down level 5: atomic contract / account / holding id.',
    POSITION_NAME COMMENT 'Display name of the position.',
    LCR_TREATMENT COMMENT 'HQLA, OUTFLOW, INFLOW or FACILITY.',
    HQLA_LEVEL COMMENT 'HQLA level for HQLA positions.',
    BUSINESS_UNIT COMMENT 'Business unit filter attribute (REQ-UI-02).',
    CURRENCY COMMENT 'Currency filter attribute.',
    RATING COMMENT 'Rating filter attribute.',
    EXPOSURE_CHF COMMENT 'Exposure in CHF (market value, balance or outstanding).',
    WEIGHTED_CHF COMMENT 'Weighted LCR amount (post-haircut HQLA, stressed outflow or weighted inflow).',
    LCR_IMPACT_CHF COMMENT 'Signed impact on the liquidity buffer: +HQLA, -outflow, +inflow (pre-cap).'
) COMMENT = 'Five-level LCR drill-down from asset class to product group, product, counterparty and individual contract (REQ-ANL-02) with business unit, currency and rating filter attributes (REQ-UI-02). Treemap and table source in the LCR page; filter by AS_OF_DATE and STRESS_SCENARIO.'
AS
SELECT AS_OF_DATE, STRESS_SCENARIO, ASSET_CLASS, PRODUCT_GROUP, PRODUCT_TYPE, COUNTERPARTY_ID, COUNTERPARTY_NAME,
       POSITION_ID, POSITION_NAME, LCR_TREATMENT, HQLA_LEVEL, BUSINESS_UNIT, CURRENCY, RATING, EXPOSURE_CHF,
       HQLA_WEIGHTED_CHF + OUTFLOW_CHF + INFLOW_CHF,
       HQLA_WEIGHTED_CHF - OUTFLOW_CHF + INFLOW_CHF
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS;
