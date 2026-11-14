/*
 * 360_LIQA_analytics.sql
 * LCR engine - part 2: scenario positions, components, scenario daily results,
 * and the existing LCR dynamic tables rebuilt on the BASE (regulatory) scenario.
 * (REQ-ENG-03/04/05). Reads REPP_AGG_DT_LCR_POSITIONS from 359_LIQA_engine_base.sql.
 */
DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS(
    AS_OF_DATE DATE COMMENT 'Snapshot (cut-off) date of the position (daily COB).',
    STRESS_SCENARIO VARCHAR(20) COMMENT 'Scenario the position is evaluated in: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS, COMBINED (REQ-ENG-05).',
    POSITION_ID VARCHAR(50) COMMENT 'Atomic position key (holding id, deposit account id or contract id); driver key for variance attribution and drill-down.',
    SOURCE_SYSTEM VARCHAR(20) COMMENT 'Source feed of the position: HOLDINGS, DEPOSITS or CONTRACTS.',
    PRODUCT_TYPE VARCHAR(50) COMMENT 'Source product code (asset type, deposit type or contract product type).',
    POSITION_NAME VARCHAR(200) COMMENT 'Display name of the position (security name, account id or contract id).',
    PRODUCT_GROUP VARCHAR(40) COMMENT 'Level-2 taxonomy (product group) from LIQI_RAW_TB_PRODUCT_MAPPING.',
    ASSET_CLASS VARCHAR(40) COMMENT 'Level-1 LCR taxonomy (e.g. HQLA_L1_CASH, RETAIL_DEPOSITS, LOANS) from the product mapping.',
    LCR_TREATMENT VARCHAR(20) COMMENT 'How the position enters the LCR: HQLA, OUTFLOW, INFLOW or FACILITY.',
    HQLA_LEVEL VARCHAR(5) COMMENT 'HQLA level L1 / L2A / L2B for HQLA positions; NULL otherwise.',
    COUNTERPARTY_ID VARCHAR(50) COMMENT 'Counterparty / issuer of the position.',
    COUNTERPARTY_NAME VARCHAR(200) COMMENT 'Counterparty name shown in drill-downs and Top-10 driver lists.',
    COUNTERPARTY_SECTOR VARCHAR(30) COMMENT 'Rule sector used for the behavioral rule lookup (deposit counterparty type, contract sector, ALL for HQLA).',
    BUSINESS_UNIT VARCHAR(30) COMMENT 'Owning business unit for filtering (REQ-UI-02).',
    CURRENCY VARCHAR(3) COMMENT 'Original currency of the position (ISO 4217).',
    RATING VARCHAR(10) COMMENT 'Instrument / counterparty rating for rating filters.',
    SIM_CATEGORY VARCHAR(30) COMMENT 'Simulator component the position belongs to (e.g. RETAIL_STABLE, CORP_NON_OPERATIONAL, FI_FUNDING, FACILITIES, INFLOW_NONFIN, INFLOW_FI, L1_CASH, L2A). Target of what-if multipliers.',
    EXPOSURE_CHF NUMBER(18,2) COMMENT 'Exposure in CHF: market value (HQLA), balance (deposits) or drawn outstanding (contracts).',
    UNDRAWN_CHF NUMBER(18,2) COMMENT 'Undrawn committed facility amount in CHF (basis of drawdown outflows).',
    CF_IN_30D_CHF NUMBER(18,2) COMMENT 'Contractual cash inflows (principal + interest) due within 30 days in CHF.',
    CF_OUT_30D_CHF NUMBER(18,2) COMMENT 'Contractual cash outflows (e.g. interbank borrowing repayments) due within 30 days in CHF.',
    COMPONENT_GROSS_CHF NUMBER(18,2) COMMENT 'Gross amount the scenario factor is applied to: market value (HQLA), balance (deposits), 30-day outflow (interbank borrowing), undrawn amount (facilities), 30-day performing inflow (loans).',
    APPLIED_FACTOR NUMBER(8,4) COMMENT 'Scenario factor applied to COMPONENT_GROSS_CHF: haircut multiplier (HQLA, incl. stress add-on), run-off rate (deposits incl. account adjustments, clamped to 3%-100%), drawdown rate (facilities) or inflow rate (0 for non-performing).',
    HQLA_WEIGHTED_CHF NUMBER(18,2) COMMENT 'Post-haircut HQLA value in CHF before the L2 caps (numerator contribution).',
    OUTFLOW_CHF NUMBER(18,2) COMMENT 'Stressed 30-day outflow in CHF (denominator contribution).',
    INFLOW_CHF NUMBER(18,2) COMMENT 'Weighted 30-day inflow in CHF before the 75% inflow cap.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Position-level LCR contributions for every scenario (REQ-ENG-03/04/05): each valid in-scope position from REPP_AGG_DT_LCR_POSITIONS is combined with the scenario-specific behavioral rule (run-off, inflow, drawdown, haircut add-on) to produce weighted HQLA, outflow and inflow amounts. Single source for scenario components, variance attribution, drill-down and the simulator.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH cf30 AS (
    SELECT AS_OF_DATE, CONTRACT_ID,
           SUM(IFF(DIRECTION = 'IN', AMOUNT_CCY, 0)) AS IN_CCY,
           SUM(IFF(DIRECTION = 'OUT', AMOUNT_CCY, 0)) AS OUT_CCY
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_CASHFLOWS
    WHERE SNAPSHOT_SCOPE = 'DAILY' AND CF_DATE > AS_OF_DATE AND CF_DATE <= DATEADD(day, 30, AS_OF_DATE)
    GROUP BY AS_OF_DATE, CONTRACT_ID
),
base AS (
    SELECT p.*, s.SCENARIO_CODE, r.SIM_CATEGORY, r.RUNOFF_RATE, r.INFLOW_RATE, r.DRAWDOWN_RATE, r.HAIRCUT_ADDON_PCT,
           (COALESCE(c.IN_CCY, 0) * p.FX_RATE_TO_CHF) AS CF_IN_CHF,
           (COALESCE(c.OUT_CCY, 0) * p.FX_RATE_TO_CHF) AS CF_OUT_CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_POSITIONS p
    CROSS JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_SCENARIOS s
    JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LIQ_RULES r
        ON r.PRODUCT_TYPE = p.PRODUCT_TYPE AND r.COUNTERPARTY_SECTOR = p.COUNTERPARTY_SECTOR
       AND r.STRESS_SCENARIO = s.SCENARIO_CODE
    LEFT JOIN cf30 c
        ON c.AS_OF_DATE = p.AS_OF_DATE AND c.CONTRACT_ID = p.POSITION_ID AND p.SOURCE_SYSTEM = 'CONTRACTS'
    WHERE p.DQ_STATUS = 'OK' AND p.INCLUDED_IN_LCR
),
calc AS (
    SELECT b.*,
        CASE b.LCR_TREATMENT
            WHEN 'HQLA' THEN b.AMOUNT_CHF
            WHEN 'OUTFLOW' THEN IFF(b.SOURCE_SYSTEM = 'DEPOSITS', b.AMOUNT_CHF, b.CF_OUT_CHF)
            WHEN 'FACILITY' THEN b.UNDRAWN_CHF
            WHEN 'INFLOW' THEN IFF(b.IS_PERFORMING, b.CF_IN_CHF, 0)
        END AS GROSS_CHF,
        CASE b.LCR_TREATMENT
            WHEN 'HQLA' THEN GREATEST(0, b.HAIRCUT_FACTOR - COALESCE(b.HAIRCUT_ADDON_PCT, 0) / 100)
            WHEN 'OUTFLOW' THEN IFF(b.SOURCE_SYSTEM = 'DEPOSITS',
                                    GREATEST(0.03, LEAST(1, b.RUNOFF_RATE + b.RUNOFF_ADJUSTMENT)), b.RUNOFF_RATE)
            WHEN 'FACILITY' THEN b.DRAWDOWN_RATE
            WHEN 'INFLOW' THEN IFF(b.IS_PERFORMING, b.INFLOW_RATE, 0)
        END AS FACTOR
    FROM base b
)
SELECT
    AS_OF_DATE, SCENARIO_CODE, POSITION_ID, SOURCE_SYSTEM, PRODUCT_TYPE, POSITION_NAME, PRODUCT_GROUP, ASSET_CLASS,
    LCR_TREATMENT, HQLA_LEVEL, COUNTERPARTY_ID, COUNTERPARTY_NAME, COUNTERPARTY_SECTOR, BUSINESS_UNIT, CURRENCY, RATING,
    SIM_CATEGORY, AMOUNT_CHF, UNDRAWN_CHF, CF_IN_CHF::NUMBER(18,2), CF_OUT_CHF::NUMBER(18,2), GROSS_CHF::NUMBER(18,2),
    FACTOR::NUMBER(8,4),
    IFF(LCR_TREATMENT = 'HQLA', GROSS_CHF * FACTOR, 0)::NUMBER(18,2),
    IFF(LCR_TREATMENT IN ('OUTFLOW', 'FACILITY'), GROSS_CHF * FACTOR, 0)::NUMBER(18,2),
    IFF(LCR_TREATMENT = 'INFLOW', GROSS_CHF * FACTOR, 0)::NUMBER(18,2),
    CURRENT_TIMESTAMP()
FROM calc;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_COMPONENTS(
    AS_OF_DATE DATE COMMENT 'Snapshot (cut-off) date.',
    STRESS_SCENARIO VARCHAR(20) COMMENT 'Scenario code (BASE, BANK_STRESS, MARKET_STRESS, COMBINED).',
    COMPONENT_TYPE VARCHAR(10) COMMENT 'LCR side of the component: HQLA (numerator), OUTFLOW (incl. facility drawdowns) or INFLOW.',
    SIM_CATEGORY VARCHAR(30) COMMENT 'Simulator component category (e.g. L1_CASH, L2B_EQUITY, RETAIL_STABLE, FI_FUNDING, FACILITIES, INFLOW_FI).',
    HQLA_LEVEL VARCHAR(5) COMMENT 'HQLA level for HQLA components (L1, L2A, L2B); NULL for flows.',
    GROSS_AMOUNT_CHF NUMBER(18,2) COMMENT 'Sum of gross amounts the scenario factors apply to (market value, balance, undrawn, 30-day contractual flow).',
    WEIGHTED_AMOUNT_CHF NUMBER(18,2) COMMENT 'Sum of weighted amounts (post-haircut HQLA, stressed outflow or weighted inflow) before LCR caps.',
    POSITION_COUNT NUMBER(10,0) COMMENT 'Number of positions in the component.',
    COUNTERPARTY_COUNT NUMBER(10,0) COMMENT 'Number of distinct counterparties in the component.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Compact LCR component vectors per date and scenario (a few dozen rows per day). Input for REPP_AGG_DT_LCR_SCENARIO_DAILY, the scenario simulator procedure and the agent what-if tool, so what-if recalculations run in milliseconds without touching position data.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT AS_OF_DATE, STRESS_SCENARIO,
       IFF(LCR_TREATMENT = 'FACILITY', 'OUTFLOW', LCR_TREATMENT)::VARCHAR(10),
       SIM_CATEGORY, HQLA_LEVEL,
       SUM(COMPONENT_GROSS_CHF)::NUMBER(18,2),
       SUM(HQLA_WEIGHTED_CHF + OUTFLOW_CHF + INFLOW_CHF)::NUMBER(18,2),
       COUNT(*), COUNT(DISTINCT COUNTERPARTY_ID), CURRENT_TIMESTAMP()
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS
GROUP BY AS_OF_DATE, STRESS_SCENARIO, IFF(LCR_TREATMENT = 'FACILITY', 'OUTFLOW', LCR_TREATMENT), SIM_CATEGORY, HQLA_LEVEL;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY(
    AS_OF_DATE DATE COMMENT 'Snapshot (cut-off) date.',
    STRESS_SCENARIO VARCHAR(20) COMMENT 'Scenario code; BASE is the regulatory FINMA LCR.',
    SCENARIO_NAME VARCHAR(100) COMMENT 'Scenario display name.',
    IS_REGULATORY BOOLEAN COMMENT 'TRUE for the regulatory BASE scenario.',
    L1_TOTAL NUMBER(18,2) COMMENT 'Level 1 HQLA after haircut (0% regulatory; stress add-ons in stress scenarios).',
    L2A_TOTAL NUMBER(18,2) COMMENT 'Level 2A HQLA after 15% haircut (plus stress add-on) before caps.',
    L2B_TOTAL NUMBER(18,2) COMMENT 'Level 2B HQLA after 50% haircut (plus stress add-on) before caps.',
    L2B_CAPPED NUMBER(18,2) COMMENT 'Level 2B after the 15% cap: min(L2B, 15/85 x (L1 + L2A)).',
    L2_UNCAPPED NUMBER(18,2) COMMENT 'L2A + L2B before any cap.',
    L2_CAPPED NUMBER(18,2) COMMENT 'Level 2 after the 40% cap: min(L2A + L2B_CAPPED, 2/3 x L1).',
    HQLA_TOTAL NUMBER(18,2) COMMENT 'Stock of HQLA = L1 + L2_CAPPED (LCR numerator).',
    L2B_CAP_APPLIED BOOLEAN COMMENT 'TRUE when the 15% L2B cap reduced the stock.',
    L2_CAP_APPLIED BOOLEAN COMMENT 'TRUE when the 40% L2 cap reduced the stock.',
    OUTFLOW_RETAIL NUMBER(18,2) COMMENT 'Stressed 30-day outflows from retail deposits.',
    OUTFLOW_CORP NUMBER(18,2) COMMENT 'Stressed 30-day outflows from corporate / SME deposits.',
    OUTFLOW_FI NUMBER(18,2) COMMENT 'Stressed 30-day outflows from financial-institution deposits and interbank borrowing.',
    OUTFLOW_FACILITIES NUMBER(18,2) COMMENT 'Drawdown outflows on undrawn committed credit and liquidity facilities.',
    OUTFLOW_TOTAL NUMBER(18,2) COMMENT 'Total gross stressed 30-day outflows (retail + corporate + FI + facilities).',
    INFLOW_NONFIN NUMBER(18,2) COMMENT 'Weighted 30-day inflows from retail, SME and corporate loans.',
    INFLOW_FI NUMBER(18,2) COMMENT 'Weighted 30-day inflows from interbank placements.',
    INFLOW_TOTAL NUMBER(18,2) COMMENT 'Total weighted 30-day inflows before the cap.',
    INFLOW_CAPPED NUMBER(18,2) COMMENT 'Inflows after the 75% cap: min(INFLOW_TOTAL, 0.75 x OUTFLOW_TOTAL).',
    INFLOW_CAP_APPLIED BOOLEAN COMMENT 'TRUE when the 75% inflow cap was binding.',
    NET_OUTFLOW NUMBER(18,2) COMMENT 'Net cash outflows = OUTFLOW_TOTAL - INFLOW_CAPPED (LCR denominator).',
    LCR_RATIO NUMBER(8,2) COMMENT 'LCR in percent = HQLA_TOTAL / NET_OUTFLOW x 100 (9999.99 if no net outflows).',
    LCR_STATUS VARCHAR(10) COMMENT 'FAIL (< 100%), WARNING (100% to < 110%), PASS (>= 110%).',
    SEVERITY VARCHAR(10) COMMENT 'Dashboard colour for LCR_STATUS: RED, YELLOW, GREEN.',
    LCR_BUFFER_CHF NUMBER(18,2) COMMENT 'HQLA_TOTAL - NET_OUTFLOW: liquidity buffer above the 100% minimum.',
    RETAIL_ACCOUNTS NUMBER(10,0) COMMENT 'Number of retail deposit positions in the calculation.',
    CORP_ACCOUNTS NUMBER(10,0) COMMENT 'Number of corporate / SME deposit positions in the calculation.',
    FI_ACCOUNTS NUMBER(10,0) COMMENT 'Number of FI deposit and interbank borrowing positions in the calculation.',
    L1_COUNT NUMBER(10,0) COMMENT 'Number of Level 1 HQLA positions.',
    L2A_COUNT NUMBER(10,0) COMMENT 'Number of Level 2A HQLA positions.',
    L2B_COUNT NUMBER(10,0) COMMENT 'Number of Level 2B HQLA positions.',
    RETAIL_BALANCE_CHF NUMBER(18,2) COMMENT 'Gross retail deposit balance (basis for the effective retail run-off rate).',
    CORP_BALANCE_CHF NUMBER(18,2) COMMENT 'Gross corporate deposit balance (basis for the effective corporate run-off rate).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Daily LCR result per scenario (REQ-ENG-05): HQLA by level with both regulatory caps, outflows by segment, inflows with the 75% cap, net outflows, LCR ratio and status (FAIL < 100%, WARNING 100-110%, PASS >= 110%). All values computed with the shared REPP_AGG_FN_LCR_CALC formula. BASE rows feed the regulatory LCR dynamic tables.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH c AS (
    SELECT AS_OF_DATE, STRESS_SCENARIO,
        SUM(IFF(COMPONENT_TYPE = 'HQLA' AND HQLA_LEVEL = 'L1', WEIGHTED_AMOUNT_CHF, 0)) AS L1,
        SUM(IFF(COMPONENT_TYPE = 'HQLA' AND HQLA_LEVEL = 'L2A', WEIGHTED_AMOUNT_CHF, 0)) AS L2A,
        SUM(IFF(COMPONENT_TYPE = 'HQLA' AND HQLA_LEVEL = 'L2B', WEIGHTED_AMOUNT_CHF, 0)) AS L2B,
        SUM(IFF(SIM_CATEGORY IN ('RETAIL_STABLE', 'RETAIL_LESS_STABLE'), WEIGHTED_AMOUNT_CHF, 0)) AS O_RETAIL,
        SUM(IFF(SIM_CATEGORY IN ('CORP_OPERATIONAL', 'CORP_NON_OPERATIONAL'), WEIGHTED_AMOUNT_CHF, 0)) AS O_CORP,
        SUM(IFF(SIM_CATEGORY = 'FI_FUNDING', WEIGHTED_AMOUNT_CHF, 0)) AS O_FI,
        SUM(IFF(SIM_CATEGORY = 'FACILITIES', WEIGHTED_AMOUNT_CHF, 0)) AS O_FAC,
        SUM(IFF(COMPONENT_TYPE = 'OUTFLOW', WEIGHTED_AMOUNT_CHF, 0)) AS O_TOT,
        SUM(IFF(SIM_CATEGORY = 'INFLOW_NONFIN', WEIGHTED_AMOUNT_CHF, 0)) AS I_NONFIN,
        SUM(IFF(SIM_CATEGORY = 'INFLOW_FI', WEIGHTED_AMOUNT_CHF, 0)) AS I_FI,
        SUM(IFF(COMPONENT_TYPE = 'INFLOW', WEIGHTED_AMOUNT_CHF, 0)) AS I_TOT,
        SUM(IFF(SIM_CATEGORY IN ('RETAIL_STABLE', 'RETAIL_LESS_STABLE'), POSITION_COUNT, 0)) AS N_RETAIL,
        SUM(IFF(SIM_CATEGORY IN ('CORP_OPERATIONAL', 'CORP_NON_OPERATIONAL'), POSITION_COUNT, 0)) AS N_CORP,
        SUM(IFF(SIM_CATEGORY = 'FI_FUNDING', POSITION_COUNT, 0)) AS N_FI,
        SUM(IFF(HQLA_LEVEL = 'L1', POSITION_COUNT, 0)) AS N_L1,
        SUM(IFF(HQLA_LEVEL = 'L2A', POSITION_COUNT, 0)) AS N_L2A,
        SUM(IFF(HQLA_LEVEL = 'L2B', POSITION_COUNT, 0)) AS N_L2B,
        SUM(IFF(SIM_CATEGORY IN ('RETAIL_STABLE', 'RETAIL_LESS_STABLE'), GROSS_AMOUNT_CHF, 0)) AS B_RETAIL,
        SUM(IFF(SIM_CATEGORY IN ('CORP_OPERATIONAL', 'CORP_NON_OPERATIONAL'), GROSS_AMOUNT_CHF, 0)) AS B_CORP
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_COMPONENTS
    GROUP BY AS_OF_DATE, STRESS_SCENARIO
),
r AS (
    SELECT c.*, {{ db }}.{{ rep_agg }}.REPP_AGG_FN_LCR_CALC(L1, L2A, L2B, O_TOT, I_TOT, 0.75) AS RES
    FROM c
)
SELECT
    r.AS_OF_DATE, r.STRESS_SCENARIO, s.SCENARIO_NAME, s.IS_REGULATORY,
    r.L1::NUMBER(18,2), r.L2A::NUMBER(18,2), r.L2B::NUMBER(18,2),
    RES:L2B_CAPPED::NUMBER(18,2), (r.L2A + r.L2B)::NUMBER(18,2), RES:L2_CAPPED::NUMBER(18,2), RES:HQLA::NUMBER(18,2),
    RES:L2B_CAP_APPLIED::BOOLEAN, RES:L2_CAP_APPLIED::BOOLEAN,
    r.O_RETAIL::NUMBER(18,2), r.O_CORP::NUMBER(18,2), r.O_FI::NUMBER(18,2), r.O_FAC::NUMBER(18,2), r.O_TOT::NUMBER(18,2),
    r.I_NONFIN::NUMBER(18,2), r.I_FI::NUMBER(18,2), r.I_TOT::NUMBER(18,2),
    RES:INFLOW_CAPPED::NUMBER(18,2), RES:INFLOW_CAP_APPLIED::BOOLEAN, RES:NET_OUTFLOW::NUMBER(18,2),
    COALESCE(RES:LCR::NUMBER(8,2), 9999.99) AS LCR_RATIO_V,
    CASE WHEN LCR_RATIO_V < 100 THEN 'FAIL' WHEN LCR_RATIO_V < 110 THEN 'WARNING' ELSE 'PASS' END,
    CASE WHEN LCR_RATIO_V < 100 THEN 'RED' WHEN LCR_RATIO_V < 110 THEN 'YELLOW' ELSE 'GREEN' END,
    (RES:HQLA - RES:NET_OUTFLOW)::NUMBER(18,2),
    r.N_RETAIL, r.N_CORP, r.N_FI, r.N_L1, r.N_L2A, r.N_L2B,
    r.B_RETAIL::NUMBER(18,2), r.B_CORP::NUMBER(18,2),
    CURRENT_TIMESTAMP()
FROM r
JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_SCENARIOS s ON s.SCENARIO_CODE = r.STRESS_SCENARIO;

-- ============================================================
-- Existing LCR dynamic tables, rebuilt on the BASE scenario.
-- Column lists unchanged; new columns appended at the end.
-- ============================================================

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_HQLA(
    AS_OF_DATE DATE COMMENT 'Reporting date for HQLA positions (daily COB snapshot). Primary time dimension for portfolio composition and asset allocation trends.',
    HQLA_LEVEL VARCHAR(10) COMMENT 'Basel III HQLA level: L1 (0% haircut), L2A (15% haircut), L2B (50% haircut). Primary grouping for regulatory reporting and cap monitoring.',
    ASSET_TYPE VARCHAR(50) COMMENT 'HQLA asset type code (e.g. CASH_SNB, GOVT_BOND_CHF, CANTON_BOND, CORPORATE_BOND_AA, CORPORATE_BOND_A_BBB, EQUITY_SMI). Links to LIQI_RAW_TB_HQLA_ELIGIBILITY.',
    HOLDINGS_COUNT NUMBER(10,0) COMMENT 'Number of eligible positions of this asset type on the date; diversification / concentration indicator.',
    MARKET_VALUE_CHF NUMBER(18,2) COMMENT 'Gross market value in CHF (end-of-day FX) before regulatory haircuts.',
    HAIRCUT_PCT NUMBER(5,2) COMMENT 'Regulatory haircut percentage of the asset type (0, 15 or 50) from LIQI_RAW_TB_HQLA_ELIGIBILITY.',
    HQLA_VALUE_CHF NUMBER(18,2) COMMENT 'HQLA value after the regulatory haircut (Market_Value x Haircut_Factor), before the L2B and L2 caps.',
    PCT_OF_TOTAL_HQLA NUMBER(5,2) COMMENT 'Share of this asset type in total post-haircut (pre-cap) HQLA in percent; concentration indicator.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh for lineage and freshness monitoring.'
) COMMENT = 'Daily regulatory (BASE scenario) HQLA holdings aggregated by HQLA level and asset type: holding counts, gross market value, haircut and post-haircut value. Built from REPP_AGG_DT_LCR_SCENARIO_POSITIONS so the composition always matches the LCR numerator. Used by semantic views, composition dashboards and the liquidity agent.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH agg AS (
    SELECT p.AS_OF_DATE, p.HQLA_LEVEL, p.PRODUCT_TYPE AS ASSET_TYPE, COUNT(*) AS HOLDINGS_COUNT,
           SUM(p.EXPOSURE_CHF) AS MARKET_VALUE_CHF, SUM(p.HQLA_WEIGHTED_CHF) AS HQLA_VALUE_CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS p
    WHERE p.STRESS_SCENARIO = 'BASE' AND p.LCR_TREATMENT = 'HQLA'
    GROUP BY p.AS_OF_DATE, p.HQLA_LEVEL, p.PRODUCT_TYPE
)
SELECT a.AS_OF_DATE, a.HQLA_LEVEL::VARCHAR(10), a.ASSET_TYPE, a.HOLDINGS_COUNT,
       ROUND(a.MARKET_VALUE_CHF, 2), e.HAIRCUT_PCT, ROUND(a.HQLA_VALUE_CHF, 2),
       ROUND(a.HQLA_VALUE_CHF / NULLIF(SUM(a.HQLA_VALUE_CHF) OVER (PARTITION BY a.AS_OF_DATE), 0) * 100, 2),
       CURRENT_TIMESTAMP()
FROM agg a
LEFT JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY e ON e.ASSET_TYPE = a.ASSET_TYPE;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW(
    AS_OF_DATE DATE COMMENT 'Reporting date (daily COB snapshot). Primary time dimension for funding composition and run-off trends.',
    COUNTERPARTY_TYPE VARCHAR(50) COMMENT 'Outflow segment: RETAIL, CORPORATE (incl. SME), FINANCIAL_INSTITUTION (FI deposits and interbank borrowing) or COMMITTED_FACILITIES (undrawn credit / liquidity facility drawdowns).',
    ACCOUNT_COUNT NUMBER(10,0) COMMENT 'Number of positions (deposit accounts, borrowings or facilities) in the segment.',
    CUSTOMER_COUNT NUMBER(10,0) COMMENT 'Number of distinct counterparties in the segment.',
    BALANCE_CHF NUMBER(18,2) COMMENT 'Gross amount the run-off applies to in CHF: deposit balances, 30-day borrowing repayments or undrawn facility amounts.',
    RUN_OFF_RATE NUMBER(8,4) COMMENT 'Weighted effective run-off / drawdown rate (decimal) = OUTFLOW_AMOUNT_CHF / BALANCE_CHF, after account-level adjustments.',
    OUTFLOW_AMOUNT_CHF NUMBER(18,2) COMMENT 'Stressed 30-day outflow of the segment in CHF (regulatory BASE scenario).',
    PCT_OF_TOTAL_OUTFLOWS NUMBER(5,2) COMMENT 'Share of the segment in total gross outflows in percent.',
    PCT_OF_TOTAL_DEPOSITS NUMBER(5,2) COMMENT 'Share of the segment in the total gross amount (BALANCE_CHF) in percent.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Daily regulatory (BASE scenario) stressed outflows by segment: retail, corporate, financial institutions (deposits + interbank borrowing) and committed facilities. Gross amounts, effective run-off rates, outflows and shares. Built from REPP_AGG_DT_LCR_SCENARIO_POSITIONS so totals equal OUTFLOW_TOTAL of REPP_AGG_DT_LCR_DAILY.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH seg AS (
    SELECT p.AS_OF_DATE,
           CASE WHEN p.LCR_TREATMENT = 'FACILITY' THEN 'COMMITTED_FACILITIES'
                WHEN p.SIM_CATEGORY = 'FI_FUNDING' THEN 'FINANCIAL_INSTITUTION'
                WHEN p.SIM_CATEGORY LIKE 'RETAIL%' THEN 'RETAIL'
                ELSE 'CORPORATE' END AS COUNTERPARTY_TYPE,
           COUNT(*) AS ACCOUNT_COUNT, COUNT(DISTINCT p.COUNTERPARTY_ID) AS CUSTOMER_COUNT,
           SUM(p.COMPONENT_GROSS_CHF) AS BALANCE_CHF, SUM(p.OUTFLOW_CHF) AS OUTFLOW_AMOUNT_CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_POSITIONS p
    WHERE p.STRESS_SCENARIO = 'BASE' AND p.LCR_TREATMENT IN ('OUTFLOW', 'FACILITY')
    GROUP BY 1, 2
)
SELECT AS_OF_DATE, COUNTERPARTY_TYPE, ACCOUNT_COUNT, CUSTOMER_COUNT, ROUND(BALANCE_CHF, 2),
       ROUND(OUTFLOW_AMOUNT_CHF / NULLIF(BALANCE_CHF, 0), 4), ROUND(OUTFLOW_AMOUNT_CHF, 2),
       ROUND(OUTFLOW_AMOUNT_CHF / NULLIF(SUM(OUTFLOW_AMOUNT_CHF) OVER (PARTITION BY AS_OF_DATE), 0) * 100, 2),
       ROUND(BALANCE_CHF / NULLIF(SUM(BALANCE_CHF) OVER (PARTITION BY AS_OF_DATE), 0) * 100, 2),
       CURRENT_TIMESTAMP()
FROM seg;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_HQLA_CALCULATION(
    AS_OF_DATE DATE COMMENT 'Reporting date (daily COB snapshot) of the HQLA stock calculation.',
    L1_TOTAL NUMBER(18,2) COMMENT 'Level 1 HQLA in CHF (0% haircut): SNB reserves, vault cash, Swiss Confederation and 0%-risk-weight foreign government bonds.',
    L2A_TOTAL NUMBER(18,2) COMMENT 'Level 2A HQLA in CHF after 15% haircut: canton bonds, covered bonds (Pfandbriefe), AA- or better corporate bonds.',
    L2B_TOTAL NUMBER(18,2) COMMENT 'Level 2B HQLA in CHF after 50% haircut, before the 15% cap: SMI equities and A+/BBB- corporate bonds.',
    L2_UNCAPPED NUMBER(18,2) COMMENT 'L2A + L2B before any regulatory cap.',
    L2_CAPPED NUMBER(18,2) COMMENT 'Level 2 included in HQLA after the L2B 15% cap and the L2 40% cap (max 2/3 of L1).',
    HQLA_TOTAL NUMBER(18,2) COMMENT 'Stock of HQLA = L1_TOTAL + L2_CAPPED (LCR numerator).',
    L1_COUNT NUMBER(10,0) COMMENT 'Number of Level 1 positions.',
    L2A_COUNT NUMBER(10,0) COMMENT 'Number of Level 2A positions.',
    L2B_COUNT NUMBER(10,0) COMMENT 'Number of Level 2B positions.',
    TOTAL_HOLDINGS NUMBER(10,0) COMMENT 'Total number of eligible HQLA positions.',
    CAP_APPLIED BOOLEAN COMMENT 'TRUE when either the L2B 15% cap or the L2 40% cap reduced the HQLA stock.',
    DISCARDED_L2 NUMBER(18,2) COMMENT 'Level 2 assets excluded by the caps (L2_UNCAPPED - L2_CAPPED).',
    MAX_L2_ALLOWED NUMBER(18,2) COMMENT 'Maximum Level 2 allowed under the 40% cap: 2/3 x L1_TOTAL.',
    L2_TO_L1_PCT NUMBER(8,2) COMMENT 'L2_CAPPED / L1_TOTAL in percent (regulatory maximum 66.67%).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.',
    L2B_CAPPED NUMBER(18,2) COMMENT 'Level 2B after the 15% cap: min(L2B, 15/85 x (L1 + L2A)).',
    L2B_CAP_APPLIED BOOLEAN COMMENT 'TRUE when the 15% L2B cap was binding.',
    L2B_DISCARDED NUMBER(18,2) COMMENT 'Level 2B excluded by the 15% cap (L2B_TOTAL - L2B_CAPPED).'
) COMMENT = 'Daily regulatory (BASE) HQLA stock with both Basel III / FINMA caps: L2B limited to 15/85 of (L1 + L2A) and L2 limited to 2/3 of L1. LCR numerator. Built from REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT AS_OF_DATE, L1_TOTAL, L2A_TOTAL, L2B_TOTAL, L2_UNCAPPED, L2_CAPPED, HQLA_TOTAL,
       L1_COUNT, L2A_COUNT, L2B_COUNT, L1_COUNT + L2A_COUNT + L2B_COUNT,
       (L2B_CAP_APPLIED OR L2_CAP_APPLIED), (L2_UNCAPPED - L2_CAPPED)::NUMBER(18,2),
       (2.0 / 3.0 * L1_TOTAL)::NUMBER(18,2), ROUND(L2_CAPPED / NULLIF(L1_TOTAL, 0) * 100, 2),
       CURRENT_TIMESTAMP(), L2B_CAPPED, L2B_CAP_APPLIED, (L2B_TOTAL - L2B_CAPPED)::NUMBER(18,2)
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY
WHERE STRESS_SCENARIO = 'BASE';

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW_CALCULATION(
    AS_OF_DATE DATE COMMENT 'Reporting date (daily COB snapshot) of the net cash outflow calculation.',
    OUTFLOW_RETAIL NUMBER(18,2) COMMENT 'Stressed 30-day retail deposit outflows (3-10% base rates with relationship discounts and tenure penalty).',
    OUTFLOW_CORP NUMBER(18,2) COMMENT 'Stressed 30-day corporate / SME deposit outflows (25% operational, 40% non-operational).',
    OUTFLOW_FI NUMBER(18,2) COMMENT 'Stressed 30-day outflows from FI deposits and interbank borrowing maturing within 30 days (100%).',
    OUTFLOW_TOTAL NUMBER(18,2) COMMENT 'Total gross stressed 30-day outflows incl. facility drawdowns (OUTFLOW_RETAIL + OUTFLOW_CORP + OUTFLOW_FI + OUTFLOW_FACILITIES). The LCR denominator is NET_OUTFLOW.',
    RETAIL_ACCOUNTS NUMBER(10,0) COMMENT 'Number of active retail deposit accounts.',
    CORP_ACCOUNTS NUMBER(10,0) COMMENT 'Number of active corporate / SME deposit accounts.',
    FI_ACCOUNTS NUMBER(10,0) COMMENT 'Number of FI deposit and interbank borrowing positions.',
    TOTAL_ACCOUNTS NUMBER(10,0) COMMENT 'Total number of deposit and borrowing positions.',
    RETAIL_EFFECTIVE_RUN_OFF_PCT NUMBER(8,2) COMMENT 'Effective retail run-off rate in percent (OUTFLOW_RETAIL / retail balance).',
    CORP_EFFECTIVE_RUN_OFF_PCT NUMBER(8,2) COMMENT 'Effective corporate run-off rate in percent (OUTFLOW_CORP / corporate balance).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.',
    OUTFLOW_FACILITIES NUMBER(18,2) COMMENT 'Drawdown outflows on undrawn committed credit and liquidity facilities.',
    INFLOW_TOTAL NUMBER(18,2) COMMENT 'Weighted 30-day contractual inflows before the 75% cap (50% non-financial, 100% financial, 0% non-performing).',
    INFLOW_CAPPED NUMBER(18,2) COMMENT 'Inflows after the 75% cap: min(INFLOW_TOTAL, 0.75 x OUTFLOW_TOTAL).',
    NET_OUTFLOW NUMBER(18,2) COMMENT 'Net cash outflows = OUTFLOW_TOTAL - INFLOW_CAPPED (LCR denominator).'
) COMMENT = 'Daily regulatory (BASE) net cash outflow calculation: stressed outflows by segment incl. facility drawdowns, weighted contractual inflows with the 75% cap, and net outflows (LCR denominator). Built from REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT AS_OF_DATE, OUTFLOW_RETAIL, OUTFLOW_CORP, OUTFLOW_FI, OUTFLOW_TOTAL,
       RETAIL_ACCOUNTS, CORP_ACCOUNTS, FI_ACCOUNTS, RETAIL_ACCOUNTS + CORP_ACCOUNTS + FI_ACCOUNTS,
       ROUND(OUTFLOW_RETAIL / NULLIF(RETAIL_BALANCE_CHF, 0) * 100, 2),
       ROUND(OUTFLOW_CORP / NULLIF(CORP_BALANCE_CHF, 0) * 100, 2),
       CURRENT_TIMESTAMP(), OUTFLOW_FACILITIES, INFLOW_TOTAL, INFLOW_CAPPED, NET_OUTFLOW
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY
WHERE STRESS_SCENARIO = 'BASE';

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL (
    AS_OF_DATE COMMENT 'Reporting (cut-off) date of the record (daily COB snapshot).',
    HOLDING_ID COMMENT 'Persistent HQLA position identifier from the treasury system.',
    ASSET_TYPE COMMENT 'HQLA asset type code (e.g. CASH_SNB, GOVT_BOND_CHF, CANTON_BOND, EQUITY_SMI).',
    ASSET_NAME COMMENT 'Descriptive name of the HQLA asset type.',
    REGULATORY_LEVEL COMMENT 'Basel III HQLA level (L1, L2A, L2B) from LIQI_RAW_TB_HQLA_ELIGIBILITY.',
    ISIN COMMENT 'ISO 6166 security identifier; NULL for cash positions.',
    SECURITY_NAME COMMENT 'Descriptive name of the security or cash balance.',
    CURRENCY COMMENT 'Original currency of the position (ISO 4217).',
    QUANTITY COMMENT 'Number of shares held (equities only).',
    MARKET_VALUE_CCY COMMENT 'Market value in original currency at COB prices.',
    MARKET_VALUE_CHF COMMENT 'Gross market value in CHF before regulatory haircuts.',
    HAIRCUT_PCT COMMENT 'Regulatory haircut in percent (0 for L1, 15 for L2A, 50 for L2B).',
    HAIRCUT_FACTOR COMMENT 'Haircut multiplier applied to market value (1.00, 0.85, 0.50).',
    WEIGHTED_VALUE_CHF COMMENT 'Post-haircut value in CHF (market value x haircut factor), before caps.',
    MATURITY_DATE COMMENT 'Final maturity date of the security; NULL for cash and equities.',
    CREDIT_RATING COMMENT 'Credit rating of the security (S&P scale).',
    SMI_CONSTITUENT COMMENT 'TRUE when the equity is a Swiss Market Index constituent (L2B eligibility).',
    HQLA_ELIGIBLE COMMENT 'TRUE when the holding passes all FINMA eligibility criteria and counts as HQLA.',
    INELIGIBILITY_REASON COMMENT 'Reason why the holding is excluded from HQLA (e.g. rating downgrade).',
    PORTFOLIO_CODE COMMENT 'Treasury portfolio of the position (TREASURY_LIQ, ALM_BUFFER, TREASURY_INV).',
    CUSTODIAN COMMENT 'Institution holding the security (SNB, SIX SIS, Euroclear, Clearstream).',
    SNB_COORDINATE COMMENT 'SNB LCR reporting template coordinate for regulatory submission mapping.'
)
    COMMENT = 'Security-level detail view of HQLA holdings with regulatory level classification and haircut calculations. Provides drill-down capability from aggregate HQLA metrics to individual securities for portfolio analysis, concentration risk monitoring, and regulatory reporting. Includes market values, weighted values after haircuts, credit ratings, and SNB coordinate mappings. Used by Treasury for portfolio rebalancing decisions and by Compliance for regulatory submission preparation.'
    AS
SELECT
    h.AS_OF_DATE,
    h.HOLDING_ID,
    h.ASSET_TYPE,
    e.ASSET_NAME,
    e.REGULATORY_LEVEL,
    h.ISIN,
    h.SECURITY_NAME,
    h.CURRENCY,
    h.QUANTITY,
    h.MARKET_VALUE_CCY,
    h.MARKET_VALUE_CHF,
    e.HAIRCUT_PCT,
    e.HAIRCUT_FACTOR,
    h.MARKET_VALUE_CHF * e.HAIRCUT_FACTOR AS WEIGHTED_VALUE_CHF,
    h.MATURITY_DATE,
    h.CREDIT_RATING,
    h.SMI_CONSTITUENT,
    h.HQLA_ELIGIBLE,
    h.INELIGIBILITY_REASON,
    h.PORTFOLIO_CODE,
    h.CUSTODIAN,
    e.SNB_COORDINATE
FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS h
INNER JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY e
    ON h.ASSET_TYPE = e.ASSET_TYPE
WHERE e.IS_ACTIVE = TRUE AND COALESCE(h.SNAPSHOT_SCOPE, 'DAILY') = 'DAILY'
ORDER BY h.AS_OF_DATE DESC, h.MARKET_VALUE_CHF DESC;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL (
    AS_OF_DATE COMMENT 'Reporting (cut-off) date of the record (daily COB snapshot).',
    ACCOUNT_ID COMMENT 'Deposit account identifier (links to core banking accounts).',
    CUSTOMER_ID COMMENT 'Customer / counterparty identifier of the deposit (links to CRM).',
    DEPOSIT_TYPE COMMENT 'Deposit run-off class (e.g. RETAIL_STABLE_INSURED, CORPORATE_OPERATIONAL).',
    DEPOSIT_NAME COMMENT 'Descriptive name of the deposit run-off class.',
    COUNTERPARTY_TYPE COMMENT 'Outflow segment / counterparty type (RETAIL, CORPORATE, FINANCIAL_INSTITUTION, COMMITTED_FACILITIES).',
    CUSTOMER_SEGMENT COMMENT 'Customer segment of the depositor (MASS, AFFLUENT, PRIVATE, CORPORATE).',
    CURRENCY COMMENT 'Original currency of the position (ISO 4217).',
    BALANCE_CCY COMMENT 'Deposit balance in original currency.',
    BALANCE_CHF COMMENT 'Deposit balance converted to CHF.',
    BASE_RUN_OFF_RATE COMMENT 'Base regulatory run-off rate of the deposit type (decimal).',
    DISCOUNT_STEP1 COMMENT 'Run-off rate after the -2pp discount for 3+ products (where allowed).',
    DISCOUNT_STEP2 COMMENT 'Run-off rate after the additional -1pp direct-debit discount (where allowed).',
    PENALTY_APPLIED COMMENT 'Run-off rate after the +5pp penalty for accounts younger than 18 months.',
    FINAL_RUN_OFF_RATE COMMENT 'Final run-off rate after adjustments, clamped to 3%-100% (decimal).',
    OUTFLOW_AMOUNT_CHF COMMENT 'Stressed 30-day outflow in CHF (balance x run-off rate).',
    IS_INSURED COMMENT 'TRUE when the balance is within the CHF 100k deposit insurance limit.',
    PRODUCT_COUNT COMMENT 'Number of products the customer holds (3+ qualifies for a run-off discount).',
    ACCOUNT_TENURE_DAYS COMMENT 'Days since account opening (under 540 days adds a +5pp penalty).',
    HAS_DIRECT_DEBIT COMMENT 'TRUE when the account has a direct-debit mandate (-1pp discount).',
    IS_OPERATIONAL COMMENT 'TRUE for operational deposits (payroll, clearing) with lower run-off.',
    ACCOUNT_STATUS COMMENT 'Account status; only ACTIVE accounts enter the LCR.',
    SNB_COORDINATE COMMENT 'SNB LCR reporting template coordinate for regulatory submission mapping.'
)
    COMMENT = 'Account-level detail view of deposit balances with run-off rate calculations and relationship-based discount logic. Provides drill-down capability from aggregate outflow metrics to individual accounts for funding stability analysis, customer relationship quality assessment, and deposit retention strategy optimization. Includes base run-off rates, relationship discounts (product count, direct debit), tenure penalties, and final calculated outflow amounts. Used by Treasury for funding planning and by Retail Banking for customer relationship management.'
    AS
SELECT
    d.AS_OF_DATE,
    d.ACCOUNT_ID,
    d.CUSTOMER_ID,
    d.DEPOSIT_TYPE,
    dt.DEPOSIT_NAME,
    d.COUNTERPARTY_TYPE,
    d.CUSTOMER_SEGMENT,
    d.CURRENCY,
    d.BALANCE_CCY,
    d.BALANCE_CHF,
    dt.BASE_RUN_OFF_RATE,
    CASE
        WHEN dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.PRODUCT_COUNT >= 3
        THEN dt.BASE_RUN_OFF_RATE - 0.02
        ELSE dt.BASE_RUN_OFF_RATE
    END AS DISCOUNT_STEP1,
    CASE
        WHEN dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.HAS_DIRECT_DEBIT
        THEN DISCOUNT_STEP1 - 0.01
        ELSE DISCOUNT_STEP1
    END AS DISCOUNT_STEP2,
    CASE
        WHEN d.ACCOUNT_TENURE_DAYS < (18 * 30)
        THEN DISCOUNT_STEP2 + 0.05
        ELSE DISCOUNT_STEP2
    END AS PENALTY_APPLIED,
    GREATEST(0.03, LEAST(1.00, PENALTY_APPLIED)) AS FINAL_RUN_OFF_RATE,
    d.BALANCE_CHF * FINAL_RUN_OFF_RATE AS OUTFLOW_AMOUNT_CHF,
    d.IS_INSURED,
    d.PRODUCT_COUNT,
    d.ACCOUNT_TENURE_DAYS,
    d.HAS_DIRECT_DEBIT,
    d.IS_OPERATIONAL,
    d.ACCOUNT_STATUS,
    dt.SNB_COORDINATE
FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES d
INNER JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES dt
    ON d.DEPOSIT_TYPE = dt.DEPOSIT_TYPE
WHERE dt.IS_ACTIVE = TRUE AND COALESCE(d.SNAPSHOT_SCOPE, 'DAILY') = 'DAILY'
ORDER BY d.AS_OF_DATE DESC, d.BALANCE_CHF DESC;
