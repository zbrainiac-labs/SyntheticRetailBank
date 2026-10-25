/*
 * 363_LIQA_simulation.sql
 * LCR engine - part 4: what-if scenario simulator (REQ-ENG-05, Simulator tab, agent what-if tool).
 * Works on REPP_AGG_DT_LCR_SCENARIO_COMPONENTS (a few dozen rows per day), so every call
 * returns in well under a second and uses the shared formula REPP_AGG_FN_LCR_CALC.
 */
DEFINE PROCEDURE {{ db }}.{{ rep_agg }}.REPP_AGG_SP_LCR_SIMULATE(P_AS_OF_DATE VARCHAR, P_PARAMS VARCHAR)
RETURNS TABLE (
    STEP_ORDER NUMBER, STEP_NAME VARCHAR, AS_OF_DATE DATE, PRESET VARCHAR,
    LCR_RATIO FLOAT, DELTA_PP FLOAT, HQLA_TOTAL FLOAT, OUTFLOW_TOTAL FLOAT, INFLOW_TOTAL FLOAT,
    INFLOW_CAPPED FLOAT, NET_OUTFLOW FLOAT, LCR_STATUS VARCHAR, BREAKEVEN_WITHDRAWAL_CHF FLOAT
)
LANGUAGE SQL
COMMENT = 'What-if LCR simulator. P_AS_OF_DATE (YYYY-MM-DD, empty = latest) and P_PARAMS (JSON) with optional keys: preset (BASE | BANK_STRESS | MARKET_STRESS | COMBINED, default BASE), retail_stable_mult, retail_less_stable_mult, corp_operational_mult, corp_non_operational_mult, fi_mult, facilities_mult, inflow_mult (multipliers on weighted flows, capped at 100% of gross), haircut_addon_l1_pp, haircut_addon_l2a_pp, haircut_addon_l2b_pp (extra haircut in percentage points), price_shock_pct (market value shock on securities), withdrawal_chf (corporate non-operational withdrawal settled from SNB cash), inflow_cap_pct (default 0.75). Returns a cumulative waterfall (one row per lever) and, on the final row, the additional corporate withdrawal that brings the LCR to 100% (breakeven; funded from SNB cash, then by liquidating L1 bonds; resolution 0.05% of the L1 stock).'
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH prm AS (
            SELECT PARSE_JSON(COALESCE(NULLIF(:P_PARAMS, ''), '{}')) AS J
        ),
        p AS (
            SELECT
                UPPER(COALESCE(J:preset::STRING, 'BASE')) AS PRESET,
                COALESCE(J:retail_stable_mult::FLOAT, 1) AS M_RS,
                COALESCE(J:retail_less_stable_mult::FLOAT, 1) AS M_RLS,
                COALESCE(J:corp_operational_mult::FLOAT, 1) AS M_CO,
                COALESCE(J:corp_non_operational_mult::FLOAT, 1) AS M_CN,
                COALESCE(J:fi_mult::FLOAT, 1) AS M_FI,
                COALESCE(J:facilities_mult::FLOAT, 1) AS M_FAC,
                COALESCE(J:inflow_mult::FLOAT, 1) AS M_IN,
                COALESCE(J:haircut_addon_l1_pp::FLOAT, 0) / 100 AS A_L1,
                COALESCE(J:haircut_addon_l2a_pp::FLOAT, 0) / 100 AS A_L2A,
                COALESCE(J:haircut_addon_l2b_pp::FLOAT, 0) / 100 AS A_L2B,
                COALESCE(J:price_shock_pct::FLOAT, 0) / 100 AS SHOCK,
                COALESCE(J:withdrawal_chf::FLOAT, 0) AS WD,
                COALESCE(J:inflow_cap_pct::FLOAT, 0.75) AS CAP
            FROM prm
        ),
        d AS (
            SELECT COALESCE(TRY_TO_DATE(NULLIF(:P_AS_OF_DATE, '')),
                            (SELECT MAX(AS_OF_DATE) FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_COMPONENTS)) AS DT
        ),
        c AS (
            SELECT
                SUM(IFF(SIM_CATEGORY = 'L1_CASH', GROSS_AMOUNT_CHF, 0)) AS G_L1C, SUM(IFF(SIM_CATEGORY = 'L1_CASH', WEIGHTED_AMOUNT_CHF, 0)) AS W_L1C,
                SUM(IFF(SIM_CATEGORY = 'L1_BOND', GROSS_AMOUNT_CHF, 0)) AS G_L1B, SUM(IFF(SIM_CATEGORY = 'L1_BOND', WEIGHTED_AMOUNT_CHF, 0)) AS W_L1B,
                SUM(IFF(SIM_CATEGORY = 'L2A', GROSS_AMOUNT_CHF, 0)) AS G_L2A, SUM(IFF(SIM_CATEGORY = 'L2A', WEIGHTED_AMOUNT_CHF, 0)) AS W_L2A,
                SUM(IFF(SIM_CATEGORY = 'L2B_BOND', GROSS_AMOUNT_CHF, 0)) AS G_L2BB, SUM(IFF(SIM_CATEGORY = 'L2B_BOND', WEIGHTED_AMOUNT_CHF, 0)) AS W_L2BB,
                SUM(IFF(SIM_CATEGORY = 'L2B_EQUITY', GROSS_AMOUNT_CHF, 0)) AS G_L2BE, SUM(IFF(SIM_CATEGORY = 'L2B_EQUITY', WEIGHTED_AMOUNT_CHF, 0)) AS W_L2BE,
                SUM(IFF(SIM_CATEGORY = 'RETAIL_STABLE', GROSS_AMOUNT_CHF, 0)) AS G_RS, SUM(IFF(SIM_CATEGORY = 'RETAIL_STABLE', WEIGHTED_AMOUNT_CHF, 0)) AS W_RS,
                SUM(IFF(SIM_CATEGORY = 'RETAIL_LESS_STABLE', GROSS_AMOUNT_CHF, 0)) AS G_RLS, SUM(IFF(SIM_CATEGORY = 'RETAIL_LESS_STABLE', WEIGHTED_AMOUNT_CHF, 0)) AS W_RLS,
                SUM(IFF(SIM_CATEGORY = 'CORP_OPERATIONAL', GROSS_AMOUNT_CHF, 0)) AS G_CO, SUM(IFF(SIM_CATEGORY = 'CORP_OPERATIONAL', WEIGHTED_AMOUNT_CHF, 0)) AS W_CO,
                SUM(IFF(SIM_CATEGORY = 'CORP_NON_OPERATIONAL', GROSS_AMOUNT_CHF, 0)) AS G_CN, SUM(IFF(SIM_CATEGORY = 'CORP_NON_OPERATIONAL', WEIGHTED_AMOUNT_CHF, 0)) AS W_CN,
                SUM(IFF(SIM_CATEGORY = 'FI_FUNDING', GROSS_AMOUNT_CHF, 0)) AS G_FI, SUM(IFF(SIM_CATEGORY = 'FI_FUNDING', WEIGHTED_AMOUNT_CHF, 0)) AS W_FI,
                SUM(IFF(SIM_CATEGORY = 'FACILITIES', GROSS_AMOUNT_CHF, 0)) AS G_FAC, SUM(IFF(SIM_CATEGORY = 'FACILITIES', WEIGHTED_AMOUNT_CHF, 0)) AS W_FAC,
                SUM(IFF(SIM_CATEGORY = 'INFLOW_NONFIN', GROSS_AMOUNT_CHF, 0)) AS G_INF, SUM(IFF(SIM_CATEGORY = 'INFLOW_NONFIN', WEIGHTED_AMOUNT_CHF, 0)) AS W_INF,
                SUM(IFF(SIM_CATEGORY = 'INFLOW_FI', GROSS_AMOUNT_CHF, 0)) AS G_IFI, SUM(IFF(SIM_CATEGORY = 'INFLOW_FI', WEIGHTED_AMOUNT_CHF, 0)) AS W_IFI
            FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_COMPONENTS, d, p
            WHERE AS_OF_DATE = d.DT AND STRESS_SCENARIO = p.PRESET
        ),
        steps AS (
            SELECT column1 AS K, column2 AS STEP_NAME, 0::FLOAT AS EXTRA FROM VALUES
                (0, 'Baseline'), (1, '+ Deposit run-off multipliers'), (2, '+ Facility drawdown multiplier'),
                (3, '+ Inflow multiplier'), (4, '+ HQLA haircut add-ons'), (5, '+ Market price shock'),
                (6, '+ Corporate withdrawal'), (7, '+ Inflow cap override')
            UNION ALL
            SELECT 8, 'BREAKEVEN_GRID', SEQ4() / 2000.0 FROM TABLE(GENERATOR(ROWCOUNT => 2001))
        ),
        x AS (
            SELECT s.K, s.STEP_NAME, s.EXTRA, d.DT, p.PRESET, c.*,
                   IFF(s.K >= 1, p.M_RS, 1) AS MRS, IFF(s.K >= 1, p.M_RLS, 1) AS MRLS, IFF(s.K >= 1, p.M_CO, 1) AS MCO,
                   IFF(s.K >= 1, p.M_CN, 1) AS MCN, IFF(s.K >= 1, p.M_FI, 1) AS MFI, IFF(s.K >= 2, p.M_FAC, 1) AS MFAC,
                   IFF(s.K >= 3, p.M_IN, 1) AS MIN_, IFF(s.K >= 4, p.A_L1, 0) AS AL1, IFF(s.K >= 4, p.A_L2A, 0) AS AL2A,
                   IFF(s.K >= 4, p.A_L2B, 0) AS AL2B, IFF(s.K >= 5, p.SHOCK, 0) AS SH,
                   IFF(s.K >= 6, p.WD, 0) AS WD0, IFF(s.K >= 7, p.CAP, 0.75) AS CAPX
            FROM steps s, d, p, c
        ),
        y AS (
            SELECT x.*,
                   -- extra breakeven withdrawal on top of the simulated state; funded from SNB cash first,
                   -- then by liquidating L1 bonds (grid over remaining L1 stock)
                   WD0 + EXTRA * GREATEST(0, G_L1C + G_L1B - WD0) AS WD_T
            FROM x
        ),
        z AS (
            SELECT y.*,
                {{ db }}.{{ rep_agg }}.REPP_AGG_FN_LCR_CALC(
                    GREATEST(0, (G_L1C - WD_T) * GREATEST(0, COALESCE(W_L1C / NULLIF(G_L1C, 0), 0) - AL1)
                      + G_L1B * (1 - SH) * GREATEST(0, COALESCE(W_L1B / NULLIF(G_L1B, 0), 0) - AL1)),
                    G_L2A * (1 - SH) * GREATEST(0, COALESCE(W_L2A / NULLIF(G_L2A, 0), 0) - AL2A),
                    G_L2BB * (1 - SH) * GREATEST(0, COALESCE(W_L2BB / NULLIF(G_L2BB, 0), 0) - AL2B)
                      + G_L2BE * (1 - SH) * GREATEST(0, COALESCE(W_L2BE / NULLIF(G_L2BE, 0), 0) - AL2B),
                    LEAST(G_RS, W_RS * MRS) + LEAST(G_RLS, W_RLS * MRLS) + LEAST(G_CO, W_CO * MCO)
                      + LEAST(GREATEST(0, G_CN - WD_T), COALESCE(W_CN / NULLIF(G_CN, 0), 0) * GREATEST(0, G_CN - WD_T) * MCN)
                      + LEAST(G_FI, W_FI * MFI) + LEAST(G_FAC, W_FAC * MFAC),
                    LEAST(G_INF, W_INF * MIN_) + LEAST(G_IFI, W_IFI * MIN_),
                    CAPX) AS R,
                LEAST(G_RS, W_RS * MRS) + LEAST(G_RLS, W_RLS * MRLS) + LEAST(G_CO, W_CO * MCO)
                  + LEAST(GREATEST(0, G_CN - WD_T), COALESCE(W_CN / NULLIF(G_CN, 0), 0) * GREATEST(0, G_CN - WD_T) * MCN)
                  + LEAST(G_FI, W_FI * MFI) + LEAST(G_FAC, W_FAC * MFAC) AS OUT_T,
                LEAST(G_INF, W_INF * MIN_) + LEAST(G_IFI, W_IFI * MIN_) AS IN_T
            FROM y
        ),
        be AS (
            SELECT MIN(IFF(R:LCR::FLOAT <= 100, WD_T - WD0, NULL)) AS BREAKEVEN
            FROM z WHERE K = 8
        )
        SELECT z.K, z.STEP_NAME, z.DT, z.PRESET,
               ROUND(R:LCR::FLOAT, 2) AS LCR,
               ROUND(R:LCR::FLOAT - LAG(R:LCR::FLOAT) OVER (ORDER BY z.K), 2),
               ROUND(R:HQLA::FLOAT, 0), ROUND(z.OUT_T, 0), ROUND(z.IN_T, 0),
               ROUND(R:INFLOW_CAPPED::FLOAT, 0), ROUND(R:NET_OUTFLOW::FLOAT, 0),
               CASE WHEN R:LCR::FLOAT < 100 THEN 'FAIL' WHEN R:LCR::FLOAT < 110 THEN 'WARNING' ELSE 'PASS' END,
               IFF(z.K = 7, IFF(R:LCR::FLOAT <= 100, 0, be.BREAKEVEN), NULL)
        FROM z CROSS JOIN be
        WHERE z.K <= 7
        ORDER BY z.K
    );
    RETURN TABLE(res);
END;
$$;

DEFINE PROCEDURE {{ db }}.{{ rep_agg }}.REPP_AGG_SP_LCR_WHAT_IF(P_PARAMS VARCHAR, P_AS_OF_DATE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Agent-friendly what-if wrapper around REPP_AGG_SP_LCR_SIMULATE (tool LCR_What_If of LIQUIDITY_RISK_AGENT). P_PARAMS is the same JSON as the simulator (preset, *_mult, haircut_addon_*_pp, price_shock_pct, withdrawal_chf, inflow_cap_pct); P_AS_OF_DATE YYYY-MM-DD or empty for the latest date. Returns a JSON string with baseline and simulated LCR, status, delta, the step-by-step waterfall and the breakeven corporate withdrawal (CHF) that would bring the LCR to 100%.'
AS
$$
DECLARE
    out VARCHAR;
BEGIN
    CALL {{ db }}.{{ rep_agg }}.REPP_AGG_SP_LCR_SIMULATE(:P_AS_OF_DATE, :P_PARAMS);
    SELECT OBJECT_CONSTRUCT(
               'as_of_date', MAX(AS_OF_DATE)::VARCHAR,
               'preset', MAX(PRESET),
               'baseline_lcr_pct', MAX(IFF(STEP_ORDER = 0, LCR_RATIO, NULL)),
               'simulated_lcr_pct', MAX(IFF(STEP_ORDER = 7, LCR_RATIO, NULL)),
               'delta_pp', ROUND(MAX(IFF(STEP_ORDER = 7, LCR_RATIO, NULL)) - MAX(IFF(STEP_ORDER = 0, LCR_RATIO, NULL)), 2),
               'simulated_status', MAX(IFF(STEP_ORDER = 7, LCR_STATUS, NULL)),
               'simulated_hqla_chf', MAX(IFF(STEP_ORDER = 7, HQLA_TOTAL, NULL)),
               'simulated_net_outflow_chf', MAX(IFF(STEP_ORDER = 7, NET_OUTFLOW, NULL)),
               'breakeven_additional_withdrawal_chf', MAX(IFF(STEP_ORDER = 7, BREAKEVEN_WITHDRAWAL_CHF, NULL)),
               'waterfall', ARRAY_AGG(OBJECT_CONSTRUCT('step', STEP_NAME, 'lcr_pct', LCR_RATIO, 'delta_pp', DELTA_PP))
                            WITHIN GROUP (ORDER BY STEP_ORDER),
               'status_bands', 'FAIL < 100%, WARNING 100-110%, PASS >= 110%'
           )::VARCHAR
      INTO :out
      FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    RETURN out;
END;
$$;
