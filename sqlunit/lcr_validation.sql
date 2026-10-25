-- ============================================================
-- sqlunit/lcr_validation.sql
-- LCR engine validation suite (spec TC-VAL-01..04 + engine invariants).
-- Every query returns TEST_NAME, STATUS (PASS/FAIL), DETAIL.
-- Run:  snow sql -c <conn> -f sqlunit/lcr_validation.sql
-- Gate: deploy.sh fails when any STATUS = 'FAIL'.
-- ============================================================
USE DATABASE AAA_DEV_SYNTHETIC_BANK;
USE SCHEMA REP_AGG_V001;

-- TC-VAL-01: regulatory haircuts applied (BASE): L1 100%, L2A 85%, L2B 50%
SELECT 'TC-VAL-01 haircuts' AS TEST_NAME,
       IFF(COUNT_IF(ABS(HQLA_WEIGHTED_CHF - EXPOSURE_CHF * DECODE(HQLA_LEVEL, 'L1', 1.0, 'L2A', 0.85, 'L2B', 0.5)) > 0.02) = 0
           AND COUNT(*) > 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' HQLA positions checked' AS DETAIL
FROM REPP_AGG_DT_LCR_SCENARIO_POSITIONS
WHERE STRESS_SCENARIO = 'BASE' AND LCR_TREATMENT = 'HQLA';

-- TC-VAL-02: caps and LCR formula consistent with the shared function
SELECT 'TC-VAL-02 caps + formula' AS TEST_NAME,
       IFF(COUNT_IF(ABS(HQLA_TOTAL - (L1_TOTAL + LEAST(L2A_TOTAL + LEAST(L2B_TOTAL, 15/85 * (L1_TOTAL + L2A_TOTAL)), 2/3 * L1_TOTAL))) > 1
                    OR ABS(NET_OUTFLOW - (OUTFLOW_TOTAL - LEAST(INFLOW_TOTAL, 0.75 * OUTFLOW_TOTAL))) > 1
                    OR ABS(LCR_RATIO - HQLA_TOTAL / NULLIF(NET_OUTFLOW, 0) * 100) > 0.01) = 0
           AND COUNT(*) > 0, 'PASS', 'FAIL'),
       COUNT(*) || ' date x scenario rows checked'
FROM REPP_AGG_DT_LCR_SCENARIO_DAILY;

-- TC-VAL-03: injected bad records quarantined, not calculated
SELECT 'TC-VAL-03 exceptions' AS TEST_NAME,
       IFF(COUNT(DISTINCT IFF(e.ERROR_CODE IN ('UNMAPPED_ASSET_CLASS', 'MISSING_FX', 'INVALID_DATE'), e.ERROR_CODE, NULL)) = 3
           AND (SELECT COUNT(*) FROM REPP_AGG_DT_LCR_SCENARIO_POSITIONS sp
                WHERE sp.POSITION_ID IN (SELECT CONTRACT_ID FROM REPP_AGG_DT_LCR_EXCEPTIONS)) = 0, 'PASS', 'FAIL'),
       LISTAGG(DISTINCT e.ERROR_CODE, ', ')
FROM REPP_AGG_DT_LCR_EXCEPTIONS e
WHERE e.SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM REPP_AGG_DT_LCR_EXCEPTIONS);

-- TC-VAL-04a: each showcase event moves the LCR in the expected direction by the expected size
WITH ev AS (
    SELECT * FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY INSERT_TIMESTAMP_UTC DESC) = 1
)
SELECT 'TC-VAL-04a event ' || ev.EVENT_ID AS TEST_NAME,
       IFF(d.LCR_DELTA_PP IS NOT NULL
           AND SIGN(d.LCR_DELTA_PP) = IFF(ev.EXPECTED_DIRECTION = 'UP', 1, -1)
           AND ABS(d.LCR_DELTA_PP) >= ev.EXPECTED_MIN_DELTA_PP, 'PASS', 'FAIL'),
       ev.EVENT_DATE || ' ' || ev.EVENT_TYPE || ': delta ' || COALESCE(d.LCR_DELTA_PP::VARCHAR, 'n/a')
         || 'pp (expected ' || ev.EXPECTED_DIRECTION || ' >= ' || ev.EXPECTED_MIN_DELTA_PP || 'pp)'
FROM ev
LEFT JOIN REPP_AGG_DT_LCR_CUTOFF_DELTAS d
    ON d.AS_OF_DATE = ev.EVENT_DATE AND d.COMPARISON_TYPE = 'DOD' AND d.STRESS_SCENARIO = 'BASE' AND d.ASSET_CLASS = 'TOTAL'
ORDER BY ev.EVENT_ID;

-- TC-VAL-04b: root cause - for withdrawal events the event deposit is the #1 client driver
WITH ev AS (
    SELECT * FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS
    WHERE EVENT_TYPE = 'DEPOSIT_WITHDRAWAL'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY INSERT_TIMESTAMP_UTC DESC) = 1
)
SELECT 'TC-VAL-04b top driver ' || ev.EVENT_ID AS TEST_NAME,
       IFF(v.POSITION_ID = ev.CONTRACT_ID, 'PASS', 'FAIL'),
       'expected ' || ev.CONTRACT_ID || ', got ' || COALESCE(v.POSITION_ID, 'none') || ' (' || COALESCE(v.COUNTERPARTY_NAME, '') || ')'
FROM ev
LEFT JOIN REPP_AGG_VW_LCR_VARIANCE_DRIVERS v
    ON v.AS_OF_DATE = ev.EVENT_DATE AND v.COMPARISON_TYPE = 'DOD' AND v.STRESS_SCENARIO = 'BASE' AND v.CLIENT_RANK = 1;

-- ENG-01: exact attribution - contributions sum to the LCR change (all dates, BASE, DOD)
SELECT 'ENG-01 attribution sums' AS TEST_NAME,
       IFF(MAX(ABS(GAP)) < 0.0001 AND COUNT(*) > 0, 'PASS', 'FAIL'),
       'max residual ' || ROUND(MAX(ABS(GAP)), 8) || 'pp over ' || COUNT(*) || ' dates'
FROM (
    SELECT AS_OF_DATE, SUM(CONTRIBUTION_PP) - ANY_VALUE(LCR_DELTA_PP) AS GAP
    FROM REPP_AGG_VW_LCR_VARIANCE_ATTRIBUTION
    WHERE COMPARISON_TYPE = 'DOD' AND STRESS_SCENARIO = 'BASE'
    GROUP BY AS_OF_DATE
);

-- ENG-02: regulatory LCR daily table equals BASE scenario, status bands 100/110
SELECT 'ENG-02 daily = BASE + bands' AS TEST_NAME,
       IFF(COUNT_IF(d.LCR_RATIO <> s.LCR_RATIO
                    OR d.LCR_STATUS <> CASE WHEN d.LCR_RATIO < 100 THEN 'FAIL' WHEN d.LCR_RATIO < 110 THEN 'WARNING' ELSE 'PASS' END) = 0
           AND COUNT(*) > 0, 'PASS', 'FAIL'),
       COUNT(*) || ' days; FAIL days = ' || COUNT_IF(d.LCR_STATUS = 'FAIL') || ', min LCR ' || MIN(d.LCR_RATIO) || '%, max ' || MAX(d.LCR_RATIO) || '%'
FROM REPP_AGG_DT_LCR_DAILY d
JOIN REPP_AGG_DT_LCR_SCENARIO_DAILY s ON s.AS_OF_DATE = d.AS_OF_DATE AND s.STRESS_SCENARIO = 'BASE';

-- ENG-03: stress scenarios never better than BASE
SELECT 'ENG-03 stress <= base' AS TEST_NAME,
       IFF(COUNT_IF(s.LCR_RATIO > b.LCR_RATIO + 0.01) = 0 AND COUNT(*) > 0, 'PASS', 'FAIL'),
       COUNT_IF(s.LCR_RATIO > b.LCR_RATIO + 0.01) || ' violations of ' || COUNT(*)
FROM REPP_AGG_DT_LCR_SCENARIO_DAILY s
JOIN REPP_AGG_DT_LCR_SCENARIO_DAILY b ON b.AS_OF_DATE = s.AS_OF_DATE AND b.STRESS_SCENARIO = 'BASE'
WHERE s.STRESS_SCENARIO <> 'BASE';

-- ENG-04: showcase shape - at least one FAIL day, latest day not failing
SELECT 'ENG-04 showcase shape' AS TEST_NAME,
       IFF(COUNT_IF(LCR_STATUS = 'FAIL') BETWEEN 1 AND 10
           AND MAX_BY(LCR_STATUS, AS_OF_DATE) <> 'FAIL' AND MIN(LCR_RATIO) >= 85 AND MAX(LCR_RATIO) <= 200, 'PASS', 'FAIL'),
       'FAIL days ' || COUNT_IF(LCR_STATUS = 'FAIL') || ', range ' || MIN(LCR_RATIO) || '-' || MAX(LCR_RATIO) || '%, latest ' || MAX_BY(LCR_RATIO, AS_OF_DATE) || '%'
FROM REPP_AGG_DT_LCR_DAILY;

-- ENG-05: simulator with no levers reproduces the reported LCR (latest date)
CALL REPP_AGG_SP_LCR_SIMULATE('', '{}');
SELECT 'ENG-05 simulator baseline' AS TEST_NAME,
       IFF(ABS(s.LCR_RATIO - d.LCR_RATIO) <= 0.01, 'PASS', 'FAIL'),
       'simulator ' || s.LCR_RATIO || '% vs reported ' || d.LCR_RATIO || '%'
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) s
JOIN REPP_AGG_DT_LCR_DAILY d ON d.AS_OF_DATE = s.AS_OF_DATE
WHERE s.STEP_ORDER = 7;

-- ENG-06: breakeven withdrawal brings the LCR to ~100%
CALL REPP_AGG_SP_LCR_SIMULATE('', '{}');
SET BREAKEVEN = (SELECT BREAKEVEN_WITHDRAWAL_CHF FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE STEP_ORDER = 7);
CALL REPP_AGG_SP_LCR_SIMULATE('', '{"withdrawal_chf": ' || $BREAKEVEN || '}');
SELECT 'ENG-06 breakeven' AS TEST_NAME,
       IFF(ABS(LCR_RATIO - 100) <= 0.25, 'PASS', 'FAIL'),
       'withdrawal CHF ' || ROUND($BREAKEVEN / 1e6, 1) || 'M -> LCR ' || LCR_RATIO || '%'
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE STEP_ORDER = 7;
