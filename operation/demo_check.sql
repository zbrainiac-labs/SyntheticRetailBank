-- ============================================================
-- operation/demo_check.sql
-- Demo-readiness gate of the LCR + DtI showcase on DEV (read-only).
-- Encodes the acceptance values of business_requirements/LIQUIDITY_DTI_LCR_DEMO_STORYLINE.md.
-- DEMO-01..08 have an offline twin in generators/liquidity_checks.py (run before every upload);
-- DEMO-09 needs the DtI engine and only runs here.
-- Returns TEST_NAME, STATUS, DETAIL. Run after the engines are refreshed:
--   snow sql -c <conn> -f operation/demo_check.sql
-- ============================================================
USE DATABASE AAA_DEV_SYNTHETIC_BANK;

SET DEMO_AS_OF = (SELECT MAX_BY(AS_OF_DATE, INSERT_TIMESTAMP_UTC) FROM REP_RAW_V001.LIQI_RAW_TB_GENERATION_RUN);
SET EVT_STRESS = (SELECT MAX(EVENT_DATE) FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS WHERE EVENT_ID = 'EVT_02');
SET EVT_RECOVERY = (SELECT MAX(EVENT_DATE) FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS WHERE EVENT_ID = 'EVT_03');
SET EVT_W2 = (SELECT MAX(EVENT_DATE) FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS WHERE EVENT_ID = 'EVT_05');
SET EVT_FUNDING = (SELECT MAX(CONTRACT_ID) FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS WHERE EVENT_ID = 'EVT_02');

-- DEMO-01: the September funding-cliff breach is preserved: drop >= 30pp on the stress start, >= 5 business
--          days below 100%, low between 90% and 97%, and no breach outside the stress window
WITH l AS (
    SELECT AS_OF_DATE, LCR_RATIO, LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE) AS STEP
    FROM REP_AGG_V001.REPP_AGG_DT_LCR_DAILY
)
SELECT 'DEMO-01 LCR breach preserved' AS TEST_NAME,
       IFF(MAX(IFF(AS_OF_DATE = $EVT_STRESS, STEP, NULL)) <= -30
           AND COUNT_IF(LCR_RATIO < 100 AND DAYOFWEEKISO(AS_OF_DATE) <= 5) >= 5
           AND MIN(LCR_RATIO) BETWEEN 90 AND 97
           AND COUNT_IF(LCR_RATIO < 100 AND (AS_OF_DATE < $EVT_STRESS OR AS_OF_DATE > DATEADD(day, 7, $EVT_RECOVERY))) = 0,
           'PASS', 'FAIL') AS STATUS,
       'stress step ' || ROUND(MAX(IFF(AS_OF_DATE = $EVT_STRESS, STEP, NULL)), 1) || 'pp, '
           || COUNT_IF(LCR_RATIO < 100 AND DAYOFWEEKISO(AS_OF_DATE) <= 5) || ' business days < 100%, low '
           || ROUND(MIN(LCR_RATIO), 1) || '%' AS DETAIL
FROM l;

-- DEMO-02: no weekday pattern - mean day-over-day change per weekday within +-0.5pp (event days, the two
--          days after each event and the stress-to-recovery period excluded)
WITH l AS (
    SELECT AS_OF_DATE, LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE) AS STEP
    FROM REP_AGG_V001.REPP_AGG_DT_LCR_DAILY
),
ev AS (
    SELECT DISTINCT DATEADD(day, g.value::INT, e.EVENT_DATE) AS D
    FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS e
    CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(0, 1, 2))) g
),
w AS (
    SELECT DAYOFWEEKISO(l.AS_OF_DATE) AS WD, AVG(l.STEP) AS MEAN_STEP
    FROM l
    LEFT JOIN ev ON ev.D = l.AS_OF_DATE
    WHERE l.STEP IS NOT NULL AND ev.D IS NULL
      AND NOT l.AS_OF_DATE BETWEEN $EVT_STRESS AND DATEADD(day, 7, $EVT_RECOVERY)
    GROUP BY 1
)
SELECT 'DEMO-02 no weekday pattern in LCR' AS TEST_NAME,
       IFF(MAX(ABS(MEAN_STEP)) <= 0.5, 'PASS', 'FAIL') AS STATUS,
       LISTAGG(WD || ':' || TO_VARCHAR(ROUND(MEAN_STEP, 2)), ' ') WITHIN GROUP (ORDER BY WD) AS DETAIL
FROM w;

-- DEMO-03: gradual recovery - no single day-over-day step above +25pp in the week of the recovery
WITH l AS (
    SELECT AS_OF_DATE, LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE) AS STEP
    FROM REP_AGG_V001.REPP_AGG_DT_LCR_DAILY
)
SELECT 'DEMO-03 gradual recovery' AS TEST_NAME,
       IFF(MAX(STEP) <= 25, 'PASS', 'FAIL') AS STATUS,
       'max step ' || ROUND(MAX(STEP), 1) || 'pp from ' || $EVT_RECOVERY AS DETAIL
FROM l
WHERE AS_OF_DATE BETWEEN $EVT_RECOVERY AND DATEADD(day, 7, $EVT_RECOVERY);

-- DEMO-04: the second corporate withdrawal is a visible step (<= -3pp)
WITH l AS (
    SELECT AS_OF_DATE, LCR_RATIO - LAG(LCR_RATIO) OVER (ORDER BY AS_OF_DATE) AS STEP
    FROM REP_AGG_V001.REPP_AGG_DT_LCR_DAILY
)
SELECT 'DEMO-04 EVT_05 visible' AS TEST_NAME,
       IFF(MAX(STEP) <= -3, 'PASS', 'FAIL') AS STATUS,
       'step ' || ROUND(MAX(STEP), 1) || 'pp on ' || $EVT_W2 AS DETAIL
FROM l
WHERE AS_OF_DATE = $EVT_W2;

-- DEMO-05: scenarios on the as-of date - BASE 132-138%, COMBINED 100-105%, every scenario >= 100%;
--          three days into the stress every scenario is below 100%
SELECT 'DEMO-05 scenario LCR on the as-of date' AS TEST_NAME,
       IFF(MAX(IFF(AS_OF_DATE = $DEMO_AS_OF AND STRESS_SCENARIO = 'BASE', LCR_RATIO, NULL)) BETWEEN 132 AND 138
           AND MAX(IFF(AS_OF_DATE = $DEMO_AS_OF AND STRESS_SCENARIO = 'COMBINED', LCR_RATIO, NULL)) BETWEEN 100 AND 105
           AND MIN(IFF(AS_OF_DATE = $DEMO_AS_OF, LCR_RATIO, NULL)) >= 100
           AND MAX(IFF(AS_OF_DATE = DATEADD(day, 3, $EVT_STRESS), LCR_RATIO, NULL)) < 100,
           'PASS', 'FAIL') AS STATUS,
       LISTAGG(IFF(AS_OF_DATE = $DEMO_AS_OF, STRESS_SCENARIO || ' ' || TO_VARCHAR(ROUND(LCR_RATIO, 1)), NULL), ', ')
           WITHIN GROUP (ORDER BY STRESS_SCENARIO)
           || ' | stress max ' || ROUND(MAX(IFF(AS_OF_DATE = DATEADD(day, 3, $EVT_STRESS), LCR_RATIO, NULL)), 1) AS DETAIL
FROM REP_AGG_V001.REPP_AGG_DT_LCR_SCENARIO_DAILY
WHERE AS_OF_DATE IN ($DEMO_AS_OF, DATEADD(day, 3, $EVT_STRESS));

-- DEMO-06: entity DtI is stable - on reporting dates without an event (stress-to-recovery period excluded) the
--          95th percentile of the day-over-day change is <= 15 days and the largest <= 60 days (DtI is a
--          first-crossing measure: a single maturing deal can move it by weeks where the curve is flat), and no
--          entity is below 20 days on the as-of date (> 365 counts as 366)
WITH s AS (
    SELECT AS_OF_DATE, ENTITY_ID, COALESCE(DTI_DAYS_STRESSED, 366) AS D
    FROM REP_AGG_V001.REPP_AGG_DT_DTI_SUMMARY
    WHERE AS_OF_DATE >= DATEADD(day, -90, $DEMO_AS_OF)
),
ev AS (
    SELECT EVENT_DATE AS D FROM REP_RAW_V001.LIQI_RAW_TB_EVENTS
    UNION
    SELECT EVENT_DATE FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS
),
j AS (
    SELECT s.AS_OF_DATE, s.ENTITY_ID, s.D, LAG(s.D) OVER (PARTITION BY s.ENTITY_ID ORDER BY s.AS_OF_DATE) AS PREV
    FROM s
),
moves AS (
    SELECT j.*, ABS(j.D - j.PREV) AS JUMP
    FROM j
    LEFT JOIN ev ON ev.D = j.AS_OF_DATE
    WHERE ev.D IS NULL AND j.PREV IS NOT NULL
      AND NOT j.AS_OF_DATE BETWEEN $EVT_STRESS AND DATEADD(day, 7, $EVT_RECOVERY)
),
stat AS (
    SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY JUMP) AS P95, MAX(JUMP) AS MX,
           MAX_BY(ENTITY_ID || ' ' || AS_OF_DATE || ' ' || PREV || '->' || D, JUMP) AS WORST
    FROM moves
),
latest AS (
    SELECT MIN(D) AS MIN_D, MIN_BY(ENTITY_ID, D) AS MIN_E FROM s WHERE AS_OF_DATE = $DEMO_AS_OF
)
SELECT 'DEMO-06 entity DtI stable' AS TEST_NAME,
       IFF(stat.P95 <= 15 AND stat.MX <= 60 AND latest.MIN_D >= 20, 'PASS', 'FAIL') AS STATUS,
       'p95 ' || ROUND(stat.P95, 0) || 'd, max ' || stat.WORST || '; as-of min ' || latest.MIN_D || 'd (' || latest.MIN_E || ')' AS DETAIL
FROM stat CROSS JOIN latest;

-- DEMO-07: central bank balance per entity after manual corrections never negative
WITH cb AS (
    SELECT AS_OF_DATE, ENTITY_ID, SUM(BASE_CHF) AS BAL
    FROM REP_AGG_V001.REPP_AGG_DT_DTI_CASHFLOWS
    WHERE POSITION_CODE = 'POOL_CB_BALANCE'
    GROUP BY 1, 2
)
SELECT 'DEMO-07 CB balance >= 0 after corrections' AS TEST_NAME,
       IFF(MIN(BAL) >= 0, 'PASS', 'FAIL') AS STATUS,
       'min ' || ROUND(MIN(BAL) / 1e6, 1) || 'M (' || MIN_BY(ENTITY_ID || ' ' || AS_OF_DATE, BAL) || ')' AS DETAIL
FROM cb;

-- DEMO-08: the free collateral pool of the DtI equals the unencumbered LCR securities on the as-of date
WITH p AS (
    SELECT SUM(BASE_CHF) AS V FROM REP_AGG_V001.REPP_AGG_DT_DTI_CASHFLOWS
    WHERE AS_OF_DATE = $DEMO_AS_OF AND POSITION_CODE LIKE 'POOL_SEC_%'
),
h AS (
    SELECT SUM(MARKET_VALUE_CHF) AS V FROM REP_RAW_V001.LIQI_RAW_TB_HQLA_HOLDINGS
    WHERE AS_OF_DATE = $DEMO_AS_OF AND HQLA_ELIGIBLE AND ASSET_TYPE NOT LIKE 'CASH%'
)
SELECT 'DEMO-08 DtI free pool = LCR securities' AS TEST_NAME,
       IFF(ABS(p.V - h.V) < 1, 'PASS', 'FAIL') AS STATUS,
       'DtI ' || ROUND(p.V / 1e6, 1) || 'M, LCR ' || ROUND(h.V / 1e6, 1) || 'M' AS DETAIL
FROM p CROSS JOIN h;

-- DEMO-09: the same funding contract that breaks the LCR on the stress start is a DtI outflow on that date
--          and lowers the group DtI (all entities) versus the previous reporting date
WITH g AS (
    SELECT AS_OF_DATE, MIN(IFF(TOTAL_STRESSED_CHF <= 0, DAY_OFFSET, NULL)) AS DTI
    FROM (
        SELECT AS_OF_DATE, DAY_OFFSET, SUM(TOTAL_STRESSED_CHF) AS TOTAL_STRESSED_CHF
        FROM REP_AGG_V001.REPP_AGG_DT_DTI_PROJECTION
        WHERE AS_OF_DATE BETWEEN DATEADD(day, -7, $EVT_STRESS) AND $EVT_STRESS
        GROUP BY 1, 2
    )
    GROUP BY 1
),
f AS (
    SELECT SUM(STRESSED_CHF) AS AMT FROM REP_AGG_V001.REPP_AGG_DT_DTI_CASHFLOWS
    WHERE AS_OF_DATE = $EVT_STRESS AND SOURCE_ID = $EVT_FUNDING AND CF_DATE <= DATEADD(day, 30, AS_OF_DATE)
)
SELECT 'DEMO-09 funding cliff visible in DtI' AS TEST_NAME,
       IFF(f.AMT < 0 AND MAX(IFF(g.AS_OF_DATE = $EVT_STRESS, g.DTI, NULL))
                         < MAX_BY(g.DTI, IFF(g.AS_OF_DATE < $EVT_STRESS, g.AS_OF_DATE, NULL)), 'PASS', 'FAIL') AS STATUS,
       $EVT_FUNDING || ' ' || ROUND(f.AMT / 1e6, 0) || 'M within 30d; group DtI '
           || MAX_BY(g.DTI, IFF(g.AS_OF_DATE < $EVT_STRESS, g.AS_OF_DATE, NULL)) || ' -> '
           || MAX(IFF(g.AS_OF_DATE = $EVT_STRESS, g.DTI, NULL)) || ' days' AS DETAIL
FROM g CROSS JOIN f
GROUP BY f.AMT;
