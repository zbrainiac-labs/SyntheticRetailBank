-- ============================================================
-- sqlunit/dti_validation.sql
-- DtI / Stichtagsvergleich (UC2) engine validation suite.
-- Every query returns TEST_NAME, STATUS (PASS/FAIL), DETAIL.
-- Run:  snow sql -c <conn> -f sqlunit/dti_validation.sql
-- Gate: deploy.sh fails when any STATUS = 'FAIL'.
-- ============================================================
USE DATABASE AAA_DEV_SYNTHETIC_BANK;
USE SCHEMA REP_AGG_V001;

-- Reference dates come from the loaded book (no hardcoded dates): latest / previous reporting date and event dates
SET LATEST = (SELECT MAX(AS_OF_DATE) FROM REPP_AGG_DT_DTI_SUMMARY);
SET PREV = (SELECT MAX(AS_OF_DATE) FROM REPP_AGG_DT_DTI_SUMMARY WHERE AS_OF_DATE < $LATEST);
SET D01 = (SELECT EVENT_DATE FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_01');
SET P01 = (SELECT MAX(AS_OF_DATE) FROM REPP_AGG_DT_DTI_SUMMARY WHERE AS_OF_DATE < $D01);
SET E01 = (SELECT ENTITY_ID FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_01');
SET A01 = (SELECT AMOUNT_CHF FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_01');
SET D02 = (SELECT EVENT_DATE FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_02');
SET P02 = (SELECT MAX(AS_OF_DATE) FROM REPP_AGG_DT_DTI_SUMMARY WHERE AS_OF_DATE < $D02);
SET E02 = (SELECT ENTITY_ID FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_02');
SET WINDOW_START = (SELECT MIN(AS_OF_DATE) FROM REPP_AGG_DT_LCR_DAILY);

-- DTI-01: every flow maps to a leaf LST position, entity and status
SELECT 'DTI-01 flow mapping' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(p.POSITION_CODE IS NULL OR NOT p.IS_LEAF OR c.ENTITY_ID IS NULL) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' flows, ' || COUNT_IF(p.POSITION_CODE IS NULL OR NOT p.IS_LEAF) || ' without leaf position' AS DETAIL
FROM REPP_AGG_DT_DTI_CASHFLOWS c
LEFT JOIN REP_RAW_V001.LIQI_RAW_TB_DTI_LST_POSITIONS p ON p.POSITION_CODE = c.POSITION_CODE;

-- DTI-02: deterministic flows identical in base and stressed (planned funding is 0 under stress by design);
--         stressed pool <= base pool
SELECT 'DTI-02 base vs stressed' AS TEST_NAME,
       IFF(COUNT_IF((FLOW_KIND IN ('CONTRACTUAL', 'CORRECTION') OR (FLOW_KIND = 'PLANNING' AND PRODUCT_TYPE <> 'FUNDING_PLAN'))
                    AND BASE_CHF <> STRESSED_CHF) = 0
           AND COUNT_IF(FLOW_KIND = 'PLANNING' AND PRODUCT_TYPE = 'FUNDING_PLAN' AND STRESSED_CHF <> 0) = 0
           AND COUNT_IF(FLOW_KIND = 'POOL' AND STRESSED_CHF > BASE_CHF + 0.01) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT_IF(FLOW_KIND = 'POOL') || ' pool lines checked' AS DETAIL
FROM REPP_AGG_DT_DTI_CASHFLOWS;

-- DTI-03: projection day 365 equals the sum of all flows within the horizon (control sum)
WITH f AS (
    SELECT AS_OF_DATE, ENTITY_ID, SUM(STRESSED_CHF) AS S
    FROM REPP_AGG_DT_DTI_CASHFLOWS WHERE CF_DATE <= DATEADD(day, 365, AS_OF_DATE) GROUP BY 1, 2
)
SELECT 'DTI-03 projection control sum' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(ABS(f.S - p.TOTAL_STRESSED_CHF) > 1) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' entity x date checked, max diff ' || ROUND(MAX(ABS(f.S - p.TOTAL_STRESSED_CHF)), 2) AS DETAIL
FROM f JOIN REPP_AGG_DT_DTI_PROJECTION p ON p.AS_OF_DATE = f.AS_OF_DATE AND p.ENTITY_ID = f.ENTITY_ID AND p.DAY_OFFSET = 365;

-- DTI-04: DtI is the first day with total <= 0 and every entity has a summary per date
SELECT 'DTI-04 DtI definition' AS TEST_NAME,
       IFF(COUNT(*) = (SELECT COUNT(DISTINCT AS_OF_DATE || ENTITY_ID) FROM REPP_AGG_DT_DTI_CASHFLOWS)
           AND COUNT_IF(s.DTI_DAYS_STRESSED IS NOT NULL AND (p.TOTAL_STRESSED_CHF > 0 OR prev.TOTAL_STRESSED_CHF <= 0)) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' summaries, ' || COUNT_IF(s.DTI_DAYS_STRESSED IS NULL) || ' with DtI > 365' AS DETAIL
FROM REPP_AGG_DT_DTI_SUMMARY s
LEFT JOIN REPP_AGG_DT_DTI_PROJECTION p ON p.AS_OF_DATE = s.AS_OF_DATE AND p.ENTITY_ID = s.ENTITY_ID AND p.DAY_OFFSET = s.DTI_DAYS_STRESSED
LEFT JOIN REPP_AGG_DT_DTI_PROJECTION prev ON prev.AS_OF_DATE = s.AS_OF_DATE AND prev.ENTITY_ID = s.ENTITY_ID AND prev.DAY_OFFSET = s.DTI_DAYS_STRESSED - 1
                                          AND s.DTI_DAYS_STRESSED > 0;

-- DTI-05: bucket view and daily view reconcile with the flow table (reference date, all entities)
WITH b AS (
    SELECT SUM(REF_VALUE) AS S FROM TABLE(REPP_AGG_FN_DTI_COMPARE($LATEST::DATE, $PREV::DATE,
        ['LE_01','LE_02','LE_03','LE_04','LE_05','LE_06','LE_07','LE_08','LE_09'], 'STRESSED', 'BUCKETS', TRUE)) WHERE NOT IS_INFO_ROW
),
d AS (
    SELECT SUM(REF_VALUE) AS S FROM TABLE(REPP_AGG_FN_DTI_COMPARE($LATEST::DATE, $PREV::DATE,
        ['LE_01','LE_02','LE_03','LE_04','LE_05','LE_06','LE_07','LE_08','LE_09'], 'STRESSED', 'DAYS', TRUE)) WHERE NOT IS_INFO_ROW
),
t AS (
    SELECT SUM(STRESSED_CHF) AS S, SUM(IFF(c.NEXT_BUSINESS_DAY <= DATEADD(day, 365, AS_OF_DATE), STRESSED_CHF, 0)) AS S365
    -- LEFT JOIN: flows outside the calendar would be lost by the comparison and must fail this check
    FROM REPP_AGG_DT_DTI_CASHFLOWS f LEFT JOIN REPP_AGG_VW_DTI_CALENDAR c ON c.CAL_DATE = f.CF_DATE
    WHERE AS_OF_DATE = $LATEST
)
SELECT 'DTI-05 bucket/day reconciliation' AS TEST_NAME,
       IFF(ABS(b.S - t.S) <= 1 AND ABS(d.S - t.S365) <= 1, 'PASS', 'FAIL') AS STATUS,
       'buckets ' || ROUND(b.S) || ' vs flows ' || ROUND(t.S) || '; days ' || ROUND(d.S) || ' vs flows<=365 ' || ROUND(t.S365) AS DETAIL
FROM b, d, t;

-- DTI-06: group = sum of entities (additivity) for one date pair
WITH g AS (
    SELECT SUM(DELTA) AS S FROM TABLE(REPP_AGG_FN_DTI_COMPARE($D01::DATE, $P01::DATE, ['LE_01','LE_02','LE_03'], 'STRESSED', 'BUCKETS', FALSE)) WHERE NOT IS_INFO_ROW
),
e AS (
    SELECT SUM(DELTA) AS S FROM (
        SELECT DELTA FROM TABLE(REPP_AGG_FN_DTI_COMPARE($D01::DATE, $P01::DATE, ['LE_01'], 'STRESSED', 'BUCKETS', FALSE)) WHERE NOT IS_INFO_ROW
        UNION ALL SELECT DELTA FROM TABLE(REPP_AGG_FN_DTI_COMPARE($D01::DATE, $P01::DATE, ['LE_02'], 'STRESSED', 'BUCKETS', FALSE)) WHERE NOT IS_INFO_ROW
        UNION ALL SELECT DELTA FROM TABLE(REPP_AGG_FN_DTI_COMPARE($D01::DATE, $P01::DATE, ['LE_03'], 'STRESSED', 'BUCKETS', FALSE)) WHERE NOT IS_INFO_ROW)
)
SELECT 'DTI-06 group additivity' AS TEST_NAME, IFF(ABS(g.S - e.S) <= 1, 'PASS', 'FAIL') AS STATUS,
       'group ' || ROUND(g.S) || ' vs sum of entities ' || ROUND(e.S) AS DETAIL
FROM g, e;

-- DTI-07a: DTI_01 withdrawal lowers its entity's central bank balance and its share of the group cash
-- (central cash is allocated by the current deposit share of each entity)
WITH e AS (
    SELECT SUM(REF_VALUE) AS REF_E, SUM(COMP_VALUE) AS COMP_E
    FROM TABLE(REPP_AGG_FN_DTI_COMPARE($D01::DATE, $P01::DATE, [$E01], 'STRESSED', 'BUCKETS', FALSE))
    WHERE NOT IS_INFO_ROW AND POSITION_CODE = 'POOL_CB_BALANCE'
), g AS (
    SELECT SUM(REF_VALUE) AS REF_G, SUM(COMP_VALUE) AS COMP_G
    FROM TABLE(REPP_AGG_FN_DTI_COMPARE($D01::DATE, $P01::DATE,
        ['LE_01','LE_02','LE_03','LE_04','LE_05','LE_06','LE_07','LE_08','LE_09'], 'STRESSED', 'BUCKETS', TRUE))
    WHERE NOT IS_INFO_ROW AND POSITION_CODE = 'POOL_CB_BALANCE'
)
SELECT 'DTI-07a event withdrawal' AS TEST_NAME,
       IFF(e.REF_E < e.COMP_E AND e.REF_E / g.REF_G < e.COMP_E / g.COMP_G, 'PASS', 'FAIL') AS STATUS,
       $E01 || ' CB balance ' || ROUND(e.COMP_E / 1e6, 1) || 'M -> ' || ROUND(e.REF_E / 1e6, 1) || 'M, share '
           || ROUND(100 * e.COMP_E / g.COMP_G, 1) || '% -> ' || ROUND(100 * e.REF_E / g.REF_G, 1) || '% (event ' || ROUND($A01 / 1e6, 1) || 'M)' AS DETAIL
FROM e CROSS JOIN g;

-- DTI-07b: DTI_02 bucket shift lands in M01 for the corporate loans of its entity
SELECT 'DTI-07b event bucket shift' AS TEST_NAME,
       IFF(MAX_BY(PERIOD_KEY, ABS(D)) = 'M01' AND MAX_BY(D, ABS(D)) > 0, 'PASS', 'FAIL') AS STATUS,
       MAX_BY(PERIOD_KEY, ABS(D)) || ' ' || ROUND(MAX_BY(D, ABS(D)) / 1e6, 1) || 'M' AS DETAIL
FROM (SELECT PERIOD_KEY, SUM(DELTA) AS D
      FROM TABLE(REPP_AGG_FN_DTI_COMPARE($D02::DATE, $P02::DATE, [$E02], 'STRESSED', 'BUCKETS', FALSE))
      WHERE NOT IS_INFO_ROW AND POSITION_CODE = 'DET_FIX_LOANS_CORP' GROUP BY 1);

-- DTI-07c: DTI_03 correction is a separate line of -15M
SELECT 'DTI-07c event correction' AS TEST_NAME,
       IFF(ROUND(SUM(REF_VALUE)) = -15000000, 'PASS', 'FAIL') AS STATUS, ROUND(SUM(REF_VALUE)) || ' CHF' AS DETAIL
FROM TABLE(REPP_AGG_FN_DTI_COMPARE($LATEST::DATE, $PREV::DATE, ['LE_01'], 'STRESSED', 'BUCKETS', FALSE))
WHERE PRODUCT_TYPE = 'CORRECTION';

-- DTI-07d: DTI_04 PRELIMINARY data of LE_07 excluded unless requested
WITH x AS (SELECT COUNT(*) AS N FROM TABLE(REPP_AGG_FN_DTI_COMPARE($LATEST::DATE, $PREV::DATE, ['LE_07'], 'STRESSED', 'BUCKETS', FALSE)) WHERE REF_VALUE <> 0),
     i AS (SELECT COUNT(*) AS N FROM TABLE(REPP_AGG_FN_DTI_COMPARE($LATEST::DATE, $PREV::DATE, ['LE_07'], 'STRESSED', 'BUCKETS', TRUE)) WHERE REF_VALUE <> 0)
SELECT 'DTI-07d preliminary filter' AS TEST_NAME, IFF(x.N = 0 AND i.N > 0, 'PASS', 'FAIL') AS STATUS,
       'excluded ' || x.N || ' rows, included ' || i.N || ' rows' AS DETAIL
FROM x, i;

-- DTI-08: daily view only returns business days
SELECT 'DTI-08 business days only' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(NOT c.IS_BUSINESS_DAY) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(DISTINCT f.PERIOD_DATE) || ' distinct days' AS DETAIL
FROM TABLE(REPP_AGG_FN_DTI_COMPARE($LATEST::DATE, $PREV::DATE, ['LE_01'], 'STRESSED', 'DAYS', FALSE)) f
JOIN REPP_AGG_VW_DTI_CALENDAR c ON c.CAL_DATE = f.PERIOD_DATE
WHERE NOT f.IS_INFO_ROW;

-- DTI-09: reporting dates = Zurich business days (window) plus month-end history (last business day of the month)
WITH r AS (SELECT DISTINCT AS_OF_DATE FROM REPP_AGG_DT_DTI_SUMMARY)
SELECT 'DTI-09 reporting dates' AS TEST_NAME,
       IFF(COUNT_IF(NOT c.IS_BUSINESS_DAY) = 0 AND COUNT_IF(MONTH(n.NEXT_BUSINESS_DAY) <> MONTH(r.AS_OF_DATE) AND r.AS_OF_DATE < $WINDOW_START) >= 15,
           'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' dates, ' || COUNT_IF(NOT c.IS_BUSINESS_DAY) || ' non-business days, '
           || COUNT_IF(MONTH(n.NEXT_BUSINESS_DAY) <> MONTH(r.AS_OF_DATE) AND r.AS_OF_DATE < $WINDOW_START) || ' history month-ends' AS DETAIL
FROM r
JOIN REPP_AGG_VW_DTI_CALENDAR c ON c.CAL_DATE = r.AS_OF_DATE
JOIN REPP_AGG_VW_DTI_CALENDAR n ON n.CAL_DATE = DATEADD(day, 1, r.AS_OF_DATE);

-- DTI-10: pool value conservation - cashflow values of every free security add up to its market value (+-1%)
WITH v AS (
    SELECT s.AS_OF_DATE, s.SECURITY_ID, s.MARKET_VALUE_CHF AS MV, SUM(f.BASE_CHF) AS CF
    FROM REP_RAW_V001.LIQI_RAW_TB_DTI_SECURITIES s
    JOIN REPP_AGG_DT_DTI_CASHFLOWS f ON f.AS_OF_DATE = s.AS_OF_DATE AND f.SOURCE_ID = s.SECURITY_ID AND f.FLOW_KIND = 'POOL'
    WHERE NOT s.IS_ENCUMBERED AND s.HQLA_CATEGORY IN ('L1', 'L2A', 'L2B')
    GROUP BY 1, 2, 3
)
SELECT 'DTI-10 pool value conservation' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(ABS(CF - MV) > 0.01 * ABS(MV) + 1) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' security x date, max rel. diff ' || ROUND(MAX(ABS(CF - MV) / NULLIF(ABS(MV), 0)), 6) AS DETAIL
FROM v;

-- DTI-11a: DTI_05 purchase - WLK II of LE_01 rises by about the purchase amount vs the previous reporting date
SET D05 = (SELECT EVENT_DATE FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_05');
SET A05 = (SELECT AMOUNT_CHF FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_05');
SET P05 = (SELECT MAX(AS_OF_DATE) FROM REPP_AGG_DT_DTI_SUMMARY WHERE AS_OF_DATE < $D05);
SELECT 'DTI-11a event securities purchase' AS TEST_NAME,
       IFF(SUM(DELTA) BETWEEN 0.7 * $A05 AND 1.3 * $A05, 'PASS', 'FAIL') AS STATUS,
       'WLK II delta ' || ROUND(SUM(DELTA) / 1e6, 1) || 'M on ' || $D05 || ' (purchase ' || ROUND($A05 / 1e6, 1) || 'M)' AS DETAIL
FROM TABLE(REPP_AGG_FN_DTI_COMPARE($D05::DATE, $P05::DATE, ['LE_01'], 'STRESSED', 'BUCKETS', FALSE))
WHERE NOT IS_INFO_ROW AND PRODUCT_TYPE = 'WLK II';

-- DTI-11b: DTI_06 downgrade - the security moves from WLK VI to VII
SET D06 = (SELECT EVENT_DATE FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_06');
SET S06 = (SELECT POSITION_ID FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_06');
SELECT 'DTI-11b event WLK downgrade' AS TEST_NAME,
       IFF(MAX(IFF(AS_OF_DATE = $D06, WLK_CLUSTER, NULL)) = 'VII'
           AND MAX(IFF(AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM REPP_AGG_DT_DTI_SUMMARY WHERE AS_OF_DATE < $D06), WLK_CLUSTER, NULL)) = 'VI',
           'PASS', 'FAIL') AS STATUS,
       $S06 || ': ' || LISTAGG(DISTINCT WLK_CLUSTER, ' -> ') WITHIN GROUP (ORDER BY WLK_CLUSTER DESC) AS DETAIL
FROM REP_RAW_V001.LIQI_RAW_TB_DTI_SECURITIES WHERE SECURITY_ID = $S06;

-- DTI-11c: DTI_07 maturity - the canton bond redeems inside W2 on the latest date, not on the previous one
SET D07 = (SELECT EVENT_DATE FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_07');
SET S07 = (SELECT POSITION_ID FROM REP_RAW_V001.LIQI_RAW_TB_DTI_EVENTS WHERE EVENT_ID = 'DTI_07');
SELECT 'DTI-11c event security maturity' AS TEST_NAME,
       IFF(MAX(IFF(AS_OF_DATE = $D07, DATEDIFF(day, AS_OF_DATE, MATURITY_DATE), NULL)) BETWEEN 8 AND 14
           AND MAX(IFF(AS_OF_DATE < $D07, MATURITY_DATE, NULL)) > DATEADD(day, 365, $D07::DATE), 'PASS', 'FAIL') AS STATUS,
       $S07 || ' matures ' || MAX(IFF(AS_OF_DATE = $D07, MATURITY_DATE, NULL)) AS DETAIL
FROM REP_RAW_V001.LIQI_RAW_TB_DTI_SECURITIES WHERE SECURITY_ID = $S07;

-- DTI-12: richer data - on the default date pair at least 80% of the product clusters show a delta
WITH d AS (
    SELECT PRODUCT_CLUSTER, SUM(ABS(DELTA)) AS A
    FROM TABLE(REPP_AGG_FN_DTI_COMPARE($LATEST::DATE, $PREV::DATE,
        ['LE_01','LE_02','LE_03','LE_04','LE_05','LE_06','LE_07','LE_08','LE_09'], 'STRESSED', 'BUCKETS', TRUE))
    WHERE NOT IS_INFO_ROW AND PRODUCT_CLUSTER IS NOT NULL GROUP BY 1
)
SELECT 'DTI-12 product cluster deltas' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(A >= 1000) >= 0.8 * COUNT(*), 'PASS', 'FAIL') AS STATUS,
       COUNT_IF(A >= 1000) || ' of ' || COUNT(*) || ' clusters with a delta; none: ' || COALESCE(LISTAGG(IFF(A < 1000, PRODUCT_CLUSTER, NULL), ', '), '-') AS DETAIL
FROM d;

-- DTI-13: calibration - group (all entities) stressed DtI around the LCR horizon (20-90 days median: one book with
--         LCR ~135% and the free pool as next-day repo capacity), base > 365 days on every date
WITH g AS (
    SELECT AS_OF_DATE, DAY_OFFSET, SUM(TOTAL_STRESSED_CHF) AS TS, SUM(TOTAL_BASE_CHF) AS TB
    FROM REPP_AGG_DT_DTI_PROJECTION GROUP BY 1, 2
),
s AS (
    SELECT AS_OF_DATE, MIN(IFF(TS <= 0, DAY_OFFSET, NULL)) AS DS, MIN(IFF(TB <= 0, DAY_OFFSET, NULL)) AS DB FROM g GROUP BY 1
)
SELECT 'DTI-13 DtI calibration' AS TEST_NAME,
       IFF(MEDIAN(COALESCE(DS, 366)) BETWEEN 20 AND 90 AND COUNT(DB) = 0, 'PASS', 'FAIL') AS STATUS,
       'stressed median ' || MEDIAN(COALESCE(DS, 366)) || ' days (min ' || MIN(DS) || ', max ' || MAX(DS) || '), base <= 365 on '
           || COUNT(DB) || ' of ' || COUNT(*) || ' dates' AS DETAIL
FROM s;

-- DTI-14a: entity row access policy attached to every DtI object with ENTITY_ID
SELECT 'DTI-14a row access policy attached' AS TEST_NAME,
       IFF(COUNT(*) = 6 AND COUNT_IF(POLICY_STATUS <> 'ACTIVE') = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' objects: ' || LISTAGG(REF_ENTITY_NAME, ', ') WITHIN GROUP (ORDER BY REF_ENTITY_NAME) AS DETAIL
FROM TABLE(AAA_DEV_SYNTHETIC_BANK.INFORMATION_SCHEMA.POLICY_REFERENCES(
    POLICY_NAME => 'AAA_DEV_SYNTHETIC_BANK.REP_AGG_V001.REPP_AGG_RAP_DTI_ENTITY'));

-- DTI-14b/c: per-role row access checks run as the DtI roles: sqlunit/dti_governance_{read_only,lrc_read}.sql

-- DTI-14d: only DTI_OWNER may write the manual inputs
SHOW GRANTS ON TABLE REP_RAW_V001.LIQI_RAW_TB_DTI_CORRECTIONS;
SELECT 'DTI-14d editor grants' AS TEST_NAME,
       IFF(COUNT_IF("privilege" IN ('INSERT', 'UPDATE', 'DELETE') AND "grantee_name" LIKE 'DTI%' AND "grantee_name" <> 'DTI_OWNER') = 0
           AND COUNT_IF("privilege" = 'INSERT' AND "grantee_name" = 'DTI_OWNER') = 1, 'PASS', 'FAIL') AS STATUS,
       LISTAGG(IFF("grantee_name" LIKE 'DTI%', "grantee_name" || ':' || "privilege", NULL), ', ') AS DETAIL
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
