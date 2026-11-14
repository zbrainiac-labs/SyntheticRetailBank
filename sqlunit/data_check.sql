-- ============================================================
-- sqlunit/data_check.sql
-- Data integrity gate: key tables must be unique on their business key.
-- Duplicates here usually come from a 1:n join (for example several sanctions
-- or PEP hits per customer) that leaks into a one-row-per-entity table.
-- Returns TEST_NAME, STATUS, DETAIL (row count, distinct keys, sample keys).
-- ============================================================
USE DATABASE AAA_DEV_SYNTHETIC_BANK;

SELECT 'DATA-01 Customer 360 unique per CUSTOMER_ID' AS TEST_NAME,
       IFF(COUNT(*) = COUNT(DISTINCT CUSTOMER_ID), 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' rows, ' || COUNT(DISTINCT CUSTOMER_ID) || ' customers' AS DETAIL
FROM CRM_AGG_V001.CRMA_AGG_DT_CUSTOMER_360;

SELECT 'DATA-02 customer PII vault unique per CUSTOMER_ID' AS TEST_NAME,
       IFF(COUNT(*) = COUNT(DISTINCT CUSTOMER_ID), 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' rows, ' || COUNT(DISTINCT CUSTOMER_ID) || ' customers' AS DETAIL
FROM CRM_AGG_V001.CRMI_AGG_DT_CUSTOMER_PII;

SELECT 'DATA-03 accounts unique per ACCOUNT_ID' AS TEST_NAME,
       IFF(COUNT(*) = COUNT(DISTINCT ACCOUNT_ID), 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' rows, ' || COUNT(DISTINCT ACCOUNT_ID) || ' accounts' AS DETAIL
FROM CRM_AGG_V001.ACCA_AGG_DT_ACCOUNTS;

-- ============================================================
-- DATA-LIQ: the LCR (/liquidity/lcr) and DtI (/dti) pages run on ONE liquidity book.
-- Same generator run, same latest date, same securities, deposits and contracts.
-- ============================================================
SET LIQ_LATEST_RUN = (SELECT MAX_BY(RUN_ID, INSERT_TIMESTAMP_UTC) FROM REP_RAW_V001.LIQI_RAW_TB_GENERATION_RUN);
SET LIQ_AS_OF = (SELECT MAX_BY(AS_OF_DATE, INSERT_TIMESTAMP_UTC) FROM REP_RAW_V001.LIQI_RAW_TB_GENERATION_RUN);

-- DATA-LIQ-01: both engines end on the as-of date of the run (default date of both pages)
WITH l AS (SELECT MAX(AS_OF_DATE) AS D FROM REP_AGG_V001.REPP_AGG_DT_LCR_DAILY),
d AS (SELECT MAX(AS_OF_DATE) AS D FROM REP_AGG_V001.REPP_AGG_DT_DTI_SUMMARY)
SELECT 'DATA-LIQ-01 same latest date LCR / DtI' AS TEST_NAME,
       IFF(l.D = $LIQ_AS_OF AND d.D = $LIQ_AS_OF, 'PASS', 'FAIL') AS STATUS,
       'run as-of ' || $LIQ_AS_OF || ', LCR latest ' || l.D || ', DtI latest ' || d.D AS DETAIL
FROM l CROSS JOIN d;

-- DATA-LIQ-02: one calendar - every DtI reporting date (the as-of date included, see DATA-LIQ-01) is a Zurich
--              business day of the shared calendar
WITH r AS (SELECT DISTINCT AS_OF_DATE FROM REP_AGG_V001.REPP_AGG_DT_DTI_SUMMARY),
hol AS (SELECT COUNT(*) AS N FROM REP_RAW_V001.LIQI_RAW_TB_DTI_HOLIDAYS)
SELECT 'DATA-LIQ-02 shared business-day calendar' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(NOT c.IS_BUSINESS_DAY) = 0 AND COUNT_IF(r.AS_OF_DATE = $LIQ_AS_OF) = 1, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' reporting dates, ' || COUNT_IF(NOT c.IS_BUSINESS_DAY) || ' on weekends / Zurich holidays ('
           || ANY_VALUE(hol.N) || ' holidays loaded)' AS DETAIL
FROM r
JOIN REP_AGG_V001.REPP_AGG_VW_LIQ_CALENDAR c ON c.CAL_DATE = r.AS_OF_DATE
CROSS JOIN hol;

-- DATA-LIQ-03: one securities book - every DtI security is an LCR holding with the same id, value and encumbrance
--              (credit claims WLK X are SNB collateral only) and every LCR security holding is a DtI security
WITH s AS (
    SELECT s.SECURITY_ID, s.AS_OF_DATE, s.MARKET_VALUE_CHF, s.IS_ENCUMBERED
    FROM REP_RAW_V001.LIQI_RAW_TB_DTI_SECURITIES s
    WHERE s.WLK_CLUSTER <> 'X'
      AND EXISTS (SELECT 1 FROM REP_RAW_V001.LIQI_RAW_TB_HQLA_HOLDINGS h WHERE h.AS_OF_DATE = s.AS_OF_DATE)
),
h AS (
    SELECT h.HOLDING_ID, h.AS_OF_DATE, h.MARKET_VALUE_CHF, h.IS_ENCUMBERED
    FROM REP_RAW_V001.LIQI_RAW_TB_HQLA_HOLDINGS h
    WHERE h.ASSET_TYPE NOT LIKE 'CASH%'
      AND EXISTS (SELECT 1 FROM REP_RAW_V001.LIQI_RAW_TB_DTI_SECURITIES s WHERE s.AS_OF_DATE = h.AS_OF_DATE)
),
j AS (
    SELECT COALESCE(s.AS_OF_DATE, h.AS_OF_DATE) AS AS_OF_DATE,
           h.HOLDING_ID IS NULL AS MISSING_LCR, s.SECURITY_ID IS NULL AS MISSING_DTI,
           COALESCE(ABS(s.MARKET_VALUE_CHF - h.MARKET_VALUE_CHF) > 0.01, FALSE) AS VALUE_DIFF,
           COALESCE(s.IS_ENCUMBERED <> h.IS_ENCUMBERED, FALSE) AS ENC_DIFF
    FROM s FULL OUTER JOIN h ON h.HOLDING_ID = s.SECURITY_ID AND h.AS_OF_DATE = s.AS_OF_DATE
)
SELECT 'DATA-LIQ-03 one securities book' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(MISSING_LCR OR MISSING_DTI OR VALUE_DIFF OR ENC_DIFF) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' security x date, ' || COUNT_IF(MISSING_LCR) || ' missing in LCR, '
           || COUNT_IF(MISSING_DTI) || ' missing in DtI, ' || COUNT_IF(VALUE_DIFF) || ' value diffs, '
           || COUNT(DISTINCT AS_OF_DATE) || ' shared dates' AS DETAIL
FROM j;

-- DATA-LIQ-04: engine outputs agree - the HQLA securities of the position port (market value, before haircut) equal the
--              free DtI collateral pool, and the cash equals the DtI central bank / vault cash, on every DtI date
--              (daily window and month-end history)
WITH lcr AS (
    SELECT AS_OF_DATE,
           SUM(IFF(PRODUCT_TYPE NOT LIKE 'CASH%', AMOUNT_CHF, 0)) AS SEC, SUM(IFF(PRODUCT_TYPE LIKE 'CASH%', AMOUNT_CHF, 0)) AS CASH
    FROM REP_AGG_V001.REPP_AGG_DT_LIQ_POSITIONS
    WHERE SOURCE_SYSTEM = 'HOLDINGS' AND INCLUDED_IN_LCR AND DQ_STATUS = 'OK'
    GROUP BY 1
),
dti AS (
    SELECT AS_OF_DATE,
           SUM(IFF(PRODUCT_TYPE LIKE 'WLK %', BASE_CHF, 0)) AS SEC, SUM(IFF(PRODUCT_TYPE LIKE 'CASH%', BASE_CHF, 0)) AS CASH
    FROM REP_AGG_V001.REPP_AGG_DT_DTI_CASHFLOWS
    WHERE FLOW_KIND = 'POOL'
    GROUP BY 1
)
SELECT 'DATA-LIQ-04 LCR HQLA = DtI pool' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(ABS(l.SEC - d.SEC) > 0.0005 * l.SEC OR ABS(l.CASH - d.CASH) > 0.0005 * l.CASH) = 0,
           'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' shared dates, max securities diff ' || ROUND(MAX(ABS(l.SEC - d.SEC))) || ' CHF, max cash diff '
           || ROUND(MAX(ABS(l.CASH - d.CASH))) || ' CHF; latest pool ' || ROUND(MAX_BY(l.SEC, l.AS_OF_DATE) / 1e6, 1) || 'M' AS DETAIL
FROM lcr l JOIN dti d ON d.AS_OF_DATE = l.AS_OF_DATE;

-- DATA-LIQ-05: same exclusions - records quarantined by the shared data-quality check (port DQ_STATUS = EXCEPTION)
--              are missing from the LCR and from every DtI flow; the injected DQ issues must exist (non-trivial test)
WITH exc AS (
    SELECT AS_OF_DATE, POSITION_ID FROM REP_AGG_V001.REPP_AGG_DT_LIQ_POSITIONS WHERE DQ_STATUS = 'EXCEPTION'
),
in_dti AS (
    SELECT DISTINCT f.AS_OF_DATE, f.SOURCE_ID AS POSITION_ID
    FROM REP_AGG_V001.REPP_AGG_DT_DTI_CASHFLOWS f
    JOIN exc e ON e.AS_OF_DATE = f.AS_OF_DATE AND e.POSITION_ID = f.SOURCE_ID
),
in_lcr AS (
    SELECT DISTINCT s.AS_OF_DATE, s.POSITION_ID
    FROM REP_AGG_V001.REPP_AGG_DT_LCR_SCENARIO_POSITIONS s
    JOIN exc e ON e.AS_OF_DATE = s.AS_OF_DATE AND e.POSITION_ID = s.POSITION_ID
),
flagged AS (
    SELECT e.AS_OF_DATE, e.POSITION_ID, d.POSITION_ID IS NOT NULL AS IN_DTI, l.POSITION_ID IS NOT NULL AS IN_LCR
    FROM exc e
    LEFT JOIN in_dti d ON d.AS_OF_DATE = e.AS_OF_DATE AND d.POSITION_ID = e.POSITION_ID
    LEFT JOIN in_lcr l ON l.AS_OF_DATE = e.AS_OF_DATE AND l.POSITION_ID = e.POSITION_ID
),
ctr AS (
    SELECT COUNT(DISTINCT SOURCE_ID) AS N FROM REP_AGG_V001.REPP_AGG_DT_DTI_CASHFLOWS
    WHERE FLOW_KIND = 'CONTRACTUAL' AND AS_OF_DATE = $LIQ_AS_OF AND SOURCE_ID LIKE 'CTR%'
)
SELECT 'DATA-LIQ-05 same exclusions LCR / DtI' AS TEST_NAME,
       IFF(COUNT(f.POSITION_ID) > 0 AND COUNT_IF(f.IN_DTI) = 0 AND COUNT_IF(f.IN_LCR) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(f.POSITION_ID) || ' quarantined position x date, used by LCR ' || COUNT_IF(f.IN_LCR) || ', by DtI '
           || COUNT_IF(f.IN_DTI) || '; DtI contracts on the latest date: ' || ANY_VALUE(c.N) AS DETAIL
FROM ctr c
LEFT JOIN flagged f ON TRUE;

-- DATA-LIQ-07: one raw book - the LCR input is exactly the DAILY slice of the position port, and the port has a
--              snapshot for every DtI reporting date (daily window + month-end history)
WITH l AS (SELECT COUNT(*) AS N FROM REP_AGG_V001.REPP_AGG_DT_LCR_POSITIONS),
p AS (
    SELECT COUNT_IF(SNAPSHOT_SCOPE = 'DAILY') AS N_DAILY, COUNT_IF(SNAPSHOT_SCOPE = 'MONTH_END') AS N_MONTH_END,
           COUNT(DISTINCT IFF(SNAPSHOT_SCOPE = 'MONTH_END', AS_OF_DATE, NULL)) AS D_MONTH_END
    FROM REP_AGG_V001.REPP_AGG_DT_LIQ_POSITIONS
),
dti_dates AS (SELECT DISTINCT AS_OF_DATE FROM REP_AGG_V001.REPP_AGG_DT_DTI_SUMMARY),
m AS (
    SELECT COUNT(*) AS MISSING
    FROM dti_dates s
    WHERE NOT EXISTS (SELECT 1 FROM REP_AGG_V001.REPP_AGG_DT_LIQ_POSITIONS q WHERE q.AS_OF_DATE = s.AS_OF_DATE)
)
SELECT 'DATA-LIQ-07 one raw book (daily slice + month-ends)' AS TEST_NAME,
       IFF(l.N = p.N_DAILY AND p.N_MONTH_END > 0 AND m.MISSING = 0, 'PASS', 'FAIL') AS STATUS,
       'LCR positions ' || l.N || ', port DAILY ' || p.N_DAILY || ', port MONTH_END ' || p.N_MONTH_END
           || ' (' || p.D_MONTH_END || ' dates), DtI dates without port snapshot ' || m.MISSING AS DETAIL
FROM l CROSS JOIN p CROSS JOIN m;

-- DATA-LIQ-06: one generator run - holdings, deposits and DtI securities all carry the latest run id
WITH runs AS (
    SELECT 'holdings' AS SRC, COUNT(DISTINCT RUN_ID) AS N, MAX(RUN_ID) AS RUN_ID, COUNT_IF(RUN_ID IS NULL) AS MISSING
    FROM REP_RAW_V001.LIQI_RAW_TB_HQLA_HOLDINGS
),
dep AS (
    SELECT 'deposits' AS SRC, COUNT(DISTINCT RUN_ID) AS N, MAX(RUN_ID) AS RUN_ID, COUNT_IF(RUN_ID IS NULL) AS MISSING
    FROM REP_RAW_V001.LIQI_RAW_TB_DEPOSIT_BALANCES
),
sec AS (
    SELECT 'securities' AS SRC, COUNT(DISTINCT RUN_ID) AS N, MAX(RUN_ID) AS RUN_ID, COUNT_IF(RUN_ID IS NULL) AS MISSING
    FROM REP_RAW_V001.LIQI_RAW_TB_DTI_SECURITIES
)
SELECT 'DATA-LIQ-06 one generator run' AS TEST_NAME,
       IFF(h.N = 1 AND d.N = 1 AND s.N = 1 AND h.MISSING + d.MISSING + s.MISSING = 0
           AND h.RUN_ID = $LIQ_LATEST_RUN AND d.RUN_ID = $LIQ_LATEST_RUN AND s.RUN_ID = $LIQ_LATEST_RUN, 'PASS', 'FAIL') AS STATUS,
       'run ' || COALESCE($LIQ_LATEST_RUN, 'none') || ', ids in book: holdings ' || COALESCE(h.RUN_ID, '-')
           || ', deposits ' || COALESCE(d.RUN_ID, '-') || ', securities ' || COALESCE(s.RUN_ID, '-')
           || ' (' || (h.N + d.N + s.N) || ' id sets, ' || (h.MISSING + d.MISSING + s.MISSING) || ' rows without id)' AS DETAIL
FROM runs h CROSS JOIN dep d CROSS JOIN sec s;
