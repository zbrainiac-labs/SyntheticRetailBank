/*
 * sqlunit/fco_validation.sql
 * Financial Control consolidation showcase (580_FCOA_consolidation.sql) validation suite.
 * Every query returns TEST_NAME, STATUS (PASS/FAIL), DETAIL.
 * Run:  snow sql -c <conn> -f sqlunit/fco_validation.sql
 * Gate: deploy.sh fails when any STATUS = 'FAIL'.
 */
USE DATABASE AAA_DEV_SYNTHETIC_BANK;
USE SCHEMA REP_AGG_V001;

SET FC_LATEST = (SELECT MAX(PERIOD_MONTH) FROM FCOA_AGG_DT_FX_PERIODS);

-- FCO-01: every entity and the group balance in every month (assets = liabilities + equity)
SELECT 'FCO-01 balance sheet balances' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(STATUS = 'FAIL') = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' months, max difference CHF ' || ROUND(MAX(DIFFERENCE), 2) AS DETAIL
FROM FCOA_AGG_VW_CONSOLIDATION_CHECKS WHERE CHECK_ID = 'FC-C1';

-- FCO-02: intercompany receivables, payables, IC interest and service fees net to zero without the demo exception
SELECT 'FCO-02 IC nets to zero' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(STATUS = 'FAIL') = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' months, max difference CHF ' || ROUND(MAX(DIFFERENCE), 2) AS DETAIL
FROM FCOA_AGG_VW_CONSOLIDATION_CHECKS WHERE CHECK_ID = 'FC-C2';

-- FCO-03: the demo exception turns exactly one control red (FC-C2) on the latest month and names one pair
SELECT 'FCO-03 demo exception fails one control' AS TEST_NAME,
       IFF(COUNT_IF(DEMO_STATUS = 'FAIL') = 1 AND MAX(IFF(DEMO_STATUS = 'FAIL', CHECK_ID, NULL)) = 'FC-C2'
           AND MAX(IFF(CHECK_ID = 'FC-C2', DEMO_AFFECTED, NULL)) LIKE 'IC02 %', 'PASS', 'FAIL') AS STATUS,
       COUNT_IF(DEMO_STATUS = 'FAIL') || ' failing, ' || COALESCE(MAX(IFF(CHECK_ID = 'FC-C2', DEMO_AFFECTED, NULL)), 'none')
           || ', difference CHF ' || ROUND(MAX(IFF(CHECK_ID = 'FC-C2', DEMO_DIFFERENCE, NULL)), 0) AS DETAIL
FROM FCOA_AGG_VW_CONSOLIDATION_CHECKS WHERE PERIOD_MONTH = $FC_LATEST;

-- FCO-04: CTA reconciles and is 0 for all CHF entities
SELECT 'FCO-04 CTA reconciles' AS TEST_NAME,
       IFF(c.FAILS = 0 AND t.CHF_CTA < 1 AND t.FOREIGN_ENTITIES = 3, 'PASS', 'FAIL') AS STATUS,
       c.FAILS || ' failing months, CTA of CHF entities ' || ROUND(t.CHF_CTA, 2) || ', foreign entities ' || t.FOREIGN_ENTITIES AS DETAIL
FROM (SELECT COUNT_IF(STATUS = 'FAIL') AS FAILS FROM FCOA_AGG_VW_CONSOLIDATION_CHECKS WHERE CHECK_ID = 'FC-C3') c
CROSS JOIN (
    SELECT SUM(IFF(FUNCTIONAL_CCY = 'CHF', ABS(CTA_CHF), 0)) AS CHF_CTA,
           COUNT(DISTINCT IFF(FUNCTIONAL_CCY <> 'CHF', ENTITY_ID, NULL)) AS FOREIGN_ENTITIES
    FROM FCOA_AGG_VW_FX_TRANSLATION
) t;

-- FCO-05: source completeness - every valid liquidity book position is consolidated and mapped
SELECT 'FCO-05 source completeness' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND COUNT_IF(STATUS = 'FAIL') = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' months, max difference CHF ' || ROUND(MAX(DIFFERENCE), 2) AS DETAIL
FROM FCOA_AGG_VW_CONSOLIDATION_CHECKS WHERE CHECK_ID = 'FC-C4';

-- FCO-06: bridge components add up (opening + movements = closing) for both metrics in every month
WITH b AS (
    SELECT PERIOD_MONTH, METRIC,
           SUM(IFF(COMPONENT NOT IN ('OPENING', 'CLOSING'), AMOUNT_CHF, 0)) + SUM(IFF(COMPONENT = 'OPENING', AMOUNT_CHF, 0))
             - SUM(IFF(COMPONENT = 'CLOSING', AMOUNT_CHF, 0)) AS GAP
    FROM FCOA_AGG_VW_CONSOLIDATION_BRIDGE
    GROUP BY 1, 2
)
SELECT 'FCO-06 bridge adds up' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND MAX(ABS(GAP)) < 1, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' month x metric bridges, max gap CHF ' || ROUND(MAX(ABS(GAP)), 2) AS DETAIL
FROM b;

-- FCO-07: the answer trail ends at the group value for every FS line of the latest month
WITH t AS (
    SELECT FS_LINE, SUM(GROUP_CONTRIBUTION_CHF) AS TRAIL FROM FCOA_AGG_VW_CONSOLIDATION_TRAIL
    WHERE PERIOD_MONTH = $FC_LATEST GROUP BY 1
),
g AS (
    SELECT FS_LINE, SUM(AMOUNT_CHF) AS GRP FROM FCOA_AGG_DT_GROUP_CONSOLIDATION
    WHERE PERIOD_MONTH = $FC_LATEST AND COLUMN_TYPE = 'GROUP' GROUP BY 1
)
SELECT 'FCO-07 trail ends at group value' AS TEST_NAME,
       IFF(COUNT(*) > 0 AND MAX(ABS(COALESCE(t.TRAIL, 0) - g.GRP)) < 1, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' FS lines, max gap CHF ' || ROUND(MAX(ABS(COALESCE(t.TRAIL, 0) - g.GRP)), 2) AS DETAIL
FROM g LEFT JOIN t ON t.FS_LINE = g.FS_LINE;
