-- ============================================================
-- sqlunit/dti_governance_read_only.sql
-- DtI row access as DTI_READ_ONLY. Run with the role set at login
-- (PAT sessions cannot USE ROLE):  snow sql --role DTI_READ_ONLY -f ...
-- ============================================================
USE DATABASE AAA_DEV_SYNTHETIC_BANK;
USE SCHEMA REP_AGG_V001;
SET LATEST = (SELECT MAX(AS_OF_DATE) FROM REPP_AGG_DT_DTI_SUMMARY);
SET PREV = (SELECT MAX(AS_OF_DATE) FROM REPP_AGG_DT_DTI_SUMMARY WHERE AS_OF_DATE < $LATEST);

-- DTI-14b: only the mapped entities (LE_01..LE_03) are visible, unmapped entities return nothing
WITH s AS (SELECT LISTAGG(DISTINCT ENTITY_ID, ',') WITHIN GROUP (ORDER BY ENTITY_ID) AS E FROM REPP_AGG_DT_DTI_SUMMARY),
c AS (SELECT COUNT(*) AS N FROM TABLE(REPP_AGG_FN_DTI_COMPARE($LATEST::DATE, $PREV::DATE, ['LE_04','LE_05'], 'STRESSED', 'BUCKETS', TRUE))),
k AS (SELECT COUNT_IF(ENTITY_ID NOT IN ('LE_01', 'LE_02', 'LE_03')) AS N FROM REP_RAW_V001.LIQI_RAW_TB_DTI_SECURITIES)
SELECT 'DTI-14b row access DTI_READ_ONLY' AS TEST_NAME,
       IFF(CURRENT_ROLE() = 'DTI_READ_ONLY' AND s.E = 'LE_01,LE_02,LE_03' AND c.N = 0 AND k.N = 0, 'PASS', 'FAIL') AS STATUS,
       CURRENT_ROLE() || ': entities ' || s.E || ', unmapped compare rows ' || c.N || ', unmapped securities ' || k.N AS DETAIL
FROM s, c, k;
