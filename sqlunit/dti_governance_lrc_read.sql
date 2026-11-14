-- ============================================================
-- sqlunit/dti_governance_lrc_read.sql
-- DtI row access as DTI_LRC_READ (all entities, read-only).
-- Run with the role set at login:  snow sql --role DTI_LRC_READ -f ...
-- ============================================================
USE DATABASE AAA_DEV_SYNTHETIC_BANK;
USE SCHEMA REP_AGG_V001;

-- DTI-14c: liquidity risk control sees every entity
SELECT 'DTI-14c row access DTI_LRC_READ' AS TEST_NAME,
       IFF(CURRENT_ROLE() = 'DTI_LRC_READ' AND COUNT(DISTINCT ENTITY_ID) = 9, 'PASS', 'FAIL') AS STATUS,
       CURRENT_ROLE() || ': ' || COUNT(DISTINCT ENTITY_ID) || ' entities' AS DETAIL
FROM REPP_AGG_DT_DTI_SUMMARY;
