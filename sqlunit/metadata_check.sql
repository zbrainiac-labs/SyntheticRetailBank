-- ============================================================
-- sqlunit/metadata_check.sql
-- Metadata completeness gate for the LCR domain: every LIQI/LCR table, view,
-- dynamic table and column must carry a meaningful comment (>= 20 chars).
-- COREP_* objects are managed outside this repository and are excluded.
-- Returns TEST_NAME, STATUS, DETAIL (lists missing objects/columns).
-- ============================================================
USE DATABASE AAA_DEV_SYNTHETIC_BANK;

SELECT 'META-01 object comments' AS TEST_NAME,
       IFF(COUNT_IF(COALESCE(LENGTH(COMMENT), 0) < 20) = 0, 'PASS', 'FAIL') AS STATUS,
       COUNT(*) || ' objects; missing: ' || COALESCE(LISTAGG(IFF(COALESCE(LENGTH(COMMENT), 0) < 20, TABLE_NAME, NULL), ', '), 'none') AS DETAIL
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('REP_RAW_V001', 'REP_AGG_V001')
  AND (TABLE_NAME LIKE 'LIQI%' OR TABLE_NAME LIKE '%LCR%')
  AND TABLE_NAME NOT LIKE 'COREP%';

SELECT 'META-02 column comments' AS TEST_NAME,
       IFF(COUNT_IF(COALESCE(LENGTH(c.COMMENT), 0) < 20) = 0, 'PASS', 'FAIL'),
       COUNT(*) || ' columns; missing: ' || COALESCE(LISTAGG(IFF(COALESCE(LENGTH(c.COMMENT), 0) < 20, c.TABLE_NAME || '.' || c.COLUMN_NAME, NULL), ', '), 'none')
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA IN ('REP_RAW_V001', 'REP_AGG_V001')
  AND (c.TABLE_NAME LIKE 'LIQI%' OR c.TABLE_NAME LIKE '%LCR%')
  AND c.TABLE_NAME NOT LIKE 'COREP%';
