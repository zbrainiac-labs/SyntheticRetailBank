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
