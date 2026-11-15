-- ============================================================
-- operation/demo_reset.sql - DEV ONLY
-- Puts the LCR + DtI showcase back into its generated state after a demo or a rehearsal:
--   1. removes the manual corrections entered in the app during the demo (the generated ones,
--      CORR-0001 / CORR-0002 by LRCU_ANALYST, stay)
--   2. refreshes the DtI dynamic tables so the pages show the generated figures again
--   3. suspends the LIQI load tasks (a DCM deploy resumes them; the book must not change mid-demo)
-- Data, the generation run and the LCR engine are not touched.
--
-- Run:   snow sql -c <conn> --role CICD -f operation/demo_reset.sql
-- Check: snow sql -c <conn> --role CICD -f operation/demo_check.sql
-- ============================================================
USE DATABASE AAA_DEV_SYNTHETIC_BANK;
USE SCHEMA REP_RAW_V001;

-- guard: never against another database
EXECUTE IMMEDIATE $$
BEGIN
    IF (CURRENT_DATABASE() <> 'AAA_DEV_SYNTHETIC_BANK') THEN
        RAISE STATEMENT_ERROR;
    END IF;
    RETURN 'DEV guard ok';
END;
$$;

-- 1. corrections entered in the app (CREATED_BY = the app user, ids CORR-<timestamp>)
DELETE FROM LIQI_RAW_TB_DTI_CORRECTIONS WHERE COALESCE(CREATED_BY, '') <> 'LRCU_ANALYST';

-- 2. DtI engine back on the generated book
ALTER DYNAMIC TABLE AAA_DEV_SYNTHETIC_BANK.REP_AGG_V001.REPP_AGG_DT_DTI_CASHFLOWS REFRESH;
ALTER DYNAMIC TABLE AAA_DEV_SYNTHETIC_BANK.REP_AGG_V001.REPP_AGG_DT_DTI_PROJECTION REFRESH;
ALTER DYNAMIC TABLE AAA_DEV_SYNTHETIC_BANK.REP_AGG_V001.REPP_AGG_DT_DTI_SUMMARY REFRESH;

-- 3. suspend the LIQI load tasks (roots first, then the children)
EXECUTE IMMEDIATE $$
DECLARE
    n INT DEFAULT 0;
    rs RESULTSET DEFAULT (SHOW TASKS LIKE 'LIQI_RAW_TK_%' IN SCHEMA AAA_DEV_SYNTHETIC_BANK.REP_RAW_V001);
    c CURSOR FOR SELECT "name" AS NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "state" = 'started'
                 ORDER BY IFF(ARRAY_SIZE("predecessors") = 0, 0, 1);
BEGIN
    FOR r IN c DO
        EXECUTE IMMEDIATE 'ALTER TASK AAA_DEV_SYNTHETIC_BANK.REP_RAW_V001.' || r.NAME || ' SUSPEND';
        n := n + 1;
    END FOR;
    RETURN n || ' LIQI tasks suspended';
END;
$$;

-- 4. control: only the generated corrections are left
SELECT 'demo reset' AS STEP, COUNT(*) AS CORRECTIONS, LISTAGG(CORRECTION_ID, ', ') WITHIN GROUP (ORDER BY CORRECTION_ID) AS IDS
FROM LIQI_RAW_TB_DTI_CORRECTIONS;
