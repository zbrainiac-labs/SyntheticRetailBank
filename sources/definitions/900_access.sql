/*
 * 900_access.sql
 * Access control: role-based grants
 */
GRANT USAGE ON DATABASE {{ db }} TO ROLE PUBLIC;

GRANT USAGE ON SCHEMA {{ db }}.{{ cmd_agg }} TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA {{ db }}.{{ crm_agg }} TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA {{ db }}.{{ eqt_agg }} TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA {{ db }}.{{ fii_agg }} TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA {{ db }}.{{ loa_agg }} TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA {{ db }}.{{ pay_agg }} TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA {{ db }}.{{ ref_agg }} TO ROLE PUBLIC;

GRANT USAGE ON SCHEMA {{ db }}.{{ rep_agg }} TO ROLE PUBLIC;
