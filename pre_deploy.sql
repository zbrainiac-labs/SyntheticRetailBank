-- ============================================================
-- pre_deploy.sql - bootstrap per environment (runs as CICD; CI runs it on every push)
-- Creates what the DCM project needs before it can run: the database, the AAA_DCM
-- schema that holds the DCM project, the project itself and the shared warehouse.
-- CICD owns them, like every object the project deploys. These stay outside DCM on
-- purpose: a project cannot manage the containers it lives in, and the warehouse is shared.
-- Account-level grants for CICD (ACCOUNTADMIN) live in operation/setup_listings.sql.
-- Idempotent: safe to re-run.
--
-- snow sql -c <conn> --role CICD -f pre_deploy.sql \
--   --variable "db=AAA_DEV_SYNTHETIC_BANK" --variable "wh=MD_TEST_WH" --enable-templating JINJA
-- ============================================================

USE ROLE CICD;

CREATE DATABASE IF NOT EXISTS {{ db }}
    COMMENT = 'Synthetic Retail Bank - Snowflake demo database (managed by the DCM project AAA_DCM.SYNTHETIC_RETAIL_BANK)';

CREATE SCHEMA IF NOT EXISTS {{ db }}.AAA_DCM;

CREATE DCM PROJECT IF NOT EXISTS {{ db }}.AAA_DCM.SYNTHETIC_RETAIL_BANK
;

CREATE WAREHOUSE IF NOT EXISTS {{ wh }}
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'
    AUTO_SUSPEND = 5
    AUTO_RESUME = true
    COMMENT = 'Development and testing warehouse - X-SMALL size with auto-suspend for cost optimization';
