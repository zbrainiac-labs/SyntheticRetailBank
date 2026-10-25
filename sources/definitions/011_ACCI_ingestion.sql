/*
 * 011_ACCI_ingestion.sql
 * Account raw ingestion: accounts, stages, tasks
 */
DEFINE STAGE {{ db }}.{{ crm_raw }}.ACCI_RAW_ST_ACCOUNTS
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for account master data CSV files. Expected pattern: *accounts*.csv with multi-currency support (EUR, GBP, USD, CHF, etc.)';

DEFINE TABLE {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS (
    ACCOUNT_ID VARCHAR(30) NOT NULL COMMENT 'Unique account identifier (CUSTOMER_ID_ACCOUNT_TYPE_XX format)',
    ACCOUNT_TYPE VARCHAR(20) NOT NULL COMMENT 'Type of account (CHECKING, SAVINGS, BUSINESS, INVESTMENT)',
    BASE_CURRENCY VARCHAR(3) NOT NULL COMMENT 'Account base currency (EUR, GBP, USD, CHF, NOK, SEK, DKK)',
    CUSTOMER_ID VARCHAR(30) NOT NULL COMMENT 'Reference to customer (foreign key to CRMI_RAW_001.CRMI_RAW_TB_CUSTOMER)',
    STATUS VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' COMMENT 'Account status (ACTIVE, INACTIVE, CLOSED, SUSPENDED)',

    CONSTRAINT PK_ACCI_RAW_TB_ACCOUNTS PRIMARY KEY (ACCOUNT_ID)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Account master data table supporting multi-currency retail banking operations. Each customer can have multiple accounts of different types. Investment accounts are used for equity trading settlement. FK to {{ db }}.{{ crm_raw }}.CRMI_RAW_TB_CUSTOMER removed due to SCD Type 2 composite PK.';

DEFINE TASK {{ db }}.{{ crm_raw }}.ACCI_RAW_TK_LOAD_ACCOUNTS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ crm_raw }}.ACCI_RAW_SM_ACCOUNT_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS (ACCOUNT_ID, ACCOUNT_TYPE, BASE_CURRENCY, CUSTOMER_ID, STATUS)
    FROM @{{ db }}.{{ crm_raw }}.ACCI_RAW_ST_ACCOUNTS
    PATTERN = '.*accounts.*\.csv'
    FILE_FORMAT = ACCI_RAW_FF_ACCOUNT_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ crm_raw }}.ACCI_RAW_TK_CLEANUP_AFTER_LOAD_ACCOUNTS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER account data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ crm_raw }}.ACCI_RAW_TK_LOAD_ACCOUNTS
    STARTED
AS
    CALL CRMI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ crm_raw }}.ACCI_RAW_ST_ACCOUNTS', 5);

-- ============================================================
-- FILE FORMATS (moved from post_deploy.sql)
-- ============================================================

DEFINE FILE FORMAT {{ db }}.{{ crm_raw }}.ACCI_RAW_FF_ACCOUNT_CSV
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"'
    COMMENT = 'CSV format for account master data files with multi-currency support. Expected columns: account_id, account_type, base_currency, customer_id, status';

-- ============================================================
-- STREAMS (moved from post_deploy.sql)
-- ============================================================

DEFINE STREAM {{ db }}.{{ crm_raw }}.ACCI_RAW_SM_ACCOUNT_FILES
    ON STAGE {{ db }}.{{ crm_raw }}.ACCI_RAW_ST_ACCOUNTS
    COMMENT = 'Monitors ACCI_RAW_ST_ACCOUNTS stage for new account CSV files. Triggers ACCI_RAW_TK_LOAD_ACCOUNTS when files matching *accounts*.csv pattern are detected';
