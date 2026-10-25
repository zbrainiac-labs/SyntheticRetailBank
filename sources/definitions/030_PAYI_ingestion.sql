/*
 * 030_PAYI_ingestion.sql
 * Payment raw ingestion: transactions, stages, tasks
 */
DEFINE STAGE {{ db }}.{{ pay_raw }}.PAYI_RAW_ST_TRANSACTIONS
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for payment transaction CSV files. Expected pattern: *pay_transactions*.csv with fields: booking_date, value_date, transaction_id, account_id, amount, currency, etc.';

DEFINE TABLE {{ db }}.{{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS (
    BOOKING_DATE TIMESTAMP_NTZ NOT NULL COMMENT 'Transaction timestamp when recorded (ISO 8601 UTC format: YYYY-MM-DDTHH:MM:SS.fffffZ)',
    VALUE_DATE DATE NOT NULL COMMENT 'Date when funds are settled/available (YYYY-MM-DD)',
    TRANSACTION_ID VARCHAR(50) NOT NULL COMMENT 'Unique transaction identifier',
    ACCOUNT_ID VARCHAR(30) NOT NULL COMMENT 'Reference to account ID in {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS',
    AMOUNT DECIMAL(15,2) NOT NULL COMMENT 'Signed transaction amount in original currency (positive = incoming, negative = outgoing)',
    CURRENCY VARCHAR(3) NOT NULL COMMENT 'Transaction currency (USD, EUR, GBP, JPY, CAD, CHF)',
    BASE_AMOUNT DECIMAL(15,2) NOT NULL COMMENT 'Signed transaction amount converted to base currency USD (positive = incoming, negative = outgoing)',
    BASE_CURRENCY VARCHAR(3) NOT NULL COMMENT 'Currency of Account - ISO 4217 currency code',
    FX_RATE DECIMAL(15,6) NOT NULL COMMENT 'Exchange rate used for conversion (from transaction currency to base currency)',
    COUNTERPARTY_ACCOUNT VARCHAR(100) NOT NULL COMMENT 'Counterparty account identifier',
    DESCRIPTION VARCHAR(500) NOT NULL COMMENT 'Transaction description (may contain anomaly indicators in [brackets])',

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_PAYI_RAW_TB_TRANSACTIONS PRIMARY KEY (TRANSACTION_ID),
    CONSTRAINT FK_PAYI_RAW_TB_TRANSACTIONS__ACCI_RAW_TB_ACCOUNTS FOREIGN KEY (ACCOUNT_ID) REFERENCES {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS (ACCOUNT_ID)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Payment transactions with multi-currency support and anomaly detection';

DEFINE TASK {{ db }}.{{ pay_raw }}.PAYI_RAW_TK_LOAD_TRANSACTIONS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ pay_raw }}.PAYI_RAW_SM_TRANSACTION_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS (BOOKING_DATE, VALUE_DATE, TRANSACTION_ID, ACCOUNT_ID, AMOUNT, CURRENCY, BASE_AMOUNT, BASE_CURRENCY, FX_RATE, COUNTERPARTY_ACCOUNT, DESCRIPTION)
    FROM @{{ db }}.{{ pay_raw }}.PAYI_RAW_ST_TRANSACTIONS
    PATTERN = '.*pay_transactions.*\.csv'
    FILE_FORMAT = PAYI_RAW_FF_TRANSACTION_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ pay_raw }}.PAYI_RAW_TK_CLEANUP_AFTER_LOAD_TRANSACTIONS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER payment transaction data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ pay_raw }}.PAYI_RAW_TK_LOAD_TRANSACTIONS
    STARTED
AS
    CALL PAYI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ pay_raw }}.PAYI_RAW_ST_TRANSACTIONS', 5);

-- ============================================================
-- FILE FORMATS (moved from post_deploy.sql)
-- ============================================================

DEFINE FILE FORMAT {{ db }}.{{ pay_raw }}.PAYI_RAW_FF_TRANSACTION_CSV
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
    COMMENT = 'CSV format for payment transaction data with multi-currency support and anomaly detection';

-- ============================================================
-- PROCEDURES (moved from post_deploy.sql)
-- ============================================================

DEFINE PROCEDURE {{ db }}.{{ pay_raw }}.PAYI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N (
    STAGE_NAME STRING,
    KEEP_N     NUMBER
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Generic stage cleanup utility for {{ pay_raw }} schema. Removes oldest files from a stage, keeping only the N most recently modified files. Deletes in batches of 200 (one REMOVE ... PATTERN per batch); a failing REMOVE fails the call.'
EXECUTE AS CALLER
AS
$$
DECLARE
  V_FILES   NUMBER := 0;
  V_BATCHES NUMBER := 0;
  V_PATTERN STRING;
  RS RESULTSET;
BEGIN
  -- One REMOVE ... PATTERN per batch of 200 files instead of one REMOVE per file
  -- (a backlog of ~7,000 files took 15+ minutes as single statements).
  EXECUTE IMMEDIATE 'SELECT RELATIVE_PATH, ROW_NUMBER() OVER (ORDER BY LAST_MODIFIED DESC) AS RN FROM DIRECTORY(@' || STAGE_NAME || ')';
  RS := (
    SELECT LISTAGG(REGEXP_REPLACE(RELATIVE_PATH, '([.^$|()\\[\\]{}*+?\\\\])', '\\\\\\1'), '|') AS PAT,
           COUNT(*) AS N
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE RN > :KEEP_N
    GROUP BY FLOOR((RN - :KEEP_N - 1) / 200)
  );
  FOR R IN RS DO
    -- Regex-escaped exact file names; errors are not swallowed so a failing cleanup fails the task
    V_PATTERN := '(.*/)?(' || R.PAT || ')';
    EXECUTE IMMEDIATE 'REMOVE @' || STAGE_NAME || ' PATTERN = ''' || REPLACE(REPLACE(V_PATTERN, '\\', '\\\\'), '''', '\\''') || '''';
    V_FILES   := V_FILES + R.N;
    V_BATCHES := V_BATCHES + 1;
  END FOR;

  RETURN 'Stage cleanup completed. Deleted: ' || V_FILES || ' files in ' || V_BATCHES || ' batches, Kept: ' || KEEP_N || ' most recent files.';
END;
$$;

-- ============================================================
-- STREAMS (moved from post_deploy.sql)
-- ============================================================

DEFINE STREAM {{ db }}.{{ pay_raw }}.PAYI_RAW_SM_TRANSACTION_FILES
    ON STAGE {{ db }}.{{ pay_raw }}.PAYI_RAW_ST_TRANSACTIONS
    COMMENT = 'Monitors PAYI_RAW_ST_TRANSACTIONS stage for new payment transaction CSV files. Triggers PAYI_RAW_TK_LOAD_TRANSACTIONS when files matching *pay_transactions*.csv pattern are detected';
