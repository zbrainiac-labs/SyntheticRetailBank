/*
 * 040_EQTI_ingestion.sql
 * Equity raw ingestion: trades, market data, stages, tasks
 */
DEFINE STAGE {{ db }}.{{ eqt_raw }}.EQTI_RAW_ST_TRADES
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for equity trade CSV files. Expected pattern: *trades*.csv with fields: trade_date, trade_id, customer_id, account_id, symbol, side, quantity, price, etc.';

DEFINE TABLE {{ db }}.{{ eqt_raw }}.EQTI_RAW_TB_TRADES (
    TRADE_DATE TIMESTAMP_NTZ NOT NULL COMMENT 'Trade execution timestamp (ISO 8601 UTC format)',
    SETTLEMENT_DATE DATE NOT NULL COMMENT 'Settlement date (YYYY-MM-DD)',
    TRADE_ID VARCHAR(50) NOT NULL COMMENT 'Unique trade identifier',
    CUSTOMER_ID VARCHAR(30) NOT NULL COMMENT 'Reference to customer',
    ACCOUNT_ID VARCHAR(30) NOT NULL COMMENT 'Investment account used for settlement (References {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS.ACCOUNT_ID where ACCOUNT_TYPE = ''INVESTMENT'')',
    ORDER_ID VARCHAR(50) NOT NULL COMMENT 'Order reference',
    EXEC_ID VARCHAR(50) NOT NULL COMMENT 'Execution reference',
    SYMBOL VARCHAR(20) NOT NULL COMMENT 'Stock symbol',
    ISIN VARCHAR(12) COMMENT 'International Securities Identification Number',
    SIDE CHAR(1) NOT NULL COMMENT 'FIX protocol side (1=Buy, 2=Sell)',
    QUANTITY NUMBER(15,4) NOT NULL COMMENT 'Number of shares/units',
    PRICE NUMBER(18,6) NOT NULL COMMENT 'Price per share/unit',
    CURRENCY VARCHAR(3) NOT NULL COMMENT 'Trade currency',
    GROSS_AMOUNT NUMBER(18,2) NOT NULL COMMENT 'Signed gross trade amount (positive for buys, negative for sells)',
    COMMISSION NUMBER(12,4) NOT NULL COMMENT 'Trading commission',
    NET_AMOUNT NUMBER(18,2) NOT NULL COMMENT 'Signed net amount after commission',
    BASE_CURRENCY VARCHAR(3) NOT NULL DEFAULT 'CHF' COMMENT 'Base currency for reporting (CHF)',
    BASE_GROSS_AMOUNT NUMBER(18,2) NOT NULL COMMENT 'Gross amount in CHF',
    BASE_NET_AMOUNT NUMBER(18,2) NOT NULL COMMENT 'Net amount in CHF',
    FX_RATE NUMBER(12,6) NOT NULL COMMENT 'Exchange rate to CHF',
    MARKET VARCHAR(10) NOT NULL COMMENT 'Exchange/market (NYSE, LSE, XETRA, etc.)',
    ORDER_TYPE VARCHAR(15) NOT NULL COMMENT 'Order type (MARKET, LIMIT, STOP, etc.)',
    EXEC_TYPE VARCHAR(15) NOT NULL COMMENT 'Execution type (NEW, PARTIAL_FILL, FILL, etc.)',
    TIME_IN_FORCE VARCHAR(10) COMMENT 'Time in force (DAY, GTC, IOC, etc.)',
    BROKER_ID VARCHAR(20) COMMENT 'Executing broker',
    VENUE VARCHAR(20) COMMENT 'Trading venue',

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_EQTI_RAW_TB_TRADES PRIMARY KEY (TRADE_ID),
    CONSTRAINT FK_EQTI_RAW_TB_EQUITY_TRADES__ACCI_RAW_TB_ACCOUNTS FOREIGN KEY (ACCOUNT_ID) REFERENCES {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS(ACCOUNT_ID)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Equity trades via FIX protocol with CHF as base currency. Uses INVESTMENT accounts from {{ db }}.{{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS. Signed amounts: positive for purchases, negative for sales.';

DEFINE TASK {{ db }}.{{ eqt_raw }}.EQTI_RAW_TK_LOAD_TRADES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ eqt_raw }}.EQTI_RAW_SM_TRADES_FILES')
    STARTED
AS
    EXECUTE IMMEDIATE $$
    BEGIN
        COPY INTO {{ db }}.{{ eqt_raw }}.EQTI_RAW_TB_TRADES (TRADE_DATE, SETTLEMENT_DATE, TRADE_ID, CUSTOMER_ID, ACCOUNT_ID, ORDER_ID, EXEC_ID, SYMBOL, ISIN, SIDE, QUANTITY, PRICE, CURRENCY, GROSS_AMOUNT, COMMISSION, NET_AMOUNT, BASE_CURRENCY, BASE_GROSS_AMOUNT, BASE_NET_AMOUNT, FX_RATE, MARKET, ORDER_TYPE, EXEC_TYPE, TIME_IN_FORCE, BROKER_ID, VENUE)
        FROM @{{ db }}.{{ eqt_raw }}.EQTI_RAW_ST_TRADES
        PATTERN = '.*trades.*\.csv'
        FILE_FORMAT = EQTI_RAW_FF_TRADES_CSV
        ON_ERROR = CONTINUE;
        -- consume the file stream (offset only): it is empty again and does not go stale
        CREATE OR REPLACE TEMPORARY TABLE STREAM_ADVANCE AS SELECT RELATIVE_PATH FROM {{ db }}.{{ eqt_raw }}.EQTI_RAW_SM_TRADES_FILES WHERE 0 = 1;
    END;
    $$;

DEFINE TASK {{ db }}.{{ eqt_raw }}.EQTI_RAW_TK_CLEANUP_AFTER_LOAD_TRADES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER equity trade data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ eqt_raw }}.EQTI_RAW_TK_LOAD_TRADES
    STARTED
AS
    CALL EQTI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ eqt_raw }}.EQTI_RAW_ST_TRADES', 5);

-- ============================================================
-- FILE FORMATS (moved from post_deploy.sql)
-- ============================================================

DEFINE FILE FORMAT {{ db }}.{{ eqt_raw }}.EQTI_RAW_FF_TRADES_CSV
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
    COMMENT = 'CSV format for equity trade data with FIX protocol compliance and currency conversion support';

-- ============================================================
-- PROCEDURES (moved from post_deploy.sql)
-- ============================================================

DEFINE PROCEDURE {{ db }}.{{ eqt_raw }}.EQTI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N (
    STAGE_NAME STRING,
    KEEP_N     NUMBER
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Generic stage cleanup utility for {{ eqt_raw }} schema. Removes oldest files from a stage, keeping only the N most recently modified files. Deletes in batches of 200 (one REMOVE ... PATTERN per batch); a failing REMOVE fails the call.'
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

DEFINE STREAM {{ db }}.{{ eqt_raw }}.EQTI_RAW_SM_TRADES_FILES
    ON STAGE {{ db }}.{{ eqt_raw }}.EQTI_RAW_ST_TRADES
    COMMENT = 'Monitors EQTI_RAW_ST_TRADES stage for new equity trade CSV files. Triggers EQTI_RAW_TK_LOAD_TRADES when files matching *trades*.csv pattern are detected';
