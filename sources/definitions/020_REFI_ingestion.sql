/*
 * 020_REFI_ingestion.sql
 * Reference data raw ingestion: FX rates, stages, tasks
 */
DEFINE STAGE {{ db }}.{{ ref_raw }}.REFI_RAW_ST_FX_RATES
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for FX rates CSV files. Expected pattern: *fx_rates*.csv with fields: date, from_currency, to_currency, mid_rate, bid_rate, ask_rate';

DEFINE TABLE {{ db }}.{{ ref_raw }}.REFI_RAW_TB_FX_RATES (
    DATE DATE NOT NULL COMMENT 'Rate date (YYYY-MM-DD)',
    FROM_CURRENCY VARCHAR(3) NOT NULL COMMENT 'Source currency',
    TO_CURRENCY VARCHAR(3) NOT NULL COMMENT 'Target currency',
    MID_RATE DECIMAL(15,6) NOT NULL COMMENT 'Mid-market exchange rate',
    BID_RATE DECIMAL(15,6) NOT NULL COMMENT 'Bid exchange rate (bank buys at this rate)',
    ASK_RATE DECIMAL(15,6) NOT NULL COMMENT 'Ask exchange rate (bank sells at this rate)',

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_REFI_RAW_TB_FX_RATES PRIMARY KEY (DATE, FROM_CURRENCY, TO_CURRENCY)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily foreign exchange rates with realistic bid/ask spreads';

DEFINE TASK {{ db }}.{{ ref_raw }}.REFI_RAW_TK_LOAD_FX_RATES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ ref_raw }}.REFI_RAW_SM_FX_RATE_FILES')
    STARTED
AS
    EXECUTE IMMEDIATE $$
    BEGIN
        COPY INTO {{ db }}.{{ ref_raw }}.REFI_RAW_TB_FX_RATES (DATE, FROM_CURRENCY, TO_CURRENCY, MID_RATE, BID_RATE, ASK_RATE)
        FROM @{{ db }}.{{ ref_raw }}.REFI_RAW_ST_FX_RATES
        PATTERN = '.*fx_rates.*\.csv'
        FILE_FORMAT = REFI_RAW_FF_FX_RATES_CSV
        ON_ERROR = CONTINUE;
        -- consume the file stream (offset only): it is empty again and does not go stale
        CREATE OR REPLACE TEMPORARY TABLE STREAM_ADVANCE AS SELECT RELATIVE_PATH FROM {{ db }}.{{ ref_raw }}.REFI_RAW_SM_FX_RATE_FILES WHERE 0 = 1;
    END;
    $$;

DEFINE TASK {{ db }}.{{ ref_raw }}.REFI_RAW_TK_CLEANUP_AFTER_LOAD_FX_RATES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER FX rates data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ ref_raw }}.REFI_RAW_TK_LOAD_FX_RATES
    STARTED
AS
    CALL REFI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ ref_raw }}.REFI_RAW_ST_FX_RATES', 5);

-- ============================================================
-- FILE FORMATS (moved from post_deploy.sql)
-- ============================================================

DEFINE FILE FORMAT {{ db }}.{{ ref_raw }}.REFI_RAW_FF_FX_RATES_CSV
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
    COMMENT = 'CSV format for FX rates data with bid/ask spreads and currency conversion support';

-- ============================================================
-- PROCEDURES (moved from post_deploy.sql)
-- ============================================================

DEFINE PROCEDURE {{ db }}.{{ ref_raw }}.REFI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N (
    STAGE_NAME STRING,
    KEEP_N     NUMBER
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Generic stage cleanup utility for {{ ref_raw }} schema. Removes oldest files from a stage, keeping only the N most recently modified files. Deletes in batches of 200 (one REMOVE ... PATTERN per batch); a failing REMOVE fails the call.'
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

DEFINE STREAM {{ db }}.{{ ref_raw }}.REFI_RAW_SM_FX_RATE_FILES
    ON STAGE {{ db }}.{{ ref_raw }}.REFI_RAW_ST_FX_RATES
    COMMENT = 'Monitors REFI_RAW_ST_FX_RATES stage for new FX rates CSV files. Triggers REFI_RAW_TK_LOAD_FX_RATES when files matching *fx_rates*.csv pattern are detected';
