/*
 * 050_FIII_ingestion.sql
 * Fixed income raw ingestion: trades, stages, tasks
 */
DEFINE STAGE {{ db }}.{{ fii_raw }}.FIII_RAW_ST_TRADES
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for fixed income trade CSV files. Expected pattern: *fixed_income_trades*.csv with fields: trade_date, trade_id, customer_id, account_id, instrument_type, notional, price, etc.';

DEFINE TABLE {{ db }}.{{ fii_raw }}.FIII_RAW_TB_TRADES (
    TRADE_DATE TIMESTAMP_NTZ COMMENT 'Trade execution timestamp',
    SETTLEMENT_DATE DATE COMMENT 'Settlement date for cash/securities transfer',
    TRADE_ID VARCHAR(50) PRIMARY KEY COMMENT 'Unique trade identifier',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer who executed the trade',
    ACCOUNT_ID VARCHAR(30) COMMENT 'Investment account used for settlement',
    ORDER_ID VARCHAR(50) COMMENT 'Order reference for trade grouping',

    INSTRUMENT_TYPE VARCHAR(10) COMMENT 'BOND or IRS (Interest Rate Swap)',
    INSTRUMENT_ID VARCHAR(50) COMMENT 'ISIN for bonds, swap ID for swaps',
    ISSUER VARCHAR(100) COMMENT 'Bond issuer or swap counterparty',
    ISSUER_TYPE VARCHAR(20) COMMENT 'SOVEREIGN, CORPORATE, or SUPRANATIONAL',
    CURRENCY VARCHAR(3) COMMENT 'Trade currency (CHF, EUR, USD, GBP)',
    SIDE VARCHAR(1) COMMENT '1=Buy/Pay, 2=Sell/Receive (FIX standard)',

    NOTIONAL FLOAT COMMENT 'Notional amount (face value)',
    PRICE FLOAT COMMENT 'Clean price (as % of par for bonds, rate for swaps)',
    ACCRUED_INTEREST FLOAT COMMENT 'Accrued interest amount (bonds only)',
    GROSS_AMOUNT FLOAT COMMENT 'Signed gross amount: price * notional + accrued',
    COMMISSION FLOAT COMMENT 'Trading commission',
    NET_AMOUNT FLOAT COMMENT 'Signed net amount: gross_amount +/- commission',

    BASE_CURRENCY VARCHAR(3) COMMENT 'Base reporting currency (CHF)',
    BASE_GROSS_AMOUNT FLOAT COMMENT 'Gross amount in CHF',
    BASE_NET_AMOUNT FLOAT COMMENT 'Net amount in CHF',
    FX_RATE FLOAT COMMENT 'Exchange rate used for conversion to CHF',

    FIXED_RATE FLOAT COMMENT 'Fixed rate for IRS (annual %)',
    FLOATING_RATE_INDEX VARCHAR(20) COMMENT 'SARON, EURIBOR, SOFR, SONIA',
    TENOR_YEARS FLOAT COMMENT 'Swap maturity in years',

    COUPON_RATE FLOAT COMMENT 'Annual coupon rate (%)',
    MATURITY_DATE DATE COMMENT 'Instrument maturity date',
    DURATION FLOAT COMMENT 'Modified duration (years) - interest rate sensitivity',
    DV01 FLOAT COMMENT 'Dollar value of 1bp move in base currency',
    CREDIT_RATING VARCHAR(10) COMMENT 'AAA, AA, A, BBB, BB, B, CCC',
    CREDIT_SPREAD_BPS FLOAT COMMENT 'Spread over risk-free rate (basis points)',

    MARKET VARCHAR(50) COMMENT 'Trading venue/exchange',
    BROKER_ID VARCHAR(50) COMMENT 'Executing broker identifier',
    VENUE VARCHAR(50) COMMENT 'Trading venue',
    LIQUIDITY_SCORE FLOAT COMMENT '1-10 scale for NMRF classification (1=illiquid, 10=liquid)',

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Raw fixed income trades (bonds and swaps) with FRTB risk metrics';

DEFINE TASK {{ db }}.{{ fii_raw }}.FIII_RAW_TK_LOAD_TRADES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ fii_raw }}.FIII_RAW_SM_TRADES_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ fii_raw }}.FIII_RAW_TB_TRADES (
        TRADE_DATE, SETTLEMENT_DATE, TRADE_ID, CUSTOMER_ID, ACCOUNT_ID, ORDER_ID,
        INSTRUMENT_TYPE, INSTRUMENT_ID, ISSUER, ISSUER_TYPE, CURRENCY, SIDE,
        NOTIONAL, PRICE, ACCRUED_INTEREST, GROSS_AMOUNT, FIXED_RATE, FLOATING_RATE_INDEX, TENOR_YEARS,
        COMMISSION, NET_AMOUNT, BASE_CURRENCY, BASE_GROSS_AMOUNT, BASE_NET_AMOUNT, FX_RATE,
        COUPON_RATE, MATURITY_DATE, DURATION, DV01, CREDIT_RATING, CREDIT_SPREAD_BPS,
        MARKET, BROKER_ID, VENUE, LIQUIDITY_SCORE
    )
    FROM @{{ db }}.{{ fii_raw }}.FIII_RAW_ST_TRADES
    PATTERN = '.*fixed_income_trades.*\.csv'
    FILE_FORMAT = FIII_RAW_FF_TRADES_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ fii_raw }}.FIII_RAW_TK_CLEANUP_AFTER_LOAD_TRADES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER fixed income trade data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ fii_raw }}.FIII_RAW_TK_LOAD_TRADES
    STARTED
AS
    CALL FIII_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ fii_raw }}.FIII_RAW_ST_TRADES', 5);

-- ============================================================
-- FILE FORMATS (moved from post_deploy.sql)
-- ============================================================

DEFINE FILE FORMAT {{ db }}.{{ fii_raw }}.FIII_RAW_FF_TRADES_CSV
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
    COMMENT = 'CSV format for fixed income trade data with FRTB risk metrics and currency conversion support';

-- ============================================================
-- PROCEDURES (moved from post_deploy.sql)
-- ============================================================

DEFINE PROCEDURE {{ db }}.{{ fii_raw }}.FIII_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N (
    STAGE_NAME STRING,
    KEEP_N     NUMBER
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Generic stage cleanup utility for {{ fii_raw }} schema. Removes oldest files from a stage, keeping only the N most recently modified files. Deletes in batches of 200 (one REMOVE ... PATTERN per batch); a failing REMOVE fails the call.'
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

DEFINE STREAM {{ db }}.{{ fii_raw }}.FIII_RAW_SM_TRADES_FILES
    ON STAGE {{ db }}.{{ fii_raw }}.FIII_RAW_ST_TRADES
    COMMENT = 'Monitors FIII_RAW_ST_TRADES stage for new fixed income trade CSV files. Triggers FIII_RAW_TK_LOAD_TRADES when files matching *fixed_income_trades*.csv pattern are detected';
