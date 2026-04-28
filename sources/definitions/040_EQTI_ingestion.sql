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
    QUANTITY NUMBER(15,4) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Number of shares/units',
    PRICE NUMBER(18,6) NOT NULL COMMENT 'Price per share/unit',
    CURRENCY VARCHAR(3) NOT NULL COMMENT 'Trade currency',
    GROSS_AMOUNT NUMBER(18,2) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Signed gross trade amount (positive for buys, negative for sells)',
    COMMISSION NUMBER(12,4) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Trading commission',
    NET_AMOUNT NUMBER(18,2) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Signed net amount after commission',
    BASE_CURRENCY VARCHAR(3) NOT NULL DEFAULT 'CHF' COMMENT 'Base currency for reporting (CHF)',
    BASE_GROSS_AMOUNT NUMBER(18,2) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Gross amount in CHF',
    BASE_NET_AMOUNT NUMBER(18,2) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Net amount in CHF',
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
AS
    COPY INTO {{ db }}.{{ eqt_raw }}.EQTI_RAW_TB_TRADES (TRADE_DATE, SETTLEMENT_DATE, TRADE_ID, CUSTOMER_ID, ACCOUNT_ID, ORDER_ID, EXEC_ID, SYMBOL, ISIN, SIDE, QUANTITY, PRICE, CURRENCY, GROSS_AMOUNT, COMMISSION, NET_AMOUNT, BASE_CURRENCY, BASE_GROSS_AMOUNT, BASE_NET_AMOUNT, FX_RATE, MARKET, ORDER_TYPE, EXEC_TYPE, TIME_IN_FORCE, BROKER_ID, VENUE)
    FROM @{{ db }}.{{ eqt_raw }}.EQTI_RAW_ST_TRADES
    PATTERN = '.*trades.*\.csv'
    FILE_FORMAT = EQTI_RAW_FF_TRADES_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ eqt_raw }}.EQTI_RAW_TK_CLEANUP_AFTER_LOAD_TRADES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER equity trade data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ eqt_raw }}.EQTI_RAW_TK_LOAD_TRADES
AS
    CALL EQTI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ eqt_raw }}.EQTI_RAW_ST_TRADES', 5);
