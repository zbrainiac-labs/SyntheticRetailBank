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
AS
    CALL PAYI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ pay_raw }}.PAYI_RAW_ST_TRANSACTIONS', 5);
