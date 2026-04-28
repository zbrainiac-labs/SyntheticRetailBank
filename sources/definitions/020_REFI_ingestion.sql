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

    CONSTRAINT PK_{{ ref_raw }}.REFI_RAW_TB_FX_RATES PRIMARY KEY (DATE, FROM_CURRENCY, TO_CURRENCY)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily foreign exchange rates with realistic bid/ask spreads';

DEFINE TASK {{ db }}.{{ ref_raw }}.REFI_RAW_TK_LOAD_FX_RATES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ ref_raw }}.REFI_RAW_SM_FX_RATE_FILES')
AS
    COPY INTO {{ ref_raw }}.REFI_RAW_TB_FX_RATES (DATE, FROM_CURRENCY, TO_CURRENCY, MID_RATE, BID_RATE, ASK_RATE)
    FROM @{{ ref_raw }}.REFI_RAW_ST_FX_RATES
    PATTERN = '.*fx_rates.*\.csv'
    FILE_FORMAT = REFI_RAW_FF_FX_RATES_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ ref_raw }}.REFI_RAW_TK_CLEANUP_AFTER_LOAD_FX_RATES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER FX rates data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ ref_raw }}.REFI_RAW_TK_LOAD_FX_RATES
AS
    CALL REFI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ ref_raw }}.REFI_RAW_ST_FX_RATES', 5);
