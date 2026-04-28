DEFINE STAGE {{ db }}.{{ cmd_raw }}.CMDI_RAW_ST_TRADES
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for commodity trade CSV files. Expected pattern: *commodity_trades*.csv with fields: trade_date, trade_id, customer_id, account_id, commodity_type, quantity, price, etc.';

DEFINE TABLE {{ db }}.{{ cmd_raw }}.CMDI_RAW_TB_TRADES (
    TRADE_DATE TIMESTAMP_NTZ COMMENT 'Trade execution timestamp',
    SETTLEMENT_DATE DATE COMMENT 'Settlement/delivery date',
    TRADE_ID VARCHAR(50) PRIMARY KEY COMMENT 'Unique trade identifier',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer who executed the trade',
    ACCOUNT_ID VARCHAR(30) COMMENT 'Investment account used for settlement',
    ORDER_ID VARCHAR(50) COMMENT 'Order reference for trade grouping',

    COMMODITY_TYPE VARCHAR(20) COMMENT 'ENERGY, PRECIOUS_METAL, BASE_METAL, AGRICULTURAL',
    COMMODITY_NAME VARCHAR(50) COMMENT 'Crude Oil WTI, Gold, Copper, Wheat, etc.',
    COMMODITY_CODE VARCHAR(10) COMMENT 'WTI, XAU, HG, ZW, etc.',
    CONTRACT_TYPE VARCHAR(10) COMMENT 'SPOT, FUTURE, FORWARD, SWAP',

    SIDE VARCHAR(1) COMMENT '1=Buy, 2=Sell (FIX standard)',
    QUANTITY FLOAT WITH TAG (SENSITIVITY_LEVEL='restricted') COMMENT 'Quantity in commodity units',
    UNIT VARCHAR(20) COMMENT 'Barrel, Troy Ounce, Metric Ton, Bushel, etc.',
    PRICE FLOAT COMMENT 'Price per unit in trade currency',
    CURRENCY VARCHAR(3) COMMENT 'Trading currency (USD, EUR, GBP, CHF)',

    GROSS_AMOUNT FLOAT WITH TAG (SENSITIVITY_LEVEL='restricted') COMMENT 'Signed gross amount: quantity * price',
    COMMISSION FLOAT WITH TAG (SENSITIVITY_LEVEL='restricted') COMMENT 'Trading commission',
    NET_AMOUNT FLOAT WITH TAG (SENSITIVITY_LEVEL='restricted') COMMENT 'Signed net amount: gross_amount +/- commission',

    BASE_CURRENCY VARCHAR(3) COMMENT 'Base reporting currency (CHF)',
    BASE_GROSS_AMOUNT FLOAT WITH TAG (SENSITIVITY_LEVEL='restricted') COMMENT 'Gross amount in CHF',
    BASE_NET_AMOUNT FLOAT COMMENT 'Net amount in CHF',
    FX_RATE FLOAT COMMENT 'Exchange rate used for conversion to CHF',

    CONTRACT_SIZE FLOAT COMMENT 'Standard contract size',
    NUM_CONTRACTS FLOAT COMMENT 'Number of contracts',
    DELIVERY_MONTH VARCHAR(7) COMMENT 'Delivery month (YYYY-MM format)',
    DELIVERY_LOCATION VARCHAR(100) COMMENT 'Physical delivery location/hub',

    DELTA FLOAT COMMENT 'Price sensitivity (change in value for $1 move in commodity)',
    VEGA FLOAT COMMENT 'Volatility sensitivity (for options, if applicable)',
    SPOT_PRICE FLOAT COMMENT 'Current spot price',
    FORWARD_PRICE FLOAT COMMENT 'Forward/futures price',
    VOLATILITY FLOAT COMMENT 'Price volatility (%)',
    LIQUIDITY_SCORE FLOAT COMMENT '1-10 scale for NMRF classification (1=illiquid, 10=liquid)',

    EXCHANGE VARCHAR(20) COMMENT 'Trading exchange (CME, ICE, LME, NYMEX, CBOT)',
    BROKER_ID VARCHAR(50) COMMENT 'Executing broker identifier',
    VENUE VARCHAR(50) COMMENT 'Trading venue',

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Raw commodity trades (energy, metals, agricultural) with FRTB risk metrics';

DEFINE TASK {{ db }}.{{ cmd_raw }}.CMDI_RAW_TK_LOAD_TRADES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ cmd_raw }}.CMDI_RAW_SM_TRADES_FILES')
AS
    COPY INTO CMDI_RAW_TB_TRADES (
        TRADE_DATE, SETTLEMENT_DATE, TRADE_ID, CUSTOMER_ID, ACCOUNT_ID, ORDER_ID,
        COMMODITY_TYPE, COMMODITY_NAME, COMMODITY_CODE, CONTRACT_TYPE, SIDE, QUANTITY, UNIT, PRICE, CURRENCY,
        GROSS_AMOUNT, COMMISSION, NET_AMOUNT, BASE_CURRENCY, BASE_GROSS_AMOUNT, BASE_NET_AMOUNT, FX_RATE,
        CONTRACT_SIZE, NUM_CONTRACTS, DELIVERY_MONTH, DELIVERY_LOCATION, DELTA, VEGA, SPOT_PRICE, FORWARD_PRICE, VOLATILITY,
        EXCHANGE, BROKER_ID, VENUE, LIQUIDITY_SCORE
    )
    FROM @CMDI_RAW_ST_TRADES
    PATTERN = '.*commodity_trades.*\.csv'
    FILE_FORMAT = CMDI_RAW_FF_TRADES_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ cmd_raw }}.CMDI_RAW_TK_CLEANUP_AFTER_LOAD_TRADES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER {{ db }}.{{ cmd_raw }}.commodity trade data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ cmd_raw }}.CMDI_RAW_TK_LOAD_TRADES
AS
    CALL CMDI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('CMDI_RAW_ST_TRADES', 5);
