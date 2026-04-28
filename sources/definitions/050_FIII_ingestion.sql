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

    NOTIONAL FLOAT WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Notional amount (face value)',
    PRICE FLOAT COMMENT 'Clean price (as % of par for bonds, rate for swaps)',
    ACCRUED_INTEREST FLOAT WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Accrued interest amount (bonds only)',
    GROSS_AMOUNT FLOAT WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Signed gross amount: price * notional + accrued',
    COMMISSION FLOAT WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Trading commission',
    NET_AMOUNT FLOAT WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Signed net amount: gross_amount +/- commission',

    BASE_CURRENCY VARCHAR(3) COMMENT 'Base reporting currency (CHF)',
    BASE_GROSS_AMOUNT FLOAT WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Gross amount in CHF',
    BASE_NET_AMOUNT FLOAT WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Net amount in CHF',
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
AS
    COPY INTO {{ fii_raw }}.FIII_RAW_TB_TRADES (
        TRADE_DATE, SETTLEMENT_DATE, TRADE_ID, CUSTOMER_ID, ACCOUNT_ID, ORDER_ID,
        INSTRUMENT_TYPE, INSTRUMENT_ID, ISSUER, ISSUER_TYPE, CURRENCY, SIDE,
        NOTIONAL, PRICE, ACCRUED_INTEREST, GROSS_AMOUNT, FIXED_RATE, FLOATING_RATE_INDEX, TENOR_YEARS,
        COMMISSION, NET_AMOUNT, BASE_CURRENCY, BASE_GROSS_AMOUNT, BASE_NET_AMOUNT, FX_RATE,
        COUPON_RATE, MATURITY_DATE, DURATION, DV01, CREDIT_RATING, CREDIT_SPREAD_BPS,
        MARKET, BROKER_ID, VENUE, LIQUIDITY_SCORE
    )
    FROM @FIII_RAW_ST_TRADES
    PATTERN = '.*fixed_income_trades.*\.csv'
    FILE_FORMAT = FIII_RAW_FF_TRADES_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ fii_raw }}.FIII_RAW_TK_CLEANUP_AFTER_LOAD_TRADES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER fixed income trade data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ fii_raw }}.FIII_RAW_TK_LOAD_TRADES
AS
    CALL FIII_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('FIII_RAW_ST_TRADES', 5);
