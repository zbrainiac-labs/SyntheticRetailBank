/*
 * 510_REPP_equity.sql
 * Equity reporting: portfolio and trading views
 */
DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_EQUITY_SUMMARY(
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for portfolio analysis',
    ACCOUNT_ID VARCHAR(30) COMMENT 'Account identifier for position tracking',
    BASE_CURRENCY VARCHAR(3) COMMENT 'Account base currency for reporting',
    TOTAL_TRADES NUMBER(10,0) COMMENT 'Total number of equity transactions',
    BUY_TRADES NUMBER(10,0) COMMENT 'Number of buy transactions',
    SELL_TRADES NUMBER(10,0) COMMENT 'Number of sell transactions',
    UNIQUE_SYMBOLS NUMBER(10,0) COMMENT 'Number of different securities traded',
    TOTAL_CHF_VOLUME DECIMAL(28,2) COMMENT 'Total trading volume in CHF',
    NET_CHF_POSITION DECIMAL(28,2) COMMENT 'Net position (positive = net buyer, negative = net seller)',
    TOTAL_COMMISSION_CHF DECIMAL(28,2) COMMENT 'Total commission fees paid',
    AVG_TRADE_SIZE_CHF DECIMAL(28,2) COMMENT 'Average trade size for customer profiling',
    FIRST_TRADE_DATE DATE COMMENT 'First trading activity date',
    LAST_TRADE_DATE DATE COMMENT 'Most recent trading activity date'
) COMMENT = 'Customer Equity Trading Performance: To summarize the trading activity and profitability (via net position and commissions) for each customer and account.
Brokerage/CRM: Measures customer engagement and revenue generation (commissions).
Risk: Monitors net market position (buyer/seller) for risk exposure at the client level.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    t.CUSTOMER_ID,                                               -- Customer identifier for portfolio analysis
    t.ACCOUNT_ID,                                                -- Account identifier for position tracking
    a.BASE_CURRENCY,                                             -- Account base currency for reporting
    COUNT(*) AS TOTAL_TRADES,                                    -- Total number of equity transactions
    SUM(CASE WHEN t.SIDE = '1' THEN 1 ELSE 0 END) AS BUY_TRADES,  -- Number of buy transactions
    SUM(CASE WHEN t.SIDE = '2' THEN 1 ELSE 0 END) AS SELL_TRADES, -- Number of sell transactions
    COUNT(DISTINCT t.SYMBOL) AS UNIQUE_SYMBOLS,                  -- Number of different securities traded
    SUM(ABS(t.BASE_GROSS_AMOUNT)) AS TOTAL_CHF_VOLUME,          -- Total trading volume in CHF
    SUM(t.BASE_GROSS_AMOUNT) AS NET_CHF_POSITION,               -- Net position (positive = net buyer, negative = net seller)
    SUM(t.COMMISSION) AS TOTAL_COMMISSION_CHF,                   -- Total commission fees paid
    AVG(ABS(t.BASE_GROSS_AMOUNT)) AS AVG_TRADE_SIZE_CHF,        -- Average trade size for customer profiling
    MIN(t.TRADE_DATE) AS FIRST_TRADE_DATE,                       -- First trading activity date
    MAX(t.TRADE_DATE) AS LAST_TRADE_DATE                         -- Most recent trading activity date
FROM {{ db }}.{{ eqt_raw }}.EQTI_RAW_TB_TRADES t
LEFT JOIN {{ db }}.{{ crm_agg }}.ACCA_AGG_DT_ACCOUNTS a ON t.ACCOUNT_ID = a.ACCOUNT_ID
GROUP BY t.CUSTOMER_ID, t.ACCOUNT_ID, a.BASE_CURRENCY;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_EQUITY_POSITIONS(
    SYMBOL VARCHAR(20) COMMENT 'Security symbol for position tracking',
    ISIN VARCHAR(12) COMMENT 'International Securities Identification Number',
    UNIQUE_CUSTOMERS NUMBER(10,0) COMMENT 'Number of customers holding this security',
    NET_POSITION DECIMAL(28,2) COMMENT 'Net position across all customers (positive = long, negative = short)',
    TOTAL_BOUGHT DECIMAL(28,2) COMMENT 'Total quantity purchased',
    TOTAL_SOLD DECIMAL(28,2) COMMENT 'Total quantity sold',
    TOTAL_TRADES NUMBER(10,0) COMMENT 'Total number of trades in this security',
    TOTAL_CHF_VOLUME DECIMAL(28,2) COMMENT 'Total trading volume in CHF',
    AVG_PRICE DECIMAL(15,4) COMMENT 'Average trading price',
    MIN_PRICE DECIMAL(15,4) COMMENT 'Lowest trading price observed',
    MAX_PRICE DECIMAL(15,4) COMMENT 'Highest trading price observed',
    LAST_TRADE_DATE DATE COMMENT 'Most recent trading date for this security'
) COMMENT = 'Concentration Risk and Market Exposure by Security: To track the aggregate net position (long/short) for every traded security across all customers.
Market Risk: Identifies securities where the banks customers have high volume or concentrated positions, which could impact liquidity and require capital provisioning.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    SYMBOL,                                                      -- Security symbol for position tracking
    ISIN,                                                        -- International Securities Identification Number
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,            -- Number of customers holding this security
    SUM(CASE WHEN SIDE = '1' THEN QUANTITY ELSE -QUANTITY END) AS NET_POSITION, -- Net position across all customers (positive = long, negative = short)
    SUM(CASE WHEN SIDE = '1' THEN QUANTITY ELSE 0 END) AS TOTAL_BOUGHT,         -- Total quantity purchased
    SUM(CASE WHEN SIDE = '2' THEN QUANTITY ELSE 0 END) AS TOTAL_SOLD,           -- Total quantity sold
    COUNT(*) AS TOTAL_TRADES,                                    -- Total number of trades in this security
    SUM(ABS(BASE_GROSS_AMOUNT)) AS TOTAL_CHF_VOLUME,            -- Total trading volume in CHF
    AVG(PRICE) AS AVG_PRICE,                                     -- Average trading price
    MIN(PRICE) AS MIN_PRICE,                                     -- Lowest trading price observed
    MAX(PRICE) AS MAX_PRICE,                                     -- Highest trading price observed
    MAX(TRADE_DATE) AS LAST_TRADE_DATE                           -- Most recent trading date for this security
FROM {{ db }}.{{ eqt_raw }}.EQTI_RAW_TB_TRADES
GROUP BY SYMBOL, ISIN;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_EQUITY_CURRENCY_EXPOSURE(
    CURRENCY VARCHAR(3) COMMENT 'Trading currency for FX exposure analysis',
    TRADE_COUNT NUMBER(10,0) COMMENT 'Number of equity trades in this currency',
    TOTAL_ORIGINAL_VOLUME DECIMAL(28,2) COMMENT 'Total trading volume in original currency',
    TOTAL_CHF_VOLUME DECIMAL(28,2) COMMENT 'Total trading volume converted to CHF',
    AVG_FX_RATE DECIMAL(15,6) COMMENT 'Average FX rate used for currency conversion',
    MIN_FX_RATE DECIMAL(15,6) COMMENT 'Minimum FX rate observed',
    MAX_FX_RATE DECIMAL(15,6) COMMENT 'Maximum FX rate observed',
    UNIQUE_CUSTOMERS NUMBER(10,0) COMMENT 'Number of customers trading in this currency',
    UNIQUE_SYMBOLS NUMBER(10,0) COMMENT 'Number of different securities traded in this currency'
) COMMENT = 'FX Risk from Foreign Equity Trading: To measure the currency exposure generated specifically by trading securities denominated in non-base currencies.
Market Risk/Treasury: Isolates the FX risk component of the trading book, ensuring accurate currency hedging and compliance with non-base currency exposure limits.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    CURRENCY,                                                    -- Trading currency for FX exposure analysis
    COUNT(*) AS TRADE_COUNT,                                     -- Number of equity trades in this currency
    SUM(ABS(GROSS_AMOUNT)) AS TOTAL_ORIGINAL_VOLUME,            -- Total trading volume in original currency
    SUM(ABS(BASE_GROSS_AMOUNT)) AS TOTAL_CHF_VOLUME,            -- Total trading volume converted to CHF
    AVG(FX_RATE) AS AVG_FX_RATE,                                 -- Average FX rate used for currency conversion
    MIN(FX_RATE) AS MIN_FX_RATE,                                 -- Minimum FX rate observed
    MAX(FX_RATE) AS MAX_FX_RATE,                                 -- Maximum FX rate observed
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,            -- Number of customers trading in this currency
    COUNT(DISTINCT SYMBOL) AS UNIQUE_SYMBOLS                    -- Number of different securities traded in this currency
FROM {{ db }}.{{ eqt_raw }}.EQTI_RAW_TB_TRADES
WHERE CURRENCY != 'CHF'
GROUP BY CURRENCY;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_HIGH_VALUE_EQUITY_TRADES(
    TRADE_DATE DATE COMMENT 'Trade execution date for compliance tracking',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for large trade monitoring',
    ACCOUNT_ID VARCHAR(30) COMMENT 'Account identifier for position tracking',
    TRADE_ID VARCHAR(50) COMMENT 'Unique trade identifier for audit trail',
    SYMBOL VARCHAR(20) COMMENT 'Security symbol for concentration risk analysis',
    SIDE VARCHAR(1) COMMENT 'Trade direction (1=Buy, 2=Sell)',
    QUANTITY DECIMAL(28,2) COMMENT 'Number of shares/units traded',
    PRICE DECIMAL(15,4) COMMENT 'Execution price per unit',
    CHF_VALUE DECIMAL(28,2) COMMENT 'Trade value in CHF for threshold monitoring',
    MARKET VARCHAR(20) COMMENT 'Market/exchange where trade was executed',
    VENUE VARCHAR(20) COMMENT 'Trading venue for best execution analysis'
) COMMENT = 'Large Trade Compliance Monitoring: To filter and track all equity trades exceeding a significant value threshold (e.g., 100k CHF).
Compliance/Audit: Essential for compliance monitoring to detect potential market manipulation, front-running, or unauthorized large trading activity that requires immediate review and audit trail maintenance.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT
    TRADE_DATE,                                                  -- Trade execution date for compliance tracking
    CUSTOMER_ID,                                                 -- Customer identifier for large trade monitoring
    ACCOUNT_ID,                                                  -- Account identifier for position tracking
    TRADE_ID,                                                    -- Unique trade identifier for audit trail
    SYMBOL,                                                      -- Security symbol for concentration risk analysis
    SIDE,                                                        -- Trade direction (1=Buy, 2=Sell)
    QUANTITY,                                                    -- Number of shares/units traded
    PRICE,                                                       -- Execution price per unit
    ABS(BASE_GROSS_AMOUNT) AS CHF_VALUE,                         -- Trade value in CHF for threshold monitoring
    MARKET,                                                      -- Market/exchange where trade was executed
    VENUE                                                        -- Trading venue for best execution analysis
FROM {{ db }}.{{ eqt_raw }}.EQTI_RAW_TB_TRADES
WHERE ABS(BASE_GROSS_AMOUNT) > 100000 -- Trades over 100k CHF
ORDER BY ABS(BASE_GROSS_AMOUNT) DESC;
