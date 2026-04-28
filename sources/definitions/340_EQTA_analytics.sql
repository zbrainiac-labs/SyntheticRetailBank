DEFINE DYNAMIC TABLE {{ db }}.{{ eqt_agg }}.EQTA_AGG_DT_TRADE_SUMMARY(
    TRADE_ID VARCHAR(50) COMMENT 'Unique trade identifier from FIX protocol execution report. Used for trade reconciliation, audit trail, and linking to settlement systems. Primary key for trade-level analysis and regulatory reporting (MiFID II transaction reporting).',
    TRADE_DATE TIMESTAMP_NTZ COMMENT 'Exact timestamp when trade was executed on exchange (UTC). Used for intraday analysis, execution quality measurement (VWAP comparison), and regulatory time-stamping requirements. Critical for best execution analysis and market timing studies.',
    SETTLEMENT_DATE DATE COMMENT 'Date when cash and securities transfer occurs (typically T+2 for equities). Used for cash flow forecasting, settlement risk monitoring, and liquidity planning. Links to payment operations for account debits/credits.',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Foreign key to CRM_RAW_001.CRMI_RAW_TB_CUSTOMER. Enables customer-centric analytics: trading behavior by segment, relationship profitability, and personalized investment recommendations. Critical for wealth management and advisory services.',
    ACCOUNT_ID VARCHAR(30) COMMENT 'Investment account used for trade settlement. Links to account balances for cash availability checks, margin calculations, and account-level performance reporting. Used for multi-account customer portfolio analysis.',
    ORDER_ID VARCHAR(50) COMMENT 'Parent order identifier for grouping related executions. Single large order may result in multiple trade executions (partial fills). Used for order execution quality analysis, fill rate calculation, and slippage measurement.',
    EXEC_ID VARCHAR(50) COMMENT 'Unique execution identifier from FIX protocol. Used for execution venue reconciliation, broker confirmation matching, and trade lifecycle tracking from order placement through settlement.',
    SYMBOL VARCHAR(20) COMMENT 'Stock ticker symbol (e.g., NESN for Nestlé, UBS for UBS Group). Human-readable security identifier used in trading platforms, client reporting, and market data feeds. Enables security-level performance analysis.',
    ISIN VARCHAR(12) COMMENT 'International Securities Identification Number per ISO 6166 standard. Global unique security identifier used for cross-border reporting, corporate actions processing, and securities master reconciliation. Required for regulatory reporting.',
    SIDE CHAR(1) COMMENT 'FIX protocol trade side indicator: 1=Buy (customer purchasing), 2=Sell (customer liquidating). Used for position calculation, cash flow direction, and buy/sell volume analysis. Critical for portfolio construction logic.',
    SIDE_DESCRIPTION VARCHAR(4) COMMENT 'Human-readable trade direction (BUY/SELL) for reporting and client communication. Derived from SIDE field. Used in client statements, trade confirmations, and portfolio reports for clarity.',
    QUANTITY NUMBER(15,4) COMMENT 'Number of shares or units traded. Used for position size calculation, volume analysis, and liquidity assessment. Supports fractional shares for modern trading platforms. Critical for portfolio weight calculations.',
    PRICE NUMBER(18,4) COMMENT 'Execution price per share in trade currency. Used for trade valuation, average cost calculation, and execution quality analysis (vs benchmark prices). Precision supports high-value securities and FX rates.',
    CURRENCY VARCHAR(3) COMMENT 'Trade currency per ISO 4217 (USD, EUR, CHF, GBP). Used for multi-currency portfolio reporting, FX exposure analysis, and currency hedging decisions. Links to REF_RAW_001 for FX conversion to base currency.',
    GROSS_AMOUNT NUMBER(18,2) COMMENT 'Total trade value before commission in trade currency (Quantity × Price). Used for broker settlement, order value limits validation, and gross exposure calculations. Negative for sells (cash in), positive for buys (cash out).',
    COMMISSION NUMBER(12,4) COMMENT 'Trading commission charged by broker in trade currency. Used for cost analysis, broker comparison, and net return calculation. Impacts customer profitability and influences broker selection strategy.',
    NET_AMOUNT NUMBER(18,2) COMMENT 'Net settlement amount after commission in trade currency (Gross_Amount + Commission). Actual cash impact to customer account. Used for cash settlement, account balance updates, and customer invoicing.',
    BASE_CURRENCY VARCHAR(3) COMMENT 'Bank base reporting currency (CHF). All multi-currency positions converted to this for consolidated reporting. Used for enterprise-wide risk aggregation, P&L reporting, and regulatory capital calculations.',
    BASE_GROSS_AMOUNT NUMBER(18,2) COMMENT 'Gross trade value converted to CHF using FX_RATE. Used for position aggregation across currencies, risk limit monitoring, and consolidated portfolio reporting. Primary metric for bank-wide exposure management.',
    BASE_NET_AMOUNT NUMBER(18,2) COMMENT 'Net settlement amount in CHF. Total cash impact in base currency including commission and FX conversion. Used for liquidity management, capital adequacy calculations, and consolidated financial reporting.',
    FX_RATE NUMBER(15,6) COMMENT 'FX conversion rate from trade currency to CHF (CCY/CHF) at trade execution time. Links to REF_RAW_001.FX rates for reconciliation. Used for P&L attribution, FX sensitivity analysis, and hedge effectiveness testing.',
    MARKET VARCHAR(20) COMMENT 'Exchange or trading venue where execution occurred (e.g., SIX Swiss Exchange, NYSE, NASDAQ). Used for market segmentation analysis, venue quality comparison, and regulatory reporting by trading venue.',
    ORDER_TYPE VARCHAR(10) COMMENT 'Order instruction type: MARKET (immediate at current price), LIMIT (at specified price or better), STOP (triggered at threshold). Indicates customer price sensitivity and urgency. Used for execution strategy analysis.',
    EXEC_TYPE VARCHAR(15) COMMENT 'FIX protocol execution type: NEW (order accepted), FILL (fully executed), PARTIAL_FILL (partially executed). Used for order fill rate analysis, execution quality monitoring, and operational metrics.',
    TIME_IN_FORCE VARCHAR(3) COMMENT 'Order duration instruction: DAY (valid until market close), GTC (Good Till Cancelled), IOC (Immediate Or Cancel). Indicates customer execution preferences. Used for order book management and execution strategy.',
    BROKER_ID VARCHAR(20) COMMENT 'Executing broker identifier for multi-broker operations. Used for broker performance analysis, routing optimization, best execution compliance, and counterparty exposure monitoring. Critical for broker relationship management.',
    VENUE VARCHAR(20) COMMENT 'Specific trading venue or dark pool within exchange. Used for execution quality analysis by venue, liquidity source optimization, and MiFID II venue transparency reporting.',
    COMMISSION_RATE_BPS NUMBER(8,2) COMMENT 'Commission as basis points of trade value (1 bp = 0.01%). Used for commission tier analysis, volume-based pricing validation, and customer cost benchmarking. Enables comparison across different trade sizes.',
    TRADE_VALUE_CATEGORY VARCHAR(10) COMMENT 'Trade size classification: SMALL (under 10K CHF), MEDIUM (10K-100K), LARGE (100K-1M), VERY_LARGE (over 1M). Used for trade size distribution analysis, pricing tier assignment, and market impact assessment.',
    SETTLEMENT_DAYS NUMBER(3,0) COMMENT 'Number of business days between trade and settlement (typically 2 for equities = T+2). Used for settlement cycle monitoring, liquidity forecasting, and operational exception handling for non-standard settlements.',
    TRADE_YEAR NUMBER(4,0) COMMENT 'Year of trade execution. Time dimension for year-over-year analysis, annual reporting, and long-term trend identification. Supports fiscal year reporting and multi-year performance comparisons.',
    TRADE_MONTH NUMBER(2,0) COMMENT 'Month of trade execution (1-12). Time dimension for monthly volume analysis, seasonal pattern detection, and month-end reporting. Used for management dashboards and business planning.',
    TRADE_DAY_OF_WEEK NUMBER(1,0) COMMENT 'Day of week when trade executed (1=Monday, 7=Sunday). Used for trading pattern analysis, intraweek seasonality detection, and operational capacity planning. Identifies high-volume trading days.',
    CREATED_AT TIMESTAMP_NTZ COMMENT 'UTC timestamp when trade record was created in Snowflake. Used for data lineage tracking, SLA monitoring, and identifying processing delays. Critical for operational dashboards and data quality metrics.'
) COMMENT = 'Enriched trade-level analytics combining raw FIX protocol data with calculated metrics, classifications, and time dimensions. Provides comprehensive view of all equity trades for execution quality monitoring, broker performance analysis, regulatory reporting (MiFID II), and customer activity tracking. Used by Trading Desk for execution analysis, Risk for exposure monitoring, Compliance for trade surveillance, and Wealth Management for customer reporting. Automatically refreshes hourly as new trades arrive.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT 
    t.TRADE_ID,
    t.TRADE_DATE,
    t.SETTLEMENT_DATE,
    t.CUSTOMER_ID,
    t.ACCOUNT_ID,
    t.ORDER_ID,
    t.EXEC_ID,

    t.SYMBOL,
    t.ISIN,
    t.SIDE,
    CASE t.SIDE 
        WHEN '1' THEN 'BUY'
        WHEN '2' THEN 'SELL'
        ELSE 'UNKNOWN'
    END AS SIDE_DESCRIPTION,

    t.QUANTITY,
    t.PRICE,
    t.CURRENCY,
    t.GROSS_AMOUNT,
    t.COMMISSION,
    t.NET_AMOUNT,

    t.BASE_CURRENCY,
    t.BASE_GROSS_AMOUNT,
    t.BASE_NET_AMOUNT,
    t.FX_RATE,

    t.MARKET,
    t.ORDER_TYPE,
    t.EXEC_TYPE,
    t.TIME_IN_FORCE,
    t.BROKER_ID,
    t.VENUE,

    ROUND(
        CASE 
            WHEN ABS(t.GROSS_AMOUNT) > 0 THEN
                (t.COMMISSION / ABS(t.GROSS_AMOUNT)) * 10000
            ELSE 0
        END, 2
    ) AS COMMISSION_RATE_BPS,

    CASE 
        WHEN ABS(t.BASE_GROSS_AMOUNT) >= 1000000 THEN 'VERY_LARGE'
        WHEN ABS(t.BASE_GROSS_AMOUNT) >= 100000 THEN 'LARGE'
        WHEN ABS(t.BASE_GROSS_AMOUNT) >= 10000 THEN 'MEDIUM'
        ELSE 'SMALL'
    END AS TRADE_VALUE_CATEGORY,

    DATEDIFF(DAY, t.TRADE_DATE, t.SETTLEMENT_DATE) AS SETTLEMENT_DAYS,

    YEAR(t.TRADE_DATE) AS TRADE_YEAR,
    MONTH(t.TRADE_DATE) AS TRADE_MONTH,
    DAYOFWEEK(t.TRADE_DATE) AS TRADE_DAY_OF_WEEK,

    t.CREATED_AT

FROM {{ eqt_raw }}.EQTI_RAW_TB_TRADES t
ORDER BY t.TRADE_DATE DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ eqt_agg }}.EQTA_AGG_DT_PORTFOLIO_POSITIONS(
    ACCOUNT_ID VARCHAR(30) COMMENT 'Investment account identifier. Primary dimension for position aggregation. Links to account master data for account type classification and custody arrangements. Used for account-level performance reporting and margin calculations.',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer owner of the account. Foreign key to CRM_RAW_001 for customer profile integration. Enables customer-level portfolio consolidation across multiple accounts and relationship-based investment advisory.',
    SYMBOL VARCHAR(20) COMMENT 'Stock ticker symbol for the security. Used for position identification, market data lookup, and client reporting. Enables symbol-level exposure analysis and sector concentration monitoring.',
    ISIN VARCHAR(12) COMMENT 'International Securities Identification Number per ISO 6166. Global unique security identifier for cross-border positions, corporate actions processing, and regulatory reporting. Used for securities master reconciliation.',
    CURRENCY VARCHAR(3) COMMENT 'Original trading currency for this position per ISO 4217. Used for currency-specific performance calculation and FX exposure analysis. Positions in same security but different currencies tracked separately.',
    TOTAL_QUANTITY NUMBER(15,4) COMMENT 'Net position quantity (TOTAL_BUYS - TOTAL_SELLS). Positive = long position (owns shares), negative = short position (borrowed shares), zero = closed position (fully exited). Core metric for position size and market exposure.',
    TOTAL_BUYS NUMBER(15,4) COMMENT 'Cumulative shares purchased across all buy trades. Used for position build-up analysis, average cost calculation basis, and understanding customer accumulation behavior. Always positive or zero.',
    TOTAL_SELLS NUMBER(15,4) COMMENT 'Cumulative shares sold across all sell trades. Used for position reduction analysis, profit-taking behavior identification, and realized P&L calculation. Always positive or zero.',
    AVERAGE_BUY_PRICE NUMBER(18,4) COMMENT 'Volume-weighted average purchase price across all buys. Calculated as (Sum of Buy_Amount) / (Sum of Buy_Quantity). Used for cost basis determination, gain/loss calculation, and performance attribution.',
    AVERAGE_SELL_PRICE NUMBER(18,4) COMMENT 'Volume-weighted average selling price across all sells. Calculated as (Sum of Sell_Amount) / (Sum of Sell_Quantity). Used for realized P&L calculation and exit quality analysis.',
    TOTAL_BUY_AMOUNT NUMBER(18,2) COMMENT 'Total cash outflow for all purchases in original trade currency (excluding commission). Sum of all buy trade gross amounts. Used for currency-specific cash flow analysis and investment tracking.',
    TOTAL_SELL_AMOUNT NUMBER(18,2) COMMENT 'Total cash inflow from all sales in original trade currency (excluding commission). Sum of all sell trade gross amounts. Used for currency-specific proceeds tracking and liquidity planning.',
    TOTAL_COMMISSION NUMBER(12,4) COMMENT 'Cumulative commission paid on all trades (buys and sells) in trade currency. Total trading cost reducing net returns. Used for cost analysis, broker relationship evaluation, and customer profitability assessment.',
    NET_INVESTMENT NUMBER(18,2) COMMENT 'Net capital invested in position in trade currency: (Total_Buy_Amount - Total_Sell_Amount + Total_Commission). Current exposure before FX conversion. Used for currency-specific capital allocation analysis.',
    TOTAL_BUY_AMOUNT_CHF NUMBER(18,2) COMMENT 'Total purchase amount converted to CHF base currency using historical FX rates at trade execution. Used for multi-currency portfolio aggregation and consolidated cost basis reporting.',
    TOTAL_SELL_AMOUNT_CHF NUMBER(18,2) COMMENT 'Total sales proceeds converted to CHF. Used for consolidated cash flow analysis and multi-currency portfolio performance measurement in single reporting currency.',
    NET_INVESTMENT_CHF NUMBER(18,2) COMMENT 'Net capital invested in CHF (Total_Buy_CHF - Total_Sell_CHF). Core metric for consolidated position exposure, risk limit monitoring, and capital allocation across multi-currency portfolios.',
    REALIZED_PL_CHF NUMBER(18,2) COMMENT 'Realized profit or loss in CHF for shares already sold. Calculated as (Total_Sell_CHF - Cost_Basis_of_Sold_Shares). Positive = gain, negative = loss. Used for tax reporting, performance measurement, and customer statements. Only includes closed portion of position.',
    POSITION_STATUS VARCHAR(10) COMMENT 'Current position state: LONG (net positive quantity, customer owns shares), SHORT (net negative, customer borrowed shares), CLOSED (net zero, fully exited). Used for position classification, margin requirements, and portfolio strategy validation.',
    FIRST_TRADE_DATE DATE COMMENT 'Date when position was first established (earliest buy or sell trade). Used for holding period calculation, long-term vs short-term gain classification, and investment horizon analysis.',
    LAST_TRADE_DATE DATE COMMENT 'Date of most recent trade activity (latest buy or sell). Used for stale position identification, activity recency scoring, and customer engagement metrics. Identifies dormant positions.',
    TRADE_COUNT NUMBER(10,0) COMMENT 'Total number of trades (buys + sells) for this position. Indicates trading frequency and portfolio turnover. High count suggests active trading;

DEFINE DYNAMIC TABLE {{ db }}.{{ eqt_agg }}.EQTA_AGG_DT_CUSTOMER_ACTIVITY(
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier. Foreign key to CRM_RAW_001 for profile integration. Primary dimension for customer behavior analysis, relationship management scoring, and personalized service delivery.',
    TOTAL_TRADES NUMBER(10,0) COMMENT 'Lifetime total number of trade executions across all accounts. Primary activity metric for customer engagement scoring, commission revenue potential, and service tier assignment. Used for active vs passive investor classification.',
    TOTAL_BUY_TRADES NUMBER(10,0) COMMENT 'Count of buy-side trades. Indicates portfolio accumulation behavior and capital deployment activity. Used with sell trades to calculate buy/sell ratio for understanding investment style (accumulator vs trader).',
    TOTAL_SELL_TRADES NUMBER(10,0) COMMENT 'Count of sell-side trades. Indicates liquidation frequency and profit-taking behavior. High sell activity suggests tactical trading;
