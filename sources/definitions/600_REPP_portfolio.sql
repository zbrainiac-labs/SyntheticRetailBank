DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_PORTFOLIO_PERFORMANCE(
    ACCOUNT_ID VARCHAR(30) COMMENT 'Account identifier for portfolio tracking',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for relationship management',
    ACCOUNT_TYPE VARCHAR(20) COMMENT 'Account type (CHECKING/SAVINGS/BUSINESS/INVESTMENT)',
    BASE_CURRENCY VARCHAR(3) COMMENT 'Base currency for reporting',
    MEASUREMENT_PERIOD_START DATE COMMENT 'Start date of performance measurement period',
    MEASUREMENT_PERIOD_END DATE COMMENT 'End date of performance measurement period',
    DAYS_IN_PERIOD NUMBER(10,0) COMMENT 'Number of days in measurement period',

    CASH_STARTING_BALANCE DECIMAL(38,2) COMMENT 'Cash balance at start of period',
    CASH_ENDING_BALANCE DECIMAL(38,2) COMMENT 'Cash balance at end of period',
    CASH_DEPOSITS DECIMAL(38,2) COMMENT 'Total cash deposits during period',
    CASH_WITHDRAWALS DECIMAL(38,2) COMMENT 'Total cash withdrawals during period',
    CASH_NET_FLOW DECIMAL(38,2) COMMENT 'Net cash flow (deposits - withdrawals)',
    CASH_TWR_PERCENTAGE DECIMAL(38,4) COMMENT 'Time Weighted Return for cash account (%)',

    EQUITY_TRADES_COUNT NUMBER(10,0) COMMENT 'Number of equity trades during period',
    EQUITY_BUY_TRADES NUMBER(10,0) COMMENT 'Number of equity buy trades',
    EQUITY_SELL_TRADES NUMBER(10,0) COMMENT 'Number of equity sell trades',
    EQUITY_TOTAL_INVESTED_CHF DECIMAL(38,2) COMMENT 'Total amount invested in equities (CHF)',
    EQUITY_REALIZED_PL_CHF DECIMAL(38,2) COMMENT 'Realized profit/loss from equity sales (CHF)',
    EQUITY_COMMISSION_CHF DECIMAL(38,2) COMMENT 'Total trading commissions paid (CHF)',
    EQUITY_NET_RETURN_CHF DECIMAL(38,2) COMMENT 'Net return from equity trading (realized P and L - commissions)',
    EQUITY_RETURN_PERCENTAGE DECIMAL(38,4) COMMENT 'Equity return percentage (net return / invested)',

    FI_TRADES_COUNT NUMBER(10,0) COMMENT 'Number of fixed income trades during period',
    FI_BUY_TRADES NUMBER(10,0) COMMENT 'Number of fixed income buy trades',
    FI_SELL_TRADES NUMBER(10,0) COMMENT 'Number of fixed income sell trades',
    FI_TOTAL_INVESTED_CHF DECIMAL(38,2) COMMENT 'Total amount invested in fixed income (CHF)',
    FI_NET_PL_CHF DECIMAL(38,2) COMMENT 'Net profit/loss from fixed income trading (CHF)',
    FI_COMMISSION_CHF DECIMAL(38,2) COMMENT 'Total fixed income trading commissions (CHF)',
    FI_RETURN_PERCENTAGE DECIMAL(38,4) COMMENT 'Fixed income return percentage',

    CMD_TRADES_COUNT NUMBER(10,0) COMMENT 'Number of commodity trades during period',
    CMD_BUY_TRADES NUMBER(10,0) COMMENT 'Number of commodity buy trades',
    CMD_SELL_TRADES NUMBER(10,0) COMMENT 'Number of commodity sell trades',
    CMD_TOTAL_INVESTED_CHF DECIMAL(38,2) COMMENT 'Total amount invested in commodities (CHF)',
    CMD_NET_PL_CHF DECIMAL(38,2) COMMENT 'Net profit/loss from commodity trading (CHF)',
    CMD_COMMISSION_CHF DECIMAL(38,2) COMMENT 'Total commodity trading commissions (CHF)',
    CMD_RETURN_PERCENTAGE DECIMAL(38,4) COMMENT 'Commodity return percentage',

    CURRENT_CASH_VALUE_CHF DECIMAL(38,2) COMMENT 'Current cash position value (CHF)',
    CURRENT_EQUITY_POSITIONS NUMBER(10,0) COMMENT 'Number of open equity positions',
    CURRENT_EQUITY_VALUE_CHF DECIMAL(38,2) COMMENT 'Current value of equity positions (at cost, CHF)',
    CURRENT_FI_POSITIONS NUMBER(10,0) COMMENT 'Number of open fixed income positions',
    CURRENT_FI_VALUE_CHF DECIMAL(38,2) COMMENT 'Current value of fixed income positions (at cost, CHF)',
    CURRENT_CMD_POSITIONS NUMBER(10,0) COMMENT 'Number of open commodity positions',
    CURRENT_CMD_VALUE_CHF DECIMAL(38,2) COMMENT 'Current value of commodity positions (at cost, CHF)',
    TOTAL_PORTFOLIO_VALUE_CHF DECIMAL(38,2) COMMENT 'Total portfolio value (cash + all asset classes at cost)',
    CASH_ALLOCATION_PERCENTAGE DECIMAL(38,4) COMMENT 'Percentage of portfolio in cash',
    EQUITY_ALLOCATION_PERCENTAGE DECIMAL(38,4) COMMENT 'Percentage of portfolio in equities',
    FI_ALLOCATION_PERCENTAGE DECIMAL(38,4) COMMENT 'Percentage of portfolio in fixed income',
    CMD_ALLOCATION_PERCENTAGE DECIMAL(38,4) COMMENT 'Percentage of portfolio in commodities',

    TOTAL_PORTFOLIO_TWR_PERCENTAGE DECIMAL(38,4) COMMENT 'Combined Time Weighted Return for entire portfolio (%)',
    TOTAL_RETURN_CHF DECIMAL(38,2) COMMENT 'Total portfolio return in CHF',
    ANNUALIZED_PORTFOLIO_TWR DECIMAL(38,4) COMMENT 'Annualized portfolio TWR (%) - capped at +/-10000% to prevent overflow',

    PORTFOLIO_VOLATILITY DECIMAL(38,4) COMMENT 'Portfolio volatility (standard deviation of returns)',
    SHARPE_RATIO DECIMAL(38,4) COMMENT 'Risk-adjusted return (Sharpe Ratio)',
    RISK_FREE_RATE_ANNUAL_PCT DECIMAL(8,2) COMMENT 'Annual risk-free rate used in Sharpe calculation',
    MAX_DRAWDOWN_PERCENTAGE DECIMAL(38,4) COMMENT 'Maximum peak-to-trough decline (%)',

    TOTAL_TRANSACTIONS NUMBER(10,0) COMMENT 'Total transactions (cash + equity)',
    TRANSACTION_FREQUENCY DECIMAL(38,4) COMMENT 'Average transactions per month',
    TRADING_DAYS NUMBER(10,0) COMMENT 'Number of days with trading activity',

    PERFORMANCE_CATEGORY VARCHAR(30) COMMENT 'Performance classification (EXCELLENT/GOOD/NEUTRAL/POOR/NEGATIVE)',
    RISK_CATEGORY VARCHAR(20) COMMENT 'Risk classification (LOW/MODERATE/HIGH/VERY_HIGH)',
    PORTFOLIO_TYPE VARCHAR(30) COMMENT 'Portfolio composition type (CASH_ONLY/EQUITY_FOCUSED/FI_FOCUSED/COMMODITY_FOCUSED/BALANCED/MULTI_ASSET)',

    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Integrated Multi-Asset Portfolio Measurement: To deliver a single, comprehensive performance report that combines all asset classes (cash, equity, fixed income, commodities) into a single portfolio view. Calculates the Time Weighted Return (TWR).                                       
    Wealth Management / Client Reporting: Provides crucial metrics for investment advisors to present to clients, including TWR (the industry standard for external reporting), asset allocation percentages, total value, and risk categories.'
) COMMENT = 'Integrated Multi-Asset Portfolio Performance: Comprehensive portfolio analytics combining cash, equity, fixed income, and commodity trading performance with Time Weighted Return (TWR) calculations for wealth management and client reporting.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH 
cash_performance AS (
    SELECT 
        t.ACCOUNT_ID,
        MIN(DATE(t.BOOKING_DATE)) as period_start,
        MAX(DATE(t.BOOKING_DATE)) as period_end,
        DATEDIFF(DAY, MIN(DATE(t.BOOKING_DATE)), MAX(DATE(t.BOOKING_DATE))) as days_in_period,

        SUM(CASE WHEN t.AMOUNT > 0 THEN t.AMOUNT ELSE 0 END) as total_deposits,
        SUM(CASE WHEN t.AMOUNT < 0 THEN ABS(t.AMOUNT) ELSE 0 END) as total_withdrawals,
        SUM(t.AMOUNT) as net_cash_flow,

        COUNT(*) as cash_transaction_count,
        COUNT(DISTINCT DATE(t.BOOKING_DATE)) as cash_trading_days
    FROM {{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS t
    WHERE t.BOOKING_DATE >= CURRENT_DATE - INTERVAL '450 days'
    GROUP BY t.ACCOUNT_ID
),

equity_performance AS (
    SELECT 
        t.ACCOUNT_ID,
        COUNT(*) as equity_trades_count,
        SUM(CASE WHEN t.SIDE = '1' THEN 1 ELSE 0 END) as buy_trades,
        SUM(CASE WHEN t.SIDE = '2' THEN 1 ELSE 0 END) as sell_trades,

        SUM(CASE WHEN t.SIDE = '1' THEN ABS(t.BASE_GROSS_AMOUNT) ELSE 0 END) as total_invested_chf,
        SUM(CASE WHEN t.SIDE = '2' THEN ABS(t.BASE_GROSS_AMOUNT) ELSE 0 END) as total_divested_chf,

        SUM(t.COMMISSION) as total_commission_chf,

        SUM(CASE WHEN t.SIDE = '2' THEN ABS(t.BASE_GROSS_AMOUNT) ELSE 0 END) - 
        SUM(CASE WHEN t.SIDE = '1' THEN ABS(t.BASE_GROSS_AMOUNT) ELSE 0 END) as realized_pl_chf,

        COUNT(DISTINCT DATE(t.TRADE_DATE)) as equity_trading_days
    FROM {{ eqt_raw }}.EQTI_RAW_TB_TRADES t
    WHERE t.TRADE_DATE >= CURRENT_DATE - INTERVAL '450 days'
    GROUP BY t.ACCOUNT_ID
),

fixed_income_performance AS (
    SELECT 
        t.ACCOUNT_ID,
        COUNT(*) as fi_trades_count,
        SUM(CASE WHEN t.SIDE = '1' THEN 1 ELSE 0 END) as fi_buy_trades,
        SUM(CASE WHEN t.SIDE = '2' THEN 1 ELSE 0 END) as fi_sell_trades,
        SUM(ABS(t.BASE_GROSS_AMOUNT)) as fi_total_invested_chf,
        SUM(t.COMMISSION) as fi_total_commission_chf,
        SUM(CASE WHEN t.SIDE = '2' THEN ABS(t.BASE_GROSS_AMOUNT) ELSE -ABS(t.BASE_GROSS_AMOUNT) END) as fi_net_pl_chf,
        COUNT(DISTINCT DATE(t.TRADE_DATE)) as fi_trading_days
    FROM {{ fii_raw }}.FIII_RAW_TB_TRADES t
    WHERE t.TRADE_DATE >= CURRENT_DATE - INTERVAL '450 days'
    GROUP BY t.ACCOUNT_ID
),

commodity_performance AS (
    SELECT 
        t.ACCOUNT_ID,
        COUNT(*) as cmd_trades_count,
        SUM(CASE WHEN t.SIDE = '1' THEN 1 ELSE 0 END) as cmd_buy_trades,
        SUM(CASE WHEN t.SIDE = '2' THEN 1 ELSE 0 END) as cmd_sell_trades,
        SUM(ABS(t.BASE_GROSS_AMOUNT)) as cmd_total_invested_chf,
        SUM(t.COMMISSION) as cmd_total_commission_chf,
        SUM(CASE WHEN t.SIDE = '2' THEN ABS(t.BASE_GROSS_AMOUNT) ELSE -ABS(t.BASE_GROSS_AMOUNT) END) as cmd_net_pl_chf,
        COUNT(DISTINCT DATE(t.TRADE_DATE)) as cmd_trading_days
    FROM {{ cmd_raw }}.CMDI_RAW_TB_TRADES t
    WHERE t.TRADE_DATE >= CURRENT_DATE - INTERVAL '450 days'
    GROUP BY t.ACCOUNT_ID
),

current_balances AS (
    SELECT 
        b.ACCOUNT_ID,
        b.CURRENT_BALANCE_BASE as current_cash_balance,
        b.CURRENT_BALANCE_BASE - COALESCE((
            SELECT SUM(t.AMOUNT)
            FROM {{ pay_raw }}.PAYI_RAW_TB_TRANSACTIONS t
            WHERE t.ACCOUNT_ID = b.ACCOUNT_ID
              AND t.BOOKING_DATE >= CURRENT_DATE - INTERVAL '450 days'
        ), 0) as starting_cash_balance
    FROM {{ pay_agg }}.PAYA_AGG_DT_ACCOUNT_BALANCES b
),

current_equity_positions AS (
    SELECT 
        p.ACCOUNT_ID,
        COUNT(*) as open_positions,
        SUM(p.NET_INVESTMENT_CHF) as equity_value_at_cost,
        SUM(p.REALIZED_PL_CHF) as total_realized_pl
    FROM {{ eqt_agg }}.EQTA_AGG_DT_PORTFOLIO_POSITIONS p
    WHERE p.POSITION_STATUS != 'CLOSED'
    GROUP BY p.ACCOUNT_ID
),

current_fi_positions AS (
    SELECT 
        p.ACCOUNT_ID,
        COUNT(*) as fi_open_positions,
        SUM(p.NET_INVESTMENT_CHF) as fi_value_at_cost,
        SUM(p.REALIZED_PL_CHF) as fi_total_realized_pl
    FROM {{ fii_agg }}.FIIA_AGG_DT_PORTFOLIO_POSITIONS p
    WHERE p.POSITION_STATUS != 'CLOSED'
    GROUP BY p.ACCOUNT_ID
),

current_cmd_positions AS (
    SELECT 
        p.ACCOUNT_ID,
        COUNT(*) as cmd_open_positions,
        SUM(p.NET_INVESTMENT_CHF) as cmd_value_at_cost,
        SUM(p.REALIZED_PL_CHF) as cmd_total_realized_pl
    FROM {{ cmd_agg }}.CMDA_AGG_DT_PORTFOLIO_POSITIONS p
    WHERE p.POSITION_STATUS != 'CLOSED'
    GROUP BY p.ACCOUNT_ID
)

SELECT 
    COALESCE(cp.ACCOUNT_ID, ep.ACCOUNT_ID, fip.ACCOUNT_ID, cmdp.ACCOUNT_ID) as ACCOUNT_ID,
    acc.CUSTOMER_ID,
    acc.ACCOUNT_TYPE,
    acc.BASE_CURRENCY,

    COALESCE(cp.period_start, CURRENT_DATE - 450) as MEASUREMENT_PERIOD_START,
    COALESCE(cp.period_end, CURRENT_DATE) as MEASUREMENT_PERIOD_END,
    COALESCE(cp.days_in_period, 450) as DAYS_IN_PERIOD,

    ROUND(COALESCE(cb.starting_cash_balance, 0), 2) as CASH_STARTING_BALANCE,
    ROUND(COALESCE(cb.current_cash_balance, 0), 2) as CASH_ENDING_BALANCE,
    ROUND(COALESCE(cp.total_deposits, 0), 2) as CASH_DEPOSITS,
    ROUND(COALESCE(cp.total_withdrawals, 0), 2) as CASH_WITHDRAWALS,
    ROUND(COALESCE(cp.net_cash_flow, 0), 2) as CASH_NET_FLOW,

    ROUND(
        CASE 
            WHEN COALESCE(cb.starting_cash_balance, 0) > 0 THEN
                ((COALESCE(cb.current_cash_balance, 0) - COALESCE(cb.starting_cash_balance, 0) - COALESCE(cp.net_cash_flow, 0)) / 
                 COALESCE(cb.starting_cash_balance, 0)) * 100
            ELSE 0
        END, 4
    ) as CASH_TWR_PERCENTAGE,

    COALESCE(ep.equity_trades_count, 0) as EQUITY_TRADES_COUNT,
    COALESCE(ep.buy_trades, 0) as EQUITY_BUY_TRADES,
    COALESCE(ep.sell_trades, 0) as EQUITY_SELL_TRADES,
    ROUND(COALESCE(ep.total_invested_chf, 0), 2) as EQUITY_TOTAL_INVESTED_CHF,
    ROUND(COALESCE(ep.realized_pl_chf, 0), 2) as EQUITY_REALIZED_PL_CHF,
    ROUND(COALESCE(ep.total_commission_chf, 0), 2) as EQUITY_COMMISSION_CHF,
    ROUND(COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0), 2) as EQUITY_NET_RETURN_CHF,

    ROUND(
        CASE 
            WHEN COALESCE(ep.total_invested_chf, 0) > 0 THEN
                ((COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0)) / 
                 COALESCE(ep.total_invested_chf, 0)) * 100
            ELSE 0
        END, 4
    ) as EQUITY_RETURN_PERCENTAGE,

    COALESCE(fip.fi_trades_count, 0) as FI_TRADES_COUNT,
    COALESCE(fip.fi_buy_trades, 0) as FI_BUY_TRADES,
    COALESCE(fip.fi_sell_trades, 0) as FI_SELL_TRADES,
    ROUND(COALESCE(fip.fi_total_invested_chf, 0), 2) as FI_TOTAL_INVESTED_CHF,
    ROUND(COALESCE(fip.fi_net_pl_chf, 0), 2) as FI_NET_PL_CHF,
    ROUND(COALESCE(fip.fi_total_commission_chf, 0), 2) as FI_COMMISSION_CHF,
    ROUND(
        CASE 
            WHEN COALESCE(fip.fi_total_invested_chf, 0) > 0 THEN
                ((COALESCE(fip.fi_net_pl_chf, 0) - COALESCE(fip.fi_total_commission_chf, 0)) / 
                 COALESCE(fip.fi_total_invested_chf, 0)) * 100
            ELSE 0
        END, 4
    ) as FI_RETURN_PERCENTAGE,

    COALESCE(cmdp.cmd_trades_count, 0) as CMD_TRADES_COUNT,
    COALESCE(cmdp.cmd_buy_trades, 0) as CMD_BUY_TRADES,
    COALESCE(cmdp.cmd_sell_trades, 0) as CMD_SELL_TRADES,
    ROUND(COALESCE(cmdp.cmd_total_invested_chf, 0), 2) as CMD_TOTAL_INVESTED_CHF,
    ROUND(COALESCE(cmdp.cmd_net_pl_chf, 0), 2) as CMD_NET_PL_CHF,
    ROUND(COALESCE(cmdp.cmd_total_commission_chf, 0), 2) as CMD_COMMISSION_CHF,
    ROUND(
        CASE 
            WHEN COALESCE(cmdp.cmd_total_invested_chf, 0) > 0 THEN
                ((COALESCE(cmdp.cmd_net_pl_chf, 0) - COALESCE(cmdp.cmd_total_commission_chf, 0)) / 
                 COALESCE(cmdp.cmd_total_invested_chf, 0)) * 100
            ELSE 0
        END, 4
    ) as CMD_RETURN_PERCENTAGE,

    ROUND(COALESCE(cb.current_cash_balance, 0), 2) as CURRENT_CASH_VALUE_CHF,
    COALESCE(ceqp.open_positions, 0) as CURRENT_EQUITY_POSITIONS,
    ROUND(COALESCE(ceqp.equity_value_at_cost, 0), 2) as CURRENT_EQUITY_VALUE_CHF,
    COALESCE(cfip.fi_open_positions, 0) as CURRENT_FI_POSITIONS,
    ROUND(COALESCE(cfip.fi_value_at_cost, 0), 2) as CURRENT_FI_VALUE_CHF,
    COALESCE(ccmdp.cmd_open_positions, 0) as CURRENT_CMD_POSITIONS,
    ROUND(COALESCE(ccmdp.cmd_value_at_cost, 0), 2) as CURRENT_CMD_VALUE_CHF,
    ROUND(
        COALESCE(cb.current_cash_balance, 0) + 
        COALESCE(ceqp.equity_value_at_cost, 0) + 
        COALESCE(cfip.fi_value_at_cost, 0) + 
        COALESCE(ccmdp.cmd_value_at_cost, 0), 
    2) as TOTAL_PORTFOLIO_VALUE_CHF,

    ROUND(
        CASE 
            WHEN (COALESCE(cb.current_cash_balance, 0) + COALESCE(ceqp.equity_value_at_cost, 0) + 
                  COALESCE(cfip.fi_value_at_cost, 0) + COALESCE(ccmdp.cmd_value_at_cost, 0)) > 0 THEN
                (COALESCE(cb.current_cash_balance, 0) / 
                 (COALESCE(cb.current_cash_balance, 0) + COALESCE(ceqp.equity_value_at_cost, 0) + 
                  COALESCE(cfip.fi_value_at_cost, 0) + COALESCE(ccmdp.cmd_value_at_cost, 0))) * 100
            ELSE 100
        END, 2
    ) as CASH_ALLOCATION_PERCENTAGE,

    ROUND(
        CASE 
            WHEN (COALESCE(cb.current_cash_balance, 0) + COALESCE(ceqp.equity_value_at_cost, 0) + 
                  COALESCE(cfip.fi_value_at_cost, 0) + COALESCE(ccmdp.cmd_value_at_cost, 0)) > 0 THEN
                (COALESCE(ceqp.equity_value_at_cost, 0) / 
                 (COALESCE(cb.current_cash_balance, 0) + COALESCE(ceqp.equity_value_at_cost, 0) + 
                  COALESCE(cfip.fi_value_at_cost, 0) + COALESCE(ccmdp.cmd_value_at_cost, 0))) * 100
            ELSE 0
        END, 2
    ) as EQUITY_ALLOCATION_PERCENTAGE,

    ROUND(
        CASE 
            WHEN (COALESCE(cb.current_cash_balance, 0) + COALESCE(ceqp.equity_value_at_cost, 0) + 
                  COALESCE(cfip.fi_value_at_cost, 0) + COALESCE(ccmdp.cmd_value_at_cost, 0)) > 0 THEN
                (COALESCE(cfip.fi_value_at_cost, 0) / 
                 (COALESCE(cb.current_cash_balance, 0) + COALESCE(ceqp.equity_value_at_cost, 0) + 
                  COALESCE(cfip.fi_value_at_cost, 0) + COALESCE(ccmdp.cmd_value_at_cost, 0))) * 100
            ELSE 0
        END, 2
    ) as FI_ALLOCATION_PERCENTAGE,

    ROUND(
        CASE 
            WHEN (COALESCE(cb.current_cash_balance, 0) + COALESCE(ceqp.equity_value_at_cost, 0) + 
                  COALESCE(cfip.fi_value_at_cost, 0) + COALESCE(ccmdp.cmd_value_at_cost, 0)) > 0 THEN
                (COALESCE(ccmdp.cmd_value_at_cost, 0) / 
                 (COALESCE(cb.current_cash_balance, 0) + COALESCE(ceqp.equity_value_at_cost, 0) + 
                  COALESCE(cfip.fi_value_at_cost, 0) + COALESCE(ccmdp.cmd_value_at_cost, 0))) * 100
            ELSE 0
        END, 2
    ) as CMD_ALLOCATION_PERCENTAGE,

    ROUND(
        (
            CASE 
                WHEN COALESCE(cb.starting_cash_balance, 0) > 0 THEN
                    ((COALESCE(cb.current_cash_balance, 0) - COALESCE(cb.starting_cash_balance, 0) - COALESCE(cp.net_cash_flow, 0)) / 
                     COALESCE(cb.starting_cash_balance, 0)) * 100
                ELSE 0
            END * 
            CASE 
                WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                    COALESCE(cb.starting_cash_balance, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                ELSE 1
            END
        ) +
        (
            CASE 
                WHEN COALESCE(ep.total_invested_chf, 0) > 0 THEN
                    ((COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0)) / 
                     COALESCE(ep.total_invested_chf, 0)) * 100
                ELSE 0
            END *
            CASE 
                WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                    COALESCE(ep.total_invested_chf, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                ELSE 0
            END
        ), 4
    ) as TOTAL_PORTFOLIO_TWR_PERCENTAGE,

    ROUND(
        (COALESCE(cb.current_cash_balance, 0) - COALESCE(cb.starting_cash_balance, 0) - COALESCE(cp.net_cash_flow, 0)) +
        (COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0)), 2
    ) as TOTAL_RETURN_CHF,

    ROUND(
        CASE 
            WHEN COALESCE(cp.days_in_period, 450) >= 30 THEN
                LEAST(GREATEST(
                    (POWER(1 + (
                        (
                            CASE 
                                WHEN COALESCE(cb.starting_cash_balance, 0) > 0 THEN
                                    ((COALESCE(cb.current_cash_balance, 0) - COALESCE(cb.starting_cash_balance, 0) - COALESCE(cp.net_cash_flow, 0)) / 
                                     COALESCE(cb.starting_cash_balance, 0)) * 100
                                ELSE 0
                            END * 
                            CASE 
                                WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                                    COALESCE(cb.starting_cash_balance, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                                ELSE 1
                            END
                        ) +
                        (
                            CASE 
                                WHEN COALESCE(ep.total_invested_chf, 0) > 0 THEN
                                    ((COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0)) / 
                                     COALESCE(ep.total_invested_chf, 0)) * 100
                                ELSE 0
                            END *
                            CASE 
                                WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                                    COALESCE(ep.total_invested_chf, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                                ELSE 0
                            END
                        )
                    ) / 100, 365.0 / COALESCE(cp.days_in_period, 450)) - 1) * 100,
                    -10000 
                ), 10000) 
            ELSE 0 
        END, 4
    ) as ANNUALIZED_PORTFOLIO_TWR,

    0.0 as PORTFOLIO_VOLATILITY, 
    0.0 as SHARPE_RATIO, 
    3.5 as RISK_FREE_RATE_ANNUAL_PCT,
    0.0 as MAX_DRAWDOWN_PERCENTAGE, 

    COALESCE(cp.cash_transaction_count, 0) + COALESCE(ep.equity_trades_count, 0) as TOTAL_TRANSACTIONS,
    ROUND(
        CASE 
            WHEN COALESCE(cp.days_in_period, 450) > 0 THEN
                ((COALESCE(cp.cash_transaction_count, 0) + COALESCE(ep.equity_trades_count, 0)) * 30.0) / 
                COALESCE(cp.days_in_period, 450)
            ELSE 0
        END, 2
    ) as TRANSACTION_FREQUENCY,
    GREATEST(COALESCE(cp.cash_trading_days, 0), COALESCE(ep.equity_trading_days, 0)) as TRADING_DAYS,

    CASE 
        WHEN (
            (
                CASE 
                    WHEN COALESCE(cb.starting_cash_balance, 0) > 0 THEN
                        ((COALESCE(cb.current_cash_balance, 0) - COALESCE(cb.starting_cash_balance, 0) - COALESCE(cp.net_cash_flow, 0)) / 
                         COALESCE(cb.starting_cash_balance, 0)) * 100
                    ELSE 0
                END * 
                CASE 
                    WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                        COALESCE(cb.starting_cash_balance, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                    ELSE 1
                END
            ) +
            (
                CASE 
                    WHEN COALESCE(ep.total_invested_chf, 0) > 0 THEN
                        ((COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0)) / 
                         COALESCE(ep.total_invested_chf, 0)) * 100
                    ELSE 0
                END *
                CASE 
                    WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                        COALESCE(ep.total_invested_chf, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                    ELSE 0
                END
            )
        ) >= 15 THEN 'EXCELLENT_PERFORMANCE'
        WHEN (
            (
                CASE 
                    WHEN COALESCE(cb.starting_cash_balance, 0) > 0 THEN
                        ((COALESCE(cb.current_cash_balance, 0) - COALESCE(cb.starting_cash_balance, 0) - COALESCE(cp.net_cash_flow, 0)) / 
                         COALESCE(cb.starting_cash_balance, 0)) * 100
                    ELSE 0
                END * 
                CASE 
                    WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                        COALESCE(cb.starting_cash_balance, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                    ELSE 1
                END
            ) +
            (
                CASE 
                    WHEN COALESCE(ep.total_invested_chf, 0) > 0 THEN
                        ((COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0)) / 
                         COALESCE(ep.total_invested_chf, 0)) * 100
                    ELSE 0
                END *
                CASE 
                    WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                        COALESCE(ep.total_invested_chf, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                    ELSE 0
                END
            )
        ) >= 8 THEN 'GOOD_PERFORMANCE'
        WHEN (
            (
                CASE 
                    WHEN COALESCE(cb.starting_cash_balance, 0) > 0 THEN
                        ((COALESCE(cb.current_cash_balance, 0) - COALESCE(cb.starting_cash_balance, 0) - COALESCE(cp.net_cash_flow, 0)) / 
                         COALESCE(cb.starting_cash_balance, 0)) * 100
                    ELSE 0
                END * 
                CASE 
                    WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                        COALESCE(cb.starting_cash_balance, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                    ELSE 1
                END
            ) +
            (
                CASE 
                    WHEN COALESCE(ep.total_invested_chf, 0) > 0 THEN
                        ((COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0)) / 
                         COALESCE(ep.total_invested_chf, 0)) * 100
                    ELSE 0
                END *
                CASE 
                    WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                        COALESCE(ep.total_invested_chf, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                    ELSE 0
                END
            )
        ) >= 2 THEN 'NEUTRAL_PERFORMANCE'
        WHEN (
            (
                CASE 
                    WHEN COALESCE(cb.starting_cash_balance, 0) > 0 THEN
                        ((COALESCE(cb.current_cash_balance, 0) - COALESCE(cb.starting_cash_balance, 0) - COALESCE(cp.net_cash_flow, 0)) / 
                         COALESCE(cb.starting_cash_balance, 0)) * 100
                    ELSE 0
                END * 
                CASE 
                    WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                        COALESCE(cb.starting_cash_balance, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                    ELSE 1
                END
            ) +
            (
                CASE 
                    WHEN COALESCE(ep.total_invested_chf, 0) > 0 THEN
                        ((COALESCE(ep.realized_pl_chf, 0) - COALESCE(ep.total_commission_chf, 0)) / 
                         COALESCE(ep.total_invested_chf, 0)) * 100
                    ELSE 0
                END *
                CASE 
                    WHEN (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0)) > 0 THEN
                        COALESCE(ep.total_invested_chf, 0) / (COALESCE(cb.starting_cash_balance, 0) + COALESCE(ep.total_invested_chf, 0))
                    ELSE 0
                END
            )
        ) >= 0 THEN 'POOR_PERFORMANCE'
        ELSE 'NEGATIVE_PERFORMANCE'
    END as PERFORMANCE_CATEGORY,

    'LOW_RISK' as RISK_CATEGORY, 

    CASE 
        WHEN COALESCE(ep.equity_trades_count, 0) = 0 
         AND COALESCE(fip.fi_trades_count, 0) = 0 
         AND COALESCE(cmdp.cmd_trades_count, 0) = 0 THEN 'CASH_ONLY'
        WHEN COALESCE(ep.equity_trades_count, 0) > 0 
         AND COALESCE(fip.fi_trades_count, 0) > 0 
         AND COALESCE(cmdp.cmd_trades_count, 0) > 0 THEN 'MULTI_ASSET'
        WHEN COALESCE(ep.equity_trades_count, 0) > 0 
         AND (COALESCE(fip.fi_trades_count, 0) > 0 OR COALESCE(cmdp.cmd_trades_count, 0) > 0) THEN 'BALANCED'
        WHEN COALESCE(ep.equity_trades_count, 0) > 0 THEN 'EQUITY_FOCUSED'
        WHEN COALESCE(fip.fi_trades_count, 0) > 0 THEN 'FI_FOCUSED'
        WHEN COALESCE(cmdp.cmd_trades_count, 0) > 0 THEN 'COMMODITY_FOCUSED'
        ELSE 'CASH_ONLY'
    END as PORTFOLIO_TYPE,

    CURRENT_TIMESTAMP() as CALCULATION_TIMESTAMP

FROM cash_performance cp
FULL OUTER JOIN equity_performance ep ON cp.ACCOUNT_ID = ep.ACCOUNT_ID
FULL OUTER JOIN fixed_income_performance fip ON COALESCE(cp.ACCOUNT_ID, ep.ACCOUNT_ID) = fip.ACCOUNT_ID
FULL OUTER JOIN commodity_performance cmdp ON COALESCE(cp.ACCOUNT_ID, ep.ACCOUNT_ID, fip.ACCOUNT_ID) = cmdp.ACCOUNT_ID
LEFT JOIN current_balances cb ON COALESCE(cp.ACCOUNT_ID, ep.ACCOUNT_ID, fip.ACCOUNT_ID, cmdp.ACCOUNT_ID) = cb.ACCOUNT_ID
LEFT JOIN current_equity_positions ceqp ON COALESCE(cp.ACCOUNT_ID, ep.ACCOUNT_ID, fip.ACCOUNT_ID, cmdp.ACCOUNT_ID) = ceqp.ACCOUNT_ID
LEFT JOIN current_fi_positions cfip ON COALESCE(cp.ACCOUNT_ID, ep.ACCOUNT_ID, fip.ACCOUNT_ID, cmdp.ACCOUNT_ID) = cfip.ACCOUNT_ID
LEFT JOIN current_cmd_positions ccmdp ON COALESCE(cp.ACCOUNT_ID, ep.ACCOUNT_ID, fip.ACCOUNT_ID, cmdp.ACCOUNT_ID) = ccmdp.ACCOUNT_ID
LEFT JOIN {{ crm_agg }}.ACCA_AGG_DT_ACCOUNTS acc 
    ON COALESCE(cp.ACCOUNT_ID, ep.ACCOUNT_ID, fip.ACCOUNT_ID, cmdp.ACCOUNT_ID) = acc.ACCOUNT_ID
WHERE COALESCE(cb.starting_cash_balance, 0) > 0 
   OR COALESCE(ep.total_invested_chf, 0) > 0
   OR COALESCE(fip.fi_total_invested_chf, 0) > 0
   OR COALESCE(cmdp.cmd_total_invested_chf, 0) > 0
ORDER BY TOTAL_PORTFOLIO_VALUE_CHF DESC;
