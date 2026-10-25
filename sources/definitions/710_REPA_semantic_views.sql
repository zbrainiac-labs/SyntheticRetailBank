{# semantic_views='false' is only used for the first deploy into an empty database:
   semantic views validate against their base dynamic tables, which must be initialized first #}
{% if semantic_views == 'true' %}
-- ============================================================
-- WEALTH AND RISK REPORTING SEMANTIC VIEWS
-- Cortex Analyst semantic views (moved from post_deploy.sql).
-- ============================================================

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.REPA_SV_WEALTH_MANAGEMENT
tables (
  portfolio AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_PORTFOLIO_PERFORMANCE
    PRIMARY KEY (ACCOUNT_ID)
    COMMENT = 'Portfolio performance metrics and asset allocation',
  customers AS {{ db }}.{{ crm_agg }}.CRMA_AGG_VW_CUSTOMER_360_ENRICHED
    PRIMARY KEY (CUSTOMER_ID)
    COMMENT = 'Customer profile and demographic data'
)

relationships (
  portfolio_to_customer AS
    portfolio (CUSTOMER_ID) REFERENCES customers
)

facts (
  portfolio.ACCOUNT_ID as ACCOUNT_ID comment='Account ID | portfolio account | account number',
  portfolio.CUSTOMER_ID as CUSTOMER_ID comment='Customer ID | client ID | customer identifier',
  
  portfolio.MEASUREMENT_PERIOD_START as MEASUREMENT_PERIOD_START comment='Period start | measurement start | reporting period start',
  portfolio.MEASUREMENT_PERIOD_END as MEASUREMENT_PERIOD_END comment='Period end | measurement end | reporting period end',
  portfolio.DAYS_IN_PERIOD as DAYS_IN_PERIOD comment='Days in period | period length | measurement days',
  
  portfolio.CASH_STARTING_BALANCE as CASH_STARTING_BALANCE comment='Cash starting balance | opening cash | initial cash',
  portfolio.CASH_ENDING_BALANCE as CASH_ENDING_BALANCE comment='Cash ending balance | closing cash | final cash',
  portfolio.CASH_DEPOSITS as CASH_DEPOSITS comment='Cash deposits | cash inflows | deposits',
  portfolio.CASH_WITHDRAWALS as CASH_WITHDRAWALS comment='Cash withdrawals | cash outflows | withdrawals',
  portfolio.CASH_NET_FLOW as CASH_NET_FLOW comment='Net cash flow | cash flow | deposits minus withdrawals',
  portfolio.CASH_TWR_PERCENTAGE as CASH_TWR_PERCENTAGE comment='Cash time weighted return | cash TWR | cash return',
  portfolio.CURRENT_CASH_VALUE_CHF as CURRENT_CASH_VALUE_CHF comment='Current cash value | cash position | liquid assets',
  
  portfolio.EQUITY_TRADES_COUNT as EQUITY_TRADES_COUNT comment='Equity trades | stock trades | equity transactions',
  portfolio.EQUITY_BUY_TRADES as EQUITY_BUY_TRADES comment='Equity buy trades | stock purchases',
  portfolio.EQUITY_SELL_TRADES as EQUITY_SELL_TRADES comment='Equity sell trades | stock sales',
  portfolio.EQUITY_TOTAL_INVESTED_CHF as EQUITY_TOTAL_INVESTED_CHF comment='Equity invested | equity capital | stock investment',
  portfolio.EQUITY_REALIZED_PL_CHF as EQUITY_REALIZED_PL_CHF comment='Equity realized PL | equity profit loss | stock gains',
  portfolio.EQUITY_COMMISSION_CHF as EQUITY_COMMISSION_CHF comment='Equity commission | trading fees | transaction costs',
  portfolio.EQUITY_NET_RETURN_CHF as EQUITY_NET_RETURN_CHF comment='Equity net return | equity gain | net equity profit',
  portfolio.EQUITY_RETURN_PERCENTAGE as EQUITY_RETURN_PERCENTAGE comment='Equity return percentage | stock return | equity performance',
  portfolio.CURRENT_EQUITY_POSITIONS as CURRENT_EQUITY_POSITIONS comment='Current equity positions | number of stocks | equity holdings',
  portfolio.CURRENT_EQUITY_VALUE_CHF as CURRENT_EQUITY_VALUE_CHF comment='Current equity value | stock value | equity market value',
  
  portfolio.FI_TRADES_COUNT as FI_TRADES_COUNT comment='Fixed income trades | bond trades | FI transactions',
  portfolio.FI_BUY_TRADES as FI_BUY_TRADES comment='Fixed income buy trades | bond purchases',
  portfolio.FI_SELL_TRADES as FI_SELL_TRADES comment='Fixed income sell trades | bond sales',
  portfolio.FI_TOTAL_INVESTED_CHF as FI_TOTAL_INVESTED_CHF comment='Fixed income invested | bond capital | FI investment',
  portfolio.FI_NET_PL_CHF as FI_NET_PL_CHF comment='Fixed income PL | bond profit loss | FI gains',
  portfolio.FI_COMMISSION_CHF as FI_COMMISSION_CHF comment='Fixed income commission | bond fees',
  portfolio.FI_RETURN_PERCENTAGE as FI_RETURN_PERCENTAGE comment='Fixed income return | bond return | FI performance',
  portfolio.CURRENT_FI_POSITIONS as CURRENT_FI_POSITIONS comment='Current FI positions | number of bonds | bond holdings',
  portfolio.CURRENT_FI_VALUE_CHF as CURRENT_FI_VALUE_CHF comment='Current FI value | bond value | fixed income market value',
  
  portfolio.CMD_TRADES_COUNT as CMD_TRADES_COUNT comment='Commodity trades | CMD trades | commodity transactions',
  portfolio.CMD_BUY_TRADES as CMD_BUY_TRADES comment='Commodity buy trades | commodity purchases',
  portfolio.CMD_SELL_TRADES as CMD_SELL_TRADES comment='Commodity sell trades | commodity sales',
  portfolio.CMD_TOTAL_INVESTED_CHF as CMD_TOTAL_INVESTED_CHF comment='Commodity invested | commodity capital',
  portfolio.CMD_NET_PL_CHF as CMD_NET_PL_CHF comment='Commodity PL | commodity profit loss | CMD gains',
  portfolio.CMD_COMMISSION_CHF as CMD_COMMISSION_CHF comment='Commodity commission | commodity fees',
  portfolio.CMD_RETURN_PERCENTAGE as CMD_RETURN_PERCENTAGE comment='Commodity return | commodity performance',
  portfolio.CURRENT_CMD_POSITIONS as CURRENT_CMD_POSITIONS comment='Current commodity positions | commodity holdings',
  portfolio.CURRENT_CMD_VALUE_CHF as CURRENT_CMD_VALUE_CHF comment='Current commodity value | commodity market value',
  
  portfolio.TOTAL_PORTFOLIO_VALUE_CHF as TOTAL_PORTFOLIO_VALUE_CHF 
    WITH SYNONYMS = ('AUM', 'assets under management', 'managed assets', 'portfolio value', 'portfolio worth', 'client portfolio')
    comment='Per-portfolio total value in CHF. Use for individual portfolio lookups and rankings',
  portfolio.TOTAL_RETURN_CHF as TOTAL_RETURN_CHF comment='Total return | absolute return | portfolio gain | profit',
  portfolio.TOTAL_PORTFOLIO_TWR_PERCENTAGE as TOTAL_PORTFOLIO_TWR_PERCENTAGE comment='Total TWR | time weighted return | portfolio TWR | portfolio return',
  portfolio.ANNUALIZED_PORTFOLIO_TWR as ANNUALIZED_PORTFOLIO_TWR comment='Annualized return | annual return | CAGR | compound growth',
  
  portfolio.CASH_ALLOCATION_PERCENTAGE as CASH_ALLOCATION_PERCENTAGE comment='Cash allocation | cash weight | cash percentage | liquidity',
  portfolio.EQUITY_ALLOCATION_PERCENTAGE as EQUITY_ALLOCATION_PERCENTAGE comment='Equity allocation | stock weight | equity percentage',
  portfolio.FI_ALLOCATION_PERCENTAGE as FI_ALLOCATION_PERCENTAGE comment='Fixed income allocation | bond weight | FI percentage',
  portfolio.CMD_ALLOCATION_PERCENTAGE as CMD_ALLOCATION_PERCENTAGE comment='Commodity allocation | commodity weight | CMD percentage',
  
  portfolio.PORTFOLIO_VOLATILITY as PORTFOLIO_VOLATILITY comment='Portfolio volatility | risk | standard deviation | price volatility',
  portfolio.SHARPE_RATIO as SHARPE_RATIO comment='Sharpe ratio | risk adjusted return | Sharpe index',
  portfolio.RISK_FREE_RATE_ANNUAL_PCT as RISK_FREE_RATE_ANNUAL_PCT comment='Risk free rate | RFR | benchmark rate',
  portfolio.MAX_DRAWDOWN_PERCENTAGE as MAX_DRAWDOWN_PERCENTAGE comment='Max drawdown | maximum drawdown | peak to trough | largest loss',
  
  portfolio.TOTAL_TRANSACTIONS as TOTAL_TRANSACTIONS comment='Total transactions | total trades | transaction count',
  portfolio.TRANSACTION_FREQUENCY as TRANSACTION_FREQUENCY comment='Transaction frequency | trading frequency | trades per period',
  portfolio.TRADING_DAYS as TRADING_DAYS comment='Trading days | active days | days traded',
  
  portfolio.CALCULATION_TIMESTAMP as CALCULATION_TIMESTAMP comment='Calculation timestamp | measurement time | analysis time'
)

dimensions (
  portfolio.ACCOUNT_TYPE AS ACCOUNT_TYPE 
    WITH SYNONYMS = ('account type', 'portfolio type', 'investment type', 'account category')
    COMMENT = 'Account type (CHECKING, SAVINGS, INVESTMENT, BUSINESS). Primary dimension for account segmentation'
    SAMPLE_VALUES ('CHECKING', 'SAVINGS', 'INVESTMENT', 'BUSINESS')
    IS_ENUM,
  portfolio.PERFORMANCE_CATEGORY AS PERFORMANCE_CATEGORY 
    WITH SYNONYMS = ('performance', 'performance rating', 'return category', 'performance level', 'excellent', 'good', 'neutral', 'poor', 'negative')
    COMMENT = 'Performance category. Use for filtering high/low performers'
    SAMPLE_VALUES ('EXCELLENT_PERFORMANCE', 'GOOD_PERFORMANCE', 'NEUTRAL_PERFORMANCE', 'POOR_PERFORMANCE', 'NEGATIVE_PERFORMANCE')
    IS_ENUM,
  portfolio.RISK_CATEGORY AS RISK_CATEGORY 
    WITH SYNONYMS = ('risk level', 'risk rating', 'risk classification', 'high risk', 'moderate risk', 'low risk')
    COMMENT = 'Risk category by share of equity + commodity holdings: LOW (<20%), MODERATE (20-50%), HIGH (50-80%), VERY_HIGH (>80%). "High risk" means HIGH or VERY_HIGH'
    SAMPLE_VALUES ('LOW', 'MODERATE', 'HIGH', 'VERY_HIGH')
    IS_ENUM,
  
  portfolio.BASE_CURRENCY AS BASE_CURRENCY 
    WITH SYNONYMS = ('currency', 'reporting currency', 'portfolio currency')
    COMMENT = 'Base currency (CHF, USD, EUR, GBP). Use for currency-based analysis',
  portfolio.PORTFOLIO_TYPE AS PORTFOLIO_TYPE 
    COMMENT = 'Portfolio type or investment style classification',
  portfolio.IS_HIGH_PERFORMER LABELS = (FILTER) AS portfolio.PERFORMANCE_CATEGORY IN ('EXCELLENT_PERFORMANCE', 'GOOD_PERFORMANCE')
    COMMENT = 'Filter to EXCELLENT and GOOD performance portfolios',
  portfolio.IS_HIGH_RISK_PORTFOLIO LABELS = (FILTER) AS portfolio.RISK_CATEGORY IN ('HIGH', 'VERY_HIGH')
    COMMENT = 'Filter to high-risk portfolios (RISK_CATEGORY HIGH or VERY_HIGH)'
)

metrics (
  portfolio.PORTFOLIO_COUNT AS COUNT(portfolio.ACCOUNT_ID)
    WITH SYNONYMS = ('number of portfolios', 'portfolio count', 'how many portfolios', 'total portfolios', 'account count')
    COMMENT = 'Count of portfolios. Use for "how many portfolios" queries',
  portfolio.TOTAL_AUM AS SUM(portfolio.TOTAL_PORTFOLIO_VALUE_CHF)
    WITH SYNONYMS = ('total AUM', 'total assets', 'sum of portfolio values', 'aggregate wealth', 'combined AUM')
    COMMENT = 'Sum of all portfolio values. Use for total AUM calculations and wealth aggregation',
  portfolio.AVG_PORTFOLIO_VALUE AS AVG(portfolio.TOTAL_PORTFOLIO_VALUE_CHF)
    WITH SYNONYMS = ('average AUM', 'mean portfolio value', 'typical portfolio size', 'avg wealth')
    COMMENT = 'Average portfolio value. Use for typical client wealth analysis',
  portfolio.AVG_RETURN AS AVG(portfolio.TOTAL_PORTFOLIO_TWR_PERCENTAGE)
    WITH SYNONYMS = ('average return', 'mean return', 'typical return', 'avg performance')
    COMMENT = 'Average time-weighted return. Use for average performance analysis',
  portfolio.AVG_SHARPE AS AVG(portfolio.SHARPE_RATIO)
    WITH SYNONYMS = ('average Sharpe', 'mean Sharpe ratio', 'typical risk-adjusted return')
    COMMENT = 'Average Sharpe ratio. Use for risk-adjusted performance comparison',
  portfolio.HIGH_PERFORMERS AS COUNT(CASE WHEN portfolio.PERFORMANCE_CATEGORY IN ('EXCELLENT_PERFORMANCE', 'GOOD_PERFORMANCE') THEN 1 END)
    WITH SYNONYMS = ('excellent performers', 'good performers', 'top performers', 'high performance count')
    COMMENT = 'Count of portfolios with EXCELLENT or GOOD performance. Use for success rate analysis'
)

COMMENT = 'Wealth management - multi-asset portfolio performance (TWR, Sharpe, drawdown), asset allocation, and risk classification.'

AI_SQL_GENERATION 'For AUM queries, use TOTAL_PORTFOLIO_VALUE_CHF. All monetary values are in CHF. For return calculations, use TOTAL_PORTFOLIO_TWR_PERCENTAGE (not ANNUALIZED unless the user asks for annualized). When asked about "top clients", join to customer context via CUSTOMER_ID and order by TOTAL_PORTFOLIO_VALUE_CHF DESC.'

AI_QUESTION_CATEGORIZATION 'Reject questions about individual trade executions or order book details. If asked about customer names or contact info, explain that PII is in the Customer 360 view accessed via the customer context tool.'

AI_VERIFIED_QUERIES (
  top_portfolios_by_value AS (
    QUESTION 'Show me top 10 portfolios by total value'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT portfolio.ACCOUNT_ID, portfolio.CUSTOMER_ID, portfolio.TOTAL_PORTFOLIO_VALUE_CHF, portfolio.TOTAL_PORTFOLIO_TWR_PERCENTAGE, portfolio.SHARPE_RATIO, portfolio.PERFORMANCE_CATEGORY, portfolio.RISK_CATEGORY FROM __portfolio AS portfolio ORDER BY portfolio.TOTAL_PORTFOLIO_VALUE_CHF DESC NULLS LAST LIMIT 10'
  ),
  portfolios_by_performance AS (
    QUESTION 'How many portfolios per performance category?'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT portfolio.PERFORMANCE_CATEGORY, COUNT(portfolio.ACCOUNT_ID) AS PORTFOLIO_COUNT, AVG(portfolio.TOTAL_PORTFOLIO_TWR_PERCENTAGE) AS AVG_RETURN FROM __portfolio AS portfolio GROUP BY portfolio.PERFORMANCE_CATEGORY ORDER BY PORTFOLIO_COUNT DESC'
  ),
  high_risk_poor_performance AS (
    QUESTION 'Find portfolios with HIGH risk and POOR or NEGATIVE performance'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT portfolio.ACCOUNT_ID, portfolio.CUSTOMER_ID, portfolio.TOTAL_PORTFOLIO_VALUE_CHF, portfolio.TOTAL_PORTFOLIO_TWR_PERCENTAGE, portfolio.MAX_DRAWDOWN_PERCENTAGE, portfolio.RISK_CATEGORY, portfolio.PERFORMANCE_CATEGORY FROM __portfolio AS portfolio WHERE portfolio.RISK_CATEGORY IN (''HIGH'', ''VERY_HIGH'') AND portfolio.PERFORMANCE_CATEGORY IN (''POOR_PERFORMANCE'', ''NEGATIVE_PERFORMANCE'') ORDER BY portfolio.TOTAL_PORTFOLIO_VALUE_CHF DESC NULLS LAST'
  ),
  avg_return_by_account_type AS (
    QUESTION 'Average return by account type'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT portfolio.ACCOUNT_TYPE, COUNT(portfolio.ACCOUNT_ID) AS PORTFOLIO_COUNT, AVG(portfolio.TOTAL_PORTFOLIO_TWR_PERCENTAGE) AS AVG_RETURN, AVG(portfolio.SHARPE_RATIO) AS AVG_SHARPE FROM __portfolio AS portfolio GROUP BY portfolio.ACCOUNT_TYPE ORDER BY AVG_RETURN DESC NULLS LAST'
  ),
  total_aum AS (
    QUESTION 'What is our total AUM across all portfolios?'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT SUM(portfolio.TOTAL_PORTFOLIO_VALUE_CHF) AS TOTAL_AUM, COUNT(portfolio.ACCOUNT_ID) AS PORTFOLIO_COUNT, AVG(portfolio.TOTAL_PORTFOLIO_VALUE_CHF) AS AVG_PORTFOLIO_VALUE FROM __portfolio AS portfolio'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.REPA_SV_RISK_REPORTING
tables (
  risk AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_AGGREGATION
    PRIMARY KEY (CUSTOMER_ID, REPORTING_DATE, RISK_TYPE, GEOGRAPHY)
    COMMENT = 'BCBS 239 risk aggregation and capital requirements',
  customers AS {{ db }}.{{ crm_agg }}.CRMA_AGG_VW_CUSTOMER_360_ENRICHED
    PRIMARY KEY (CUSTOMER_ID)
    COMMENT = 'Customer profile for risk attribution'
)

relationships (
  risk_to_customer AS
    risk (CUSTOMER_ID) REFERENCES customers
)

facts (
  risk.CUSTOMER_ID as CUSTOMER_ID comment='Customer ID | client ID | customer identifier for individual risk tracking',
  risk.REPORTING_DATE as REPORTING_DATE comment='Reporting date | report date | as of date | business date for regulatory reporting',
  
  risk.TOTAL_EXPOSURE_CHF as TOTAL_EXPOSURE_CHF 
    WITH SYNONYMS = ('exposure', 'risk exposure', 'at-risk amount', 'exposed capital', 'risk amount')
    comment='Total risk exposure in CHF. Primary metric for risk aggregation and regulatory reporting',
  risk.TOTAL_CAPITAL_REQUIREMENT_CHF as TOTAL_CAPITAL_REQUIREMENT_CHF 
    WITH SYNONYMS = ('capital', 'capital amount', 'capital requirement', 'required capital')
    comment='Capital requirement or amount. Primary metric for capital adequacy analysis and FRTB reporting',
  risk.AVG_RISK_WEIGHT as AVG_RISK_WEIGHT comment='Average risk weight | risk weight | risk weight percentage',
  risk.CUSTOMER_COUNT as CUSTOMER_COUNT comment='Customer count | number of customers | client count in risk segment',
  risk.MAX_SINGLE_EXPOSURE_CHF as MAX_SINGLE_EXPOSURE_CHF comment='Maximum single exposure | largest exposure | max concentration',
  risk.EXPOSURE_VOLATILITY_CHF as EXPOSURE_VOLATILITY_CHF comment='Exposure volatility | volatility | standard deviation of exposures',
  risk.MAX_CONCENTRATION_PERCENT as MAX_CONCENTRATION_PERCENT comment='Maximum concentration | concentration percentage | max single customer concentration',
  risk.CAPITAL_RATIO_PERCENT as CAPITAL_RATIO_PERCENT comment='Capital ratio | capital adequacy ratio | CAR percentage',
  risk.AVG_EXPOSURE_PER_CUSTOMER_CHF as AVG_EXPOSURE_PER_CUSTOMER_CHF comment='Average exposure per customer | average exposure | mean customer exposure',
  risk.AGGREGATION_TIMESTAMP as AGGREGATION_TIMESTAMP comment='Aggregation timestamp | calculation timestamp | when aggregation was performed'
)

dimensions (
  risk.RISK_TYPE AS RISK_TYPE 
    WITH SYNONYMS = ('risk type', 'risk category', 'type of risk', 'credit risk', 'market risk', 'operational risk', 'liquidity risk')
    COMMENT = 'Risk type (CREDIT, MARKET, OPERATIONAL, LIQUIDITY). Primary dimension for risk category analysis',
  risk.GEOGRAPHY AS GEOGRAPHY 
    WITH SYNONYMS = ('geography', 'region', 'country', 'location', 'area', 'territory', 'geographic region')
    COMMENT = 'Geographic region. Primary dimension for regional risk analysis and "by region" queries',
  risk.CURRENCY AS CURRENCY 
    WITH SYNONYMS = ('currency', 'currency code', 'reporting currency', 'FX', 'foreign exchange')
    COMMENT = 'Reporting currency (CHF, USD, EUR, GBP). Use for currency-based risk analysis',
  risk.CUSTOMER_SEGMENT AS CUSTOMER_SEGMENT 
    WITH SYNONYMS = ('customer segment', 'client segment', 'risk segment', 'low risk', 'medium risk', 'high risk')
    COMMENT = 'Customer risk segment (LOW_RISK, MEDIUM_RISK, HIGH_RISK). Use for risk appetite segmentation',
  
  risk.BUSINESS_LINE AS BUSINESS_LINE 
    WITH SYNONYMS = ('business line', 'LOB', 'line of business', 'business unit')
    COMMENT = 'Business line or line of business. Use for risk allocation by business unit'
)

metrics (
  risk.CUSTOMER_RISK_COUNT AS COUNT(risk.CUSTOMER_ID)
    WITH SYNONYMS = ('number of customers', 'customer count', 'how many customers', 'at-risk customers', 'exposed customers')
    COMMENT = 'Count of customers with risk exposure. Use for "how many customers" queries',
  risk.TOTAL_EXPOSURE_SUM AS SUM(risk.TOTAL_EXPOSURE_CHF)
    WITH SYNONYMS = ('total exposure', 'aggregate exposure', 'sum of exposures', 'total risk exposure', 'exposure at default', 'EAD')
    COMMENT = 'Sum of all risk exposures (not risk-weighted). Use for total exposure; for risk-weighted assets use TOTAL_RWA_SUM',
  risk.TOTAL_RWA_SUM AS SUM(risk.TOTAL_EXPOSURE_CHF * risk.AVG_RISK_WEIGHT / 100)
    WITH SYNONYMS = ('RWA', 'risk-weighted assets', 'risk weighted assets', 'total RWA', 'risk-weighted exposure')
    COMMENT = 'Risk-weighted assets: exposure x risk weight. Approximation: each row holds the average risk weight of its exposures. Use for RWA questions',
  risk.TOTAL_CAPITAL_SUM AS SUM(risk.TOTAL_CAPITAL_REQUIREMENT_CHF)
    WITH SYNONYMS = ('total capital', 'aggregate capital', 'sum of capital requirements', 'capital requirement', 'required capital')
    COMMENT = 'Sum of all capital requirements. Use for total capital adequacy analysis',
  risk.AVG_EXPOSURE AS AVG(risk.TOTAL_EXPOSURE_CHF)
    WITH SYNONYMS = ('average exposure', 'mean exposure', 'typical exposure', 'avg risk')
    COMMENT = 'Average risk exposure. Use for typical risk level analysis',
  risk.MAX_CONCENTRATION AS MAX(risk.MAX_SINGLE_EXPOSURE_CHF)
    WITH SYNONYMS = ('maximum concentration', 'largest exposure', 'peak concentration', 'max exposure')
    COMMENT = 'Maximum single exposure. Use for concentration risk analysis'
)

AI_VERIFIED_QUERIES (
  exposure_by_risk_type AS (
    QUESTION 'What is the total exposure and capital requirement by risk type?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT risk.RISK_TYPE, SUM(risk.TOTAL_EXPOSURE_CHF) AS TOTAL_EXPOSURE_CHF, SUM(risk.TOTAL_CAPITAL_REQUIREMENT_CHF) AS TOTAL_CAPITAL_REQUIREMENT_CHF, COUNT(DISTINCT risk.CUSTOMER_ID) AS CUSTOMERS FROM __risk AS risk GROUP BY risk.RISK_TYPE ORDER BY TOTAL_EXPOSURE_CHF DESC'
  ),
  rwa_by_risk_type AS (
    QUESTION 'What are our risk-weighted assets by risk type?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT risk.RISK_TYPE, SUM(risk.TOTAL_EXPOSURE_CHF) AS TOTAL_EXPOSURE_CHF, SUM(risk.TOTAL_EXPOSURE_CHF * risk.AVG_RISK_WEIGHT / 100) AS RWA_CHF FROM __risk AS risk GROUP BY risk.RISK_TYPE ORDER BY RWA_CHF DESC'
  )
);
{% endif %}
