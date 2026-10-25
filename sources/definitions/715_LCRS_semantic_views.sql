{# semantic_views='false' is only used for the first deploy into an empty database:
   semantic views validate against their base dynamic tables, which must be initialized first #}
{% if semantic_views == 'true' %}
-- ============================================================
-- FINMA LCR SEMANTIC VIEWS
-- Cortex Analyst semantic views (moved from post_deploy.sql).
-- ============================================================

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LCRS_SV_LCR_CURRENT
tables (
  lcr_data AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
    COMMENT = 'LCR daily calculation with HQLA, outflows, and compliance status'
)

facts (
  lcr_data.AS_OF_DATE as AS_OF_DATE 
    WITH SYNONYMS = ('reporting date', 'calculation date', 'snapshot date', 'data date')
    comment='Date of LCR calculation | as of date | reporting date',
  
  lcr_data.BANK_ID as BANK_ID
    comment='Bank identifier',
  
  lcr_data.LCR_RATIO as LCR_RATIO
    WITH SYNONYMS = ('LCR', 'liquidity ratio', 'coverage ratio', 'LCR percentage', 'liquidity coverage ratio')
    comment='Liquidity Coverage Ratio percentage. Primary liquidity metric. Must be >= 100% for FINMA compliance',
  
  lcr_data.HQLA_TOTAL as HQLA_TOTAL
    WITH SYNONYMS = ('HQLA', 'liquid assets', 'high quality assets', 'total HQLA', 'numerator', 'total liquid assets')
    comment='Total High-Quality Liquid Assets in CHF. LCR numerator. Assets available for 30-day stress scenario',
  
  lcr_data.L1_TOTAL as L1_TOTAL
    WITH SYNONYMS = ('Level 1', 'Level 1 assets', 'L1 HQLA', 'highest quality assets', 'zero haircut assets')
    comment='Level 1 HQLA (0% haircut): SNB reserves, cash, Swiss government bonds',
  
  lcr_data.L2A_TOTAL as L2A_TOTAL
    WITH SYNONYMS = ('Level 2A', 'Level 2A assets', 'L2A HQLA')
    comment='Level 2A HQLA (15% haircut): Canton bonds, covered bonds',
  
  lcr_data.L2B_TOTAL as L2B_TOTAL
    WITH SYNONYMS = ('Level 2B', 'Level 2B assets', 'L2B HQLA')
    comment='Level 2B HQLA (50% haircut): SMI equities, corporate bonds',
  
  lcr_data.L2_UNCAPPED as L2_UNCAPPED
    WITH SYNONYMS = ('Level 2 before cap', 'uncapped Level 2')
    comment='Level 2 assets before applying 40% cap',
  
  lcr_data.L2_CAPPED as L2_CAPPED
    WITH SYNONYMS = ('Level 2 after cap', 'capped Level 2')
    comment='Level 2 assets after applying 40% cap',
  
  lcr_data.CAP_APPLIED as CAP_APPLIED
    WITH SYNONYMS = ('40% cap applied', 'Level 2 cap triggered', 'cap status', 'is cap active')
    comment='Whether 40% cap on Level 2 assets is applied',
  
  lcr_data.DISCARDED_L2 as DISCARDED_L2
    WITH SYNONYMS = ('discarded Level 2', 'excess Level 2', 'capped assets')
    comment='Discarded Level 2 assets due to 40% cap',
  
  lcr_data.OUTFLOW_TOTAL as OUTFLOW_TOTAL
    WITH SYNONYMS = ('total outflows', 'cash outflows', 'net outflows', 'denominator', 'stressed outflows')
    comment='Total 30-day stressed net cash outflows in CHF. LCR denominator',
  
  lcr_data.OUTFLOW_RETAIL as OUTFLOW_RETAIL
    WITH SYNONYMS = ('retail outflows', 'individual customer outflows')
    comment='Retail customer outflows',
  
  lcr_data.OUTFLOW_CORP as OUTFLOW_CORP
    WITH SYNONYMS = ('corporate outflows', 'business customer outflows')
    comment='Corporate customer outflows',
  
  lcr_data.OUTFLOW_FI as OUTFLOW_FI
    WITH SYNONYMS = ('FI outflows', 'financial institution outflows', 'bank outflows', 'wholesale outflows')
    comment='Financial institution outflows | interbank outflows',
  
  lcr_data.LCR_BUFFER_CHF as LCR_BUFFER_CHF
    WITH SYNONYMS = ('buffer', 'liquidity buffer', 'cushion', 'excess liquidity', 'safety margin')
    comment='Liquidity buffer (HQLA minus Outflows) in CHF',
  
  lcr_data.LCR_BUFFER_PCT as LCR_BUFFER_PCT
    WITH SYNONYMS = ('buffer percentage', 'buffer percent')
    comment='Liquidity buffer as percentage of outflows',
  
  lcr_data.TOTAL_HOLDINGS as TOTAL_HOLDINGS
    WITH SYNONYMS = ('securities count', 'HQLA securities', 'holdings count', 'portfolio size')
    comment='Number of HQLA securities',
  
  lcr_data.TOTAL_DEPOSIT_ACCOUNTS as TOTAL_DEPOSIT_ACCOUNTS
    WITH SYNONYMS = ('deposit accounts', 'account count')
    comment='Number of deposit accounts',
  
  lcr_data.CALCULATION_TIMESTAMP as CALCULATION_TIMESTAMP
    WITH SYNONYMS = ('calculation time', 'data timestamp', 'calculated at')
    comment='When LCR was last calculated'
)

dimensions (
  lcr_data.LCR_STATUS AS LCR_STATUS
    WITH SYNONYMS = ('compliance status', 'regulatory status', 'FINMA status', 'status')
    COMMENT = 'LCR compliance status: PASS (>=100%), WARNING (95-100%), FAIL (<95%), N/A'
    SAMPLE_VALUES ('PASS', 'WARNING', 'FAIL', 'N/A')
    IS_ENUM,
  lcr_data.SEVERITY AS SEVERITY
    WITH SYNONYMS = ('alert level', 'traffic light', 'status color')
    COMMENT = 'Alert severity: GREEN (>=100%), YELLOW (95-100%), RED (<95%), GRAY (N/A)'
    SAMPLE_VALUES ('GREEN', 'YELLOW', 'RED', 'GRAY')
    IS_ENUM
)

metrics (
  lcr_data.LATEST_LCR_RATIO AS MAX(lcr_data.LCR_RATIO)
    WITH SYNONYMS = ('current LCR', 'LCR ratio', 'liquidity ratio')
    COMMENT = 'Most recent LCR ratio percentage',
  lcr_data.LATEST_HQLA AS MAX(lcr_data.HQLA_TOTAL)
    WITH SYNONYMS = ('current HQLA', 'total liquid assets')
    COMMENT = 'Most recent total HQLA in CHF',
  lcr_data.LATEST_OUTFLOW AS MAX(lcr_data.OUTFLOW_TOTAL)
    WITH SYNONYMS = ('current outflows', 'stressed outflows')
    COMMENT = 'Most recent total stressed outflows in CHF'
)

COMMENT = 'Liquidity Coverage Ratio (LCR) per FINMA Circular 2015/2 - HQLA stock, stressed outflows, compliance status, and 40% cap monitoring.'

AI_SQL_GENERATION 'This table typically has one row per date. For "current" or "today" queries, filter to the most recent AS_OF_DATE using: WHERE AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM __lcr_data). For trend queries over multiple dates, do not apply the MAX filter. The LCR must be >= 100% for FINMA compliance. Internal target is 110%.'

AI_VERIFIED_QUERIES (
  current_lcr_ratio AS (
    QUESTION 'What is our current LCR ratio?'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT lcr_data.AS_OF_DATE, lcr_data.LCR_RATIO, lcr_data.LCR_STATUS, lcr_data.HQLA_TOTAL, lcr_data.OUTFLOW_TOTAL, lcr_data.LCR_BUFFER_CHF FROM __lcr_data AS lcr_data WHERE lcr_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __lcr_data AS sub)'
  ),
  hqla_by_level AS (
    QUESTION 'Show me HQLA breakdown by L1, L2A, L2B'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT lcr_data.AS_OF_DATE, lcr_data.L1_TOTAL, lcr_data.L2A_TOTAL, lcr_data.L2B_TOTAL, lcr_data.L2_CAPPED, lcr_data.HQLA_TOTAL, lcr_data.CAP_APPLIED FROM __lcr_data AS lcr_data WHERE lcr_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __lcr_data AS sub)'
  ),
  liquidity_buffer AS (
    QUESTION 'What is our liquidity buffer in CHF?'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT lcr_data.AS_OF_DATE, lcr_data.LCR_BUFFER_CHF, lcr_data.LCR_BUFFER_PCT, lcr_data.HQLA_TOTAL, lcr_data.OUTFLOW_TOTAL FROM __lcr_data AS lcr_data WHERE lcr_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __lcr_data AS sub)'
  ),
  is_cap_applied AS (
    QUESTION 'Is the 40% cap on Level 2 assets applied today?'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT lcr_data.AS_OF_DATE, lcr_data.CAP_APPLIED, lcr_data.L2_UNCAPPED, lcr_data.L2_CAPPED, lcr_data.DISCARDED_L2, lcr_data.L1_TOTAL FROM __lcr_data AS lcr_data WHERE lcr_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __lcr_data AS sub)'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LCRS_SV_HQLA_BREAKDOWN
tables (
  hqla_data AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_HQLA
    COMMENT = 'HQLA holdings by level and asset type'
)

facts (
  hqla_data.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('reporting date', 'date')
    comment='Date of HQLA snapshot',
  
  hqla_data.HQLA_LEVEL as HQLA_LEVEL
    WITH SYNONYMS = ('level', 'asset level', 'regulatory level')
    comment='HQLA regulatory level: L1, L2A, L2B',
  
  hqla_data.ASSET_TYPE as ASSET_TYPE
    WITH SYNONYMS = ('asset', 'asset class', 'security type')
    comment='Type of asset (CASH_SNB, GOVT_BOND_CHF, etc)',
  
  hqla_data.HOLDINGS_COUNT as HOLDINGS_COUNT
    WITH SYNONYMS = ('count', 'number of holdings', 'securities count')
    comment='Number of securities',
  
  hqla_data.MARKET_VALUE_CHF as MARKET_VALUE_CHF
    WITH SYNONYMS = ('market value', 'value', 'gross value')
    comment='Market value before haircuts',
  
  hqla_data.HAIRCUT_PCT as HAIRCUT_PCT
    WITH SYNONYMS = ('haircut', 'haircut percentage')
    comment='Regulatory haircut percentage',
  
  hqla_data.HQLA_VALUE_CHF as HQLA_VALUE_CHF
    WITH SYNONYMS = ('HQLA value', 'value after haircut', 'net value')
    comment='HQLA value after applying haircut',
  
  hqla_data.PCT_OF_TOTAL_HQLA as PCT_OF_TOTAL_HQLA
    WITH SYNONYMS = ('percentage', 'percent of total', 'composition')
    comment='Percentage of total HQLA'
)

AI_VERIFIED_QUERIES (
  hqla_by_asset_type AS (
    QUESTION 'Show the HQLA composition by asset type'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT hqla_data.HQLA_LEVEL, hqla_data.ASSET_TYPE, hqla_data.HOLDINGS_COUNT, hqla_data.MARKET_VALUE_CHF, hqla_data.HAIRCUT_PCT, hqla_data.HQLA_VALUE_CHF, hqla_data.PCT_OF_TOTAL_HQLA FROM __hqla_data AS hqla_data WHERE hqla_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __hqla_data AS sub) ORDER BY hqla_data.HQLA_VALUE_CHF DESC'
  ),
  hqla_by_level_after_haircut AS (
    QUESTION 'How much HQLA do we hold per level before and after haircuts?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT hqla_data.HQLA_LEVEL, SUM(hqla_data.MARKET_VALUE_CHF) AS MARKET_VALUE_CHF, SUM(hqla_data.HQLA_VALUE_CHF) AS HQLA_VALUE_CHF FROM __hqla_data AS hqla_data WHERE hqla_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __hqla_data AS sub) GROUP BY hqla_data.HQLA_LEVEL ORDER BY hqla_data.HQLA_LEVEL'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LCRS_SV_OUTFLOW_BREAKDOWN
tables (
  outflow_data AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW
    COMMENT = 'Deposit outflows by counterparty type'
)

facts (
  outflow_data.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('reporting date', 'date')
    comment='Date of deposit snapshot',
  
  outflow_data.COUNTERPARTY_TYPE as COUNTERPARTY_TYPE
    WITH SYNONYMS = ('counterparty', 'customer type', 'deposit type')
    comment='Type of counterparty (RETAIL, CORPORATE, FINANCIAL_INSTITUTION)',
  
  outflow_data.ACCOUNT_COUNT as ACCOUNT_COUNT
    WITH SYNONYMS = ('accounts', 'number of accounts')
    comment='Number of deposit accounts',
  
  outflow_data.CUSTOMER_COUNT as CUSTOMER_COUNT
    WITH SYNONYMS = ('customers', 'number of customers')
    comment='Number of unique customers',
  
  outflow_data.BALANCE_CHF as BALANCE_CHF
    WITH SYNONYMS = ('balance', 'deposit balance', 'deposits')
    comment='Total deposit balance in CHF',
  
  outflow_data.RUN_OFF_RATE as RUN_OFF_RATE
    WITH SYNONYMS = ('run-off rate', 'runoff rate', 'withdrawal rate')
    comment='Weighted average run-off rate',
  
  outflow_data.OUTFLOW_AMOUNT_CHF as OUTFLOW_AMOUNT_CHF
    WITH SYNONYMS = ('outflow', 'stressed outflow', 'withdrawal amount')
    comment='Expected 30-day outflow amount',
  
  outflow_data.PCT_OF_TOTAL_OUTFLOWS as PCT_OF_TOTAL_OUTFLOWS
    WITH SYNONYMS = ('percentage of outflows', 'outflow percentage')
    comment='Percentage of total outflows',
  
  outflow_data.PCT_OF_TOTAL_DEPOSITS as PCT_OF_TOTAL_DEPOSITS
    WITH SYNONYMS = ('percentage of deposits', 'deposit percentage')
    comment='Percentage of total deposits'
)

AI_VERIFIED_QUERIES (
  outflows_by_counterparty AS (
    QUESTION 'Which counterparty type drives the most stressed outflows?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT outflow_data.COUNTERPARTY_TYPE, outflow_data.BALANCE_CHF, outflow_data.RUN_OFF_RATE, outflow_data.OUTFLOW_AMOUNT_CHF, outflow_data.PCT_OF_TOTAL_OUTFLOWS FROM __outflow_data AS outflow_data WHERE outflow_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __outflow_data AS sub) ORDER BY outflow_data.OUTFLOW_AMOUNT_CHF DESC'
  ),
  deposit_base_by_counterparty AS (
    QUESTION 'How many deposit accounts and customers do we have per counterparty type?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT outflow_data.COUNTERPARTY_TYPE, outflow_data.ACCOUNT_COUNT, outflow_data.CUSTOMER_COUNT, outflow_data.BALANCE_CHF, outflow_data.PCT_OF_TOTAL_DEPOSITS FROM __outflow_data AS outflow_data WHERE outflow_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __outflow_data AS sub) ORDER BY outflow_data.BALANCE_CHF DESC'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LCRS_SV_TREND_90DAY
tables (
  trend_data AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_DAILY
    COMMENT = 'Daily LCR ratios for trend analysis'
)

facts (
  trend_data.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('date', 'reporting date', 'day')
    comment='Date of LCR calculation',
  
  trend_data.LCR_RATIO as LCR_RATIO
    WITH SYNONYMS = ('LCR', 'ratio', 'liquidity ratio')
    comment='Daily LCR ratio percentage',
  
  trend_data.LCR_STATUS as LCR_STATUS
    WITH SYNONYMS = ('status', 'compliance status')
    comment='Compliance status (PASS/WARNING/FAIL)',
  
  trend_data.SEVERITY as SEVERITY
    WITH SYNONYMS = ('severity', 'color', 'alert level')
    comment='Severity indicator (GREEN/YELLOW/RED)',
  
  trend_data.HQLA_TOTAL as HQLA_TOTAL
    WITH SYNONYMS = ('HQLA', 'liquid assets')
    comment='Total HQLA in CHF',
  
  trend_data.OUTFLOW_TOTAL as OUTFLOW_TOTAL
    WITH SYNONYMS = ('outflows', 'cash outflows')
    comment='Total net cash outflows',
  
  trend_data.LCR_BUFFER_CHF as LCR_BUFFER_CHF
    WITH SYNONYMS = ('buffer', 'liquidity buffer', 'excess')
    comment='Liquidity buffer in CHF',
  
  trend_data.CAP_APPLIED as CAP_APPLIED
    WITH SYNONYMS = ('cap applied', '40% cap', 'cap status')
    comment='Whether 40% cap was applied'
)

AI_VERIFIED_QUERIES (
  lcr_trend_90_days AS (
    QUESTION 'Show the LCR trend over the last 90 days'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT trend_data.AS_OF_DATE, trend_data.LCR_RATIO, trend_data.LCR_STATUS, trend_data.HQLA_TOTAL, trend_data.OUTFLOW_TOTAL, trend_data.LCR_BUFFER_CHF FROM __trend_data AS trend_data WHERE trend_data.AS_OF_DATE > DATEADD(day, -90, (SELECT MAX(sub.AS_OF_DATE) FROM __trend_data AS sub)) ORDER BY trend_data.AS_OF_DATE'
  ),
  days_below_minimum AS (
    QUESTION 'On how many days was the LCR below 100% in the last 90 days?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT COUNT_IF(trend_data.LCR_RATIO < 100) AS DAYS_BELOW_100, MIN(trend_data.LCR_RATIO) AS MIN_LCR, MAX(trend_data.LCR_RATIO) AS MAX_LCR, AVG(trend_data.LCR_RATIO) AS AVG_LCR FROM __trend_data AS trend_data WHERE trend_data.AS_OF_DATE > DATEADD(day, -90, (SELECT MAX(sub.AS_OF_DATE) FROM __trend_data AS sub))'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LCRS_SV_ALERTS_ACTIVE
tables (
  alerts_data AS {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_ALERTS
    COMMENT = 'Active LCR compliance alerts'
)

facts (
  alerts_data.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('date', 'alert date')
    comment='Date of alert',
  
  alerts_data.LCR_RATIO as LCR_RATIO
    WITH SYNONYMS = ('LCR', 'ratio')
    comment='LCR ratio when alert triggered',
  
  alerts_data.LCR_STATUS as LCR_STATUS
    WITH SYNONYMS = ('status', 'compliance status')
    comment='Compliance status',
  
  alerts_data.SEVERITY as SEVERITY
    WITH SYNONYMS = ('severity', 'alert severity', 'priority')
    comment='Alert severity level',
  
  alerts_data.TOTAL_ALERT_COUNT as TOTAL_ALERT_COUNT
    WITH SYNONYMS = ('alert count', 'number of alerts', 'alerts')
    comment='Total number of active alerts',
  
  alerts_data.HIGHEST_SEVERITY as HIGHEST_SEVERITY
    WITH SYNONYMS = ('highest severity', 'max severity', 'worst severity')
    comment='Highest severity among all alerts'
)

AI_VERIFIED_QUERIES (
  active_lcr_alerts AS (
    QUESTION 'Are there any active LCR alerts?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT alerts_data.AS_OF_DATE, alerts_data.LCR_RATIO, alerts_data.LCR_STATUS, alerts_data.SEVERITY, alerts_data.TOTAL_ALERT_COUNT, alerts_data.HIGHEST_SEVERITY FROM __alerts_data AS alerts_data ORDER BY alerts_data.AS_OF_DATE DESC'
  ),
  alert_count_by_severity AS (
    QUESTION 'How many LCR alerts do we have per severity?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT alerts_data.SEVERITY, COUNT(*) AS ALERT_DAYS, MIN(alerts_data.LCR_RATIO) AS MIN_LCR FROM __alerts_data AS alerts_data GROUP BY alerts_data.SEVERITY ORDER BY ALERT_DAYS DESC'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LCRS_SV_LCR_SCENARIOS
tables (
  scen AS {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_SCENARIO_DAILY
    PRIMARY KEY (AS_OF_DATE, STRESS_SCENARIO)
    COMMENT = 'Daily LCR per scenario: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS, COMBINED'
)
facts (
  scen.LCR_RATIO as LCR_RATIO
    WITH SYNONYMS = ('LCR', 'liquidity coverage ratio', 'stressed LCR')
    comment='LCR in percent = HQLA / net cash outflows x 100',
  scen.HQLA_TOTAL as HQLA_TOTAL
    WITH SYNONYMS = ('HQLA', 'liquidity buffer stock')
    comment='HQLA after haircuts and caps in CHF',
  scen.NET_OUTFLOW as NET_OUTFLOW
    WITH SYNONYMS = ('net outflows', 'net cash outflows')
    comment='Net cash outflows in CHF (outflows - inflows capped at 75%)',
  scen.OUTFLOW_TOTAL as OUTFLOW_TOTAL
    WITH SYNONYMS = ('gross outflows', 'stressed outflows')
    comment='Gross stressed 30-day outflows in CHF',
  scen.INFLOW_CAPPED as INFLOW_CAPPED
    WITH SYNONYMS = ('inflows', 'capped inflows')
    comment='30-day inflows after the 75% cap in CHF',
  scen.LCR_BUFFER_CHF as LCR_BUFFER_CHF
    WITH SYNONYMS = ('buffer', 'liquidity buffer', 'surplus')
    comment='HQLA minus net outflows in CHF'
)
dimensions (
  scen.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('date', 'reporting date', 'cut-off date')
    comment='Reporting (cut-off) date',
  scen.STRESS_SCENARIO as STRESS_SCENARIO
    WITH SYNONYMS = ('scenario', 'stress scenario', 'stress test')
    comment='Scenario code: BASE = regulatory LCR; BANK_STRESS = idiosyncratic; MARKET_STRESS = market-wide; COMBINED = both',
  scen.SCENARIO_NAME as SCENARIO_NAME
    WITH SYNONYMS = ('scenario name')
    comment='Scenario display name',
  scen.LCR_STATUS as LCR_STATUS
    WITH SYNONYMS = ('status', 'compliance status')
    comment='FAIL (<100%), WARNING (100% to <110%), PASS (>=110%)'
)
metrics (
  scen.AVG_LCR as AVG(scen.LCR_RATIO)
    WITH SYNONYMS = ('average LCR')
    comment='Average LCR in percent over the selected days',
  scen.MIN_LCR as MIN(scen.LCR_RATIO)
    WITH SYNONYMS = ('lowest LCR', 'minimum LCR')
    comment='Lowest LCR in percent over the selected days',
  scen.FAIL_DAYS as COUNT_IF(scen.LCR_RATIO < 100)
    WITH SYNONYMS = ('breach days', 'days below 100')
    comment='Number of days with LCR below 100%'
)
COMMENT = 'LCR stress scenarios side by side (BASE regulatory vs. bank, market and combined stress). Use for questions like: how low would the LCR go under market stress, which scenario breaches 100%.'

AI_VERIFIED_QUERIES (
  current_lcr_by_scenario AS (
    QUESTION 'Compare the current LCR across all stress scenarios'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT scen.STRESS_SCENARIO, scen.SCENARIO_NAME, scen.LCR_RATIO, scen.LCR_STATUS, scen.HQLA_TOTAL, scen.NET_OUTFLOW, scen.LCR_BUFFER_CHF FROM __scen AS scen WHERE scen.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __scen AS sub) ORDER BY scen.LCR_RATIO'
  ),
  worst_scenario_90_days AS (
    QUESTION 'Which stress scenario has the lowest LCR and how many days below 100%?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT scen.STRESS_SCENARIO, MIN(scen.LCR_RATIO) AS MIN_LCR, AVG(scen.LCR_RATIO) AS AVG_LCR, COUNT_IF(scen.LCR_RATIO < 100) AS FAIL_DAYS FROM __scen AS scen GROUP BY scen.STRESS_SCENARIO ORDER BY MIN_LCR'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LCRS_SV_LCR_DRIVERS
tables (
  drv AS {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_VARIANCE_DRIVERS
    COMMENT = 'Top-10 client drivers of LCR changes per date, comparison window and scenario'
)
facts (
  drv.CLIENT_RANK as CLIENT_RANK
    WITH SYNONYMS = ('rank', 'top driver')
    comment='1 = largest absolute contribution',
  drv.CLIENT_IMPACT_PP as CLIENT_IMPACT_PP
    WITH SYNONYMS = ('impact', 'client impact', 'effect on LCR', 'percentage points')
    comment='Funding-adjusted LCR impact of the client move in pp (incl. cash settlement). Negative = lowered the LCR. Use this to explain root causes.',
  drv.CONTRIBUTION_PP as CONTRIBUTION_PP
    WITH SYNONYMS = ('exact contribution', 'line contribution')
    comment='Exact contribution of the position line only (excludes the cash settlement), sums to the LCR change together with treasury lines',
  drv.LCR_DELTA_PP as LCR_DELTA_PP
    WITH SYNONYMS = ('LCR change', 'LCR move')
    comment='Total LCR change in percentage points',
  drv.DELTA_OUTFLOW_CHF as DELTA_OUTFLOW_CHF
    comment='Change in stressed outflow of the driver in CHF'
)
dimensions (
  drv.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('date', 'day of the change')
    comment='Date of the LCR change (T)',
  drv.PRIOR_DATE as PRIOR_DATE
    comment='Prior date the change is measured against',
  drv.COMPARISON_TYPE as COMPARISON_TYPE
    WITH SYNONYMS = ('comparison', 'window', 'day over day', 'week over week', 'month over month')
    comment='DOD (T vs T-1), WOW (T vs T-7), MOM (T vs T-30); default DOD',
  drv.STRESS_SCENARIO as STRESS_SCENARIO
    WITH SYNONYMS = ('scenario')
    comment='Scenario code; use BASE for the regulatory LCR',
  drv.POSITION_ID as POSITION_ID
    WITH SYNONYMS = ('contract id', 'account id', 'contract')
    comment='Driver contract / deposit account id',
  drv.COUNTERPARTY_NAME as COUNTERPARTY_NAME
    WITH SYNONYMS = ('client', 'client name', 'counterparty', 'customer')
    comment='Client name of the driver',
  drv.PRODUCT_TYPE as PRODUCT_TYPE
    WITH SYNONYMS = ('product')
    comment='Product of the driver position',
  drv.ASSET_CLASS as ASSET_CLASS
    comment='LCR asset class of the driver position',
  drv.BUSINESS_UNIT as BUSINESS_UNIT
    comment='Business unit of the driver'
)
COMMENT = 'Root cause of LCR movements: the client positions (deposits, loans, facilities, interbank) that drove the LCR change, with contract id, client name and exact contribution in pp. Use for questions like: why did the LCR drop on a date, who caused the change.'

AI_VERIFIED_QUERIES (
  largest_negative_drivers AS (
    QUESTION 'Which client moves lowered the regulatory LCR the most day over day?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT drv.AS_OF_DATE, drv.COUNTERPARTY_NAME, drv.POSITION_ID, drv.PRODUCT_TYPE, drv.CLIENT_IMPACT_PP, drv.LCR_DELTA_PP FROM __drv AS drv WHERE drv.STRESS_SCENARIO = ''BASE'' AND drv.COMPARISON_TYPE = ''DOD'' ORDER BY drv.CLIENT_IMPACT_PP ASC LIMIT 10'
  ),
  latest_change_drivers AS (
    QUESTION 'What drove the most recent day-over-day LCR change?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT drv.AS_OF_DATE, drv.PRIOR_DATE, drv.CLIENT_RANK, drv.COUNTERPARTY_NAME, drv.PRODUCT_TYPE, drv.CLIENT_IMPACT_PP, drv.LCR_DELTA_PP FROM __drv AS drv WHERE drv.STRESS_SCENARIO = ''BASE'' AND drv.COMPARISON_TYPE = ''DOD'' AND drv.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __drv AS sub WHERE sub.STRESS_SCENARIO = ''BASE'' AND sub.COMPARISON_TYPE = ''DOD'') ORDER BY drv.CLIENT_RANK'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LCRS_SV_LCR_EVENTS
tables (
  ev AS {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_EVENTS
    PRIMARY KEY (EVENT_ID)
    COMMENT = 'Known liquidity events (withdrawals, market stress, recovery, inflows)'
)
facts (
  ev.AMOUNT_CHF as AMOUNT_CHF
    WITH SYNONYMS = ('amount', 'size')
    comment='Nominal size of the event in CHF'
)
dimensions (
  ev.EVENT_ID as EVENT_ID
    comment='Event identifier',
  ev.EVENT_DATE as EVENT_DATE
    WITH SYNONYMS = ('date', 'when')
    comment='Date the event hit the balance sheet',
  ev.EVENT_TYPE as EVENT_TYPE
    WITH SYNONYMS = ('type', 'kind of event')
    comment='DEPOSIT_WITHDRAWAL, MARKET_STRESS, RECOVERY, LOAN_INFLOW',
  ev.DESCRIPTION as DESCRIPTION
    WITH SYNONYMS = ('what happened', 'explanation')
    comment='Business description of the event',
  ev.COUNTERPARTY_ID as COUNTERPARTY_ID
    comment='Counterparty driving the event',
  ev.CONTRACT_ID as CONTRACT_ID
    comment='Driver position of the event',
  ev.EXPECTED_DIRECTION as EXPECTED_DIRECTION
    comment='Expected LCR direction (UP / DOWN)'
)
COMMENT = 'Timeline of known liquidity events used to explain LCR movements (withdrawals, market stress, recovery, loan inflows).'

AI_VERIFIED_QUERIES (
  liquidity_event_timeline AS (
    QUESTION 'List all known liquidity events'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT ev.EVENT_DATE, ev.EVENT_ID, ev.EVENT_TYPE, ev.AMOUNT_CHF, ev.EXPECTED_DIRECTION, ev.DESCRIPTION FROM __ev AS ev ORDER BY ev.EVENT_DATE'
  ),
  deposit_withdrawal_events AS (
    QUESTION 'Which large deposit withdrawals happened?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT ev.EVENT_DATE, ev.COUNTERPARTY_ID, ev.CONTRACT_ID, ev.AMOUNT_CHF, ev.DESCRIPTION FROM __ev AS ev WHERE ev.EVENT_TYPE = ''DEPOSIT_WITHDRAWAL'' ORDER BY ev.AMOUNT_CHF DESC'
  )
);
{% endif %}
