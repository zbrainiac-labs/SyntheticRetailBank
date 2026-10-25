{# semantic_views='false' is only used for the first deploy into an empty database:
   semantic views validate against their base dynamic tables, which must be initialized first #}
{% if semantic_views == 'true' %}
-- ============================================================
-- PAYMENTS COMPLIANCE SEMANTIC VIEW
-- Cortex Analyst semantic views (moved from post_deploy.sql).
-- ============================================================

DEFINE SEMANTIC VIEW {{ db }}.{{ pay_agg }}.PAYA_SV_COMPLIANCE_MONITORING
tables (
  transactions AS {{ db }}.{{ pay_agg }}.PAYA_AGG_DT_TRANSACTION_ANOMALIES
    PRIMARY KEY (TRANSACTION_ID)
    COMMENT = 'Transaction anomaly detection and compliance monitoring',
  customers AS {{ db }}.{{ crm_agg }}.CRMA_AGG_VW_CUSTOMER_360_ENRICHED
    PRIMARY KEY (CUSTOMER_ID)
    COMMENT = 'Customer profile and risk data for compliance context'
)

relationships (
  transaction_to_customer AS
    transactions (CUSTOMER_ID) REFERENCES customers
)

facts (
  transactions.TRANSACTION_ID as TRANSACTION_ID comment='Transaction ID | transaction number | payment ID | txn ID',
  transactions.ACCOUNT_ID as ACCOUNT_ID comment='Account ID | account number',
  transactions.CUSTOMER_ID as CUSTOMER_ID comment='Customer ID | client ID',
  transactions.BOOKING_DATE as BOOKING_DATE comment='Booking date | posting date | transaction date',
  transactions.VALUE_DATE as VALUE_DATE comment='Value date | settlement date',
  transactions.AMOUNT as AMOUNT comment='Transaction amount | payment amount | txn amount',
  transactions.COUNTERPARTY_ACCOUNT as COUNTERPARTY_ACCOUNT 
    WITH SYNONYMS = ('counterparty', 'beneficiary', 'recipient', 'payee', 'beneficiary account', 'recipient account')
    comment='Counterparty account number. Use for filtering by beneficiary or recipient',
  transactions.DESCRIPTION as DESCRIPTION 
    WITH SYNONYMS = ('description', 'narrative', 'details', 'payment description', 'transaction details', 'memo')
    comment='Transaction description. Supports text search for finding transactions by description content',
  
  transactions.CUSTOMER_TOTAL_TRANSACTIONS as CUSTOMER_TOTAL_TRANSACTIONS comment='Customer total transactions | transaction count | total txns',
  transactions.AVG_TRANSACTION_AMOUNT as AVG_TRANSACTION_AMOUNT comment='Average transaction amount | typical amount | mean amount',
  transactions.MEDIAN_TRANSACTION_AMOUNT as MEDIAN_TRANSACTION_AMOUNT comment='Median transaction amount | middle amount',
  transactions.AVG_DAILY_TRANSACTION_COUNT as AVG_DAILY_TRANSACTION_COUNT comment='Average daily transactions | daily txn count',
  
  transactions.AMOUNT_ANOMALY_SCORE as AMOUNT_ANOMALY_SCORE comment='Amount anomaly score | unusual amount score',
  transactions.TIMING_ANOMALY_SCORE as TIMING_ANOMALY_SCORE comment='Timing anomaly score | unusual time score',
  
  transactions.IS_LARGE_TRANSACTION as IS_LARGE_TRANSACTION comment='Large transaction flag | high value | big amount',
  transactions.IS_UNUSUAL_WEEKEND_TRANSACTION as IS_UNUSUAL_WEEKEND_TRANSACTION comment='Weekend transaction flag | Saturday Sunday | unusual timing',
  transactions.IS_OFF_HOURS_TRANSACTION as IS_OFF_HOURS_TRANSACTION comment='Off hours transaction | late night | unusual time',
  transactions.IS_DELAYED_SETTLEMENT as IS_DELAYED_SETTLEMENT comment='Delayed settlement flag | late settlement',
  transactions.IS_BACKDATED_SETTLEMENT as IS_BACKDATED_SETTLEMENT comment='Backdated settlement | backdated value date',
  
  transactions.COMPOSITE_ANOMALY_SCORE as COMPOSITE_ANOMALY_SCORE comment='Composite anomaly score | overall risk score | total anomaly score',
  transactions.REQUIRES_IMMEDIATE_REVIEW as REQUIRES_IMMEDIATE_REVIEW comment='Requires immediate review | urgent review | critical flag',
  transactions.REQUIRES_ENHANCED_MONITORING as REQUIRES_ENHANCED_MONITORING comment='Requires enhanced monitoring | heightened scrutiny | watch list',
  
  transactions.TRANSACTIONS_LAST_24H as TRANSACTIONS_LAST_24H comment='Transactions last 24 hours | daily transactions',
  transactions.TRANSACTIONS_LAST_7D as TRANSACTIONS_LAST_7D comment='Transactions last 7 days | weekly transactions',
  
  transactions.TRANSACTION_HOUR as TRANSACTION_HOUR comment='Transaction hour | hour of day | time',
  transactions.SETTLEMENT_DAYS as SETTLEMENT_DAYS comment='Settlement days | days to settle',
  
  transactions.ANOMALY_ANALYSIS_TIMESTAMP as ANOMALY_ANALYSIS_TIMESTAMP comment='Analysis timestamp | analyzed at | detection time'
)

dimensions (
  transactions.OVERALL_ANOMALY_CLASSIFICATION AS OVERALL_ANOMALY_CLASSIFICATION 
    WITH SYNONYMS = ('risk level', 'anomaly level', 'classification', 'severity', 'risk category')
    COMMENT = 'Overall anomaly classification (CRITICAL, HIGH, MODERATE, LOW). Primary dimension for risk-based filtering and grouping'
    SAMPLE_VALUES ('CRITICAL', 'HIGH', 'MODERATE', 'LOW')
    IS_ENUM,
  transactions.CURRENCY AS CURRENCY 
    WITH SYNONYMS = ('currency', 'currency code', 'payment currency', 'transaction currency')
    COMMENT = 'Transaction currency. Use for currency-based analysis and reporting',
  
  transactions.AMOUNT_ANOMALY_LEVEL AS AMOUNT_ANOMALY_LEVEL 
    WITH SYNONYMS = ('amount risk', 'amount anomaly', 'unusual amount')
    COMMENT = 'Amount anomaly level. Use for filtering transactions with unusual amounts'
    SAMPLE_VALUES ('CRITICAL', 'HIGH', 'MODERATE', 'LOW')
    IS_ENUM,
  transactions.TIMING_ANOMALY_LEVEL AS TIMING_ANOMALY_LEVEL 
    WITH SYNONYMS = ('timing risk', 'timing anomaly', 'unusual timing')
    COMMENT = 'Timing anomaly level. Use for filtering transactions with unusual timing patterns'
    SAMPLE_VALUES ('CRITICAL', 'HIGH', 'MODERATE', 'LOW')
    IS_ENUM,
  transactions.VELOCITY_ANOMALY_LEVEL AS VELOCITY_ANOMALY_LEVEL 
    WITH SYNONYMS = ('velocity risk', 'frequency anomaly', 'velocity anomaly')
    COMMENT = 'Velocity anomaly level. Use for filtering transactions with unusual frequency patterns',
  transactions.TRANSACTION_DAYOFWEEK AS TRANSACTION_DAYOFWEEK 
    COMMENT = 'Day of week for temporal analysis',
  transactions.CRITICAL_AND_HIGH LABELS = (FILTER) AS transactions.OVERALL_ANOMALY_CLASSIFICATION IN ('CRITICAL', 'HIGH')
    COMMENT = 'Filter to CRITICAL and HIGH anomaly transactions',
  transactions.REQUIRES_IMMEDIATE LABELS = (FILTER) AS transactions.REQUIRES_IMMEDIATE_REVIEW = TRUE
    COMMENT = 'Filter to transactions requiring immediate compliance review'
)

metrics (
  transactions.TRANSACTION_COUNT AS COUNT(transactions.TRANSACTION_ID)
    WITH SYNONYMS = ('number of transactions', 'transaction count', 'how many transactions', 'total transactions', 'txn count')
    COMMENT = 'Count of transactions. Use for "how many transactions" queries',
  transactions.TOTAL_AMOUNT AS SUM(transactions.AMOUNT)
    WITH SYNONYMS = ('total amount', 'sum of amounts', 'total value', 'aggregate amount')
    COMMENT = 'Sum of all transaction amounts. Use for total transaction value calculations',
  transactions.AVG_AMOUNT AS AVG(transactions.AMOUNT)
    WITH SYNONYMS = ('average amount', 'mean amount', 'typical amount', 'avg transaction size')
    COMMENT = 'Average transaction amount. Use for typical transaction size analysis',
  transactions.HIGH_RISK_COUNT AS COUNT(CASE WHEN transactions.OVERALL_ANOMALY_CLASSIFICATION IN ('HIGH', 'CRITICAL') THEN 1 END)
    WITH SYNONYMS = ('high risk transactions', 'critical transactions', 'number of high risk', 'high risk count')
    COMMENT = 'Count of HIGH and CRITICAL anomaly transactions. Use for risk concentration analysis'
)

COMMENT = 'AML transaction monitoring - anomaly detection (amount, timing, velocity), compliance flags, and investigation prioritization.'

AI_SQL_GENERATION 'For date-based queries, filter on BOOKING_DATE. For "recent" or "last N days" queries, use BOOKING_DATE >= DATEADD(DAY, -N, CURRENT_DATE()). When counting anomalies, use OVERALL_ANOMALY_CLASSIFICATION as the grouping dimension. For "high risk" transactions, filter OVERALL_ANOMALY_CLASSIFICATION IN (''CRITICAL'', ''HIGH'').'

AI_QUESTION_CATEGORIZATION 'Reject questions about customer demographics, addresses, or account tier -- those belong to the Customer 360 semantic view. If asked about sanctions list matches or PEP status, explain that customer-level screening is in Customer 360; this view covers transaction-level anomalies.'

AI_VERIFIED_QUERIES (
  critical_anomalies_30d AS (
    QUESTION 'Show me CRITICAL and HIGH anomaly transactions in the last 30 days'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT transactions.TRANSACTION_ID, transactions.CUSTOMER_ID, transactions.BOOKING_DATE, transactions.AMOUNT, transactions.CURRENCY, transactions.OVERALL_ANOMALY_CLASSIFICATION, transactions.COMPOSITE_ANOMALY_SCORE FROM __transactions AS transactions WHERE transactions.OVERALL_ANOMALY_CLASSIFICATION IN (''CRITICAL_ANOMALY'', ''HIGH_ANOMALY'') AND transactions.BOOKING_DATE >= DATEADD(DAY, -30, CURRENT_DATE()) ORDER BY transactions.COMPOSITE_ANOMALY_SCORE DESC NULLS LAST'
  ),
  customers_multiple_anomalies AS (
    QUESTION 'Which customers have multiple HIGH or CRITICAL anomaly transactions?'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT transactions.CUSTOMER_ID, COUNT(transactions.TRANSACTION_ID) AS ANOMALY_COUNT, SUM(transactions.AMOUNT) AS TOTAL_ANOMALOUS_AMOUNT FROM __transactions AS transactions WHERE transactions.OVERALL_ANOMALY_CLASSIFICATION IN (''CRITICAL_ANOMALY'', ''HIGH_ANOMALY'') GROUP BY transactions.CUSTOMER_ID HAVING COUNT(transactions.TRANSACTION_ID) > 1 ORDER BY ANOMALY_COUNT DESC'
  ),
  large_offhours AS (
    QUESTION 'Show me large transactions with off-hours timing'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT transactions.TRANSACTION_ID, transactions.CUSTOMER_ID, transactions.BOOKING_DATE, transactions.AMOUNT, transactions.CURRENCY, transactions.TRANSACTION_HOUR, transactions.COMPOSITE_ANOMALY_SCORE FROM __transactions AS transactions WHERE transactions.IS_LARGE_TRANSACTION = TRUE AND transactions.IS_OFF_HOURS_TRANSACTION = TRUE ORDER BY transactions.AMOUNT DESC NULLS LAST'
  ),
  anomaly_count_by_classification AS (
    QUESTION 'How many transactions per anomaly classification?'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT transactions.OVERALL_ANOMALY_CLASSIFICATION, COUNT(transactions.TRANSACTION_ID) AS TRANSACTION_COUNT, SUM(transactions.AMOUNT) AS TOTAL_AMOUNT FROM __transactions AS transactions GROUP BY transactions.OVERALL_ANOMALY_CLASSIFICATION ORDER BY TRANSACTION_COUNT DESC'
  ),
  top_anomaly_customers AS (
    QUESTION 'Top 10 customers by composite anomaly score'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT transactions.CUSTOMER_ID, MAX(transactions.COMPOSITE_ANOMALY_SCORE) AS MAX_ANOMALY_SCORE, COUNT(transactions.TRANSACTION_ID) AS TRANSACTION_COUNT FROM __transactions AS transactions GROUP BY transactions.CUSTOMER_ID ORDER BY MAX_ANOMALY_SCORE DESC NULLS LAST LIMIT 10'
  )
);
{% endif %}
