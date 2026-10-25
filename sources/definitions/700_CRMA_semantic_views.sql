{# semantic_views='false' is only used for the first deploy into an empty database:
   semantic views validate against their base dynamic tables, which must be initialized first #}
{% if semantic_views == 'true' %}
-- ============================================================
-- CUSTOMER AND EMPLOYEE SEMANTIC VIEWS
-- Cortex Analyst semantic views (moved from post_deploy.sql).
-- ============================================================

DEFINE SEMANTIC VIEW {{ db }}.{{ crm_agg }}.CRMA_SV_CUSTOMER_360
tables (
  customers AS {{ db }}.{{ crm_agg }}.CRMA_AGG_VW_CUSTOMER_360_ENRICHED
    PRIMARY KEY (CUSTOMER_ID)
    COMMENT = 'Customer 360 enriched view with PII from vault (masking policy controlled)',
  advisors AS {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ADVISORS
    PRIMARY KEY (EMPLOYEE_ID)
    COMMENT = 'Advisor performance and client portfolio metrics'
)

relationships (
  customer_to_advisor AS
    customers (CURRENT_ADVISOR_EMPLOYEE_ID) REFERENCES advisors
)

facts (
  customers.CUSTOMER_ID as CUSTOMER_ID comment='Customer identifier | client ID | CIF | customer number | account holder ID | customer code | customer reference',
  customers.FIRST_NAME as FIRST_NAME comment='First name | given name | forename | personal name | Christian name',
  customers.FAMILY_NAME as FAMILY_NAME comment='Family name | last name | surname | family surname | last surname',
  customers.FULL_NAME as FULL_NAME comment='Full name | complete name | customer name | client name | full customer name | PRIMARY identifier for displaying clients in reports and rankings | use for showing "top clients" or "client lists" | human-readable client identifier',
  customers.DATE_OF_BIRTH as DATE_OF_BIRTH comment='Date of birth | DOB | birth date | birthday | birthdate',
  customers.ONBOARDING_DATE as ONBOARDING_DATE comment='Onboarding date | customer since | join date | account opening date | relationship start | joined on',
  customers.REPORTING_CURRENCY as REPORTING_CURRENCY comment='Reporting currency | base currency | home currency | preferred currency | default currency',
  
  customers.HAS_ANOMALY as HAS_ANOMALY comment='Anomaly flag | suspicious activity | fraud indicator | unusual pattern | red flag | suspicious flag',
  
  customers.EMPLOYER as EMPLOYER comment='Employer | company | organization | workplace | employer name | company name',
  customers.POSITION as POSITION comment='Job position | job title | role | occupation | professional title | job role',
  
  customers.EMAIL as EMAIL comment='Email address | email | contact email | email ID | electronic mail | e-mail',
  customers.PHONE as PHONE comment='Phone number | telephone | mobile | contact number | phone | cell phone | mobile number',
  customers.PREFERRED_CONTACT_METHOD as PREFERRED_CONTACT_METHOD comment='Preferred contact method | contact preference | communication preference | contact channel | preferred channel',
  
  customers.STREET_ADDRESS as STREET_ADDRESS comment='Street address | residential address | mailing address | physical address | home address | address line',
  customers.ZIPCODE as ZIPCODE comment='Postal code | ZIP code | postcode | area code | zip | post code',
  customers.ADDRESS_EFFECTIVE_DATE as ADDRESS_EFFECTIVE_DATE comment='Address effective date | address since | moved on | relocation date | address start date',
  
  customers.STATUS_EFFECTIVE_DATE as STATUS_EFFECTIVE_DATE comment='Status effective date | status since | status change date | status start date',
  
  customers.TOTAL_ACCOUNTS as TOTAL_ACCOUNTS 
    WITH SYNONYMS = ('count', 'number', 'total', 'quantity', 'how many', 'number of accounts')
    comment='Count of accounts. Use for aggregation and "how many accounts" queries',
  customers.ACCOUNT_TYPES as ACCOUNT_TYPES comment='Account types | product types | account categories | product mix | account products',
  customers.CURRENCIES as CURRENCIES comment='Currencies used | currency portfolio | multi-currency | currency mix | currencies held',
  customers.CHECKING_ACCOUNTS as CHECKING_ACCOUNTS comment='Checking accounts | current accounts | transaction accounts | checking products | transactional accounts',
  customers.SAVINGS_ACCOUNTS as SAVINGS_ACCOUNTS comment='Savings accounts | deposit accounts | savings products | term deposits | savings plans',
  customers.BUSINESS_ACCOUNTS as BUSINESS_ACCOUNTS comment='Business accounts | commercial accounts | corporate accounts | business banking | SME accounts',
  customers.INVESTMENT_ACCOUNTS as INVESTMENT_ACCOUNTS comment='Investment accounts | brokerage accounts | trading accounts | securities accounts | investment products',
  
  customers.TOTAL_BALANCE as TOTAL_BALANCE 
    WITH SYNONYMS = ('AUM', 'assets under management', 'managed assets', 'portfolio value', 'balance', 'wealth', 'client balance')
    comment='Per-customer total balance. Use for individual client AUM or portfolio value lookups',
  customers.CHECKING_BALANCE as CHECKING_BALANCE comment='Checking balance | current account balance | transaction account balance | liquid funds',
  customers.SAVINGS_BALANCE as SAVINGS_BALANCE comment='Savings balance | deposit balance | savings account balance | deposit funds',
  customers.BUSINESS_BALANCE as BUSINESS_BALANCE comment='Business balance | commercial balance | corporate balance | SME balance',
  customers.INVESTMENT_BALANCE as INVESTMENT_BALANCE comment='Investment balance | brokerage balance | securities balance | portfolio balance | invested assets',
  customers.MAX_ACCOUNT_BALANCE as MAX_ACCOUNT_BALANCE comment='Maximum balance | highest balance | peak balance | max portfolio value',
  customers.MIN_ACCOUNT_BALANCE as MIN_ACCOUNT_BALANCE comment='Minimum balance | lowest balance | min portfolio value',
  customers.AVG_ACCOUNT_BALANCE as AVG_ACCOUNT_BALANCE comment='Average balance | mean balance | typical balance',
  customers.BALANCE_AS_OF_DATE as BALANCE_AS_OF_DATE comment='Balance as of date | balance date | valuation date | snapshot date',
  
  customers.TOTAL_TRANSACTIONS as TOTAL_TRANSACTIONS comment='Total transactions | transaction count | number of transactions | transaction volume | use for ranking by activity level or transaction frequency | use when specifically asked about most active clients or transaction volume | NOT the default for "top clients" (use TOTAL_BALANCE instead)',
  customers.TOTAL_TRANSACTIONS_ALL_TIME as TOTAL_TRANSACTIONS_ALL_TIME comment='Total transactions all time | lifetime transactions | historical transaction count',
  customers.LAST_TRANSACTION_DATE as LAST_TRANSACTION_DATE comment='Last transaction date | most recent transaction | latest transaction | last activity date',
  customers.DAYS_SINCE_LAST_TRANSACTION as DAYS_SINCE_LAST_TRANSACTION comment='Days since last transaction | days inactive | inactivity days | dormancy days',
  customers.DEBIT_TRANSACTIONS as DEBIT_TRANSACTIONS comment='Debit transactions | withdrawals | outgoing transactions | debit count',
  customers.CREDIT_TRANSACTIONS as CREDIT_TRANSACTIONS comment='Credit transactions | deposits | incoming transactions | credit count',
  customers.AVG_MONTHLY_TRANSACTIONS as AVG_MONTHLY_TRANSACTIONS comment='Average monthly transactions | typical monthly activity | avg monthly volume',
  customers.IS_DORMANT_TRANSACTIONALLY as IS_DORMANT_TRANSACTIONALLY comment='Dormant flag | inactive customer | dormant account | no activity | inactive flag',
  customers.IS_HIGHLY_ACTIVE as IS_HIGHLY_ACTIVE comment='Highly active flag | active customer | frequent user | high activity',
  
  customers.EXPOSED_PERSON_MATCH_TYPE as EXPOSED_PERSON_MATCH_TYPE comment='PEP match type | politically exposed person match | PEP screening result | PEP status | exposed person status',
  customers.EXPOSED_PERSON_MATCH_ACCURACY_PERCENT as EXPOSED_PERSON_MATCH_ACCURACY_PERCENT comment='PEP match accuracy | match confidence | screening accuracy | match percentage | PEP match score',
  customers.EXPOSED_PERSON_EXACT_MATCH_NAME as EXPOSED_PERSON_EXACT_MATCH_NAME comment='PEP exact match name | PEP name match | exact PEP hit',
  customers.EXPOSED_PERSON_FUZZY_MATCH_NAME as EXPOSED_PERSON_FUZZY_MATCH_NAME comment='PEP fuzzy match name | similar PEP name | probable PEP match',
  customers.EXPOSED_PERSON_EXACT_CATEGORY as EXPOSED_PERSON_EXACT_CATEGORY comment='PEP category | PEP type | exposed person category | PEP classification',
  customers.EXPOSED_PERSON_EXACT_RISK_LEVEL as EXPOSED_PERSON_EXACT_RISK_LEVEL comment='PEP risk level | exposed person risk | PEP risk category',
  
  customers.SANCTIONS_MATCH_TYPE as SANCTIONS_MATCH_TYPE comment='Sanctions match type | watchlist match | sanctions screening result | sanctions status | watchlist status',
  customers.SANCTIONS_MATCH_ACCURACY_PERCENT as SANCTIONS_MATCH_ACCURACY_PERCENT comment='Sanctions match accuracy | match confidence | screening accuracy | match percentage | sanctions match score',
  customers.SANCTIONS_EXACT_MATCH_NAME as SANCTIONS_EXACT_MATCH_NAME comment='Sanctions exact match name | watchlist name match | exact sanctions hit',
  customers.SANCTIONS_FUZZY_MATCH_NAME as SANCTIONS_FUZZY_MATCH_NAME comment='Sanctions fuzzy match name | similar sanctions name | probable sanctions match',
  customers.SANCTIONS_EXACT_MATCH_COUNTRY as SANCTIONS_EXACT_MATCH_COUNTRY comment='Sanctions country | sanctioned country | watchlist country',
  
  customers.OVERALL_EXPOSED_PERSON_RISK as OVERALL_EXPOSED_PERSON_RISK comment='Overall PEP risk | politically exposed person risk | PEP risk level | PEP risk rating | exposed person risk score',
  customers.OVERALL_SANCTIONS_RISK as OVERALL_SANCTIONS_RISK comment='Overall sanctions risk | watchlist risk | sanctions risk level | sanctions risk rating | sanctions risk score',
  customers.OVERALL_RISK_RATING as OVERALL_RISK_RATING comment='Overall risk rating | comprehensive risk | total risk | risk assessment | combined risk | aggregate risk',
  customers.OVERALL_RISK_SCORE as OVERALL_RISK_SCORE comment='Overall risk score | risk number | numerical risk | risk points | risk value | total risk score',
  customers.REQUIRES_EXPOSED_PERSON_REVIEW as REQUIRES_EXPOSED_PERSON_REVIEW comment='Requires PEP review | needs PEP check | PEP review flag | exposed person review required | PEP review needed',
  customers.REQUIRES_SANCTIONS_REVIEW as REQUIRES_SANCTIONS_REVIEW comment='Requires sanctions review | needs sanctions check | sanctions review flag | watchlist review required | sanctions review needed',
  customers.HIGH_RISK_CUSTOMER as HIGH_RISK_CUSTOMER comment='High risk customer | high risk flag | risky customer | elevated risk | risk alert | high-risk indicator',
  
  customers.CURRENT_ADVISOR_EMPLOYEE_ID as CURRENT_ADVISOR_EMPLOYEE_ID comment='Current advisor | assigned advisor | relationship manager | advisor ID | employee ID | advisor assignment',
  customers.ADVISOR_ASSIGNMENT_START_DATE as ADVISOR_ASSIGNMENT_START_DATE comment='Advisor assignment date | assignment start | relationship start | advisor since | assigned since',
  
  customers.LAST_UPDATED as LAST_UPDATED comment='Last updated timestamp | refresh timestamp | data freshness | update time | last refresh'
)

dimensions (
  customers.COUNTRY AS COUNTRY 
    WITH SYNONYMS = ('country', 'nation', 'geography', 'location', 'region')
    COMMENT = 'Country code or name. Primary dimension for geographic grouping. Use with "by country" or "per country" queries'
    SAMPLE_VALUES ('CHE', 'GBR', 'DEU')
    IS_ENUM,
  customers.ACCOUNT_TIER AS ACCOUNT_TIER 
    WITH SYNONYMS = ('tier', 'service level', 'customer segment', 'membership level')
    COMMENT = 'Account tier (PLATINUM, GOLD, SILVER, BRONZE). Primary dimension for customer segmentation'
    SAMPLE_VALUES ('STANDARD', 'SILVER', 'GOLD', 'PLATINUM', 'PREMIUM')
    IS_ENUM,
  customers.CURRENT_STATUS AS CURRENT_STATUS 
    WITH SYNONYMS = ('status', 'active', 'inactive', 'customer status')
    COMMENT = 'Customer status (ACTIVE, INACTIVE, CLOSED). Use for filtering active customers'
    SAMPLE_VALUES ('ACTIVE', 'INACTIVE', 'CLOSED')
    IS_ENUM,
  customers.RISK_CLASSIFICATION AS RISK_CLASSIFICATION 
    WITH SYNONYMS = ('risk level', 'risk category', 'risk rating')
    COMMENT = 'Risk classification (LOW_RISK, MEDIUM_RISK, HIGH_RISK). Use for risk-based grouping'
    SAMPLE_VALUES ('LOW_RISK', 'MEDIUM_RISK', 'HIGH_RISK')
    IS_ENUM,
  
  customers.CITY AS CITY COMMENT = 'City for geographic analysis',
  customers.STATE AS STATE COMMENT = 'State/province for regional grouping',
  customers.EMPLOYMENT_TYPE AS EMPLOYMENT_TYPE COMMENT = 'Employment type for demographic analysis',
  customers.INCOME_RANGE AS INCOME_RANGE COMMENT = 'Income range for customer segmentation',
  customers.CREDIT_SCORE_BAND AS CREDIT_SCORE_BAND COMMENT = 'Credit score band for risk grouping',
  customers.ACTIVE_CUSTOMERS LABELS = (FILTER) AS customers.CURRENT_STATUS = 'ACTIVE'
    COMMENT = 'Filter to active customers only',
  customers.HIGH_RISK_ONLY LABELS = (FILTER) AS customers.OVERALL_RISK_RATING IN ('HIGH', 'CRITICAL')
    COMMENT = 'Filter to HIGH and CRITICAL risk customers',
  customers.REQUIRES_REVIEW LABELS = (FILTER) AS (customers.REQUIRES_SANCTIONS_REVIEW = TRUE OR customers.REQUIRES_EXPOSED_PERSON_REVIEW = TRUE)
    COMMENT = 'Filter to customers requiring compliance review'
)

metrics (
  customers.CUSTOMER_COUNT AS COUNT(customers.CUSTOMER_ID)
    WITH SYNONYMS = ('number of customers', 'customer count', 'how many customers', 'total customers')
    COMMENT = 'Count of customers. DEFAULT metric for counting.
               EXAMPLES: "How many customers per country?" → COUNT(CUSTOMER_ID) GROUP BY COUNTRY
                        "Top 10 countries by customer count" → COUNT(CUSTOMER_ID) GROUP BY COUNTRY LIMIT 10
                        "How many PLATINUM customers?" → COUNT WHERE ACCOUNT_TIER = PLATINUM',
  customers.TOTAL_BALANCE_SUM AS SUM(customers.TOTAL_BALANCE)
    WITH SYNONYMS = ('total AUM', 'total assets', 'total wealth', 'aggregate balance', 'sum of balances')
    COMMENT = 'Sum of all customer balances. Use for wealth/AUM aggregation.
               EXAMPLES: "Top 10 clients by total AUM" → ORDER BY TOTAL_BALANCE DESC LIMIT 10
                        "Total AUM by country" → SUM(TOTAL_BALANCE) GROUP BY COUNTRY
                        "Which tier has highest total assets?" → SUM(TOTAL_BALANCE) GROUP BY ACCOUNT_TIER',
  customers.AVG_BALANCE AS AVG(customers.TOTAL_BALANCE)
    WITH SYNONYMS = ('average balance', 'mean balance', 'average AUM')
    COMMENT = 'Average customer balance. Use for typical customer value.
               EXAMPLES: "Average balance by country" → AVG(TOTAL_BALANCE) GROUP BY COUNTRY
                        "Which country has highest average wealth?" → AVG(TOTAL_BALANCE) GROUP BY COUNTRY'
)

COMMENT = 'Customer 360 intelligence - profiles, risk, compliance (PEP/sanctions), accounts, lifecycle, and advisor assignments.'

AI_SQL_GENERATION 'When ranking customers (top clients, highest value), use TOTAL_BALANCE unless the user explicitly asks about transactions. Always include CUSTOMER_ID in SELECT. For dormancy queries, use IS_DORMANT_TRANSACTIONALLY = TRUE. For high-risk queries, use OVERALL_RISK_RATING IN (''HIGH'', ''CRITICAL'') unless a specific field is named.'

AI_QUESTION_CATEGORIZATION 'Reject questions about individual transaction details (amounts, dates, counterparties) -- those belong to the Compliance Monitoring semantic view. If the user asks about a specific customer by name and the result is masked, explain that PII is protected by masking policies.'

AI_VERIFIED_QUERIES (
  top_customers_by_aum AS (
    QUESTION 'Show me top 10 customers by AUM'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT customers.CUSTOMER_ID, customers.FULL_NAME, customers.TOTAL_BALANCE, customers.COUNTRY, customers.ACCOUNT_TIER FROM __customers AS customers ORDER BY customers.TOTAL_BALANCE DESC NULLS LAST LIMIT 10'
  ),
  customer_count_by_country AS (
    QUESTION 'How many customers per country?'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT customers.COUNTRY, COUNT(customers.CUSTOMER_ID) AS CUSTOMER_COUNT FROM __customers AS customers GROUP BY customers.COUNTRY ORDER BY CUSTOMER_COUNT DESC'
  ),
  platinum_customers_switzerland AS (
    QUESTION 'Show me all PLATINUM customers in Switzerland'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT customers.CUSTOMER_ID, customers.FULL_NAME, customers.TOTAL_BALANCE, customers.OVERALL_RISK_RATING FROM __customers AS customers WHERE customers.ACCOUNT_TIER = ''PLATINUM'' AND customers.COUNTRY = ''Switzerland'' ORDER BY customers.TOTAL_BALANCE DESC NULLS LAST'
  ),
  high_risk_customers AS (
    QUESTION 'Which customers have CRITICAL or HIGH risk rating?'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT customers.CUSTOMER_ID, customers.FULL_NAME, customers.OVERALL_RISK_RATING, customers.OVERALL_RISK_SCORE, customers.REQUIRES_SANCTIONS_REVIEW, customers.REQUIRES_EXPOSED_PERSON_REVIEW FROM __customers AS customers WHERE customers.OVERALL_RISK_RATING IN (''CRITICAL'', ''HIGH'') ORDER BY customers.OVERALL_RISK_SCORE DESC NULLS LAST'
  ),
  pep_review_required AS (
    QUESTION 'Which customers require PEP review?'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT customers.CUSTOMER_ID, customers.FULL_NAME, customers.EXPOSED_PERSON_MATCH_TYPE, customers.EXPOSED_PERSON_EXACT_CATEGORY, customers.EXPOSED_PERSON_EXACT_RISK_LEVEL FROM __customers AS customers WHERE customers.REQUIRES_EXPOSED_PERSON_REVIEW = TRUE ORDER BY customers.EXPOSED_PERSON_EXACT_RISK_LEVEL DESC NULLS LAST'
  ),
  dormant_customers AS (
    QUESTION 'Show me dormant customers with no recent transactions'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT customers.CUSTOMER_ID, customers.FULL_NAME, customers.DAYS_SINCE_LAST_TRANSACTION, customers.LAST_TRANSACTION_DATE, customers.ACCOUNT_TIER, customers.TOTAL_BALANCE FROM __customers AS customers WHERE customers.IS_DORMANT_TRANSACTIONALLY = TRUE ORDER BY customers.DAYS_SINCE_LAST_TRANSACTION DESC NULLS LAST'
  ),
  customer_count_by_tier AS (
    QUESTION 'How many customers per account tier?'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT customers.ACCOUNT_TIER, COUNT(customers.CUSTOMER_ID) AS CUSTOMER_COUNT, SUM(customers.TOTAL_BALANCE) AS TOTAL_AUM FROM __customers AS customers GROUP BY customers.ACCOUNT_TIER ORDER BY TOTAL_AUM DESC NULLS LAST'
  ),
  sanctions_review_required AS (
    QUESTION 'Which customers require sanctions review?'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT customers.CUSTOMER_ID, customers.FULL_NAME, customers.SANCTIONS_MATCH_TYPE, customers.SANCTIONS_EXACT_MATCH_NAME, customers.SANCTIONS_EXACT_MATCH_COUNTRY, customers.OVERALL_SANCTIONS_RISK FROM __customers AS customers WHERE customers.REQUIRES_SANCTIONS_REVIEW = TRUE ORDER BY customers.OVERALL_SANCTIONS_RISK DESC NULLS LAST'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ crm_agg }}.EMPA_SV_EMPLOYEE_ADVISOR
tables (
  advisors AS {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED
    PRIMARY KEY (EMPLOYEE_ID)
    COMMENT = 'Advisor performance with PII from employee vault (masking policy controlled)',
  customers AS {{ db }}.{{ crm_agg }}.CRMA_AGG_VW_CUSTOMER_360_ENRICHED
    PRIMARY KEY (CUSTOMER_ID)
    COMMENT = 'Customer 360 enriched with PII from vault'
)

relationships (
  advisor_to_customers AS
    customers (CURRENT_ADVISOR_EMPLOYEE_ID) REFERENCES advisors
)

facts (
  advisors.EMPLOYEE_ID as EMPLOYEE_ID comment='Employee ID | advisor ID | employee number | staff ID | advisor code | employee identifier',
  advisors.ADVISOR_NAME as ADVISOR_NAME comment='Advisor full name | advisor name | employee name | complete name',
  advisors.TEAM_LEADER_NAME as TEAM_LEADER_NAME comment='Team leader name | manager name | supervisor name',
  advisors.HIRE_DATE as HIRE_DATE comment='Hire date | start date | employment start | join date | onboarding date | employment date',
  advisors.TENURE_DAYS as TENURE_DAYS comment='Tenure in days | days employed | employment duration | service length',
  advisors.TEAM_LEADER_ID as TEAM_LEADER_ID comment='Team leader ID | manager ID | supervisor ID | reports to ID',
  advisors.LANGUAGES_SPOKEN as LANGUAGES_SPOKEN comment='Languages spoken | language skills | multilingual | languages | language proficiency',
  advisors.TOTAL_CLIENTS as TOTAL_CLIENTS comment='Total clients | number of clients | client count | assigned clients | customer portfolio',
  advisors.TOTAL_PORTFOLIO_VALUE as TOTAL_PORTFOLIO_VALUE
    WITH SYNONYMS = ('AUM', 'assets under management', 'managed assets', 'portfolio value', 'total AUM')
    comment='Total portfolio value managed by advisor. Primary metric for advisor performance ranking',
  advisors.AVG_CLIENT_BALANCE as AVG_CLIENT_BALANCE comment='Average client balance | mean client balance | typical client value | avg portfolio size',
  advisors.MAX_CLIENT_BALANCE as MAX_CLIENT_BALANCE comment='Maximum client balance | highest client balance | largest client value',
  advisors.HIGH_RISK_CLIENTS as HIGH_RISK_CLIENTS comment='High risk clients | risky customers | high risk portfolio | elevated risk clients',
  advisors.HIGH_RISK_PERCENTAGE as HIGH_RISK_PERCENTAGE comment='High risk percentage | risk ratio | high risk proportion',
  advisors.TOTAL_CLIENT_ACCOUNTS as TOTAL_CLIENT_ACCOUNTS comment='Total client accounts | account count | managed accounts | accounts under management',
  advisors.AVG_ACCOUNTS_PER_CLIENT as AVG_ACCOUNTS_PER_CLIENT comment='Average accounts per client | accounts per customer | avg account count',
  advisors.TOTAL_TRANSACTIONS as TOTAL_TRANSACTIONS comment='Total transactions | transaction count | all transactions | transaction volume',
  advisors.AVG_TRANSACTIONS_PER_CLIENT as AVG_TRANSACTIONS_PER_CLIENT comment='Average transactions per client | txn per customer | activity rate',
  advisors.CRITICAL_RISK_CLIENTS as CRITICAL_RISK_CLIENTS comment='Critical risk clients | highest risk clients | critical risk count',
  advisors.CAPACITY_UTILIZATION_PCT as CAPACITY_UTILIZATION_PCT comment='Capacity utilization | workload percentage | capacity used | utilization rate'
)

dimensions (
  advisors.COUNTRY AS COUNTRY 
    WITH SYNONYMS = ('country', 'nation', 'location', 'geography')
    COMMENT = 'Country where advisor is located. Use for geographic analysis and regional grouping',
  advisors.REGION AS REGION 
    WITH SYNONYMS = ('region', 'territory', 'area', 'geographic region')
    COMMENT = 'Geographic region for regional performance analysis',
  advisors.EMPLOYMENT_STATUS AS EMPLOYMENT_STATUS 
    WITH SYNONYMS = ('status', 'active', 'employment status', 'work status')
    COMMENT = 'Employment status (ACTIVE, INACTIVE). Use for filtering active advisors',
  advisors.PERFORMANCE_RATING AS PERFORMANCE_RATING 
    WITH SYNONYMS = ('rating', 'performance', 'performance rating', 'advisor rating')
    COMMENT = 'Performance rating. Use for identifying top performers',
  advisors.WORKLOAD_STATUS AS WORKLOAD_STATUS 
    WITH SYNONYMS = ('workload', 'capacity status', 'load status')
    COMMENT = 'Workload status (e.g. OVERLOADED, BALANCED). Use for capacity planning',
  advisors.HIGH_RISK_CLIENTS_CLASSIFICATION AS HIGH_RISK_CLIENTS_CLASSIFICATION 
    COMMENT = 'Risk classification bucket for risk-based advisor analysis'
)

metrics (
  advisors.ADVISOR_COUNT AS COUNT(advisors.EMPLOYEE_ID)
    WITH SYNONYMS = ('number of advisors', 'advisor count', 'how many advisors', 'total advisors', 'headcount')
    COMMENT = 'Count of advisors.
               EXAMPLES: "How many advisors by region?" → COUNT(EMPLOYEE_ID) GROUP BY REGION',
  advisors.TOTAL_PORTFOLIO_SUM AS SUM(advisors.TOTAL_PORTFOLIO_VALUE)
    WITH SYNONYMS = ('total advisor AUM', 'total managed assets', 'sum of advisor AUM', 'aggregate advisor portfolio')
    COMMENT = 'Sum of all portfolio value managed by advisors. DEFAULT for advisor rankings.
               EXAMPLES: "Show me top 10 advisors by AUM" → ORDER BY TOTAL_PORTFOLIO_VALUE DESC LIMIT 10',
  advisors.AVG_PORTFOLIO AS AVG(advisors.TOTAL_PORTFOLIO_VALUE)
    WITH SYNONYMS = ('average AUM', 'mean AUM', 'typical AUM', 'avg assets per advisor')
    COMMENT = 'Average portfolio value per advisor. Use for productivity benchmarking.',
  advisors.TOTAL_CLIENTS_SUM AS SUM(advisors.TOTAL_CLIENTS)
    WITH SYNONYMS = ('total clients', 'sum of clients', 'all clients', 'total customer count')
    COMMENT = 'Total number of clients managed by all advisors.',
  advisors.AVG_CAPACITY AS AVG(advisors.CAPACITY_UTILIZATION_PCT)
    WITH SYNONYMS = ('average utilization', 'mean capacity', 'typical utilization')
    COMMENT = 'Average capacity utilization. Use for workforce planning.'
)

AI_VERIFIED_QUERIES (
  top_advisors_by_portfolio AS (
    QUESTION 'Who are the top 10 advisors by portfolio value?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT advisors.EMPLOYEE_ID, advisors.ADVISOR_NAME, advisors.REGION, advisors.TOTAL_CLIENTS, advisors.TOTAL_PORTFOLIO_VALUE FROM __advisors AS advisors ORDER BY advisors.TOTAL_PORTFOLIO_VALUE DESC NULLS LAST LIMIT 10'
  ),
  advisors_by_region AS (
    QUESTION 'How many advisors and clients do we have per region?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT advisors.REGION, COUNT(advisors.EMPLOYEE_ID) AS ADVISOR_COUNT, SUM(advisors.TOTAL_CLIENTS) AS TOTAL_CLIENTS, SUM(advisors.TOTAL_PORTFOLIO_VALUE) AS TOTAL_PORTFOLIO_VALUE, AVG(advisors.CAPACITY_UTILIZATION_PCT) AS AVG_CAPACITY_UTILIZATION_PCT FROM __advisors AS advisors GROUP BY advisors.REGION ORDER BY TOTAL_PORTFOLIO_VALUE DESC NULLS LAST'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ crm_agg }}.EMPA_SV_HR_EMPLOYEE

TABLES (
  EMPLOYEES AS (
    SELECT
      e.EMPLOYEE_ID,
      pii.FULL_NAME,
      pii.FIRST_NAME,
      pii.FAMILY_NAME,
      pii.EMAIL,
      pii.PHONE,
      pii.DATE_OF_BIRTH,
      e.HIRE_DATE,
      DATEDIFF(DAY, e.HIRE_DATE, CURRENT_DATE()) AS TENURE_DAYS,
      ROUND(DATEDIFF(DAY, e.HIRE_DATE, CURRENT_DATE()) / 365.25, 1) AS TENURE_YEARS,
      e.EMPLOYMENT_STATUS,
      e.COUNTRY,
      e.OFFICE_LOCATION,
      e.REGION,
      e.POSITION_LEVEL,
      e.MANAGER_EMPLOYEE_ID,
      e.PERFORMANCE_RATING,
      e.LANGUAGES_SPOKEN,
      e.CERTIFICATIONS
    FROM {{ db }}.{{ crm_raw }}.EMPI_RAW_TB_EMPLOYEE e
    LEFT JOIN {{ db }}.{{ crm_agg }}.EMPI_AGG_DT_EMPLOYEE_PII pii
      ON e.EMPLOYEE_ID = pii.EMPLOYEE_ID
  )
  PRIMARY KEY (EMPLOYEE_ID)
  COMMENT = 'HR Employee master data with PII from vault. Covers all staff: advisors, team leaders, super team leaders.'
)

FACTS (
  EMPLOYEES.EMPLOYEE_ID AS EMPLOYEE_ID
    COMMENT = 'Employee identifier | staff ID | employee number | advisor ID | personnel number',
  EMPLOYEES.FULL_NAME AS FULL_NAME
    COMMENT = 'Full name | employee name | staff name | complete name',
  EMPLOYEES.FIRST_NAME AS FIRST_NAME
    COMMENT = 'First name | given name | forename',
  EMPLOYEES.FAMILY_NAME AS FAMILY_NAME
    COMMENT = 'Family name | last name | surname',
  EMPLOYEES.EMAIL AS EMAIL
    COMMENT = 'Email address | work email | contact email',
  EMPLOYEES.PHONE AS PHONE
    COMMENT = 'Phone number | telephone | mobile | contact number',
  EMPLOYEES.DATE_OF_BIRTH AS DATE_OF_BIRTH
    COMMENT = 'Date of birth | DOB | birthday',
  EMPLOYEES.HIRE_DATE AS HIRE_DATE
    COMMENT = 'Hire date | start date | employment start | join date | onboarding date',
  EMPLOYEES.TENURE_DAYS AS TENURE_DAYS
    COMMENT = 'Tenure in days | days employed | service length | employment duration',
  EMPLOYEES.TENURE_YEARS AS TENURE_YEARS
    COMMENT = 'Tenure in years | years employed | years of service',
  EMPLOYEES.MANAGER_EMPLOYEE_ID AS MANAGER_EMPLOYEE_ID
    COMMENT = 'Manager ID | reports to | supervisor ID | team leader ID | boss',
  EMPLOYEES.LANGUAGES_SPOKEN AS LANGUAGES_SPOKEN
    COMMENT = 'Languages spoken | language skills | multilingual | language proficiency',
  EMPLOYEES.CERTIFICATIONS AS CERTIFICATIONS
    COMMENT = 'Certifications | qualifications | credentials | professional certifications',
  EMPLOYEES.PERFORMANCE_RATING AS PERFORMANCE_RATING
    COMMENT = 'Performance rating value | rating score | performance score | appraisal score'
)

DIMENSIONS (
  EMPLOYEES.POSITION_LEVEL AS POSITION_LEVEL
    WITH SYNONYMS = ('role', 'job title', 'position', 'level', 'job level', 'rank')
    COMMENT = 'Position level: SUPER_TEAM_LEADER, TEAM_LEADER, CLIENT_ADVISOR. Primary hierarchy dimension'
    SAMPLE_VALUES ('SUPER_TEAM_LEADER', 'TEAM_LEADER', 'CLIENT_ADVISOR')
    IS_ENUM,
  EMPLOYEES.COUNTRY AS COUNTRY
    WITH SYNONYMS = ('country', 'nation', 'location', 'geography')
    COMMENT = 'Country where employee is based. Use for geographic headcount analysis',
  EMPLOYEES.OFFICE_LOCATION AS OFFICE_LOCATION
    WITH SYNONYMS = ('office', 'branch', 'site', 'workplace', 'location')
    COMMENT = 'Office location or branch. Use for office-level headcount and capacity planning',
  EMPLOYEES.REGION AS REGION
    WITH SYNONYMS = ('region', 'territory', 'area', 'geographic region')
    COMMENT = 'Geographic region for regional grouping',
  EMPLOYEES.EMPLOYMENT_STATUS AS EMPLOYMENT_STATUS
    WITH SYNONYMS = ('status', 'active', 'inactive', 'employment status', 'work status')
    COMMENT = 'Employment status (ACTIVE, INACTIVE). Use for filtering active staff'
    SAMPLE_VALUES ('ACTIVE', 'INACTIVE')
    IS_ENUM,
  EMPLOYEES.ACTIVE_STAFF LABELS = (FILTER) AS EMPLOYEES.EMPLOYMENT_STATUS = 'ACTIVE'
    COMMENT = 'Filter to active employees only'
)

METRICS (
  EMPLOYEES.EMPLOYEE_COUNT AS COUNT(EMPLOYEES.EMPLOYEE_ID)
    WITH SYNONYMS = ('headcount', 'staff count', 'how many employees', 'workforce size', 'number of employees', 'total employees', 'team size', 'number of staff')
    COMMENT = 'Count of employees. DEFAULT metric for headcount questions.',
  EMPLOYEES.AVG_TENURE AS AVG(EMPLOYEES.TENURE_YEARS)
    WITH SYNONYMS = ('average tenure', 'mean tenure', 'avg years of service', 'average experience')
    COMMENT = 'Average tenure in years across employees'
)

COMMENT = 'HR Employee Intelligence - workforce headcount, org structure, tenure, offices, regions, performance, and certifications.'

AI_SQL_GENERATION 'For headcount queries, use EMPLOYEE_COUNT metric. Filter EMPLOYMENT_STATUS = ''ACTIVE'' by default unless the user asks about all staff or inactive employees. For \"team leaders\", filter POSITION_LEVEL = ''TEAM_LEADER''. For managers, use POSITION_LEVEL IN (''TEAM_LEADER'', ''SUPER_TEAM_LEADER'').'

AI_QUESTION_CATEGORIZATION 'Reject questions about client portfolios, AUM, or transactions -- those belong to other semantic views. If asked about advisor-client relationships or advisor AUM, redirect to the Employee Advisor semantic view.'

AI_VERIFIED_QUERIES (
  headcount_total AS (
    QUESTION 'How many employees do we have?'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT COUNT(EMPLOYEES.EMPLOYEE_ID) AS EMPLOYEE_COUNT FROM __EMPLOYEES AS EMPLOYEES WHERE EMPLOYEES.EMPLOYMENT_STATUS = ''ACTIVE'''
  ),
  headcount_by_region AS (
    QUESTION 'Show me headcount by region'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT EMPLOYEES.REGION, COUNT(EMPLOYEES.EMPLOYEE_ID) AS EMPLOYEE_COUNT FROM __EMPLOYEES AS EMPLOYEES WHERE EMPLOYEES.EMPLOYMENT_STATUS = ''ACTIVE'' GROUP BY EMPLOYEES.REGION ORDER BY EMPLOYEE_COUNT DESC'
  ),
  avg_tenure_by_position AS (
    QUESTION 'Average tenure by position level'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT EMPLOYEES.POSITION_LEVEL, COUNT(EMPLOYEES.EMPLOYEE_ID) AS EMPLOYEE_COUNT, AVG(EMPLOYEES.TENURE_YEARS) AS AVG_TENURE_YEARS FROM __EMPLOYEES AS EMPLOYEES WHERE EMPLOYEES.EMPLOYMENT_STATUS = ''ACTIVE'' GROUP BY EMPLOYEES.POSITION_LEVEL ORDER BY AVG_TENURE_YEARS DESC NULLS LAST'
  )
);
{% endif %}
