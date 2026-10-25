{# semantic_views='false' is only used for the first deploy into an empty database:
   semantic views validate against their base dynamic tables, which must be initialized first #}
{% if semantic_views == 'true' %}
-- ============================================================
-- LOANS AND MORTGAGES SEMANTIC VIEWS
-- Cortex Analyst semantic views (moved from post_deploy.sql).
-- ============================================================

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LOAS_SV_PORTFOLIO_CURRENT
tables (
  portfolio_data AS {{ db }}.{{ rep_agg }}.LOAR_AGG_DT_PORTFOLIO_SUMMARY
    COMMENT = 'Loan portfolio summary by product, country, and status'
)

facts (
  portfolio_data.AS_OF_DATE as AS_OF_DATE 
    WITH SYNONYMS = ('reporting date', 'calculation date', 'snapshot date', 'data date', 'as of date')
    comment='Date of portfolio snapshot | reporting date',
  
  portfolio_data.LOAN_COUNT as LOAN_COUNT
    WITH SYNONYMS = ('count', 'number of loans', 'loan volume', 'applications count', 'total loans')
    comment='Number of loan applications in this category',
  
  portfolio_data.TOTAL_REQUESTED_AMOUNT as TOTAL_REQUESTED_AMOUNT
    WITH SYNONYMS = ('total amount', 'total exposure', 'total loan amount', 'exposure', 'portfolio value')
    comment='Total requested loan amount in CHF',
  
  portfolio_data.AVG_REQUESTED_AMOUNT as AVG_REQUESTED_AMOUNT
    WITH SYNONYMS = ('average amount', 'average loan', 'mean amount', 'average loan size')
    comment='Average requested loan amount in CHF',
  
  portfolio_data.MIN_REQUESTED_AMOUNT as MIN_REQUESTED_AMOUNT
    WITH SYNONYMS = ('minimum amount', 'smallest loan', 'min loan')
    comment='Minimum requested loan amount in CHF',
  
  portfolio_data.MAX_REQUESTED_AMOUNT as MAX_REQUESTED_AMOUNT
    WITH SYNONYMS = ('maximum amount', 'largest loan', 'max loan', 'biggest loan')
    comment='Maximum requested loan amount in CHF',
  
  portfolio_data.AVG_TERM_MONTHS as AVG_TERM_MONTHS
    WITH SYNONYMS = ('average term', 'average loan term', 'mean term', 'term length', 'average maturity')
    comment='Average loan term in months',
  
  portfolio_data.CALCULATION_TIMESTAMP as CALCULATION_TIMESTAMP
    WITH SYNONYMS = ('calculation time', 'data timestamp', 'calculated at', 'last updated')
    comment='When this portfolio aggregation was last calculated'
)

dimensions (
  portfolio_data.PRODUCT_TYPE AS PRODUCT_TYPE
    WITH SYNONYMS = ('product', 'loan type', 'loan product', 'product category')
    COMMENT = 'Loan product type'
    SAMPLE_VALUES ('MORTGAGE')
    IS_ENUM,
  portfolio_data.COUNTRY AS COUNTRY
    WITH SYNONYMS = ('country code', 'jurisdiction', 'regime', 'market')
    COMMENT = 'Country: CHE (Switzerland), GBR (UK), DEU (Germany), PRT (Portugal)'
    SAMPLE_VALUES ('CHE', 'GBR', 'DEU', 'PRT')
    IS_ENUM,
  portfolio_data.APPLICATION_STATUS AS APPLICATION_STATUS
    WITH SYNONYMS = ('status', 'loan status', 'application state', 'approval status')
    COMMENT = 'Application status'
    SAMPLE_VALUES ('APPROVED', 'DECLINED', 'UNDER_REVIEW', 'DISBURSED', 'SUBMITTED', 'DRAFT')
    IS_ENUM
)

metrics (
  portfolio_data.TOTAL_LOANS AS SUM(portfolio_data.LOAN_COUNT)
    WITH SYNONYMS = ('total applications', 'total loan count', 'application volume')
    COMMENT = 'Total number of loan applications',
  portfolio_data.TOTAL_EXPOSURE AS SUM(portfolio_data.TOTAL_REQUESTED_AMOUNT)
    WITH SYNONYMS = ('total loan exposure', 'aggregate exposure', 'total lending')
    COMMENT = 'Total requested loan amount in CHF',
  portfolio_data.AVG_LOAN AS AVG(portfolio_data.AVG_REQUESTED_AMOUNT)
    WITH SYNONYMS = ('average loan amount', 'mean loan size')
    COMMENT = 'Average requested loan amount in CHF'
)

COMMENT = 'Loan portfolio summary - application volumes, exposure by product and country, approval status tracking.'

AI_SQL_GENERATION 'For \"total exposure\" or \"total loan amount\", use SUM(TOTAL_REQUESTED_AMOUNT). For approval rates, calculate counts where APPLICATION_STATUS = ''APPROVED'' divided by total count. For \"current\" portfolio, use MAX(AS_OF_DATE). Country codes: CHE=Switzerland, GBR=United Kingdom, DEU=Germany, PRT=Portugal.'

AI_VERIFIED_QUERIES (
  total_loan_exposure AS (
    QUESTION 'What is our total loan exposure?'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT SUM(portfolio_data.TOTAL_REQUESTED_AMOUNT) AS TOTAL_EXPOSURE, SUM(portfolio_data.LOAN_COUNT) AS TOTAL_LOANS, AVG(portfolio_data.AVG_REQUESTED_AMOUNT) AS AVG_LOAN_AMOUNT FROM __portfolio_data AS portfolio_data WHERE portfolio_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __portfolio_data AS sub)'
  ),
  portfolio_by_country AS (
    QUESTION 'Show me loan portfolio breakdown by country'
    VERIFIED_AT 1726185600
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT portfolio_data.COUNTRY, SUM(portfolio_data.LOAN_COUNT) AS TOTAL_LOANS, SUM(portfolio_data.TOTAL_REQUESTED_AMOUNT) AS TOTAL_EXPOSURE, AVG(portfolio_data.AVG_REQUESTED_AMOUNT) AS AVG_LOAN FROM __portfolio_data AS portfolio_data WHERE portfolio_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __portfolio_data AS sub) GROUP BY portfolio_data.COUNTRY ORDER BY TOTAL_EXPOSURE DESC NULLS LAST'
  ),
  approved_vs_declined AS (
    QUESTION 'How many approved vs declined applications?'
    VERIFIED_AT 1726185600
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT portfolio_data.APPLICATION_STATUS, SUM(portfolio_data.LOAN_COUNT) AS LOAN_COUNT, SUM(portfolio_data.TOTAL_REQUESTED_AMOUNT) AS TOTAL_AMOUNT FROM __portfolio_data AS portfolio_data WHERE portfolio_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __portfolio_data AS sub) AND portfolio_data.APPLICATION_STATUS IN (''APPROVED'', ''DECLINED'') GROUP BY portfolio_data.APPLICATION_STATUS ORDER BY LOAN_COUNT DESC'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LOAS_SV_LTV_DISTRIBUTION
tables (
  ltv_data AS {{ db }}.{{ rep_agg }}.LOAR_AGG_DT_LTV_DISTRIBUTION
    COMMENT = 'LTV distribution by buckets for risk concentration analysis'
)

facts (
  ltv_data.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('reporting date', 'date', 'snapshot date')
    comment='Date of LTV distribution snapshot',
  
  ltv_data.LTV_BUCKET as LTV_BUCKET
    WITH SYNONYMS = ('LTV band', 'LTV range', 'LTV category', 'risk bucket')
    comment='LTV bucket: 0-50%, 50-60%, 60-70%, 70-80%, 80-90%, >90%',
  
  ltv_data.LTV_BUCKET_SORT_ORDER as LTV_BUCKET_SORT_ORDER
    WITH SYNONYMS = ('sort order', 'bucket order')
    comment='Sort order for LTV buckets (1-6)',
  
  ltv_data.LOAN_COUNT as LOAN_COUNT
    WITH SYNONYMS = ('count', 'number of loans', 'loan volume', 'loans in bucket')
    comment='Number of loans in this LTV bucket',
  
  ltv_data.TOTAL_LOAN_AMOUNT as TOTAL_LOAN_AMOUNT
    WITH SYNONYMS = ('total amount', 'total exposure', 'loan amount', 'exposure in bucket')
    comment='Total loan amount in this LTV bucket (CHF)',
  
  ltv_data.AVG_LTV_PCT as AVG_LTV_PCT
    WITH SYNONYMS = ('average LTV', 'mean LTV', 'LTV percentage', 'average loan to value')
    comment='Average LTV percentage in this bucket',
  
  ltv_data.TOTAL_COLLATERAL_VALUE as TOTAL_COLLATERAL_VALUE
    WITH SYNONYMS = ('collateral value', 'property value', 'total collateral', 'asset value')
    comment='Total collateral value in this bucket (CHF)',
  
  ltv_data.PCT_OF_TOTAL_LOANS as PCT_OF_TOTAL_LOANS
    WITH SYNONYMS = ('percentage of loans', 'loan concentration', 'portfolio percentage', 'share of portfolio')
    comment='Percentage of total loan count in this bucket',
  
  ltv_data.CALCULATION_TIMESTAMP as CALCULATION_TIMESTAMP
    WITH SYNONYMS = ('calculation time', 'calculated at', 'last updated')
    comment='When this LTV distribution was last calculated'
)

AI_VERIFIED_QUERIES (
  ltv_distribution AS (
    QUESTION 'Show the LTV distribution of the loan book'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT ltv_data.LTV_BUCKET, ltv_data.LOAN_COUNT, ltv_data.TOTAL_LOAN_AMOUNT, ltv_data.AVG_LTV_PCT, ltv_data.PCT_OF_TOTAL_LOANS FROM __ltv_data AS ltv_data WHERE ltv_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __ltv_data AS sub) ORDER BY ltv_data.LTV_BUCKET_SORT_ORDER'
  ),
  high_ltv_loans AS (
    QUESTION 'How many loans have an LTV above 80%?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT COALESCE(SUM(ltv_data.LOAN_COUNT), 0) AS HIGH_LTV_LOANS, COALESCE(SUM(ltv_data.TOTAL_LOAN_AMOUNT), 0) AS HIGH_LTV_AMOUNT FROM __ltv_data AS ltv_data WHERE ltv_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __ltv_data AS sub) AND ltv_data.LTV_BUCKET IN (''80-90%'', ''>90%'')'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LOAS_SV_APPLICATION_FUNNEL
tables (
  funnel_data AS {{ db }}.{{ rep_agg }}.LOAR_AGG_DT_APPLICATION_FUNNEL
    COMMENT = 'Application volumes and conversion rates by product, country, and channel'
)

facts (
  funnel_data.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('reporting date', 'date', 'snapshot date')
    comment='Date of application funnel snapshot',
  
  funnel_data.PRODUCT_TYPE as PRODUCT_TYPE
    WITH SYNONYMS = ('product', 'loan type', 'loan product')
    comment='Product type: MORTGAGE, PERSONAL_LOAN',
  
  funnel_data.COUNTRY as COUNTRY
    WITH SYNONYMS = ('country code', 'jurisdiction', 'market')
    comment='Country code: CHE, GBR, DEU, PRT',
  
  funnel_data.CHANNEL as CHANNEL
    WITH SYNONYMS = ('application channel', 'source channel', 'origination channel', 'source')
    comment='Application channel: EMAIL, PORTAL, BRANCH, BROKER',
  
  funnel_data.TOTAL_APPLICATIONS as TOTAL_APPLICATIONS
    WITH SYNONYMS = ('total apps', 'application count', 'total volume', 'applications received')
    comment='Total number of applications received',
  
  funnel_data.APPROVED_COUNT as APPROVED_COUNT
    WITH SYNONYMS = ('approved', 'approved apps', 'approvals', 'approved applications')
    comment='Number of approved applications',
  
  funnel_data.DECLINED_COUNT as DECLINED_COUNT
    WITH SYNONYMS = ('declined', 'declined apps', 'declines', 'rejected', 'declined applications')
    comment='Number of declined applications',
  
  funnel_data.UNDER_REVIEW_COUNT as UNDER_REVIEW_COUNT
    WITH SYNONYMS = ('under review', 'in review', 'pending', 'pending review', 'in progress')
    comment='Number of applications currently under review',
  
  funnel_data.APPROVAL_RATE_PCT as APPROVAL_RATE_PCT
    WITH SYNONYMS = ('approval rate', 'approval percentage', 'conversion rate', 'success rate')
    comment='Approval rate percentage (approved / total applications)',
  
  funnel_data.DECLINE_RATE_PCT as DECLINE_RATE_PCT
    WITH SYNONYMS = ('decline rate', 'decline percentage', 'rejection rate', 'failure rate')
    comment='Decline rate percentage (declined / total applications)',
  
  funnel_data.AVG_REQUESTED_AMOUNT as AVG_REQUESTED_AMOUNT
    WITH SYNONYMS = ('average amount', 'average loan', 'mean amount', 'average loan size')
    comment='Average requested loan amount in CHF',
  
  funnel_data.CALCULATION_TIMESTAMP as CALCULATION_TIMESTAMP
    WITH SYNONYMS = ('calculation time', 'calculated at', 'last updated')
    comment='When this funnel data was last calculated'
)

AI_VERIFIED_QUERIES (
  approval_rate_by_country AS (
    QUESTION 'What is the loan approval rate by country?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT funnel_data.COUNTRY, SUM(funnel_data.TOTAL_APPLICATIONS) AS TOTAL_APPLICATIONS, SUM(funnel_data.APPROVED_COUNT) AS APPROVED, SUM(funnel_data.DECLINED_COUNT) AS DECLINED, ROUND(SUM(funnel_data.APPROVED_COUNT) * 100.0 / NULLIF(SUM(funnel_data.TOTAL_APPLICATIONS), 0), 2) AS APPROVAL_RATE_PCT FROM __funnel_data AS funnel_data WHERE funnel_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __funnel_data AS sub) GROUP BY funnel_data.COUNTRY ORDER BY TOTAL_APPLICATIONS DESC'
  ),
  applications_by_channel AS (
    QUESTION 'How many loan applications came in per channel?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT funnel_data.CHANNEL, SUM(funnel_data.TOTAL_APPLICATIONS) AS TOTAL_APPLICATIONS, SUM(funnel_data.UNDER_REVIEW_COUNT) AS UNDER_REVIEW, AVG(funnel_data.AVG_REQUESTED_AMOUNT) AS AVG_REQUESTED_AMOUNT FROM __funnel_data AS funnel_data WHERE funnel_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __funnel_data AS sub) GROUP BY funnel_data.CHANNEL ORDER BY TOTAL_APPLICATIONS DESC'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LOAS_SV_AFFORDABILITY_ANALYSIS
tables (
  affordability_data AS {{ db }}.{{ rep_agg }}.LOAR_AGG_DT_AFFORDABILITY_SUMMARY
    COMMENT = 'Affordability assessment pass/fail rates by country'
)

facts (
  affordability_data.AS_OF_DATE as AS_OF_DATE
    WITH SYNONYMS = ('reporting date', 'date', 'snapshot date')
    comment='Date of affordability analysis snapshot',
  
  affordability_data.COUNTRY as COUNTRY
    WITH SYNONYMS = ('country code', 'jurisdiction', 'market', 'regime')
    comment='Country code: CHE, GBR, DEU',
  
  affordability_data.AFFORDABILITY_RESULT as AFFORDABILITY_RESULT
    WITH SYNONYMS = ('result', 'outcome', 'assessment result', 'pass or fail', 'affordability outcome')
    comment='Affordability assessment result: PASS, FAIL',
  
  affordability_data.ASSESSMENT_COUNT as ASSESSMENT_COUNT
    WITH SYNONYMS = ('count', 'number of assessments', 'assessments', 'volume')
    comment='Number of affordability assessments performed',
  
  affordability_data.AVG_DTI_RATIO_PCT as AVG_DTI_RATIO_PCT
    WITH SYNONYMS = ('DTI', 'debt to income', 'DTI ratio', 'average DTI', 'debt to income ratio')
    comment='Average Debt-to-Income ratio percentage',
  
  affordability_data.AVG_DSTI_RATIO_PCT as AVG_DSTI_RATIO_PCT
    WITH SYNONYMS = ('DSTI', 'debt service to income', 'DSTI ratio', 'average DSTI')
    comment='Average Debt Service-to-Income ratio percentage',
  
  affordability_data.AVG_GROSS_INCOME as AVG_GROSS_INCOME
    WITH SYNONYMS = ('average income', 'mean income', 'gross income', 'monthly income')
    comment='Average gross monthly income in CHF',
  
  affordability_data.AVG_DEBT_OBLIGATIONS as AVG_DEBT_OBLIGATIONS
    WITH SYNONYMS = ('average debt', 'monthly debt', 'debt obligations', 'existing debts')
    comment='Average monthly debt obligations in CHF',
  
  affordability_data.PASS_RATE_PCT as PASS_RATE_PCT
    WITH SYNONYMS = ('pass rate', 'pass percentage', 'success rate', 'affordability pass rate')
    comment='Percentage of affordability assessments that passed',
  
  affordability_data.CALCULATION_TIMESTAMP as CALCULATION_TIMESTAMP
    WITH SYNONYMS = ('calculation time', 'calculated at', 'last updated')
    comment='When this affordability analysis was last calculated'
)

AI_VERIFIED_QUERIES (
  affordability_pass_rate AS (
    QUESTION 'What is the affordability pass rate by country?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT affordability_data.COUNTRY, affordability_data.AFFORDABILITY_RESULT, affordability_data.ASSESSMENT_COUNT, affordability_data.PASS_RATE_PCT FROM __affordability_data AS affordability_data WHERE affordability_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __affordability_data AS sub) ORDER BY affordability_data.COUNTRY, affordability_data.AFFORDABILITY_RESULT'
  ),
  dti_dsti_by_country AS (
    QUESTION 'What are the average DTI and DSTI ratios by country?'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT affordability_data.COUNTRY, SUM(affordability_data.ASSESSMENT_COUNT) AS ASSESSMENTS, AVG(affordability_data.AVG_DTI_RATIO_PCT) AS AVG_DTI_RATIO_PCT, AVG(affordability_data.AVG_DSTI_RATIO_PCT) AS AVG_DSTI_RATIO_PCT, AVG(affordability_data.AVG_GROSS_INCOME) AS AVG_GROSS_INCOME FROM __affordability_data AS affordability_data WHERE affordability_data.AS_OF_DATE = (SELECT MAX(sub.AS_OF_DATE) FROM __affordability_data AS sub) GROUP BY affordability_data.COUNTRY ORDER BY affordability_data.COUNTRY'
  )
);

DEFINE SEMANTIC VIEW {{ db }}.{{ rep_agg }}.LOAS_SV_COMPLIANCE_SCREENING
tables (
  compliance_data AS {{ db }}.{{ rep_agg }}.LOAR_AGG_VW_COMPLIANCE_SCREENING
    COMMENT = 'Loan applications linked to sanctions and PEP screening status'
)

facts (
  compliance_data.APPLICATION_ID as APPLICATION_ID
    WITH SYNONYMS = ('app id', 'application number', 'loan id', 'reference number')
    comment='Unique application identifier',
  
  compliance_data.CUSTOMER_ID as CUSTOMER_ID
    WITH SYNONYMS = ('customer number', 'client id', 'customer reference')
    comment='Customer identifier (FK to Customer 360)',
  
  
  compliance_data.COUNTRY as COUNTRY
    WITH SYNONYMS = ('country code', 'jurisdiction', 'market')
    comment='Country code for application',
  
  compliance_data.REQUESTED_AMOUNT as REQUESTED_AMOUNT
    WITH SYNONYMS = ('loan amount', 'requested loan', 'amount', 'loan value')
    comment='Requested loan amount in CHF',
  
  compliance_data.APPLICATION_STATUS as APPLICATION_STATUS
    WITH SYNONYMS = ('status', 'application state', 'loan status')
    comment='Application status: APPROVED, DECLINED, UNDER_REVIEW',
  
  compliance_data.REQUIRES_SANCTIONS_REVIEW as REQUIRES_SANCTIONS_REVIEW
    WITH SYNONYMS = ('sanctions flag', 'sanctions review', 'sanctions hit', 'sanctions match')
    comment='Boolean flag: TRUE if customer requires sanctions compliance review',
  
  compliance_data.REQUIRES_EXPOSED_PERSON_REVIEW as REQUIRES_EXPOSED_PERSON_REVIEW
    WITH SYNONYMS = ('PEP flag', 'PEP review', 'PEP hit', 'politically exposed person', 'exposed person')
    comment='Boolean flag: TRUE if customer requires PEP compliance review',
  
  compliance_data.OVERALL_RISK_RATING as OVERALL_RISK_RATING
    WITH SYNONYMS = ('risk rating', 'risk level', 'risk classification', 'customer risk')
    comment='Overall risk rating: CRITICAL, HIGH, MEDIUM, LOW, NO_RISK',
  
  compliance_data.VULNERABLE_CUSTOMER_FLAG as VULNERABLE_CUSTOMER_FLAG
    WITH SYNONYMS = ('vulnerable flag', 'vulnerability', 'vulnerable customer', 'vulnerable')
    comment='Boolean flag: TRUE if customer has vulnerability characteristics',
  
  compliance_data.SANCTIONS_MATCH_NAME as SANCTIONS_MATCH_NAME
    WITH SYNONYMS = ('sanctions name', 'sanctioned entity', 'sanctions match')
    comment='Name of sanctions entity matched (if any)',
  
  compliance_data.SANCTIONS_AUTHORITY as SANCTIONS_AUTHORITY
    WITH SYNONYMS = ('sanctioning authority', 'sanctions issuer', 'authority')
    comment='Sanctioning authority: OFAC, EU, UN',
  
  compliance_data.SANCTIONS_RISK_SCORE as SANCTIONS_RISK_SCORE
    WITH SYNONYMS = ('sanctions score', 'risk score')
    comment='Sanctions risk score (0-100)',
  
  compliance_data.EXPOSED_PERSON_EXACT_MATCH_NAME as EXPOSED_PERSON_EXACT_MATCH_NAME
    WITH SYNONYMS = ('PEP name', 'PEP match', 'politically exposed person name')
    comment='Name of PEP matched (if exact match)',
  
  compliance_data.EXPOSED_PERSON_EXACT_CATEGORY as EXPOSED_PERSON_EXACT_CATEGORY
    WITH SYNONYMS = ('PEP category', 'PEP type', 'exposed person category')
    comment='PEP category: DOMESTIC, FOREIGN, INTERNATIONAL',
  
  compliance_data.EXPOSED_PERSON_EXACT_RISK_LEVEL as EXPOSED_PERSON_EXACT_RISK_LEVEL
    WITH SYNONYMS = ('PEP risk level', 'PEP risk', 'exposed person risk')
    comment='PEP risk level: CRITICAL, HIGH, MEDIUM, LOW',
  
  compliance_data.EXPOSED_PERSON_MATCH_TYPE as EXPOSED_PERSON_MATCH_TYPE
    WITH SYNONYMS = ('PEP match type', 'match type')
    comment='PEP match type: EXACT_MATCH, FUZZY_MATCH, NO_MATCH',
  
  compliance_data.APPLICATION_DATE_TIME as APPLICATION_DATE_TIME
    WITH SYNONYMS = ('application date', 'submitted date', 'application time', 'submission date')
    comment='Date and time when application was submitted',
  
  compliance_data.COMPLIANCE_HOLD_FLAG as COMPLIANCE_HOLD_FLAG
    WITH SYNONYMS = ('hold flag', 'compliance hold', 'blocked', 'on hold')
    comment='Boolean flag: TRUE if application is on compliance hold',
  
  compliance_data.COMPLIANCE_STATUS as COMPLIANCE_STATUS
    WITH SYNONYMS = ('compliance state', 'review status', 'compliance outcome')
    comment='Compliance status: SANCTIONS_REVIEW, PEP_REVIEW, HIGH_RISK_REVIEW, CLEAR'
)

AI_VERIFIED_QUERIES (
  applications_by_compliance_status AS (
    QUESTION 'How many loan applications are in each compliance status?'
    VERIFIED_AT 1792886400
    ONBOARDING_QUESTION TRUE
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT compliance_data.COMPLIANCE_STATUS, COUNT(compliance_data.APPLICATION_ID) AS APPLICATIONS, SUM(compliance_data.REQUESTED_AMOUNT) AS REQUESTED_AMOUNT, COUNT_IF(compliance_data.COMPLIANCE_HOLD_FLAG) AS ON_HOLD FROM __compliance_data AS compliance_data GROUP BY compliance_data.COMPLIANCE_STATUS ORDER BY APPLICATIONS DESC'
  ),
  application_screening_list AS (
    QUESTION 'List loan applications with their risk rating and screening flags'
    VERIFIED_AT 1792886400
    VERIFIED_BY '(STEWARD = mdaeppen)'
    SQL 'SELECT compliance_data.APPLICATION_ID, compliance_data.CUSTOMER_ID, compliance_data.COUNTRY, compliance_data.REQUESTED_AMOUNT, compliance_data.APPLICATION_STATUS, compliance_data.OVERALL_RISK_RATING, compliance_data.REQUIRES_SANCTIONS_REVIEW, compliance_data.REQUIRES_EXPOSED_PERSON_REVIEW, compliance_data.COMPLIANCE_STATUS FROM __compliance_data AS compliance_data ORDER BY compliance_data.APPLICATION_DATE_TIME DESC'
  )
);
{% endif %}
