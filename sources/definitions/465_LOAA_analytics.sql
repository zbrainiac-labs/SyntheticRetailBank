/*
 * 465_LOAA_analytics.sql
 * Loan analytics: application processing and scoring
 */
DEFINE TABLE {{ db }}.{{ loa_agg }}.LOAA_AGG_TB_APPLICATIONS (
    APPLICATION_ID VARCHAR(50) PRIMARY KEY COMMENT 'Unique application identifier (APP_*)',
    CUSTOMER_ID VARCHAR(30) COMMENT 'FK to {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360',
    APPLICATION_DATE_TIME TIMESTAMP_NTZ NOT NULL COMMENT 'Application submission timestamp',
    CHANNEL VARCHAR(50) COMMENT 'Application channel: EMAIL, PORTAL, BRANCH, BROKER',
    COUNTRY VARCHAR(50) COMMENT 'Country code (CHE, GBR, DEU) for regulatory parameterization',
    PRODUCT_ID VARCHAR(50) COMMENT 'FK to {{ db }}.{{ loa_raw }}.LOAI_REF_TB_PRODUCT_CATALOGUE',
    REQUESTED_AMOUNT NUMBER(18,2) COMMENT 'Requested loan amount in local currency',
    REQUESTED_TERM_MONTHS INT COMMENT 'Requested loan term in months',
    REQUESTED_CURRENCY VARCHAR(3) COMMENT 'Currency code (CHF, GBP, EUR)',
    PURPOSE VARCHAR(100) COMMENT 'Loan purpose: PURCHASE, REFINANCE, HOME_IMPROVEMENT',
    ADVICE_VS_EXECUTION_ONLY VARCHAR(50) COMMENT 'Advised sale vs execution-only (MCD requirement)',
    BROKER_ID VARCHAR(50) COMMENT 'Broker ID if application via intermediary',
    STATUS VARCHAR(50) COMMENT 'Application status: SUBMITTED, APPROVED, DECLINED, WITHDRAWN, DISBURSED',
    CREATED_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    UPDATED_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Last update timestamp'
)
COMMENT = 'Loan applications master table. Populated from DocAI-extracted email data. Contains all mortgage and unsecured loan applications with status tracking and regulatory compliance attributes.';

DEFINE TABLE {{ db }}.{{ loa_agg }}.LOAA_AGG_TB_COLLATERAL (
    COLLATERAL_ID VARCHAR(50) PRIMARY KEY COMMENT 'Unique collateral identifier (COLL_*)',
    PROPERTY_IDENTIFIER VARCHAR(100) COMMENT 'External property reference (land registry, tax ID)',
    PROPERTY_ADDRESS VARCHAR(500) COMMENT 'Full property address including postal code',
    PROPERTY_TYPE VARCHAR(50) COMMENT 'Property type: SINGLE_FAMILY, MULTI_FAMILY, APARTMENT, CONDO, TOWNHOUSE',
    OCCUPANCY_TYPE VARCHAR(50) COMMENT 'Occupancy type: OWNER_OCCUPIED, BUY_TO_LET, INVESTMENT',
    CONSTRUCTION_YEAR INT COMMENT 'Year property was built',
    ENERGY_PERFORMANCE_RATING VARCHAR(10) COMMENT 'EPC rating (A-G for UK, similar for EU)',
    VALUATION_VALUE NUMBER(18,2) NOT NULL COMMENT 'Current property valuation in local currency',
    VALUATION_DATE DATE NOT NULL COMMENT 'Date of valuation',
    VALUATION_METHOD VARCHAR(50) COMMENT 'Valuation method: AVM, FULL_APPRAISAL, DRIVE_BY, DESKTOP',
    VALUATION_PROVIDER VARCHAR(200) COMMENT 'Valuation provider name (for audit trail)',
    CREATED_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
)
COMMENT = 'Property collateral master table. Contains valuation, property characteristics, and ESG attributes (energy ratings) for mortgage lending.';

DEFINE TABLE {{ db }}.{{ loa_agg }}.LOAA_AGG_TB_LOAN_COLLATERAL_LINK (
    LINK_ID VARCHAR(50) PRIMARY KEY COMMENT 'Unique link identifier (LINK_*)',
    ACCOUNT_ID VARCHAR(50) NOT NULL COMMENT 'FK to loan account (using APPLICATION_ID for showcase)',
    COLLATERAL_ID VARCHAR(50) NOT NULL COMMENT 'FK to {{ db }}.{{ loa_agg }}.LOAA_AGG_TB_COLLATERAL',
    EFFECTIVE_FROM_DATE DATE NOT NULL COMMENT 'Date when collateral link became effective',
    EFFECTIVE_TO_DATE DATE COMMENT 'Date when collateral link ended (NULL = active)',
    CHARGE_RANK VARCHAR(10) COMMENT 'Charge rank: 1ST, 2ND, 3RD (legal priority)',
    CHARGE_AMOUNT NUMBER(18,2) COMMENT 'Amount of charge secured against this collateral',
    COLLATERAL_ALLOCATION_PCT NUMBER(5,2) COMMENT 'Percentage of collateral allocated to this loan (for M:M scenarios)',
    REGISTRATION_STATUS VARCHAR(50) COMMENT 'Legal registration status: REGISTERED, PENDING, CANCELLED',
    CREATED_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp'
)
COMMENT = 'Loan-to-collateral M:M mapping table. Supports cross-collateralization and second charges. Showcase uses simple 1:1 relationships (all 1ST charge, 100% allocation).';

DEFINE TABLE {{ db }}.{{ loa_agg }}.LOAA_AGG_TB_AFFORDABILITY_ASSESSMENTS (
    AFFORDABILITY_ID VARCHAR(50) PRIMARY KEY COMMENT 'Unique affordability assessment ID (AFF_*)',
    APPLICATION_ID VARCHAR(50) NOT NULL COMMENT 'FK to {{ db }}.{{ loa_agg }}.LOAA_AGG_TB_APPLICATIONS',
    CUSTOMER_ID VARCHAR(30) COMMENT 'FK to {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360',
    GROSS_INCOME_MONTHLY NUMBER(18,2) COMMENT 'Gross monthly income before taxes',
    NET_INCOME_MONTHLY NUMBER(18,2) COMMENT 'Net monthly income after taxes',
    FIXED_INCOME_MONTHLY NUMBER(18,2) COMMENT 'Fixed income component (salary, pension)',
    VARIABLE_INCOME_MONTHLY NUMBER(18,2) COMMENT 'Variable income component (bonus, commission, rental)',
    RENTAL_INCOME_MONTHLY NUMBER(18,2) COMMENT 'Rental income from other properties (if applicable)',
    LIVING_EXPENSES_MONTHLY NUMBER(18,2) COMMENT 'Committed living expenses',
    TOTAL_DEBT_OBLIGATIONS_MONTHLY NUMBER(18,2) COMMENT 'Total existing monthly debt obligations',
    DTI_RATIO NUMBER(5,3) COMMENT 'Debt-to-Income ratio: Existing debts / Gross income',
    DSTI_RATIO NUMBER(5,3) COMMENT 'Debt Service-to-Income ratio: (Loan payment + Existing debts) / Gross income',
    AFFORDABILITY_RESULT VARCHAR(20) NOT NULL COMMENT 'Affordability result: PASS, FAIL, MARGINAL',
    AFFORDABILITY_REASON_CODES VARCHAR(500) COMMENT 'Comma-separated reason codes for FAIL results',
    INTEREST_RATE_STRESS_APPLIED_PCT NUMBER(5,4) COMMENT 'Stress test interest rate applied (e.g., 5% for CHE, 7% for UK)',
    STRESSED_PAYMENT_MONTHLY NUMBER(18,2) COMMENT 'Monthly payment calculated at stressed interest rate',
    MODEL_EXPLAINABILITY_TOP_FACTORS VARCHAR(1000) COMMENT 'Top 3-5 factors contributing to decision (SHAP/LIME concept)',
    CALCULATION_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When affordability was calculated'
)
COMMENT = 'Affordability assessment snapshots for loan applications. Contains DTI/DSTI calculations with country-specific thresholds (CHE: 33%, UK: 45%, DE: 40%) and stress testing per regulatory requirements.';
