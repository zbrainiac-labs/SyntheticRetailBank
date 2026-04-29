/*
 * 065_LOAI_ingestion.sql
 * Loan raw ingestion: applications, documents, stages, tasks
 */
DEFINE STAGE {{ db }}.{{ loa_raw }}.LOAI_RAW_ST_EMAIL_INBOUND
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Staging area for mortgage email files (.txt, .eml, .msg) for DocAI processing.';

DEFINE STAGE {{ db }}.{{ loa_raw }}.LOAI_RAW_ST_PDF_INBOUND
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Staging area for loan-related PDF documents for DocAI processing.';

DEFINE TABLE {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_SCHEMA_CONFIG (
    schema_json VARIANT COMMENT 'AI_EXTRACT schema definition: {"field_name": "type: description"}'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Configuration table storing the AI_EXTRACT schema for mortgage email processing. Defines 15 fields to extract using Snowflake Cortex AI. Seed data loaded via post_deploy.sql.';

DEFINE TABLE {{ db }}.{{ loa_raw }}.LOAI_REF_TB_PRODUCT_CATALOGUE (
    PRODUCT_ID VARCHAR(50) PRIMARY KEY,
    PRODUCT_NAME VARCHAR(200) NOT NULL,
    PRODUCT_TYPE VARCHAR(50) NOT NULL,
    COUNTRY VARCHAR(3) NOT NULL,
    IS_SECURED BOOLEAN NOT NULL,
    MIN_LOAN_AMOUNT NUMBER(18,2),
    MAX_LOAN_AMOUNT NUMBER(18,2),
    MIN_TERM_MONTHS INT,
    MAX_TERM_MONTHS INT,
    DEFAULT_INTEREST_RATE NUMBER(5,4),
    RATE_TYPE VARCHAR(20),
    MAX_LTV_PCT NUMBER(5,2),
    ELIGIBILITY_CRITERIA VARCHAR(1000),
    REGULATORY_CLASSIFICATION VARCHAR(100),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    PRODUCT_LAUNCH_DATE DATE,
    PRODUCT_DISCONTINUATION_DATE DATE,
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE
COMMENT = 'Loan product catalogue for retail mortgages and loans across EMEA markets.';

DEFINE TABLE {{ db }}.{{ loa_raw }}.LOAI_REF_TB_COUNTRY_REGIME_CONFIG (
    COUNTRY_CODE VARCHAR(3) PRIMARY KEY,
    COUNTRY_NAME VARCHAR(100) NOT NULL,
    CURRENCY_CODE VARCHAR(3) NOT NULL,
    MAX_LTV_OWNER_OCCUPIED NUMBER(5,2),
    MAX_LTV_BUY_TO_LET NUMBER(5,2),
    MIN_HARD_EQUITY_PCT NUMBER(5,2),
    AFFORDABILITY_IMPUTED_RATE NUMBER(5,4),
    AFFORDABILITY_DTI_THRESHOLD NUMBER(5,2),
    AFFORDABILITY_DSTI_THRESHOLD NUMBER(5,2),
    ANCILLARY_COSTS_PCT NUMBER(5,4),
    COOLING_OFF_PERIOD_DAYS INT,
    AMORTIZATION_REQUIRED_LTV NUMBER(5,2),
    AMORTIZATION_PERIOD_YEARS INT,
    REQUIRES_VALUATION_APPRAISAL BOOLEAN,
    ALLOWS_FOREIGN_CURRENCY_LOANS BOOLEAN,
    REGULATORY_BODY VARCHAR(200),
    CONSUMER_DUTY_APPLIES BOOLEAN DEFAULT FALSE,
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE
COMMENT = 'Country-specific regulatory parameters for mortgage lending (CH/UK/DE).';

DEFINE TABLE {{ db }}.{{ loa_raw }}.LOAI_REF_TB_APPLICATION_STATUS (
    STATUS_CODE VARCHAR(50) PRIMARY KEY,
    STATUS_NAME VARCHAR(100) NOT NULL,
    STATUS_CATEGORY VARCHAR(50),
    DESCRIPTION VARCHAR(500),
    IS_FINAL BOOLEAN DEFAULT FALSE,
    REQUIRES_ACTION BOOLEAN DEFAULT FALSE,
    DISPLAY_ORDER INT,
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE
COMMENT = 'Loan application status codes for workflow management.';

DEFINE TABLE {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT (
    FILE_NAME STRING COMMENT 'Source filename from stage',
    FILE_TIMESTAMP TIMESTAMP_NTZ COMMENT 'File last modified timestamp',
    EXTRACTED_DATA VARIANT COMMENT 'Raw AI_EXTRACT JSON output: {error: null, response: {...}}',
    EXTRACTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When AI_EXTRACT was performed'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Raw AI_EXTRACT output from mortgage emails. Contains unflattened JSON with all 15 extracted fields.';

DEFINE TABLE {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT_FLAT (
    FILE_NAME STRING COMMENT 'Source filename',
    FILE_TIMESTAMP TIMESTAMP_NTZ COMMENT 'File timestamp',
    EXTRACTION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Extraction timestamp',

    DOCUMENT_TYPE STRING COMMENT 'AI-classified document type',

    CUSTOMER_NAME STRING COMMENT 'Applicant name',
    EMPLOYMENT STRING COMMENT 'Job title',
    EMPLOYMENT_TENURE_YEARS INT COMMENT 'Years in current employment',
    MONTHLY_INCOME NUMBER(18,2) COMMENT 'Monthly gross income',
    EXISTING_DEBTS_MONTHLY NUMBER(18,2) COMMENT 'Monthly debt obligations',
    CREDIT_SCORE INT COMMENT 'Credit score',

    PROPERTY_ADDRESS STRING COMMENT 'Property address',
    PROPERTY_TYPE STRING COMMENT 'Property type',
    PURCHASE_PRICE NUMBER(18,2) COMMENT 'Property purchase price',

    LOAN_AMOUNT NUMBER(18,2) COMMENT 'Requested loan amount',
    DOWN_PAYMENT NUMBER(18,2) COMMENT 'Down payment',
    LOAN_TERM_YEARS INT COMMENT 'Loan term in years',
    RATE_TYPE STRING COMMENT 'Interest rate type (Fixed/Variable)',

    COUNTRY STRING COMMENT 'Country',

    LTV_RATIO_PCT NUMBER(5,2) COMMENT 'Loan-to-Value ratio percentage',
    DTI_RATIO_PCT NUMBER(5,2) COMMENT 'Debt-to-Income ratio percentage',

    EXTRACTION_SUCCESS BOOLEAN COMMENT 'TRUE if extraction successful',
    RAW_EXTRACTED_DATA VARIANT COMMENT 'Complete raw JSON for debugging'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Flattened loan data with typed columns from AI_EXTRACT. Ready for business logic and reporting.';

DEFINE TASK {{ db }}.{{ loa_raw }}.LOAI_RAW_TK_EXTRACT_MAIL_DATA
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'SMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ loa_raw }}.LOAI_RAW_SM_EMAIL_FILES')
AS
INSERT INTO {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT (
    FILE_NAME,
    FILE_TIMESTAMP,
    EXTRACTED_DATA,
    EXTRACTION_TIMESTAMP
)
SELECT 
    RELATIVE_PATH AS FILE_NAME,
    LAST_MODIFIED::TIMESTAMP_NTZ AS FILE_TIMESTAMP, 
    SNOWFLAKE.CORTEX.AI_EXTRACT(
        TO_FILE('@{{ db }}.{{ loa_raw }}.LOAI_RAW_ST_EMAIL_INBOUND', RELATIVE_PATH),
        (SELECT schema_json FROM {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_SCHEMA_CONFIG)
    ) AS EXTRACTED_DATA,
    CURRENT_TIMESTAMP() AS EXTRACTION_TIMESTAMP
FROM DIRECTORY(@{{ db }}.{{ loa_raw }}.LOAI_RAW_ST_EMAIL_INBOUND)
WHERE RELATIVE_PATH LIKE '%mortgage%'
  AND RELATIVE_PATH LIKE '%_internal.txt' 
  AND NOT EXISTS (
      SELECT 1 FROM {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT e
      WHERE e.FILE_NAME = RELATIVE_PATH
  );

DEFINE TASK {{ db }}.{{ loa_raw }}.LOAI_RAW_TK_FLAT_MAIL_DATA
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    AFTER {{ db }}.{{ loa_raw }}.LOAI_RAW_TK_EXTRACT_MAIL_DATA
AS
INSERT INTO {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT_FLAT (
    FILE_NAME,
    FILE_TIMESTAMP,
    EXTRACTION_TIMESTAMP,
    DOCUMENT_TYPE,
    CUSTOMER_NAME,
    EMPLOYMENT,
    EMPLOYMENT_TENURE_YEARS,
    MONTHLY_INCOME,
    EXISTING_DEBTS_MONTHLY,
    CREDIT_SCORE,
    PROPERTY_ADDRESS,
    PROPERTY_TYPE,
    PURCHASE_PRICE,
    LOAN_AMOUNT,
    DOWN_PAYMENT,
    LOAN_TERM_YEARS,
    RATE_TYPE,
    COUNTRY,
    LTV_RATIO_PCT,
    DTI_RATIO_PCT,
    EXTRACTION_SUCCESS,
    RAW_EXTRACTED_DATA
)
SELECT 
    s.FILE_NAME,
    s.FILE_TIMESTAMP,
    s.EXTRACTION_TIMESTAMP,

    s.EXTRACTED_DATA:response:document_type::STRING AS DOCUMENT_TYPE,

    NULLIF(s.EXTRACTED_DATA:response:customer_name::STRING, 'None') AS CUSTOMER_NAME,
    NULLIF(s.EXTRACTED_DATA:response:employment::STRING, 'None') AS EMPLOYMENT,
    CASE 
        WHEN s.EXTRACTED_DATA:response:employment_tenure_years::STRING = 'None' THEN NULL
        ELSE TRY_TO_NUMBER(s.EXTRACTED_DATA:response:employment_tenure_years::STRING)
    END AS EMPLOYMENT_TENURE_YEARS,

    CASE 
        WHEN s.EXTRACTED_DATA:response:monthly_income::STRING IN ('None', 'null') THEN NULL
        ELSE TRY_TO_NUMBER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(s.EXTRACTED_DATA:response:monthly_income::STRING, '[A-Z]{3}', ''),
                '[,\\s]', ''
            )
        )
    END AS MONTHLY_INCOME,

    CASE 
        WHEN s.EXTRACTED_DATA:response:existing_debts_monthly::STRING IN ('None', 'null') THEN NULL
        ELSE TRY_TO_NUMBER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(s.EXTRACTED_DATA:response:existing_debts_monthly::STRING, '[A-Z]{3}', ''),
                '[,\\s]', ''
            )
        )
    END AS EXISTING_DEBTS_MONTHLY,

    CASE 
        WHEN s.EXTRACTED_DATA:response:credit_score::STRING IN ('None', 'null') THEN NULL
        ELSE TRY_TO_NUMBER(s.EXTRACTED_DATA:response:credit_score::STRING)
    END AS CREDIT_SCORE,

    NULLIF(s.EXTRACTED_DATA:response:property_address::STRING, 'None') AS PROPERTY_ADDRESS,
    NULLIF(s.EXTRACTED_DATA:response:property_type::STRING, 'None') AS PROPERTY_TYPE,

    CASE 
        WHEN s.EXTRACTED_DATA:response:purchase_price::STRING IN ('None', 'null') THEN NULL
        ELSE TRY_TO_NUMBER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(s.EXTRACTED_DATA:response:purchase_price::STRING, '[A-Z]{3}', ''),
                '[,\\s]', ''
            )
        )
    END AS PURCHASE_PRICE,

    CASE 
        WHEN s.EXTRACTED_DATA:response:loan_amount::STRING IN ('None', 'null') THEN NULL
        ELSE TRY_TO_NUMBER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(s.EXTRACTED_DATA:response:loan_amount::STRING, '[A-Z]{3}', ''),
                '[,\\s]', ''
            )
        )
    END AS LOAN_AMOUNT,

    CASE 
        WHEN s.EXTRACTED_DATA:response:down_payment::STRING IN ('None', 'null') THEN NULL
        ELSE TRY_TO_NUMBER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(s.EXTRACTED_DATA:response:down_payment::STRING, '[A-Z]{3}', ''),
                '[,\\s]', ''
            )
        )
    END AS DOWN_PAYMENT,

    CASE 
        WHEN s.EXTRACTED_DATA:response:loan_term_years::STRING = 'None' THEN NULL
        ELSE TRY_TO_NUMBER(s.EXTRACTED_DATA:response:loan_term_years::STRING)
    END AS LOAN_TERM_YEARS,
    NULLIF(s.EXTRACTED_DATA:response:rate_type::STRING, 'None') AS RATE_TYPE,

    NULLIF(s.EXTRACTED_DATA:response:country::STRING, 'None') AS COUNTRY,

    CASE 
        WHEN TRY_TO_NUMBER(REGEXP_REPLACE(REGEXP_REPLACE(s.EXTRACTED_DATA:response:purchase_price::STRING, '[A-Z]{3}', ''), '[,\\s]', '')) > 0 
        THEN ROUND(
            (TRY_TO_NUMBER(REGEXP_REPLACE(REGEXP_REPLACE(s.EXTRACTED_DATA:response:loan_amount::STRING, '[A-Z]{3}', ''), '[,\\s]', '')) 
             / TRY_TO_NUMBER(REGEXP_REPLACE(REGEXP_REPLACE(s.EXTRACTED_DATA:response:purchase_price::STRING, '[A-Z]{3}', ''), '[,\\s]', ''))) * 100, 
            2
        )
        ELSE NULL 
    END AS LTV_RATIO_PCT,

    CASE 
        WHEN TRY_TO_NUMBER(REGEXP_REPLACE(REGEXP_REPLACE(s.EXTRACTED_DATA:response:monthly_income::STRING, '[A-Z]{3}', ''), '[,\\s]', '')) > 0 
        THEN ROUND(
            (TRY_TO_NUMBER(REGEXP_REPLACE(REGEXP_REPLACE(s.EXTRACTED_DATA:response:existing_debts_monthly::STRING, '[A-Z]{3}', ''), '[,\\s]', '')) 
             / TRY_TO_NUMBER(REGEXP_REPLACE(REGEXP_REPLACE(s.EXTRACTED_DATA:response:monthly_income::STRING, '[A-Z]{3}', ''), '[,\\s]', ''))) * 100, 
            2
        )
        ELSE NULL 
    END AS DTI_RATIO_PCT,

    CASE WHEN s.EXTRACTED_DATA:response IS NOT NULL THEN TRUE ELSE FALSE END AS EXTRACTION_SUCCESS,

    s.EXTRACTED_DATA AS RAW_EXTRACTED_DATA

FROM {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT s
WHERE NOT EXISTS (
      SELECT 1 FROM {{ db }}.{{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT_FLAT f
      WHERE f.FILE_NAME = s.FILE_NAME
        AND f.EXTRACTION_TIMESTAMP = s.EXTRACTION_TIMESTAMP
  );
