DEFINE STAGE {{ db }}.{{ crm_raw }}.CRMI_RAW_ST_CUSTOMERS
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for customer master data CSV files. Expected pattern: *customers*.csv';

DEFINE STAGE {{ db }}.{{ crm_raw }}.CRMI_RAW_ST_ADDRESSES
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for customer address CSV files with SCD Type 2 support. Expected pattern: *customer_addresses*.csv';

DEFINE STAGE {{ db }}.{{ crm_raw }}.CRMI_RAW_ST_EXPOSED_PERSON
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for PEP (Politically Exposed Persons) compliance CSV files. Expected pattern: *pep*.csv';

DEFINE STAGE {{ db }}.{{ crm_raw }}.CRMI_RAW_ST_CUSTOMER_EVENTS
    DIRECTORY = (
        ENABLE = TRUE
        AUTO_REFRESH = TRUE
    )
    COMMENT = 'Internal stage for customer lifecycle event and status CSV files. Expected patterns: *customer_events*.csv, *customer_status*.csv';

DEFINE TABLE {{ db }}.{{ crm_raw }}.CRMI_RAW_TB_CUSTOMER (
    CUSTOMER_ID VARCHAR(30) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='top_secret') COMMENT 'Unique customer identifier (CUST_XXXXX format)',
    FIRST_NAME VARCHAR(100) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Customer first name (localized to country)',
    FAMILY_NAME VARCHAR(100) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Customer family/last name (localized to country)',
    DATE_OF_BIRTH DATE NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Date of birth (YYYY-MM-DD format)',
    ONBOARDING_DATE DATE NOT NULL COMMENT 'Customer onboarding date (YYYY-MM-DD)',
    REPORTING_CURRENCY VARCHAR(3) NOT NULL COMMENT 'Customer reporting currency based on country (EUR, GBP, USD, CHF, NOK, SEK, DKK, PLN)',
    HAS_ANOMALY BOOLEAN NOT NULL DEFAULT FALSE WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Flag indicating customer has anomalous transaction patterns for compliance testing',
    EMPLOYER VARCHAR(200) WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Employer name (nullable for unemployed/retired)',
    POSITION VARCHAR(100) COMMENT 'Job position/title',
    EMPLOYMENT_TYPE VARCHAR(30) COMMENT 'Employment type (FULL_TIME, PART_TIME, CONTRACT, SELF_EMPLOYED, RETIRED, UNEMPLOYED)',
    INCOME_RANGE VARCHAR(30) COMMENT 'Income range bracket (e.g., 50K-75K, 100K-150K)',
    ACCOUNT_TIER VARCHAR(30) COMMENT 'Account tier (STANDARD, SILVER, GOLD, PLATINUM, PREMIUM)',
    EMAIL VARCHAR(255) WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Customer email address',
    PHONE VARCHAR(50) WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Customer phone number',
    PREFERRED_CONTACT_METHOD VARCHAR(20) COMMENT 'Preferred contact method (EMAIL, SMS, POST, MOBILE_APP)',
    RISK_CLASSIFICATION VARCHAR(20) WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Risk classification (LOW, MEDIUM, HIGH)',
    CREDIT_SCORE_BAND VARCHAR(20) WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Credit score band (POOR, FAIR, GOOD, VERY_GOOD, EXCELLENT)',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ NOT NULL COMMENT 'UTC timestamp when this customer record version was inserted (for SCD Type 2)',

    CONSTRAINT PK_CRMI_RAW_TB_CUSTOMER PRIMARY KEY (CUSTOMER_ID, INSERT_TIMESTAMP_UTC)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Customer master data table with SCD Type 2 support for tracking attribute changes over time. Extended attributes include employment, account tier, contact preferences, and risk profile. Multiple records per customer allowed, uniquely identified by (CUSTOMER_ID, INSERT_TIMESTAMP_UTC). Address data stored separately in CRMI_RAW_TB_ADDRESSES with its own SCD Type 2 tracking.';

DEFINE TABLE {{ db }}.{{ crm_raw }}.CRMI_RAW_TB_ADDRESSES (
    CUSTOMER_ID VARCHAR(30) NOT NULL COMMENT 'Reference to customer (foreign key to CRMI_RAW_TB_CUSTOMER)',
    STREET_ADDRESS VARCHAR(200) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='top_secret') COMMENT 'Street address (localized format)',
    CITY VARCHAR(100) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'City name (localized to country)',
    STATE VARCHAR(100) COMMENT 'State/Region (where applicable for the country)',
    ZIPCODE VARCHAR(20) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Postal code (country-specific format)',
    COUNTRY VARCHAR(50) NOT NULL COMMENT 'Customer country (12 EMEA countries supported)',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ NOT NULL COMMENT 'UTC timestamp when this address record was inserted (for SCD Type 2)',

    CONSTRAINT PK_CRMI_RAW_TB_ADDRESSES PRIMARY KEY (CUSTOMER_ID, INSERT_TIMESTAMP_UTC)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Customer address base table with append-only structure (SCD Type 2). Multiple records per customer are allowed, uniquely identified by (CUSTOMER_ID, INSERT_TIMESTAMP_UTC). Dynamic tables in CRM_AGG_001 provide current and historical views.';

DEFINE TABLE {{ db }}.{{ crm_raw }}.CRMI_RAW_TB_EXPOSED_PERSON (
    EXPOSED_PERSON_ID VARCHAR(50) NOT NULL COMMENT 'Unique PEP identifier',
    FULL_NAME VARCHAR(200) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='top_secret') COMMENT 'Full name of the politically exposed person',
    FIRST_NAME VARCHAR(100) WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='top_secret') COMMENT 'First name',
    LAST_NAME VARCHAR(100) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='top_secret') COMMENT 'Last name/family name',
    DATE_OF_BIRTH DATE WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Date of birth (YYYY-MM-DD)',
    NATIONALITY VARCHAR(50) COMMENT 'Nationality/citizenship',
    POSITION_TITLE VARCHAR(200) NOT NULL COMMENT 'Political position or title held',
    ORGANIZATION VARCHAR(200) COMMENT 'Government organization or political party',
    COUNTRY VARCHAR(50) NOT NULL COMMENT 'Country where political position is/was held',
    EXPOSED_PERSON_CATEGORY VARCHAR(50) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'PEP category: DOMESTIC, FOREIGN, INTERNATIONAL_ORG, FAMILY_MEMBER, CLOSE_ASSOCIATE',
    RISK_LEVEL VARCHAR(20) NOT NULL WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Risk assessment level: LOW, MEDIUM, HIGH, CRITICAL',
    STATUS VARCHAR(20) NOT NULL COMMENT 'Current status: ACTIVE, INACTIVE, DECEASED',
    START_DATE DATE COMMENT 'Date when PEP status began (YYYY-MM-DD)',
    END_DATE DATE COMMENT 'Date when PEP status ended (YYYY-MM-DD), NULL if still active',
    REFERENCE_LINK VARCHAR(500) COMMENT 'URL reference to official source or documentation',
    SOURCE VARCHAR(100) COMMENT 'Data source (e.g., government website, sanctions list)',
    LAST_UPDATED DATE NOT NULL COMMENT 'Date when record was last updated (YYYY-MM-DD)',
    CREATED_DATE DATE NOT NULL COMMENT 'Date when record was created (YYYY-MM-DD)',

    CONSTRAINT PK_CRMI_RAW_TB_EXPOSED_PERSON PRIMARY KEY (EXPOSED_PERSON_ID)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Politically Exposed Persons (PEP) master data for compliance and risk management. Tracks current and former political figures, their family members, and close associates for regulatory compliance.';

DEFINE TABLE {{ db }}.{{ crm_raw }}.CRMI_RAW_TB_CUSTOMER_EVENT (
    EVENT_ID VARCHAR(50) NOT NULL COMMENT 'Unique event identifier (EVT_XXXXX format)',
    CUSTOMER_ID VARCHAR(30) NOT NULL COMMENT 'Reference to customer (foreign key to CRMI_RAW_TB_CUSTOMER)',
    EVENT_TYPE VARCHAR(30) NOT NULL COMMENT 'Type of event (ONBOARDING, ADDRESS_CHANGE, EMPLOYMENT_CHANGE, ACCOUNT_UPGRADE, ACCOUNT_DOWNGRADE, ACCOUNT_CLOSE, REACTIVATION, CHURN)',
    EVENT_DATE DATE NOT NULL COMMENT 'Date when the event occurred (YYYY-MM-DD)',
    EVENT_TIMESTAMP_UTC TIMESTAMP_NTZ NOT NULL COMMENT 'UTC timestamp of the event for precise ordering',
    CHANNEL VARCHAR(50) COMMENT 'Channel through which the event occurred (ONLINE, BRANCH, MOBILE, PHONE, SYSTEM)',
    EVENT_DETAILS VARIANT COMMENT 'JSON object containing event-specific details (e.g., old/new address, job title, account type, income changes)',
    PREVIOUS_VALUE VARCHAR(500) COMMENT 'Previous state before event (e.g., "Old Company", "STANDARD tier", "123 Old St, City") - for quick filtering without JSON parsing',
    NEW_VALUE VARCHAR(500) COMMENT 'New state after event (e.g., "New Company", "PREMIUM tier", "456 New Ave, Town") - for quick filtering without JSON parsing',
    TRIGGERED_BY VARCHAR(100) COMMENT 'User/system that triggered event (e.g., CUSTOMER_SELF_SERVICE, BRANCH_OFFICER_123, SYSTEM_AUTO)',
    REQUIRES_REVIEW BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating if event requires manual compliance review',
    REVIEW_STATUS VARCHAR(20) COMMENT 'Review status (PENDING/APPROVED/REJECTED/NOT_REQUIRED)',
    REVIEW_DATE DATE COMMENT 'Date when review was completed',
    NOTES VARCHAR(1000) WITH TAG ({{ db }}.PUBLIC.SENSITIVITY_LEVEL='restricted') COMMENT 'Free-text notes about the event for compliance or customer service',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'System timestamp when record was inserted',

    CONSTRAINT PK_CRMI_RAW_TB_CUSTOMER_EVENT PRIMARY KEY (EVENT_ID)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Customer lifecycle event log tracking all significant customer status changes, account modifications, and behavioral milestones. Used for lifecycle analytics, churn prediction, and AML correlation. PREVIOUS_VALUE and NEW_VALUE provide quick summaries;

DEFINE TABLE CRMI_RAW_TB_CUSTOMER_STATUS (
    STATUS_ID VARCHAR(50) NOT NULL COMMENT 'Unique status record identifier (STAT_XXXXX format)',
    CUSTOMER_ID VARCHAR(30) NOT NULL COMMENT 'Reference to customer (foreign key to CRMI_RAW_TB_CUSTOMER)',
    STATUS VARCHAR(30) NOT NULL COMMENT 'Customer status (ACTIVE/INACTIVE/DORMANT/SUSPENDED/CLOSED/REACTIVATED)',
    STATUS_REASON VARCHAR(100) COMMENT 'Reason for status change (e.g., VOLUNTARY_CLOSURE, INACTIVITY, REGULATORY_SUSPENSION)',
    STATUS_START_DATE DATE NOT NULL COMMENT 'Date when this status became effective',
    STATUS_END_DATE DATE COMMENT 'Date when this status ended (NULL if current)',
    IS_CURRENT BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'Flag indicating if this is the current status',
    LINKED_EVENT_ID VARCHAR(50) COMMENT 'Reference to triggering event in CRMI_RAW_TB_CUSTOMER_EVENT',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'System timestamp when record was inserted',

    CONSTRAINT PK_CRMI_RAW_TB_CUSTOMER_STATUS PRIMARY KEY (STATUS_ID),
    CONSTRAINT FK_CRMI_RAW_TB_CUSTOMER_STATUS__CRMI_RAW_TB_CUSTOMER_EVENT FOREIGN KEY (LINKED_EVENT_ID) REFERENCES {{ crm_raw }}.CRMI_RAW_TB_CUSTOMER_EVENT (EVENT_ID)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Customer status history with SCD Type 2 tracking. Maintains current and historical customer status for lifecycle analysis, churn prediction, and regulatory reporting. Linked to CRMI_RAW_TB_CUSTOMER_EVENT for complete audit trail. FK to CRMI_RAW_TB_CUSTOMER removed due to SCD Type 2 composite PK.';

DEFINE TASK {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_CUSTOMERS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ crm_raw }}.CRMI_RAW_SM_CUSTOMER_FILES')
AS
    COPY INTO {{ crm_raw }}.CRMI_RAW_TB_CUSTOMER (
        CUSTOMER_ID, 
        FIRST_NAME, 
        FAMILY_NAME, 
        DATE_OF_BIRTH, 
        ONBOARDING_DATE, 
        REPORTING_CURRENCY, 
        HAS_ANOMALY,
        EMPLOYER,
        POSITION,
        EMPLOYMENT_TYPE,
        INCOME_RANGE,
        ACCOUNT_TIER,
        EMAIL,
        PHONE,
        PREFERRED_CONTACT_METHOD,
        RISK_CLASSIFICATION,
        CREDIT_SCORE_BAND,
        INSERT_TIMESTAMP_UTC
    )
    FROM (
        SELECT 
            $1::VARCHAR(30),
            $2::VARCHAR(100),
            $3::VARCHAR(100),
            $4::DATE,
            $5::DATE,
            $6::VARCHAR(3),
            $7::BOOLEAN,
            NULLIF($8, '')::VARCHAR(200), 
            NULLIF($9, '')::VARCHAR(100),  
            NULLIF($10, '')::VARCHAR(30),  
            NULLIF($11, '')::VARCHAR(30),  
            NULLIF($12, '')::VARCHAR(30),  
            NULLIF($13, '')::VARCHAR(255), 
            NULLIF($14, '')::VARCHAR(50),  
            NULLIF($15, '')::VARCHAR(20),  
            NULLIF($16, '')::VARCHAR(20),  
            NULLIF($17, '')::VARCHAR(20),  
            COALESCE(
                TRY_CAST($18 AS TIMESTAMP_NTZ), 
                CURRENT_TIMESTAMP()              
            ) AS INSERT_TIMESTAMP_UTC
        FROM @CRMI_RAW_ST_CUSTOMERS
    )
    PATTERN = '.*customers.*\.csv'
    FILE_FORMAT = CRMI_RAW_FF_CUSTOMER_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_ADDRESSES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ crm_raw }}.CRMI_RAW_SM_ADDRESS_FILES')
AS
    COPY INTO {{ crm_raw }}.CRMI_RAW_TB_ADDRESSES (
        CUSTOMER_ID, 
        STREET_ADDRESS, 
        CITY, 
        STATE, 
        ZIPCODE, 
        COUNTRY, 
        INSERT_TIMESTAMP_UTC
    )
    FROM (
        SELECT 
            $1::VARCHAR(30) AS CUSTOMER_ID,
            $2::VARCHAR(200) AS STREET_ADDRESS,
            $3::VARCHAR(100) AS CITY,
            NULLIF($4, '')::VARCHAR(100) AS STATE, 
            $5::VARCHAR(20) AS ZIPCODE,
            $6::VARCHAR(50) AS COUNTRY,
            $7::TIMESTAMP_NTZ AS INSERT_TIMESTAMP_UTC
        FROM @CRMI_RAW_ST_ADDRESSES
    )
    PATTERN = '.*customer_addresses.*\.csv'
    FILE_FORMAT = CRMI_RAW_FF_ADDRESS_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_EXPOSED_PERSON
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ crm_raw }}.CRMI_RAW_SM_EXPOSED_PERSON_FILES')
AS
    COPY INTO {{ crm_raw }}.CRMI_RAW_TB_EXPOSED_PERSON (
        EXPOSED_PERSON_ID, 
        FULL_NAME, 
        FIRST_NAME, 
        LAST_NAME, 
        DATE_OF_BIRTH, 
        NATIONALITY,
        POSITION_TITLE, 
        ORGANIZATION, 
        COUNTRY, 
        EXPOSED_PERSON_CATEGORY, 
        RISK_LEVEL, 
        STATUS,
        START_DATE, 
        END_DATE, 
        REFERENCE_LINK, 
        SOURCE, 
        LAST_UPDATED, 
        CREATED_DATE
    )
    FROM @CRMI_RAW_ST_EXPOSED_PERSON
    PATTERN = '.*pep.*\.csv'
    FILE_FORMAT = CRMI_RAW_FF_EXPOSED_PERSON_CSV
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_CUSTOMER_EVENTS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ crm_raw }}.CRMI_RAW_SM_CUSTOMER_EVENT_FILES')
AS
    COPY INTO {{ crm_raw }}.CRMI_RAW_TB_CUSTOMER_EVENT (
        EVENT_ID,
        CUSTOMER_ID,
        EVENT_TYPE,
        EVENT_DATE,
        EVENT_TIMESTAMP_UTC,
        CHANNEL,
        EVENT_DETAILS,
        PREVIOUS_VALUE,
        NEW_VALUE,
        TRIGGERED_BY,
        REQUIRES_REVIEW,
        REVIEW_STATUS,
        REVIEW_DATE,
        NOTES
    )
    FROM (
        SELECT 
            $1::VARCHAR(50),
            $2::VARCHAR(30),
            $3::VARCHAR(30),
            $4::DATE,
            $5::TIMESTAMP_NTZ,
            $6::VARCHAR(50),
            PARSE_JSON(REPLACE($7, '''', '"')), 
            $8::VARCHAR(500),
            $9::VARCHAR(500),
            $10::VARCHAR(100),
            $11::BOOLEAN,
            $12::VARCHAR(20),
            NULLIF($13, '')::DATE, 
            $14::VARCHAR(1000)
        FROM @CRMI_RAW_ST_CUSTOMER_EVENTS
    )
    FILE_FORMAT = CRMI_RAW_FF_CUSTOMER_EVENT_CSV
    PATTERN = '.*customer_events.*\.csv'
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_CUSTOMER_STATUS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    AFTER {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_CUSTOMER_EVENTS
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ crm_raw }}.CRMI_RAW_SM_CUSTOMER_STATUS_FILES')
AS
    COPY INTO {{ crm_raw }}.CRMI_RAW_TB_CUSTOMER_STATUS (
        STATUS_ID,
        CUSTOMER_ID,
        STATUS,
        STATUS_REASON,
        STATUS_START_DATE,
        STATUS_END_DATE,
        IS_CURRENT,
        LINKED_EVENT_ID
    )
    FROM (
        SELECT 
            $1::VARCHAR(50),
            $2::VARCHAR(30),
            $3::VARCHAR(30),
            $4::VARCHAR(100),
            $5::DATE,
            $6::DATE,
            $7::BOOLEAN,
            $8::VARCHAR(50)
        FROM @CRMI_RAW_ST_CUSTOMER_EVENTS
    )
    FILE_FORMAT = CRMI_RAW_FF_CUSTOMER_STATUS_CSV
    PATTERN = '.*customer_status.*\.csv'
    ON_ERROR = CONTINUE;

DEFINE TASK {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_CLEANUP_AFTER_LOAD_CUSTOMERS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER customer data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_CUSTOMERS
AS
    CALL CRMI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('CRMI_RAW_ST_CUSTOMERS', 5);

DEFINE TASK {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_CLEANUP_AFTER_LOAD_ADDRESSES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER address data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_ADDRESSES
AS
    CALL CRMI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('CRMI_RAW_ST_ADDRESSES', 5);

DEFINE TASK {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_CLEANUP_AFTER_LOAD_EXPOSED_PERSON
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER PEP data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ crm_raw }}.CRMI_RAW_TK_LOAD_EXPOSED_PERSON
AS
    CALL CRMI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('CRMI_RAW_ST_EXPOSED_PERSON', 5);
