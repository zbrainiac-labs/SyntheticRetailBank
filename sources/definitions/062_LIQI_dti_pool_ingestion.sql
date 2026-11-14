/*
 * 062_LIQI_dti_pool_ingestion.sql
 * DtI (UC2) securities pool and business-day transactions.
 * Simulated ANUBIS APEL / FRISCO feeds written by generators/dti_securities_generator.py
 * and lcr_data_generator.py. All files land in one stage. The month-end history of holdings,
 * deposits, contracts and cashflows lives in the main LIQI tables (SNAPSHOT_SCOPE = MONTH_END).
 */

-- ============================================================
-- RAW FEEDS
-- ============================================================

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURITIES (
    SECURITY_ID VARCHAR(20) NOT NULL COMMENT 'Persistent identifier of the security position in the DtI collateral pool (SEC-nnnnn).',
    AS_OF_DATE DATE NOT NULL COMMENT 'Reporting date (data set) of the valuation.',
    ENTITY_ID VARCHAR(10) NOT NULL COMMENT 'Legal entity holding the security (LE_01..LE_09).',
    ISIN VARCHAR(12) COMMENT 'ISIN of the security (synthetic, country prefix by currency).',
    SECURITY_NAME VARCHAR(200) COMMENT 'Display name: issuer, instrument type, coupon and maturity.',
    ISSUER VARCHAR(100) COMMENT 'Issuer name of the security (UC2 6.8 master data).',
    ISSUER_GROUP VARCHAR(50) COMMENT 'Issuer group (GD226-like) for the free assets breakdown (UC2 6.9 element D).',
    INDUSTRY VARCHAR(50) COMMENT 'Industry of the issuer (GD205B-like) for the free assets breakdown.',
    CURRENCY VARCHAR(3) COMMENT 'Currency of the security (CHF, EUR for WLK XIa, USD for WLK XIb); values are in CHF.',
    WLK_CLUSTER VARCHAR(5) NOT NULL COMMENT 'Internal collateral cluster (FK to LIQI_RAW_TB_DTI_WLK_CLUSTERS), I..XIb.',
    HQLA_CATEGORY VARCHAR(5) COMMENT 'Regulatory liquidity category of the cluster: L1, L2A, L2B or NONE.',
    HAIRCUT NUMBER(6,4) COMMENT 'WLK haircut applied to the base value to derive the stressed liquidity value.',
    NOMINAL_CHF NUMBER(18,2) COMMENT 'Nominal amount of the position in CHF.',
    COUPON_RATE NUMBER(8,5) COMMENT 'Annual coupon rate (decimal); 0 for bills and equities.',
    COUPON_FREQ INTEGER COMMENT 'Coupon payments per year (1, 2, 12); 0 for zero coupon paper and equities.',
    MATURITY_DATE DATE COMMENT 'Final maturity (redemption date); NULL for equities.',
    PRICE NUMBER(12,4) COMMENT 'Price in percent of nominal on the reporting date (cluster and idiosyncratic moves).',
    MARKET_VALUE_CHF NUMBER(18,2) COMMENT 'Market value = nominal x price; basis of the DtI base values.',
    COLLATERAL_VALUE_CHF NUMBER(18,2) COMMENT 'Value after haircut = market value x (1 - haircut); basis of the stressed values.',
    IS_ENCUMBERED BOOLEAN COMMENT 'TRUE when pledged in secured funding; encumbered securities are not part of the free DtI pool.',
    IS_MOBILISED BOOLEAN COMMENT 'TRUE when already mobilised at the central bank (UC2 6.9 element E).',
    IS_SHORT_TERM BOOLEAN COMMENT 'TRUE for short-term paper (bills, short remaining maturity) that rolls within weeks.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the record was loaded into Snowflake.',
    RUN_ID VARCHAR(30) COMMENT 'Generation run of the liquidity book (FK to LIQI_RAW_TB_GENERATION_RUN). The same securities (same ids and market values) feed the LCR HQLA holdings.',
    PRIMARY KEY (SECURITY_ID, AS_OF_DATE)
)
CLUSTER BY (AS_OF_DATE)
CHANGE_TRACKING = TRUE
COMMENT = 'DtI collateral pool securities per reporting date (simulated ANUBIS APEL feed, UC2 6.8/6.9): 15-40 securities per WLK cluster with master data, price, market value and value after haircut. Loaded from dti_securities_YYYYMMDD.csv via stage LIQI_RAW_ST_DTI_POOL.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURITY_CASHFLOWS (
    AS_OF_DATE DATE NOT NULL COMMENT 'Reporting date (data set) the cashflow schedule belongs to.',
    SECURITY_ID VARCHAR(20) NOT NULL COMMENT 'Security (FK to LIQI_RAW_TB_DTI_SECURITIES).',
    CF_DATE DATE NOT NULL COMMENT 'Cashflow date (coupon or redemption); LATER rows carry all flows beyond the horizon at maturity.',
    CF_TYPE VARCHAR(12) NOT NULL COMMENT 'COUPON, REDEMPTION or LATER (collapsed flows beyond 400 days, bucket GT12M).',
    CF_NOMINAL_CHF NUMBER(18,2) COMMENT 'Contractual cashflow amount in CHF (coupon or principal).',
    BASE_VALUE_CHF NUMBER(18,2) COMMENT 'Liquidity value (base) = market value x cashflow / sum of cashflows (prototype valuation).',
    STRESSED_VALUE_CHF NUMBER(18,2) COMMENT 'Liquidity value (stressed) = base value x (1 - haircut).',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the record was loaded into Snowflake.',
    PRIMARY KEY (AS_OF_DATE, SECURITY_ID, CF_DATE, CF_TYPE)
)
CLUSTER BY (AS_OF_DATE)
CHANGE_TRACKING = TRUE
COMMENT = 'Liquidity value of each pool security spread over its own cashflow dates and valued with the price of the data set (as in the UC2 prototype Daten_Zeitreihen). Feeds the Collateral Pool securities of the DtI projection. Loaded from dti_security_cashflows_YYYYMMDD.csv.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_TRANSACTIONS (
    TXN_ID VARCHAR(20) NOT NULL COMMENT 'Identifier of the business-day transaction (TXN-nnnnnn).',
    AS_OF_DATE DATE NOT NULL COMMENT 'Reporting date on which the transaction is still alive (booked, not yet ended).',
    ENTITY_ID VARCHAR(10) NOT NULL COMMENT 'Legal entity (LE_01..LE_09).',
    TXN_TYPE VARCHAR(20) NOT NULL COMMENT 'DEPOSIT_MOVEMENT (temporary large corporate / FI deposit change) or NEW_LOAN (new business).',
    PRODUCT_TYPE VARCHAR(50) NOT NULL COMMENT 'Deposit type or loan product (FK to the DtI assignment rules).',
    COUNTERPARTY_TYPE VARCHAR(30) COMMENT 'Counterparty sector used for the behavioral run-off rules (CORPORATE, FINANCIAL_INSTITUTION, RETAIL).',
    APPLICATION_ID VARCHAR(10) COMMENT 'Source application (EIN deposits, KRE / HYP loans).',
    AKTIV_PASSIV VARCHAR(1) COMMENT 'Balance sheet side: A = asset (loan), P = liability (deposit).',
    BOOK_DATE DATE NOT NULL COMMENT 'Business day the transaction was booked.',
    END_DATE DATE NOT NULL COMMENT 'Date the deposit movement reverses or the new loan matures.',
    AMOUNT_CHF NUMBER(18,2) NOT NULL COMMENT 'Amount in CHF (signed for deposit movements: positive = inflow of funds).',
    INTEREST_RATE NUMBER(8,5) COMMENT 'Annual interest rate of new loans (decimal); 0 for deposit movements.',
    IS_NEW_BUSINESS BOOLEAN COMMENT 'TRUE for new business (status Neu on the deal level page).',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the record was loaded into Snowflake.',
    PRIMARY KEY (TXN_ID, AS_OF_DATE)
)
CLUSTER BY (AS_OF_DATE)
CHANGE_TRACKING = TRUE
COMMENT = 'DtI-only business-day transactions (simulated FRISCO feed): large corporate deposit movements and loan new business, so every reporting date shows real product deltas. Added on top of the LCR book in the DtI engine; the LCR is not affected. Loaded from dti_transactions_YYYYMMDD.csv.';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DTI_POOL
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    COMMENT = 'Internal landing stage for the DtI pool feeds: dti_securities_*, dti_security_cashflows_*, dti_transactions_*. Monitored by stream LIQI_RAW_SM_DTI_POOL_FILES.';

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_POOL_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DTI_POOL
    COMMENT = 'Monitors LIQI_RAW_ST_DTI_POOL for new files; consumed by LIQI_RAW_TK_LOAD_DTI_SECURITIES only (one stream per consuming task).';

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_SECURITY_CASHFLOW_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DTI_POOL
    COMMENT = 'Monitors LIQI_RAW_ST_DTI_POOL for new files; consumed by LIQI_RAW_TK_LOAD_DTI_SECURITY_CASHFLOWS only (one stream per consuming task).';

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_TRANSACTION_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DTI_POOL
    COMMENT = 'Monitors LIQI_RAW_ST_DTI_POOL for new files; consumed by LIQI_RAW_TK_LOAD_DTI_TRANSACTIONS only (one stream per consuming task).';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_DTI_SECURITIES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly load of DtI pool securities into LIQI_RAW_TB_DTI_SECURITIES when stream LIQI_RAW_SM_DTI_POOL_FILES has data.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_POOL_FILES')
    STARTED
AS
    EXECUTE IMMEDIATE $$
    BEGIN
        COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURITIES (
            SECURITY_ID, AS_OF_DATE, ENTITY_ID, ISIN, SECURITY_NAME, ISSUER, ISSUER_GROUP, INDUSTRY, CURRENCY, WLK_CLUSTER,
            HQLA_CATEGORY, HAIRCUT, NOMINAL_CHF, COUPON_RATE, COUPON_FREQ, MATURITY_DATE, PRICE, MARKET_VALUE_CHF,
            COLLATERAL_VALUE_CHF, IS_ENCUMBERED, IS_MOBILISED, IS_SHORT_TERM, RUN_ID
        )
        FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DTI_POOL
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = ','
                       TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '') ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
        PATTERN = '.*dti_securities_.*\\.csv'
        ON_ERROR = 'CONTINUE';
        -- consume the file stream (offset only): it is empty again and does not go stale
        CREATE OR REPLACE TEMPORARY TABLE STREAM_ADVANCE AS SELECT RELATIVE_PATH FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_POOL_FILES WHERE 0 = 1;
    END;
    $$;

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_DTI_SECURITY_CASHFLOWS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly load of security cashflow values into LIQI_RAW_TB_DTI_SECURITY_CASHFLOWS when stream LIQI_RAW_SM_DTI_POOL_FILES has data.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_SECURITY_CASHFLOW_FILES')
    STARTED
AS
    EXECUTE IMMEDIATE $$
    BEGIN
        COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURITY_CASHFLOWS (
            AS_OF_DATE, SECURITY_ID, CF_DATE, CF_TYPE, CF_NOMINAL_CHF, BASE_VALUE_CHF, STRESSED_VALUE_CHF
        )
        FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DTI_POOL
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = ','
                       TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '') ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
        PATTERN = '.*dti_security_cashflows_.*\\.csv'
        ON_ERROR = 'CONTINUE';
        -- consume the file stream (offset only): it is empty again and does not go stale
        CREATE OR REPLACE TEMPORARY TABLE STREAM_ADVANCE AS SELECT RELATIVE_PATH FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_SECURITY_CASHFLOW_FILES WHERE 0 = 1;
    END;
    $$;

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_DTI_TRANSACTIONS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly load of DtI business-day transactions into LIQI_RAW_TB_DTI_TRANSACTIONS when stream LIQI_RAW_SM_DTI_POOL_FILES has data.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_TRANSACTION_FILES')
    STARTED
AS
    EXECUTE IMMEDIATE $$
    BEGIN
        COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_TRANSACTIONS (
            TXN_ID, AS_OF_DATE, ENTITY_ID, TXN_TYPE, PRODUCT_TYPE, COUNTERPARTY_TYPE, APPLICATION_ID, AKTIV_PASSIV,
            BOOK_DATE, END_DATE, AMOUNT_CHF, INTEREST_RATE, IS_NEW_BUSINESS
        )
        FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DTI_POOL
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = ','
                       TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '') ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
        PATTERN = '.*dti_transactions_.*\\.csv'
        ON_ERROR = 'CONTINUE';
        -- consume the file stream (offset only): it is empty again and does not go stale
        CREATE OR REPLACE TEMPORARY TABLE STREAM_ADVANCE AS SELECT RELATIVE_PATH FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DTI_TRANSACTION_FILES WHERE 0 = 1;
    END;
    $$;
