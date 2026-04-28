DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY (
 ASSET_TYPE VARCHAR(50) PRIMARY KEY,
 ASSET_NAME VARCHAR(200) NOT NULL,
 REGULATORY_LEVEL VARCHAR(10) NOT NULL,
 HAIRCUT_PCT NUMBER(5,2) NOT NULL,
 HAIRCUT_FACTOR NUMBER(5,4) NOT NULL,
 SNB_COORDINATE VARCHAR(20),
 ELIGIBILITY_CRITERIA VARCHAR(500),
 MIN_CREDIT_RATING VARCHAR(10),
 REQUIRES_SMI_CONSTITUENT BOOLEAN DEFAULT FALSE,
 IS_ACTIVE BOOLEAN DEFAULT TRUE,
 EFFECTIVE_DATE DATE NOT NULL,
 END_DATE DATE,
 INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE
COMMENT = 'Master reference table for FINMA LCR High-Quality Liquid Assets (HQLA) classification. Defines regulatory levels (L1, L2A, L2B), haircut factors (0%, 15%, 50%), and eligibility criteria per FINMA Circular 2015/2 and Basel III. Used by Treasury for liquidity buffer management, by Risk for LCR monitoring, and by Compliance for SNB regulatory submissions. Supports automated eligibility screening, portfolio composition analysis, and 40% cap rule enforcement. Critical reference data for daily LCR calculation and strategic liquidity planning.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES (
 DEPOSIT_TYPE VARCHAR(50) PRIMARY KEY,
 DEPOSIT_NAME VARCHAR(200) NOT NULL,
 BASE_RUN_OFF_RATE NUMBER(5,4) NOT NULL,
 COUNTERPARTY_TYPE VARCHAR(50) NOT NULL,
 SNB_COORDINATE VARCHAR(20),
 ELIGIBILITY_CRITERIA VARCHAR(500),
 ALLOWS_RELATIONSHIP_DISCOUNT BOOLEAN DEFAULT FALSE,
 IS_INSURED BOOLEAN DEFAULT FALSE,
 IS_OPERATIONAL BOOLEAN DEFAULT FALSE,
 IS_ACTIVE BOOLEAN DEFAULT TRUE,
 EFFECTIVE_DATE DATE NOT NULL,
 END_DATE DATE,
 INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE
COMMENT = 'Master reference table for FINMA LCR deposit classification and stressed run-off rate assumptions. Defines counterparty types (Retail/Corporate/FI), base run-off rates (3% to 100%), relationship discount eligibility, and operational deposit treatment per FINMA Circular 2015/2 and Basel III. Used by Treasury for funding stability analysis, by ALM for liquidity stress testing, and by Compliance for SNB regulatory submissions. Supports automated outflow calculation, customer relationship scoring, and deposit retention strategy optimization. Critical reference data for LCR denominator calculation and strategic funding planning.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS (
 HOLDING_ID VARCHAR(50) NOT NULL,
 AS_OF_DATE DATE NOT NULL,
 ASSET_TYPE VARCHAR(50) NOT NULL,
 ISIN VARCHAR(12),
 SECURITY_NAME VARCHAR(200),
 CURRENCY VARCHAR(3) NOT NULL,
 QUANTITY NUMBER(18,6),
 MARKET_VALUE_CCY NUMBER(18,2) NOT NULL,
 MARKET_VALUE_CHF NUMBER(18,2) NOT NULL,
 FX_RATE NUMBER(12,6),
 MATURITY_DATE DATE,
 CREDIT_RATING VARCHAR(10),
 SMI_CONSTITUENT BOOLEAN DEFAULT FALSE,
 HQLA_ELIGIBLE BOOLEAN DEFAULT TRUE,
 INELIGIBILITY_REASON VARCHAR(500),
 PORTFOLIO_CODE VARCHAR(50),
 CUSTODIAN VARCHAR(100),
 INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
 PRIMARY KEY (HOLDING_ID, AS_OF_DATE)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily snapshot table of High-Quality Liquid Assets (HQLA) held by Treasury for liquidity buffer management per FINMA Circular 2015/2. Contains securities positions with market values, regulatory classifications, and eligibility flags. Source data loaded from treasury management systems via CSV extracts. Used for LCR numerator calculation (HQLA stock with haircuts), portfolio concentration analysis, and SNB monthly regulatory reporting. Supports real-time liquidity monitoring, investment committee reporting, and Basel III compliance. Critical operational data for Treasury decision-making and regulatory supervision.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES (
 ACCOUNT_ID VARCHAR(50) NOT NULL,
 AS_OF_DATE DATE NOT NULL,
 CUSTOMER_ID VARCHAR(50) NOT NULL,
 DEPOSIT_TYPE VARCHAR(50) NOT NULL,
 CURRENCY VARCHAR(3) NOT NULL,
 BALANCE_CCY NUMBER(18,2) NOT NULL,
 BALANCE_CHF NUMBER(18,2) NOT NULL,
 FX_RATE NUMBER(12,6),
 IS_INSURED BOOLEAN DEFAULT FALSE,
 PRODUCT_COUNT INTEGER DEFAULT 1,
 ACCOUNT_TENURE_DAYS INTEGER,
 HAS_DIRECT_DEBIT BOOLEAN DEFAULT FALSE,
 IS_OPERATIONAL BOOLEAN DEFAULT FALSE,
 COUNTERPARTY_TYPE VARCHAR(50) NOT NULL,
 CUSTOMER_SEGMENT VARCHAR(50),
 ACCOUNT_STATUS VARCHAR(20) DEFAULT 'ACTIVE',
 INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
 PRIMARY KEY (ACCOUNT_ID, AS_OF_DATE)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily snapshot table of customer deposit balances enriched with regulatory attributes for FINMA LCR net cash outflow calculation per Circular 2015/2. Links to actual customer base (CRM_RAW_001) enabling cross-domain analytics between customer profiles and funding stability. Contains balance data, relationship indicators (product count, direct debit, tenure), and operational designations. Source data loaded from core banking systems via CSV extracts. Used for LCR denominator calculation with relationship discounts (-2% for 3+ products, -1% for direct debit) and tenure penalties (+5% for under 18 months). Supports funding stability analysis, customer segmentation by deposit behavior, and SNB monthly regulatory reporting. Critical operational data for Treasury funding decisions and deposit retention strategies.';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_HQLA_HOLDINGS
 DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
 COMMENT = 'Stage for HQLA holdings CSV files';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DEPOSIT_BALANCES
 DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
 COMMENT = 'Stage for deposit balances CSV files';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_HQLA_HOLDINGS
 USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 SCHEDULE = '60 MINUTE'
 WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_HQLA_FILES')
AS
 COPY INTO {{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS (
 HOLDING_ID, AS_OF_DATE, ASSET_TYPE, ISIN, SECURITY_NAME, CURRENCY,
 QUANTITY, MARKET_VALUE_CCY, MARKET_VALUE_CHF, FX_RATE, MATURITY_DATE,
 CREDIT_RATING, SMI_CONSTITUENT, HQLA_ELIGIBLE, PORTFOLIO_CODE, CUSTODIAN
 )
 FROM @LIQI_RAW_ST_HQLA_HOLDINGS
 FILE_FORMAT = (
 TYPE = 'CSV'
 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
 SKIP_HEADER = 1
 FIELD_DELIMITER = ','
 TRIM_SPACE = TRUE
 NULL_IF = ('NULL', 'null', '')
 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
 )
 PATTERN = '.*hqla_holdings.*\\.csv'
 ON_ERROR = 'CONTINUE';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_DEPOSIT_BALANCES
 USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 SCHEDULE = '60 MINUTE'
 WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DEPOSIT_FILES')
AS
 COPY INTO {{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES (
 ACCOUNT_ID, AS_OF_DATE, CUSTOMER_ID, DEPOSIT_TYPE, CURRENCY,
 BALANCE_CCY, BALANCE_CHF, FX_RATE, IS_INSURED, PRODUCT_COUNT,
 ACCOUNT_TENURE_DAYS, HAS_DIRECT_DEBIT, IS_OPERATIONAL, COUNTERPARTY_TYPE,
 CUSTOMER_SEGMENT, ACCOUNT_STATUS
 )
 FROM @LIQI_RAW_ST_DEPOSIT_BALANCES
 FILE_FORMAT = (
 TYPE = 'CSV'
 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
 SKIP_HEADER = 1
 FIELD_DELIMITER = ','
 TRIM_SPACE = TRUE
 NULL_IF = ('NULL', 'null', '')
 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
 )
 PATTERN = '.*deposit_balances.*\\.csv'
 ON_ERROR = 'CONTINUE';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_CLEANUP_AFTER_LOAD_HQLA_HOLDINGS
 USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 COMMENT = 'Automated stage cleanup AFTER HQLA holdings data load. Keeps last 5 files to manage storage costs.'
 AFTER {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_HQLA_HOLDINGS
AS
 CALL LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('LIQI_RAW_ST_HQLA_HOLDINGS', 5);

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_CLEANUP_AFTER_LOAD_DEPOSIT_BALANCES
 USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
 COMMENT = 'Automated stage cleanup AFTER deposit balances data load. Keeps last 5 files to manage storage costs.'
 AFTER {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_DEPOSIT_BALANCES
AS
 CALL LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('LIQI_RAW_ST_DEPOSIT_BALANCES', 5);
