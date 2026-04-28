DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY (
    ASSET_TYPE VARCHAR(50) PRIMARY KEY COMMENT 'Unique HQLA asset type code (e.g., CASH_SNB, GOVT_BOND_CHF). Used to classify treasury holdings and determine regulatory treatment. Key for joining with holdings data.',
    ASSET_NAME VARCHAR(200) NOT NULL COMMENT 'Descriptive name of the asset type for reporting and user interface display. Human-readable classification used in Treasury dashboards and FINMA submissions.',
    REGULATORY_LEVEL VARCHAR(10) NOT NULL COMMENT 'Basel III HQLA level: L1 (highest quality, 0% haircut), L2A (high quality, 15% haircut), L2B (acceptable quality, 50% haircut). Critical for LCR calculation and 40% cap rule application.',
    HAIRCUT_PCT NUMBER(5,2) NOT NULL COMMENT 'Regulatory haircut percentage (0%, 15%, or 50%) applied to market value per Basel III. Displayed in reports and used for compliance documentation. User-friendly representation of haircut factor.',
    HAIRCUT_FACTOR NUMBER(5,4) NOT NULL COMMENT 'Haircut multiplier for HQLA stock calculation (1.0000 for L1, 0.8500 for L2A, 0.5000 for L2B). Applied directly to market values: Weighted_Value = Market_Value × Haircut_Factor. Core calculation field for LCR numerator.',
    SNB_COORDINATE VARCHAR(20) COMMENT 'Swiss National Bank reporting coordinate for regulatory submission mapping (e.g., 1.1.1.1 for SNB reserves). Required for monthly LCR reports to SNB and FINMA data quality validation.',
    ELIGIBILITY_CRITERIA VARCHAR(500) COMMENT 'Business rules defining asset eligibility under FINMA Circular 2015/2. Used by Compliance for audit trails and by Treasury for pre-trade compliance checks. Documents regulatory requirements for each asset type.',
    MIN_CREDIT_RATING VARCHAR(10) COMMENT 'Minimum credit rating required for HQLA eligibility (e.g., AA- for foreign govt bonds). Used for automated eligibility screening and counterparty risk assessment. NULL if no rating requirement.',
    REQUIRES_SMI_CONSTITUENT BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating whether security must be in Swiss Market Index (SMI) for L2B equity eligibility. Used for automated equity screening and portfolio rebalancing decisions. TRUE only for EQUITY_SMI type.',
    IS_ACTIVE BOOLEAN DEFAULT TRUE COMMENT 'Active status flag for reference data versioning. FALSE when asset type is deprecated due to regulatory changes. Used to filter current rules for LCR calculation while maintaining audit history.',
    EFFECTIVE_DATE DATE NOT NULL COMMENT 'Date when this classification rule becomes effective. Supports regulatory change management and historical LCR recalculation. Critical for audit trail and time-travel queries.',
    END_DATE DATE COMMENT 'Date when this rule was superseded or deactivated. NULL for currently active rules. Used with EFFECTIVE_DATE for SCD Type 2 temporal logic and regulatory impact analysis.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp of record creation for data lineage tracking. Used for operational monitoring and compliance audit trails.'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Master reference table for FINMA LCR High-Quality Liquid Assets (HQLA) classification. Defines regulatory levels (L1, L2A, L2B), haircut factors (0%, 15%, 50%), and eligibility criteria per FINMA Circular 2015/2 and Basel III. Used by Treasury for liquidity buffer management, by Risk for LCR monitoring, and by Compliance for SNB regulatory submissions. Supports automated eligibility screening, portfolio composition analysis, and 40% cap rule enforcement. Critical reference data for daily LCR calculation and strategic liquidity planning.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES (
    DEPOSIT_TYPE VARCHAR(50) PRIMARY KEY COMMENT 'Unique deposit type code (e.g., RETAIL_STABLE_INSURED, CORPORATE_OPERATIONAL). Used to classify deposit accounts and determine stressed outflow assumptions. Key for joining with deposit balance data.',
    DEPOSIT_NAME VARCHAR(200) NOT NULL COMMENT 'Descriptive name of deposit type for reporting and user interface display. Human-readable classification used in Treasury dashboards, customer analytics, and FINMA submissions.',
    BASE_RUN_OFF_RATE NUMBER(5,4) NOT NULL COMMENT 'Base stressed run-off rate under Basel III 30-day scenario (0.03 to 1.00). Represents percentage of deposits assumed to be withdrawn during liquidity stress. Core input for LCR denominator calculation. Lower rates = more stable funding.',
    COUNTERPARTY_TYPE VARCHAR(50) NOT NULL COMMENT 'Counterparty classification: RETAIL (individuals), CORPORATE (businesses), FINANCIAL_INSTITUTION (banks/insurers). Determines regulatory treatment, concentration limits, and reporting segmentation. Critical for Basel III compliance.',
    SNB_COORDINATE VARCHAR(20) COMMENT 'Swiss National Bank reporting coordinate for outflow classification (e.g., 1.2.1.1 for retail stable). Required for monthly LCR regulatory submission to SNB and data quality reconciliation with FINMA templates.',
    ELIGIBILITY_CRITERIA VARCHAR(500) COMMENT 'Business rules for deposit type classification under FINMA Circular 2015/2. Documents criteria such as balance thresholds (CHF 100K), product counts (3+), operational status. Used by Compliance for audit and customer classification validation.',
    ALLOWS_RELATIONSHIP_DISCOUNT BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating whether relationship-based run-off discounts apply (-2% for 3+ products, -1% for direct debit). Used in outflow calculation logic. TRUE for stable retail deposits, FALSE for wholesale/institutional funding.',
    IS_INSURED BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating deposits within CHF 100K deposit insurance limit per Basel III. Insured deposits receive lower run-off rates due to perceived stability. Used for customer segmentation and deposit insurance reporting.',
    IS_OPERATIONAL BOOLEAN DEFAULT FALSE COMMENT 'Flag for operational deposits per Basel III (e.g., payroll, clearing accounts with lower run-off). Corporate operational deposits receive 25% run-off vs 40% for non-operational. Critical for correct LCR calculation.',
    IS_ACTIVE BOOLEAN DEFAULT TRUE COMMENT 'Active status flag for reference data versioning. FALSE when deposit type is deprecated due to regulatory or product changes. Used to filter current classification rules while maintaining audit history.',
    EFFECTIVE_DATE DATE NOT NULL COMMENT 'Date when this run-off rate rule becomes effective. Supports regulatory change management (e.g., Basel III revisions) and historical LCR recalculation. Critical for audit trail and time-travel analysis.',
    END_DATE DATE COMMENT 'Date when this rule was superseded or deactivated. NULL for currently active rules. Used with EFFECTIVE_DATE for SCD Type 2 temporal logic and regulatory impact analysis of rate changes.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp of record creation for data lineage tracking. Used for operational monitoring and compliance audit trails.'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Master reference table for FINMA LCR deposit classification and stressed run-off rate assumptions. Defines counterparty types (Retail/Corporate/FI), base run-off rates (3% to 100%), relationship discount eligibility, and operational deposit treatment per FINMA Circular 2015/2 and Basel III. Used by Treasury for funding stability analysis, by ALM for liquidity stress testing, and by Compliance for SNB regulatory submissions. Supports automated outflow calculation, customer relationship scoring, and deposit retention strategy optimization. Critical reference data for LCR denominator calculation and strategic funding planning.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS (
    HOLDING_ID VARCHAR(50) NOT NULL COMMENT 'Unique identifier for each HQLA holding position. Format: HOLD-YYYYMMDD-XXXXX for daily tracking. Used for position reconciliation, audit trail, and inventory management across treasury systems.',
    AS_OF_DATE DATE NOT NULL COMMENT 'Reporting date for HQLA position (daily COB snapshot). Used for time-series analysis, historical LCR trends, and regulatory reporting periods. Primary date dimension for LCR calculations.',
    ASSET_TYPE VARCHAR(50) NOT NULL COMMENT 'HQLA asset type code linking to eligibility rules (e.g., CASH_SNB, GOVT_BOND_CHF). Determines regulatory level (L1/L2A/L2B) and haircut treatment. Critical for automated HQLA classification and LCR stock calculation.',
    ISIN VARCHAR(12) COMMENT 'International Securities Identification Number per ISO 6166 standard. Used for security master reconciliation, price verification, and external reporting. NULL for cash positions. Enables integration with market data providers.',
    SECURITY_NAME VARCHAR(200) COMMENT 'Descriptive name of the security for reporting and portfolio analysis. Used in Treasury dashboards, FINMA submissions, and Board reports. Human-readable identifier for investment committee reviews.',
    CURRENCY VARCHAR(3) NOT NULL COMMENT 'Original currency of the HQLA holding per ISO 4217 (CHF, EUR, USD, GBP). Used for FX exposure analysis, currency diversification monitoring, and multi-currency liquidity management. Critical for FX risk reporting.',
    QUANTITY NUMBER(18,6) COMMENT 'Number of shares or units held (for equities, funds). NULL for bonds (face value) and cash. Used for position size tracking, concentration analysis, and SMI constituent verification. Enables unit-level portfolio analytics.',
    MARKET_VALUE_CCY NUMBER(18,2) NOT NULL COMMENT 'Market value in original currency at COB pricing. Source data for FX conversion and P&L attribution. Used for currency-level portfolio analysis and hedge effectiveness monitoring.',
    MARKET_VALUE_CHF NUMBER(18,2) NOT NULL COMMENT 'Market value converted to CHF base currency using FX_RATE. Primary value field for LCR calculation, portfolio aggregation, and regulatory reporting. All HQLA calculations performed in CHF per FINMA requirements.',
    FX_RATE NUMBER(12,6) COMMENT 'FX conversion rate used for CCY to CHF conversion. Links to REF_RAW_001.FX rates for reconciliation. Used for P&L attribution, FX sensitivity analysis, and audit trail. NULL when CURRENCY=CHF.',
    MATURITY_DATE DATE COMMENT 'Final maturity date for bonds and fixed-term instruments. Used for duration analysis, rollover planning, and maturity ladder reporting. NULL for equities and perpetual instruments. Critical for term structure management.',
    CREDIT_RATING VARCHAR(10) COMMENT 'Current credit rating from S&P, Moody\'s, or Fitch. Used for eligibility screening (min AA- for some asset types), credit risk monitoring, and rating migration analysis. Critical for maintaining HQLA status.',
    SMI_CONSTITUENT BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating whether security is Swiss Market Index constituent. Required TRUE for EQUITY_SMI type eligibility. Used for automated L2B equity screening and SMI rebalancing impact analysis.',
    HQLA_ELIGIBLE BOOLEAN DEFAULT TRUE COMMENT 'Flag indicating whether holding passes all FINMA eligibility criteria. FALSE triggers exclusion from LCR calculation and alerts Treasury. Used for pre-trade compliance checks and portfolio optimization.',
    INELIGIBILITY_REASON VARCHAR(500) COMMENT 'Explanation for HQLA exclusion when HQLA_ELIGIBLE=FALSE. Documents rating downgrades, maturity mismatches, or regulatory breaches. Used for audit trail, Compliance investigations, and remediation tracking.',
    PORTFOLIO_CODE VARCHAR(50) COMMENT 'Treasury portfolio identifier (e.g., TREASURY_LIQ, ALM_BUFFER). Used for portfolio segmentation, mandate compliance, and investment committee reporting. Links to portfolio strategy and risk limits.',
    CUSTODIAN VARCHAR(100) COMMENT 'Institution holding the securities (e.g., SIX SIS, Euroclear, Clearstream). Used for custodian risk concentration monitoring, settlement tracking, and operational due diligence. Critical for counterparty exposure management.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when record was loaded into Snowflake. Used for data lineage, SLA monitoring, and identifying late-arriving data. Critical for operational dashboards and data quality tracking.',
    PRIMARY KEY (HOLDING_ID, AS_OF_DATE)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily snapshot table of High-Quality Liquid Assets (HQLA) held by Treasury for liquidity buffer management per FINMA Circular 2015/2. Contains securities positions with market values, regulatory classifications, and eligibility flags. Source data loaded from treasury management systems via CSV extracts. Used for LCR numerator calculation (HQLA stock with haircuts), portfolio concentration analysis, and SNB monthly regulatory reporting. Supports real-time liquidity monitoring, investment committee reporting, and Basel III compliance. Critical operational data for Treasury decision-making and regulatory supervision.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES (
    ACCOUNT_ID VARCHAR(50) NOT NULL COMMENT 'Unique deposit account identifier linking to customer accounts. Format: CUST_XXXXX_DEP_XX for traceability. Used for account-level outflow analysis, customer relationship scoring, and referential integrity with CRM_RAW_001.',
    AS_OF_DATE DATE NOT NULL COMMENT 'Reporting date for deposit position (daily COB snapshot). Used for time-series funding analysis, LCR trend monitoring, and regulatory reporting periods. Primary date dimension for outflow calculations.',
    CUSTOMER_ID VARCHAR(50) NOT NULL COMMENT 'Foreign key to CRM_RAW_001.CRMI_RAW_TB_CUSTOMER for customer profile integration. Enables cross-domain analytics: deposit stability by account tier, risk classification impact on funding, and relationship banking effectiveness. Critical for customer-centric liquidity management.',
    DEPOSIT_TYPE VARCHAR(50) NOT NULL COMMENT 'Deposit type code linking to run-off rate rules (e.g., RETAIL_STABLE_INSURED, CORPORATE_OPERATIONAL). Determines base stressed outflow rate (3% to 100%) per Basel III. Critical for automated LCR denominator calculation.',
    CURRENCY VARCHAR(3) NOT NULL COMMENT 'Deposit currency per ISO 4217 (CHF, EUR, USD). Used for FX exposure analysis, currency concentration monitoring, and multi-currency funding management. Supports FX risk reporting and hedge strategy validation.',
    BALANCE_CCY NUMBER(18,2) NOT NULL COMMENT 'Deposit balance in original account currency. Source data for FX conversion and currency-level funding analysis. Used for customer statements, P&L attribution, and hedge effectiveness monitoring.',
    BALANCE_CHF NUMBER(18,2) NOT NULL COMMENT 'Deposit balance converted to CHF base currency using FX_RATE. Primary value field for LCR outflow calculation, funding aggregation, and regulatory reporting. All LCR calculations performed in CHF per FINMA requirements.',
    FX_RATE NUMBER(12,6) COMMENT 'FX conversion rate used for CCY to CHF conversion. Links to REF_RAW_001.FX rates for reconciliation. Used for P&L attribution, FX sensitivity analysis, and audit trail. NULL when CURRENCY=CHF.',
    IS_INSURED BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating balance within CHF 100K deposit insurance limit per Basel III. Insured deposits qualify for lower run-off rates (3-5%) due to perceived stability. Used for deposit insurance reporting and customer communication.',
    PRODUCT_COUNT INTEGER DEFAULT 1 COMMENT 'Number of active products held by customer for relationship discount eligibility. 3+ products qualify for -2% run-off discount. Used for relationship banking scoring, customer retention analytics, and cross-sell effectiveness measurement.',
    ACCOUNT_TENURE_DAYS INTEGER COMMENT 'Days since account opening for stability assessment. Accounts under 18 months incur +5% tenure penalty on run-off rate. Used for customer lifecycle analysis, churn prediction, and new account quality monitoring.',
    HAS_DIRECT_DEBIT BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating active direct debit mandate (salary, rent, utilities). Qualifies for -1% relationship discount. Proxy for operational relationship strength. Used for deposit retention strategy and payment behavior analysis.',
    IS_OPERATIONAL BOOLEAN DEFAULT FALSE COMMENT 'Corporate operational deposit designation per Basel III (payroll, clearing accounts). Operational deposits receive 25% run-off vs 40% non-operational. Must demonstrate operational necessity. Critical for correct LCR calculation and corporate relationship classification.',
    COUNTERPARTY_TYPE VARCHAR(50) NOT NULL COMMENT 'Counterparty classification: RETAIL (individuals), CORPORATE (businesses), FINANCIAL_INSTITUTION (banks/insurers). Determines regulatory treatment, concentration limits, and reporting segmentation. Critical for Basel III compliance and funding strategy.',
    CUSTOMER_SEGMENT VARCHAR(50) COMMENT 'Customer segment classification (MASS, AFFLUENT, PRIVATE, CORPORATE). Used for customer analytics, deposit stability analysis by segment, and relationship banking strategy. Enables targeted retention programs and pricing optimization.',
    ACCOUNT_STATUS VARCHAR(20) DEFAULT 'ACTIVE' COMMENT 'Account operational status (ACTIVE, DORMANT, CLOSED). Only ACTIVE accounts included in LCR calculation. Used for account lifecycle management, dormancy tracking, and customer reactivation campaigns.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when record was loaded into Snowflake. Used for data lineage, SLA monitoring, and identifying late-arriving data. Critical for operational dashboards and data quality tracking.',
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
    FROM @{{ rep_raw }}.LIQI_RAW_ST_HQLA_HOLDINGS
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
    FROM @{{ rep_raw }}.LIQI_RAW_ST_DEPOSIT_BALANCES
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
    CALL LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ rep_raw }}.LIQI_RAW_ST_HQLA_HOLDINGS', 5);

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_CLEANUP_AFTER_LOAD_DEPOSIT_BALANCES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER deposit balances data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_DEPOSIT_BALANCES
AS
    CALL LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ rep_raw }}.LIQI_RAW_ST_DEPOSIT_BALANCES', 5);
