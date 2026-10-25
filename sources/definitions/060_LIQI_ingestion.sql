/*
 * 060_LIQI_ingestion.sql
 * Liquidity raw ingestion: LCR, HQLA, deposits, stages, tasks
 */
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
    FX_RATE NUMBER(12,6) COMMENT 'FX conversion rate used for CCY to CHF conversion. Links to REF_RAW_v001.FX rates for reconciliation. Used for P&L attribution, FX sensitivity analysis, and audit trail. NULL when CURRENCY=CHF.',
    MATURITY_DATE DATE COMMENT 'Final maturity date for bonds and fixed-term instruments. Used for duration analysis, rollover planning, and maturity ladder reporting. NULL for equities and perpetual instruments. Critical for term structure management.',
    CREDIT_RATING VARCHAR(10) COMMENT 'Current credit rating from S&P, Moody\'s, or Fitch. Used for eligibility screening (min AA- for some asset types), credit risk monitoring, and rating migration analysis. Critical for maintaining HQLA status.',
    SMI_CONSTITUENT BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating whether security is Swiss Market Index constituent. Required TRUE for EQUITY_SMI type eligibility. Used for automated L2B equity screening and SMI rebalancing impact analysis.',
    HQLA_ELIGIBLE BOOLEAN DEFAULT TRUE COMMENT 'Flag indicating whether holding passes all FINMA eligibility criteria. FALSE triggers exclusion from LCR calculation and alerts Treasury. Used for pre-trade compliance checks and portfolio optimization.',
    INELIGIBILITY_REASON VARCHAR(500) COMMENT 'Explanation for HQLA exclusion when HQLA_ELIGIBLE=FALSE. Documents rating downgrades, maturity mismatches, or regulatory breaches. Used for audit trail, Compliance investigations, and remediation tracking.',
    PORTFOLIO_CODE VARCHAR(50) COMMENT 'Treasury portfolio identifier (e.g., TREASURY_LIQ, ALM_BUFFER). Used for portfolio segmentation, mandate compliance, and investment committee reporting. Links to portfolio strategy and risk limits.',
    CUSTODIAN VARCHAR(100) COMMENT 'Institution holding the securities (e.g., SIX SIS, Euroclear, Clearstream). Used for custodian risk concentration monitoring, settlement tracking, and operational due diligence. Critical for counterparty exposure management.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when record was loaded into Snowflake. Used for data lineage, SLA monitoring, and identifying late-arriving data. Critical for operational dashboards and data quality tracking.',
    BUSINESS_UNIT VARCHAR(30) COMMENT 'Owning business unit of the HQLA position (always TREASURY for the liquidity buffer). Used for the REQ-UI-02 business-unit filter and for allocating the liquidity buffer to the treasury book in drill-downs.',
    ISSUER_ID VARCHAR(50) COMMENT 'Issuer / obligor of the security or cash balance (FK to LIQI_RAW_TB_COUNTERPARTIES, e.g. ISS_SNB, ISS_CH, ISS_PFB). Provides the counterparty level of the REQ-ANL-02 drill-down path Asset Class -> Product -> Counterparty -> Contract for HQLA positions.',
    PRIMARY KEY (HOLDING_ID, AS_OF_DATE)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily snapshot table of High-Quality Liquid Assets (HQLA) held by Treasury for liquidity buffer management per FINMA Circular 2015/2. Contains securities positions with market values, regulatory classifications, and eligibility flags. Source data loaded from treasury management systems via CSV extracts. Used for LCR numerator calculation (HQLA stock with haircuts), portfolio concentration analysis, and SNB monthly regulatory reporting. Supports real-time liquidity monitoring, investment committee reporting, and Basel III compliance. Critical operational data for Treasury decision-making and regulatory supervision.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES (
    ACCOUNT_ID VARCHAR(50) NOT NULL COMMENT 'Unique deposit account identifier linking to customer accounts. Format: CUST_XXXXX_DEP_XX for traceability. Used for account-level outflow analysis, customer relationship scoring, and referential integrity with CRM_RAW_v001.',
    AS_OF_DATE DATE NOT NULL COMMENT 'Reporting date for deposit position (daily COB snapshot). Used for time-series funding analysis, LCR trend monitoring, and regulatory reporting periods. Primary date dimension for outflow calculations.',
    CUSTOMER_ID VARCHAR(50) NOT NULL COMMENT 'Foreign key to CRM_RAW_v001.CRMI_RAW_TB_CUSTOMER for customer profile integration. Enables cross-domain analytics: deposit stability by account tier, risk classification impact on funding, and relationship banking effectiveness. Critical for customer-centric liquidity management.',
    DEPOSIT_TYPE VARCHAR(50) NOT NULL COMMENT 'Deposit type code linking to run-off rate rules (e.g., RETAIL_STABLE_INSURED, CORPORATE_OPERATIONAL). Determines base stressed outflow rate (3% to 100%) per Basel III. Critical for automated LCR denominator calculation.',
    CURRENCY VARCHAR(3) NOT NULL COMMENT 'Deposit currency per ISO 4217 (CHF, EUR, USD). Used for FX exposure analysis, currency concentration monitoring, and multi-currency funding management. Supports FX risk reporting and hedge strategy validation.',
    BALANCE_CCY NUMBER(18,2) NOT NULL COMMENT 'Deposit balance in original account currency. Source data for FX conversion and currency-level funding analysis. Used for customer statements, P&L attribution, and hedge effectiveness monitoring.',
    BALANCE_CHF NUMBER(18,2) NOT NULL COMMENT 'Deposit balance converted to CHF base currency using FX_RATE. Primary value field for LCR outflow calculation, funding aggregation, and regulatory reporting. All LCR calculations performed in CHF per FINMA requirements.',
    FX_RATE NUMBER(12,6) COMMENT 'FX conversion rate used for CCY to CHF conversion. Links to REF_RAW_v001.FX rates for reconciliation. Used for P&L attribution, FX sensitivity analysis, and audit trail. NULL when CURRENCY=CHF.',
    IS_INSURED BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating balance within CHF 100K deposit insurance limit per Basel III. Insured deposits qualify for lower run-off rates (3-5%) due to perceived stability. Used for deposit insurance reporting and customer communication.',
    PRODUCT_COUNT INTEGER DEFAULT 1 COMMENT 'Number of active products held by customer for relationship discount eligibility. 3+ products qualify for -2% run-off discount. Used for relationship banking scoring, customer retention analytics, and cross-sell effectiveness measurement.',
    ACCOUNT_TENURE_DAYS INTEGER COMMENT 'Days since account opening for stability assessment. Accounts under 18 months incur +5% tenure penalty on run-off rate. Used for customer lifecycle analysis, churn prediction, and new account quality monitoring.',
    HAS_DIRECT_DEBIT BOOLEAN DEFAULT FALSE COMMENT 'Flag indicating active direct debit mandate (salary, rent, utilities). Qualifies for -1% relationship discount. Proxy for operational relationship strength. Used for deposit retention strategy and payment behavior analysis.',
    IS_OPERATIONAL BOOLEAN DEFAULT FALSE COMMENT 'Corporate operational deposit designation per Basel III (payroll, clearing accounts). Operational deposits receive 25% run-off vs 40% non-operational. Must demonstrate operational necessity. Critical for correct LCR calculation and corporate relationship classification.',
    COUNTERPARTY_TYPE VARCHAR(50) NOT NULL COMMENT 'Counterparty classification: RETAIL (individuals), CORPORATE (businesses), FINANCIAL_INSTITUTION (banks/insurers). Determines regulatory treatment, concentration limits, and reporting segmentation. Critical for Basel III compliance and funding strategy.',
    CUSTOMER_SEGMENT VARCHAR(50) COMMENT 'Customer segment classification (MASS, AFFLUENT, PRIVATE, CORPORATE). Used for customer analytics, deposit stability analysis by segment, and relationship banking strategy. Enables targeted retention programs and pricing optimization.',
    ACCOUNT_STATUS VARCHAR(20) DEFAULT 'ACTIVE' COMMENT 'Account operational status (ACTIVE, DORMANT, CLOSED). Only ACTIVE accounts included in LCR calculation. Used for account lifecycle management, dormancy tracking, and customer reactivation campaigns.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when record was loaded into Snowflake. Used for data lineage, SLA monitoring, and identifying late-arriving data. Critical for operational dashboards and data quality tracking.',
    BUSINESS_UNIT VARCHAR(30) COMMENT 'Owning business unit of the deposit (RETAIL, PRIVATE_BANKING, CORPORATE, TREASURY for interbank deposits). Used for the REQ-UI-02 business-unit filter, funding-mix analysis by segment and allocation of stressed outflows to business units.',
    COUNTERPARTY_ID VARCHAR(50) COMMENT 'Counterparty of the deposit (FK to LIQI_RAW_TB_COUNTERPARTIES; equals CUSTOMER_ID for retail customers, CP_CORP_* / CP_FI_* for wholesale clients). Used to show client names in the Top-10 variance drivers (REQ-UI-03) and the counterparty level of the drill-down.',
    PRIMARY KEY (ACCOUNT_ID, AS_OF_DATE)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily snapshot table of customer deposit balances enriched with regulatory attributes for FINMA LCR net cash outflow calculation per Circular 2015/2. Links to actual customer base (CRM_RAW_v001) enabling cross-domain analytics between customer profiles and funding stability. Contains balance data, relationship indicators (product count, direct debit, tenure), and operational designations. Source data loaded from core banking systems via CSV extracts. Used for LCR denominator calculation with relationship discounts (-2% for 3+ products, -1% for direct debit) and tenure penalties (+5% for under 18 months). Supports funding stability analysis, customer segmentation by deposit behavior, and SNB monthly regulatory reporting. Critical operational data for Treasury funding decisions and deposit retention strategies.';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_HQLA_HOLDINGS
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    COMMENT = 'Internal landing stage for daily HQLA holdings CSV extracts (hqla_holdings_YYYYMMDD.csv) from the treasury system. Monitored by stream LIQI_RAW_SM_HQLA_FILES; loaded hourly by task LIQI_RAW_TK_LOAD_HQLA_HOLDINGS; last 5 files kept by the cleanup task.';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DEPOSIT_BALANCES
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    COMMENT = 'Internal landing stage for daily deposit balance CSV extracts (deposit_balances_YYYYMMDD.csv) from core banking. Monitored by stream LIQI_RAW_SM_DEPOSIT_FILES; loaded hourly by task LIQI_RAW_TK_LOAD_DEPOSIT_BALANCES; last 5 files kept by the cleanup task.';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_HQLA_HOLDINGS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly load of new HQLA holdings CSV files from LIQI_RAW_ST_HQLA_HOLDINGS into LIQI_RAW_TB_HQLA_HOLDINGS (runs only when stream LIQI_RAW_SM_HQLA_FILES has data, ON_ERROR=CONTINUE). Feeds the LCR numerator.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_HQLA_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS (
        HOLDING_ID, AS_OF_DATE, ASSET_TYPE, ISIN, SECURITY_NAME, CURRENCY,
        QUANTITY, MARKET_VALUE_CCY, MARKET_VALUE_CHF, FX_RATE, MATURITY_DATE,
        CREDIT_RATING, SMI_CONSTITUENT, HQLA_ELIGIBLE, PORTFOLIO_CODE, CUSTODIAN,
        INELIGIBILITY_REASON, BUSINESS_UNIT, ISSUER_ID
    )
    FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_HQLA_HOLDINGS
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
    COMMENT = 'Hourly load of new deposit balance CSV files from LIQI_RAW_ST_DEPOSIT_BALANCES into LIQI_RAW_TB_DEPOSIT_BALANCES (runs only when stream LIQI_RAW_SM_DEPOSIT_FILES has data, ON_ERROR=CONTINUE). Feeds deposit run-off outflows of the LCR denominator.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DEPOSIT_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES (
        ACCOUNT_ID, AS_OF_DATE, CUSTOMER_ID, DEPOSIT_TYPE, CURRENCY,
        BALANCE_CCY, BALANCE_CHF, FX_RATE, IS_INSURED, PRODUCT_COUNT,
        ACCOUNT_TENURE_DAYS, HAS_DIRECT_DEBIT, IS_OPERATIONAL, COUNTERPARTY_TYPE,
        CUSTOMER_SEGMENT, ACCOUNT_STATUS, BUSINESS_UNIT, COUNTERPARTY_ID
    )
    FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DEPOSIT_BALANCES
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
    STARTED
AS
    CALL LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_HQLA_HOLDINGS', 5);

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_CLEANUP_AFTER_LOAD_DEPOSIT_BALANCES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER deposit balances data load. Keeps last 5 files to manage storage costs.'
    AFTER {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_DEPOSIT_BALANCES
    STARTED
AS
    CALL LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DEPOSIT_BALANCES', 5);

-- ============================================================
-- LCR BOOK EXTENSION: counterparties, contracts, cashflows, events, rules
-- (REQ-ING-01/02, REQ-MAP-01, REQ-ENG-01..05, REQ-DQ-01)
-- ============================================================

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_COUNTERPARTIES (
    COUNTERPARTY_ID VARCHAR(50) NOT NULL COMMENT 'Counterparty key. Retail = CRM customer_id (links to CRMI_RAW_TB_CUSTOMER), CP_CORP_* = corporate / SME clients, CP_FI_* = banks and other financial institutions, ISS_* = HQLA issuers (SNB, Confederation, cantons, Pfandbrief institutes). Join key for deposits, contracts and holdings.',
    COUNTERPARTY_NAME VARCHAR(200) COMMENT 'Legal or display name of the counterparty shown in drill-downs and the Top-10 variance driver widget (REQ-UI-03), e.g. Alpenbau Holding AG. Retail customers carry a pseudonymised name.',
    SECTOR VARCHAR(30) NOT NULL COMMENT 'Basel III / FINMA counterparty sector: RETAIL, SME, CORPORATE, FINANCIAL, SOVEREIGN, CENTRAL_BANK. Drives run-off, inflow and drawdown rates via LIQI_RAW_TB_BEHAVIORAL_RULES (e.g. 50% inflow for non-financials, 100% for financials).',
    COUNTRY VARCHAR(2) COMMENT 'ISO 3166 alpha-2 country of domicile of the counterparty. Used for geographic concentration analysis of funding and lending exposures and for cross-border liquidity reporting.',
    RATING VARCHAR(10) COMMENT 'External credit rating of the counterparty (S&P scale, e.g. A-, BBB+). Used for the rating filter (REQ-UI-02), credit quality of inflows and counterparty concentration monitoring. NULL for retail customers.',
    CUSTOMER_ID VARCHAR(50) COMMENT 'Link to the CRM customer master (CRMI_RAW_TB_CUSTOMER.CUSTOMER_ID) for retail counterparties; NULL for wholesale counterparties and issuers. Enables cross-domain customer 360 / liquidity analytics.',
    BUSINESS_UNIT VARCHAR(30) COMMENT 'Business unit owning the client relationship: RETAIL, PRIVATE_BANKING, CORPORATE or TREASURY. Used for business-unit filtering and allocation of liquidity metrics (REQ-UI-02).',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the record was loaded into Snowflake. Used for lineage and to keep only the latest version per COUNTERPARTY_ID when the master file is reloaded.'
)
CHANGE_TRACKING = TRUE
COMMENT = 'Counterparty master for the LCR book (SPOT-like master data, REQ-ING-01). One row per counterparty (latest load wins). Covers retail customers (linked to CRM), corporates, SMEs, financial institutions and HQLA issuers. Provides sector classification for regulatory run-off / inflow treatment, client names for root-cause analysis and the counterparty level of the REQ-ANL-02 drill-down. Loaded from counterparties.csv via stage LIQI_RAW_ST_COUNTERPARTIES.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_CONTRACTS (
    CONTRACT_ID VARCHAR(50) NOT NULL COMMENT 'Persistent contract identifier (CTR_nnnnnn) of a loan, credit / liquidity facility or interbank contract - the atomic level of the REQ-ANL-02 drill-down and the driver key in the variance attribution.',
    AS_OF_DATE DATE NOT NULL COMMENT 'Snapshot (cut-off) date of the contract state (daily COB). Partitioning dimension for DoD / WoW / MoM comparisons (REQ-ANL-01).',
    COUNTERPARTY_ID VARCHAR(50) NOT NULL COMMENT 'Counterparty of the contract (FK to LIQI_RAW_TB_COUNTERPARTIES). Determines sector-specific inflow / drawdown rates and the client name shown in root-cause analysis.',
    PRODUCT_TYPE VARCHAR(30) NOT NULL COMMENT 'Source product code: LOAN_RETAIL, LOAN_CORP, INTERBANK_LOAN, CREDIT_FACILITY, LIQUIDITY_FACILITY, INTERBANK_BORROWING. Unknown codes (e.g. NEW_DERIVATIVE_X) are quarantined as UNMAPPED_ASSET_CLASS exceptions (REQ-DQ-01, TC-VAL-03).',
    BUSINESS_UNIT VARCHAR(30) COMMENT 'Business unit booking the contract (RETAIL, CORPORATE, TREASURY). Used for the business-unit filter (REQ-UI-02) and allocation of inflows / facility outflows.',
    CURRENCY VARCHAR(3) NOT NULL COMMENT 'Contract currency per ISO 4217. Converted to CHF with the latest available daily FX rate (daily end-of-day rate from REFI_RAW_TB_FX_RATES joined on DATE = AS_OF_DATE, cross via USD). Currencies without an FX rate (e.g. SEK) are quarantined as MISSING_FX (REQ-ING-03, REQ-DQ-01).',
    RATING VARCHAR(10) COMMENT 'Counterparty rating at contract level (S&P scale). Used for rating filters and credit quality analysis of contractual inflows.',
    START_DATE DATE COMMENT 'Contract start / last roll-over date. A START_DATE after MATURITY_DATE is quarantined as INVALID_DATE (REQ-DQ-01).',
    MATURITY_DATE DATE COMMENT 'Contractual final maturity / next roll-over date. Contracts maturing within 30 days generate LCR inflows (assets) or outflows (interbank borrowing); also drives the maturity ladder (REQ-ENG-01).',
    OUTSTANDING_CCY NUMBER(18,2) COMMENT 'Drawn / outstanding principal in contract currency. Source of principal cashflows; zero for pure undrawn facilities.',
    UNDRAWN_CCY NUMBER(18,2) COMMENT 'Committed but undrawn facility amount in contract currency. Basis for LCR drawdown outflows (5% retail, 10% corporate credit, 30% corporate liquidity, 40% banks, 100% other financials per FINMA LiqV).',
    INTEREST_RATE NUMBER(8,5) COMMENT 'Annual contractual interest rate as decimal (e.g. 0.025 = 2.5%). Used to derive interest cashflows in the cashflow schedule.',
    IS_PERFORMING BOOLEAN COMMENT 'Performing status of the exposure. Non-performing assets receive a 0% inflow rate in the LCR (Basel III para 142).',
    COUNTERPARTY_SECTOR VARCHAR(30) COMMENT 'Sector of the counterparty at booking (RETAIL, SME, CORPORATE, FINANCIAL). Key, together with PRODUCT_TYPE, into product mapping and behavioral rules.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the record was loaded into Snowflake. Used for lineage, SLA monitoring and identification of late-arriving snapshots.',
    PRIMARY KEY (CONTRACT_ID, AS_OF_DATE)
)
CLUSTER BY (AS_OF_DATE)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily snapshots of loans, credit / liquidity facilities and interbank contracts (SPOT-like transactions, REQ-ING-01/02). Grain: one row per CONTRACT_ID x AS_OF_DATE. Provides drawn and undrawn amounts, maturities and performing status for LCR inflows, facility drawdown outflows and interbank funding outflows, plus the atomic contract level for drill-down and variance root-cause analysis. Loaded hourly from contracts_YYYYMMDD.csv via stage LIQI_RAW_ST_CONTRACTS.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_CASHFLOWS (
    AS_OF_DATE DATE NOT NULL COMMENT 'Snapshot (cut-off) date of the cashflow projection. Each snapshot contains the full contractual schedule for the next 365 days as seen on that date.',
    CONTRACT_ID VARCHAR(50) NOT NULL COMMENT 'Contract generating the cashflow (FK to LIQI_RAW_TB_CONTRACTS on CONTRACT_ID + AS_OF_DATE).',
    CF_DATE DATE NOT NULL COMMENT 'Contractual payment date. DATEDIFF(day, AS_OF_DATE, CF_DATE) assigns the flow to a maturity bucket (O/N, 2-7D, 8-30D, 1-3M, 3-6M, 6-12M, >1Y; REQ-ENG-01); flows within 30 days enter the LCR.',
    CF_TYPE VARCHAR(20) COMMENT 'Cashflow type: PRINCIPAL (repayment / amortisation) or INTEREST. Both count towards contractual LCR inflows / outflows.',
    DIRECTION VARCHAR(3) COMMENT 'Cashflow direction from the bank perspective: IN = receivable (loans, interbank placements), OUT = payable (interbank borrowing).',
    CURRENCY VARCHAR(3) COMMENT 'Cashflow currency per ISO 4217 (same as contract currency); converted to CHF with the end-of-day FX rate of the snapshot date.',
    AMOUNT_CCY NUMBER(18,2) COMMENT 'Cashflow amount in original currency (always positive; sign given by DIRECTION).',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the record was loaded into Snowflake. Used for lineage and load monitoring.'
)
CLUSTER BY (AS_OF_DATE)
CHANGE_TRACKING = TRUE
COMMENT = 'Daily contractual cashflow schedules per contract (REQ-ING-02, REQ-ENG-01). Grain: one row per AS_OF_DATE x CONTRACT_ID x CF_DATE x CF_TYPE. Basis for the maturity ladder / liquidity gap and for 30-day LCR inflows and interbank funding outflows. Snapshot partitioning by AS_OF_DATE supports cut-off comparisons. Loaded hourly from cashflows_YYYYMMDD.csv via stage LIQI_RAW_ST_CASHFLOWS.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_EVENTS (
    EVENT_ID VARCHAR(20) NOT NULL COMMENT 'Identifier of the showcase liquidity event (EVT_01..EVT_05).',
    EVENT_DATE DATE NOT NULL COMMENT 'Business date on which the event hits the balance sheet; the LCR change between the previous day and this date is attributed to the event.',
    EVENT_TYPE VARCHAR(30) COMMENT 'Event category: DEPOSIT_WITHDRAWAL, MARKET_STRESS, RECOVERY, LOAN_INFLOW. Used for chart annotations and agent narratives.',
    DESCRIPTION VARCHAR(500) COMMENT 'Business description of the event as shown in the app and used by the liquidity agent to explain LCR movements.',
    COUNTERPARTY_ID VARCHAR(50) COMMENT 'Counterparty driving the event (FK to LIQI_RAW_TB_COUNTERPARTIES).',
    CONTRACT_ID VARCHAR(50) COMMENT 'Position (deposit account or contract) expected to be the top client variance driver on EVENT_DATE (ground truth for TC-VAL-04).',
    AMOUNT_CHF NUMBER(18,2) COMMENT 'Nominal size of the event in CHF (e.g. 50,000,000 for the Alpenbau withdrawal).',
    EXPECTED_DIRECTION VARCHAR(4) COMMENT 'Expected direction of the LCR move on EVENT_DATE: UP or DOWN. Validated by sqlunit/lcr_validation.sql.',
    EXPECTED_MIN_DELTA_PP NUMBER(8,2) COMMENT 'Minimum expected absolute day-over-day LCR change in percentage points on EVENT_DATE. Validated by sqlunit/lcr_validation.sql.',
    INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'UTC timestamp when the record was loaded; latest load per EVENT_ID wins.'
)
COMMENT = 'Deterministic showcase events injected by the LCR generator (deposit withdrawals, market stress, recovery, loan inflows). Single source of truth for chart annotations, validation tests (TC-VAL-04) and the liquidity agent root-cause narrative. Loaded from lcr_events.csv via stage LIQI_RAW_ST_EVENTS.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_PRODUCT_MAPPING (
    PRODUCT_TYPE VARCHAR(50) NOT NULL COMMENT 'Source product / asset / deposit type code as delivered by the source systems (holdings ASSET_TYPE, deposits DEPOSIT_TYPE, contracts PRODUCT_TYPE).',
    SOURCE_SYSTEM VARCHAR(20) NOT NULL COMMENT 'Source feed the code belongs to: HOLDINGS, DEPOSITS or CONTRACTS.',
    ASSET_CLASS VARCHAR(40) NOT NULL COMMENT 'Multi-level LCR asset class (REQ-MAP-01): HQLA_L1_CASH, HQLA_L1_SOVEREIGN, HQLA_L2A, HQLA_L2B, RETAIL_DEPOSITS, WHOLESALE_DEPOSITS, INTERBANK_FUNDING, LOANS, INTERBANK_PLACEMENTS, COMMITTED_FACILITIES. Top level of the drill-down path.',
    PRODUCT_GROUP VARCHAR(40) NOT NULL COMMENT 'Second drill-down level (e.g. SNB reserves, Government bonds, Covered bonds, Retail stable deposits, Corporate loans). Business-friendly grouping of product types.',
    LCR_TREATMENT VARCHAR(20) NOT NULL COMMENT 'How the product enters the LCR: HQLA (numerator), OUTFLOW (run-off), INFLOW (30-day contractual inflow), FACILITY (undrawn drawdown outflow) or NONE.',
    HQLA_LEVEL VARCHAR(5) COMMENT 'Basel III HQLA level for HQLA products (L1, L2A, L2B); NULL otherwise.',
    DESCRIPTION VARCHAR(500) COMMENT 'Business description of the mapping rule and its regulatory basis (FINMA LiqV / Circular 2015/2).',
    IS_ACTIVE BOOLEAN DEFAULT TRUE COMMENT 'Active flag; inactive mappings are treated as unmapped and quarantined.',
    PRIMARY KEY (PRODUCT_TYPE, SOURCE_SYSTEM)
)
COMMENT = 'Multi-taxonomy mapping of source product codes to LCR asset classes, product groups and LCR treatment (REQ-MAP-01). Any product code without an active mapping is routed to the exceptions table as UNMAPPED_ASSET_CLASS (REQ-DQ-01). Seeded by post_deploy.sql.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_BEHAVIORAL_RULES (
    PRODUCT_TYPE VARCHAR(50) NOT NULL COMMENT 'Product / deposit / asset type the rule applies to (see LIQI_RAW_TB_PRODUCT_MAPPING).',
    COUNTERPARTY_SECTOR VARCHAR(30) NOT NULL COMMENT 'Counterparty sector the rule applies to (RETAIL, SME, CORPORATE, FINANCIAL, FINANCIAL_INSTITUTION, ALL for HQLA).',
    STRESS_SCENARIO VARCHAR(20) NOT NULL COMMENT 'Scenario: BASE (regulatory FINMA LCR), BANK_STRESS (idiosyncratic), MARKET_STRESS (systemic), COMBINED (REQ-ENG-05). BASE is the reported LCR; others are internal stress tests.',
    RUNOFF_RATE NUMBER(6,4) COMMENT 'Base run-off rate (decimal) for deposit / funding outflows in this scenario, before account-level relationship discounts and tenure penalties (REQ-ENG-03).',
    INFLOW_RATE NUMBER(6,4) COMMENT 'Inflow rate (decimal) applied to contractual inflows within 30 days (50% non-financial, 100% financial in BASE; reduced in stress). Total inflows are capped at 75% of outflows.',
    DRAWDOWN_RATE NUMBER(6,4) COMMENT 'Drawdown rate (decimal) applied to undrawn committed facilities (BASE: 5% retail, 10% corporate credit, 30% corporate liquidity, 40% banks).',
    HAIRCUT_ADDON_PCT NUMBER(6,2) COMMENT 'Additional haircut in percentage points on top of the regulatory haircut for HQLA products in stress scenarios (0 in BASE) (REQ-ENG-04).',
    DECAY_PROFILE_DAYS INTEGER COMMENT 'Behavioral maturity horizon in days for non-maturity deposits: balances are spread across maturity buckets up to this horizon in the liquidity gap view (REQ-ENG-02).',
    DECAY_METHOD VARCHAR(20) COMMENT 'Behavioral decay method for non-maturity products: LINEAR (equal daily run-down) or NONE (contractual only).',
    SIM_CATEGORY VARCHAR(30) COMMENT 'Component category used by the scenario simulator (e.g. RETAIL_STABLE, CORP_NON_OPERATIONAL, FACILITIES, INFLOW_NONFIN, L2B) to apply what-if multipliers.',
    RULE_DESCRIPTION VARCHAR(500) COMMENT 'Business explanation and regulatory reference of the rule (FINMA LiqV Annex 2/3, Basel III LCR).',
    IS_ACTIVE BOOLEAN DEFAULT TRUE COMMENT 'Active flag for rule versioning; only active rules are used in calculations.',
    EFFECTIVE_DATE DATE COMMENT 'Date from which the rule is effective (regulatory change management).',
    PRIMARY KEY (PRODUCT_TYPE, COUNTERPARTY_SECTOR, STRESS_SCENARIO)
)
COMMENT = 'Scenario-specific behavioral and regulatory rule set for the LCR engine (spec 3.1, REQ-ENG-02/03/04/05): run-off, inflow and drawdown rates, stress haircut add-ons, decay profiles and simulator categories per product x counterparty sector x scenario. BASE rows implement FINMA LiqV; stress rows are illustrative internal stress assumptions. Seeded by post_deploy.sql.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_MATURITY_BUCKETS (
    BUCKET_CODE VARCHAR(10) NOT NULL COMMENT 'Maturity bucket code: ON, D2_7, D8_30, M1_3, M3_6, M6_12, Y1P.',
    BUCKET_NAME VARCHAR(30) NOT NULL COMMENT 'Display name of the bucket (Overnight, 2-7 days, 8-30 days, 1-3 months, 3-6 months, 6-12 months, >1 year).',
    FROM_DAY INTEGER NOT NULL COMMENT 'Inclusive lower bound in days from the snapshot date.',
    TO_DAY INTEGER NOT NULL COMMENT 'Inclusive upper bound in days from the snapshot date.',
    SORT_ORDER INTEGER NOT NULL COMMENT 'Display order of the bucket in maturity ladders and gap charts.',
    IN_LCR_HORIZON BOOLEAN NOT NULL COMMENT 'TRUE when the bucket lies within the 30-day LCR stress horizon.',
    PRIMARY KEY (BUCKET_CODE)
)
COMMENT = 'Standardised maturity buckets for the cashflow engine and liquidity gap analysis (REQ-ENG-01): Overnight, 2-7D, 8-30D, 1-3M, 3-6M, 6-12M, >1Y. Seeded by post_deploy.sql.';

DEFINE TABLE {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_SCENARIOS (
    SCENARIO_CODE VARCHAR(20) NOT NULL COMMENT 'Scenario code used across the LCR engine: BASE, BANK_STRESS, MARKET_STRESS, COMBINED.',
    SCENARIO_NAME VARCHAR(100) NOT NULL COMMENT 'Display name of the scenario in the app, semantic views and agent answers.',
    DESCRIPTION VARCHAR(500) COMMENT 'Business narrative of the scenario assumptions.',
    IS_REGULATORY BOOLEAN NOT NULL COMMENT 'TRUE for the regulatory FINMA LCR (BASE); FALSE for internal stress tests.',
    SORT_ORDER INTEGER NOT NULL COMMENT 'Display order of the scenario.',
    PRIMARY KEY (SCENARIO_CODE)
)
COMMENT = 'Catalogue of liquidity stress scenarios evaluated simultaneously by the LCR engine (REQ-ENG-05). Seeded by post_deploy.sql.';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_COUNTERPARTIES
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    COMMENT = 'Internal landing stage for the counterparty master file (counterparties.csv). Monitored by stream LIQI_RAW_SM_COUNTERPARTY_FILES and loaded by task LIQI_RAW_TK_LOAD_COUNTERPARTIES.';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_CONTRACTS
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    COMMENT = 'Internal landing stage for daily contract snapshots (contracts_YYYYMMDD.csv). Monitored by stream LIQI_RAW_SM_CONTRACT_FILES and loaded hourly by task LIQI_RAW_TK_LOAD_CONTRACTS.';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_CASHFLOWS
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    COMMENT = 'Internal landing stage for daily contractual cashflow schedules (cashflows_YYYYMMDD.csv). Monitored by stream LIQI_RAW_SM_CASHFLOW_FILES and loaded hourly by task LIQI_RAW_TK_LOAD_CASHFLOWS.';

DEFINE STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_EVENTS
    DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)
    COMMENT = 'Internal landing stage for the showcase event file (lcr_events.csv). Monitored by stream LIQI_RAW_SM_EVENT_FILES and loaded by task LIQI_RAW_TK_LOAD_EVENTS.';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_COUNTERPARTIES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly load of the counterparty master from LIQI_RAW_ST_COUNTERPARTIES into LIQI_RAW_TB_COUNTERPARTIES when stream LIQI_RAW_SM_COUNTERPARTY_FILES has data. Run first: deposits, contracts and holdings reference counterparties.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_COUNTERPARTY_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_COUNTERPARTIES (
        COUNTERPARTY_ID, COUNTERPARTY_NAME, SECTOR, COUNTRY, RATING, CUSTOMER_ID, BUSINESS_UNIT
    )
    FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_COUNTERPARTIES
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = ','
                   TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '') ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
    PATTERN = '.*counterparties.*\\.csv'
    ON_ERROR = 'CONTINUE';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_EVENTS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly load of showcase events from LIQI_RAW_ST_EVENTS into LIQI_RAW_TB_EVENTS when stream LIQI_RAW_SM_EVENT_FILES has data. Events drive chart annotations, validation tests and agent narratives.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_EVENT_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_EVENTS (
        EVENT_ID, EVENT_DATE, EVENT_TYPE, DESCRIPTION, COUNTERPARTY_ID, CONTRACT_ID, AMOUNT_CHF,
        EXPECTED_DIRECTION, EXPECTED_MIN_DELTA_PP
    )
    FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_EVENTS
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = ','
                   TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '') ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
    PATTERN = '.*lcr_events.*\\.csv'
    ON_ERROR = 'CONTINUE';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_CONTRACTS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly load of daily contract snapshots from LIQI_RAW_ST_CONTRACTS into LIQI_RAW_TB_CONTRACTS when stream LIQI_RAW_SM_CONTRACT_FILES has data (ON_ERROR=CONTINUE). Feeds LCR inflows, facility outflows and interbank funding outflows.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_CONTRACT_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_CONTRACTS (
        CONTRACT_ID, AS_OF_DATE, COUNTERPARTY_ID, PRODUCT_TYPE, BUSINESS_UNIT, CURRENCY, RATING,
        START_DATE, MATURITY_DATE, OUTSTANDING_CCY, UNDRAWN_CCY, INTEREST_RATE, IS_PERFORMING, COUNTERPARTY_SECTOR
    )
    FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_CONTRACTS
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = ','
                   TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '') ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
    PATTERN = '.*contracts_.*\\.csv'
    ON_ERROR = 'CONTINUE';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_CASHFLOWS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly load of daily cashflow schedules from LIQI_RAW_ST_CASHFLOWS into LIQI_RAW_TB_CASHFLOWS when stream LIQI_RAW_SM_CASHFLOW_FILES has data (ON_ERROR=CONTINUE). Feeds the maturity ladder and 30-day contractual LCR flows.'
    WHEN SYSTEM$STREAM_HAS_DATA('{{ db }}.{{ rep_raw }}.LIQI_RAW_SM_CASHFLOW_FILES')
    STARTED
AS
    COPY INTO {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_CASHFLOWS (
        AS_OF_DATE, CONTRACT_ID, CF_DATE, CF_TYPE, DIRECTION, CURRENCY, AMOUNT_CCY
    )
    FROM @{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_CASHFLOWS
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = ','
                   TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '') ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
    PATTERN = '.*cashflows_.*\\.csv'
    ON_ERROR = 'CONTINUE';

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_CLEANUP_AFTER_LOAD_CONTRACTS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER contract snapshot load. Keeps the last 5 files in LIQI_RAW_ST_CONTRACTS to manage storage costs.'
    AFTER {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_CONTRACTS
    STARTED
AS
    CALL LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_CONTRACTS', 5);

DEFINE TASK {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_CLEANUP_AFTER_LOAD_CASHFLOWS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Automated stage cleanup AFTER cashflow schedule load. Keeps the last 5 files in LIQI_RAW_ST_CASHFLOWS to manage storage costs.'
    AFTER {{ db }}.{{ rep_raw }}.LIQI_RAW_TK_LOAD_CASHFLOWS
    STARTED
AS
    CALL LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N('{{ db }}.{{ rep_raw }}.LIQI_RAW_ST_CASHFLOWS', 5);

-- ============================================================
-- PROCEDURES (moved from post_deploy.sql)
-- ============================================================

DEFINE PROCEDURE {{ db }}.{{ rep_raw }}.LIQI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N (
    STAGE_NAME STRING,
    KEEP_N     NUMBER
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Generic stage cleanup utility for {{ rep_raw }} schema. Removes oldest files from a stage, keeping only the N most recently modified files. Deletes in batches of 200 (one REMOVE ... PATTERN per batch); a failing REMOVE fails the call.'
EXECUTE AS CALLER
AS
$$
DECLARE
  V_FILES   NUMBER := 0;
  V_BATCHES NUMBER := 0;
  V_PATTERN STRING;
  RS RESULTSET;
BEGIN
  -- One REMOVE ... PATTERN per batch of 200 files instead of one REMOVE per file
  -- (a backlog of ~7,000 files took 15+ minutes as single statements).
  EXECUTE IMMEDIATE 'SELECT RELATIVE_PATH, ROW_NUMBER() OVER (ORDER BY LAST_MODIFIED DESC) AS RN FROM DIRECTORY(@' || STAGE_NAME || ')';
  RS := (
    SELECT LISTAGG(REGEXP_REPLACE(RELATIVE_PATH, '([.^$|()\\[\\]{}*+?\\\\])', '\\\\\\1'), '|') AS PAT,
           COUNT(*) AS N
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE RN > :KEEP_N
    GROUP BY FLOOR((RN - :KEEP_N - 1) / 200)
  );
  FOR R IN RS DO
    -- Regex-escaped exact file names; errors are not swallowed so a failing cleanup fails the task
    V_PATTERN := '(.*/)?(' || R.PAT || ')';
    EXECUTE IMMEDIATE 'REMOVE @' || STAGE_NAME || ' PATTERN = ''' || REPLACE(REPLACE(V_PATTERN, '\\', '\\\\'), '''', '\\''') || '''';
    V_FILES   := V_FILES + R.N;
    V_BATCHES := V_BATCHES + 1;
  END FOR;

  RETURN 'Stage cleanup completed. Deleted: ' || V_FILES || ' files in ' || V_BATCHES || ' batches, Kept: ' || KEEP_N || ' most recent files.';
END;
$$;

-- ============================================================
-- STREAMS (moved from post_deploy.sql)
-- ============================================================

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_HQLA_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_HQLA_HOLDINGS;

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_DEPOSIT_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_DEPOSIT_BALANCES;

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_COUNTERPARTY_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_COUNTERPARTIES
    COMMENT = 'Monitors LIQI_RAW_ST_COUNTERPARTIES for new counterparty master files; triggers LIQI_RAW_TK_LOAD_COUNTERPARTIES.';

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_CONTRACT_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_CONTRACTS
    COMMENT = 'Monitors LIQI_RAW_ST_CONTRACTS for new daily contract snapshot files; triggers LIQI_RAW_TK_LOAD_CONTRACTS.';

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_CASHFLOW_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_CASHFLOWS
    COMMENT = 'Monitors LIQI_RAW_ST_CASHFLOWS for new daily cashflow schedule files; triggers LIQI_RAW_TK_LOAD_CASHFLOWS.';

DEFINE STREAM {{ db }}.{{ rep_raw }}.LIQI_RAW_SM_EVENT_FILES
    ON STAGE {{ db }}.{{ rep_raw }}.LIQI_RAW_ST_EVENTS
    COMMENT = 'Monitors LIQI_RAW_ST_EVENTS for new showcase event files; triggers LIQI_RAW_TK_LOAD_EVENTS.';
