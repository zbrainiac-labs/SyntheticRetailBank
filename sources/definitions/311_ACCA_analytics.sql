DEFINE DYNAMIC TABLE {{ db }}.{{ crm_agg }}.ACCA_AGG_DT_ACCOUNTS(
    ACCOUNT_ID VARCHAR(30) COMMENT 'Unique account identifier for transaction allocation and balance tracking',
    ACCOUNT_TYPE VARCHAR(20) COMMENT 'Type of account (CHECKING/SAVINGS/BUSINESS/INVESTMENT)',
    BASE_CURRENCY VARCHAR(3) COMMENT 'Base currency of the account (EUR/GBP/USD/CHF/NOK/SEK/DKK)',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Customer identifier for account ownership and relationship management',
    STATUS VARCHAR(20) COMMENT 'Current account status (ACTIVE/INACTIVE/CLOSED)',
    IS_ACTIVE BOOLEAN COMMENT 'Boolean flag indicating if account status is ACTIVE',
    IS_CHECKING_ACCOUNT BOOLEAN COMMENT 'Boolean flag for checking/transaction accounts',
    IS_SAVINGS_ACCOUNT BOOLEAN COMMENT 'Boolean flag for savings accounts',
    IS_BUSINESS_ACCOUNT BOOLEAN COMMENT 'Boolean flag for business/commercial accounts',
    IS_INVESTMENT_ACCOUNT BOOLEAN COMMENT 'Boolean flag for investment/securities accounts',
    IS_USD_ACCOUNT BOOLEAN COMMENT 'Boolean flag for USD-denominated accounts',
    IS_EUR_ACCOUNT BOOLEAN COMMENT 'Boolean flag for EUR-denominated accounts',
    IS_OTHER_CURRENCY_ACCOUNT BOOLEAN COMMENT 'Boolean flag for accounts in other currencies (GBP/CHF/NOK/SEK/DKK)',
    ACCOUNT_TYPE_PRIORITY NUMBER(2,0) COMMENT 'Priority ranking for account type (1=CHECKING, 2=SAVINGS, 3=BUSINESS, 4=INVESTMENT)',
    CURRENCY_GROUP VARCHAR(20) COMMENT 'Currency grouping for reporting (MAJOR_EUROPEAN/USD_BASE/OTHER_EUROPEAN/OTHER)',
    AGGREGATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Timestamp when aggregation processing was performed',
    AGGREGATION_TYPE VARCHAR(20) COMMENT 'Type of aggregation processing (1:1_COPY_FROM_RAW)',
    SOURCE_TABLE VARCHAR(50) COMMENT 'Source table reference for data lineage (CRM_RAW_001.ACCI_RAW_TB_ACCOUNTS)'
) COMMENT = '1:1 aggregation of account master data from raw layer (CRM_RAW_001.ACCI_RAW_TB_ACCOUNTS). Provides clean aggregation layer access for downstream analytics, balance calculations, and reporting. Maintains real-time refresh for data currency while serving as bridge between raw data and analytical data products.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
SELECT 
    ACCOUNT_ID,
    ACCOUNT_TYPE,
    BASE_CURRENCY,
    CUSTOMER_ID,
    STATUS,

    CASE 
        WHEN STATUS = 'ACTIVE' THEN TRUE
        ELSE FALSE
    END AS IS_ACTIVE,

    CASE 
        WHEN ACCOUNT_TYPE = 'CHECKING' THEN TRUE
        ELSE FALSE
    END AS IS_CHECKING_ACCOUNT,

    CASE 
        WHEN ACCOUNT_TYPE = 'SAVINGS' THEN TRUE
        ELSE FALSE
    END AS IS_SAVINGS_ACCOUNT,

    CASE 
        WHEN ACCOUNT_TYPE = 'BUSINESS' THEN TRUE
        ELSE FALSE
    END AS IS_BUSINESS_ACCOUNT,

    CASE 
        WHEN ACCOUNT_TYPE = 'INVESTMENT' THEN TRUE
        ELSE FALSE
    END AS IS_INVESTMENT_ACCOUNT,

    CASE 
        WHEN BASE_CURRENCY = 'USD' THEN TRUE
        ELSE FALSE
    END AS IS_USD_ACCOUNT,

    CASE 
        WHEN BASE_CURRENCY = 'EUR' THEN TRUE
        ELSE FALSE
    END AS IS_EUR_ACCOUNT,

    CASE 
        WHEN BASE_CURRENCY IN ('GBP', 'CHF', 'NOK', 'SEK', 'DKK') THEN TRUE
        ELSE FALSE
    END AS IS_OTHER_CURRENCY_ACCOUNT,

    CASE 
        WHEN ACCOUNT_TYPE = 'CHECKING' THEN 1
        WHEN ACCOUNT_TYPE = 'SAVINGS' THEN 2
        WHEN ACCOUNT_TYPE = 'BUSINESS' THEN 3
        WHEN ACCOUNT_TYPE = 'INVESTMENT' THEN 4
        ELSE 99
    END AS ACCOUNT_TYPE_PRIORITY,

    CASE 
        WHEN BASE_CURRENCY IN ('EUR', 'GBP') THEN 'MAJOR_EUROPEAN'
        WHEN BASE_CURRENCY = 'USD' THEN 'USD_BASE'
        WHEN BASE_CURRENCY IN ('CHF', 'NOK', 'SEK', 'DKK') THEN 'OTHER_EUROPEAN'
        ELSE 'OTHER'
    END AS CURRENCY_GROUP,

    CURRENT_TIMESTAMP() AS AGGREGATION_TIMESTAMP,
    '1:1_COPY_FROM_RAW' AS AGGREGATION_TYPE,
    'CRM_RAW_001.ACCI_RAW_TB_ACCOUNTS' AS SOURCE_TABLE

FROM {{ crm_raw }}.ACCI_RAW_TB_ACCOUNTS
WHERE 1=1 
ORDER BY CUSTOMER_ID, ACCOUNT_TYPE_PRIORITY, ACCOUNT_ID;
