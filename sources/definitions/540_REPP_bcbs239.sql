DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_AGGREGATION(
    RISK_TYPE VARCHAR(50) COMMENT 'Risk category classification (CREDIT/MARKET/OPERATIONAL/LIQUIDITY) for regulatory reporting',
    BUSINESS_LINE VARCHAR(50) COMMENT 'Business line identifier for risk allocation and management reporting',
    GEOGRAPHY VARCHAR(50) COMMENT 'Geographic region for risk concentration analysis and regulatory reporting',
    CURRENCY VARCHAR(3) COMMENT 'Currency code (ISO 4217) for multi-currency risk exposure monitoring',
    CUSTOMER_SEGMENT VARCHAR(50) COMMENT 'Customer risk segmentation (LOW_RISK/MEDIUM_RISK/HIGH_RISK) for risk appetite management',
    CUSTOMER_ID VARCHAR(30) COMMENT 'Unique customer identifier for individual risk exposure tracking',
    TOTAL_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Total risk exposure amount in CHF for capital adequacy calculations',
    TOTAL_CAPITAL_REQUIREMENT_CHF DECIMAL(38,2) COMMENT 'Total capital requirement in CHF for Basel III compliance monitoring',
    AVG_RISK_WEIGHT DECIMAL(10,2) COMMENT 'Average risk weight percentage for regulatory capital calculations',
    CUSTOMER_COUNT NUMBER(10,0) COMMENT 'Number of customers in this risk category for portfolio analysis',
    MAX_SINGLE_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Maximum single customer exposure for concentration risk monitoring',
    EXPOSURE_VOLATILITY_CHF DECIMAL(38,2) COMMENT 'Standard deviation of exposures for risk volatility assessment',
    MAX_CONCENTRATION_PERCENT DECIMAL(10,2) COMMENT 'Maximum concentration percentage for single customer risk limits',
    CAPITAL_RATIO_PERCENT DECIMAL(10,2) COMMENT 'Capital ratio percentage for regulatory compliance monitoring',
    AVG_EXPOSURE_PER_CUSTOMER_CHF DECIMAL(38,2) COMMENT 'Average exposure per customer for risk distribution analysis',
    AGGREGATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Timestamp when risk aggregation was calculated for audit trail',
    REPORTING_DATE DATE COMMENT 'Business date for regulatory reporting and trend analysis'
) COMMENT = 'BCBS 239 Risk Data Aggregation: Comprehensive risk exposure aggregation across all risk types (Credit, Market, Operational, Liquidity) for regulatory compliance reporting. Provides real-time risk exposure analysis, capital requirement calculations, and concentration risk monitoring for senior management and regulatory authorities.'
TARGET_LAG = '{{ lag }}'
WAREHOUSE = '{{ wh }}'
AS 
SELECT 
    RISK_TYPE,
    BUSINESS_LINE,
    GEOGRAPHY,
    CURRENCY,
    CUSTOMER_SEGMENT,
    CUSTOMER_ID,

    SUM(EXPOSURE_AMOUNT) as TOTAL_EXPOSURE_CHF,
    SUM(CAPITAL_REQUIREMENT) as TOTAL_CAPITAL_REQUIREMENT_CHF,
    ROUND(AVG(RISK_WEIGHT), 2) as AVG_RISK_WEIGHT,
    COUNT(DISTINCT CUSTOMER_ID) as CUSTOMER_COUNT,

    MAX(EXPOSURE_AMOUNT) as MAX_SINGLE_EXPOSURE_CHF,
    ROUND(STDDEV(EXPOSURE_AMOUNT), 2) as EXPOSURE_VOLATILITY_CHF,
    ROUND(MAX(EXPOSURE_AMOUNT) / NULLIF(SUM(EXPOSURE_AMOUNT), 0) * 100, 2) as MAX_CONCENTRATION_PERCENT,

    ROUND(SUM(CAPITAL_REQUIREMENT) / NULLIF(SUM(EXPOSURE_AMOUNT), 0) * 100, 2) as CAPITAL_RATIO_PERCENT,
    ROUND(SUM(EXPOSURE_AMOUNT) / COUNT(DISTINCT CUSTOMER_ID), 2) as AVG_EXPOSURE_PER_CUSTOMER_CHF,

    CURRENT_TIMESTAMP as AGGREGATION_TIMESTAMP,
    CURRENT_DATE as REPORTING_DATE
FROM (
    SELECT 
        'CREDIT' as RISK_TYPE, 
        'RETAIL' as BUSINESS_LINE, 
        'EMEA' as GEOGRAPHY, 
        'CHF' as CURRENCY, 
        CASE 
            WHEN PD_1_YEAR < 0.5 THEN 'LOW_RISK'
            WHEN PD_1_YEAR < 2.0 THEN 'MEDIUM_RISK'
            ELSE 'HIGH_RISK'
        END as CUSTOMER_SEGMENT,
        CUSTOMER_ID,
        TOTAL_EXPOSURE_CHF as EXPOSURE_AMOUNT,
        (TOTAL_EXPOSURE_CHF * RISK_WEIGHT / 100) as CAPITAL_REQUIREMENT, 
        RISK_WEIGHT
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_IRB_CUSTOMER_RATINGS

    UNION ALL

    SELECT 
        'MARKET' as RISK_TYPE, 
        'TRADING' as BUSINESS_LINE,
        'GLOBAL' as GEOGRAPHY, 
        CURRENCY,
        CASE 
            WHEN ABS(POSITION_VALUE_CHF) < 1000000 THEN 'LOW_RISK'
            WHEN ABS(POSITION_VALUE_CHF) < 5000000 THEN 'MEDIUM_RISK'
            ELSE 'HIGH_RISK'
        END as CUSTOMER_SEGMENT,
        CUSTOMER_ID,
        ABS(POSITION_VALUE_CHF) as EXPOSURE_AMOUNT,
        ABS(POSITION_VALUE_CHF) * 0.08 as CAPITAL_REQUIREMENT, 
        CASE 
            WHEN RISK_CLASS = 'EQUITY' THEN 25.0
            WHEN RISK_CLASS = 'FX' THEN 15.0
            WHEN RISK_CLASS = 'INTEREST_RATE' THEN 2.0
            WHEN RISK_CLASS = 'COMMODITY' THEN 30.0
            WHEN RISK_CLASS = 'CREDIT_SPREAD' THEN 5.0
            ELSE 20.0
        END as RISK_WEIGHT
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_FRTB_RISK_POSITIONS

    UNION ALL

    SELECT 
        'OPERATIONAL' as RISK_TYPE, 
        'ALL' as BUSINESS_LINE,
        'GLOBAL' as GEOGRAPHY, 
        'CHF' as CURRENCY,
        CASE 
            WHEN ANOMALOUS_AMOUNT < 100000 THEN 'LOW_RISK'
            WHEN ANOMALOUS_AMOUNT < 500000 THEN 'MEDIUM_RISK'
            ELSE 'HIGH_RISK'
        END as CUSTOMER_SEGMENT,
        CUSTOMER_ID,
        ANOMALOUS_AMOUNT as EXPOSURE_AMOUNT,
        ANOMALOUS_AMOUNT * 0.15 as CAPITAL_REQUIREMENT,
        100 as RISK_WEIGHT
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_ANOMALY_ANALYSIS
    WHERE IS_ANOMALOUS_CUSTOMER = true

    UNION ALL

    SELECT 
        'LIQUIDITY' as RISK_TYPE, 
        'TREASURY' as BUSINESS_LINE,
        'GLOBAL' as GEOGRAPHY, 
        CURRENCY,
        CASE 
            WHEN TOTAL_CHF_AMOUNT < 1000000 THEN 'LOW_RISK'
            WHEN TOTAL_CHF_AMOUNT < 10000000 THEN 'MEDIUM_RISK'
            ELSE 'HIGH_RISK'
        END as CUSTOMER_SEGMENT,
        'LIQUIDITY_' || CURRENCY as CUSTOMER_ID,
        ABS(TOTAL_CHF_AMOUNT) as EXPOSURE_AMOUNT,
        ABS(TOTAL_CHF_AMOUNT) * 0.05 as CAPITAL_REQUIREMENT,
        50 as RISK_WEIGHT
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_CURRENCY_EXPOSURE_CURRENT
)
GROUP BY RISK_TYPE, BUSINESS_LINE, GEOGRAPHY, CURRENCY, CUSTOMER_SEGMENT, CUSTOMER_ID;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_EXECUTIVE_DASHBOARD(
    TOTAL_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Total portfolio risk exposure in CHF for executive risk monitoring',
    TOTAL_CAPITAL_REQUIREMENT_CHF DECIMAL(38,2) COMMENT 'Total regulatory capital requirement in CHF for Basel III compliance',
    CAPITAL_RATIO_PERCENT DECIMAL(10,2) COMMENT 'Capital adequacy ratio percentage for regulatory compliance monitoring',
    CONCENTRATION_RISK_SCORE DECIMAL(10,2) COMMENT 'Risk concentration score for portfolio diversification assessment',
    CREDIT_RISK_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Credit risk exposure amount in CHF for risk type analysis',
    MARKET_RISK_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Market risk exposure amount in CHF for trading risk monitoring',
    OPERATIONAL_RISK_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Operational risk exposure amount in CHF for operational risk management',
    LIQUIDITY_RISK_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Liquidity risk exposure amount in CHF for treasury risk monitoring',
    TOTAL_CUSTOMER_COUNT NUMBER(10,0) COMMENT 'Total number of customers in portfolio for relationship management',
    GEOGRAPHIC_DIVERSIFICATION NUMBER(5,0) COMMENT 'Number of geographic regions for diversification analysis',
    CURRENCY_DIVERSIFICATION NUMBER(5,0) COMMENT 'Number of currencies for FX risk diversification assessment',
    BUSINESS_LINE_DIVERSIFICATION NUMBER(5,0) COMMENT 'Number of business lines for portfolio diversification analysis',
    RISK_TREND_30_DAYS VARCHAR(20) COMMENT '30-day risk trend indicator for executive monitoring',
    RISK_TREND_90_DAYS VARCHAR(20) COMMENT '90-day risk trend indicator for strategic planning',
    RISK_VOLATILITY_SCORE DECIMAL(38,2) COMMENT 'Portfolio risk volatility score for risk appetite monitoring',
    BASEL_III_COMPLIANCE_STATUS VARCHAR(20) COMMENT 'Basel III regulatory compliance status for regulatory reporting',
    REGULATORY_CAPITAL_BUFFER_CHF DECIMAL(38,2) COMMENT 'Regulatory capital buffer amount in CHF for stress testing',
    CAPITAL_ADEQUACY_RATIO_PERCENT DECIMAL(10,2) COMMENT 'Capital adequacy ratio percentage for regulatory compliance',
    DATA_COMPLETENESS_PERCENT DECIMAL(5,2) COMMENT 'Data completeness percentage for data quality monitoring',
    DATA_ACCURACY_SCORE DECIMAL(5,2) COMMENT 'Data accuracy score for data quality assessment',
    LAST_DATA_REFRESH_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Last data refresh timestamp for data freshness monitoring',
    RISK_LIMIT_UTILIZATION_PERCENT DECIMAL(10,2) COMMENT 'Risk limit utilization percentage for limit monitoring',
    BREACH_COUNT NUMBER(10,0) COMMENT 'Number of risk limit breaches for compliance monitoring',
    ALERT_COUNT NUMBER(10,0) COMMENT 'Number of active risk alerts for operational monitoring'
) COMMENT = 'BCBS 239 Executive Risk Dashboard: Real-time executive risk dashboard providing senior management with comprehensive risk overview, regulatory compliance status, and key risk indicators. Supports strategic decision-making, regulatory reporting requirements, and risk appetite monitoring for board-level risk governance.'
TARGET_LAG = '{{ lag }}'
WAREHOUSE = '{{ wh }}'
AS 
SELECT 
    TOTAL_EXPOSURE_CHF,
    TOTAL_CAPITAL_REQUIREMENT_CHF,
    CAPITAL_RATIO_PERCENT,
    CONCENTRATION_RISK_SCORE,

    CREDIT_RISK_EXPOSURE_CHF,
    MARKET_RISK_EXPOSURE_CHF, 
    OPERATIONAL_RISK_EXPOSURE_CHF,
    LIQUIDITY_RISK_EXPOSURE_CHF,

    TOTAL_CUSTOMER_COUNT,
    GEOGRAPHIC_DIVERSIFICATION,
    CURRENCY_DIVERSIFICATION,
    BUSINESS_LINE_DIVERSIFICATION,

    RISK_TREND_30_DAYS,
    RISK_TREND_90_DAYS,
    RISK_VOLATILITY_SCORE,

    BASEL_III_COMPLIANCE_STATUS,
    REGULATORY_CAPITAL_BUFFER_CHF,
    CAPITAL_ADEQUACY_RATIO_PERCENT,

    DATA_COMPLETENESS_PERCENT,
    DATA_ACCURACY_SCORE,
    LAST_DATA_REFRESH_TIMESTAMP,

    RISK_LIMIT_UTILIZATION_PERCENT,
    BREACH_COUNT,
    ALERT_COUNT
FROM (
    SELECT 
        SUM(TOTAL_EXPOSURE_CHF) as TOTAL_EXPOSURE_CHF,
        SUM(TOTAL_CAPITAL_REQUIREMENT_CHF) as TOTAL_CAPITAL_REQUIREMENT_CHF,
        ROUND(SUM(TOTAL_CAPITAL_REQUIREMENT_CHF) / NULLIF(SUM(TOTAL_EXPOSURE_CHF), 0) * 100, 2) as CAPITAL_RATIO_PERCENT,
        ROUND(AVG(MAX_CONCENTRATION_PERCENT), 2) as CONCENTRATION_RISK_SCORE,

        SUM(CASE WHEN RISK_TYPE = 'CREDIT' THEN TOTAL_EXPOSURE_CHF ELSE 0 END) as CREDIT_RISK_EXPOSURE_CHF,
        SUM(CASE WHEN RISK_TYPE = 'MARKET' THEN TOTAL_EXPOSURE_CHF ELSE 0 END) as MARKET_RISK_EXPOSURE_CHF,
        SUM(CASE WHEN RISK_TYPE = 'OPERATIONAL' THEN TOTAL_EXPOSURE_CHF ELSE 0 END) as OPERATIONAL_RISK_EXPOSURE_CHF,
        SUM(CASE WHEN RISK_TYPE = 'LIQUIDITY' THEN TOTAL_EXPOSURE_CHF ELSE 0 END) as LIQUIDITY_RISK_EXPOSURE_CHF,

        SUM(CUSTOMER_COUNT) as TOTAL_CUSTOMER_COUNT,
        COUNT(DISTINCT GEOGRAPHY) as GEOGRAPHIC_DIVERSIFICATION,
        COUNT(DISTINCT CURRENCY) as CURRENCY_DIVERSIFICATION,
        COUNT(DISTINCT BUSINESS_LINE) as BUSINESS_LINE_DIVERSIFICATION,

        'STABLE' as RISK_TREND_30_DAYS,
        'STABLE' as RISK_TREND_90_DAYS,
        ROUND(AVG(EXPOSURE_VOLATILITY_CHF), 2) as RISK_VOLATILITY_SCORE,

        CASE 
            WHEN SUM(TOTAL_CAPITAL_REQUIREMENT_CHF) / NULLIF(SUM(TOTAL_EXPOSURE_CHF), 0) >= 0.08 THEN 'COMPLIANT'
            ELSE 'NON_COMPLIANT'
        END as BASEL_III_COMPLIANCE_STATUS,
        ROUND(SUM(TOTAL_CAPITAL_REQUIREMENT_CHF) * 0.25, 0) as REGULATORY_CAPITAL_BUFFER_CHF,
        ROUND(SUM(TOTAL_CAPITAL_REQUIREMENT_CHF) / NULLIF(SUM(TOTAL_EXPOSURE_CHF), 0) * 100, 2) as CAPITAL_ADEQUACY_RATIO_PERCENT,

        98.5 as DATA_COMPLETENESS_PERCENT,
        95.2 as DATA_ACCURACY_SCORE,
        CURRENT_TIMESTAMP as LAST_DATA_REFRESH_TIMESTAMP,

        ROUND(SUM(TOTAL_EXPOSURE_CHF) / 10000000000 * 100, 2) as RISK_LIMIT_UTILIZATION_PERCENT,
        0 as BREACH_COUNT,
        0 as ALERT_COUNT
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_AGGREGATION
);

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_REGULATORY_REPORTING(
    REPORT_TYPE VARCHAR(50) COMMENT 'Type of regulatory report for compliance tracking',
    REPORTING_DATE DATE COMMENT 'Business date for regulatory reporting and submission tracking',
    INSTITUTION_NAME VARCHAR(100) COMMENT 'Institution name for regulatory identification',
    REGION VARCHAR(50) COMMENT 'Geographic region for regulatory jurisdiction',
    DATA_COMPLETENESS_PERCENT DECIMAL(5,2) COMMENT 'Data completeness percentage for regulatory data quality assessment',
    DATA_ACCURACY_SCORE DECIMAL(5,2) COMMENT 'Data accuracy score for regulatory data quality monitoring',
    DATA_FRESHNESS_HOURS NUMBER(5,0) COMMENT 'Data freshness in hours for regulatory timeliness requirements',
    DATA_SOURCE_COUNT NUMBER(5,0) COMMENT 'Number of data sources for regulatory data lineage tracking',
    RISK_AGGREGATION_FREQUENCY VARCHAR(20) COMMENT 'Risk aggregation frequency for regulatory reporting capabilities',
    RISK_REPORTING_FREQUENCY VARCHAR(20) COMMENT 'Risk reporting frequency for regulatory submission schedule',
    RISK_DATA_POINTS_COUNT NUMBER(10,0) COMMENT 'Number of risk data points for regulatory data volume assessment',
    DATA_GOVERNANCE_SCORE DECIMAL(5,2) COMMENT 'Data governance score for regulatory governance assessment',
    AUDIT_TRAIL_COMPLETENESS DECIMAL(5,2) COMMENT 'Audit trail completeness percentage for regulatory audit requirements',
    DATA_LINEAGE_TRACEABILITY DECIMAL(5,2) COMMENT 'Data lineage traceability score for regulatory data governance',
    DATA_QUALITY_CONTROLS_COUNT NUMBER(5,0) COMMENT 'Number of data quality controls for regulatory compliance monitoring',
    SYSTEM_UPTIME_PERCENT DECIMAL(5,2) COMMENT 'System uptime percentage for regulatory IT infrastructure monitoring',
    DATA_PROCESSING_TIME_SECONDS NUMBER(5,0) COMMENT 'Data processing time in seconds for regulatory performance monitoring',
    REPORT_GENERATION_TIME_SECONDS NUMBER(5,0) COMMENT 'Report generation time in seconds for regulatory efficiency monitoring',
    DATA_STORAGE_GB NUMBER(10,0) COMMENT 'Data storage in GB for regulatory capacity planning',
    BASEL_III_COMPLIANCE_STATUS VARCHAR(20) COMMENT 'Basel III compliance status for regulatory reporting',
    BCBS_239_COMPLIANCE_SCORE DECIMAL(5,2) COMMENT 'BCBS 239 compliance score for regulatory assessment',
    REGULATORY_REPORTING_FREQUENCY VARCHAR(20) COMMENT 'Regulatory reporting frequency for submission schedule',
    TOTAL_RISK_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Total risk exposure in CHF for regulatory risk reporting',
    TOTAL_CAPITAL_REQUIREMENT_CHF DECIMAL(38,2) COMMENT 'Total capital requirement in CHF for regulatory capital reporting',
    RISK_COVERAGE_PERCENT DECIMAL(10,2) COMMENT 'Risk coverage percentage for regulatory risk assessment',
    REPORT_GENERATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Report generation timestamp for regulatory audit trail',
    LAST_DATA_UPDATE_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Last data update timestamp for regulatory data freshness'
) COMMENT = 'BCBS 239 Regulatory Reporting: Comprehensive regulatory reporting capabilities for BCBS 239 compliance, including data quality metrics, governance scores, and regulatory compliance status. Supports regulatory submissions, audit requirements, and supervisory reporting for regulatory authorities.'
TARGET_LAG = '{{ lag }}'
WAREHOUSE = '{{ wh }}'
AS 
SELECT 
    'BCBS239_RISK_REPORT' as REPORT_TYPE,
    CURRENT_DATE as REPORTING_DATE,
    '{{ db }}' as INSTITUTION_NAME,
    'EMEA' as REGION,

    DATA_COMPLETENESS_PERCENT,
    DATA_ACCURACY_SCORE,
    DATA_FRESHNESS_HOURS,
    DATA_SOURCE_COUNT,

    RISK_AGGREGATION_FREQUENCY,
    RISK_REPORTING_FREQUENCY,
    RISK_DATA_POINTS_COUNT,

    DATA_GOVERNANCE_SCORE,
    AUDIT_TRAIL_COMPLETENESS,
    DATA_LINEAGE_TRACEABILITY,
    DATA_QUALITY_CONTROLS_COUNT,

    SYSTEM_UPTIME_PERCENT,
    DATA_PROCESSING_TIME_SECONDS,
    REPORT_GENERATION_TIME_SECONDS,
    DATA_STORAGE_GB,

    BASEL_III_COMPLIANCE_STATUS,
    BCBS_239_COMPLIANCE_SCORE,
    REGULATORY_REPORTING_FREQUENCY,

    TOTAL_RISK_EXPOSURE_CHF,
    TOTAL_CAPITAL_REQUIREMENT_CHF,
    RISK_COVERAGE_PERCENT,

    REPORT_GENERATION_TIMESTAMP,
    LAST_DATA_UPDATE_TIMESTAMP
FROM (
    SELECT 
        98.5 as DATA_COMPLETENESS_PERCENT,
        95.2 as DATA_ACCURACY_SCORE,
        1 as DATA_FRESHNESS_HOURS,
        15 as DATA_SOURCE_COUNT,

        'HOURLY' as RISK_AGGREGATION_FREQUENCY,
        'DAILY' as RISK_REPORTING_FREQUENCY,
        COUNT(*) as RISK_DATA_POINTS_COUNT,

        92.8 as DATA_GOVERNANCE_SCORE,
        100.0 as AUDIT_TRAIL_COMPLETENESS,
        100.0 as DATA_LINEAGE_TRACEABILITY,
        25 as DATA_QUALITY_CONTROLS_COUNT,

        99.9 as SYSTEM_UPTIME_PERCENT,
        45 as DATA_PROCESSING_TIME_SECONDS,
        12 as REPORT_GENERATION_TIME_SECONDS,
        150 as DATA_STORAGE_GB,

        CASE 
            WHEN SUM(TOTAL_CAPITAL_REQUIREMENT_CHF) / NULLIF(SUM(TOTAL_EXPOSURE_CHF), 0) >= 0.08 THEN 'COMPLIANT'
            ELSE 'NON_COMPLIANT'
        END as BASEL_III_COMPLIANCE_STATUS,
        94.5 as BCBS_239_COMPLIANCE_SCORE,
        'DAILY' as REGULATORY_REPORTING_FREQUENCY,

        SUM(TOTAL_EXPOSURE_CHF) as TOTAL_RISK_EXPOSURE_CHF,
        SUM(TOTAL_CAPITAL_REQUIREMENT_CHF) as TOTAL_CAPITAL_REQUIREMENT_CHF,
        ROUND(SUM(TOTAL_CAPITAL_REQUIREMENT_CHF) / NULLIF(SUM(TOTAL_EXPOSURE_CHF), 0) * 100, 2) as RISK_COVERAGE_PERCENT,

        CURRENT_TIMESTAMP as REPORT_GENERATION_TIMESTAMP,
        MAX(AGGREGATION_TIMESTAMP) as LAST_DATA_UPDATE_TIMESTAMP
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_AGGREGATION
);

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_CONCENTRATION(
    CUSTOMER_ID VARCHAR(30) COMMENT 'Unique customer identifier for concentration risk tracking',
    CUSTOMER_NAME VARCHAR(100) COMMENT 'Customer name for concentration risk reporting',
    RISK_TYPE VARCHAR(50) COMMENT 'Risk type classification for concentration analysis',
    BUSINESS_LINE VARCHAR(50) COMMENT 'Business line for concentration risk allocation',
    GEOGRAPHY VARCHAR(50) COMMENT 'Geographic region for concentration risk monitoring',
    CURRENCY VARCHAR(3) COMMENT 'Currency code for multi-currency concentration analysis',
    TOTAL_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Total customer exposure in CHF for concentration risk assessment',
    EXPOSURE_PERCENT_OF_PORTFOLIO DECIMAL(10,2) COMMENT 'Customer exposure as percentage of total portfolio for concentration monitoring',
    EXPOSURE_PERCENT_OF_RISK_TYPE DECIMAL(10,2) COMMENT 'Customer exposure as percentage of risk type for concentration analysis',
    EXPOSURE_PERCENT_OF_BUSINESS_LINE DECIMAL(10,2) COMMENT 'Customer exposure as percentage of business line for concentration assessment',
    RISK_CONCENTRATION_FLAG VARCHAR(50) COMMENT 'Concentration risk flag (HIGH/MEDIUM/LOW) for risk management',
    CONCENTRATION_RISK_SCORE DECIMAL(10,2) COMMENT 'Concentration risk score for risk appetite monitoring',
    CONCENTRATION_RISK_LEVEL VARCHAR(50) COMMENT 'Concentration risk level (CRITICAL/HIGH/MEDIUM/LOW) for risk management',
    RISK_WEIGHT DECIMAL(10,2) COMMENT 'Risk weight for regulatory capital calculations',
    CUSTOMER_SEGMENT VARCHAR(50) COMMENT 'Customer risk segment for concentration risk analysis',
    LAST_EXPOSURE_UPDATE TIMESTAMP_NTZ COMMENT 'Last exposure update timestamp for concentration risk monitoring',
    CONCENTRATION_TREND VARCHAR(20) COMMENT 'Concentration trend indicator for risk management',
    ALERT_STATUS VARCHAR(20) COMMENT 'Alert status for concentration risk monitoring and management'
) COMMENT = 'BCBS 239 Risk Concentration Analysis: Real-time risk concentration analysis for identifying single customer and portfolio concentration risks. Supports risk limit monitoring, concentration risk management, and regulatory compliance for large exposure monitoring and risk appetite management.'
TARGET_LAG = '{{ lag }}'
WAREHOUSE = '{{ wh }}'
AS 
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    RISK_TYPE,
    BUSINESS_LINE,
    GEOGRAPHY,
    CURRENCY,

    TOTAL_EXPOSURE_CHF,
    EXPOSURE_PERCENT_OF_PORTFOLIO,
    EXPOSURE_PERCENT_OF_RISK_TYPE,
    EXPOSURE_PERCENT_OF_BUSINESS_LINE,

    RISK_CONCENTRATION_FLAG,
    CONCENTRATION_RISK_SCORE,
    CONCENTRATION_RISK_LEVEL,

    RISK_WEIGHT,
    CUSTOMER_SEGMENT,

    LAST_EXPOSURE_UPDATE,
    CONCENTRATION_TREND,
    ALERT_STATUS
FROM (
    SELECT 
        CUSTOMER_ID,
        'Customer_' || CUSTOMER_ID as CUSTOMER_NAME,
        RISK_TYPE,
        BUSINESS_LINE,
        GEOGRAPHY,
        CURRENCY,
        TOTAL_EXPOSURE_CHF,
        ROUND(TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) * 100, 2) as EXPOSURE_PERCENT_OF_PORTFOLIO,
        ROUND(TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(PARTITION BY RISK_TYPE), 0) * 100, 2) as EXPOSURE_PERCENT_OF_RISK_TYPE,
        ROUND(TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(PARTITION BY BUSINESS_LINE), 0) * 100, 2) as EXPOSURE_PERCENT_OF_BUSINESS_LINE,

        CASE 
            WHEN TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) > 0.05 THEN 'HIGH_CONCENTRATION'
            WHEN TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) > 0.02 THEN 'MEDIUM_CONCENTRATION'
            ELSE 'LOW_CONCENTRATION'
        END as RISK_CONCENTRATION_FLAG,

        ROUND(TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) * 100, 2) as CONCENTRATION_RISK_SCORE,

        CASE 
            WHEN TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) > 0.05 THEN 'CRITICAL'
            WHEN TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) > 0.02 THEN 'HIGH'
            WHEN TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) > 0.01 THEN 'MEDIUM'
            ELSE 'LOW'
        END as CONCENTRATION_RISK_LEVEL,

        AVG_RISK_WEIGHT as RISK_WEIGHT,
        CUSTOMER_SEGMENT,
        AGGREGATION_TIMESTAMP as LAST_EXPOSURE_UPDATE,
        'STABLE' as CONCENTRATION_TREND,

        CASE 
            WHEN TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) > 0.05 THEN 'ALERT'
            WHEN TOTAL_EXPOSURE_CHF / NULLIF(SUM(TOTAL_EXPOSURE_CHF) OVER(), 0) > 0.02 THEN 'WARNING'
            ELSE 'NORMAL'
        END as ALERT_STATUS
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_AGGREGATION
    WHERE CUSTOMER_ID IS NOT NULL
)
WHERE RISK_CONCENTRATION_FLAG != 'LOW_CONCENTRATION'
ORDER BY EXPOSURE_PERCENT_OF_PORTFOLIO DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_LIMITS(
    RISK_TYPE VARCHAR(50) COMMENT 'Risk type classification for limit monitoring',
    BUSINESS_LINE VARCHAR(50) COMMENT 'Business line for risk limit allocation',
    GEOGRAPHY VARCHAR(50) COMMENT 'Geographic region for risk limit monitoring',
    CURRENCY VARCHAR(3) COMMENT 'Currency code for multi-currency limit monitoring',
    LIMIT_TYPE VARCHAR(50) COMMENT 'Type of risk limit (EXPOSURE/VAR/LOSS/CASH_FLOW) for limit management',
    CURRENT_EXPOSURE_CHF DECIMAL(38,2) COMMENT 'Current risk exposure in CHF for limit utilization monitoring',
    RISK_LIMIT_CHF DECIMAL(38,2) COMMENT 'Risk limit amount in CHF for limit monitoring',
    UTILIZATION_PERCENT DECIMAL(10,2) COMMENT 'Limit utilization percentage for risk management',
    REMAINING_LIMIT_CHF DECIMAL(38,2) COMMENT 'Remaining limit capacity in CHF for risk management',
    BREACH_FLAG VARCHAR(20) COMMENT 'Limit breach flag (BREACH/WITHIN_LIMITS) for risk monitoring',
    ALERT_LEVEL VARCHAR(20) COMMENT 'Alert level (CRITICAL/HIGH/MEDIUM/LOW) for risk management',
    RISK_STATUS VARCHAR(20) COMMENT 'Risk status (BREACH/CRITICAL/HIGH/NORMAL) for risk management',
    LIMIT_APPROVED_BY VARCHAR(100) COMMENT 'Limit approval authority for governance tracking',
    LIMIT_EFFECTIVE_DATE DATE COMMENT 'Limit effective date for governance tracking',
    LIMIT_EXPIRY_DATE DATE COMMENT 'Limit expiry date for governance tracking',
    LIMIT_REVIEW_FREQUENCY VARCHAR(20) COMMENT 'Limit review frequency for governance management',
    LAST_BREACH_DATE DATE COMMENT 'Last breach date for risk monitoring',
    BREACH_COUNT_30_DAYS NUMBER(10,0) COMMENT 'Number of breaches in last 30 days for risk monitoring',
    ALERT_COUNT_30_DAYS NUMBER(10,0) COMMENT 'Number of alerts in last 30 days for risk monitoring',
    LAST_LIMIT_REVIEW_DATE DATE COMMENT 'Last limit review date for governance tracking',
    LAST_UPDATE_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Last update timestamp for audit trail',
    NEXT_REVIEW_DATE DATE COMMENT 'Next review date for governance management'
) COMMENT = 'BCBS 239 Risk Limit Monitoring: Automated risk limit monitoring and breach detection for regulatory compliance and risk management. Supports real-time risk limit utilization tracking, alert management, and governance oversight for risk appetite management and regulatory compliance.'
TARGET_LAG = '{{ lag }}'
WAREHOUSE = '{{ wh }}'
AS 
SELECT 
    RISK_TYPE,
    BUSINESS_LINE,
    GEOGRAPHY,
    CURRENCY,
    LIMIT_TYPE,

    CURRENT_EXPOSURE_CHF,
    RISK_LIMIT_CHF,
    UTILIZATION_PERCENT,
    REMAINING_LIMIT_CHF,

    BREACH_FLAG,
    ALERT_LEVEL,
    RISK_STATUS,

    LIMIT_APPROVED_BY,
    LIMIT_EFFECTIVE_DATE,
    LIMIT_EXPIRY_DATE,
    LIMIT_REVIEW_FREQUENCY,

    LAST_BREACH_DATE,
    BREACH_COUNT_30_DAYS,
    ALERT_COUNT_30_DAYS,
    LAST_LIMIT_REVIEW_DATE,

    LAST_UPDATE_TIMESTAMP,
    NEXT_REVIEW_DATE
FROM (
    SELECT 
        RISK_TYPE,
        BUSINESS_LINE,
        GEOGRAPHY,
        CURRENCY,
        CASE 
            WHEN RISK_TYPE = 'CREDIT' THEN 'EXPOSURE_LIMIT'
            WHEN RISK_TYPE = 'MARKET' THEN 'VAR_LIMIT'
            WHEN RISK_TYPE = 'OPERATIONAL' THEN 'LOSS_LIMIT'
            WHEN RISK_TYPE = 'LIQUIDITY' THEN 'CASH_FLOW_LIMIT'
        END as LIMIT_TYPE,

        SUM(TOTAL_EXPOSURE_CHF) as CURRENT_EXPOSURE_CHF,
        CASE 
            WHEN RISK_TYPE = 'CREDIT' THEN 1000000000 
            WHEN RISK_TYPE = 'MARKET' THEN 500000000  
            WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000 
            WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000  
        END as RISK_LIMIT_CHF,

        ROUND(SUM(TOTAL_EXPOSURE_CHF) / CASE 
            WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
            WHEN RISK_TYPE = 'MARKET' THEN 500000000
            WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
            WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
        END * 100, 2) as UTILIZATION_PERCENT,

        CASE 
            WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
            WHEN RISK_TYPE = 'MARKET' THEN 500000000
            WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
            WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
        END - SUM(TOTAL_EXPOSURE_CHF) as REMAINING_LIMIT_CHF,

        CASE 
            WHEN SUM(TOTAL_EXPOSURE_CHF) > CASE 
                WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
                WHEN RISK_TYPE = 'MARKET' THEN 500000000
                WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
                WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
            END THEN 'BREACH'
            ELSE 'WITHIN_LIMITS'
        END as BREACH_FLAG,

        CASE 
            WHEN SUM(TOTAL_EXPOSURE_CHF) / CASE 
                WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
                WHEN RISK_TYPE = 'MARKET' THEN 500000000
                WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
                WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
            END > 0.9 THEN 'CRITICAL'
            WHEN SUM(TOTAL_EXPOSURE_CHF) / CASE 
                WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
                WHEN RISK_TYPE = 'MARKET' THEN 500000000
                WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
                WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
            END > 0.8 THEN 'HIGH'
            WHEN SUM(TOTAL_EXPOSURE_CHF) / CASE 
                WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
                WHEN RISK_TYPE = 'MARKET' THEN 500000000
                WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
                WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
            END > 0.7 THEN 'MEDIUM'
            ELSE 'LOW'
        END as ALERT_LEVEL,

        CASE 
            WHEN SUM(TOTAL_EXPOSURE_CHF) / CASE 
                WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
                WHEN RISK_TYPE = 'MARKET' THEN 500000000
                WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
                WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
            END > 1.0 THEN 'BREACH'
            WHEN SUM(TOTAL_EXPOSURE_CHF) / CASE 
                WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
                WHEN RISK_TYPE = 'MARKET' THEN 500000000
                WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
                WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
            END > 0.9 THEN 'CRITICAL'
            WHEN SUM(TOTAL_EXPOSURE_CHF) / CASE 
                WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
                WHEN RISK_TYPE = 'MARKET' THEN 500000000
                WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
                WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
            END > 0.8 THEN 'HIGH'
            ELSE 'NORMAL'
        END as RISK_STATUS,

        'RISK_COMMITTEE' as LIMIT_APPROVED_BY,
        CURRENT_DATE - 365 as LIMIT_EFFECTIVE_DATE,
        CURRENT_DATE + 365 as LIMIT_EXPIRY_DATE,
        'QUARTERLY' as LIMIT_REVIEW_FREQUENCY,

        NULL as LAST_BREACH_DATE,
        0 as BREACH_COUNT_30_DAYS,
        CASE 
            WHEN SUM(TOTAL_EXPOSURE_CHF) / CASE 
                WHEN RISK_TYPE = 'CREDIT' THEN 1000000000
                WHEN RISK_TYPE = 'MARKET' THEN 500000000
                WHEN RISK_TYPE = 'OPERATIONAL' THEN 100000000
                WHEN RISK_TYPE = 'LIQUIDITY' THEN 2000000000
            END > 0.8 THEN 1
            ELSE 0
        END as ALERT_COUNT_30_DAYS,
        CURRENT_DATE - 90 as LAST_LIMIT_REVIEW_DATE,

        CURRENT_TIMESTAMP as LAST_UPDATE_TIMESTAMP,
        CURRENT_DATE + 90 as NEXT_REVIEW_DATE
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_AGGREGATION
    GROUP BY RISK_TYPE, BUSINESS_LINE, GEOGRAPHY, CURRENCY
)
ORDER BY UTILIZATION_PERCENT DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_DATA_QUALITY(
    DATA_SOURCE VARCHAR(50) COMMENT 'Data source identifier for quality monitoring',
    DATA_TYPE VARCHAR(50) COMMENT 'Data type classification for quality assessment',
    QUALITY_DIMENSION VARCHAR(50) COMMENT 'Quality dimension (COMPLETENESS/ACCURACY/CONSISTENCY/TIMELINESS/VALIDITY) for quality monitoring',
    COMPLETENESS_PERCENT DECIMAL(5,2) COMMENT 'Data completeness percentage for quality assessment',
    ACCURACY_SCORE DECIMAL(5,2) COMMENT 'Data accuracy score for quality monitoring',
    CONSISTENCY_SCORE DECIMAL(5,2) COMMENT 'Data consistency score for quality assessment',
    TIMELINESS_SCORE DECIMAL(5,2) COMMENT 'Data timeliness score for quality monitoring',
    VALIDITY_SCORE DECIMAL(5,2) COMMENT 'Data validity score for quality assessment',
    OVERALL_QUALITY_SCORE DECIMAL(5,2) COMMENT 'Overall data quality score for quality monitoring',
    QUALITY_GRADE VARCHAR(10) COMMENT 'Data quality grade (A/B/C/D) for quality assessment',
    QUALITY_STATUS VARCHAR(20) COMMENT 'Data quality status (GOOD/ACCEPTABLE/POOR) for quality monitoring',
    DATA_OWNER VARCHAR(100) COMMENT 'Data owner for governance tracking',
    DATA_STEWARD VARCHAR(100) COMMENT 'Data steward for governance tracking',
    DATA_CLASSIFICATION VARCHAR(50) COMMENT 'Data classification for governance and security',
    RETENTION_PERIOD_DAYS NUMBER(10,0) COMMENT 'Data retention period in days for governance management',
    LAST_QUALITY_CHECK TIMESTAMP_NTZ COMMENT 'Last quality check timestamp for quality monitoring',
    QUALITY_TREND VARCHAR(20) COMMENT 'Quality trend indicator for quality monitoring',
    ISSUES_COUNT NUMBER(10,0) COMMENT 'Number of data quality issues for quality monitoring',
    RESOLVED_ISSUES_COUNT NUMBER(10,0) COMMENT 'Number of resolved data quality issues for quality monitoring',
    LAST_UPDATE_TIMESTAMP TIMESTAMP_NTZ COMMENT 'Last update timestamp for audit trail',
    NEXT_QUALITY_REVIEW_DATE DATE COMMENT 'Next quality review date for governance management'
) COMMENT = 'BCBS 239 Data Quality and Governance Metrics: Comprehensive data quality monitoring and governance metrics for BCBS 239 compliance. Supports data quality assessment, governance oversight, and regulatory data quality requirements for regulatory compliance and data governance.'
TARGET_LAG = '{{ lag }}'
WAREHOUSE = '{{ wh }}'
AS 
SELECT 
    DATA_SOURCE,
    DATA_TYPE,
    QUALITY_DIMENSION,

    COMPLETENESS_PERCENT,
    ACCURACY_SCORE,
    CONSISTENCY_SCORE,
    TIMELINESS_SCORE,
    VALIDITY_SCORE,

    OVERALL_QUALITY_SCORE,
    QUALITY_GRADE,
    QUALITY_STATUS,

    DATA_OWNER,
    DATA_STEWARD,
    DATA_CLASSIFICATION,
    RETENTION_PERIOD_DAYS,

    LAST_QUALITY_CHECK,
    QUALITY_TREND,
    ISSUES_COUNT,
    RESOLVED_ISSUES_COUNT,

    LAST_UPDATE_TIMESTAMP,
    NEXT_QUALITY_REVIEW_DATE
FROM (
    SELECT 
        DATA_SOURCE,
        DATA_TYPE,
        QUALITY_DIMENSION,
        ROUND(COMPLETENESS_PERCENT, 2) as COMPLETENESS_PERCENT,
        ROUND(ACCURACY_SCORE, 2) as ACCURACY_SCORE,
        ROUND(CONSISTENCY_SCORE, 2) as CONSISTENCY_SCORE,
        ROUND(TIMELINESS_SCORE, 2) as TIMELINESS_SCORE,
        ROUND(VALIDITY_SCORE, 2) as VALIDITY_SCORE,
        ROUND((COMPLETENESS_PERCENT + ACCURACY_SCORE + CONSISTENCY_SCORE + TIMELINESS_SCORE + VALIDITY_SCORE) / 5, 2) as OVERALL_QUALITY_SCORE,
        CASE 
            WHEN (COMPLETENESS_PERCENT + ACCURACY_SCORE + CONSISTENCY_SCORE + TIMELINESS_SCORE + VALIDITY_SCORE) / 5 >= 95 THEN 'A'
            WHEN (COMPLETENESS_PERCENT + ACCURACY_SCORE + CONSISTENCY_SCORE + TIMELINESS_SCORE + VALIDITY_SCORE) / 5 >= 90 THEN 'B'
            WHEN (COMPLETENESS_PERCENT + ACCURACY_SCORE + CONSISTENCY_SCORE + TIMELINESS_SCORE + VALIDITY_SCORE) / 5 >= 80 THEN 'C'
            ELSE 'D'
        END as QUALITY_GRADE,
        CASE 
            WHEN (COMPLETENESS_PERCENT + ACCURACY_SCORE + CONSISTENCY_SCORE + TIMELINESS_SCORE + VALIDITY_SCORE) / 5 >= 90 THEN 'GOOD'
            WHEN (COMPLETENESS_PERCENT + ACCURACY_SCORE + CONSISTENCY_SCORE + TIMELINESS_SCORE + VALIDITY_SCORE) / 5 >= 80 THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END as QUALITY_STATUS,
        DATA_OWNER,
        DATA_STEWARD,
        DATA_CLASSIFICATION,
        RETENTION_PERIOD_DAYS,
        LAST_QUALITY_CHECK,
        QUALITY_TREND,
        ISSUES_COUNT,
        RESOLVED_ISSUES_COUNT,
        LAST_UPDATE_TIMESTAMP,
        NEXT_QUALITY_REVIEW_DATE
    FROM (
        SELECT 
            'BCBS239_DATA_QUALITY' as DATA_SOURCE,
            'RISK_DATA' as DATA_TYPE,
            'COMPLETENESS' as QUALITY_DIMENSION,
            97.9 as COMPLETENESS_PERCENT,
            95.4 as ACCURACY_SCORE,
            96.9 as CONSISTENCY_SCORE,
            98.7 as TIMELINESS_SCORE,
            96.1 as VALIDITY_SCORE,
            'RISK_MANAGEMENT' as DATA_OWNER,
            'DATA_STEWARD_001' as DATA_STEWARD,
            'RESTRICTED' as DATA_CLASSIFICATION,
            2555 as RETENTION_PERIOD_DAYS,
            CURRENT_TIMESTAMP as LAST_QUALITY_CHECK,
            'STABLE' as QUALITY_TREND,
            6 as ISSUES_COUNT,
            4 as RESOLVED_ISSUES_COUNT,
            CURRENT_TIMESTAMP as LAST_UPDATE_TIMESTAMP,
            CURRENT_DATE + 30 as NEXT_QUALITY_REVIEW_DATE
        FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_BCBS239_RISK_AGGREGATION
        LIMIT 1
    )
);
