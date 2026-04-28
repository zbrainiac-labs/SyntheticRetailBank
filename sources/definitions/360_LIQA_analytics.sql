DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_HQLA(
    AS_OF_DATE DATE COMMENT 'Reporting date for HQLA positions (daily COB snapshot). Primary time dimension for portfolio composition analysis, asset allocation monitoring, and strategic rebalancing decisions. Used for time-series analysis of HQLA mix evolution and regulatory level distribution trends.',
    HQLA_LEVEL VARCHAR(10) COMMENT 'Basel III HQLA regulatory level classification: L1 (highest quality, 0% haircut), L2A (high quality, 15% haircut), L2B (acceptable quality, 50% haircut). Primary grouping dimension for regulatory reporting, cap rule monitoring, and portfolio quality assessment. Critical for automated compliance validation.',
    ASSET_TYPE VARCHAR(50) COMMENT 'HQLA asset type code from eligibility rules (e.g., CASH_SNB, GOVT_BOND_CHF, CANTON_BOND, EQUITY_SMI). Secondary grouping dimension for granular portfolio analysis, concentration risk monitoring, and strategic asset allocation. Links to {{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY for regulatory metadata.',
    HOLDINGS_COUNT NUMBER(10,0) COMMENT 'Number of individual securities/positions within this asset type for this reporting date. Portfolio diversification metric indicating concentration risk. Low counts (1-2) indicate potential single-security dependence;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW(
    AS_OF_DATE DATE COMMENT 'Reporting date for deposit positions (daily COB snapshot). Primary time dimension for funding composition analysis, deposit stability monitoring, and customer relationship assessment. Used for time-series analysis of funding mix evolution and run-off rate trends by counterparty segment.',
    COUNTERPARTY_TYPE VARCHAR(50) COMMENT 'Counterparty classification: RETAIL (individuals), CORPORATE (businesses), FINANCIAL_INSTITUTION (banks/insurers). Primary grouping dimension for regulatory reporting, funding strategy development, and Basel III compliance. Determines base run-off rate assumptions and concentration limits per FINMA Circular 2015/2.',
    ACCOUNT_COUNT NUMBER(10,0) COMMENT 'Number of active deposit accounts in this counterparty segment for this reporting date. Deposit base breadth metric indicating diversification and concentration risk. High counts (over 5000) provide statistical diversification benefits;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_HQLA_CALCULATION(
    AS_OF_DATE DATE COMMENT 'Reporting date for HQLA positions (daily COB snapshot). Primary time dimension for LCR trend analysis, historical liquidity monitoring, and monthly SNB regulatory submissions. Used for time-series analysis of liquidity buffer evolution and stress testing scenarios.',
    L1_TOTAL NUMBER(18,2) COMMENT 'Total Level 1 HQLA value in CHF after 0% haircut. Includes SNB reserves (highest quality), physical cash, Swiss Confederation bonds, and foreign government bonds rated AA- or higher. Most liquid and highest-quality assets with immediate convertibility and no restrictions. Core liquidity buffer providing maximum LCR benefit. Used for strategic liquidity planning and overnight funding decisions.',
    L2A_TOTAL NUMBER(18,2) COMMENT 'Total Level 2A HQLA value in CHF after 15% regulatory haircut. Includes Swiss canton bonds (cantonal government debt) and covered bonds (Pfandbriefe mortgage-backed securities). High-quality liquid assets with minor marketability discount. Used for yield optimization while maintaining strong LCR contribution. Preferred over L2B for liquidity buffer composition.',
    L2B_TOTAL NUMBER(18,2) COMMENT 'Total Level 2B HQLA value in CHF after 50% regulatory haircut. Includes SMI constituent equities (major Swiss stocks) and AA- rated corporate bonds. Lower-quality liquid assets with significant haircut reflecting higher volatility and liquidation risk. Used for portfolio diversification but limited by 40% cap rule. Subject to Treasury rebalancing when cap is breached.',
    L2_UNCAPPED NUMBER(18,2) COMMENT 'Total Level 2 assets (L2A + L2B) before applying 40% regulatory cap. Gross exposure to Level 2 assets before cap enforcement. Used for monitoring proximity to cap threshold, triggering pre-emptive portfolio rebalancing, and identifying over-concentration in Level 2 holdings. Gap between uncapped and capped indicates portfolio optimization opportunity.',
    L2_CAPPED NUMBER(18,2) COMMENT 'Total Level 2 assets after applying Basel III 40% cap rule (max 2/3 of L1). Final L2 value included in HQLA numerator per FINMA Circular 2015/2. When L2_Uncapped exceeds cap, excess is discarded and cannot contribute to LCR. Triggers Treasury alert to convert L2B to L1 assets for LCR optimization. Critical for accurate regulatory reporting.',
    HQLA_TOTAL NUMBER(18,2) COMMENT 'Final total HQLA stock in CHF (L1 + L2_capped). The numerator in LCR ratio calculation: LCR = (HQLA_Total / Net_Outflows) × 100. Must maintain minimum CHF 2B for systemic banks per Swiss regulation. Critical metric for daily liquidity risk management, intraday monitoring, and monthly FINMA/SNB reporting. Board-level metric for strategic liquidity risk appetite.',
    L1_COUNT NUMBER(10,0) COMMENT 'Number of Level 1 HQLA securities in portfolio. Used for Level 1 diversification analysis across asset types (cash, SNB reserves, government bonds). Low count with high concentration may indicate over-reliance on single security type. Monitored for operational risk and custody concentration. Typically 5-15 positions for mid-sized bank.',
    L2A_COUNT NUMBER(10,0) COMMENT 'Number of Level 2A HQLA securities in portfolio. Used for canton bond and covered bond diversification assessment. High count indicates well-diversified Level 2A holdings reducing single-issuer concentration risk. Monitored for issuer limits and custodian concentration. Optimal range 10-30 positions depending on portfolio size.',
    L2B_COUNT NUMBER(10,0) COMMENT 'Number of Level 2B HQLA securities in portfolio. Used for equity and corporate bond diversification within Level 2B classification. SMI stocks should be spread across sectors to reduce correlation risk. High count may indicate fragmented portfolio;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW_CALCULATION(
    AS_OF_DATE DATE COMMENT 'Reporting date for deposit positions (daily COB snapshot). Primary time dimension for funding stability analysis, deposit base evolution tracking, and monthly SNB regulatory submissions. Used for time-series analysis of outflow trends and stress testing deposit retention under Basel III scenarios.',
    OUTFLOW_RETAIL NUMBER(18,2) COMMENT 'Total expected 30-day outflows from retail customer deposits in CHF under stress scenario. Aggregates stable insured deposits (base 3% run-off), stable deposits (base 5%), and less-stable deposits (base 10%) with relationship-based discounts applied (-2% for 3+ products, -1% for direct debit). Typically most stable funding source due to deposit insurance, payment behavior, and relationship banking. Critical for retail funding strategy and customer relationship management.',
    OUTFLOW_CORP NUMBER(18,2) COMMENT 'Total expected 30-day outflows from corporate business deposits in CHF under stress scenario. Separates operational deposits (25% run-off for payroll, clearing accounts) from non-operational deposits (40% run-off for treasury deposits). Higher run-off rates reflect corporate treasury optimization behavior and active cash management. Used for commercial banking funding strategy and operational deposit designation validation.',
    OUTFLOW_FI NUMBER(18,2) COMMENT 'Total expected 30-day outflows from financial institution deposits (banks, insurance companies) in CHF. Assumes 100% immediate run-off per Basel III stress assumptions reflecting highly unstable wholesale funding. Most volatile and concentration-sensitive funding source. Monitored daily for counterparty concentration limits. Triggers immediate liquidity coverage requirements. Used for wholesale funding risk management and counterparty exposure limits.',
    OUTFLOW_TOTAL NUMBER(18,2) COMMENT 'Total expected 30-day stressed net cash outflows in CHF across all deposit types. The denominator in LCR ratio formula: LCR = (HQLA / Outflow_Total) × 100. Must maintain sufficient HQLA to cover this amount and achieve minimum 100% LCR per FINMA regulation. Critical metric for daily liquidity planning, intraday monitoring, funding cost optimization, and monthly regulatory reporting to SNB. Board-level metric for funding risk appetite.',
    RETAIL_ACCOUNTS NUMBER(10,0) COMMENT 'Count of active retail deposit accounts contributing to funding base. Used for deposit base size assessment, customer retention metrics, and relationship banking program effectiveness. Large stable retail base (over 10K accounts) provides diversification benefits and pricing power. Monitored for deposit concentration risk (no single retail customer should exceed 5% of retail deposits).',
    CORP_ACCOUNTS NUMBER(10,0) COMMENT 'Count of active corporate deposit accounts in commercial banking portfolio. Used for business banking client base analysis, operational deposit penetration rate calculation, and commercial relationship quality assessment. High operational account ratio indicates strong commercial relationships. Monitored for large depositor concentration risk per Basel III concentration limits.',
    FI_ACCOUNTS NUMBER(10,0) COMMENT 'Count of active financial institution deposit accounts (wholesale funding counterparties). Used for wholesale funding concentration monitoring and counterparty exposure management. High count indicates diversified wholesale funding;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL 
    COMMENT = 'Security-level detail view of HQLA holdings with regulatory level classification and haircut calculations. Provides drill-down capability from aggregate HQLA metrics to individual securities for portfolio analysis, concentration risk monitoring, and regulatory reporting. Includes market values, weighted values after haircuts, credit ratings, and SNB coordinate mappings. Used by Treasury for portfolio rebalancing decisions and by Compliance for regulatory submission preparation.'
    AS
SELECT 
    h.AS_OF_DATE,
    h.HOLDING_ID,
    h.ASSET_TYPE,
    e.ASSET_NAME,
    e.REGULATORY_LEVEL,
    h.ISIN,
    h.SECURITY_NAME,
    h.CURRENCY,
    h.QUANTITY,
    h.MARKET_VALUE_CCY,
    h.MARKET_VALUE_CHF,
    e.HAIRCUT_PCT,
    e.HAIRCUT_FACTOR,
    h.MARKET_VALUE_CHF * e.HAIRCUT_FACTOR AS WEIGHTED_VALUE_CHF,
    h.MATURITY_DATE,
    h.CREDIT_RATING,
    h.SMI_CONSTITUENT,
    h.HQLA_ELIGIBLE,
    h.INELIGIBILITY_REASON,
    h.PORTFOLIO_CODE,
    h.CUSTODIAN,
    e.SNB_COORDINATE
FROM {{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS h
INNER JOIN {{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY e
    ON h.ASSET_TYPE = e.ASSET_TYPE
WHERE e.IS_ACTIVE = TRUE
ORDER BY h.AS_OF_DATE DESC, h.MARKET_VALUE_CHF DESC;

DEFINE VIEW {{ db }}.{{ rep_agg }}.REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL 
    COMMENT = 'Account-level detail view of deposit balances with run-off rate calculations and relationship-based discount logic. Provides drill-down capability from aggregate outflow metrics to individual accounts for funding stability analysis, customer relationship quality assessment, and deposit retention strategy optimization. Includes base run-off rates, relationship discounts (product count, direct debit), tenure penalties, and final calculated outflow amounts. Used by Treasury for funding planning and by Retail Banking for customer relationship management.'
    AS
SELECT 
    d.AS_OF_DATE,
    d.ACCOUNT_ID,
    d.CUSTOMER_ID,
    d.DEPOSIT_TYPE,
    dt.DEPOSIT_NAME,
    d.COUNTERPARTY_TYPE,
    d.CUSTOMER_SEGMENT,
    d.CURRENCY,
    d.BALANCE_CCY,
    d.BALANCE_CHF,
    dt.BASE_RUN_OFF_RATE,
    CASE 
        WHEN dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.PRODUCT_COUNT >= 3 
        THEN dt.BASE_RUN_OFF_RATE - 0.02
        ELSE dt.BASE_RUN_OFF_RATE
    END AS DISCOUNT_STEP1,
    CASE 
        WHEN dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.HAS_DIRECT_DEBIT 
        THEN DISCOUNT_STEP1 - 0.01
        ELSE DISCOUNT_STEP1
    END AS DISCOUNT_STEP2,
    CASE 
        WHEN d.ACCOUNT_TENURE_DAYS < (18 * 30) 
        THEN DISCOUNT_STEP2 + 0.05
        ELSE DISCOUNT_STEP2
    END AS PENALTY_APPLIED,
    GREATEST(0.03, LEAST(1.00, PENALTY_APPLIED)) AS FINAL_RUN_OFF_RATE,
    d.BALANCE_CHF * FINAL_RUN_OFF_RATE AS OUTFLOW_AMOUNT_CHF,
    d.IS_INSURED,
    d.PRODUCT_COUNT,
    d.ACCOUNT_TENURE_DAYS,
    d.HAS_DIRECT_DEBIT,
    d.IS_OPERATIONAL,
    d.ACCOUNT_STATUS,
    dt.SNB_COORDINATE
FROM {{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES d
INNER JOIN {{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES dt
    ON d.DEPOSIT_TYPE = dt.DEPOSIT_TYPE
WHERE dt.IS_ACTIVE = TRUE
ORDER BY d.AS_OF_DATE DESC, d.BALANCE_CHF DESC;
