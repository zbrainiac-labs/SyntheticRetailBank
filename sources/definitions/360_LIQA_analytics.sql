/*
 * 360_LIQA_analytics.sql
 * Liquidity analytics: LCR computation and monitoring
 */
DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_HQLA(
    AS_OF_DATE DATE COMMENT 'Reporting date for HQLA positions (daily COB snapshot). Primary time dimension for portfolio composition analysis, asset allocation monitoring, and strategic rebalancing decisions. Used for time-series analysis of HQLA mix evolution and regulatory level distribution trends.',
    HQLA_LEVEL VARCHAR(10) COMMENT 'Basel III HQLA regulatory level classification: L1 (highest quality, 0% haircut), L2A (high quality, 15% haircut), L2B (acceptable quality, 50% haircut). Primary grouping dimension for regulatory reporting, cap rule monitoring, and portfolio quality assessment. Critical for automated compliance validation.',
    ASSET_TYPE VARCHAR(50) COMMENT 'HQLA asset type code from eligibility rules (e.g., CASH_SNB, GOVT_BOND_CHF, CANTON_BOND, EQUITY_SMI). Secondary grouping dimension for granular portfolio analysis, concentration risk monitoring, and strategic asset allocation. Links to LIQI_RAW_TB_HQLA_ELIGIBILITY for regulatory metadata.',
    HOLDINGS_COUNT NUMBER(10,0) COMMENT 'Number of individual securities/positions within this asset type for this reporting date. Portfolio diversification metric indicating concentration risk. Low counts (1-2) indicate potential single-security dependence; high counts (20+) indicate well-diversified holdings. Used for issuer concentration analysis and custody risk assessment.',
    MARKET_VALUE_CHF NUMBER(18,2) COMMENT 'Total gross market value in CHF before regulatory haircuts. Sum of all securities in this asset type at market prices. Used for portfolio valuation, P&L attribution, and comparing pre-haircut vs post-haircut values. Represents fair value of holdings before regulatory adjustments for liquidity stress.',
    HAIRCUT_PCT NUMBER(5,2) COMMENT 'Regulatory haircut percentage applied to this asset type per Basel III (0%, 15%, or 50%). Standard haircut from LIQI_RAW_TB_HQLA_ELIGIBILITY reference table. Used for user-friendly reporting in dashboards and executive summaries. Displayed as percentage for business readability.',
    HQLA_VALUE_CHF NUMBER(18,2) COMMENT 'Total HQLA value in CHF after applying regulatory haircuts (Market_Value × Haircut_Factor). The actual liquidity buffer contribution from this asset type. Used for LCR numerator calculation, portfolio optimization, and rebalancing decisions. Represents stressed value available during 30-day liquidity crisis per Basel III assumptions.',
    PCT_OF_TOTAL_HQLA NUMBER(5,2) COMMENT 'Percentage of total HQLA stock represented by this asset type. Portfolio composition metric for strategic asset allocation monitoring and diversification assessment. High percentages (over 30% in single asset type) may indicate concentration risk. Used for portfolio rebalancing triggers and strategic liquidity planning.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp when this aggregation was executed in Snowflake. Used for data lineage tracking, audit trail maintenance, and validating calculation freshness. Critical for ensuring portfolio metrics reflect latest market values and holdings changes. Monitored for SLA compliance (must be within 60 minutes of AS_OF_DATE).'
) COMMENT = 'Daily HQLA holdings aggregated by regulatory level (L1, L2A, L2B) and asset type (CASH_SNB, GOVT_BOND_CHF, etc.). Provides intermediate-level detail between raw security holdings and fully aggregated HQLA totals. Includes market values, regulatory haircuts, HQLA values after haircuts, and holding counts for diversification analysis. Source data for semantic views, portfolio composition dashboards, and AI agent queries about HQLA breakdown. Used by Treasury for strategic asset allocation, by Risk for concentration monitoring, and by Compliance for regulatory reporting preparation. Updated hourly for near-real-time portfolio composition visibility.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH hqla_base AS (
    SELECT
        h.AS_OF_DATE,
        h.HOLDING_ID,
        h.ASSET_TYPE,
        h.MARKET_VALUE_CHF,
        h.HQLA_ELIGIBLE,
        e.REGULATORY_LEVEL,
        e.HAIRCUT_PCT,
        e.HAIRCUT_FACTOR,
        h.MARKET_VALUE_CHF * e.HAIRCUT_FACTOR AS HQLA_VALUE_CHF
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS h
    INNER JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY e
        ON h.ASSET_TYPE = e.ASSET_TYPE
    WHERE h.HQLA_ELIGIBLE = TRUE
      AND e.IS_ACTIVE = TRUE
),
hqla_aggregated AS (
    SELECT
        AS_OF_DATE,
        REGULATORY_LEVEL AS HQLA_LEVEL,
        ASSET_TYPE,
        COUNT(*) AS HOLDINGS_COUNT,
        SUM(MARKET_VALUE_CHF) AS MARKET_VALUE_CHF,
        MAX(HAIRCUT_PCT) AS HAIRCUT_PCT,  -- Same for all in group
        SUM(HQLA_VALUE_CHF) AS HQLA_VALUE_CHF
    FROM hqla_base
    GROUP BY AS_OF_DATE, REGULATORY_LEVEL, ASSET_TYPE
),
total_hqla_per_date AS (
    SELECT
        AS_OF_DATE,
        SUM(HQLA_VALUE_CHF) AS TOTAL_HQLA
    FROM hqla_aggregated
    GROUP BY AS_OF_DATE
)
SELECT
    h.AS_OF_DATE,
    h.HQLA_LEVEL,
    h.ASSET_TYPE,
    h.HOLDINGS_COUNT,
    ROUND(h.MARKET_VALUE_CHF, 2) AS MARKET_VALUE_CHF,
    h.HAIRCUT_PCT,
    ROUND(h.HQLA_VALUE_CHF, 2) AS HQLA_VALUE_CHF,
    ROUND((h.HQLA_VALUE_CHF / NULLIF(t.TOTAL_HQLA, 0)) * 100, 2) AS PCT_OF_TOTAL_HQLA,
    CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM hqla_aggregated h
LEFT JOIN total_hqla_per_date t
    ON h.AS_OF_DATE = t.AS_OF_DATE
ORDER BY h.AS_OF_DATE DESC, h.HQLA_LEVEL, h.HQLA_VALUE_CHF DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW(
    AS_OF_DATE DATE COMMENT 'Reporting date for deposit positions (daily COB snapshot). Primary time dimension for funding composition analysis, deposit stability monitoring, and customer relationship assessment. Used for time-series analysis of funding mix evolution and run-off rate trends by counterparty segment.',
    COUNTERPARTY_TYPE VARCHAR(50) COMMENT 'Counterparty classification: RETAIL (individuals), CORPORATE (businesses), FINANCIAL_INSTITUTION (banks/insurers). Primary grouping dimension for regulatory reporting, funding strategy development, and Basel III compliance. Determines base run-off rate assumptions and concentration limits per FINMA Circular 2015/2.',
    ACCOUNT_COUNT NUMBER(10,0) COMMENT 'Number of active deposit accounts in this counterparty segment for this reporting date. Deposit base breadth metric indicating diversification and concentration risk. High counts (over 5000) provide statistical diversification benefits; low counts (under 100) indicate concentration vulnerability. Used for customer acquisition effectiveness and retention program targeting.',
    CUSTOMER_COUNT NUMBER(10,0) COMMENT 'Number of unique customers with deposits in this counterparty segment. Customer relationship breadth metric distinct from account count (customers may have multiple accounts). Used for customer-level concentration analysis, relationship banking program effectiveness, and deposit insurance calculation (retail customers within CHF 100K limit).',
    BALANCE_CHF NUMBER(18,2) COMMENT 'Total deposit balance in CHF for this counterparty segment before stressed outflows. Aggregate funding base by customer type. Used for funding composition analysis, customer segment contribution assessment, and strategic pricing decisions. Represents total available funding under normal operating conditions.',
    RUN_OFF_RATE NUMBER(8,4) COMMENT 'Weighted average effective run-off rate as decimal (0.03 to 1.00) after applying relationship discounts, direct debit adjustments, and tenure penalties. Blended rate reflecting customer relationship quality and account maturity. Lower rates (under 0.06) indicate stable retail funding; higher rates (over 0.40) indicate volatile wholesale funding. Used for funding stability benchmarking.',
    OUTFLOW_AMOUNT_CHF NUMBER(18,2) COMMENT 'Total expected 30-day stressed outflow in CHF for this counterparty segment (Balance × Run_Off_Rate). Aggregate withdrawal amount assumed during Basel III liquidity stress scenario. Used for LCR denominator calculation, funding gap analysis, and contingency funding plan development. Represents required HQLA coverage for this funding segment.',
    PCT_OF_TOTAL_OUTFLOWS NUMBER(5,2) COMMENT 'Percentage of total stressed outflows represented by this counterparty segment. Funding composition metric for strategic funding strategy and concentration monitoring. High percentages (over 50% from single segment) may indicate funding concentration risk requiring diversification. Used for funding source optimization and deposit pricing strategy.',
    PCT_OF_TOTAL_DEPOSITS NUMBER(5,2) COMMENT 'Percentage of total deposit balances represented by this counterparty segment. Funding base composition metric distinct from outflow percentage. Comparison of PCT_OF_TOTAL_DEPOSITS vs PCT_OF_TOTAL_OUTFLOWS reveals relative stability (retail high deposits, low outflows = stable). Used for strategic funding mix optimization.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp when this aggregation was executed in Snowflake. Used for data lineage tracking, audit trail maintenance, and validating calculation freshness. Critical for ensuring funding metrics reflect latest deposit balances and customer relationship changes. Monitored for SLA compliance (must be within 60 minutes of AS_OF_DATE).'
) COMMENT = 'Daily deposit balances and stressed outflows aggregated by counterparty type (RETAIL, CORPORATE, FINANCIAL_INSTITUTION). Provides intermediate-level detail between individual accounts and fully aggregated outflow totals. Includes balances, weighted average run-off rates after relationship adjustments, outflow amounts, and account/customer counts. Source data for semantic views, funding composition dashboards, and AI agent queries about deposit breakdown. Used by Treasury for funding strategy, by ALM for stress testing, and by Retail Banking for relationship management. Updated hourly for near-real-time funding composition visibility.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH deposit_enriched AS (
    SELECT
        d.AS_OF_DATE,
        d.ACCOUNT_ID,
        d.CUSTOMER_ID,
        d.DEPOSIT_TYPE,
        d.BALANCE_CHF,
        d.IS_INSURED,
        d.PRODUCT_COUNT,
        d.ACCOUNT_TENURE_DAYS,
        d.HAS_DIRECT_DEBIT,
        d.IS_OPERATIONAL,
        d.COUNTERPARTY_TYPE,
        d.CUSTOMER_SEGMENT,
        dt.BASE_RUN_OFF_RATE,
        dt.ALLOWS_RELATIONSHIP_DISCOUNT,
        CASE
            WHEN dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.PRODUCT_COUNT >= 3
            THEN dt.BASE_RUN_OFF_RATE - 0.02  -- -2% for 3+ products
            ELSE dt.BASE_RUN_OFF_RATE
        END AS ADJUSTED_RUN_OFF_STEP1,
        CASE
            WHEN dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.HAS_DIRECT_DEBIT
            THEN ADJUSTED_RUN_OFF_STEP1 - 0.01  -- -1% for direct debit
            ELSE ADJUSTED_RUN_OFF_STEP1
        END AS ADJUSTED_RUN_OFF_STEP2,
        CASE
            WHEN d.ACCOUNT_TENURE_DAYS < (18 * 30)
            THEN ADJUSTED_RUN_OFF_STEP2 + 0.05  -- +5% for new accounts
            ELSE ADJUSTED_RUN_OFF_STEP2
        END AS ADJUSTED_RUN_OFF_STEP3,
        GREATEST(0.03, LEAST(1.00, ADJUSTED_RUN_OFF_STEP3)) AS FINAL_RUN_OFF_RATE
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES d
    INNER JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES dt
        ON d.DEPOSIT_TYPE = dt.DEPOSIT_TYPE
    WHERE d.ACCOUNT_STATUS = 'ACTIVE'
      AND dt.IS_ACTIVE = TRUE
),
deposit_outflows AS (
    SELECT
        AS_OF_DATE,
        ACCOUNT_ID,
        CUSTOMER_ID,
        COUNTERPARTY_TYPE,
        BALANCE_CHF,
        FINAL_RUN_OFF_RATE,
        BALANCE_CHF * FINAL_RUN_OFF_RATE AS OUTFLOW_AMOUNT_CHF
    FROM deposit_enriched
),
outflows_by_type AS (
    SELECT
        AS_OF_DATE,
        COUNTERPARTY_TYPE,
        COUNT(*) AS ACCOUNT_COUNT,
        COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMER_COUNT,
        SUM(BALANCE_CHF) AS BALANCE_CHF,
        SUM(OUTFLOW_AMOUNT_CHF) / NULLIF(SUM(BALANCE_CHF), 0) AS RUN_OFF_RATE,
        SUM(OUTFLOW_AMOUNT_CHF) AS OUTFLOW_AMOUNT_CHF
    FROM deposit_outflows
    GROUP BY AS_OF_DATE, COUNTERPARTY_TYPE
),
total_per_date AS (
    SELECT
        AS_OF_DATE,
        SUM(BALANCE_CHF) AS TOTAL_DEPOSITS,
        SUM(OUTFLOW_AMOUNT_CHF) AS TOTAL_OUTFLOWS
    FROM outflows_by_type
    GROUP BY AS_OF_DATE
)
SELECT
    o.AS_OF_DATE,
    o.COUNTERPARTY_TYPE,
    o.ACCOUNT_COUNT,
    o.CUSTOMER_COUNT,
    ROUND(o.BALANCE_CHF, 2) AS BALANCE_CHF,
    ROUND(o.RUN_OFF_RATE, 4) AS RUN_OFF_RATE,
    ROUND(o.OUTFLOW_AMOUNT_CHF, 2) AS OUTFLOW_AMOUNT_CHF,
    ROUND((o.OUTFLOW_AMOUNT_CHF / NULLIF(t.TOTAL_OUTFLOWS, 0)) * 100, 2) AS PCT_OF_TOTAL_OUTFLOWS,
    ROUND((o.BALANCE_CHF / NULLIF(t.TOTAL_DEPOSITS, 0)) * 100, 2) AS PCT_OF_TOTAL_DEPOSITS,
    CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM outflows_by_type o
LEFT JOIN total_per_date t
    ON o.AS_OF_DATE = t.AS_OF_DATE
ORDER BY o.AS_OF_DATE DESC, o.OUTFLOW_AMOUNT_CHF DESC;

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
    L2B_COUNT NUMBER(10,0) COMMENT 'Number of Level 2B HQLA securities in portfolio. Used for equity and corporate bond diversification within Level 2B classification. SMI stocks should be spread across sectors to reduce correlation risk. High count may indicate fragmented portfolio; low count may indicate concentration risk. Balance between diversification and operational complexity.',
    TOTAL_HOLDINGS NUMBER(10,0) COMMENT 'Total number of HQLA securities across all regulatory levels. Portfolio complexity metric for operational management, custody arrangements, and collateral management systems. High count (over 100) may indicate over-diversification and operational inefficiency; low count (under 20) may indicate concentration risk. Used for staffing decisions and technology requirements.',
    CAP_APPLIED BOOLEAN COMMENT 'Boolean flag indicating whether 40% Basel III cap was triggered (TRUE = L2 exceeded 2/3 of L1 and was reduced). When TRUE, triggers Treasury alert to rebalance portfolio by converting L2B to L1 assets or increasing L1 holdings. Chronic TRUE status indicates structural portfolio imbalance requiring strategic adjustment. Monitored daily by ALM and reported to ALCO monthly.',
    DISCARDED_L2 NUMBER(18,2) COMMENT 'CHF amount of Level 2 assets excluded from HQLA stock due to 40% cap breach. Represents opportunity cost - liquidity buffer that provides no LCR benefit. Can be optimized by: (1) converting to L1 assets, (2) using for other balance sheet purposes, or (3) reducing holdings. High values (over CHF 100M) trigger strategic review by Treasury and ALCO.',
    MAX_L2_ALLOWED NUMBER(18,2) COMMENT 'Maximum Level 2 assets allowed under 40% cap rule: 2/3 × L1_Total. Treasury operational limit for Level 2 portfolio management. When L2_Uncapped approaches this threshold (within 90%), pre-emptive portfolio rebalancing initiated. Used for forward-looking capacity planning and strategic asset allocation decisions. Daily monitoring by Treasury desk.',
    L2_TO_L1_PCT NUMBER(8,2) COMMENT 'Level 2 to Level 1 ratio expressed as percentage. Regulatory maximum 66.67% (2/3) per Basel III. Target operating range typically 40-60% for buffer below cap. Values approaching 66% trigger rebalancing actions. Key metric for Treasury dashboards, ALCO reporting, and regulatory submissions to SNB. Used for assessing portfolio composition quality and optimization opportunities.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp when this HQLA calculation was executed in Snowflake. Used for data lineage tracking, audit trail maintenance, and validating calculation freshness. Critical for ensuring LCR metrics reflect latest market values and portfolio changes. Monitored for SLA compliance (must be within 60 minutes of AS_OF_DATE for intraday reporting).'
) COMMENT = 'Daily HQLA stock calculation with Basel III 40% cap rule applied. Aggregates eligible liquid assets by regulatory level (L1, L2A, L2B), applies required haircuts, and implements the regulatory cap limiting Level 2 assets to 2/3 of Level 1 assets. Forms the numerator of the LCR ratio calculation. Updated hourly for real-time liquidity monitoring and FINMA compliance reporting. Critical for Treasury decision-making and regulatory submissions to Swiss National Bank (SNB).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH hqla_base AS (
    SELECT
        h.AS_OF_DATE,
        h.HOLDING_ID,
        h.ASSET_TYPE,
        h.ISIN,
        h.SECURITY_NAME,
        h.MARKET_VALUE_CHF,
        h.HQLA_ELIGIBLE,
        h.INELIGIBILITY_REASON,
        e.REGULATORY_LEVEL,
        e.HAIRCUT_FACTOR,
        e.SNB_COORDINATE,
        h.MARKET_VALUE_CHF * e.HAIRCUT_FACTOR AS WEIGHTED_VALUE_CHF
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS h
    INNER JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY e
        ON h.ASSET_TYPE = e.ASSET_TYPE
    WHERE h.HQLA_ELIGIBLE = TRUE
      AND e.IS_ACTIVE = TRUE
),
hqla_by_level AS (
    SELECT
        AS_OF_DATE,
        REGULATORY_LEVEL,
        COUNT(*) AS HOLDING_COUNT,
        SUM(MARKET_VALUE_CHF) AS GROSS_VALUE_CHF,
        SUM(WEIGHTED_VALUE_CHF) AS WEIGHTED_VALUE_CHF
    FROM hqla_base
    GROUP BY AS_OF_DATE, REGULATORY_LEVEL
),
hqla_pivot AS (
    SELECT
        AS_OF_DATE,
        MAX(CASE WHEN REGULATORY_LEVEL = 'L1' THEN WEIGHTED_VALUE_CHF ELSE 0 END) AS L1_TOTAL,
        MAX(CASE WHEN REGULATORY_LEVEL = 'L2A' THEN WEIGHTED_VALUE_CHF ELSE 0 END) AS L2A_TOTAL,
        MAX(CASE WHEN REGULATORY_LEVEL = 'L2B' THEN WEIGHTED_VALUE_CHF ELSE 0 END) AS L2B_TOTAL,
        MAX(CASE WHEN REGULATORY_LEVEL = 'L1' THEN HOLDING_COUNT ELSE 0 END) AS L1_COUNT,
        MAX(CASE WHEN REGULATORY_LEVEL = 'L2A' THEN HOLDING_COUNT ELSE 0 END) AS L2A_COUNT,
        MAX(CASE WHEN REGULATORY_LEVEL = 'L2B' THEN HOLDING_COUNT ELSE 0 END) AS L2B_COUNT
    FROM hqla_by_level
    GROUP BY AS_OF_DATE
),
hqla_capped AS (
    SELECT
        AS_OF_DATE,
        L1_TOTAL,
        L2A_TOTAL,
        L2B_TOTAL,
        L1_COUNT,
        L2A_COUNT,
        L2B_COUNT,
        (2.0 / 3.0) * L1_TOTAL AS MAX_L2_ALLOWED,
        L2A_TOTAL + L2B_TOTAL AS L2_UNCAPPED,
        CASE
            WHEN (L2A_TOTAL + L2B_TOTAL) <= (2.0 / 3.0) * L1_TOTAL THEN L2A_TOTAL + L2B_TOTAL
            ELSE (2.0 / 3.0) * L1_TOTAL
        END AS L2_CAPPED,
        (L2A_TOTAL + L2B_TOTAL) > (2.0 / 3.0) * L1_TOTAL AS CAP_APPLIED,
        CASE
            WHEN (L2A_TOTAL + L2B_TOTAL) > (2.0 / 3.0) * L1_TOTAL
            THEN (L2A_TOTAL + L2B_TOTAL) - (2.0 / 3.0) * L1_TOTAL
            ELSE 0
        END AS DISCARDED_L2
    FROM hqla_pivot
)
SELECT
    AS_OF_DATE,
    L1_TOTAL,
    L2A_TOTAL,
    L2B_TOTAL,
    L2_UNCAPPED,
    L2_CAPPED,
    L1_TOTAL + L2_CAPPED AS HQLA_TOTAL,
    L1_COUNT,
    L2A_COUNT,
    L2B_COUNT,
    L1_COUNT + L2A_COUNT + L2B_COUNT AS TOTAL_HOLDINGS,
    CAP_APPLIED,
    DISCARDED_L2,
    MAX_L2_ALLOWED,
    ROUND((L2_CAPPED / NULLIF(L1_TOTAL, 0)) * 100, 2) AS L2_TO_L1_PCT,
    CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM hqla_capped
ORDER BY AS_OF_DATE DESC;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_OUTFLOW_CALCULATION(
    AS_OF_DATE DATE COMMENT 'Reporting date for deposit positions (daily COB snapshot). Primary time dimension for funding stability analysis, deposit base evolution tracking, and monthly SNB regulatory submissions. Used for time-series analysis of outflow trends and stress testing deposit retention under Basel III scenarios.',
    OUTFLOW_RETAIL NUMBER(18,2) COMMENT 'Total expected 30-day outflows from retail customer deposits in CHF under stress scenario. Aggregates stable insured deposits (base 3% run-off), stable deposits (base 5%), and less-stable deposits (base 10%) with relationship-based discounts applied (-2% for 3+ products, -1% for direct debit). Typically most stable funding source due to deposit insurance, payment behavior, and relationship banking. Critical for retail funding strategy and customer relationship management.',
    OUTFLOW_CORP NUMBER(18,2) COMMENT 'Total expected 30-day outflows from corporate business deposits in CHF under stress scenario. Separates operational deposits (25% run-off for payroll, clearing accounts) from non-operational deposits (40% run-off for treasury deposits). Higher run-off rates reflect corporate treasury optimization behavior and active cash management. Used for commercial banking funding strategy and operational deposit designation validation.',
    OUTFLOW_FI NUMBER(18,2) COMMENT 'Total expected 30-day outflows from financial institution deposits (banks, insurance companies) in CHF. Assumes 100% immediate run-off per Basel III stress assumptions reflecting highly unstable wholesale funding. Most volatile and concentration-sensitive funding source. Monitored daily for counterparty concentration limits. Triggers immediate liquidity coverage requirements. Used for wholesale funding risk management and counterparty exposure limits.',
    OUTFLOW_TOTAL NUMBER(18,2) COMMENT 'Total expected 30-day stressed net cash outflows in CHF across all deposit types. The denominator in LCR ratio formula: LCR = (HQLA / Outflow_Total) × 100. Must maintain sufficient HQLA to cover this amount and achieve minimum 100% LCR per FINMA regulation. Critical metric for daily liquidity planning, intraday monitoring, funding cost optimization, and monthly regulatory reporting to SNB. Board-level metric for funding risk appetite.',
    RETAIL_ACCOUNTS NUMBER(10,0) COMMENT 'Count of active retail deposit accounts contributing to funding base. Used for deposit base size assessment, customer retention metrics, and relationship banking program effectiveness. Large stable retail base (over 10K accounts) provides diversification benefits and pricing power. Monitored for deposit concentration risk (no single retail customer should exceed 5% of retail deposits).',
    CORP_ACCOUNTS NUMBER(10,0) COMMENT 'Count of active corporate deposit accounts in commercial banking portfolio. Used for business banking client base analysis, operational deposit penetration rate calculation, and commercial relationship quality assessment. High operational account ratio indicates strong commercial relationships. Monitored for large depositor concentration risk per Basel III concentration limits.',
    FI_ACCOUNTS NUMBER(10,0) COMMENT 'Count of active financial institution deposit accounts (wholesale funding counterparties). Used for wholesale funding concentration monitoring and counterparty exposure management. High count indicates diversified wholesale funding; low count indicates concentration risk. Each FI counterparty subject to credit limits and monitored for credit rating changes. Target maximum 10% of total deposits from any single FI.',
    TOTAL_ACCOUNTS NUMBER(10,0) COMMENT 'Total number of active deposit accounts across all counterparty segments. Deposit base breadth metric for operational complexity, customer acquisition effectiveness, and funding diversification. Large account base (over 50K) provides statistical diversification benefits and reduces concentration risk. Used for operational staffing decisions, technology capacity planning, and deposit insurance calculations.',
    RETAIL_EFFECTIVE_RUN_OFF_PCT NUMBER(8,2) COMMENT 'Weighted average effective run-off rate for retail deposits as percentage after applying relationship discounts and tenure penalties. Lower values (under 6%) indicate strong customer relationships with multiple products and direct debit mandates. Higher values (over 8%) suggest less-stable retail base requiring deposit retention programs. Key performance indicator for Retail Banking relationship quality and deposit pricing strategy. Benchmark against peer banks for competitive positioning.',
    CORP_EFFECTIVE_RUN_OFF_PCT NUMBER(8,2) COMMENT 'Weighted average effective run-off rate for corporate deposits as percentage. Lower values (under 30%) indicate high operational deposit ratio and strong commercial relationships. Higher values (over 35%) suggest treasury deposits vulnerable to rate competition. Used for commercial relationship quality assessment, operational deposit designation strategy, and corporate pricing decisions. Drives commercial banking product bundling and cash management service offerings.',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp when this outflow calculation was executed in Snowflake. Used for data lineage tracking, audit trail maintenance, and validating calculation freshness. Critical for ensuring outflow metrics reflect latest deposit balances and customer relationship changes. Monitored for SLA compliance (must be within 60 minutes of AS_OF_DATE). Required for regulatory audit trail per FINMA data quality requirements.'
) COMMENT = 'Daily net cash outflow calculation with deposit run-off rates and relationship-based discounts. Applies Basel III stress assumptions to deposit balances, incorporating customer relationship strength (product count, direct debit) and account maturity. Aggregates expected 30-day outflows by counterparty type for liquidity planning. Forms the denominator of the LCR ratio calculation. Updated hourly for real-time liquidity monitoring and FINMA compliance reporting. Critical for funding risk assessment and deposit retention strategies.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
AS
WITH deposit_enriched AS (
    SELECT
        d.AS_OF_DATE,
        d.ACCOUNT_ID,
        d.CUSTOMER_ID,
        d.DEPOSIT_TYPE,
        d.BALANCE_CHF,
        d.IS_INSURED,
        d.PRODUCT_COUNT,
        d.ACCOUNT_TENURE_DAYS,
        d.HAS_DIRECT_DEBIT,
        d.IS_OPERATIONAL,
        d.COUNTERPARTY_TYPE,
        d.CUSTOMER_SEGMENT,
        dt.BASE_RUN_OFF_RATE,
        dt.ALLOWS_RELATIONSHIP_DISCOUNT,
        dt.SNB_COORDINATE,
        CASE
            WHEN dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.PRODUCT_COUNT >= 3
            THEN dt.BASE_RUN_OFF_RATE - 0.02  -- -2% for 3+ products
            ELSE dt.BASE_RUN_OFF_RATE
        END AS ADJUSTED_RUN_OFF_STEP1,
        CASE
            WHEN dt.ALLOWS_RELATIONSHIP_DISCOUNT AND d.HAS_DIRECT_DEBIT
            THEN ADJUSTED_RUN_OFF_STEP1 - 0.01  -- -1% for direct debit
            ELSE ADJUSTED_RUN_OFF_STEP1
        END AS ADJUSTED_RUN_OFF_STEP2,
        CASE
            WHEN d.ACCOUNT_TENURE_DAYS < (18 * 30)
            THEN ADJUSTED_RUN_OFF_STEP2 + 0.05  -- +5% for new accounts
            ELSE ADJUSTED_RUN_OFF_STEP2
        END AS ADJUSTED_RUN_OFF_STEP3,
        GREATEST(0.03, LEAST(1.00, ADJUSTED_RUN_OFF_STEP3)) AS FINAL_RUN_OFF_RATE
    FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES d
    INNER JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES dt
        ON d.DEPOSIT_TYPE = dt.DEPOSIT_TYPE
    WHERE d.ACCOUNT_STATUS = 'ACTIVE'
      AND dt.IS_ACTIVE = TRUE
),
deposit_outflows AS (
    SELECT
        AS_OF_DATE,
        ACCOUNT_ID,
        CUSTOMER_ID,
        DEPOSIT_TYPE,
        COUNTERPARTY_TYPE,
        BALANCE_CHF,
        FINAL_RUN_OFF_RATE,
        BALANCE_CHF * FINAL_RUN_OFF_RATE AS OUTFLOW_AMOUNT_CHF,
        SNB_COORDINATE
    FROM deposit_enriched
),
outflows_by_type AS (
    SELECT
        AS_OF_DATE,
        COUNTERPARTY_TYPE,
        COUNT(*) AS ACCOUNT_COUNT,
        COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMER_COUNT,
        SUM(BALANCE_CHF) AS TOTAL_BALANCE_CHF,
        SUM(OUTFLOW_AMOUNT_CHF) AS TOTAL_OUTFLOW_CHF,
        ROUND(SUM(OUTFLOW_AMOUNT_CHF) / NULLIF(SUM(BALANCE_CHF), 0) * 100, 2) AS EFFECTIVE_RUN_OFF_PCT
    FROM deposit_outflows
    GROUP BY AS_OF_DATE, COUNTERPARTY_TYPE
),
outflows_pivot AS (
    SELECT
        AS_OF_DATE,
        MAX(CASE WHEN COUNTERPARTY_TYPE = 'RETAIL' THEN TOTAL_OUTFLOW_CHF ELSE 0 END) AS OUTFLOW_RETAIL,
        MAX(CASE WHEN COUNTERPARTY_TYPE = 'CORPORATE' THEN TOTAL_OUTFLOW_CHF ELSE 0 END) AS OUTFLOW_CORP,
        MAX(CASE WHEN COUNTERPARTY_TYPE = 'FINANCIAL_INSTITUTION' THEN TOTAL_OUTFLOW_CHF ELSE 0 END) AS OUTFLOW_FI,
        MAX(CASE WHEN COUNTERPARTY_TYPE = 'RETAIL' THEN ACCOUNT_COUNT ELSE 0 END) AS RETAIL_ACCOUNTS,
        MAX(CASE WHEN COUNTERPARTY_TYPE = 'CORPORATE' THEN ACCOUNT_COUNT ELSE 0 END) AS CORP_ACCOUNTS,
        MAX(CASE WHEN COUNTERPARTY_TYPE = 'FINANCIAL_INSTITUTION' THEN ACCOUNT_COUNT ELSE 0 END) AS FI_ACCOUNTS,
        MAX(CASE WHEN COUNTERPARTY_TYPE = 'RETAIL' THEN EFFECTIVE_RUN_OFF_PCT ELSE 0 END) AS RETAIL_EFFECTIVE_RUN_OFF_PCT,
        MAX(CASE WHEN COUNTERPARTY_TYPE = 'CORPORATE' THEN EFFECTIVE_RUN_OFF_PCT ELSE 0 END) AS CORP_EFFECTIVE_RUN_OFF_PCT
    FROM outflows_by_type
    GROUP BY AS_OF_DATE
)
SELECT
    AS_OF_DATE,
    OUTFLOW_RETAIL,
    OUTFLOW_CORP,
    OUTFLOW_FI,
    OUTFLOW_RETAIL + OUTFLOW_CORP + OUTFLOW_FI AS OUTFLOW_TOTAL,
    RETAIL_ACCOUNTS,
    CORP_ACCOUNTS,
    FI_ACCOUNTS,
    RETAIL_ACCOUNTS + CORP_ACCOUNTS + FI_ACCOUNTS AS TOTAL_ACCOUNTS,
    RETAIL_EFFECTIVE_RUN_OFF_PCT,
    CORP_EFFECTIVE_RUN_OFF_PCT,
    CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM outflows_pivot
ORDER BY AS_OF_DATE DESC;

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
FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_HOLDINGS h
INNER JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_HQLA_ELIGIBILITY e
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
FROM {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_BALANCES d
INNER JOIN {{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DEPOSIT_TYPES dt
    ON d.DEPOSIT_TYPE = dt.DEPOSIT_TYPE
WHERE dt.IS_ACTIVE = TRUE
ORDER BY d.AS_OF_DATE DESC, d.BALANCE_CHF DESC;
