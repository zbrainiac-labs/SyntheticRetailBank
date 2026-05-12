# Business Requirements: Consolidated Risk Dashboard (Cross-Project)

## Executive Summary

Unified CRO-level risk view combining retail banking exposures (credit risk, liquidity, market risk) with asset management portfolio risk (factor exposures, benchmark tracking error, ESG risk). Single dashboard for enterprise-wide risk aggregation across both business lines.

## Business Value

### Value Add

- Holistic risk view for CRO and Board: see banking + asset management risk on one screen
- BCBS 239 Principle 1 (Governance) compliance: enterprise-wide risk aggregation across business lines
- Early detection of correlated risks: a credit crisis in banking may coincide with portfolio drawdowns in asset management
- Regulatory examination readiness: consolidated risk data available in minutes, not weeks

### Risk of Inaction

- Siloed risk views per business line -- CRO cannot see total enterprise exposure
- BCBS 239 non-compliance on Principle 1 (risk aggregation) and Principle 2 (data architecture)
- Correlated risks across business lines go undetected until crisis materializes
- Board receives fragmented risk reports requiring manual consolidation

## Business Context

Current state: Banking risk data (IRB, FRTB, LCR, BCBS 239) exists in SyntheticRetailBank. Asset management risk data (factor exposures, benchmark tracking, compliance alerts) exists in SAM_DEMO. No consolidated view.

Target state: A single dynamic table and Streamlit dashboard combining both risk domains. CRO can drill from enterprise summary into banking or asset management detail.

## Stakeholders

| Role | Interest |
|------|----------|
| Chief Risk Officer | Enterprise-wide risk aggregation |
| Board of Directors | Consolidated risk reporting |
| FINMA/ECB Examiners | BCBS 239 compliance evidence |
| Treasury | Cross-business liquidity view |
| Internal Audit | Enterprise risk data lineage |

## Functional Requirements

### FR-RD-01: Banking Risk Aggregation View -- `NEW`
Create a view aggregating banking risk metrics: total credit RWA (from `REPP_AGG_DT_IRB_PORTFOLIO_METRICS`), market risk capital (from `REPP_AGG_DT_FRTB_CAPITAL_CHARGES`), current LCR ratio (from `REPP_AGG_DT_LCR_DAILY`), and BCBS 239 data quality score (from `REPP_AGG_DT_BCBS239_DATA_QUALITY`).

### FR-RD-02: Asset Management Risk Aggregation View -- `NEW`
Create a view aggregating asset management risk: total AUM, active risk (tracking error vs benchmark from `FACT_BENCHMARK_PERFORMANCE`), factor concentrations (from `FACT_FACTOR_EXPOSURES`), mandate breach count (from `FACT_COMPLIANCE_ALERTS`), and ESG score distribution (from `FACT_ESG_SCORES`).

### FR-RD-03: Cross-Business Risk Correlation -- `NEW`
Identify correlated exposures across business lines. Example: if banking has high credit exposure to sector X AND asset management holds securities in sector X, flag the concentration. Join banking FRTB risk positions with SAM portfolio positions by sector/issuer.

### FR-RD-04: Enterprise Risk Score -- `NEW`
Compute a composite enterprise risk score (0-100) combining: banking credit risk weight (40%), market risk weight (30%), liquidity risk weight (20%), asset management active risk weight (10%). Configurable weights.

### FR-RD-05: Risk Limit Monitoring -- `NEW`
Compare enterprise risk metrics against defined limits. Combine banking `REPP_AGG_DT_BCBS239_RISK_LIMITS` with asset management `FACT_RISK_LIMITS`. Flag breaches with severity and trend direction.

### FR-RD-06: Daily Dynamic Table -- `NEW`
Implement as a cross-database dynamic table refreshing daily. Must detect changes in both banking and asset management upstream tables.

### FR-RD-07: CRO Streamlit Dashboard -- `NEW`
Add a "Consolidated Risk" tab to the Streamlit dashboard. Layout: enterprise risk score gauge, banking risk summary (IRB/FRTB/LCR), asset management risk summary (factor/tracking/ESG), cross-business correlation alerts, and limit breach timeline.

### FR-RD-08: Cortex Agent Integration -- `NEW`
Create semantic view enabling CRO-level queries: "What is our total enterprise risk exposure?", "Are there correlated sector concentrations across banking and asset management?", "Show risk limit breaches across both business lines".

### FR-RD-09: Trend Analysis -- `NEW`
Compute 30-day rolling trends for all enterprise risk metrics. Flag deteriorating trends (> 1 standard deviation move) with automated alerts.

### FR-RD-10: Board Reporting View -- `NEW`
Create a summary view formatted for board reporting: top 5 risk concerns, risk appetite utilization (%), key changes since last reporting period, and recommended actions.

## Snowflake Features

- Cross-database views (AAA_DEV_SYNTHETIC_BANK + SAM_DEMO)
- Dynamic table for automated daily aggregation
- Semantic view for Cortex Agent integration

## Required Data Sources

| Source | Database | Schema | Object | Status | Purpose |
|--------|----------|--------|--------|--------|---------|
| IRB portfolio metrics | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_IRB_PORTFOLIO_METRICS | EXISTS | Aggregate credit RWA |
| IRB RWA summary | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_IRB_RWA_SUMMARY | EXISTS | RWA breakdown |
| FRTB capital charges | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_FRTB_CAPITAL_CHARGES | EXISTS | Market risk capital |
| FRTB sensitivities | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_FRTB_SENSITIVITIES | EXISTS | Risk factor sensitivities |
| LCR daily | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_LCR_DAILY | EXISTS | Liquidity coverage ratio |
| BCBS 239 aggregation | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_BCBS239_RISK_AGGREGATION | EXISTS | Risk aggregation |
| BCBS 239 data quality | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_BCBS239_DATA_QUALITY | EXISTS | Data quality metrics |
| BCBS 239 risk limits | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_BCBS239_RISK_LIMITS | EXISTS | Banking risk limits |
| BCBS 239 executive | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_BCBS239_EXECUTIVE_DASHBOARD | EXISTS | Executive KPIs |
| Portfolio positions | SAM_DEMO | CURATED | FACT_POSITION_DAILY_ABOR | EXISTS | Daily AM holdings |
| Benchmark performance | SAM_DEMO | CURATED | FACT_BENCHMARK_PERFORMANCE | EXISTS | Benchmark returns |
| Factor exposures | SAM_DEMO | CURATED | FACT_FACTOR_EXPOSURES | EXISTS | 7-factor risk model |
| Compliance alerts | SAM_DEMO | CURATED | FACT_COMPLIANCE_ALERTS | EXISTS | Mandate breach alerts |
| ESG scores | SAM_DEMO | CURATED | FACT_ESG_SCORES | EXISTS | Security ESG ratings |
| AM risk limits | SAM_DEMO | CURATED | FACT_RISK_LIMITS | EXISTS | AM mandate limits |

## Acceptance Criteria

- Enterprise risk score computed daily from both business lines
- Cross-business sector concentration detection works correctly
- All banking and AM risk limit breaches surfaced in a single view
- CRO can drill from enterprise summary into banking or AM detail
- Dashboard loads in under 5 seconds
