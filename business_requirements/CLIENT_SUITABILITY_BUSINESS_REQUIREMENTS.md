# Business Requirements: Client Investment Suitability Assessment (Cross-Project)

## Executive Summary

Cross-reference retail banking customer risk profiles with asset management portfolio positions and mandate limits. Automated suitability assessment for MiFID II (EU), FIDLEG (Switzerland), and FCA Consumer Duty (UK) compliance.

## Business Value

### Value Add

- Proactive suitability breach detection before regulatory audit -- automated daily checks across 5K+ clients
- Automated MiFID II/FIDLEG compliance evidence generation reduces manual review from weeks to minutes
- Identifies clients whose portfolio risk exceeds their stated risk tolerance (liability exposure)
- Advisor dashboard surfaces suitability issues ranked by severity, enabling prioritized remediation

### Risk of Inaction

- Suitability violations discovered during regulatory audit (fines up to 10% of annual revenue under MiFID II)
- Manual review cannot scale beyond a few hundred clients -- 5K clients = months of analyst time
- Advisors unknowingly maintain unsuitable portfolios for clients with changed circumstances
- No cross-system view between banking risk profile and portfolio holdings

## Business Context

Current state: Customer risk profiles exist in SyntheticRetailBank (IRB ratings, AML flags, account tier). Portfolio positions exist in SAM_DEMO (daily ABOR, mandate limits, factor exposures). These systems are not connected -- suitability is assessed manually, if at all.

Target state: Automated daily suitability assessment joining banking customer risk data with asset management portfolio data. Breaches flagged with severity, root cause, and recommended action.

## Stakeholders

| Role | Interest |
|------|----------|
| Chief Compliance Officer | Regulatory compliance evidence (MiFID II, FIDLEG) |
| Client Advisors | Proactive suitability alerts before client review meetings |
| Risk Management | Portfolio risk vs. client risk tolerance alignment |
| Internal Audit | Automated suitability testing evidence |
| Regulators (FINMA/FCA) | Demonstrable suitability governance framework |

## Functional Requirements

### FR-IS-01: Cross-Database Customer-Portfolio Join -- `NEW`
Create a view joining `AAA_DEV_SYNTHETIC_BANK.CRM_AGG_V001.CRMA_AGG_DT_CUSTOMER_360` (risk classification, credit score, account tier, income range) with `SAM_DEMO.CURATED.FACT_POSITION_DAILY_ABOR` (portfolio positions) and `SAM_DEMO.CURATED.DIM_PORTFOLIO` (portfolio metadata). Match on customer_id.

### FR-IS-02: Risk Tolerance Mapping -- `NEW`
Map banking risk classifications (LOW/MEDIUM/HIGH/CRITICAL) to portfolio risk tolerance bands. Define maximum equity allocation, maximum single-issuer concentration, and maximum currency exposure per risk band.

### FR-IS-03: Concentration Limit Check -- `NEW`
Check each client's portfolio against `SAM_DEMO.CURATED.FACT_RISK_LIMITS` (mandate concentration limits). Flag positions exceeding: single-issuer limit (default 10%), sector limit (default 25%), country limit (default 30%).

### FR-IS-04: Factor Exposure Assessment -- `NEW`
Compare client risk tolerance with portfolio factor exposures from `SAM_DEMO.CURATED.FACT_FACTOR_EXPOSURES` (7-factor model: market, size, value, momentum, quality, volatility, credit). Flag misaligned factor tilts.

### FR-IS-05: Suitability Breach Scoring -- `NEW`
Compute a suitability breach score per client combining: risk tolerance mismatch, concentration breaches, factor misalignment, PEP enhanced suitability requirements. Output: client_id, breach_score, breach_type, severity (INFO/WARNING/CRITICAL), recommended_action.

### FR-IS-06: Jurisdiction-Specific Rules -- `NEW`
Apply per-country suitability rules: MiFID II (EU clients), FIDLEG (Swiss clients), FCA Consumer Duty (UK clients). Rules differ on: product complexity limits, leverage restrictions, and reporting thresholds.

### FR-IS-07: Daily Dynamic Table Refresh -- `NEW`
Implement as a dynamic table refreshing daily. Upstream changes in customer risk profile or portfolio positions must trigger re-assessment.

### FR-IS-08: Advisor Suitability Dashboard -- `NEW`
Add a "Suitability" tab to the Streamlit banking dashboard. Show: breach summary by severity, drill-down per client, advisor workload (breaches per advisor), and remediation tracking.

### FR-IS-09: Semantic View for Agent -- `NEW`
Create semantic view enabling natural language queries: "Which clients have suitability breaches?", "Show me all CRITICAL suitability issues for Swiss clients".

### FR-IS-10: Audit Evidence Report -- `NEW`
Generate a regulatory-ready suitability report per client: assessment date, risk profile, portfolio summary, suitability result, any breaches, and remediation status. Exportable as structured data.

## Snowflake Features

- Cross-database views (AAA_DEV_SYNTHETIC_BANK + SAM_DEMO)
- Dynamic table for automated daily assessment
- Semantic view for Cortex Agent integration

## Required Data Sources

| Source | Database | Schema | Object | Status | Purpose |
|--------|----------|--------|--------|--------|---------|
| Customer 360 | AAA_DEV_SYNTHETIC_BANK | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360 | EXISTS | Risk classification, credit score, account tier |
| IRB ratings | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_IRB_CUSTOMER_RATINGS | EXISTS | Default probability per customer |
| Equity positions | AAA_DEV_SYNTHETIC_BANK | EQT_AGG_V001 | EQTA_AGG_DT_PORTFOLIO_POSITIONS | EXISTS | Customer equity holdings |
| FI positions | AAA_DEV_SYNTHETIC_BANK | FII_AGG_V001 | FIIA_AGG_DT_PORTFOLIO_POSITIONS | EXISTS | Customer fixed income holdings |
| Commodity positions | AAA_DEV_SYNTHETIC_BANK | CMD_AGG_V001 | CMDA_AGG_DT_PORTFOLIO_POSITIONS | EXISTS | Customer commodity holdings |
| PEP data | AAA_DEV_SYNTHETIC_BANK | CRM_RAW_V001 | CRMI_RAW_TB_EXPOSED_PERSON | EXISTS | PEP status for enhanced suitability |
| Portfolio master | SAM_DEMO | CURATED | DIM_PORTFOLIO | EXISTS | Portfolio metadata |
| Security master | SAM_DEMO | CURATED | DIM_SECURITY | EXISTS | Security risk attributes |
| Daily positions (ABOR) | SAM_DEMO | CURATED | FACT_POSITION_DAILY_ABOR | EXISTS | Daily portfolio holdings |
| Risk limits | SAM_DEMO | CURATED | FACT_RISK_LIMITS | EXISTS | Mandate concentration limits |
| Factor exposures | SAM_DEMO | CURATED | FACT_FACTOR_EXPOSURES | EXISTS | 7-factor risk decomposition |
| Client mandates | SAM_DEMO | CURATED | DIM_CLIENT_MANDATES | EXISTS | Client investment mandates |

## Acceptance Criteria

- Suitability assessment runs daily for all active clients
- Concentration breaches detected against mandate limits with zero false negatives
- Jurisdiction rules correctly applied per client country (CH/UK/EU)
- PEP clients automatically flagged for enhanced suitability review
- Results accessible via Streamlit dashboard and Cortex Agent
