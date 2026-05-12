# Business Requirements: Regulatory Reporting Analyst (Cortex Analyst)

## Executive Summary

A natural language interface for regulatory reporting that enables board members, regulators, and risk officers to query complex risk and compliance data without SQL knowledge. Built on Cortex Analyst with purpose-built semantic views and verified query representations (VQRs).

## Business Value

### Value Add

- Board members and regulators can self-serve risk data without depending on data teams
- FINMA examination preparation drops from weeks to hours: pre-built VQRs cover the most common regulatory questions
- Cross-domain queries (LCR + IRB + FRTB + BCBS 239) that previously required custom SQL are answered in natural language
- Demonstrates AI-powered regulatory reporting as a governance showcase

### Risk of Inaction

- Every regulatory question requires a data analyst to write SQL (bottleneck, cost, delay)
- Board members receive static reports instead of interactive exploration
- Cross-domain risk questions remain unanswered or require multi-day turnaround
- No natural language access to the regulatory data already in the platform

## Business Context

Current state: Six Cortex Agents exist for domain-specific queries (CRM, compliance, risk, wealth, LCR, loans). However, regulatory reporting questions often span multiple domains (e.g., "What is our total risk-weighted asset exposure for PEP customers with equity positions exceeding 1M CHF?"). No single agent covers cross-domain regulatory queries.

Target state: A dedicated Regulatory Reporting Analyst semantic view that joins risk, compliance, trading, and customer data. Pre-configured with verified queries matching common regulatory examination questions (FINMA, ECB, BIS).

## Stakeholders

| Role | Interest |
|------|----------|
| Board of Directors | Plain-language access to risk dashboards |
| Chief Risk Officer | Ad-hoc regulatory scenario analysis |
| FINMA Examiners | Self-service access during on-site inspections |
| Internal Audit | Evidence gathering for audit findings |
| Compliance Reporting | Automated responses to regulatory data requests |

## Functional Requirements

### FR-RA-01: Unified Regulatory Semantic View -- `NEW`
Create semantic view `REPA_SV_REGULATORY_DASHBOARD` joining data across 5 regulatory domains: LCR liquidity (HQLA, outflows, daily LCR), IRB credit risk (customer ratings, RWA, portfolio metrics), FRTB market risk (capital charges, sensitivities, NMRF), BCBS 239 compliance (risk aggregation, data quality, risk limits), and customer risk profile (PEP status, sanctions, anomalies).

### FR-RA-02: LCR Verified Queries -- `NEW`
Implement VQRs for LCR questions: "What is today's LCR ratio and are we above the 100% minimum?", "Show the 90-day LCR trend with regulatory threshold". Validate against `REPP_AGG_DT_LCR_DAILY` and `REPP_AGG_DT_LCR_TREND`.

### FR-RA-03: IRB Credit Risk Verified Queries -- `NEW`
Implement VQRs: "Show me the top 10 customers by risk-weighted assets", "How many PEP customers have positions exceeding 500K CHF?". Validate against `REPP_AGG_DT_IRB_CUSTOMER_RATINGS` and `CRMI_RAW_TB_EXPOSED_PERSON`.

### FR-RA-04: FRTB Market Risk Verified Queries -- `NEW`
Implement VQRs: "Which asset classes have the highest FRTB capital charges?", "What is our total market risk capital requirement?". Validate against `REPP_AGG_DT_FRTB_CAPITAL_CHARGES` and `REPP_AGG_DT_FRTB_SENSITIVITIES`.

### FR-RA-05: BCBS 239 Compliance Verified Queries -- `NEW`
Implement VQRs: "Are we compliant with BCBS 239 Principle 6 (accuracy)?". Validate against `REPP_AGG_DT_BCBS239_DATA_QUALITY` and `REPP_AGG_DT_BCBS239_EXECUTIVE_DASHBOARD`.

### FR-RA-06: Cross-Domain Risk Queries -- `NEW`
Implement VQRs spanning multiple domains: "List all customers with high credit risk and active sanctions flags". Must join IRB ratings with PEP/sanctions and anomaly analysis.

### FR-RA-07: Cortex Agent Wrapper -- `NEW`
Create a dedicated Cortex Agent `REGULATORY_REPORTING_ANALYST` wrapping the semantic view. Agent must handle follow-up questions and maintain conversation context.

### FR-RA-08: Streamlit Regulatory Mode -- `NEW`
Add a "Regulatory Reporting" mode selector to the existing "Ask AI" Streamlit tab. When selected, routes queries to the regulatory agent instead of the general-purpose agents.

### FR-RA-09: Query Audit Trail -- `NEW`
Log all regulatory queries to an audit table: user, timestamp, question text, generated SQL, result row count, response time. Required for FINMA examination evidence.

### FR-RA-10: Notebook Demonstration -- `NEW`
Create an interactive notebook (`notebooks/regulatory_analyst.ipynb`) demonstrating all 8 VQRs with expected results, showing Cortex Analyst API usage and result validation.

## Snowflake Features

- Cortex Analyst (semantic view + VQRs)
- Semantic view with cross-schema joins
- Cortex Agent wrapping the semantic view for conversational access

## Required Data Sources

| Source | Schema | Object | Status | Purpose |
|--------|--------|--------|--------|---------|
| LCR daily | REP_AGG_V001 | REPP_AGG_DT_LCR_DAILY | EXISTS | Daily LCR ratio |
| LCR trend | REP_AGG_V001 | REPP_AGG_DT_LCR_TREND | EXISTS | 90-day LCR history |
| HQLA calculation | REP_AGG_V001 | REPP_AGG_DT_LCR_HQLA_CALCULATION | EXISTS | HQLA components |
| IRB ratings | REP_AGG_V001 | REPP_AGG_DT_IRB_CUSTOMER_RATINGS | EXISTS | Per-customer credit ratings |
| IRB portfolio | REP_AGG_V001 | REPP_AGG_DT_IRB_PORTFOLIO_METRICS | EXISTS | Aggregate RWA metrics |
| FRTB capital | REP_AGG_V001 | REPP_AGG_DT_FRTB_CAPITAL_CHARGES | EXISTS | Market risk capital |
| FRTB sensitivities | REP_AGG_V001 | REPP_AGG_DT_FRTB_SENSITIVITIES | EXISTS | Risk factor sensitivities |
| BCBS 239 aggregation | REP_AGG_V001 | REPP_AGG_DT_BCBS239_RISK_AGGREGATION | EXISTS | Aggregated risk positions |
| BCBS 239 data quality | REP_AGG_V001 | REPP_AGG_DT_BCBS239_DATA_QUALITY | EXISTS | Data quality metrics |
| BCBS 239 executive | REP_AGG_V001 | REPP_AGG_DT_BCBS239_EXECUTIVE_DASHBOARD | EXISTS | Executive KPIs |
| Customer 360 | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360 | EXISTS | Customer risk profiles |
| Anomaly analysis | REP_AGG_V001 | REPP_AGG_DT_ANOMALY_ANALYSIS | EXISTS | AML anomaly flags |
| High-risk patterns | REP_AGG_V001 | REPP_AGG_DT_HIGH_RISK_PATTERNS | EXISTS | Suspicious activity patterns |
| PEP data | CRM_RAW_V001 | CRMI_RAW_TB_EXPOSED_PERSON | EXISTS | Politically exposed persons |

## Acceptance Criteria

- Semantic view successfully joins data across CRM, PAY, EQT, and REP schemas
- All 8 verified queries return correct, validated results
- Cortex Analyst answers regulatory questions with < 3 second response time
- Non-technical users (board members) can use the interface without training
- Query audit trail captures: user, timestamp, question, generated SQL, result summary
- Integration into Streamlit "Ask AI" tab with regulatory mode toggle
