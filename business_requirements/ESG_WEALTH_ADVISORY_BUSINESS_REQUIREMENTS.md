# Business Requirements: ESG-Aware Wealth Advisory (Cross-Project)

## Executive Summary

Enrich the SyntheticRetailBank Customer 360 with ESG preferences and security-level ESG scores from the asset management platform. Enable advisors to recommend ESG-aligned portfolio adjustments and generate sustainability compliance reports (EU SFDR, Swiss AMAS guidelines).

## Business Value

### Value Add

- EU SFDR compliance readiness: automated sustainability preference assessment per client
- Advisors can proactively recommend green alternatives using pre-screened replacements from SAM_DEMO
- Client retention for ESG-conscious HNW segment (fastest-growing wealth segment in EMEA)
- Portfolio-level ESG scoring enables marketing of ESG-themed products

### Risk of Inaction

- No ESG visibility at client level -- advisors cannot answer "how sustainable is my portfolio?"
- SFDR reporting gaps: unable to classify clients by sustainability preference (Article 6/8/9)
- Losing ESG-conscious clients to competitors with sustainability dashboards
- ESG controversies detected by SAM CrewAI agents are not linked to affected client portfolios

## Business Context

Current state: ESG scores exist at security level in SAM_DEMO (`FACT_ESG_SCORES`). ESG controversies are detected by CrewAI `crew_esg`. Customer profiles exist in SyntheticRetailBank. These datasets are not connected -- no way to see ESG impact at client or advisor level.

Target state: Each customer's portfolio has an aggregated ESG score. Advisors see ESG alerts when their clients hold securities involved in controversies. Pre-screened ESG-compliant alternatives are suggested.

## Stakeholders

| Role | Interest |
|------|----------|
| Wealth Advisors | ESG-aligned recommendations for client meetings |
| Sustainability Officer | SFDR compliance reporting |
| Product Management | ESG product suitability assessment |
| Client Relationship Managers | Client retention for ESG-conscious segment |
| Compliance | Sustainability disclosure requirements |

## Functional Requirements

### FR-ES-01: Portfolio ESG Score Aggregation -- `NEW`
For each customer, compute a weighted-average ESG score across their equity and fixed income holdings. Join `EQTA_AGG_DT_PORTFOLIO_POSITIONS` with `SAM_DEMO.CURATED.FACT_ESG_SCORES` by security identifier. Weight by position market value.

### FR-ES-02: ESG Score Decomposition -- `NEW`
Break down the portfolio ESG score into E (Environmental), S (Social), and G (Governance) components. Show which securities are the top ESG detractors and contributors.

### FR-ES-03: ESG Controversy Alert Integration -- `NEW`
When SAM CrewAI `crew_esg` detects an ESG controversy for an issuer, identify all customers holding that issuer's securities. Generate per-advisor alerts: "3 of your clients hold positions in [ISSUER] which has an active ESG controversy: [DESCRIPTION]".

### FR-ES-04: SFDR Client Classification -- `NEW`
Classify each customer's sustainability preference based on their actual holdings: Article 6 (no ESG integration), Article 8 (promotes E/S characteristics), Article 9 (sustainable investment objective). Use thresholds on portfolio ESG score.

### FR-ES-05: Pre-Screened ESG Replacements -- `NEW`
When a client holds a low-ESG-score security, suggest alternatives from `SAM_DEMO.CURATED.FACT_PRE_SCREENED_REPLACEMENTS`. Show: replacement security, ESG score improvement, expected return impact, sector alignment.

### FR-ES-06: Customer 360 ESG Enrichment -- `NEW`
Add ESG fields to the Customer 360 view: portfolio_esg_score, sfdr_classification, esg_controversy_count, top_esg_detractor. Visible in Customer 360 Streamlit tab.

### FR-ES-07: Advisor ESG Dashboard -- `NEW`
Add ESG columns to the existing Advisor Management Streamlit tab: average client ESG score, number of clients with active controversies, SFDR classification distribution per advisor's book.

### FR-ES-08: Holdings-with-ESG View -- `NEW`
Leverage existing `SAM_DEMO.CURATED.V_HOLDINGS_WITH_ESG` view and enrich with customer context from SyntheticRetailBank. Create a cross-database view joining holdings + ESG + customer risk profile.

### FR-ES-09: Semantic View for Agent -- `NEW`
Create semantic view enabling queries: "Which of my clients have the lowest ESG scores?", "Show me clients holding securities with active ESG controversies", "What are the best ESG replacement options for client X?".

### FR-ES-10: Notebook Visualization -- `NEW`
Create an interactive notebook (`notebooks/esg_wealth_advisory.ipynb`) showing: portfolio ESG distribution, controversy impact analysis, SFDR classification breakdown, and replacement opportunity sizing.

## Snowflake Features

- Cross-database views (AAA_DEV_SYNTHETIC_BANK + SAM_DEMO)
- Dynamic table for ESG score aggregation
- Semantic view for Cortex Agent integration

## Required Data Sources

| Source | Database | Schema | Object | Status | Purpose |
|--------|----------|--------|--------|--------|---------|
| Customer 360 | AAA_DEV_SYNTHETIC_BANK | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360 | EXISTS | Customer demographics, risk profile |
| Equity positions | AAA_DEV_SYNTHETIC_BANK | EQT_AGG_V001 | EQTA_AGG_DT_PORTFOLIO_POSITIONS | EXISTS | Customer equity holdings |
| FI positions | AAA_DEV_SYNTHETIC_BANK | FII_AGG_V001 | FIIA_AGG_DT_PORTFOLIO_POSITIONS | EXISTS | Customer fixed income holdings |
| Advisor performance | AAA_DEV_SYNTHETIC_BANK | CRM_AGG_V001 | EMPA_AGG_DT_ADVISOR_PERFORMANCE | EXISTS | Advisor-client relationships |
| Portfolio performance | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_PORTFOLIO_PERFORMANCE | EXISTS | Portfolio returns (TWR, Sharpe) |
| ESG scores | SAM_DEMO | CURATED | FACT_ESG_SCORES | EXISTS | Security-level ESG ratings |
| Holdings with ESG | SAM_DEMO | CURATED | V_HOLDINGS_WITH_ESG | EXISTS | Pre-joined holdings + ESG |
| ESG latest | SAM_DEMO | CURATED | V_ESG_LATEST | EXISTS | Latest ESG scores per security |
| Security master | SAM_DEMO | CURATED | DIM_SECURITY | EXISTS | Security ESG classification |
| Pre-screened replacements | SAM_DEMO | CURATED | FACT_PRE_SCREENED_REPLACEMENTS | EXISTS | ESG-compliant alternatives |
| ESG controversy reports | SAM_DEMO | SAM_RAW_V001 | NEWS_RAW_TB_ESG_COMMITTEE_RUNS | EXISTS | CrewAI ESG controversy output |

## Acceptance Criteria

- Portfolio ESG score computed for all customers with equity/FI holdings
- SFDR classification correctly assigned based on configurable thresholds
- ESG controversy alerts linked to affected customers within 24 hours
- Pre-screened replacements suggested for low-ESG securities
- Results visible in Customer 360 and Advisor Management Streamlit tabs
