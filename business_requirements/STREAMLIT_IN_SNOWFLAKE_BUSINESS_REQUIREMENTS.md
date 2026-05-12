# Business Requirements: Streamlit in Snowflake Deployment (Phase 3 -- Technical Migration)

## Executive Summary

Deploy the existing 16-tab Streamlit banking dashboard as a native Streamlit in Snowflake (SiS) application. Eliminates local installation requirements, centralizes access control via Snowflake RBAC, and enables zero-install demos for prospects, regulators, and internal stakeholders.

## Business Value

### Value Add

- Zero-install demo: share a Snowsight URL instead of asking users to clone a repo and set up Python
- Centralized access control via Snowflake RBAC (no local credential management)
- Production-ready deployment path for the banking dashboard
- Demonstrates Streamlit in Snowflake as an enterprise application platform

### Risk of Inaction

- Every demo requires local Python setup (friction, support overhead)
- Credentials stored in local secrets.toml files (security risk)
- App cannot be shared with prospects or regulators without local setup
- Dashboard remains a prototype rather than a deployable product

## Business Context

Current state: The Streamlit app (`the_bank_app/`) runs locally, requiring Python, pip dependencies, and a manually configured Snowflake connection. Each user must clone the repo, set up a virtual environment, and manage credentials locally.

Target state: A production-grade Streamlit in Snowflake application accessible directly from Snowsight. Authentication is handled by Snowflake SSO. Data access is governed by existing RBAC roles. Cortex Agent integration works natively without REST API workarounds.

## Stakeholders

| Role | Interest |
|------|----------|
| Sales Engineering | Zero-install demo for prospects -- share a URL |
| Compliance Officers | Secure access without local data exposure |
| Board Members | Browser-based access to dashboards |
| IT Security | Centralized authentication, no local credentials |
| DevOps | Simplified deployment via DCM or snow CLI |

## Functional Requirements

### FR-SI-01: SiS Application Deployment -- `NEW`
Deploy all 16 tabs of `the_bank_app/app.py` as a native Streamlit in Snowflake application via `snow streamlit deploy` or DCM post_deploy.sql.

### FR-SI-02: Connection Layer Refactor -- `NEW`
Replace `snowflake.connector.connect()` in `utils/snowflake_connection.py` with `snowflake.snowpark.context.get_active_session()`. Remove all local credential management (secrets.toml).

### FR-SI-03: Cortex Agent Call Refactor -- `NEW`
Replace REST API calls in `utils/agent_caller.py` with native `snowflake.cortex` Python SDK or direct SQL `SELECT SNOWFLAKE.CORTEX.COMPLETE(...)` calls. Eliminate external HTTP dependencies.

### FR-SI-04: Dependency Audit -- `NEW`
Audit `requirements.txt` against Snowflake Anaconda channel. Replace unavailable packages with SiS-compatible alternatives. No file system access allowed -- all data must come from Snowflake tables/stages.

### FR-SI-05: Full Functionality Preservation -- `NEW`
All 16 tabs must render correctly in Snowsight: Customer 360 search/drill-down, risk/compliance dashboards, LCR monitoring gauges, loan portfolio analytics, and "Ask AI" natural language interface.

### FR-SI-06: RBAC Tab Visibility -- `NEW`
Implement role-based tab access: Public role sees Customer 360 and Portfolio Analytics; Compliance role sees AML, KYC, Sanctions, Risk tabs; Executive role sees all tabs including Settings.

### FR-SI-07: Performance Target -- `NEW`
Page load time under 5 seconds for each tab. Data refreshes must reflect dynamic table updates within target lag.

## Snowflake Features

- Streamlit in Snowflake (SiS) for native deployment
- `snowflake.snowpark` session for data access
- `snowflake.cortex` SDK for AI function calls
- RBAC integration for tab-level access control

## Required Data Sources

All existing data sources used by the current local app, accessed via Snowpark session:

| Source | Schema | Objects | Status | Tabs Using |
|--------|--------|---------|--------|------------|
| Customer 360 | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360, views | EXISTS | Customer 360, Risk, KYC |
| Accounts | CRM_AGG_V001 | ACCA_AGG_DT_ACCOUNTS | EXISTS | Customer 360, Portfolio |
| Advisors | CRM_AGG_V001 | EMPA_AGG_DT_* views | EXISTS | Advisor Mgmt, Wealth |
| Transactions | PAY_AGG_V001 | PAYA_AGG_DT_* | EXISTS | Fraud, AML, Portfolio |
| SWIFT messages | PAY_AGG_V001 | ICGA_AGG_DT_SWIFT_* | EXISTS | AML, Sanctions |
| Equity positions | EQT_AGG_V001 | EQTA_AGG_DT_* | EXISTS | Wealth, Portfolio |
| LCR data | REP_AGG_V001 | REPP_AGG_DT_LCR_* | EXISTS | LCR Monitoring |
| Credit risk | REP_AGG_V001 | REPP_AGG_DT_IRB_* | EXISTS | Risk, Lending |
| Loan portfolio | REP_AGG_V001 | LOAR_AGG_DT_* | EXISTS | Loans Portfolio |
| Cortex Agents | Various | 6 agents via semantic views | EXISTS | Ask AI |

## Migration Steps

1. Audit `requirements.txt` against Snowflake Anaconda channel availability
2. Refactor `utils/snowflake_connection.py` for SiS session
3. Refactor `utils/agent_caller.py` for native Cortex calls
4. Replace `utils/data_loaders.py` SQL execution with `session.sql()`
5. Test all 16 tabs in SiS environment
6. Add deployment to `post_deploy.sql` or via `snow streamlit deploy`

## Acceptance Criteria

- All 16 tabs render correctly in Snowsight
- No local dependencies required (no pip install, no secrets.toml)
- Cortex Agent "Ask AI" works natively without REST API
- Page load time under 5 seconds for each tab
- Data refreshes reflect dynamic table updates within target lag
- Application accessible via shareable Snowsight URL
- RBAC controls restrict tab visibility based on user role
