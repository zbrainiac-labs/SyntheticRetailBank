# Synthetic Retail Bank

A comprehensive synthetic banking environment demonstrating modern risk management, governance, and compliance challenges faced by EMEA financial institutions. Managed via Snowflake DCM (Database Change Management) with automated CI/CD.

---

## Quick Start

```bash
git clone https://github.com/zbrainiac-labs/SyntheticRetailBank.git
cd SyntheticRetailBank

snow connection add <my-sf-connection>

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# One-time setup (requires ACCOUNTADMIN):
snow sql -c <my-sf-connection> -f operation/setup_listings.sql

./data_generator.sh 5000 --clean
./upload-data.sh --CONNECTION_NAME=<my-sf-connection>
```

---

## DCM Project Structure

```
SyntheticRetailBank/
├── manifest.yml                    DCM manifest (v2, targets: DEV + PROD)
├── pre_deploy.sql                  DB/schema/project creation
├── post_deploy.sql                 Streams, file formats, procedures, agents, semantic views
├── github-workflow-verification_v1.sh  Workflow SHA256 integrity check
├── .github/workflows/
│   └── update-local-repo.yml       CI/CD: DCM deploy, SonarQube, clone/test/drop, release
├── sources/
│   ├── definitions/
│   │   ├── 000_infrastructure.sql  Schemas (16), tag
│   │   ├── 01x_*_ingestion.sql    CRM, accounts, employees
│   │   ├── 02x_*_ingestion.sql    FX rates
│   │   ├── 03x_*_ingestion.sql    Payments, SWIFT
│   │   ├── 04x-06x_*_ingestion.sql  Equities, fixed income, commodities, LCR, loans
│   │   ├── 3xx_*_analytics.sql    Dynamic tables (analytics layer)
│   │   ├── 4xx_*_analytics.sql    Customer 360, employees, loans
│   │   ├── 5xx_*_reporting.sql    Dynamic tables + views (reporting)
│   │   ├── 600_REPP_portfolio.sql  Portfolio performance
│   │   └── 900_access.sql          GRANT statements
│   └── macros/
│       └── common.sql              Shared Jinja macros
├── sqlunit/
│   └── tests.sqltest               59 SQL validation tests
├── notebooks/                      9 interactive Snowflake notebooks
├── the_bank_app/                   16-tab Streamlit banking dashboard
├── business_requirements/          Business requirement documents
├── generators/                     21 Python data generator modules
├── generated_data/                 Output CSV/XML files
├── operation/                      Manual SQL operations
└── data_generator.sh               Data generation wrapper
```

### DCM Project

| Property | Value |
|----------|-------|
| Project Identifier | `AAA_DEV_SYNTHETIC_BANK.AAA_DCM.SYNTHETIC_RETAIL_BANK` |
| Account | `SFSEEUROPE-ZS28104` |
| Targets | `DEV`, `PROD` |
| Owner | `CICD` |
| Warehouse | `MD_TEST_WH` (not DCM-managed) |

### Object Inventory

| Layer | Objects | Type |
|-------|---------|------|
| Infrastructure | 16 schemas, 1 tag | DEFINE SCHEMA, DEFINE TAG |
| Raw Ingestion | 17 stages, 29 tables, 32 tasks | DEFINE STAGE/TABLE/TASK |
| Analytics | 71 dynamic tables | DEFINE DYNAMIC TABLE |
| Reporting | 28 views | DEFINE VIEW |
| Access | 9 grants | GRANT statements |
| Post-deploy | 17 streams, 14 file formats, 7 procedures, 15 semantic views, 6 agents | CREATE (unsupported by DCM) |

---

## CI/CD Workflow

Triggered on push to `main` or manual dispatch:

1. Workflow integrity check (SHA256)
2. Pre-deploy SQL (DB, schema, DCM project, marketplace listings)
3. DCM Analyze + Plan + Deploy
4. Post-deploy SQL (streams, file formats, agents, semantic views)
5. Extract dependencies
6. SonarQube scan
7. Clone schema for regression tests
8. SQL validation (sqlunit)
9. Drop cloned schema
10. Create GitHub release

---

## Data Generators

| Type | Generators |
|------|------------|
| Master Data | `customer_generator`, `employee_generator`, `pep_generator` |
| Transactions | `pay_transaction_generator`, `equity_generator`, `fixed_income_generator`, `commodity_generator` |
| Supporting | `fx_generator`, `swift_generator`, `mortgage_email_generator`, `lcr_data_generator` |
| Lifecycle | `customer_lifecycle_generator`, `address_update_generator`, `customer_update_generator` |
| Compliance | `anomaly_patterns` (AML testing) |

### Data Volumes

| Customers | Employees | Transactions | Equity Trades | FI Trades | Commodity | SWIFT XMLs | Runtime |
|-----------|-----------|--------------|---------------|-----------|-----------|------------|---------|
| 1,000 | 15-18 | ~150K | ~290K | ~1.8K | ~740 | ~600 | 5-7 min |
| 5,000 | 39 | ~760K | ~1.45M | ~9K | ~3.7K | ~4.8K | 10 min |
| 10,000 | 66 | ~1.5M | ~2.9M | ~18K | ~7.4K | ~10K | 15-20 min |

---

## Domains

| Code | Domain | Schemas |
|------|--------|---------|
| CRM | Customer Information | CRM_RAW_V001, CRM_AGG_V001 |
| ACC | Accounts | (within CRM_RAW_V001) |
| EMP | Employees | (within CRM_RAW_V001) |
| PAY | Payments | PAY_RAW_V001, PAY_AGG_V001 |
| ICG | SWIFT Messaging | (within PAY_RAW_V001, PAY_AGG_V001) |
| EQT | Equity Trading | EQT_RAW_V001, EQT_AGG_V001 |
| FII | Fixed Income | FII_RAW_V001, FII_AGG_V001 |
| CMD | Commodities | CMD_RAW_V001, CMD_AGG_V001 |
| REF | Reference Data | REF_RAW_V001, REF_AGG_V001 |
| REP | Reporting | REP_RAW_V001, REP_AGG_V001 |
| LOA | Loans | LOA_RAW_V001, LOA_AGG_V001 |
| LIQ | Liquidity (LCR) | (within REP_RAW_V001, REP_AGG_V001) |

---

## Interactive Notebooks

| Notebook | Audience |
|----------|----------|
| Customer Screening & KYC | CCO, Compliance |
| AML & Transaction Monitoring | AML Teams, FIU |
| Sanctions & Embargo | Sanctions Officer, Legal |
| Compliance Risk Mgmt | CCO, Board, Audit |
| Controls & Data Quality | Internal Audit, Data Gov |
| Employee Relationship Mgmt | Wealth Mgmt, COO, HR |
| Wealth Management | Wealth Advisors |
| Lending Operations | Lending, Credit |
| Liquidity Risk (LCR) | Treasury, Risk, FINMA |

---

## Cortex Agents

| Agent | Domain | Semantic Views |
|-------|--------|----------------|
| CRM Customer 360 | CRM | CRMA_SV_CUSTOMER_360, EMPA_SV_EMPLOYEE_ADVISOR |
| Compliance Monitoring | Payments | PAYA_SV_COMPLIANCE_MONITORING |
| Risk & Regulatory | Reporting | REPA_SV_RISK_REPORTING |
| Wealth Advisor | Reporting | REPA_SV_WEALTH_MANAGEMENT |
| Liquidity Risk | Reporting | LCRS_SV_LCR_CURRENT, LCRS_SV_HQLA_BREAKDOWN, LCRS_SV_OUTFLOW_BREAKDOWN, LCRS_SV_TREND_90DAY, LCRS_SV_ALERTS_ACTIVE |
| Loan Portfolio | Reporting | LOAS_SV_PORTFOLIO_CURRENT, LOAS_SV_LTV_DISTRIBUTION, LOAS_SV_APPLICATION_FUNNEL, LOAS_SV_AFFORDABILITY_ANALYSIS, LOAS_SV_COMPLIANCE_SCREENING |

---

## Streamlit App (the_bank_app)

16-tab banking dashboard with AI-powered analytics:

| Tab | Features |
|-----|----------|
| Customer 360 | Full search, profile, identity, accounts, risk |
| Risk & Compliance | Risk distribution, PEP/sanctions status |
| Portfolio Analytics | Tier distribution, geographic, multi-currency |
| Fraud Detection | Anomaly priority queue, overlap analysis |
| Churn & Lifecycle | Churn probability, dormant reactivation, revenue-at-risk |
| AML Monitoring | Alert trends, severity classification |
| Lending Operations | Credit risk distribution, score bands |
| Wealth Management | Advisor AUM, capacity, performance |
| Sanctions Control | Exact/fuzzy screening, risk levels |
| Advisor Management | Workload status, team performance |
| KYC Screening | PEP matches, completeness by country |
| Data Quality | Completeness metrics, missing data |
| LCR Monitoring | Ratio gauge, HQLA, outflows, trend, SNB summary |
| Loans Portfolio | LTV, application funnel, affordability, compliance |
| Ask AI | Natural language queries via Cortex Agents |
| Settings | Connection management |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: 'faker'` | `pip install -r requirements.txt` |
| `Customer file not found` | `./data_generator.sh 5000 --clean` |
| `Connection not found` | `snow connection add <connection>` |
| `Stream has no data` | `./upload-data.sh --CONNECTION_NAME=<connection>` |
| AGG/REP tables empty after upload | Run `operation/execute_all_tasks_and_refresh_dts.sql` |
