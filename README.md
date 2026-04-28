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

./data_generator.sh 1000 --clean
./upload-data.sh --CONNECTION_NAME=<my-sf-connection>
```

---

## DCM Project Structure

```
SyntheticRetailBank/
├── manifest.yml                    DCM manifest (v2, target: DEV)
├── pre_deploy.sql                  DB/schema/project creation
├── post_deploy.sql                 Streams, file formats, procedures, agents, semantic views
├── github-workflow-verification_v1.sh  Workflow SHA256 integrity check
├── .github/workflows/
│   └── update-local-repo.yml       CI/CD: DCM deploy, SonarQube, clone/test/drop, release
├── sources/
│   └── definitions/
│       ├── 000_infrastructure.sql  Schemas (16), tag
│       ├── 010_CRMI_ingestion.sql  CRM: stages, tables, tasks
│       ├── 011_ACCI_ingestion.sql  Accounts: stage, table, tasks
│       ├── 015_EMPI_ingestion.sql  Employees: stages, tables, tasks
│       ├── 020_REFI_ingestion.sql  FX rates: stage, table, tasks
│       ├── 030_PAYI_ingestion.sql  Payments: stage, table, tasks
│       ├── 035_ICGI_ingestion.sql  SWIFT: stage, table, tasks
│       ├── 040_EQTI_ingestion.sql  Equities: stage, table, tasks
│       ├── 050_FIII_ingestion.sql  Fixed income: stage, table, tasks
│       ├── 055_CMDI_ingestion.sql  Commodities: stage, table, tasks
│       ├── 060_LIQI_ingestion.sql  LCR: stages, tables, tasks
│       ├── 065_LOAI_ingestion.sql  Loans: stages, tables, tasks
│       ├── 3xx_*_analytics.sql     Dynamic tables (analytics layer)
│       ├── 5xx_*_reporting.sql     Dynamic tables + views (reporting)
│       ├── 600_REPP_portfolio.sql  Portfolio performance
│       └── 900_access.sql          GRANT statements
├── sqlunit/
│   └── tests.sqltest               SQL validation tests
├── notebooks/                      Interactive Snowflake notebooks
├── generated_data/                 Python-generated CSV data
└── *.py                            Data generator modules
```

### DCM Project

| Property | Value |
|----------|-------|
| Project Identifier | `AAA_DEV_SYNTHETIC_BANK.PUBLIC.SYNTHETIC_RETAIL_BANK` |
| Account | `SFSEEUROPE-ZS28104` |
| Target | `DEV` |
| Owner | `CICD` |
| Warehouse | `MD_TEST_WH` (not DCM-managed) |

### Object Inventory

| Layer | Objects | Type |
|-------|---------|------|
| Infrastructure | 16 schemas, 1 tag | DEFINE SCHEMA, DEFINE TAG |
| Raw Ingestion | 17 stages, 29 tables, 32 tasks | DEFINE STAGE/TABLE/TASK |
| Analytics | 71 dynamic tables | DEFINE DYNAMIC TABLE |
| Reporting | 32 views | DEFINE VIEW |
| Access | 10 grants | GRANT statements |
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
| Supporting | `fx_generator`, `swift_generator`, `mortgage_email_generator` |
| Lifecycle | `customer_lifecycle_generator`, `address_update_generator` |
| Compliance | `anomaly_patterns` (AML testing) |

### Data Volumes

| Customers | Employees | Transactions | Trades | Runtime |
|-----------|-----------|--------------|--------|---------|
| 100 | 15 | ~20K | ~600 | 1-2 min |
| 1,000 | 15-18 | ~200K | ~6K | 5-7 min |
| 10,000 | 66 | ~2M | ~60K | 10-15 min |

---

## Domains

| Code | Domain | Schemas |
|------|--------|---------|
| CRM | Customer Information | CRM_RAW_001, CRM_AGG_001 |
| ACC | Accounts | (within CRM_RAW_001) |
| PAY | Payments | PAY_RAW_001, PAY_AGG_001 |
| ICG | SWIFT Messaging | (within PAY_RAW_001, PAY_AGG_001) |
| EQT | Equity Trading | EQT_RAW_001, EQT_AGG_001 |
| FII | Fixed Income | FII_RAW_001, FII_AGG_001 |
| CMD | Commodities | CMD_RAW_001, CMD_AGG_001 |
| REF | Reference Data | REF_RAW_001, REF_AGG_001 |
| REP | Reporting | REP_RAW_001, REP_AGG_001 |
| LOA | Loans | LOA_RAW_001, LOA_AGG_001 |
| LIQ | Liquidity (LCR) | (within REP_RAW_001, REP_AGG_001) |

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

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: 'faker'` | `pip install -r requirements.txt` |
| `Customer file not found` | `./data_generator.sh 100 --clean` |
| `Connection not found` | `snow connection add <connection>` |
| `Stream has no data` | `./upload-data.sh --CONNECTION_NAME=<connection>` |
