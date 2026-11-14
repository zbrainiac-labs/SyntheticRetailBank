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

# One-time account setup (requires ACCOUNTADMIN): EXECUTE DATA METRIC FUNCTION
#   grant for CICD, sanctions listing and its local copy
snow sql -c <my-sf-connection> --role ACCOUNTADMIN -f operation/setup_listings.sql

# Bootstrap per environment (as CICD; CI runs it on every push):
#   database, AAA_DCM schema, DCM project and shared warehouse
snow sql -c <my-sf-connection> --role CICD -f pre_deploy.sql \
  --variable "db=AAA_DEV_SYNTHETIC_BANK" --variable "wh=MD_TEST_WH" --enable-templating JINJA

./data_generator.sh 5000 --clean
./upload-data.sh --CONNECTION_NAME=<my-sf-connection>

# Deploy (DCM + post-deploy hooks); SNOW_CONNECTION selects the CLI connection:
SNOW_CONNECTION=<my-sf-connection> ./deploy.sh DEV   # or PROD
```

On an empty database `deploy.sh` deploys in two passes: first without the semantic views (`-D semantic_views=false`), then it refreshes all dynamic tables and deploys everything. Semantic views can only be created once their base dynamic tables are initialized.

---

## DCM Project Structure

```
SyntheticRetailBank/
├── manifest.yml                    DCM manifest (v2, targets: DEV + PROD)
├── pre_deploy.sql                  Bootstrap (as CICD): DB, AAA_DCM schema, DCM project, warehouse
├── post_deploy.sql                 Seeds, sub-agents, shares/listings (not in DCM); re-runnable
├── post_deploy_master-agent.sql    Master agent: hybrid router, caching, observability, regression
├── deploy.sh                       Deployment script (DCM + post-deploy hooks)
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
│   │   ├── 7xx_*_semantic_views.sql  Semantic views (guarded by the semantic_views variable)
│   │   ├── 900_access.sql          GRANT statements
│   │   └── 905_governance.sql      Masking policies, tags, DMF attachments with expectations
│   └── macros/
│       └── common.sql              Shared Jinja macros
├── sqlunit/
│   ├── tests.sqltest               59 SQL validation tests
│   ├── lcr_validation.sql          LCR test gate (TC-/ENG- checks)
│   ├── metadata_check.sql          Metadata gate (META- checks)
│   ├── data_check.sql              Data gate: Customer 360, PII vault, accounts (DATA- checks)
│   ├── run_semantic_view_vqrs.py   Runs all 54 verified queries of the 19 semantic views
│   └── agent_smoke_test.py         One question per agent
├── notebooks/                      9 interactive Snowflake notebooks
├── the_bank_app/                   16-tab Streamlit banking dashboard
├── README_master_agent.md          Master agent docs
├── business_requirements/          Business requirement documents
├── generators/                     21 Python data generator modules
├── generated_data/                 Output CSV/XML files
├── operation/                      Manual operations, incl. full_rebuild_test.sh (from-scratch regression)
└── data_generator.sh               Data generation wrapper
```

### DCM Project

| Property | Value |
|----------|-------|
| Project Identifier | `AAA_DEV_SYNTHETIC_BANK.AAA_DCM.SYNTHETIC_RETAIL_BANK` |
| Account | `SFSEEUROPE-DEMO_MDAEPPEN` (locator ZS28104) |
| Targets | `DEV`, `PROD` |
| Owner | `CICD` |
| Warehouse | `MD_TEST_WH` (not DCM-managed) |

### Object Inventory

| Layer | Objects | Type |
|-------|---------|------|
| Infrastructure | 16 schemas, 1 tag | DEFINE SCHEMA, DEFINE TAG |
| Raw Ingestion | 21 stages, 36 tables, 38 tasks (STARTED), 21 streams, 14 file formats, 7 cleanup procedures | DEFINE STAGE/TABLE/TASK/STREAM/FILE FORMAT/PROCEDURE |
| Analytics | 81 dynamic tables, 42 views, 3 procedures, 1 function | DEFINE DYNAMIC TABLE/VIEW/PROCEDURE/FUNCTION |
| Semantic layer | 19 semantic views (`700`-`720_*_semantic_views.sql`), each with verified queries (54 in total) | DEFINE SEMANTIC VIEW |
| Governance | 4 masking policies, 17 masking and 41 tag assignments, 8 DMF attachments with expectations (`905_governance.sql`) | DEFINE MASKING POLICY, ATTACH |
| Access | 39 grants | GRANT statements |
| Post-deploy | Seed data, 8 agents + Snowflake Intelligence, 3 shares + listings (DEV: `SH_*` / `LS_*`, PROD: `SH_PROD_*` / `LS_PROD_*`) | SQL script (not supported by DCM, or needs privileges CICD lacks) |
| Master Agent | 10 UDFs, 4 tables, 4 procedures, 5 views, 1 agent | `post_deploy_master-agent.sql` |

---

## CI/CD Workflow

Triggered on push to `main` or manual dispatch:

1. Workflow integrity check (SHA256)
2. Pre-deploy SQL (DB, schema, DCM project, marketplace listings)
3. DCM Analyze + Plan + Deploy
4. Post-deploy SQL (seeds, agents, shares, listings)
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

The equity, fixed income and commodity generators keep holdings per account and only sell what an account holds (no short positions; each generator asserts this). Each uses its own seeded random generator, so runs are reproducible.

### Data Volumes

| Customers | Employees | Transactions | Equity Trades | FI Trades | Commodity | SWIFT XMLs | Runtime |
|-----------|-----------|--------------|---------------|-----------|-----------|------------|---------|
| 1,000 | 15-18 | ~150K | ~290K | ~1.8K | ~740 | ~600 | 5-7 min |
| 5,000 | 39 | ~760K | ~1.45M | ~9K | ~3.7K | ~4.8K | 10 min |
| 10,000 | 66 | ~1.5M | ~2.9M | ~18K | ~7.4K | ~10K | 15-20 min |

---

### Key calculations

| Topic | Rule |
|-------|------|
| Portfolio value (AUM) | Cash + held positions valued at held quantity × average buy cost (`REPP_AGG_DT_PORTFOLIO_PERFORMANCE`) |
| Risk category | Share of equity + commodity in the portfolio: ≤ 0 or < 20% LOW, 20–50% MODERATE, > 50% HIGH, > 80% VERY_HIGH |
| PEP screening | Exact and fuzzy name match on the unmasked raw customer names (the Customer 360 DT refreshes as CICD, which only sees masked PII); one best match per customer |
| Sanctions screening | Exact match, or fuzzy match with Jaro-Winkler ≥ 90 and edit distance ≤ 20% of the name length; one best hit per customer, `SANCTIONS_HIT_COUNT` keeps the number of hits |
| Data quality | 8 DMFs with expectations (row counts, null share of names, Customer 360 freshness < 2 h); `EXECUTE DCM PROJECT AAA_DEV_SYNTHETIC_BANK.AAA_DCM.SYNTHETIC_RETAIL_BANK TEST ALL` |

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
| Liquidity Risk | Reporting | LCRS_SV_LCR_CURRENT, LCRS_SV_HQLA_BREAKDOWN, LCRS_SV_OUTFLOW_BREAKDOWN, LCRS_SV_TREND_90DAY, LCRS_SV_ALERTS_ACTIVE, LCRS_SV_LCR_SCENARIOS, LCRS_SV_LCR_DRIVERS, LCRS_SV_LCR_EVENTS + what-if tool (REPP_AGG_SP_LCR_WHAT_IF) |
| Loan Portfolio | Reporting | LOAS_SV_PORTFOLIO_CURRENT, LOAS_SV_LTV_DISTRIBUTION, LOAS_SV_APPLICATION_FUNNEL, LOAS_SV_AFFORDABILITY_ANALYSIS, LOAS_SV_COMPLIANCE_SCREENING |
| HR Employee | CRM | EMPA_SV_HR_EMPLOYEE |
| **Uber Agent** (orchestrator) | Cross-domain | Routes to all 7 specialist sub-agents |
| **Master Agent** (enhanced orchestrator) | Cross-domain | Hybrid router (regex + AI_CLASSIFY), response caching, golden regression. See [README_master_agent.md](README_master_agent.md) |

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
| LCR Monitoring | 11 tabs: status bands + events, HQLA caps, inflow cap, maturity ladder, stress scenarios, root cause (top 10 drivers), drill-down, currency, data quality, what-if simulator, SNB reporting |
| Loans Portfolio | LTV, application funnel, affordability, compliance |
| Ask AI | Natural language queries via Cortex Agents |
| Settings | Connection management |

---

## LCR Showcase Runbook (FINMA, CHF)

The Liquidity Risk domain runs a FINMA LCR engine (LiqV / Circ. 2015/2) on a regional-bank book (~CHF 0.7bn HQLA) with 90 daily cut-offs and five scripted liquidity events (EVT_01..05), including a stress window with 7 days below 100%.
Status bands: **FAIL** < 100%, **WARNING** 100–110%, **PASS** >= 110%.

| Step | Command |
|------|---------|
| 1. Generate all data (incl. LCR book, daily EOD FX for every calendar day) | `./data_generator.sh 5000 --clean` |
| 2. Deploy DCM + post-deploy + master agent + test gate | `./deploy.sh DEV` (`--skip-tests` to skip step 4) |
| 3. Upload generated files | `./upload-data.sh --CONNECTION_NAME=<conn>` |
| 4. Load + refresh (tasks and all dynamic tables) | `snow sql -c <conn> -f operation/execute_all_tasks_and_refresh_dts.sql` |
| 5. Test gate (16 LCR checks + metadata gate) | `snow sql -c <conn> -f sqlunit/lcr_validation.sql` and `-f sqlunit/metadata_check.sql` |
| 6. App tests | `cd the_bank_app && venv/bin/python tests/test_lcr_page.py && venv/bin/python tests/test_all_pages.py` |

**Operation scripts (`operation/`):**

| Script | Purpose |
|--------|---------|
| `tasks_and_dts.sql` | Suspend or resume all tasks and dynamic tables, read from Snowflake at run time (`-D action=suspend\|resume`, optional `-D task_like=` / `-D dt_like=`). After a DCM deploy, which resumes every task: `snow sql -c <conn> --role CICD -f operation/tasks_and_dts.sql --enable-templating JINJA -D db=AAA_DEV_SYNTHETIC_BANK -D action=suspend -D task_like=LIQI_RAW_TK_% -D dt_like=-` |
| `execute_all_tasks_and_refresh_dts.sql` | Run every load task and refresh all dynamic tables in dependency order |
| `reset_liqi_data.sql` | DEV only: empty the generated LIQI (LCR + DtI) book and its stages before loading a new generator run |
| `render_reference_seeds.py` | Render the LIQI reference seeds of `post_deploy.sql` from `reference_data/liqi` (`--check` fails if out of sync) |
| `full_rebuild_test.sh` | From-scratch regression (see below) |
| `setup_listings.sql` | One-time account-level setup (ACCOUNTADMIN) |

**One liquidity book, shared ports (359):** the LCR (`/liquidity/lcr`) and the DtI (`/dti`) run on one generator run. The raw holdings, deposit, contract and cashflow tables hold the daily window and the month-end history (`SNAPSHOT_SCOPE` = `DAILY` / `MONTH_END`). Both engines read the same ports: `REPP_AGG_DT_LIQ_POSITIONS` (CHF, product mapping, data quality, DtI attributes), `REPP_AGG_DT_LIQ_CASHFLOWS` (valid contracts in CHF), `REPP_AGG_VW_LIQ_RULES` (behavioral rules) and `REPP_AGG_VW_LIQ_CALENDAR` (Zurich business days). A quarantined record is therefore missing on both pages. `sqlunit/data_check.sql` DATA-LIQ-01..07 checks the shared book.

**Engine (single calculation path):** `REPP_AGG_DT_LCR_POSITIONS` (daily slice of the position port) → `..._SCENARIO_POSITIONS` → `..._SCENARIO_COMPONENTS` → `..._SCENARIO_DAILY` (4 scenarios, shared UDF `REPP_AGG_FN_LCR_CALC` with L2B 15/85, L2 2/3 and 75% inflow caps). `REPP_AGG_DT_LCR_DAILY` is the BASE scenario.

**Root cause / attribution:** `REPP_AGG_VW_LCR_VARIANCE_ATTRIBUTION` splits every LCR change exactly per position (dW/N1 − H0·dO/(N0·N1) + H0·dI/(N0·N1)) plus cap adjustment lines; contributions sum to ΔLCR. `CLIENT_IMPACT_PP` adds the cash settlement (a withdrawn deposit also reduces SNB cash) and ranks the top 10 drivers in `REPP_AGG_VW_LCR_VARIANCE_DRIVERS`.

**Simulator / what-if:** `CALL REPP_AGG_SP_LCR_SIMULATE('<date>', '<json levers>')` returns a waterfall plus breakeven grid; `REPP_AGG_SP_LCR_WHAT_IF` is the agent tool (JSON).

The gate in step 5 also includes `sqlunit/data_check.sql` (Customer 360, PII vault and accounts uniqueness).

Docs: [FINMA_LCR_BUSINESS_REQUIREMENTS.md](business_requirements/FINMA_LCR_BUSINESS_REQUIREMENTS.md) (business rules, requirements → objects, test plan, open points), [LCR_DATA_DICTIONARY.md](business_requirements/LCR_DATA_DICTIONARY.md) (snapshot of object and column descriptions).

---

## Full Regression Test (from scratch)

`operation/full_rebuild_test.sh` drops `AAA_DEV_SYNTHETIC_BANK` and rebuilds everything, then runs all test suites. DEV only; `--yes` is required.

```bash
bash operation/full_rebuild_test.sh DEV --yes                    # regenerate data (~7 min extra)
bash operation/full_rebuild_test.sh DEV --yes --skip-generate    # reuse generated_data/
```

| Step | What happens |
|------|--------------|
| 0 | Snapshot of row counts, AUM, PEP matches and LCR |
| 1 | Detach the database from its shares (`REVOKE USAGE ... FROM SHARE`), drop the database |
| 2 | `pre_deploy.sql` as CICD (bootstrap) |
| 3 | `data_generator.sh 5000 --clean` (skipped with `--skip-generate`) |
| 4 | `deploy.sh DEV --skip-tests` (two-pass first deploy, post-deploy, master agent) |
| 5 | `upload-data.sh` |
| 6 | Run all tasks, wait for them, run `post_deploy.sql` again (loan applications need the extracted mails), refresh all 81 dynamic tables |
| 7 | Structure counts, second DCM plan must show no changes, second `post_deploy.sql` run must succeed |
| 8 | Test gate, DCM `TEST ALL`, VQRs, agent smoke test, golden regression (FULL), app tests |

Runtime is about 70 minutes with `--skip-generate`. Logs go to a temp folder printed at the end. The shares keep existing; consumers lose access until `post_deploy.sql` grants the database again (step 4). `UNDROP DATABASE` only helps while no new database with the same name exists, so in practice there is no rollback once step 2 has run.

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: 'faker'` | `pip install -r requirements.txt` |
| `Customer file not found` | `./data_generator.sh 5000 --clean` |
| `Connection not found` | `snow connection add <connection>` |
| `Stream has no data` | `./upload-data.sh --CONNECTION_NAME=<connection>` |
| AGG/REP tables empty after upload | Run `operation/execute_all_tasks_and_refresh_dts.sql` |
| DCM plan fails: `Dynamic Table ... is not initialized` (semantic view) | First deploy into an empty database: run `./deploy.sh`, which deploys in two passes |
| `Database ... cannot be dropped. It is still shared by 3 shares` | `REVOKE USAGE ON DATABASE <db> FROM SHARE <share>` for each share first (`full_rebuild_test.sh` does this) |
| Loan applications empty on a new database | The mail task must run first; then run `post_deploy.sql` again |
| `LIST snow://workspace/USER$.PUBLIC.DEFAULT$/...` fails with `Unsupported statement type 'LIST_FILES'` | Logged by agent runs started from stored procedures (`CALL_SUB_AGENT`); issued by the platform, answers are not affected |
| Deploy master agent only | `snow sql -f post_deploy_master-agent.sql --variable "db='AAA_DEV_SYNTHETIC_BANK'" --variable "rep_agg='REP_AGG_V001'" --variable "crm_agg='CRM_AGG_V001'" --variable "wh='MD_TEST_WH'" --enable-templating JINJA` |
