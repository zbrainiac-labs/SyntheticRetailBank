# Open Points - SyntheticRetailBank

## Open

| # | Priority | Item | Details |
|---|----------|------|---------|
| O5 | Low | **No data quality expectations** | Add `ATTACH DATA METRIC FUNCTION` for null checks, freshness |
| O7 | Low | **WITH TAG moved to post_deploy** | 63 tags removed from DEFINE (DCM unsupported), ALTER TAG in post_deploy |
| O8 | Low | **Cross-schema dependency: REPA_SV_WEALTH_MANAGEMENT_DETAILED** | SonarQube `Disallow_Cross_Schema_Dependencies` -- SV in `REP_AGG_V001` references `CRMA_AGG_DT_CUSTOMER_360` in `CRM_AGG_V001`. Architecturally intentional (wealth report joins customer + portfolio data). Accept or restructure. |
| O9 | Low | **Python code design issues (SonarQube)** | ~90 issues: S3776 cognitive complexity (32), S3358 nested ternaries (22), S1192 duplicate literals (21), S7498 format spec (15). Major refactoring required. |
| O10 | Medium | **SWIFT generation slow** | 2400 subprocess calls for 5K customers (~30 min). Consider batch mode or direct Python XML generation instead of subprocess per pair. |
| O11 | Low | **generate_all_files always runs** | When specific flags are passed (e.g. --generate-swift), base data (customers, transactions, equity) is always regenerated. Consider caching or skip-if-exists logic. |

## Closed

| # | Date | Item | Resolution |
|---|------|------|------------|
| C1 | 2026-05-12 | **Upload script schema names** | All `_001` -> `_V001` in `upload-data.sh` (CRM, PAY, EQT, FII, CMD, REF, REP, LOA) |
| C2 | 2026-05-12 | **Task names in operation SQL** | All `_TASK_` -> `_TK_` in `operation/execute_all_tasks_and_refresh_dts.sql` (16 tasks) |
| C3 | 2026-05-12 | **Generator exclusive mode bug** | `main.py` skipped FX/PAY/EQT when specific flags were passed. Fixed to always call `generate_all_files()`. |
| C4 | 2026-05-12 | **Commodity trades one-file-per-timestamp** | `commodity_generator.py` split on space instead of `T` in ISO timestamp. Fixed to group by date (3686 -> 406 files). |
| C5 | 2026-05-12 | **SWIFT generator script not found** | Default path `swift_message_generator.py` not found from project root. Fixed to `generators/swift_message_generator.py` in both `main.py` and `swift_generator.py`. |
| C6 | 2026-05-12 | **CI workflow SOURCE_SCHEMA** | `update-local-repo.yml` updated from `AAA_DCM` to `AAA_RAW_V001` |
| C7 | 2026-05-12 | **CI workflow CLONE_PER_BUILD** | Removed `CLONE_PER_BUILD: true` from CI workflow |

## Architecture

```
SyntheticRetailBank/
├── manifest.yml                        DCM v2 (DEV + PROD targets, 19 Jinja vars)
├── pre_deploy.sql                      DB + schema + project (Jinja parameterized)
├── post_deploy.sql                     Streams, file formats, procedures, agents, semantic views
├── github-workflow-verification_v1.sh  SHA256 integrity check
├── open_points.md                      This file
├── .github/workflows/
│   └── update-local-repo.yml           CI/CD pipeline (reusable DataOpsBackbone workflow)
├── sources/
│   ├── definitions/                    33 SQL files (192 DEFINE + 9 GRANT)
│   └── macros/
│       └── common.sql                  Shared Jinja macros
├── sqlunit/
│   └── tests.sqltest                   59 validation tests (Jinja parameterized)
├── generators/                         21 Python modules + requirements.txt
├── generated_data/                     Output CSV/XML files
├── notebooks/                          9 interactive Snowflake notebooks
├── business_requirements/              Business requirement documents
├── operation/                          Manual SQL operations (Jinja parameterized)
├── the_bank_app/                       16-tab Streamlit banking dashboard
├── data_generator.sh                   Data generation wrapper (default: 5000 customers)
└── upload-data.sh                      Parallel stage upload (10 threads, batched)
```

## DCM Object Summary

| Object Type | Count | Location |
|-------------|-------|----------|
| DEFINE SCHEMA | 16 | `000_infrastructure.sql` |
| DEFINE TAG | 1 | `000_infrastructure.sql` |
| DEFINE STAGE | 17 | `0xx_*_ingestion.sql` |
| DEFINE TABLE | 28 | `0xx_*_ingestion.sql`, `465_LOAA_analytics.sql` |
| DEFINE TASK | 32 | `0xx_*_ingestion.sql` |
| DEFINE DYNAMIC TABLE | 71 | `3xx-6xx_*.sql` |
| DEFINE VIEW | 28 | `302`, `360`, `361`, `410`, `415`, `565` |
| GRANT | 9 | `900_access.sql` |
| **Total DCM-managed** | **202** | |

| Unsupported Type | Count | Location |
|------------------|-------|----------|
| Streams | 17 | `post_deploy.sql` |
| File Formats | 14 | `post_deploy.sql` |
| Stored Procedures | 7 | `post_deploy.sql` |
| Semantic Views | 15 | `post_deploy.sql` |
| Cortex Agents | 6 | `post_deploy.sql` |
| **Total post-deploy** | **59** | |

## Naming Convention (DataOpsBackbone aligned)

Pattern: `{DOM}{COMP}_{MAT}_{TYPE}_{TEXT}`

| Type Code | Object | Example |
|-----------|--------|---------|
| `_TB_` | Table | `CRMI_RAW_TB_CUSTOMER` |
| `_DT_` | Dynamic Table | `CRMA_AGG_DT_CUSTOMER_360` |
| `_VW_` | View | `CRMA_AGG_VW_SANCTIONS_ENRICHED` |
| `_ST_` | Stage | `CRMI_RAW_ST_CUSTOMERS` |
| `_TK_` | Task | `CRMI_RAW_TK_LOAD_CUSTOMERS` |
| `_FF_` | File Format | `CRMI_RAW_FF_CUSTOMER_CSV` |
| `_SM_` | Stream | `CRMI_RAW_SM_CUSTOMER_FILES` |
| `_SP_` | Stored Procedure | `CRMI_RAW_SP_CLEANUP_STAGE_KEEP_LAST_N` |
| `_SV_` | Semantic View | `CRMA_SV_CUSTOMER_360` |

## Manifest Variables

| Variable | DEV | PROD |
|----------|-----|------|
| `db` | `AAA_DEV_SYNTHETIC_BANK` | `AAA_PRD_SYNTHETIC_BANK` |
| `wh` | `MD_TEST_WH` | `MD_TEST_WH` |
| `lag` | `60 MINUTE` | `30 MINUTE` |
| `crm_raw` | `CRM_RAW_V001` | `CRM_RAW_V001` |
| `crm_agg` | `CRM_AGG_V001` | `CRM_AGG_V001` |
| `pay_raw`..`rep_agg` | `*_RAW_V001` / `*_AGG_V001` | (same) |
