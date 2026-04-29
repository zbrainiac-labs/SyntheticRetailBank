# Open Points - SyntheticRetailBank DCM Migration

## Open

| # | Priority | Item | Count | Fix |
|---|----------|------|-------|-----|
| O5 | Low | **No data quality expectations** | 0 | Add `ATTACH DATA METRIC FUNCTION` for null checks, freshness |
| O7 | Low | **WITH TAG moved to post_deploy** | 63 | Tags removed from DEFINE (DCM unsupported), ALTER TAG in post_deploy |
| O8 | Low | **Cross-schema dependency: REPA_SV_WEALTH_MANAGEMENT_DETAILED** | 1 | SonarQube `Disallow_Cross_Schema_Dependencies` — SV in `REP_AGG_001` references `CRMA_AGG_DT_CUSTOMER_360` in `CRM_AGG_001`. Architecturally intentional (wealth report joins customer + portfolio data). Accept or restructure. |
| O9 | Low | **Python code design issues (SonarQube)** | ~90 | S3776 cognitive complexity (32), S3358 nested ternaries in email templates (22), S1192 duplicate string literals (21), S7498 format spec mini-language (15). Require major refactoring, not quick fixes. |

## Architecture

```
SyntheticRetailBank/
├── manifest.yml                        DCM v2 (DEV + PROD targets, 19 Jinja vars)
├── pre_deploy.sql                      DB + schema + project (Jinja parameterized)
├── post_deploy.sql                     Streams, file formats, procedures, agents, semantic views (Jinja parameterized)
├── github-workflow-verification_v1.sh  SHA256 integrity check
├── open_points.md                      This file
├── .github/workflows/
│   └── update-local-repo.yml           CI/CD pipeline
├── sources/
│   ├── definitions/                    33 SQL files (192 DEFINE + 9 GRANT)
│   └── macros/
│       └── common.sql                  Shared Jinja macros
├── sqlunit/
│   └── tests.sqltest                   59 validation tests (Jinja parameterized)
├── generators/                         21 Python modules + requirements.txt
├── generated_data/                     Output CSV files
├── notebooks/                          8 interactive Snowflake notebooks
├── business_requirements/              Business requirement docs
├── operation/                          Manual SQL operations (Jinja parameterized)
├── the_bank_app/                       Streamlit app
└── data_generator.sh                   Data generation wrapper
```

## DCM Object Summary

| Object Type | Count | Location |
|-------------|-------|----------|
| DEFINE SCHEMA | 16 | `000_infrastructure.sql` |
| DEFINE TAG | 1 | `000_infrastructure.sql` |
| DEFINE STAGE | 17 | `0xx_*_ingestion.sql` |
| DEFINE TABLE | 28 | `0xx_*_ingestion.sql`, `465_LOAA_analytics.sql` |
| DEFINE TASK | 32 | `0xx_*_ingestion.sql` |
| DEFINE DYNAMIC TABLE | 70 | `3xx-6xx_*.sql` |
| DEFINE VIEW | 28 | `302`, `360`, `361`, `410`, `415`, `565` |
| GRANT | 9 | `900_access.sql` |
| **Total DCM-managed** | **201** | |

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

## Manifest Variables

| Variable | DEV | PROD |
|----------|-----|------|
| `db` | `AAA_DEV_SYNTHETIC_BANK` | `AAA_PRD_SYNTHETIC_BANK` |
| `wh` | `MD_TEST_WH` | `MD_TEST_WH` |
| `lag` | `60 MINUTE` | `30 MINUTE` |
| `crm_raw` | `CRM_RAW_001` | `CRM_RAW_001` |
| `crm_agg` | `CRM_AGG_001` | `CRM_AGG_001` |
| `pay_raw`..`rep_agg` | `*_RAW_001` / `*_AGG_001` | (same) |
