# Open Points - SyntheticRetailBank DCM Migration

## Completed

| # | Item | Details |
|---|------|---------|
| 1 | DCM project structure | `manifest.yml` (v2), `sources/definitions/` (33 files), `pre_deploy.sql`, `post_deploy.sql` |
| 2 | Full SQL conversion | 192 DEFINE + 9 GRANT = 201 DCM-managed objects |
| 3 | Jinja templating | 749 refs: `{{ db }}`, `{{ wh }}`, `{{ lag }}`, 16 schema vars |
| 4 | TARGET_LAG standardized | 3 variants unified to `{{ lag }}` across 64 dynamic tables |
| 5 | GitHub Actions workflow | Aligned with MOAP: pre-deploy, DCM deploy, post-deploy, SonarQube, clone/test/drop, release |
| 6 | Workflow integrity | `github-workflow-verification_v1.sh` with SHA256 check |
| 7 | pre/post_deploy parameterized | `${SOURCE_DATABASE}` + 16 schema vars via `envsubst` |
| 8 | sqlunit tests | 59 tests (schemas, tables, stages, DTs, views, tasks, data quality), `${SOURCE_DATABASE}` via `envsubst` |
| 9 | Python generators | 22 files moved to `generators/` |
| 10 | Access control | `900_access.sql` with GRANT statements using Jinja vars |
| 11 | Unsupported objects | 17 streams, 14 file formats, 7 procedures, 15 semantic views, 6 agents in `post_deploy.sql` |
| 12 | CHANGE_TRACKING | 24/24 raw ingestion tables have `CHANGE_TRACKING = TRUE` |
| 13 | Duplicate removed | `312_CRMA_lifecycle.sql` deleted, canonical in `410_CRMA_customer360.sql` |
| 14 | DCM project creation | Automated via `pre_deploy.sql` (`CREATE DCM PROJECT IF NOT EXISTS`) |
| 15 | Blank lines collapsed | All files cleaned |
| 16 | Macros directory | `sources/macros/common.sql` with `fqn()` helper |
| 17 | PROD target | Added to manifest (`AAA_PRD_SYNTHETIC_BANK`, `lag: 30 MINUTE`) |
| 18 | DataOpsBackbone naming | Full rename: `_ST_` (17), `_TK_` (32), `_RAW_FF_` (14), `_SM_` (17), `_RAW_SP_` (7). Verified: zero violations in definitions + post_deploy |
| 19 | Constraint naming | Standardized FK to `FK_{SOURCE}__{TARGET}` pattern |
| 20 | Remote | `zbrainiac-labs` remote added |

## Open

| # | Priority | Item | Count | Fix |
|---|----------|------|-------|-----|
| O1 | ~~High~~ | ~~**DCM plan not yet validated**~~ | - | ~~DONE — 193 entities (192 create, 1 alter)~~ |
| O2 | Medium | **post_deploy.sql / pre_deploy.sql hardcoded DB/schema** | ~200 refs | Parameterize with `snow sql -D` variables or `envsubst` for PROD support |
| O3 | Medium | **Column COMMENTs verbose** | 1,591 | Strip to reduce 8,474 to ~3,500 lines |
| O4 | Medium | **Object-level COMMENTs verbose** | 61 | Shorten to one-liners or remove |
| O5 | Low | **No data quality expectations** | 0 | Add `ATTACH DATA METRIC FUNCTION` for null checks, freshness |
| O6 | Low | **sqlunit schema names hardcoded** | 54 | Add schema `envsubst` vars (same as post_deploy pattern) |
| O7 | Low | **WITH TAG moved to post_deploy** | 63 | Tags removed from DEFINE (DCM unsupported), ALTER TAG in post_deploy |

## Architecture

```
SyntheticRetailBank/
├── manifest.yml                        DCM v2 (DEV + PROD targets, 19 Jinja vars)
├── pre_deploy.sql                      DB + schema + project + listings
├── post_deploy.sql                     Streams, file formats, procedures, agents, semantic views
├── github-workflow-verification_v1.sh  SHA256 integrity check
├── open_points.md                      This file
├── .github/workflows/
│   └── update-local-repo.yml           CI/CD pipeline
├── sources/
│   ├── definitions/                    33 SQL files (192 DEFINE + 9 GRANT)
│   └── macros/
│       └── common.sql                  Shared Jinja macros
├── sqlunit/
│   └── tests.sqltest                   59 validation tests
├── generators/                         22 Python modules + requirements.txt
├── generated_data/                     Output CSV files
├── notebooks/                          8 interactive Snowflake notebooks
├── business_requirements/              Business requirement docs
├── operation/                          Manual SQL operations
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
