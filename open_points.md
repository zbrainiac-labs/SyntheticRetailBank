# Open Points - SyntheticRetailBank

## Open

| # | Priority | Item | Details |
|---|----------|------|---------|
| O8 | Low | **Cross-schema dependency: REPA_SV_WEALTH_MANAGEMENT_DETAILED** | SonarQube `Disallow_Cross_Schema_Dependencies` -- SV in `REP_AGG_V001` references `CRMA_AGG_DT_CUSTOMER_360` in `CRM_AGG_V001`. Architecturally intentional (wealth report joins customer + portfolio data). Accept or restructure. |
| O9 | Low | **Python code design issues (SonarQube)** | ~90 issues: S3776 cognitive complexity (32), S3358 nested ternaries (22), S1192 duplicate literals (21), S7498 format spec (15). Major refactoring required. |
| O10 | Medium | **SWIFT generation slow** | 2400 subprocess calls for 5K customers (~30 min). Consider batch mode or direct Python XML generation instead of subprocess per pair. |
| O11 | Low | **generate_all_files always runs** | When specific flags are passed (e.g. --generate-swift), base data (customers, transactions, equity) is always regenerated. Consider caching or skip-if-exists logic. |
| O13 | Medium | **Negative advisor AUM** | Found 2026-11-15 on the bank-app Wealth page (`/risk/wealth`): Total AUM shows CHF -313M and Avg AUM/Client CHF -63K. 19 of 34 advisors have a negative `TOTAL_PORTFOLIO_VALUE` in `CRM_AGG_V001.EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED`. AUM should not be negative. Root cause is upstream: 2,137 of 5,001 customers have a negative `TOTAL_BALANCE` in `CRMA_AGG_DT_CUSTOMER_360` (min CHF -24.9M), which `415_EMPA_analytics.sql:159` sums into `TOTAL_PORTFOLIO_VALUE`. Account balances look like a running sum of transactions without an opening balance - fix in the generator / balance DT, then decide whether AUM should net overdrafts. |
| O14 | Medium | **CHURNED customers still transacting** | `LIFECYCLE_STAGE = 'CHURNED'` (1,134 customers, 22.7%) comes from a CHURN / ACCOUNT_CLOSE customer event, but all of them have transactions in the last 40 days. The generator keeps creating transactions after the churn event. Stop transactions after churn in the generator, or require no recent transaction for CHURNED. Also: `CHURN_PROBABILITY` still uses the CRM-event recency (`IS_DORMANT_LIFECYCLE`), not transactions. |
| O16 | Low | **Generated transactions cover only ~40 days** | Every customer's last transaction is 16-43 days old, so DORMANT (>180d) and DECLINING (>90d) never occur and the Churn page "Dormant" panel only shows customers with no transaction yet. Generate some customers with older last activity for a realistic demo. |

## Closed

| # | Date | Item | Resolution |
|---|------|------|------------|
| C1 | 2026-05-12 | **Upload script schema names** | All `_001` -> `_V001` in `upload-data.sh` (CRM, PAY, EQT, FII, CMD, REF, REP, LOA) |
| C2 | 2026-05-12 | **Task names in operation SQL** | All `_TASK_` -> `_TK_` in `operation/execute_all_tasks_and_refresh_dts.sql` (16 tasks) |
| C3 | 2026-05-12 | **Generator exclusive mode bug** | `main.py` skipped FX/PAY/EQT when specific flags were passed. Fixed to always call `generate_all_files()`. |
| C4 | 2026-05-12 | **Commodity trades one-file-per-timestamp** | `commodity_generator.py` split on space instead of `T` in ISO timestamp. Fixed to group by date (3686 -> 406 files). |
| C5 | 2026-05-12 | **SWIFT generator script not found** | Default path `swift_message_generator.py` not found from project root. Fixed to `generators/swift_message_generator.py` in both `main.py` and `swift_generator.py`. |
| C6 | 2026-05-12 | **CI workflow SOURCE_SCHEMA** | `update-local-repo.yml` updated from `AAA_DCM` to `AAA_RAW_V001` (workflow removed since) |
| C7 | 2026-05-12 | **CI workflow CLONE_PER_BUILD** | Removed `CLONE_PER_BUILD: true` from CI workflow |
| C8 | 2026-10-25 | **No data quality expectations (O5)** | 8 DMF attachments with expectations in DCM (`905_governance.sql`), checked by `EXECUTE DCM PROJECT ... TEST ALL` |
| C9 | 2026-10-25 | **Obsolete per-file DCM test** | Deleted `operation/test_dcm_individual.sh` (files reference each other, single-file plans fail); replaced by `operation/full_rebuild_test.sh` |
| C10 | 2026-10-25 | **WITH TAG moved to post_deploy (O7)** | Tags are back in DCM: 41 tag assignments via `ATTACH TAG` in `905_governance.sql`; no tag statements left in `post_deploy.sql` |
| C11 | 2026-11-15 | **DORMANT lifecycle stage based on CRM events** | 42.3% of customers were DORMANT although all transacted within 40 days. `LIFECYCLE_STAGE` in `410_CRMA_customer360.sql` now uses days since last transaction for DORMANT (>=180d) and DECLINING (>90d). Verified after the DEV deploy: ACTIVE 60.4%, CHURNED 22.7%, MATURE 14.5%, NEW 2.5%, no DORMANT. |
| C12 | 2026-11-15 | **Zip codes lost leading zeros in the bank app (O15)** | Data was fine (`ZIPCODE` is VARCHAR from CSV to `CRMI_RAW_TB_ADDRESSES`, "0133" stored correctly). The app's `DataTable` formats numeric-looking strings as numbers ("0133" -> 133, "44414" -> 44,414). Fixed by rendering the Customer 360 address-history Zipcode column as plain text. |
| C13 | 2026-11-15 | **Share grants relied on secondary roles (O12)** | The 3 shares and listings were dropped and recreated by `post_deploy.sql` as CICD on 2026-11-14; `SHOW SHARES` shows owner CICD for `SH_CUSTOMER_360_KYC`, `SH_CUSTOMER_PII_VAULT`, `SH_EMPLOYEE_PII_VAULT`. |

## Architecture

Project structure, object inventory (DCM-managed and post-deploy) and operation scripts: see [README.md](README.md) (DCM Project Structure, Object Inventory). Not duplicated here to avoid stale counts.

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
