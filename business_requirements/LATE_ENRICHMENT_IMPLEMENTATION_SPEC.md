# Late Enrichment -- Implementation Specification

Appendix to [LATE_ENRICHMENT_BUSINESS_REQUIREMENTS.md](LATE_ENRICHMENT_BUSINESS_REQUIREMENTS.md).

---

## Phase Overview

| Phase | Name | Description | Breaking Change | Dependencies |
|-------|------|-------------|-----------------|--------------|
| 1 | Foundation | Create PII vault tables, masking/row access policies, sensitivity tags | No | None |
| 2 | DT/View Refactoring | Remove PII columns from all 22 downstream objects | Yes | Phase 1 |
| 3 | Consumer Migration | Create enrichment views, update semantic views, Streamlit, notebooks | No | Phase 2 |
| 4 | Governance | GDPR erasure procedure, audit view, data sharing validation | No | Phase 3 |

---

## Phase 1: Foundation (Additive -- No Breaking Changes)

| # | Task | File | Object | Action | Status |
|---|------|------|--------|--------|--------|
| 1.1 | Create customer PII vault | `sources/definitions/410_CRMA_customer360.sql` (new section) | `CRMI_AGG_TB_CUSTOMER_PII` | DEFINE TABLE: CUSTOMER_ID (PK), FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMAIL, PHONE, EMPLOYER, POSITION, EMPLOYMENT_TYPE, PREFERRED_CONTACT_METHOD. Populated via DT from `CRMI_RAW_TB_CUSTOMER`. | `DONE` |
| 1.2 | Create address PII vault | `sources/definitions/410_CRMA_customer360.sql` (new section) | `CRMI_AGG_TB_ADDRESS_PII` | DEFINE TABLE: CUSTOMER_ID, STREET_ADDRESS, CITY, STATE, ZIPCODE, COUNTRY, ADDRESS_EFFECTIVE_DATE. Populated via DT from `CRMI_RAW_TB_ADDRESSES` (latest per customer). | `DONE` |
| 1.3 | Create employee PII vault | `sources/definitions/415_EMPA_analytics.sql` (new section) | `EMPI_AGG_TB_EMPLOYEE_PII` | DEFINE TABLE: EMPLOYEE_ID (PK), FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMAIL, PHONE. Populated via DT from `EMPI_RAW_TB_EMPLOYEE`. | `DONE` |
| 1.4 | Create customer PII DT | `sources/definitions/410_CRMA_customer360.sql` | `CRMI_AGG_DT_CUSTOMER_PII` | DEFINE DYNAMIC TABLE selecting PII columns from `CRMI_RAW_TB_CUSTOMER` into the vault table. LAG = `{{ lag }}`. | `DONE` |
| 1.5 | Create address PII DT | `sources/definitions/410_CRMA_customer360.sql` | `CRMI_AGG_DT_ADDRESS_PII` | DEFINE DYNAMIC TABLE selecting latest address per customer from `CRMI_RAW_TB_ADDRESSES`. | `DONE` |
| 1.6 | Create employee PII DT | `sources/definitions/415_EMPA_analytics.sql` | `EMPI_AGG_DT_EMPLOYEE_PII` | DEFINE DYNAMIC TABLE selecting PII from `EMPI_RAW_TB_EMPLOYEE`. | `DONE` |
| 1.7 | Masking policy (customer) | `post_deploy.sql` (new section) | `CRMI_AGG_MP_CUSTOMER_PII` | Column masking policy: COMPLIANCE/ACCOUNTADMIN see full values; others see `J*** D***` (names), `****@****.com` (email), `****` (phone), `19XX-XX-XX` (DOB). | `DONE` |
| 1.8 | Masking policy (employee) | `post_deploy.sql` (new section) | `EMPI_AGG_MP_EMPLOYEE_PII` | Same masking pattern for employee PII columns. | `DONE` |
| 1.9 | Row access policy | `post_deploy.sql` (new section) | `CRMI_AGG_RP_CUSTOMER_JURISDICTION` | Row-level filter by customer COUNTRY: Swiss compliance sees CH only, UK compliance sees UK only, ACCOUNTADMIN/DPO see all. | `DONE` |
| 1.10 | Sensitivity tagging | `post_deploy.sql` (existing tag section) | `SENSITIVITY_LEVEL` tag | Apply `RESTRICTED` tag to all PII columns in vault tables. | `DONE` |

---

## Phase 2: DT/View Refactoring (Breaking Changes)

### Customer DTs (file: `sources/definitions/410_CRMA_customer360.sql`)

| # | Task | Object | Columns to Remove | Lines Affected | Status |
|---|------|--------|-------------------|----------------|--------|
| 2.1 | Remove address PII from addresses current | `CRMA_AGG_DT_ADDRESSES_CURRENT` | STREET_ADDRESS, CITY, STATE, ZIPCODE | def: 7-10, select: 20-24 | `DONE` |
| 2.2 | Remove address PII from addresses history | `CRMA_AGG_DT_ADDRESSES_HISTORY` | STREET_ADDRESS, CITY, STATE, ZIPCODE | def: 44-47, select: 59-63 | `DONE` |
| 2.3 | Remove customer PII from customer current | `CRMA_AGG_DT_CUSTOMER_CURRENT` | FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMPLOYER, POSITION, EMAIL, PHONE | def: 82-95, select: 107-120 | `DONE` |
| 2.4 | Remove customer PII from customer history | `CRMA_AGG_DT_CUSTOMER_HISTORY` | FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMPLOYER, POSITION, EMAIL, PHONE | def: 154-168, select: 181-195 | `DONE` |
| 2.5 | Remove customer PII from lifecycle | `CRMA_AGG_DT_CUSTOMER_LIFECYCLE` | FIRST_NAME, FAMILY_NAME, FULL_NAME | def: 216-218, select: 245-247 | `DONE` |
| 2.6 | Remove all PII from Customer 360 | `CRMA_AGG_DT_CUSTOMER_360` | FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMPLOYER, POSITION, EMAIL, PHONE, STREET_ADDRESS, CITY, STATE, ZIPCODE | def: 290-311, select: 408-430 | `DONE` |
| 2.7 | Remove PII from screening status view | `CRMA_AGG_VW_SCREENING_STATUS` | FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH | select: 896-899 | `DONE` |

### Sanctions Views (file: `sources/definitions/302_CRMA_analytics.sql`)

| # | Task | Object | Columns to Remove | Lines Affected | Status |
|---|------|--------|-------------------|----------------|--------|
| 2.8 | Remove PII from sanctions screening | `CRMA_AGG_VW_SANCTIONS_CUSTOMER_SCREENING` | FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH | CTE: 79-81, output: 98-99, final: 207-208 | `DONE` |
| 2.9 | Remove PII from high-risk alerts | `CRMA_AGG_VW_SANCTIONS_HIGH_RISK_ALERTS` | CUSTOMER_NAME (FULL_NAME), CUSTOMER_DOB | select: 322-323 | `DONE` |

### Reporting DTs (file: `sources/definitions/500_REPP_reporting.sql`)

| # | Task | Object | Columns to Remove | Lines Affected | Status |
|---|------|--------|-------------------|----------------|--------|
| 2.10 | Remove FULL_NAME from customer summary | `REPP_AGG_DT_CUSTOMER_SUMMARY` | FULL_NAME | def: 7, select: 24 | `DONE` |
| 2.11 | Remove FULL_NAME from anomaly analysis | `REPP_AGG_DT_ANOMALY_ANALYSIS` | FULL_NAME | def: 225, select: 238 | `DONE` |
| 2.12 | Remove FULL_NAME from lifecycle anomalies | `REPP_AGG_DT_LIFECYCLE_ANOMALIES` | FULL_NAME | def: 337, select: 357 | `DONE` |

### Credit Risk DTs (file: `sources/definitions/520_REPP_credit_risk.sql`)

| # | Task | Object | Columns to Remove | Lines Affected | Status |
|---|------|--------|-------------------|----------------|--------|
| 2.13 | Remove FULL_NAME from IRB ratings | `REPP_AGG_DT_IRB_CUSTOMER_RATINGS` | FULL_NAME | def: 7, select: 30 | `DONE` |

### Loan Reporting (file: `sources/definitions/565_LOAR_reporting.sql`)

| # | Task | Object | Columns to Remove | Lines Affected | Status |
|---|------|--------|-------------------|----------------|--------|
| 2.14 | Remove FULL_NAME from compliance screening | `LOAR_AGG_VW_COMPLIANCE_SCREENING` | FULL_NAME | select: 292 | `DONE` |

### Employee Analytics (file: `sources/definitions/415_EMPA_analytics.sql`)

| # | Task | Object | Columns to Remove | Lines Affected | Status |
|---|------|--------|-------------------|----------------|--------|
| 2.15 | Remove employee PII from hierarchy view | `EMPA_AGG_VW_EMPLOYEE_HIERARCHY` | FIRST_NAME, FAMILY_NAME, FULL_NAME | base: 11-13, recursive: 35-37 | `DONE` |
| 2.16 | Remove employee PII from org chart view | `EMPA_AGG_VW_ORGANIZATIONAL_CHART` | EMPLOYEE_NAME, MANAGER_NAME (FULL_NAME) | select: 71, 82 | `DONE` |
| 2.17 | Remove employee PII from advisor performance | `EMPA_AGG_DT_ADVISOR_PERFORMANCE` | ADVISOR_NAME, TEAM_LEADER_NAME (FULL_NAME) | def: 96/106, select: 138/148 | `DONE` |
| 2.18 | Remove employee PII from portfolio by advisor | `EMPA_AGG_DT_PORTFOLIO_BY_ADVISOR` | ADVISOR_NAME (FULL_NAME) | def: 198, select: 223 | `DONE` |
| 2.19 | Remove employee PII from team leader dashboard | `EMPA_AGG_DT_TEAM_LEADER_DASHBOARD` | TEAM_LEADER_NAME, SUPER_LEADER_NAME (FULL_NAME) | def: 251/257, select: 285/291 | `DONE` |
| 2.20 | Remove mixed PII from current assignments | `EMPA_AGG_VW_CURRENT_ASSIGNMENTS` | CUSTOMER_NAME, ADVISOR_NAME, TEAM_LEADER_NAME | select: 334, 339, 345 | `DONE` |
| 2.21 | Remove mixed PII from assignment history | `EMPA_AGG_VW_ASSIGNMENT_HISTORY` | CUSTOMER_NAME, ADVISOR_NAME | select: 373, 376 | `DONE` |
| 2.22 | Remove PII stubs from advisors view | `EMPA_AGG_VW_ADVISORS` | FULL_NAME, FIRST_NAME, FAMILY_NAME, EMAIL, PHONE, DATE_OF_BIRTH | select: 446-451 | `DONE` |

---

## Phase 3: Consumer Migration

### Late Enrichment Secure Views

| # | Task | File | Object | Join Logic | Status |
|---|------|------|--------|------------|--------|
| 3.1 | Customer 360 enriched view | `sources/definitions/410_CRMA_customer360.sql` | `CRMA_AGG_VW_CUSTOMER_360_ENRICHED` | JOIN `CRMA_AGG_DT_CUSTOMER_360` (ID only) with `CRMI_AGG_TB_CUSTOMER_PII` ON CUSTOMER_ID + `CRMI_AGG_TB_ADDRESS_PII` ON CUSTOMER_ID. SECURE VIEW. | `DONE` |
| 3.2 | Customer summary enriched view | `sources/definitions/500_REPP_reporting.sql` | `REPP_AGG_VW_CUSTOMER_SUMMARY_ENRICHED` | JOIN `REPP_AGG_DT_CUSTOMER_SUMMARY` with `CRMI_AGG_TB_CUSTOMER_PII` ON CUSTOMER_ID. SECURE VIEW. | `DONE` |
| 3.3 | Anomaly analysis enriched view | `sources/definitions/500_REPP_reporting.sql` | `REPP_AGG_VW_ANOMALY_ENRICHED` | JOIN `REPP_AGG_DT_ANOMALY_ANALYSIS` with `CRMI_AGG_TB_CUSTOMER_PII` ON CUSTOMER_ID. SECURE VIEW. | `DONE` |
| 3.4 | IRB ratings enriched view | `sources/definitions/520_REPP_credit_risk.sql` | `REPP_AGG_VW_IRB_ENRICHED` | JOIN `REPP_AGG_DT_IRB_CUSTOMER_RATINGS` with `CRMI_AGG_TB_CUSTOMER_PII` ON CUSTOMER_ID. SECURE VIEW. | `DONE` |
| 3.5 | Employee enriched views | `sources/definitions/415_EMPA_analytics.sql` | `EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED`, `EMPA_AGG_VW_TEAM_DASHBOARD_ENRICHED` | JOIN advisor/team DTs with `EMPI_AGG_TB_EMPLOYEE_PII` ON EMPLOYEE_ID. SECURE VIEW. | `DONE` |
| 3.6 | Screening enriched view | `sources/definitions/302_CRMA_analytics.sql` | `CRMA_AGG_VW_SCREENING_ENRICHED` | JOIN screening view with `CRMI_AGG_TB_CUSTOMER_PII` ON CUSTOMER_ID. Required for PEP name matching. SECURE VIEW. | `DONE` |

### Semantic View Updates (file: `post_deploy.sql`)

Semantic views define the schema that Cortex Agents expose. PII columns in semantic views mean agents return PII in natural language answers. Each must be refactored to reference the enriched secure views, and PII dimension columns must be marked with masking policy annotations.

| # | Task | Object | PII Columns Exposed | Line | Action | Status |
|---|------|--------|---------------------|------|--------|--------|
| 3.7 | Refactor CRM Customer 360 semantic view | `CRMA_SV_CUSTOMER_360` | FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMAIL, PHONE | 1385-1527 | Change base table from `CRMA_AGG_DT_CUSTOMER_360` to `CRMA_AGG_VW_CUSTOMER_360_ENRICHED`. PII columns remain as dimensions but are now served via masking policy on the underlying PII vault. | `DONE` |
| 3.8 | Refactor Employee Advisor semantic view | `EMPA_SV_EMPLOYEE_ADVISOR` | FULL_NAME, FIRST_NAME, FAMILY_NAME, EMAIL, PHONE, DATE_OF_BIRTH | 1532-1625 | Change base table from `EMPA_AGG_DT_ADVISOR_PERFORMANCE` to `EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED`. Employee PII served via masking on `EMPI_AGG_TB_EMPLOYEE_PII`. | `DONE` |
| 3.9 | Refactor Compliance Monitoring semantic view | `PAYA_SV_COMPLIANCE_MONITORING` | FULL_NAME (via Customer 360 join) | 1631-1725 | Change Customer 360 join target to enriched view. Verify FULL_NAME dimension inherits masking. | `DONE` |
| 3.10 | Refactor Wealth Management semantic view | `REPA_SV_WEALTH_MANAGEMENT` | FULL_NAME (via Customer 360 join) | 1731-1890 | Change Customer 360 join target to enriched view. | `DONE` |
| 3.11 | Refactor Risk Reporting semantic view | `REPA_SV_RISK_REPORTING` | FULL_NAME (via Customer 360 join) | 1896-1982 | Change Customer 360 join target to enriched view. | `DONE` |
| 3.12 | Verify LCR semantic views | `LCRS_SV_LCR_CURRENT`, `LCRS_SV_HQLA_BREAKDOWN`, `LCRS_SV_OUTFLOW_BREAKDOWN`, `LCRS_SV_TREND_90DAY`, `LCRS_SV_ALERTS_ACTIVE` | None expected | 1988-2246 | Verify no PII references. No changes expected (LCR is account-level, not customer-name-level). | `DONE` |
| 3.13 | Refactor Loan Compliance semantic view | `LOAS_SV_COMPLIANCE_SCREENING` | FULL_NAME | 2460-2528 | Change base to reference enriched compliance screening view. | `DONE` |
| 3.14 | Verify Loan portfolio semantic views | `LOAS_SV_PORTFOLIO_CURRENT`, `LOAS_SV_LTV_DISTRIBUTION`, `LOAS_SV_APPLICATION_FUNNEL`, `LOAS_SV_AFFORDABILITY_ANALYSIS` | None expected | 2252-2456 | Verify no PII references. No changes expected. | `DONE` |

### Cortex Agent Impact (file: `post_deploy.sql`)

All 6 Cortex Agents use semantic views as their data source. After semantic view refactoring, agents automatically inherit the late enrichment pattern. Key considerations:

| # | Task | Agent | Semantic Views Used | Action | Status |
|---|------|-------|---------------------|--------|--------|
| 3.15 | Validate CRM agent | `CRM_Customer_360` | `CRMA_SV_CUSTOMER_360`, `EMPA_SV_EMPLOYEE_ADVISOR` | After SV refactor (3.7, 3.8), test that agent returns masked names for non-COMPLIANCE roles and full names for COMPLIANCE role. Verify queries like "show top 10 clients" still work. | `DONE` |
| 3.16 | Validate Compliance agent | `COMPLIANCE_MONITORING_AGENT` | `PAYA_SV_COMPLIANCE_MONITORING` | After SV refactor (3.9), test anomaly queries return masked/full names based on role. | `DONE` |
| 3.17 | Validate Risk agent | `RISK_REGULATORY_AGENT` | `REPA_SV_RISK_REPORTING` | After SV refactor (3.11), test that risk queries with customer-level drill-down respect masking. | `DONE` |
| 3.18 | Validate Wealth agent | `WEALTH_ADVISOR_AGENT` | `REPA_SV_WEALTH_MANAGEMENT` | After SV refactor (3.10), test portfolio queries return correct masked/unmasked names. | `DONE` |
| 3.19 | Validate LCR agent | `LIQUIDITY_RISK_AGENT` | `LCRS_SV_*` | Verify no PII impact (LCR is aggregate-level). Smoke test only. | `DONE` |
| 3.20 | Validate Loan agent | `LOAN_PORTFOLIO_AGENT` | `LOAS_SV_*` | After SV refactor (3.13), test compliance screening queries respect masking on FULL_NAME. | `DONE` |
| 3.21 | Agent service role permissions | All agents | N/A | Ensure the agent service role (CICD or dedicated agent role) has COMPLIANCE-level access to PII vaults so agents can return full PII to authorized callers. The masking policy must check the *calling user's role*, not the agent's role. | `DONE` |

### Loan Application Join Fix (file: `post_deploy.sql`)

| # | Task | Line | Current Logic | New Logic | Status |
|---|------|------|---------------|-----------|--------|
| 3.22 | Fix loan-customer join | 786-790 | Joins `LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT_FLAT` to `CRMA_AGG_DT_CUSTOMER_360` ON `FULL_NAME` match | Join via `CRMI_AGG_TB_CUSTOMER_PII` for name matching (PII vault is the only place with names). Or refactor to match on CUSTOMER_ID if email extraction captures it. | `DONE` |

### Streamlit App Updates

| # | Task | File | Action | Status |
|---|------|------|--------|--------|
| 3.23 | Update data loaders | `the_bank_app/utils/data_loaders.py` | Replace all queries referencing PII-containing DTs with `*_ENRICHED` secure views | `DONE` |
| 3.24 | Update Customer 360 tab | `the_bank_app/app.py` | Customer search/display queries use `CRMA_AGG_VW_CUSTOMER_360_ENRICHED` | `DONE` |
| 3.25 | Update AML/Sanctions tabs | `the_bank_app/app.py` | Anomaly and screening tabs use enriched views | `DONE` |
| 3.26 | Update Advisor/Employee tabs | `the_bank_app/app.py` | Advisor management uses `EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED` | `DONE` |

### Notebook Updates

| # | Task | Files | Action | Status |
|---|------|-------|--------|--------|
| 3.27 | Update customer screening notebook | `notebooks/customer_screening_kyc.ipynb` | Query enriched views | `DONE` |
| 3.28 | Update AML notebook | `notebooks/aml_transaction_monitoring.ipynb` | Query enriched views | `DONE` |
| 3.29 | Update sanctions notebook | `notebooks/sanctions_embargo_control.ipynb` | Query enriched views | `DONE` |
| 3.30 | Update compliance notebook | `notebooks/compliance_risk_management.ipynb` | Query enriched views | `DONE` |
| 3.31 | Update wealth notebook | `notebooks/wealth_management.ipynb` | Query enriched views | `DONE` |
| 3.32 | Update employee notebook | `notebooks/employee_relationship_management.ipynb` | Query enriched views | `DONE` |
| 3.33 | Update remaining notebooks | `notebooks/controls_data_quality.ipynb`, `lending_operations.ipynb`, `liquidity_risk_lcr.ipynb` | Verify no PII references; update if needed | `DONE` |

---

## Phase 4: Governance and Validation

| # | Task | File | Object | Action | Status |
|---|------|------|--------|--------|--------|
| 4.1 | GDPR erasure procedure | `post_deploy.sql` | `CRMI_AGG_SP_ERASE_CUSTOMER_PII` | Stored procedure: given CUSTOMER_ID, UPDATE all PII fields to NULL in `CRMI_AGG_TB_CUSTOMER_PII` and `CRMI_AGG_TB_ADDRESS_PII`. Log erasure to audit table. | `DONE` |
| 4.2 | PII audit governance view | `sources/definitions/410_CRMA_customer360.sql` | `CRMI_AGG_VW_PII_GOVERNANCE` | View listing: vault table, row count, masking policy name, tag status, last_access (from ACCESS_HISTORY). | `DONE` |
| 4.3 | PII leakage scan | `sqlunit/tests.sqltest` | New test assertions | Add tests validating zero PII columns (FIRST_NAME, FAMILY_NAME, EMAIL, PHONE, DOB) exist in any non-vault, non-enriched object. | `DONE` |
| 4.4 | Data sharing validation | Manual | All non-PII DTs | Verify all DTs in AGG/REP schemas can be shared via Snowflake Secure Data Sharing without masking policies. | `DONE` |
| 4.5 | Regression test | `sqlunit/tests.sqltest` | Enriched views | Add tests that enriched views return PII for COMPLIANCE role and masked values for PUBLIC role. | `DONE` |

---

## Test Specification

### TS-1: PII Leakage Verification (automated, sqlunit)

Confirms PII is completely removed from all non-vault, non-enriched objects.

| # | Test | SQL | Expected Result | Status |
|---|------|-----|-----------------|--------|
| TS-1.1 | No FIRST_NAME in AGG/REP DTs | `SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'FIRST_NAME' AND TABLE_NAME LIKE '%_DT_%' AND TABLE_NAME NOT LIKE '%_PII%'` | 0 rows | `PENDING` |
| TS-1.2 | No FAMILY_NAME in AGG/REP DTs | Same pattern for FAMILY_NAME | 0 rows | `PENDING` |
| TS-1.3 | No FULL_NAME in AGG/REP DTs | Same pattern for FULL_NAME | 0 rows | `PENDING` |
| TS-1.4 | No EMAIL in AGG/REP DTs | Same pattern for EMAIL | 0 rows | `PENDING` |
| TS-1.5 | No PHONE in AGG/REP DTs | Same pattern for PHONE | 0 rows | `PENDING` |
| TS-1.6 | No DATE_OF_BIRTH in AGG/REP DTs | Same pattern for DATE_OF_BIRTH | 0 rows | `PENDING` |
| TS-1.7 | No STREET_ADDRESS in AGG/REP DTs | Same pattern for STREET_ADDRESS | 0 rows | `PENDING` |
| TS-1.8 | No PII in non-enriched views | `SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME IN ('FIRST_NAME','FAMILY_NAME','FULL_NAME','EMAIL','PHONE','DATE_OF_BIRTH') AND TABLE_TYPE = 'VIEW' AND TABLE_NAME NOT LIKE '%_ENRICHED%' AND TABLE_NAME NOT LIKE '%_PII%'` | 0 rows | `PENDING` |

### TS-2: PII Vault Completeness (automated, sqlunit)

Confirms the PII vault tables contain all customers/employees with correct data.

| # | Test | SQL | Expected Result | Status |
|---|------|-----|-----------------|--------|
| TS-2.1 | Customer PII vault row count | `SELECT COUNT(*) FROM CRMI_AGG_TB_CUSTOMER_PII` | Equals `COUNT(*) FROM CRMI_RAW_TB_CUSTOMER` | `PENDING` |
| TS-2.2 | Address PII vault row count | `SELECT COUNT(*) FROM CRMI_AGG_TB_ADDRESS_PII` | Equals unique customers in CRMI_RAW_TB_ADDRESSES (latest per customer) | `PENDING` |
| TS-2.3 | Employee PII vault row count | `SELECT COUNT(*) FROM EMPI_AGG_TB_EMPLOYEE_PII` | Equals `COUNT(*) FROM EMPI_RAW_TB_EMPLOYEE` | `PENDING` |
| TS-2.4 | No orphan CUSTOMER_IDs | `SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_360 c LEFT JOIN CRMI_AGG_TB_CUSTOMER_PII p ON c.CUSTOMER_ID = p.CUSTOMER_ID WHERE p.CUSTOMER_ID IS NULL` | 0 | `PENDING` |
| TS-2.5 | No orphan EMPLOYEE_IDs | `SELECT COUNT(*) FROM EMPA_AGG_DT_ADVISOR_PERFORMANCE a LEFT JOIN EMPI_AGG_TB_EMPLOYEE_PII p ON a.EMPLOYEE_ID = p.EMPLOYEE_ID WHERE p.EMPLOYEE_ID IS NULL` | 0 | `PENDING` |

### TS-3: Masking Policy Verification (manual, role-switching)

Confirms masking policies work correctly per role. Run each query AS two different roles.

| # | Test | Role | Query | Expected Result | Status |
|---|------|------|-------|-----------------|--------|
| TS-3.1 | Full PII visible for COMPLIANCE | `USE ROLE COMPLIANCE` | `SELECT FULL_NAME, EMAIL, PHONE FROM CRMI_AGG_TB_CUSTOMER_PII LIMIT 5` | Full clear-text values (e.g., "Hans Mueller", "hans@example.com") | `PENDING` |
| TS-3.2 | Masked PII for PUBLIC | `USE ROLE PUBLIC` | Same query | Masked values (e.g., "H*** M***", "****@****.com", "****") | `PENDING` |
| TS-3.3 | DOB masked for PUBLIC | `USE ROLE PUBLIC` | `SELECT DATE_OF_BIRTH FROM CRMI_AGG_TB_CUSTOMER_PII LIMIT 5` | Masked (e.g., "19XX-XX-XX") | `PENDING` |
| TS-3.4 | Employee PII masked for PUBLIC | `USE ROLE PUBLIC` | `SELECT FULL_NAME, EMAIL FROM EMPI_AGG_TB_EMPLOYEE_PII LIMIT 5` | Masked values | `PENDING` |
| TS-3.5 | ACCOUNTADMIN sees full PII | `USE ROLE ACCOUNTADMIN` | `SELECT FULL_NAME, EMAIL FROM CRMI_AGG_TB_CUSTOMER_PII LIMIT 5` | Full clear-text values | `PENDING` |

### TS-4: Late Enrichment Join Correctness (automated, sqlunit)

Confirms enriched views return correct data by comparing against pre-refactor baseline.

| # | Test | SQL | Expected Result | Status |
|---|------|-----|-----------------|--------|
| TS-4.1 | Enriched 360 row count matches | `SELECT COUNT(*) FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED` | Equals `COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_360` | `PENDING` |
| TS-4.2 | Enriched 360 has PII columns | `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'CRMA_AGG_VW_CUSTOMER_360_ENRICHED' AND COLUMN_NAME = 'FULL_NAME'` | 1 row | `PENDING` |
| TS-4.3 | Enriched 360 non-PII columns unchanged | `SELECT CUSTOMER_ID, TOTAL_ACCOUNTS, RISK_CLASSIFICATION, TOTAL_TRANSACTIONS FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED WHERE CUSTOMER_ID = 'CUST_00001'` | Same values as pre-refactor baseline | `PENDING` |
| TS-4.4 | Customer summary enriched matches | `SELECT COUNT(*) FROM REPP_AGG_VW_CUSTOMER_SUMMARY_ENRICHED` | Equals `COUNT(*) FROM REPP_AGG_DT_CUSTOMER_SUMMARY` | `PENDING` |
| TS-4.5 | IRB enriched has FULL_NAME | `SELECT FULL_NAME FROM REPP_AGG_VW_IRB_ENRICHED LIMIT 1` | Non-null value (for COMPLIANCE role) | `PENDING` |
| TS-4.6 | Anomaly enriched has FULL_NAME | `SELECT FULL_NAME FROM REPP_AGG_VW_ANOMALY_ENRICHED LIMIT 1` | Non-null value (for COMPLIANCE role) | `PENDING` |

### TS-5: Cortex Agent PII Behavior (manual, agent testing)

Confirms agents respect masking based on calling user's role.

| # | Test | Agent | Question | COMPLIANCE Role Result | PUBLIC Role Result | Status |
|---|------|-------|----------|----------------------|-------------------|--------|
| TS-5.1 | Customer name query | CRM_Customer_360 | "Who are the top 5 customers by total balance?" | Returns full names (e.g., "Hans Mueller") | Returns masked names (e.g., "H*** M***") | `PENDING` |
| TS-5.2 | Customer search | CRM_Customer_360 | "Show me customer CUST_00001" | Full name, email, phone visible | Masked PII | `PENDING` |
| TS-5.3 | Anomaly with names | COMPLIANCE_MONITORING_AGENT | "Which customers have the most anomalies?" | Full names in results | Masked names | `PENDING` |
| TS-5.4 | Advisor query | WEALTH_ADVISOR_AGENT | "Show advisor performance" | Full advisor names | Masked names | `PENDING` |
| TS-5.5 | Risk query with PII | RISK_REGULATORY_AGENT | "Top customers by risk-weighted assets" | Full names | Masked names | `PENDING` |

### TS-6: GDPR Erasure (manual, procedure testing)

Confirms right-to-erasure works correctly.

| # | Test | Action | Expected Result | Status |
|---|------|--------|-----------------|--------|
| TS-6.1 | Erase single customer | `CALL CRMI_AGG_SP_ERASE_CUSTOMER_PII('CUST_00001')` | All PII fields NULL in `CRMI_AGG_TB_CUSTOMER_PII` for CUST_00001 | `PENDING` |
| TS-6.2 | Address PII also erased | Check `CRMI_AGG_TB_ADDRESS_PII` for CUST_00001 | All address fields NULL | `PENDING` |
| TS-6.3 | Enriched view shows NULL | `SELECT FULL_NAME FROM CRMA_AGG_VW_CUSTOMER_360_ENRICHED WHERE CUSTOMER_ID = 'CUST_00001'` | NULL (not masked, actually erased) | `PENDING` |
| TS-6.4 | Non-PII data intact | `SELECT CUSTOMER_ID, TOTAL_ACCOUNTS, RISK_CLASSIFICATION FROM CRMA_AGG_DT_CUSTOMER_360 WHERE CUSTOMER_ID = 'CUST_00001'` | All non-PII columns unchanged | `PENDING` |
| TS-6.5 | Erasure logged | Check audit table for erasure log entry | Row with CUSTOMER_ID, erasure_timestamp, erased_by | `PENDING` |
| TS-6.6 | Other customers unaffected | `SELECT FULL_NAME FROM CRMI_AGG_TB_CUSTOMER_PII WHERE CUSTOMER_ID = 'CUST_00002'` | Full PII still present | `PENDING` |

### TS-7: Data Sharing Readiness (manual, validation)

Confirms non-PII tables are safe for external sharing.

| # | Test | Action | Expected Result | Status |
|---|------|--------|-----------------|--------|
| TS-7.1 | DTs have no PII | Scan all DTs with `INFORMATION_SCHEMA.COLUMNS` query from TS-1 | 0 PII columns in any DT except PII vaults | `PENDING` |
| TS-7.2 | Create test share | `CREATE SHARE test_share; GRANT USAGE ON ... TO SHARE test_share;` for `CRMA_AGG_DT_CUSTOMER_360` | Share creation succeeds, no masking policy warnings | `PENDING` |
| TS-7.3 | Shared data has no PII | Consumer account queries the shared Customer 360 | Only CUSTOMER_ID, no names/email/phone/DOB columns exist | `PENDING` |

### TS-8: Regression -- Functional Parity (automated + manual)

Confirms existing functionality works identically after refactoring.

| # | Test | Component | Action | Expected Result | Status |
|---|------|-----------|--------|-----------------|--------|
| TS-8.1 | sqlunit full suite | `sqlunit/tests.sqltest` | Run all 59 existing tests | All pass | `PENDING` |
| TS-8.2 | Streamlit Customer 360 | `the_bank_app` | Search for customer, view profile | Name, email, phone displayed (for authorized user) | `PENDING` |
| TS-8.3 | Streamlit AML tab | `the_bank_app` | View anomaly alerts | Customer names displayed alongside alerts | `PENDING` |
| TS-8.4 | Streamlit Advisor tab | `the_bank_app` | View advisor performance | Advisor names, team leader names displayed | `PENDING` |
| TS-8.5 | Notebook KYC screening | `notebooks/customer_screening_kyc.ipynb` | Run all cells | PEP matching works with names from enriched view | `PENDING` |
| TS-8.6 | Notebook sanctions | `notebooks/sanctions_embargo_control.ipynb` | Run all cells | Sanctions name matching works | `PENDING` |
| TS-8.7 | PEP fuzzy matching accuracy | Compare PEP match results pre vs post refactoring | Same customers flagged, same match scores | `PENDING` |

---

## Test Summary

| Category | Tests | Automated | Manual | Purpose |
|----------|-------|-----------|--------|---------|
| TS-1: PII Leakage | 8 | 8 | 0 | Confirm zero PII outside vaults |
| TS-2: Vault Completeness | 5 | 5 | 0 | Confirm vault has all data |
| TS-3: Masking Policies | 5 | 0 | 5 | Confirm role-based masking works |
| TS-4: Join Correctness | 6 | 6 | 0 | Confirm enriched views return correct data |
| TS-5: Agent PII Behavior | 5 | 0 | 5 | Confirm agents respect masking |
| TS-6: GDPR Erasure | 6 | 0 | 6 | Confirm right-to-erasure works |
| TS-7: Data Sharing | 3 | 0 | 3 | Confirm tables are share-safe |
| TS-8: Regression | 7 | 2 | 5 | Confirm no functionality broken |
| **Total** | **45** | **21** | **24** | |

---

## Implementation Summary

| Phase | Tasks | New Objects | Modified Objects | Files Changed |
|-------|-------|-------------|------------------|---------------|
| 1 | 10 | 6 tables/DTs, 3 policies, 1 tag update | 0 | 3 (410, 415, post_deploy) |
| 2 | 22 | 0 | 22 DTs/views | 6 (410, 302, 500, 520, 565, 415) |
| 3 | 33 | 6 secure views | 8 semantic views, 6 agents (validate), 1 loan join, 1 Streamlit, 9 notebooks | 12+ (post_deploy, 410, 302, 500, 520, 415, app.py, data_loaders, 9 notebooks) |
| 4 | 5 | 1 procedure, 1 view | test suite | 3 (post_deploy, 410, tests.sqltest) |
| **Total** | **70** | **14** | **55+** | **15+** |

---

## Risk and Rollback

| Risk | Mitigation |
|------|-----------|
| Phase 2 breaks downstream consumers | Deploy Phase 3 (enriched views) simultaneously with Phase 2 using DCM single transaction |
| PEP screening requires name matching | `CRMA_AGG_VW_SCREENING_ENRICHED` must join PII before fuzzy match logic -- validate matching accuracy unchanged |
| Performance impact of late JOIN | PII vault tables are small (5K customers, 39 employees) -- JOIN on PK is negligible. Index CUSTOMER_ID. |
| Masking policies block Cortex Agent | Cortex Agents inherit calling user's role -- ensure agent service role has COMPLIANCE privileges |
| Existing data in DTs contains PII | After Phase 2 deploy, DTs will re-compute without PII on next refresh. Force refresh after deploy. |
