# Business Requirements: Late Enrichment -- PII Data Separation

## Executive Summary

Centralize all customer PII (Personally Identifiable Information) in a single protected table at the AGG maturity level (`CRMI_AGG_TB_CUSTOMER_PII`). Remove PII columns from all downstream dynamic tables, views, and reporting objects. At display time (Streamlit, notebooks, semantic views), authorized users join back to the PII table -- unauthorized users see only surrogate keys.

## Business Value

### Value Add

- GDPR Article 25 compliance (Data Protection by Design): PII exists in exactly one place, simplifying right-to-erasure and data subject access requests
- Eliminates cross-maturity-level access: AGG/REP objects never reach back to RAW for PII
- Single point of masking policy enforcement -- one table to protect instead of 16 objects
- Enables data sharing without PII: downstream tables can be shared to external consumers (marketplace, partners) without masking concerns
- Reduces blast radius of a data breach: compromising a reporting table reveals no personal data

### Risk of Inaction

- PII scattered across 76 column instances in 16 objects -- every object requires individual masking policies
- GDPR right-to-erasure requires updating/deleting PII in 16 places instead of 1
- Cross-maturity-level access: reporting views currently reach into RAW tables for names -- violates data architecture principles
- Data sharing is impossible without per-table masking -- blocks marketplace and partner use cases
- Audit finding: "PII proliferation" is a common regulatory concern in banking examinations

## Business Context

Current state: PII (names, DOB, email, phone, address, employer) is copied from `CRMI_RAW_TB_CUSTOMER` into every downstream dynamic table via SQL SELECT. Customer 360, anomaly analysis, IRB ratings, lifecycle, and screening views all contain full PII. Sensitivity tags exist (`SENSITIVITY_LEVEL`) but are applied post-hoc.

Target state: A single PII vault table at AGG level (`CRMI_AGG_TB_CUSTOMER_PII`) holds all personal data. All other objects carry only `CUSTOMER_ID`. The Streamlit app and notebooks perform a late JOIN to the PII table at display time, governed by masking policies and RBAC roles.

## Stakeholders

| Role | Interest |
|------|----------|
| Data Protection Officer | GDPR compliance, right-to-erasure, data minimization |
| Data Governance | Single point of PII control, sensitivity tagging |
| Data Architecture | Clean maturity-level boundaries (RAW -> AGG -> REP) |
| Compliance | Audit-ready PII inventory, masking evidence |
| IT Security | Reduced attack surface, breach containment |
| Data Sharing / Marketplace | PII-free datasets for external sharing |

## Functional Requirements

### FR-LE-01: PII Vault Table at AGG Level -- `NEW`
Create `CRM_AGG_V001.CRMI_AGG_TB_CUSTOMER_PII` containing all customer PII columns: CUSTOMER_ID (PK), FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMAIL, PHONE, EMPLOYER, POSITION, EMPLOYMENT_TYPE, PREFERRED_CONTACT_METHOD. Populated from `CRMI_RAW_TB_CUSTOMER` via a dynamic table or task, respecting RAW -> AGG data flow.

### FR-LE-02: Address PII Vault -- `NEW`
Create `CRM_AGG_V001.CRMI_AGG_TB_ADDRESS_PII` containing: CUSTOMER_ID, STREET_ADDRESS, CITY, STATE, ZIPCODE, COUNTRY, ADDRESS_EFFECTIVE_DATE. Populated from `CRMI_RAW_TB_ADDRESSES` via existing address current/history DTs, but separated from non-PII attributes.

### FR-LE-03: Employee PII Vault -- `NEW`
Create `CRM_AGG_V001.EMPI_AGG_TB_EMPLOYEE_PII` containing: EMPLOYEE_ID, FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMAIL, PHONE. Populated from `EMPI_RAW_TB_EMPLOYEE`. Employee views (`EMPA_AGG_VW_ADVISORS`, `EMPA_AGG_VW_EMPLOYEE_HIERARCHY`) join here at query time.

### FR-LE-04: Masking Policy on PII Tables -- `NEW`
Apply column-level masking policies on all PII vault tables. Users with role `COMPLIANCE` or `ACCOUNTADMIN` see full values. All other roles see masked values (e.g., `J*** D***` for names, `****@****.com` for email, `****` for phone).

### FR-LE-05: Row Access Policy -- `NEW`
Apply row access policies on PII vault tables restricting access by data jurisdiction. Swiss compliance officers see Swiss customers only. UK compliance sees UK customers only. Only `ACCOUNTADMIN` and `DPO` roles see all rows.

### FR-LE-06: Remove PII from Customer 360 DT -- `NEW`
Refactor `CRMA_AGG_DT_CUSTOMER_360` definition (in `sources/definitions/410_CRMA_customer360.sql`) to exclude: FIRST_NAME, FAMILY_NAME, FULL_NAME, DATE_OF_BIRTH, EMAIL, PHONE, EMPLOYER, POSITION, STREET_ADDRESS, CITY, ZIPCODE. Retain only CUSTOMER_ID and all non-PII analytics columns.

### FR-LE-07: Remove PII from Downstream AGG DTs -- `NEW`
Refactor the following dynamic tables to remove PII columns, retaining only CUSTOMER_ID:
- `CRMA_AGG_DT_CUSTOMER_CURRENT` -- remove FIRST_NAME, FAMILY_NAME, FULL_NAME, DOB, EMAIL, PHONE, EMPLOYER, POSITION
- `CRMA_AGG_DT_CUSTOMER_HISTORY` -- same columns
- `CRMA_AGG_DT_CUSTOMER_LIFECYCLE` -- remove FIRST_NAME, FAMILY_NAME, FULL_NAME
- `CRMA_AGG_DT_ADDRESSES_CURRENT` -- remove STREET_ADDRESS, CITY, ZIPCODE
- `CRMA_AGG_DT_ADDRESSES_HISTORY` -- same columns

### FR-LE-08: Remove PII from Reporting DTs -- `NEW`
Refactor the following REP_AGG_V001 dynamic tables to replace FULL_NAME with CUSTOMER_ID-only reference:
- `REPP_AGG_DT_CUSTOMER_SUMMARY` -- remove FULL_NAME
- `REPP_AGG_DT_ANOMALY_ANALYSIS` -- remove FULL_NAME
- `REPP_AGG_DT_IRB_CUSTOMER_RATINGS` -- remove FULL_NAME
- `REPP_AGG_DT_LIFECYCLE_ANOMALIES` -- remove FULL_NAME

### FR-LE-09: Remove PII from Views -- `NEW`
Refactor the following views to remove PII, joining to PII vault at query time instead:
- `CRMA_AGG_VW_SCREENING_ALERTS` -- remove FIRST_NAME, FAMILY_NAME, FULL_NAME, DOB
- `CRMA_AGG_VW_SCREENING_STATUS` -- same
- `LOAR_AGG_VW_COMPLIANCE_SCREENING` -- remove FULL_NAME
- `EMPA_AGG_VW_ADVISORS` -- join to EMPI_AGG_TB_EMPLOYEE_PII
- `EMPA_AGG_VW_EMPLOYEE_HIERARCHY` -- same

### FR-LE-10: Late Enrichment Secure Views -- `NEW`
Create secure views that perform the late join for authorized users:
- `CRMA_AGG_VW_CUSTOMER_360_ENRICHED` -- joins `CRMA_AGG_DT_CUSTOMER_360` + `CRMI_AGG_TB_CUSTOMER_PII` + `CRMI_AGG_TB_ADDRESS_PII`
- `REPP_AGG_VW_CUSTOMER_SUMMARY_ENRICHED` -- joins reporting summary + PII
- `REPP_AGG_VW_ANOMALY_ENRICHED` -- joins anomaly analysis + PII
These views are SECURE VIEWS, ensuring the masking policies on the PII tables are respected.

### FR-LE-11: Semantic View Refactoring -- `NEW`
Update all 15 semantic views to reference the late-enrichment secure views instead of the PII-containing DTs. The Cortex Agents see PII only if the calling user has the appropriate role.

### FR-LE-12: Streamlit App Refactoring -- `NEW`
Refactor `the_bank_app/utils/data_loaders.py` to query the late-enrichment secure views (`*_ENRICHED`) instead of the PII-containing DTs directly. The masking policy handles what the user sees -- no application-level PII filtering needed.

### FR-LE-13: Notebook Refactoring -- `NEW`
Update all 9 notebooks to query the late-enrichment secure views. Notebooks run in user context, so masking policies apply automatically.

### FR-LE-14: PII Audit and Governance -- `NEW`
Create a governance view listing all PII vault tables, their row counts, masking policy status, last access timestamp, and data subject request queue. Tag all PII columns with `SENSITIVITY_LEVEL = 'RESTRICTED'` using the existing tag infrastructure.

### FR-LE-15: GDPR Right-to-Erasure Procedure -- `NEW`
Create stored procedure `CRMI_AGG_SP_ERASE_CUSTOMER_PII` that, given a CUSTOMER_ID, nullifies all PII fields in the vault tables. Since downstream objects no longer contain PII, a single UPDATE covers the entire erasure obligation.

### FR-LE-16: Data Sharing Readiness -- `NEW`
Validate that all non-PII tables (DTs, reporting) can be shared via Snowflake Secure Data Sharing or Marketplace listings without additional masking. The late enrichment pattern makes this possible by design.

## Snowflake Features

- Column-level masking policies
- Row access policies
- Secure views
- Sensitivity tags (existing `SENSITIVITY_LEVEL`)
- Dynamic tables (PII vault populated from RAW)
- RBAC (role-based access control)

## Required Data Sources

| Source | Schema | Object | Status | Purpose |
|--------|--------|--------|--------|---------|
| Customer master (PII source) | CRM_RAW_V001 | CRMI_RAW_TB_CUSTOMER | EXISTS | Source of customer PII |
| Address data (PII source) | CRM_RAW_V001 | CRMI_RAW_TB_ADDRESSES | EXISTS | Source of address PII |
| Employee data (PII source) | CRM_RAW_V001 | EMPI_RAW_TB_EMPLOYEE | EXISTS | Source of employee PII |
| PII vault (customer) | CRM_AGG_V001 | CRMI_AGG_TB_CUSTOMER_PII | NEW | Centralized customer PII table |
| PII vault (address) | CRM_AGG_V001 | CRMI_AGG_TB_ADDRESS_PII | NEW | Centralized address PII table |
| PII vault (employee) | CRM_AGG_V001 | EMPI_AGG_TB_EMPLOYEE_PII | NEW | Centralized employee PII table |
| Customer 360 (refactored) | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360 | EXISTS (modify) | Remove PII columns |
| Customer current (refactored) | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_CURRENT | EXISTS (modify) | Remove PII columns |
| Customer history (refactored) | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_HISTORY | EXISTS (modify) | Remove PII columns |
| Customer lifecycle (refactored) | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_LIFECYCLE | EXISTS (modify) | Remove PII columns |
| Addresses current (refactored) | CRM_AGG_V001 | CRMA_AGG_DT_ADDRESSES_CURRENT | EXISTS (modify) | Remove PII columns |
| Addresses history (refactored) | CRM_AGG_V001 | CRMA_AGG_DT_ADDRESSES_HISTORY | EXISTS (modify) | Remove PII columns |
| Customer summary (refactored) | REP_AGG_V001 | REPP_AGG_DT_CUSTOMER_SUMMARY | EXISTS (modify) | Remove FULL_NAME |
| Anomaly analysis (refactored) | REP_AGG_V001 | REPP_AGG_DT_ANOMALY_ANALYSIS | EXISTS (modify) | Remove FULL_NAME |
| IRB ratings (refactored) | REP_AGG_V001 | REPP_AGG_DT_IRB_CUSTOMER_RATINGS | EXISTS (modify) | Remove FULL_NAME |
| Lifecycle anomalies (refactored) | REP_AGG_V001 | REPP_AGG_DT_LIFECYCLE_ANOMALIES | EXISTS (modify) | Remove FULL_NAME |
| Sensitivity tag | CRM_AGG_V001 | SENSITIVITY_LEVEL tag | EXISTS | PII column tagging |

## Acceptance Criteria

- Zero PII columns in any DT or reporting table outside the PII vault tables
- All PII vault tables protected by masking and row access policies
- Late-enrichment secure views return full PII for authorized roles, masked PII for others
- GDPR erasure procedure deletes PII in a single operation (no multi-table cleanup)
- All 9 notebooks and 16 Streamlit tabs continue to function correctly via enriched views
- Cortex Agents respect masking policies based on calling user's role
- Non-PII tables pass a data sharing readiness check (no PII leakage)
