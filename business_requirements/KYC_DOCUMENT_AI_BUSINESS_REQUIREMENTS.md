# Business Requirements: KYC Document Processing (Document AI)

## Executive Summary

Automated extraction and validation of Know Your Customer (KYC) documents using Snowflake Document AI. Extends the existing loan email extraction pattern to cover ID verification, proof of address, and source of wealth declarations.

## Business Value

### Value Add

- Reduce KYC processing time from 45 minutes to under 2 minutes per customer (97% reduction)
- Automated completeness validation catches missing documents before onboarding completes
- Multi-jurisdiction support (CH/UK/EU) ensures regulatory compliance without manual per-country checklists
- Expired document detection prevents compliance gaps from aging documentation

### Risk of Inaction

- KYC remains the #1 onboarding bottleneck (average 45 min/customer)
- Manual review introduces human error in document validation
- Expired documents go undetected until the next periodic review (12-month cycle)
- Scaling to more customers requires proportional headcount increase in compliance

## Business Context

Current state: Document AI is used only for mortgage loan emails (`LOAI_RAW_TK_EXTRACT_MAIL_DATA`). KYC document review is a manual process -- compliance analysts read PDFs and enter data into forms. Average processing time: 45 minutes per customer onboarding.

Target state: Automated KYC document pipeline that extracts structured data from uploaded documents, validates completeness against regulatory requirements per jurisdiction, and flags missing or expired documents. Target processing time: under 2 minutes per customer.

## Stakeholders

| Role | Interest |
|------|----------|
| KYC Analysts | Automated extraction reduces manual data entry |
| Compliance Officers | Completeness validation ensures regulatory adherence |
| Client Advisors | Faster onboarding improves client experience |
| Data Governance | Structured PII handling with sensitivity tagging |
| Operations | SLA tracking for document processing |

## Functional Requirements

### FR-KY-01: Document Upload Stage -- `NEW`
Create stage `CRMI_RAW_ST_KYC_DOCS` accepting PDF, scanned images, and structured text files. Directory table enabled for metadata tracking.

### FR-KY-02: ID Document Extraction -- `NEW`
Extract from ID documents using `SNOWFLAKE.CORTEX.AI_EXTRACT`: full name, date of birth, nationality, document number, expiry date, issuing country. Store in `CRMI_RAW_TB_KYC_EXTRACTS`.

### FR-KY-03: Proof of Address Extraction -- `NEW`
Extract from proof of address documents: full name, street, city, postal code, country, document date. Validate address freshness (must be < 3 months old).

### FR-KY-04: Source of Wealth Extraction -- `NEW`
Extract from source of wealth declarations: employment details, income range, wealth origin, declared assets. Required for accounts exceeding CHF 250K (FINMA threshold).

### FR-KY-05: Customer Master Validation -- `NEW`
Fuzzy-match extracted names against `CRMI_RAW_TB_CUSTOMER` (Levenshtein distance). Flag mismatches with similarity score. Cross-validate DOB and nationality.

### FR-KY-06: Jurisdiction Completeness Rules -- `NEW`
Assess KYC completeness per customer country using `LOAI_REF_TB_COUNTRY_REGIME_CONFIG`:
- Switzerland (FINMA): ID + proof of address + source of wealth (accounts > CHF 250K)
- UK (FCA): ID + proof of address + enhanced due diligence for PEPs
- EU (AMLD6): ID + proof of address + beneficial ownership declaration

### FR-KY-07: Expired Document Detection -- `NEW`
Flag expired ID documents (expiry_date < today) and stale address proofs (document_date > 3 months ago). Output: customer_id, document_type, days_since_expiry, severity.

### FR-KY-08: KYC Completeness Score -- `NEW`
Generate a per-customer KYC completeness score (0-100%) as a dynamic table. Dimensions: documents_present, documents_valid, documents_fresh, PEP_enhanced_due_diligence.

### FR-KY-09: Customer 360 Integration -- `NEW`
Feed KYC completeness score and document status into `CRMA_AGG_DT_CUSTOMER_360`. Surface in Customer 360 Streamlit tab and KYC Screening notebook.

### FR-KY-10: PII Sensitivity Tagging -- `NEW`
Apply `SENSITIVITY_LEVEL = 'RESTRICTED'` tags to all extracted PII columns (name, DOB, address, document number) in `CRMI_RAW_TB_KYC_EXTRACTS`.

### FR-KY-11: Synthetic Data Generator -- `NEW`
Create `generators/kyc_document_generator.py` producing synthetic ID documents, proof of address, and source of wealth declarations. Include: complete, incomplete, expired, and mismatched variants for testing.

## Snowflake Features

- `SNOWFLAKE.CORTEX.AI_EXTRACT` for document field extraction
- `SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT` for PDF/image parsing
- Dynamic table for automated completeness scoring
- Sensitivity tags on extracted PII columns

## Required Data Sources

| Source | Schema | Object | Status | Purpose |
|--------|--------|--------|--------|---------|
| KYC documents | CRM_RAW_V001 | CRMI_RAW_ST_KYC_DOCS | NEW | Uploaded PDF/image files |
| KYC extracts | CRM_RAW_V001 | CRMI_RAW_TB_KYC_EXTRACTS | NEW | AI-extracted structured data |
| Customer master | CRM_RAW_V001 | CRMI_RAW_TB_CUSTOMER | EXISTS | Name, DOB, nationality for validation |
| Customer addresses | CRM_RAW_V001 | CRMI_RAW_TB_ADDRESSES | EXISTS | Address matching for proof of address |
| Customer 360 | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360 | EXISTS | Enriched customer context |
| PEP data | CRM_RAW_V001 | CRMI_RAW_TB_EXPOSED_PERSON | EXISTS | PEP status for enhanced due diligence |
| Loan email schema config | LOA_RAW_V001 | LOAI_RAW_TB_EMAIL_INBOUND_LOAN_SCHEMA_CONFIG | EXISTS | Pattern for AI_EXTRACT schema definition |
| Country regulatory regimes | LOA_RAW_V001 | LOAI_REF_TB_COUNTRY_REGIME_CONFIG | EXISTS | Per-country KYC/lending requirements |

## Data Generator Requirements

New generator `generators/kyc_document_generator.py` producing:
- Synthetic ID documents (structured text simulating passport/ID card extracts)
- Synthetic proof of address documents (utility bill / bank statement extracts)
- Synthetic source of wealth declarations
- Mix of complete, incomplete, expired, and mismatched documents for testing

## Acceptance Criteria

- AI extraction accuracy above 90% on synthetic KYC documents
- Completeness scoring covers all 3 jurisdiction types (CH, UK, EU)
- Expired documents flagged with days-since-expiry metric
- Name matching between extracted ID and customer master uses fuzzy logic (Levenshtein)
- KYC completeness results visible in Customer 360 and KYC Screening notebook
- PII columns tagged with SENSITIVITY_LEVEL = 'RESTRICTED'
