# Business Requirements: Compliance Document Search (Cortex Search)

## Executive Summary

Full-text semantic search over compliance documents, SWIFT payment messages, and internal communications. Enables compliance officers, auditors, and investigators to find relevant documents using natural language queries instead of SQL filters.

## Business Value

### Value Add

- Reduce investigation time from hours to minutes: compliance officers search natural language instead of writing SQL joins across SWIFT, loan, and customer tables
- Audit response time drops from days to hours for regulatory data requests
- Cross-document pattern detection: find connections between SWIFT payments, loan applications, and PEP status that manual review misses
- Demonstrates Cortex Search as a differentiator for unstructured banking data

### Risk of Inaction

- Investigations remain SQL-dependent, limiting who can perform them
- Regulatory data requests take days, increasing examination friction and potential findings
- Hidden patterns across document types go undetected (AML exposure)
- SWIFT and loan data remain siloed despite being in the same platform

## Business Context

Current state: SWIFT messages are stored as parsed XML in tables, queryable only via SQL. Loan emails are extracted via Document AI but not searchable by content. Compliance policies exist as static markdown files with no search interface.

Target state: A unified search service where users can ask "show me all SWIFT payments over 100K EUR involving counterparties in sanctioned jurisdictions" and get ranked, contextual results across all document types.

## Stakeholders

| Role | Interest |
|------|----------|
| Compliance Officers | Investigate suspicious payment patterns across documents |
| AML Analysts | Search SWIFT messages for specific counterparties, amounts, or remittance info |
| Internal Audit | Find policy-relevant evidence across communications |
| Legal | Locate relevant documents for regulatory inquiries |
| Lending Officers | Search loan application correspondence by topic |

## Functional Requirements

### FR-CS-01: SWIFT Message Indexing -- `NEW`
Create a unified document table that flattens SWIFT pacs.008 and pacs.002 messages into searchable text. Include: debtor name, creditor name, amount, currency, BIC codes, remittance info, message ID, and timestamp. Source: `ICGA_AGG_DT_SWIFT_PACS008`, `ICGA_AGG_DT_SWIFT_PACS002`.

### FR-CS-02: Loan Document Indexing -- `NEW`
Index loan mortgage email documents from `LOAI_RAW_ST_EMAIL_INBOUND` stage. Include extracted metadata from `LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT`: customer name, property address, loan amount, employment details.

### FR-CS-03: Compliance Policy Indexing -- `NEW`
Ingest business requirement markdown files (AML, LCR, Lending) into a searchable table. Parse section headings as metadata for filtering by policy domain.

### FR-CS-04: Cortex Search Service Creation -- `NEW`
Create a Cortex Search Service over the unified document table with: text column (document content), filter columns (document_type, date, currency, amount_range), and embedding-based semantic retrieval.

### FR-CS-05: Structured Metadata Filtering -- `NEW`
Support filtering search results by: document type (SWIFT/LOAN/POLICY), date range (from/to), amount range (min/max), currency, and counterparty name. Filters must work in combination with natural language queries.

### FR-CS-06: Result Ranking and Snippets -- `NEW`
Return search results ranked by relevance score. Each result must include: document type, date, key entities (debtor, creditor, customer), matched snippet with context, and relevance score.

### FR-CS-07: Customer Context Enrichment -- `NEW`
Enrich search results with customer risk context by joining hits against `CRMI_RAW_TB_CUSTOMER` (risk classification, account tier) and `CRMI_RAW_TB_EXPOSED_PERSON` (PEP status). Flag results involving PEP or high-risk customers.

### FR-CS-08: Hybrid Search + Agent Mode -- `NEW`
Integrate Cortex Search into the existing "Ask AI" Streamlit tab. When a user query is better answered by document search than SQL, route to Cortex Search. Support hybrid: combine search results (unstructured) with Cortex Agent answers (structured SQL).

### FR-CS-09: Notebook Demonstration -- `NEW`
Create an interactive notebook (`notebooks/compliance_document_search.ipynb`) demonstrating: search queries, filter combinations, result exploration, and cross-referencing with PEP/sanctions data.

### FR-CS-10: Audit Trail -- `NEW`
Log all compliance search queries with: user, timestamp, query text, number of results, and documents accessed. Store in a dedicated audit table for regulatory examination evidence.

## Snowflake Features

- Cortex Search Service for semantic document retrieval
- Cortex Search with filter columns for structured metadata filtering
- Integration with existing Cortex Agents for hybrid RAG + SQL

## Required Data Sources

| Source | Schema | Object | Status | Purpose |
|--------|--------|--------|--------|---------|
| SWIFT messages (raw XML) | PAY_RAW_V001 | ICGI_RAW_TB_SWIFT_MESSAGES | EXISTS | Payment messages with XML content |
| SWIFT parsed (pacs.008) | PAY_AGG_V001 | ICGA_AGG_DT_SWIFT_PACS008 | EXISTS | Structured debtor/creditor/amount fields |
| SWIFT parsed (pacs.002) | PAY_AGG_V001 | ICGA_AGG_DT_SWIFT_PACS002 | EXISTS | Payment status and settlement info |
| Loan emails (raw) | LOA_RAW_V001 | LOAI_RAW_ST_EMAIL_INBOUND | EXISTS | Original email .eml files on stage |
| Loan extracts | LOA_RAW_V001 | LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT | EXISTS | AI-extracted loan application data |
| Compliance policies | (new) | Business requirement markdown files | NEW | AML, LCR, Lending policy text |
| Customer master | CRM_RAW_V001 | CRMI_RAW_TB_CUSTOMER | EXISTS | Customer context for enriching search results |
| PEP/Sanctions data | CRM_RAW_V001 | CRMI_RAW_TB_EXPOSED_PERSON | EXISTS | Cross-reference search hits with PEP status |

## Acceptance Criteria

- Search service indexes all SWIFT XML messages and loan documents
- Natural language queries return relevant results within 2 seconds
- Results include document type, date, key entities, and relevance score
- Filters on amount, currency, date range, and counterparty work correctly
- Search accessible via Streamlit "Ask AI" tab and standalone notebook
- Hybrid mode: search results can be combined with SQL-based agent answers
