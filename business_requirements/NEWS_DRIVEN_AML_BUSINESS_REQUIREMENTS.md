# Business Requirements: News-Driven AML Enrichment (Cross-Project)

## Executive Summary

Correlate payment transaction anomalies from SyntheticRetailBank AML monitoring with real-time news events from the crew_asset_management RSS pipeline. Provides context-aware AML investigation by automatically linking suspicious transactions to relevant market events, sanctions announcements, or company crises.

## Business Value

### Value Add

- Context-aware AML: "this large EUR transfer coincides with a sanctions escalation in that sector" -- reduces investigation time by 60%
- Reduces false positive rate: industry average is 95%+ false positives; news context immediately explains many legitimate alerts
- Accelerates SAR (Suspicious Activity Report) decision-making with automated evidence gathering
- Demonstrates Cortex Search + structured SQL hybrid -- a unique differentiator for AML platforms

### Risk of Inaction

- AML alerts reviewed in isolation without market context -- analysts waste time on alerts that news context would explain
- False positive rate remains above 95%, consuming compliance analyst capacity
- SAR filing decisions lack supporting evidence from market events
- No connection between geopolitical events and transaction pattern changes

## Business Context

Current state: Transaction anomalies are flagged by rule-based scoring in `PAYA_AGG_DT_TRANSACTION_ANOMALIES`. SWIFT payments are parsed into structured fields. Separately, crew_asset_management collects 31 RSS feeds, enriches headlines with entity extraction (Cortex AI_COMPLETE), and scores materiality. These systems are not connected.

Target state: When a transaction is flagged as anomalous, automatically search for correlated news events involving the same counterparty, sector, or country. Present news context alongside the alert for faster investigation.

## Stakeholders

| Role | Interest |
|------|----------|
| AML Analysts | Faster investigation with news context |
| Financial Intelligence Unit (FIU) | Evidence for SAR filings |
| Compliance Officers | Reduced false positive workload |
| Sanctions Officers | Link sanctions news to affected transactions |
| Internal Audit | Investigation audit trail with evidence |

## Functional Requirements

### FR-NA-01: Anomaly-News Correlation Engine -- `NEW`
For each flagged transaction anomaly, search SAM_DEMO news events for correlated articles. Match on: counterparty name (fuzzy), country, sector (via `NEWS_RAW_TB_SECTOR_TAXONOMY`), and time window (+/- 7 days of transaction date).

### FR-NA-02: Entity Matching -- `NEW`
Join transaction counterparty names from `ICGA_AGG_DT_SWIFT_PACS008` (debtor/creditor) with enriched news entities from SAM_DEMO (extracted via Cortex AI_COMPLETE). Use `NEWS_RAW_TB_ISSUER_TICKER_MAP` for company name resolution and `NEWS_RAW_TB_COUNTRY_ALIAS_MAP` for country matching.

### FR-NA-03: Cortex Search Integration -- `NEW`
Use SAM_DEMO's `KNOWLEDGE_SEARCH_SVC` Cortex Search Service to semantically search for news relevant to flagged transactions. Query format: "[counterparty] [country] [sector] sanctions OR crisis OR fraud".

### FR-NA-04: News Context Enrichment Table -- `NEW`
Create a dynamic table joining each anomaly alert with its top 5 correlated news articles. Columns: anomaly_id, transaction_id, customer_id, news_headline, news_source, news_date, relevance_score, entity_match_type (COUNTERPARTY/SECTOR/COUNTRY).

### FR-NA-05: Alert Severity Re-Scoring -- `NEW`
Re-score anomaly severity based on news context: if correlated news indicates sanctions/crisis/fraud, escalate severity. If news explains legitimate activity (market disruption, corporate action), suggest de-escalation. Output: original_severity, news_adjusted_severity, adjustment_reason.

### FR-NA-06: SAR Evidence Package -- `NEW`
For alerts recommended for SAR filing, generate an evidence package: transaction details, anomaly score breakdown, correlated news articles with dates, customer risk profile (PEP status, sanctions flags), and SWIFT message details.

### FR-NA-07: Investigation Workflow -- `NEW`
Add "News Context" panel to the AML Monitoring Streamlit tab. When analyst clicks an alert, show: transaction details (left), correlated news articles (right), customer risk profile (bottom). Include "Escalate" and "Dismiss with Reason" actions.

### FR-NA-08: Sanctions News Fast-Track -- `NEW`
When SAM CrewAI `crew_crisis` detects a sanctions-related crisis event, immediately scan all recent transactions involving the affected country or entity. Generate priority alerts bypassing normal threshold scoring.

### FR-NA-09: Semantic View for Agent -- `NEW`
Create semantic view enabling queries: "Are there any news events related to today's anomaly alerts?", "Show me transactions correlated with recent sanctions news", "Which flagged transactions have supporting crisis context?".

### FR-NA-10: Audit Trail -- `NEW`
Log all news-enriched investigation actions: analyst, alert_id, news articles reviewed, severity adjustment, decision (escalate/dismiss), and rationale. Required for FIU examination.

## Snowflake Features

- Cross-database views (AAA_DEV_SYNTHETIC_BANK + SAM_DEMO)
- Cortex Search Service for semantic news retrieval
- Dynamic table for automated correlation
- Semantic view for Cortex Agent integration

## Required Data Sources

| Source | Database | Schema | Object | Status | Purpose |
|--------|----------|--------|--------|--------|---------|
| Transaction anomalies | AAA_DEV_SYNTHETIC_BANK | PAY_AGG_V001 | PAYA_AGG_DT_TRANSACTION_ANOMALIES | EXISTS | Flagged suspicious transactions |
| High-risk patterns | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_HIGH_RISK_PATTERNS | EXISTS | Suspicious activity patterns |
| SWIFT pacs.008 | AAA_DEV_SYNTHETIC_BANK | PAY_AGG_V001 | ICGA_AGG_DT_SWIFT_PACS008 | EXISTS | Payment counterparty details |
| Customer 360 | AAA_DEV_SYNTHETIC_BANK | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360 | EXISTS | Customer risk context |
| PEP data | AAA_DEV_SYNTHETIC_BANK | CRM_RAW_V001 | CRMI_RAW_TB_EXPOSED_PERSON | EXISTS | PEP status for escalation |
| Anomaly analysis | AAA_DEV_SYNTHETIC_BANK | REP_AGG_V001 | REPP_AGG_DT_ANOMALY_ANALYSIS | EXISTS | AML anomaly details |
| News events enriched | SAM_DEMO | RAW | NEWS_RAW_TB_EVENTS_ENRICHED | EXISTS | Enriched news with entities |
| Event triggers | SAM_DEMO | RAW | NEWS_RAW_TB_EVENT_TRIGGERS | EXISTS | Materiality-scored triggers |
| Issuer-ticker map | SAM_DEMO | RAW | NEWS_RAW_TB_ISSUER_TICKER_MAP | EXISTS | Company name resolution |
| Sector taxonomy | SAM_DEMO | RAW | NEWS_RAW_TB_SECTOR_TAXONOMY | EXISTS | Sector alias matching |
| Country alias map | SAM_DEMO | RAW | NEWS_RAW_TB_COUNTRY_ALIAS_MAP | EXISTS | Country name resolution |
| Knowledge search | SAM_DEMO | AI | KNOWLEDGE_SEARCH_SVC | EXISTS | Cortex Search for semantic news retrieval |
| Crisis event log | SAM_DEMO | RAW | NEWS_RAW_TB_IC_EVENT_LOG | EXISTS | Crisis detection output |

## Acceptance Criteria

- Every anomaly alert enriched with top 5 correlated news articles (if any exist)
- Entity matching resolves counterparty names against news entities with > 80% accuracy
- Sanctions-related crisis events trigger fast-track alerts within 1 hour
- Investigation workflow accessible in AML Monitoring Streamlit tab
- Audit trail captures all analyst decisions with supporting evidence
