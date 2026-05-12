# Plan: Add Business Value Section to All Specs

Add a new `## Business Value` section after the Executive Summary in each of the 6 new business requirement documents. Each section contains two parts:

1. **Value Add** -- concrete business benefits (revenue, cost, risk reduction, time savings)
2. **Risk of Inaction** -- what happens if we don't implement this

---

### 1. [CASHFLOW_FORECASTING_BUSINESS_REQUIREMENTS.md](business_requirements/CASHFLOW_FORECASTING_BUSINESS_REQUIREMENTS.md)

**Value Add:**
- Reduce overdraft losses by proactively contacting clients before balances breach thresholds
- Treasury can optimize liquidity buffers (lower cost of carry on excess reserves)
- Advisors deliver proactive service: "we noticed your account may need attention" instead of reactive calls
- Regulatory benefit: demonstrates forward-looking risk management (BCBS 239 Principle 5: Timeliness)

**Risk of Inaction:**
- Overdraft losses continue as reactive discovery only
- Treasury maintains unnecessarily large liquidity buffers (capital inefficiency)
- Client satisfaction drops when problems are discovered after the fact
- Competitors with predictive capabilities win advisory mandates

---

### 2. [COMPLIANCE_DOCUMENT_SEARCH_BUSINESS_REQUIREMENTS.md](business_requirements/COMPLIANCE_DOCUMENT_SEARCH_BUSINESS_REQUIREMENTS.md)

**Value Add:**
- Reduce investigation time from hours to minutes: compliance officers search natural language instead of writing SQL joins across SWIFT, loan, and customer tables
- Audit response time drops from days to hours for regulatory data requests
- Cross-document pattern detection: find connections between SWIFT payments, loan applications, and PEP status that manual review misses
- Demonstrates Cortex Search as a differentiator for unstructured banking data

**Risk of Inaction:**
- Investigations remain SQL-dependent, limiting who can perform them
- Regulatory data requests take days, increasing examination friction and potential findings
- Hidden patterns across document types go undetected (AML exposure)
- SWIFT and loan data remain siloed despite being in the same platform

---

### 3. [CREDIT_SCORING_ML_BUSINESS_REQUIREMENTS.md](business_requirements/CREDIT_SCORING_ML_BUSINESS_REQUIREMENTS.md)

**Value Add:**
- Improved discriminatory power vs. rule-based IRB: ML models capture non-linear relationships between customer behavior and default risk
- Capital optimization: customers rated too conservatively by IRB can be reclassified, freeing regulatory capital
- Faster credit decisions: automated scoring replaces manual review for standard cases
- Model governance: Snowflake Model Registry provides audit trail required by FINMA for model risk management

**Risk of Inaction:**
- Rule-based IRB continues to misclassify customers in both directions (over-conservative = capital waste, too lenient = hidden risk)
- No ability to demonstrate ML-based credit risk to regulators (competitive disadvantage vs. peers adopting AI)
- Manual credit review remains the bottleneck for lending operations
- No model versioning or governance framework for future ML initiatives

---

### 4. [KYC_DOCUMENT_AI_BUSINESS_REQUIREMENTS.md](business_requirements/KYC_DOCUMENT_AI_BUSINESS_REQUIREMENTS.md)

**Value Add:**
- Reduce KYC processing time from 45 minutes to under 2 minutes per customer (97% reduction)
- Automated completeness validation catches missing documents before onboarding completes
- Multi-jurisdiction support (CH/UK/EU) ensures regulatory compliance without manual per-country checklists
- Expired document detection prevents compliance gaps from aging documentation

**Risk of Inaction:**
- KYC remains the #1 onboarding bottleneck (average 45 min/customer)
- Manual review introduces human error in document validation
- Expired documents go undetected until the next periodic review (12-month cycle)
- Scaling to more customers requires proportional headcount increase in compliance

---

### 5. [REGULATORY_ANALYST_BUSINESS_REQUIREMENTS.md](business_requirements/REGULATORY_ANALYST_BUSINESS_REQUIREMENTS.md)

**Value Add:**
- Board members and regulators can self-serve risk data without depending on data teams
- FINMA examination preparation drops from weeks to hours: pre-built VQRs cover the most common regulatory questions
- Cross-domain queries (LCR + IRB + FRTB + BCBS 239) that previously required custom SQL are answered in natural language
- Demonstrates AI-powered regulatory reporting as a governance showcase

**Risk of Inaction:**
- Every regulatory question requires a data analyst to write SQL (bottleneck, cost, delay)
- Board members receive static reports instead of interactive exploration
- Cross-domain risk questions remain unanswered or require multi-day turnaround
- No natural language access to the regulatory data already in the platform

---

### 6. [STREAMLIT_IN_SNOWFLAKE_BUSINESS_REQUIREMENTS.md](business_requirements/STREAMLIT_IN_SNOWFLAKE_BUSINESS_REQUIREMENTS.md) (Phase 3)

**Value Add:**
- Zero-install demo: share a Snowsight URL instead of asking users to clone a repo and set up Python
- Centralized access control via Snowflake RBAC (no local credential management)
- Production-ready deployment path for the banking dashboard
- Demonstrates Streamlit in Snowflake as an enterprise application platform

**Risk of Inaction:**
- Every demo requires local Python setup (friction, support overhead)
- Credentials stored in local secrets.toml files (security risk)
- App cannot be shared with prospects or regulators without local setup
- Dashboard remains a prototype rather than a deployable product
