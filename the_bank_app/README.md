# Synthetic Retail Bank - Streamlit Prototype

> **Comprehensive customer intelligence dashboard for AAA Synthetic Bank**
>
> Provides 360° customer views, risk assessment, compliance monitoring, and AI-powered natural language queries.

---

## Features

### **16 Integrated Dashboards**

1. **Customer 360° Search** - Find and view complete customer profiles with all 48 attributes
2. **Risk & Compliance Dashboard** - Executive view of risk distribution, PEP/sanctions screening
3. **Portfolio Analytics** - Account tier distribution, geographic analysis, multi-currency portfolios
4. **Fraud & Anomaly Detection** - Identify suspicious patterns and anomalous behavior
5. **Churn & Lifecycle Management** - Churn prediction and dormant account reactivation
6. **AML & Transaction Monitoring** - Transaction monitoring alerts, SAR/STR filings, backlog management
7. **Lending & Credit Operations** - Loan portfolio analysis, credit risk assessment, NPL tracking
8. **Wealth Management** - Portfolio performance, advisor metrics, AUM tracking, client segmentation
9. **Sanctions & Embargo Control** - Sanctions screening effectiveness, breach detection, list management
10. **Advisor & Employee Management** - Advisor capacity, performance tracking, workload distribution
11. **KYC & Customer Screening** - PEP identification, KYC completeness, screening audit trails
12. **Data Quality & Controls** - Data completeness metrics, control effectiveness, quality monitoring
13. **LCR Monitoring** - FINMA Liquidity Coverage Ratio reporting, real-time liquidity risk monitoring, regulatory compliance
14. **Loans Portfolio** - Retail loans & mortgages portfolio analysis, LTV/DTI monitoring, affordability assessment, compliance screening
15. **Ask AI** - Natural language queries powered by Snowflake Cortex AI Agent
16. **Settings** - Data refresh, preferences, connection status

### **Key Capabilities**

- **Real-Time Data** - 1-hour cache with manual refresh option
- **360° Customer View** - All 48 customer attributes in one click
- **Risk-First Design** - Compliance dashboard as default landing
- **Natural Language Query** - Ask questions in plain English via Cortex AI
- **Export Everywhere** - CSV downloads on every tab
- **Color-Coded Alerts** - Red/Orange/Yellow/Green risk indicators
- **Responsive Design** - Works on desktop, tablet, mobile

---

## Prerequisites

### Required Software

- **Python 3.10+**
- **Snowflake Account** with access to:
 - Database: `AAA_DEV_SYNTHETIC_BANK`
 - Schema: `CRM_AGG_001`
 - Table: `CRMA_AGG_DT_CUSTOMER_360`
 - Agent: `SNOWFLAKE_INTELLIGENCE.AGENTS.CRM_INTELLIGENCE_AGENT` (optional, for AI queries)

### Required Snowflake Objects

Ensure these are deployed before running the app:

```sql
-- Database and schema
AAA_DEV_SYNTHETIC_BANK.CRM_AGG_001 -- Required (contains CRM, Employee, Account data)
AAA_DEV_SYNTHETIC_BANK.PAY_AGG_001 -- Optional (for AML monitoring)

-- Customer & CRM tables (in CRM_AGG_001)
CRMA_AGG_DT_CUSTOMER_360 -- Customer 360° view (required)
CRMA_AGG_DT_CUSTOMER_CURRENT -- Current customer attributes
CRMA_AGG_DT_CUSTOMER_HISTORY -- Historical customer changes
CRMA_AGG_DT_CUSTOMER_LIFECYCLE -- Lifecycle analytics (required for tab 5)

-- Payment & Transaction tables (in PAY_AGG_001)
PAYA_AGG_DT_TRANSACTION_ANOMALIES -- AML alerts and transaction monitoring
PAYA_AGG_DT_ACCOUNT_BALANCES -- Account balance tracking

-- Account tables (in CRM_AGG_001)
ACCA_AGG_DT_ACCOUNTS -- Account master data

-- Employee & Advisor tables (in CRM_AGG_001, optional for Wealth Management)
EMPA_AGG_DT_ADVISOR_PERFORMANCE -- Advisor KPIs and performance (tab 8, 10)
EMPA_AGG_DT_PORTFOLIO_BY_ADVISOR -- Portfolio valuations by advisor
EMPA_AGG_DT_TEAM_LEADER_DASHBOARD -- Team-level metrics
EMPA_AGG_VW_WORKLOAD_DISTRIBUTION -- Workload analysis
EMPA_AGG_VW_CURRENT_ASSIGNMENTS -- Client-advisor relationships

-- Semantic views (optional, for AI)
CRMA_SV_CUSTOMER_360 -- Customer 360 semantic view for Cortex AI
CRMA_AGG_VW_CUSTOMER_RISK_PROFILE -- Risk metrics view

-- LCR (Liquidity Coverage Ratio) tables (in REP_AGG_V001, for LCR Monitoring tab)
REPP_AGG_DT_LCR_DAILY -- Daily LCR calculations
REPP_AGG_DT_LCR_TREND -- 90-day rolling trend analysis
REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL -- HQLA holdings breakdown
REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL -- Deposit outflows detail
REPP_AGG_VW_LCR_ALERTS -- Active LCR alerts
REPP_AGG_VW_LCR_MONTHLY_SUMMARY -- Monthly compliance summary
REPP_AGG_VW_LCR_MONITORING -- Consolidated LCR monitoring view

-- Loan Portfolio tables (in REP_AGG_V001, for Loans Portfolio tab)
LOAR_AGG_DT_PORTFOLIO_SUMMARY -- Portfolio summary by country/product
LOAR_AGG_DT_LTV_DISTRIBUTION -- LTV ratio distribution analysis
LOAR_AGG_DT_APPLICATION_FUNNEL -- Application status funnel
LOAR_AGG_DT_AFFORDABILITY_SUMMARY -- Affordability assessment metrics
LOAR_AGG_DT_CUSTOMER_LOAN_SUMMARY -- Customer-level loan summary
LOAR_AGG_VW_PORTFOLIO_CURRENT -- Current portfolio view
LOAR_AGG_VW_LTV_DISTRIBUTION -- LTV distribution view
LOAR_AGG_VW_APPLICATION_FUNNEL -- Application funnel view
LOAR_AGG_VW_AFFORDABILITY_ANALYSIS -- Affordability analysis view
LOAR_AGG_VW_COMPLIANCE_SCREENING -- Compliance screening results

-- Cortex AI Agent (optional, for Ask AI tab)
AAA_DEV_SYNTHETIC_BANK.CRM_AGG_001.CRM_CUSTOMER_360 -- AI Agent
```

**Note on AI Agent**: The app uses **REST API** to call the Snowflake Cortex AI Agent instead of SQL functions (`SNOWFLAKE.CORTEX.COMPLETE_AGENT`). This is more reliable and works with standard Snowflake authentication.

---

## Installation

### 1. Clone/Navigate to the App Directory

```bash
cd /Users/mdaeppen/workspace/AAA_synthetic_bank/the_bank_app
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Snowflake Connection

Create `.streamlit/secrets.toml` from the example:

```bash
cp.streamlit/secrets.toml.example.streamlit/secrets.toml
```

Edit `.streamlit/secrets.toml` with your Snowflake credentials:

```toml
[snowflake]
account = "sfseeurope-mdaeppen" # Your Snowflake account
user = "your-username"
password = "your-password"
warehouse = "your-warehouse"
database = "AAA_DEV_SYNTHETIC_BANK"
schema = "CRM_AGG_001"
role = "ACCOUNTADMIN"
```

** Important:** Add `secrets.toml` to `.gitignore` to prevent committing credentials!

---

## Running the App

### Local Development

```bash
streamlit run app.py
```

The app will open in your browser at: `http://localhost:8501`

### Production Deployment

#### Option 1: Streamlit Cloud

1. Push code to GitHub (without `secrets.toml`)
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub repository
4. Add secrets via Streamlit Cloud UI (Settings → Secrets)

#### Option 2: Snowflake Native App

Deploy as a Snowflake Native App for seamless integration.

#### Option 3: Docker

```bash
# Build image
docker build -t crm-intelligence-app.

# Run container
docker run -p 8501:8501 \
 -v $(pwd)/.streamlit/secrets.toml:/app/.streamlit/secrets.toml \
 crm-intelligence-app
```

---

## Usage Guide

### Tab 1: Customer 360° Search

**Purpose:** Find customers and view complete profiles

**How to use:**
1. Enter search criteria (name, country, tier, risk level)
2. Apply quick filters (high-risk, PEP matches, anomalies)
3. Click "Search" to see results
4. Select a customer to view detailed 360° profile

**Best for:**
- Relationship managers preparing for customer calls
- Compliance officers investigating specific customers
- Customer service representatives needing full context

### Tab 2: Risk & Compliance Dashboard

**Purpose:** Executive view of compliance posture and risk exposure

**How to use:**
1. Review key metrics at top (critical/high risk, PEP/sanctions)
2. Examine risk distribution pie chart
3. Check PEP and sanctions screening breakdown
4. Review high-risk customers requiring action
5. Export compliance reports (CSV)

**Best for:**
- Chief Compliance Officers preparing board presentations
- AML investigators prioritizing case reviews
- Risk analysts monitoring portfolio risk

### Tab 3: Portfolio Analytics

**Purpose:** Understand customer segmentation and account holdings

**How to use:**
1. Review portfolio metrics (avg accounts, multi-currency customers)
2. Analyze account tier distribution chart
3. Examine geographic distribution
4. Review account holdings by tier table

**Best for:**
- CFO evaluating market expansion
- Marketing teams planning campaigns
- Relationship managers identifying cross-sell opportunities

### Tab 4: Fraud & Anomaly Detection

**Purpose:** Identify suspicious patterns and anomalous behavior

**How to use:**
1. Check anomaly metrics (total anomalies, high-risk + anomaly)
2. Review anomaly priority queue (sorted by risk score)
3. Export AML investigation reports

**Best for:**
- AML investigators identifying fraud patterns
- Fraud analysts prioritizing cases
- Compliance officers monitoring suspicious activity

### Tab 5: Churn & Lifecycle Management

**Status:** ✅ Active

**Purpose:** Predict and prevent customer churn through lifecycle analysis

**How to use:**
1. Review lifecycle stage distribution (NEW, ACTIVE, MATURE, DECLINING, DORMANT, CHURNED)
2. Monitor churn probability metrics across customer base
3. Identify premium customers at risk (GOLD/PLATINUM with >70% churn)
4. Track dormant accounts requiring reactivation (>180 days inactive)
5. Calculate total revenue at risk from potential churn
6. Export at-risk customer lists for retention campaigns

**Features:**
- Lifecycle stage distribution pie chart
- Churn probability histogram with risk thresholds
- Revenue analysis by lifecycle stage
- Premium customers at-risk dashboard
- Dormant account reactivation tracker
- Revenue at risk gauge and calculator
- Days inactive distribution analysis
- Action recommendations for each segment

**Best for:**
- Head of Customer Success planning retention strategies
- Marketing Teams designing reactivation campaigns
- Account Managers prioritizing outreach
- CFO tracking revenue at risk
- Customer Experience Teams improving lifecycle transitions

### Tab 6: AML & Transaction Monitoring

**Purpose:** Monitor transaction alerts and AML compliance

**How to use:**
1. Review key AML metrics (alerts, anomalies, unique customers)
2. Analyze alert trends over time
3. Investigate recent alerts and anomalous transactions
4. Export AML reports for regulatory submissions

**Best for:**
- Chief Compliance Officer monitoring AML program effectiveness
- Transaction Monitoring Teams investigating alerts
- Financial Intelligence Unit (FIU) responding to regulatory inquiries
- AML analysts tracking SAR/STR filing metrics

### Tab 7: Lending & Credit Operations

**Purpose:** Monitor lending portfolio and credit risk

**How to use:**
1. Review lending portfolio overview (total customers, credit scores)
2. Analyze credit risk distribution
3. Track risk classification by customer segment
4. Monitor account tier distribution for lending customers

**Best for:**
- Head of Lending tracking portfolio quality
- Credit Officers assessing credit risk exposure
- Loan Operations managing application pipelines
- Risk Management monitoring NPL ratios

### Tab 8: Wealth Management

**Purpose:** Track advisor performance and portfolio management

**How to use:**
1. Review total AUM and client distribution
2. Analyze top advisors by Assets Under Management
3. Monitor advisor capacity and workload
4. Track performance ratings and regional distribution

**Best for:**
- Wealth Advisors reviewing client portfolios
- Head of Private Banking monitoring team performance
- Relationship Managers identifying cross-sell opportunities
- Portfolio Managers tracking investment performance

### Tab 9: Sanctions & Embargo Control

**Purpose:** Monitor sanctions screening and compliance

**How to use:**
1. Review sanctions match metrics (exact matches, reviews needed)
2. Analyze sanctions screening results by match type
3. Monitor risk level distribution
4. Export sanctions reports for regulatory submissions

**Best for:**
- Chief Compliance Officer ensuring zero sanctions breaches
- Sanctions Officers investigating matches
- Trade Compliance teams validating controls
- Legal & Regulatory Affairs responding to inquiries

### Tab 10: Advisor & Employee Management

**Purpose:** Manage advisor capacity and workload distribution

**How to use:**
1. Review advisor capacity metrics (available, at capacity)
2. Identify advisors available for new client assignments
3. Monitor workload distribution across regions
4. Track team performance metrics

**Best for:**
- Head of Wealth Management assigning new clients
- COO planning resource allocation
- Team Leaders monitoring advisor workload
- HR planning hiring and capacity needs

### Tab 11: KYC & Customer Screening

**Purpose:** Monitor KYC completeness and PEP screening

**How to use:**
1. Review PEP match metrics and screening results
2. Analyze KYC data completeness by country
3. Identify customers requiring PEP review
4. Track screening accuracy and risk levels

**Best for:**
- Head of AML/KYC ensuring screening effectiveness
- Compliance Officers investigating PEP matches
- KYC Teams managing document collection
- Internal Audit validating screening controls

### Tab 12: Data Quality & Controls

**Purpose:** Monitor data quality and control effectiveness

**How to use:**
1. Review data completeness metrics (email, phone, DOB)
2. Analyze missing data by field
3. Monitor quality against defined thresholds
4. Track data quality trends over time

**Best for:**
- Data Governance Officer monitoring quality standards
- Chief Compliance Officer ensuring data integrity
- Internal Audit validating control effectiveness
- IT Teams prioritizing data remediation

### Tab 13: Compliance Risk Management

**Purpose:** Monitor overall compliance risk profile

**How to use:**
1. Review comprehensive compliance risk metrics
2. Analyze risk distribution and geographic concentration
3. Track action items (PEP reviews, sanctions reviews, anomalies)
4. Monitor against risk appetite thresholds

**Best for:**
- Chief Risk Officer monitoring enterprise risk
- Board of Directors reviewing compliance posture
- CCO preparing board presentations
- Regulatory Affairs responding to supervisory inquiries

### Tab 14: Ask AI

**Purpose:** Natural language query interface powered by Snowflake Cortex AI

**How to use:**
1. Review example queries for inspiration
2. Type your question in plain English
3. Click "Ask" to get AI-powered response
4. View results (text answer + data table if applicable)
5. Export results if needed

**Example queries:**
- "Show me all PLATINUM customers in Switzerland"
- "Which customers require PEP review?"
- "Find high-risk customers with anomalous transactions"
- "What is our customer distribution by account tier?"

**Best for:**
- Executives needing instant answers
- Business analysts without SQL knowledge
- Ad-hoc investigation and exploration

### Tab 15: Settings

**Purpose:** Data refresh, preferences, connection status

**Features:**
- Manual data refresh (clears 1-hour cache)
- Display preferences (rows per page, date format, currency)
- Snowflake connection status and testing

---

## UI/UX Design

### Design System (Apple-inspired)

- **Theme:** `.streamlit/config.toml` - light and dark palettes (blue `#007AFF` / `#0A84FF`, greys `#F5F5F7` / `#1C1C1E`), Inter font with system fallback, 12px radius, pill buttons, Apple chart palette. No custom CSS.
- **Charts:** `utils/ui_helpers.py` registers the Plotly template `apple` (transparent background, hairline grid) as default; imported in `app.py`.
- **Helpers (`utils/ui_helpers.py`):** `page_header(title, subtitle)`, `section`, `kpi_row` (bordered metric cards, short labels with unit, e.g. `HQLA (CHF)` / `721M`), `status_badge`, `empty_state`, `STATUS_COLORS` (PASS `#34C759`, WARNING `#FF9F0A`, FAIL `#FF3B30`).
- **Regression test:** `venv/bin/python tests/test_all_pages.py` renders every page headless; `tests/test_lcr_page.py` checks the LCR page (tabs, KPIs, simulator, breach banner).

### Risk Indicators

| Risk Level | Color | Icon | Action Required |
|------------|-------|------|-----------------|
| CRITICAL | Red | | Immediate action, senior officer review |
| HIGH | Orange | | 4-hour SLA, detailed investigation |
| MEDIUM | Yellow | | 24-hour SLA, standard review |
| LOW | Green | | Automated approval with monitoring |
| NO_RISK | Blue | | Straight-through processing |

---

## Security Considerations

### Data Protection

- **Secrets Management**: Never commit `secrets.toml` to version control
- **Access Control**: Use least-privilege Snowflake roles
- **PII Handling**: Consider masking sensitive fields (email, phone) in exports
- **Audit Logging**: Enable Snowflake query history for compliance

### Best Practices

1. **Use separate credentials** for development vs. production
2. **Rotate passwords** regularly
3. **Enable MFA** on Snowflake accounts
4. **Restrict warehouse size** to control costs
5. **Monitor query execution** for performance and security

---

## Technical Details

### AI Agent Implementation (REST API)

The **Ask AI** tab uses Snowflake Cortex AI Agent via **REST API** instead of SQL functions. This approach is more reliable and avoids the `SNOWFLAKE.CORTEX.COMPLETE_AGENT` function error.

**Architecture**:
```
User Question
  ↓
Streamlit App (app.py)
  ↓
Agent Caller (utils/agent_caller.py)
  ↓
Snowflake REST API
  ↓
POST /api/v2/databases/{db}/schemas/{schema}/agents/{agent}:run
  ↓
AI Agent (CRM_CUSTOMER_360)
  ↓
Server-Sent Events (SSE) Stream
  ↓
Response Text Extraction
  ↓
Display to User
```

**Key Files**:
- `utils/agent_caller.py` - REST API client for agent invocation
- `app.py` (Tab 13) - UI for AI queries with fallback search

**How it works**:
1. Gets REST API token from Snowflake session
2. Calls `/api/v2/databases/.../agents/{agent}:run` endpoint
3. Handles Server-Sent Events (SSE) streaming response
4. Parses event stream to extract text responses
5. Falls back to keyword search if agent fails

**Benefits**:
- ✅ Works with standard Snowflake authentication
- ✅ Handles streaming responses correctly
- ✅ Better error handling and debugging
- ✅ Compatible with test_single_agent.py approach
- ✅ No SQL function dependency

**Agent Configuration**:
```python
agent_full_name = "AAA_DEV_SYNTHETIC_BANK.CRM_AGG_001.CRM_CUSTOMER_360"
timeout = 60  # seconds
```

**Testing**:
```bash
# Test agent via CLI
python test_agents/test_single_agent.py CRM_CUSTOMER_360 "Show me PLATINUM customers"
```

---

## Troubleshooting

### Connection Issues

**Problem:** "Failed to connect to Snowflake"

**Solutions:**
1. Verify credentials in `.streamlit/secrets.toml`
2. Check Snowflake account name format: `account.region.cloud` (e.g., `xy12345.eu-central-1.aws`)
3. Ensure warehouse is running
4. Test connection via Settings tab

### Data Loading Issues

**Problem:** "Error loading customer data"

**Solutions:**
1. Verify table exists: `CRMA_AGG_DT_CUSTOMER_360`
2. Check user has SELECT privileges
3. Ensure database/schema are correct
4. Test query in Snowsight UI first

### AI Query Issues

**Problem:** "AI query failed"

**Solutions:**
1. Verify Cortex AI Agent exists: `SNOWFLAKE_INTELLIGENCE.AGENTS.CRM_INTELLIGENCE_AGENT`
2. Ensure user has USAGE privilege on agent
3. Check semantic view is deployed: `CRMA_SV_CUSTOMER_360`
4. Fallback: Use manual search in Customer 360° tab

### Performance Issues

**Problem:** App is slow to load

**Solutions:**
1. Reduce `ttl` in cache decorators (default: 3600 seconds = 1 hour)
2. Increase Snowflake warehouse size
3. Add filters before loading large datasets
4. Check network latency to Snowflake region

---

## Performance Optimization

### Caching Strategy

The app uses Streamlit's caching to minimize Snowflake queries:

```python
@st.cache_data(ttl=3600) # Cache for 1 hour
def load_customer_360():
 #... data loading logic
```

**Tune caching:**
- Increase `ttl` (time-to-live) for less frequent refreshes
- Decrease `ttl` for more real-time data
- Use manual refresh button in Settings tab to clear cache

### Query Optimization

All data loading functions use optimized SQL:

1. **Select only needed columns** (avoid `SELECT *` in production)
2. **Filter at database level** (push filters to Snowflake)
3. **Use aggregations** (GROUP BY) for summary views
4. **Leverage dynamic tables** (CRMA_AGG_DT_* tables are pre-aggregated)

---

## Testing

### Manual Testing Checklist

- [ ] Connection test passes (Settings tab)
- [ ] Customer search returns results (Tab 1)
- [ ] Risk dashboard displays correctly (Tab 2)
- [ ] Portfolio charts render (Tab 3)
- [ ] Anomaly detection shows flagged customers (Tab 4)
- [ ] AI query returns response (Tab 6)
- [ ] Export CSV downloads successfully
- [ ] Responsive design works on mobile/tablet

### Test with Sample Queries

```python
# Test connection
streamlit run app.py

# Navigate to each tab and verify:
# 1. Data loads without errors
# 2. Visualizations render correctly
# 3. Filters work as expected
# 4. Export buttons function
```

---

## Monitoring

### Key Metrics to Track

1. **User Adoption**
 - Daily active users
 - Most popular tabs
 - Query frequency

2. **Performance**
 - Average page load time
 - Query execution time
 - Cache hit rate

3. **Data Quality**
 - Successful data loads
 - Error rates
 - Data freshness

### Logging

Enable Streamlit logging for debugging:

```bash
streamlit run app.py --logger.level=debug
```

---

## Roadmap

### Phase 1: Core Prototype (✅ Complete)
- [x] Tab 1: Customer 360° Search
- [x] Tab 2: Risk & Compliance Dashboard
- [x] Tab 3: Portfolio Analytics
- [x] Tab 4: Fraud Detection
- [x] Tab 5: Churn & Lifecycle Management
- [x] Tab 6: AML & Transaction Monitoring
- [x] Tab 7: Lending & Credit Operations
- [x] Tab 8: Wealth Management
- [x] Tab 9: Sanctions & Embargo Control
- [x] Tab 10: Advisor & Employee Management
- [x] Tab 11: KYC & Customer Screening
- [x] Tab 12: Data Quality & Controls
- [x] Tab 13: Compliance Risk Management
- [x] Tab 14: Ask AI (with fallback)
- [x] Tab 15: Settings

### Phase 2: Advanced Features (🔄 In Progress)
- [ ] User authentication and role-based access
- [ ] Enhanced AI query with conversation history
- [ ] PDF export for compliance reports
- [ ] Mobile app optimization
- [ ] Real-time alert notifications
- [ ] Advanced portfolio analytics (TWR, Sharpe ratio)

### Phase 3: Production Readiness ( Planned)
- [ ] Automated testing suite
- [ ] CI/CD pipeline
- [ ] Performance monitoring dashboard
- [ ] User analytics and feedback collection
- [ ] Multi-language support

---

## Support

### Documentation

- **Business Guide**: `docs/crm_business_guide.md`
- **Streamlit UX Plan**: `docs/crm_streamlit_ux_plan.md`
- **Snowflake SQL**: `structure/700_CRM_SEMANTIC_VIEW.sql`

### Getting Help

1. **Connection issues**: Check `.streamlit/secrets.toml` configuration
2. **Data issues**: Verify Snowflake objects are deployed
3. **AI issues**: Ensure Cortex AI Agent is created
4. **General questions**: Contact Data Platform team

---

## License

AAA Synthetic Bank - Internal Use Only

---

## Contributors

- **Data Platform Team** - Backend development
- **Business Intelligence Team** - Requirements and testing
- **Compliance Team** - Risk framework validation

---

**Built with using Streamlit + Snowflake**

_Synthetic Retail Bank v1.0.0 | 2025_

