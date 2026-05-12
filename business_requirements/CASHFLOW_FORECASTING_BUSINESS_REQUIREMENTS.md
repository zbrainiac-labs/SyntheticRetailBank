# Business Requirements: Cash Flow Forecasting (ML Forecast)

## Executive Summary

Proactive cash flow forecasting to predict account balance trajectories 30/60/90 days forward. Enables overdraft prevention, treasury liquidity planning, and personalized client advisory for wealth management.

## Business Value

### Value Add

- Reduce overdraft losses by proactively contacting clients before balances breach thresholds
- Treasury can optimize liquidity buffers (lower cost of carry on excess reserves)
- Advisors deliver proactive service ("we noticed your account may need attention") instead of reactive calls
- Regulatory benefit: demonstrates forward-looking risk management (BCBS 239 Principle 5: Timeliness)

### Risk of Inaction

- Overdraft losses continue as reactive discovery only
- Treasury maintains unnecessarily large liquidity buffers (capital inefficiency)
- Client satisfaction drops when problems are discovered after the fact
- Competitors with predictive capabilities win advisory mandates

## Business Context

Current state: Account balances are computed reactively from completed transactions. Advisors discover problematic balances only after they occur.

Target state: ML-driven daily forecasts per account predicting future balance trends, with automated alerts when accounts are projected to breach thresholds (negative balance, minimum required balance, dormancy risk).

## Stakeholders

| Role | Interest |
|------|----------|
| Treasury | Aggregate cash flow projections for liquidity planning |
| Client Advisors | Early warning for at-risk client accounts |
| Risk Management | Counterparty exposure based on projected balances |
| Operations | Overdraft prevention, proactive client outreach |

## Functional Requirements

### FR-CF-01: Time-Series Model Training -- `NEW`
Train a `SNOWFLAKE.ML.FORECAST` model per account using minimum 6 months of historical daily transaction data. Group transactions by account and date, computing daily net cash flow (credits minus debits).

### FR-CF-02: Multi-Horizon Forecast Generation -- `NEW`
Generate rolling balance projections at 30-day, 60-day, and 90-day horizons. Each prediction must include point estimate, upper bound (95th percentile), and lower bound (5th percentile).

### FR-CF-03: Threshold Breach Alerts -- `NEW`
Flag accounts where the forecasted balance is projected to drop below zero (overdraft) or below a configurable minimum balance threshold within 30 days. Output: account_id, breach_date, projected_balance, threshold, severity (WARNING/CRITICAL).

### FR-CF-04: Daily Automated Refresh -- `NEW`
Implement as a dynamic table pipeline that re-trains and re-scores daily. New transactions from `PAYI_RAW_TB_TRANSACTIONS` must be reflected in the next forecast cycle.

### FR-CF-05: Currency Normalization -- `NEW`
Convert all forecasts to CHF base currency using `REFA_AGG_DT_FX_RATES_ENHANCED` for consistent treasury aggregation across multi-currency accounts.

### FR-CF-06: Treasury Aggregation View -- `NEW`
Aggregate individual account forecasts into summary views by: currency, country, account tier, and customer risk classification. Provide total projected inflows/outflows per aggregation dimension.

### FR-CF-07: Backtest Metrics -- `NEW`
Compare forecast predictions against actual balances for completed periods. Compute MAPE, RMSE, and directional accuracy. Store backtest results for model governance.

### FR-CF-08: Cortex Agent Integration -- `NEW`
Create a semantic view over forecast results enabling natural language queries such as "which accounts are projected to go negative in the next 30 days?" via the existing Cortex Agent infrastructure.

### FR-CF-09: Notebook Visualization -- `NEW`
Create an interactive notebook (`notebooks/cashflow_forecasting.ipynb`) displaying forecast curves with confidence bands, breach alerts, and backtest accuracy metrics.

### FR-CF-10: Streamlit Tab -- `NEW`
Add a "Cash Flow Forecast" tab to the Streamlit banking dashboard with interactive horizon selector (30/60/90 days) and drill-down from treasury summary to individual account forecasts.

## Snowflake Features

- `SNOWFLAKE.ML.FORECAST` for time-series prediction
- Dynamic table for automated daily refresh
- Semantic view for Cortex Agent integration

## Required Data Sources

| Source | Schema | Object | Status | Purpose |
|--------|--------|--------|--------|---------|
| Payment transactions | PAY_RAW_V001 | PAYI_RAW_TB_TRANSACTIONS | EXISTS | Historical transaction amounts, dates, currencies |
| Account balances | PAY_AGG_V001 | PAYA_AGG_DT_ACCOUNT_BALANCES | EXISTS | Current running balances per account |
| Account master | CRM_RAW_V001 | ACCI_RAW_TB_ACCOUNTS | EXISTS | Account type, currency, status |
| Customer master | CRM_RAW_V001 | CRMI_RAW_TB_CUSTOMER | EXISTS | Customer tier, risk classification |
| FX rates | REF_AGG_V001 | REFA_AGG_DT_FX_RATES_ENHANCED | EXISTS | Currency conversion for CHF-normalized forecasts |
| Customer 360 | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360 | EXISTS | Enriched customer context for segmented forecasting |

## Acceptance Criteria

- Forecast model trained on minimum 6 months of transaction history
- Daily predictions for all active accounts (status = ACTIVE)
- MAPE (Mean Absolute Percentage Error) below 15% on 30-day horizon
- Alerts generated for accounts projected to go negative within 30 days
- Forecast results accessible via Cortex Agent natural language queries
