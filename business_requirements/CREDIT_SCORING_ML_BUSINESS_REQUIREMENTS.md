# Business Requirements: Automated Credit Scoring (ML Classification)

## Executive Summary

ML-based credit risk classification that replaces or augments the existing rule-based IRB (Internal Ratings-Based) approach. Uses Snowflake-native classification to predict customer default probability from behavioral and financial attributes.

## Business Value

### Value Add

- Improved discriminatory power vs. rule-based IRB: ML models capture non-linear relationships between customer behavior and default risk
- Capital optimization: customers rated too conservatively by IRB can be reclassified, freeing regulatory capital
- Faster credit decisions: automated scoring replaces manual review for standard cases
- Model governance: Snowflake Model Registry provides audit trail required by FINMA for model risk management

### Risk of Inaction

- Rule-based IRB continues to misclassify customers in both directions (over-conservative = capital waste, too lenient = hidden risk)
- No ability to demonstrate ML-based credit risk to regulators (competitive disadvantage vs. peers adopting AI)
- Manual credit review remains the bottleneck for lending operations
- No model versioning or governance framework for future ML initiatives

## Business Context

Current state: Credit ratings are computed via rule-based logic in `REPP_AGG_DT_IRB_CUSTOMER_RATINGS` using static thresholds on anomaly flags, transaction volume, and account tier. This approach has limited discriminatory power and cannot adapt to changing customer behavior.

Target state: An ML classification model trained on customer attributes and transaction patterns that assigns a probability-of-default score. Results are compared side-by-side with the existing IRB ratings to demonstrate model lift and identify misclassified customers.

## Stakeholders

| Role | Interest |
|------|----------|
| Chief Risk Officer | Model validation, regulatory capital optimization |
| Credit Risk Team | Default probability scoring, portfolio segmentation |
| Lending Officers | Automated pre-approval decisioning |
| Internal Audit | Model governance, fairness, explainability |
| Regulators (FINMA) | Model documentation, backtesting evidence |

## Functional Requirements

### FR-CR-01: Feature Engineering View -- `NEW`
Build a training view joining Customer 360 (demographics, account tier, onboarding date), transaction summary (avg amount, frequency, currency mix), transaction anomalies (anomaly scores), account balances, portfolio performance, equity positions, lifecycle stage, and PEP status. One row per customer.

### FR-CR-02: Model Training -- `NEW`
Train a `SNOWFLAKE.ML.CLASSIFICATION` model predicting binary default risk (high-risk vs. standard) using the feature view. Split data 80/20 train/test with stratified sampling.

### FR-CR-03: Per-Customer Probability Scoring -- `NEW`
Output a probability-of-default score (0.0 to 1.0) for each customer. Include the top 5 contributing features per prediction for explainability.

### FR-CR-04: IRB Comparison Analysis -- `NEW`
Join ML predictions against existing IRB ratings from `REPP_AGG_DT_IRB_CUSTOMER_RATINGS`. Classify each customer into: AGREE (both methods same), OVER_CONSERVATIVE (IRB high-risk, ML low-risk), UNDER_CONSERVATIVE (IRB low-risk, ML high-risk).

### FR-CR-05: Segmented Results -- `NEW`
Segment comparison results by country, account tier, and risk classification. Compute agreement rate, over-conservative rate, and under-conservative rate per segment.

### FR-CR-06: Model Performance Metrics -- `NEW`
Compute and store: AUC-ROC, precision, recall, F1-score, confusion matrix, and feature importance rankings. Backtest against historical outcomes.

### FR-CR-07: Daily Prediction Refresh -- `NEW`
Implement scoring as a dynamic table that refreshes daily when upstream data changes. New customers and updated transaction patterns must be scored automatically.

### FR-CR-08: Model Registry -- `NEW`
Register the trained model in Snowflake Model Registry with version metadata, training date, dataset size, performance metrics, and feature list. Support model comparison across versions.

### FR-CR-09: Notebook Visualization -- `NEW`
Create an interactive notebook (`notebooks/credit_scoring_ml.ipynb`) displaying: ROC curve, feature importance chart, IRB vs ML comparison matrix, and per-segment analysis.

### FR-CR-10: Streamlit Integration -- `NEW`
Add credit scoring results to the existing "Lending Operations" Streamlit tab. Show: ML score vs IRB rating side-by-side, misclassification highlights, and model performance summary.

## Snowflake Features

- `SNOWFLAKE.ML.CLASSIFICATION` for model training and inference
- Dynamic table for automated scoring refresh
- Snowflake Model Registry for versioning and governance

## Required Data Sources

| Source | Schema | Object | Status | Purpose |
|--------|--------|--------|--------|---------|
| Customer 360 | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_360 | EXISTS | Demographics, account tier, risk flags, onboarding date |
| Transaction summary | PAY_AGG_V001 | PAYA_AGG_DT_CUSTOMER_TRANSACTION_SUMMARY | EXISTS | Avg amount, frequency, currency mix |
| Transaction anomalies | PAY_AGG_V001 | PAYA_AGG_DT_TRANSACTION_ANOMALIES | EXISTS | Anomaly scores, velocity flags |
| Account balances | PAY_AGG_V001 | PAYA_AGG_DT_ACCOUNT_BALANCES | EXISTS | Current balance, trend indicators |
| IRB ratings (existing) | REP_AGG_V001 | REPP_AGG_DT_IRB_CUSTOMER_RATINGS | EXISTS | Baseline for comparison |
| Portfolio positions | REP_AGG_V001 | REPP_AGG_DT_PORTFOLIO_PERFORMANCE | EXISTS | AUM, asset allocation, returns |
| Equity positions | EQT_AGG_V001 | EQTA_AGG_DT_PORTFOLIO_POSITIONS | EXISTS | Trading activity, concentration |
| Customer lifecycle | CRM_AGG_V001 | CRMA_AGG_DT_CUSTOMER_LIFECYCLE | EXISTS | Lifecycle stage, churn indicators |
| PEP/Sanctions | CRM_RAW_V001 | CRMI_RAW_TB_EXPOSED_PERSON | EXISTS | Politically exposed person flag |

## Acceptance Criteria

- Model trained on minimum 3,000 customers with 6+ months of history
- AUC score above 0.75 on holdout test set
- Feature importance analysis identifies top 10 predictive features
- Side-by-side comparison dashboard: ML score vs. IRB rating
- Misclassification report highlighting customers where ML and IRB disagree
- Model registered in Snowflake Model Registry with version metadata
- Predictions refreshed daily via dynamic table
