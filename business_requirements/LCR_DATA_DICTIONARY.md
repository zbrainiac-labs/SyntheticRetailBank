# LCR Data Dictionary

Snapshot of the deployed Snowflake comments in `AAA_DEV_SYNTHETIC_BANK` taken on 2026-10-25. The source of truth is the `COMMENT`s in `sources/definitions/*.sql` and `post_deploy.sql`; this file is not regenerated automatically.

39 objects, 648 columns, 4 functions/procedures.

## Objects

| Schema | Object | Type | Rows | Description |
|---|---|---|---:|---|
| REP_RAW_V001 | [LIQI_RAW_TB_BEHAVIORAL_RULES](#liqi_raw_tb_behavioral_rules) | BASE TABLE | 104 | Scenario-specific behavioral and regulatory rule set for the LCR engine (spec 3.1, REQ-ENG-02/03/04/05): run-off, inflow and drawdown rates, stress haircut add-ons, decay profiles and simulator categories per product x counterparty sector x scenario. BASE rows implement FINMA LiqV; stress rows are illustrative internal stress assumptions. Seeded by post_deploy.sql. |
| REP_RAW_V001 | [LIQI_RAW_TB_CASHFLOWS](#liqi_raw_tb_cashflows) | BASE TABLE | 1,202,196 | Daily contractual cashflow schedules per contract (REQ-ING-02, REQ-ENG-01). Grain: one row per AS_OF_DATE x CONTRACT_ID x CF_DATE x CF_TYPE. Basis for the maturity ladder / liquidity gap and for 30-day LCR inflows and interbank funding outflows. Snapshot partitioning by AS_OF_DATE supports cut-off comparisons. Loaded hourly from cashflows_YYYYMMDD.csv via stage LIQI_RAW_ST_CASHFLOWS. |
| REP_RAW_V001 | [LIQI_RAW_TB_CONTRACTS](#liqi_raw_tb_contracts) | BASE TABLE | 233,370 | Daily snapshots of loans, credit / liquidity facilities and interbank contracts (SPOT-like transactions, REQ-ING-01/02). Grain: one row per CONTRACT_ID x AS_OF_DATE. Provides drawn and undrawn amounts, maturities and performing status for LCR inflows, facility drawdown outflows and interbank funding outflows, plus the atomic contract level for drill-down and variance root-cause analysis. Loaded hourly from contracts_YYYYMMDD.csv via stage LIQI_RAW_ST_CONTRACTS. |
| REP_RAW_V001 | [LIQI_RAW_TB_COUNTERPARTIES](#liqi_raw_tb_counterparties) | BASE TABLE | 5,298 | Counterparty master for the LCR book (SPOT-like master data, REQ-ING-01). One row per counterparty (latest load wins). Covers retail customers (linked to CRM), corporates, SMEs, financial institutions and HQLA issuers. Provides sector classification for regulatory run-off / inflow treatment, client names for root-cause analysis and the counterparty level of the REQ-ANL-02 drill-down. Loaded from counterparties.csv via stage LIQI_RAW_ST_COUNTERPARTIES. |
| REP_RAW_V001 | [LIQI_RAW_TB_DEPOSIT_BALANCES](#liqi_raw_tb_deposit_balances) | BASE TABLE | 942,750 | Daily snapshot table of customer deposit balances enriched with regulatory attributes for FINMA LCR net cash outflow calculation per Circular 2015/2. Links to actual customer base (CRM_RAW_v001) enabling cross-domain analytics between customer profiles and funding stability. Contains balance data, relationship indicators (product count, direct debit, tenure), and operational designations. Source data loaded from core banking systems via CSV extracts. Used for LCR denominator calculation with relationship discounts (-2% for 3+ products, -1% for direct debit) and tenure penalties (+5% for under 18 months). Supports funding stability analysis, customer segmentation by deposit behavior, and SNB monthly regulatory reporting. Critical operational data for Treasury funding decisions and deposit retention strategies. |
| REP_RAW_V001 | [LIQI_RAW_TB_DEPOSIT_TYPES](#liqi_raw_tb_deposit_types) | BASE TABLE | 7 | Master reference table for FINMA LCR deposit classification and stressed run-off rate assumptions. Defines counterparty types (Retail/Corporate/FI), base run-off rates (3% to 100%), relationship discount eligibility, and operational deposit treatment per FINMA Circular 2015/2 and Basel III. Used by Treasury for funding stability analysis, by ALM for liquidity stress testing, and by Compliance for SNB regulatory submissions. Supports automated outflow calculation, customer relationship scoring, and deposit retention strategy optimization. Critical reference data for LCR denominator calculation and strategic funding planning. |
| REP_RAW_V001 | [LIQI_RAW_TB_EVENTS](#liqi_raw_tb_events) | BASE TABLE | 5 | Deterministic showcase events injected by the LCR generator (deposit withdrawals, market stress, recovery, loan inflows). Single source of truth for chart annotations, validation tests (TC-VAL-04) and the liquidity agent root-cause narrative. Loaded from lcr_events.csv via stage LIQI_RAW_ST_EVENTS. |
| REP_RAW_V001 | [LIQI_RAW_TB_HQLA_ELIGIBILITY](#liqi_raw_tb_hqla_eligibility) | BASE TABLE | 9 | Master reference table for FINMA LCR High-Quality Liquid Assets (HQLA) classification. Defines regulatory levels (L1, L2A, L2B), haircut factors (0%, 15%, 50%), and eligibility criteria per FINMA Circular 2015/2 and Basel III. Used by Treasury for liquidity buffer management, by Risk for LCR monitoring, and by Compliance for SNB regulatory submissions. Supports automated eligibility screening, portfolio composition analysis, and 40% cap rule enforcement. Critical reference data for daily LCR calculation and strategic liquidity planning. |
| REP_RAW_V001 | [LIQI_RAW_TB_HQLA_HOLDINGS](#liqi_raw_tb_hqla_holdings) | BASE TABLE | 5,580 | Daily snapshot table of High-Quality Liquid Assets (HQLA) held by Treasury for liquidity buffer management per FINMA Circular 2015/2. Contains securities positions with market values, regulatory classifications, and eligibility flags. Source data loaded from treasury management systems via CSV extracts. Used for LCR numerator calculation (HQLA stock with haircuts), portfolio concentration analysis, and SNB monthly regulatory reporting. Supports real-time liquidity monitoring, investment committee reporting, and Basel III compliance. Critical operational data for Treasury decision-making and regulatory supervision. |
| REP_RAW_V001 | [LIQI_RAW_TB_MATURITY_BUCKETS](#liqi_raw_tb_maturity_buckets) | BASE TABLE | 7 | Standardised maturity buckets for the cashflow engine and liquidity gap analysis (REQ-ENG-01): Overnight, 2-7D, 8-30D, 1-3M, 3-6M, 6-12M, >1Y. Seeded by post_deploy.sql. |
| REP_RAW_V001 | [LIQI_RAW_TB_PRODUCT_MAPPING](#liqi_raw_tb_product_mapping) | BASE TABLE | 22 | Multi-taxonomy mapping of source product codes to LCR asset classes, product groups and LCR treatment (REQ-MAP-01). Any product code without an active mapping is routed to the exceptions table as UNMAPPED_ASSET_CLASS (REQ-DQ-01). Seeded by post_deploy.sql. |
| REP_RAW_V001 | [LIQI_RAW_TB_SCENARIOS](#liqi_raw_tb_scenarios) | BASE TABLE | 4 | Catalogue of liquidity stress scenarios evaluated simultaneously by the LCR engine (REQ-ENG-05). Seeded by post_deploy.sql. |
| REP_AGG_V001 | [LCRS_AGG_VW_ALERTS_ACTIVE](#lcrs_agg_vw_alerts_active) | VIEW |  | Active LCR compliance alerts filtered for dashboard display. Pre-filtered to show only unresolved alerts requiring attention. Used for notebook queries and real-time monitoring dashboards. Includes severity classification and recommended actions for Treasury escalation. |
| REP_AGG_V001 | [LCRS_AGG_VW_HQLA_BREAKDOWN](#lcrs_agg_vw_hqla_breakdown) | VIEW |  | HQLA composition by level (L1/L2A/L2B) and asset type with market value, haircuts, and percentage of total. Used for portfolio analysis, rebalancing decisions, and notebook queries about asset composition. Includes all HQLA holdings for latest reporting date. |
| REP_AGG_V001 | [LCRS_AGG_VW_LCR_CURRENT](#lcrs_agg_vw_lcr_current) | VIEW |  | Current LCR status - single row with all key metrics for executive dashboard. Returns latest available LCR calculation with compliance status, HQLA composition, outflow breakdown, and buffer analysis. Optimized for fast retrieval and notebook queries. |
| REP_AGG_V001 | [LCRS_AGG_VW_OUTFLOW_BREAKDOWN](#lcrs_agg_vw_outflow_breakdown) | VIEW |  | Deposit outflow breakdown by counterparty type showing balances, run-off rates, and outflow amounts. Used for funding strategy analysis and notebook queries about deposit composition. Includes percentage breakdowns for executive reporting and waterfall chart visualization. |
| REP_AGG_V001 | [LCRS_AGG_VW_TREND_90DAY](#lcrs_agg_vw_trend_90day) | VIEW |  | 90-day LCR historical trend with 7-day and 30-day moving averages. Used for volatility analysis, compliance tracking, and notebook queries about historical performance. Includes day-over-day change calculations and breach detection for management escalation. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_CUTOFF_DELTAS](#repp_agg_dt_lcr_cutoff_deltas) | DYNAMIC TABLE | 10,208 | Snapshot comparison of LCR components between cut-off dates (REQ-ANL-01): day-over-day, week-over-week and month-over-month deltas of HQLA, outflows and inflows per asset class, plus a TOTAL row with the LCR change. Calendar-day comparisons (data exists for every calendar day). |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_DAILY](#repp_agg_dt_lcr_daily) | DYNAMIC TABLE | 90 | Daily Liquidity Coverage Ratio (LCR) calculation per FINMA Circular 2015/2. Combines HQLA stock and 30-day net cash outflows to calculate the primary Basel III liquidity metric. Built from the BASE rows of REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC incl. L2B 15% cap, L2 40% cap and 75% inflow cap). Determines status FAIL (<100%), WARNING (100-110%), PASS (>=110%) and the liquidity buffer. Critical metric monitored daily by Treasury, reported monthly to Swiss National Bank (SNB), and used for strategic liquidity planning. Regulatory minimum 100% must be maintained at all times. Updated hourly for real-time compliance monitoring and management escalation. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_EXCEPTIONS](#repp_agg_dt_lcr_exceptions) | DYNAMIC TABLE | 630 | Quarantine of invalid LCR source records (REQ-DQ-01, spec 3.2). Records that fail mapping, FX or date validation are excluded from all LCR calculations and listed here with an error code and explanation, so the run never fails and stewards can remediate (TC-VAL-03). |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_FX_DAILY](#repp_agg_dt_lcr_fx_daily) | DYNAMIC TABLE | 3,426 | Daily end-of-day CCY->CHF conversion rates derived from REFI_RAW_TB_FX_RATES (USD-based quotes crossed via USD). One row per calendar date and currency so the LCR engine converts all positions with a plain equality join (REQ-ING-03). Currencies without a rate on a date are absent and surface as MISSING_FX exceptions. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_HQLA](#repp_agg_dt_lcr_hqla) | DYNAMIC TABLE | 810 | Daily regulatory (BASE scenario) HQLA holdings aggregated by HQLA level and asset type: holding counts, gross market value, haircut and post-haircut value. Built from REPP_AGG_DT_LCR_SCENARIO_POSITIONS so the composition always matches the LCR numerator. Used by semantic views, composition dashboards and the liquidity agent. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_HQLA_CALCULATION](#repp_agg_dt_lcr_hqla_calculation) | DYNAMIC TABLE | 90 | Daily regulatory (BASE) HQLA stock with both Basel III / FINMA caps: L2B limited to 15/85 of (L1 + L2A) and L2 limited to 2/3 of L1. LCR numerator. Built from REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC). |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_LIQUIDITY_GAP](#repp_agg_dt_lcr_liquidity_gap) | DYNAMIC TABLE | 2,520 | Maturity ladder / liquidity gap per date, scenario and standard bucket (REQ-ENG-01, REQ-ENG-02): contractual cashflows from LIQI_RAW_TB_CASHFLOWS plus behavioral decay of non-maturity deposits (linear over the scenario decay horizon), with counterbalancing HQLA and cumulative gap. Feeds the gap chart and the behavioral heatmap. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_OUTFLOW](#repp_agg_dt_lcr_outflow) | DYNAMIC TABLE | 360 | Daily regulatory (BASE scenario) stressed outflows by segment: retail, corporate, financial institutions (deposits + interbank borrowing) and committed facilities. Gross amounts, effective run-off rates, outflows and shares. Built from REPP_AGG_DT_LCR_SCENARIO_POSITIONS so totals equal OUTFLOW_TOTAL of REPP_AGG_DT_LCR_DAILY. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_OUTFLOW_CALCULATION](#repp_agg_dt_lcr_outflow_calculation) | DYNAMIC TABLE | 90 | Daily regulatory (BASE) net cash outflow calculation: stressed outflows by segment incl. facility drawdowns, weighted contractual inflows with the 75% cap, and net outflows (LCR denominator). Built from REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC). |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_POSITIONS](#repp_agg_dt_lcr_positions) | DYNAMIC TABLE | 1,181,700 | Unified, CHF-converted position layer of the LCR engine: HQLA holdings, deposits and contracts in one grain (AS_OF_DATE x POSITION_ID) enriched with counterparty, product mapping (REQ-MAP-01), FX (REQ-ING-03) and data-quality validation (REQ-DQ-01). All downstream scenario, attribution and drill-down objects read from here; only DQ_STATUS = OK and INCLUDED_IN_LCR rows enter the LCR. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_SCENARIO_COMPONENTS](#repp_agg_dt_lcr_scenario_components) | DYNAMIC TABLE | 4,680 | Compact LCR component vectors per date and scenario (a few dozen rows per day). Input for REPP_AGG_DT_LCR_SCENARIO_DAILY, the scenario simulator procedure and the agent what-if tool, so what-if recalculations run in milliseconds without touching position data. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_SCENARIO_DAILY](#repp_agg_dt_lcr_scenario_daily) | DYNAMIC TABLE | 360 | Daily LCR result per scenario (REQ-ENG-05): HQLA by level with both regulatory caps, outflows by segment, inflows with the 75% cap, net outflows, LCR ratio and status (FAIL < 100%, WARNING 100-110%, PASS >= 110%). All values computed with the shared REPP_AGG_FN_LCR_CALC formula. BASE rows feed the regulatory LCR dynamic tables. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_SCENARIO_POSITIONS](#repp_agg_dt_lcr_scenario_positions) | DYNAMIC TABLE | 4,650,480 | Position-level LCR contributions for every scenario (REQ-ENG-03/04/05): each valid in-scope position from REPP_AGG_DT_LCR_POSITIONS is combined with the scenario-specific behavioral rule (run-off, inflow, drawdown, haircut add-on) to produce weighted HQLA, outflow and inflow amounts. Single source for scenario components, variance attribution, drill-down and the simulator. |
| REP_AGG_V001 | [REPP_AGG_DT_LCR_TREND](#repp_agg_dt_lcr_trend) | DYNAMIC TABLE | 90 | Rolling trend analysis of LCR ratio with statistical metrics and automated alert detection. Calculates 7/30/90-day moving averages, volatility measures, day-over-day changes, and consecutive breach tracking. Enables proactive liquidity risk management by identifying adverse trends before regulatory breaches occur. Triggers Treasury alerts for high volatility (>10% daily change), sustained breaches (3+ consecutive days below 100%), and critical breaches (<95%). Used for Board reporting, management dashboards, and early warning system integration. |
| REP_AGG_V001 | [REPP_AGG_VW_LCR_ALERTS](#repp_agg_vw_lcr_alerts) | VIEW |  | Automated alert generation view for LCR compliance monitoring with structured alert messages and severity classification. Generates real-time alerts for regulatory breaches (LCR <100%), critical breaches (LCR <95%), high volatility (>10% daily change), sustained breaches (3+ consecutive days), and 40% cap violations. Each alert includes severity level (CRITICAL/HIGH/MEDIUM/INFO), alert type, descriptive message, and recommended action. Used by Treasury operations for real-time monitoring, by Compliance for breach documentation, and integrated with notification systems for management escalation. Critical for FINMA reporting obligations and audit trail maintenance. |
| REP_AGG_V001 | [REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL](#repp_agg_vw_lcr_deposit_balances_detail) | VIEW |  | Account-level detail view of deposit balances with run-off rate calculations and relationship-based discount logic. Provides drill-down capability from aggregate outflow metrics to individual accounts for funding stability analysis, customer relationship quality assessment, and deposit retention strategy optimization. Includes base run-off rates, relationship discounts (product count, direct debit), tenure penalties, and final calculated outflow amounts. Used by Treasury for funding planning and by Retail Banking for customer relationship management. |
| REP_AGG_V001 | [REPP_AGG_VW_LCR_DRILLDOWN](#repp_agg_vw_lcr_drilldown) | VIEW |  | Five-level LCR drill-down from asset class to product group, product, counterparty and individual contract (REQ-ANL-02) with business unit, currency and rating filter attributes (REQ-UI-02). Treemap and table source in the LCR page; filter by AS_OF_DATE and STRESS_SCENARIO. |
| REP_AGG_V001 | [REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL](#repp_agg_vw_lcr_hqla_holdings_detail) | VIEW |  | Security-level detail view of HQLA holdings with regulatory level classification and haircut calculations. Provides drill-down capability from aggregate HQLA metrics to individual securities for portfolio analysis, concentration risk monitoring, and regulatory reporting. Includes market values, weighted values after haircuts, credit ratings, and SNB coordinate mappings. Used by Treasury for portfolio rebalancing decisions and by Compliance for regulatory submission preparation. |
| REP_AGG_V001 | [REPP_AGG_VW_LCR_MONITORING](#repp_agg_vw_lcr_monitoring) | VIEW |  | Consolidated LCR monitoring dashboard view combining all key liquidity metrics in a single row. Integrates latest daily LCR ratio, compliance status, HQLA breakdown (L1/L2A/L2B), deposit outflow analysis (Retail/Corporate/FI), trend metrics (7/30/90-day averages), volatility measures, and automated alert flags. Optimized for Treasury dashboards, executive reporting, and downstream application integration. Single source of truth for current liquidity position and regulatory compliance status. Updated in real-time via underlying dynamic tables for operational decision-making and management escalation. |
| REP_AGG_V001 | [REPP_AGG_VW_LCR_MONTHLY_SUMMARY](#repp_agg_vw_lcr_monthly_summary) | VIEW |  | Monthly summary of LCR metrics for Swiss National Bank (SNB) regulatory reporting per FINMA Circular 2015/2. Aggregates daily LCR ratios by month with summary statistics (average, min, max, volatility), breach day counts (FAIL/WARNING/PASS), and compliance rates. Used by Compliance team for monthly regulatory submissions, by Treasury for performance reporting, and by Executive Management for Board reporting. Critical for demonstrating sustained compliance with Basel III liquidity requirements and trend analysis over time. |
| REP_AGG_V001 | [REPP_AGG_VW_LCR_VARIANCE_ATTRIBUTION](#repp_agg_vw_lcr_variance_attribution) | VIEW |  | Exact, additive attribution of LCR changes to individual positions (REQ-ANL-03, TC-VAL-04). Uses the decomposition dLCR = dH/N1 + H0 x (1/N1 - 1/N0), split by position, with separate lines for the L2 cap and the 75% inflow cap effects, so contributions always sum to the reported LCR change. Filter by AS_OF_DATE, COMPARISON_TYPE and STRESS_SCENARIO. |
| REP_AGG_V001 | [REPP_AGG_VW_LCR_VARIANCE_DRIVERS](#repp_agg_vw_lcr_variance_drivers) | VIEW |  | Top-10 client variance drivers per cut-off comparison and scenario (REQ-UI-03, REQ-ANL-03): the client positions (deposits, loans, facilities, interbank contracts) with the largest absolute exact contribution to the LCR change, with contract id and client name. Treasury HQLA moves and cap adjustments are excluded so the root cause (e.g. a client withdrawal) ranks first. |

## Functions and procedures

| Schema | Name | Type | Signature | Description |
|---|---|---|---|---|
| REP_AGG_V001 | REPP_AGG_FN_LCR_CALC | FUNCTION | `(L1 FLOAT, L2A FLOAT, L2B FLOAT, GROSS_OUTFLOW FLOAT, GROSS_INFLOW FLOAT, INFLOW_CAP_PCT FLOAT)` | Single shared FINMA/Basel III LCR formula used by the dynamic tables, the scenario simulator and the agent what-if tool. Inputs are post-haircut L1/L2A/L2B values and gross 30-day outflows/inflows in CHF. Applies the L2B cap (15/85 of L1+L2A), the L2 cap (2/3 of L1) and the inflow cap (INFLOW_CAP_PCT of outflows, regulatory 0.75). Returns OBJECT with L2B_CAPPED, L2_CAPPED, HQLA, INFLOW_CAPPED, NET_OUTFLOW, LCR (percent) and cap flags. |
| REP_AGG_V001 | REPP_AGG_SP_LCR_SIMULATE | PROCEDURE | `(P_AS_OF_DATE VARCHAR, P_PARAMS VARCHAR)` | What-if LCR simulator. P_AS_OF_DATE (YYYY-MM-DD, empty = latest) and P_PARAMS (JSON) with optional keys: preset (BASE \| BANK_STRESS \| MARKET_STRESS \| COMBINED, default BASE), retail_stable_mult, retail_less_stable_mult, corp_operational_mult, corp_non_operational_mult, fi_mult, facilities_mult, inflow_mult (multipliers on weighted flows, capped at 100% of gross), haircut_addon_l1_pp, haircut_addon_l2a_pp, haircut_addon_l2b_pp (extra haircut in percentage points), price_shock_pct (market value shock on securities), withdrawal_chf (corporate non-operational withdrawal settled from SNB cash), inflow_cap_pct (default 0.75). Returns a cumulative waterfall (one row per lever) and, on the final row, the additional corporate withdrawal that brings the LCR to 100% (breakeven; funded from SNB cash, then by liquidating L1 bonds; resolution 0.05% of the L1 stock). |
| REP_AGG_V001 | REPP_AGG_SP_LCR_WHAT_IF | PROCEDURE | `(P_PARAMS VARCHAR, P_AS_OF_DATE VARCHAR)` | Agent-friendly what-if wrapper around REPP_AGG_SP_LCR_SIMULATE (tool LCR_What_If of LIQUIDITY_RISK_AGENT). P_PARAMS is the same JSON as the simulator (preset, *_mult, haircut_addon_*_pp, price_shock_pct, withdrawal_chf, inflow_cap_pct); P_AS_OF_DATE YYYY-MM-DD or empty for the latest date. Returns a JSON string with baseline and simulated LCR, status, delta, the step-by-step waterfall and the breakeven corporate withdrawal (CHF) that would bring the LCR to 100%. |
| REP_AGG_V001 | SP_CHECK_LCR_BREACH | PROCEDURE | `()` |  |

## Columns

### LIQI_RAW_TB_BEHAVIORAL_RULES

REP_RAW_V001 · BASE TABLE — Scenario-specific behavioral and regulatory rule set for the LCR engine (spec 3.1, REQ-ENG-02/03/04/05): run-off, inflow and drawdown rates, stress haircut add-ons, decay profiles and simulator categories per product x counterparty sector x scenario. BASE rows implement FINMA LiqV; stress rows are illustrative internal stress assumptions. Seeded by post_deploy.sql.

| Column | Type | Description |
|---|---|---|
| PRODUCT_TYPE | TEXT | Product / deposit / asset type the rule applies to (see LIQI_RAW_TB_PRODUCT_MAPPING). |
| COUNTERPARTY_SECTOR | TEXT | Counterparty sector the rule applies to (RETAIL, SME, CORPORATE, FINANCIAL, FINANCIAL_INSTITUTION, ALL for HQLA). |
| STRESS_SCENARIO | TEXT | Scenario: BASE (regulatory FINMA LCR), BANK_STRESS (idiosyncratic), MARKET_STRESS (systemic), COMBINED (REQ-ENG-05). BASE is the reported LCR; others are internal stress tests. |
| RUNOFF_RATE | NUMBER | Base run-off rate (decimal) for deposit / funding outflows in this scenario, before account-level relationship discounts and tenure penalties (REQ-ENG-03). |
| INFLOW_RATE | NUMBER | Inflow rate (decimal) applied to contractual inflows within 30 days (50% non-financial, 100% financial in BASE; reduced in stress). Total inflows are capped at 75% of outflows. |
| DRAWDOWN_RATE | NUMBER | Drawdown rate (decimal) applied to undrawn committed facilities (BASE: 5% retail, 10% corporate credit, 30% corporate liquidity, 40% banks). |
| HAIRCUT_ADDON_PCT | NUMBER | Additional haircut in percentage points on top of the regulatory haircut for HQLA products in stress scenarios (0 in BASE) (REQ-ENG-04). |
| DECAY_PROFILE_DAYS | NUMBER | Behavioral maturity horizon in days for non-maturity deposits: balances are spread across maturity buckets up to this horizon in the liquidity gap view (REQ-ENG-02). |
| DECAY_METHOD | TEXT | Behavioral decay method for non-maturity products: LINEAR (equal daily run-down) or NONE (contractual only). |
| SIM_CATEGORY | TEXT | Component category used by the scenario simulator (e.g. RETAIL_STABLE, CORP_NON_OPERATIONAL, FACILITIES, INFLOW_NONFIN, L2B) to apply what-if multipliers. |
| RULE_DESCRIPTION | TEXT | Business explanation and regulatory reference of the rule (FINMA LiqV Annex 2/3, Basel III LCR). |
| IS_ACTIVE | BOOLEAN | Active flag for rule versioning; only active rules are used in calculations. |
| EFFECTIVE_DATE | DATE | Date from which the rule is effective (regulatory change management). |

### LIQI_RAW_TB_CASHFLOWS

REP_RAW_V001 · BASE TABLE — Daily contractual cashflow schedules per contract (REQ-ING-02, REQ-ENG-01). Grain: one row per AS_OF_DATE x CONTRACT_ID x CF_DATE x CF_TYPE. Basis for the maturity ladder / liquidity gap and for 30-day LCR inflows and interbank funding outflows. Snapshot partitioning by AS_OF_DATE supports cut-off comparisons. Loaded hourly from cashflows_YYYYMMDD.csv via stage LIQI_RAW_ST_CASHFLOWS.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Snapshot (cut-off) date of the cashflow projection. Each snapshot contains the full contractual schedule for the next 365 days as seen on that date. |
| CONTRACT_ID | TEXT | Contract generating the cashflow (FK to LIQI_RAW_TB_CONTRACTS on CONTRACT_ID + AS_OF_DATE). |
| CF_DATE | DATE | Contractual payment date. DATEDIFF(day, AS_OF_DATE, CF_DATE) assigns the flow to a maturity bucket (O/N, 2-7D, 8-30D, 1-3M, 3-6M, 6-12M, >1Y; REQ-ENG-01); flows within 30 days enter the LCR. |
| CF_TYPE | TEXT | Cashflow type: PRINCIPAL (repayment / amortisation) or INTEREST. Both count towards contractual LCR inflows / outflows. |
| DIRECTION | TEXT | Cashflow direction from the bank perspective: IN = receivable (loans, interbank placements), OUT = payable (interbank borrowing). |
| CURRENCY | TEXT | Cashflow currency per ISO 4217 (same as contract currency); converted to CHF with the end-of-day FX rate of the snapshot date. |
| AMOUNT_CCY | NUMBER | Cashflow amount in original currency (always positive; sign given by DIRECTION). |
| INSERT_TIMESTAMP_UTC | TIMESTAMP_NTZ | UTC timestamp when the record was loaded into Snowflake. Used for lineage and load monitoring. |

### LIQI_RAW_TB_CONTRACTS

REP_RAW_V001 · BASE TABLE — Daily snapshots of loans, credit / liquidity facilities and interbank contracts (SPOT-like transactions, REQ-ING-01/02). Grain: one row per CONTRACT_ID x AS_OF_DATE. Provides drawn and undrawn amounts, maturities and performing status for LCR inflows, facility drawdown outflows and interbank funding outflows, plus the atomic contract level for drill-down and variance root-cause analysis. Loaded hourly from contracts_YYYYMMDD.csv via stage LIQI_RAW_ST_CONTRACTS.

| Column | Type | Description |
|---|---|---|
| CONTRACT_ID | TEXT | Persistent contract identifier (CTR_nnnnnn) of a loan, credit / liquidity facility or interbank contract - the atomic level of the REQ-ANL-02 drill-down and the driver key in the variance attribution. |
| AS_OF_DATE | DATE | Snapshot (cut-off) date of the contract state (daily COB). Partitioning dimension for DoD / WoW / MoM comparisons (REQ-ANL-01). |
| COUNTERPARTY_ID | TEXT | Counterparty of the contract (FK to LIQI_RAW_TB_COUNTERPARTIES). Determines sector-specific inflow / drawdown rates and the client name shown in root-cause analysis. |
| PRODUCT_TYPE | TEXT | Source product code: LOAN_RETAIL, LOAN_CORP, INTERBANK_LOAN, CREDIT_FACILITY, LIQUIDITY_FACILITY, INTERBANK_BORROWING. Unknown codes (e.g. NEW_DERIVATIVE_X) are quarantined as UNMAPPED_ASSET_CLASS exceptions (REQ-DQ-01, TC-VAL-03). |
| BUSINESS_UNIT | TEXT | Business unit booking the contract (RETAIL, CORPORATE, TREASURY). Used for the business-unit filter (REQ-UI-02) and allocation of inflows / facility outflows. |
| CURRENCY | TEXT | Contract currency per ISO 4217. Converted to CHF with the latest available daily FX rate (daily end-of-day rate from REFI_RAW_TB_FX_RATES joined on DATE = AS_OF_DATE, cross via USD). Currencies without an FX rate (e.g. SEK) are quarantined as MISSING_FX (REQ-ING-03, REQ-DQ-01). |
| RATING | TEXT | Counterparty rating at contract level (S&P scale). Used for rating filters and credit quality analysis of contractual inflows. |
| START_DATE | DATE | Contract start / last roll-over date. A START_DATE after MATURITY_DATE is quarantined as INVALID_DATE (REQ-DQ-01). |
| MATURITY_DATE | DATE | Contractual final maturity / next roll-over date. Contracts maturing within 30 days generate LCR inflows (assets) or outflows (interbank borrowing); also drives the maturity ladder (REQ-ENG-01). |
| OUTSTANDING_CCY | NUMBER | Drawn / outstanding principal in contract currency. Source of principal cashflows; zero for pure undrawn facilities. |
| UNDRAWN_CCY | NUMBER | Committed but undrawn facility amount in contract currency. Basis for LCR drawdown outflows (5% retail, 10% corporate credit, 30% corporate liquidity, 40% banks, 100% other financials per FINMA LiqV). |
| INTEREST_RATE | NUMBER | Annual contractual interest rate as decimal (e.g. 0.025 = 2.5%). Used to derive interest cashflows in the cashflow schedule. |
| IS_PERFORMING | BOOLEAN | Performing status of the exposure. Non-performing assets receive a 0% inflow rate in the LCR (Basel III para 142). |
| COUNTERPARTY_SECTOR | TEXT | Sector of the counterparty at booking (RETAIL, SME, CORPORATE, FINANCIAL). Key, together with PRODUCT_TYPE, into product mapping and behavioral rules. |
| INSERT_TIMESTAMP_UTC | TIMESTAMP_NTZ | UTC timestamp when the record was loaded into Snowflake. Used for lineage, SLA monitoring and identification of late-arriving snapshots. |

### LIQI_RAW_TB_COUNTERPARTIES

REP_RAW_V001 · BASE TABLE — Counterparty master for the LCR book (SPOT-like master data, REQ-ING-01). One row per counterparty (latest load wins). Covers retail customers (linked to CRM), corporates, SMEs, financial institutions and HQLA issuers. Provides sector classification for regulatory run-off / inflow treatment, client names for root-cause analysis and the counterparty level of the REQ-ANL-02 drill-down. Loaded from counterparties.csv via stage LIQI_RAW_ST_COUNTERPARTIES.

| Column | Type | Description |
|---|---|---|
| COUNTERPARTY_ID | TEXT | Counterparty key. Retail = CRM customer_id (links to CRMI_RAW_TB_CUSTOMER), CP_CORP_* = corporate / SME clients, CP_FI_* = banks and other financial institutions, ISS_* = HQLA issuers (SNB, Confederation, cantons, Pfandbrief institutes). Join key for deposits, contracts and holdings. |
| COUNTERPARTY_NAME | TEXT | Legal or display name of the counterparty shown in drill-downs and the Top-10 variance driver widget (REQ-UI-03), e.g. Alpenbau Holding AG. Retail customers carry a pseudonymised name. |
| SECTOR | TEXT | Basel III / FINMA counterparty sector: RETAIL, SME, CORPORATE, FINANCIAL, SOVEREIGN, CENTRAL_BANK. Drives run-off, inflow and drawdown rates via LIQI_RAW_TB_BEHAVIORAL_RULES (e.g. 50% inflow for non-financials, 100% for financials). |
| COUNTRY | TEXT | ISO 3166 alpha-2 country of domicile of the counterparty. Used for geographic concentration analysis of funding and lending exposures and for cross-border liquidity reporting. |
| RATING | TEXT | External credit rating of the counterparty (S&P scale, e.g. A-, BBB+). Used for the rating filter (REQ-UI-02), credit quality of inflows and counterparty concentration monitoring. NULL for retail customers. |
| CUSTOMER_ID | TEXT | Link to the CRM customer master (CRMI_RAW_TB_CUSTOMER.CUSTOMER_ID) for retail counterparties; NULL for wholesale counterparties and issuers. Enables cross-domain customer 360 / liquidity analytics. |
| BUSINESS_UNIT | TEXT | Business unit owning the client relationship: RETAIL, PRIVATE_BANKING, CORPORATE or TREASURY. Used for business-unit filtering and allocation of liquidity metrics (REQ-UI-02). |
| INSERT_TIMESTAMP_UTC | TIMESTAMP_NTZ | UTC timestamp when the record was loaded into Snowflake. Used for lineage and to keep only the latest version per COUNTERPARTY_ID when the master file is reloaded. |

### LIQI_RAW_TB_DEPOSIT_BALANCES

REP_RAW_V001 · BASE TABLE — Daily snapshot table of customer deposit balances enriched with regulatory attributes for FINMA LCR net cash outflow calculation per Circular 2015/2. Links to actual customer base (CRM_RAW_v001) enabling cross-domain analytics between customer profiles and funding stability. Contains balance data, relationship indicators (product count, direct debit, tenure), and operational designations. Source data loaded from core banking systems via CSV extracts. Used for LCR denominator calculation with relationship discounts (-2% for 3+ products, -1% for direct debit) and tenure penalties (+5% for under 18 months). Supports funding stability analysis, customer segmentation by deposit behavior, and SNB monthly regulatory reporting. Critical operational data for Treasury funding decisions and deposit retention strategies.

| Column | Type | Description |
|---|---|---|
| ACCOUNT_ID | TEXT | Unique deposit account identifier linking to customer accounts. Format: CUST_XXXXX_DEP_XX for traceability. Used for account-level outflow analysis, customer relationship scoring, and referential integrity with CRM_RAW_v001. |
| AS_OF_DATE | DATE | Reporting date for deposit position (daily COB snapshot). Used for time-series funding analysis, LCR trend monitoring, and regulatory reporting periods. Primary date dimension for outflow calculations. |
| CUSTOMER_ID | TEXT | Foreign key to CRM_RAW_v001.CRMI_RAW_TB_CUSTOMER for customer profile integration. Enables cross-domain analytics: deposit stability by account tier, risk classification impact on funding, and relationship banking effectiveness. Critical for customer-centric liquidity management. |
| DEPOSIT_TYPE | TEXT | Deposit type code linking to run-off rate rules (e.g., RETAIL_STABLE_INSURED, CORPORATE_OPERATIONAL). Determines base stressed outflow rate (3% to 100%) per Basel III. Critical for automated LCR denominator calculation. |
| CURRENCY | TEXT | Deposit currency per ISO 4217 (CHF, EUR, USD). Used for FX exposure analysis, currency concentration monitoring, and multi-currency funding management. Supports FX risk reporting and hedge strategy validation. |
| BALANCE_CCY | NUMBER | Deposit balance in original account currency. Source data for FX conversion and currency-level funding analysis. Used for customer statements, P&L attribution, and hedge effectiveness monitoring. |
| BALANCE_CHF | NUMBER | Deposit balance converted to CHF base currency using FX_RATE. Primary value field for LCR outflow calculation, funding aggregation, and regulatory reporting. All LCR calculations performed in CHF per FINMA requirements. |
| FX_RATE | NUMBER | FX conversion rate used for CCY to CHF conversion. Links to REF_RAW_v001.FX rates for reconciliation. Used for P&L attribution, FX sensitivity analysis, and audit trail. NULL when CURRENCY=CHF. |
| IS_INSURED | BOOLEAN | Flag indicating balance within CHF 100K deposit insurance limit per Basel III. Insured deposits qualify for lower run-off rates (3-5%) due to perceived stability. Used for deposit insurance reporting and customer communication. |
| PRODUCT_COUNT | NUMBER | Number of active products held by customer for relationship discount eligibility. 3+ products qualify for -2% run-off discount. Used for relationship banking scoring, customer retention analytics, and cross-sell effectiveness measurement. |
| ACCOUNT_TENURE_DAYS | NUMBER | Days since account opening for stability assessment. Accounts under 18 months incur +5% tenure penalty on run-off rate. Used for customer lifecycle analysis, churn prediction, and new account quality monitoring. |
| HAS_DIRECT_DEBIT | BOOLEAN | Flag indicating active direct debit mandate (salary, rent, utilities). Qualifies for -1% relationship discount. Proxy for operational relationship strength. Used for deposit retention strategy and payment behavior analysis. |
| IS_OPERATIONAL | BOOLEAN | Corporate operational deposit designation per Basel III (payroll, clearing accounts). Operational deposits receive 25% run-off vs 40% non-operational. Must demonstrate operational necessity. Critical for correct LCR calculation and corporate relationship classification. |
| COUNTERPARTY_TYPE | TEXT | Counterparty classification: RETAIL (individuals), CORPORATE (businesses), FINANCIAL_INSTITUTION (banks/insurers). Determines regulatory treatment, concentration limits, and reporting segmentation. Critical for Basel III compliance and funding strategy. |
| CUSTOMER_SEGMENT | TEXT | Customer segment classification (MASS, AFFLUENT, PRIVATE, CORPORATE). Used for customer analytics, deposit stability analysis by segment, and relationship banking strategy. Enables targeted retention programs and pricing optimization. |
| ACCOUNT_STATUS | TEXT | Account operational status (ACTIVE, DORMANT, CLOSED). Only ACTIVE accounts included in LCR calculation. Used for account lifecycle management, dormancy tracking, and customer reactivation campaigns. |
| INSERT_TIMESTAMP_UTC | TIMESTAMP_NTZ | UTC timestamp when record was loaded into Snowflake. Used for data lineage, SLA monitoring, and identifying late-arriving data. Critical for operational dashboards and data quality tracking. |
| BUSINESS_UNIT | TEXT | Owning business unit of the deposit (RETAIL, PRIVATE_BANKING, CORPORATE, TREASURY for interbank deposits). Used for the REQ-UI-02 business-unit filter, funding-mix analysis by segment and allocation of stressed outflows to business units. |
| COUNTERPARTY_ID | TEXT | Counterparty of the deposit (FK to LIQI_RAW_TB_COUNTERPARTIES; equals CUSTOMER_ID for retail customers, CP_CORP_* / CP_FI_* for wholesale clients). Used to show client names in the Top-10 variance drivers (REQ-UI-03) and the counterparty level of the drill-down. |

### LIQI_RAW_TB_DEPOSIT_TYPES

REP_RAW_V001 · BASE TABLE — Master reference table for FINMA LCR deposit classification and stressed run-off rate assumptions. Defines counterparty types (Retail/Corporate/FI), base run-off rates (3% to 100%), relationship discount eligibility, and operational deposit treatment per FINMA Circular 2015/2 and Basel III. Used by Treasury for funding stability analysis, by ALM for liquidity stress testing, and by Compliance for SNB regulatory submissions. Supports automated outflow calculation, customer relationship scoring, and deposit retention strategy optimization. Critical reference data for LCR denominator calculation and strategic funding planning.

| Column | Type | Description |
|---|---|---|
| DEPOSIT_TYPE | TEXT | Unique deposit type code (e.g., RETAIL_STABLE_INSURED, CORPORATE_OPERATIONAL). Used to classify deposit accounts and determine stressed outflow assumptions. Key for joining with deposit balance data. |
| DEPOSIT_NAME | TEXT | Descriptive name of deposit type for reporting and user interface display. Human-readable classification used in Treasury dashboards, customer analytics, and FINMA submissions. |
| BASE_RUN_OFF_RATE | NUMBER | Base stressed run-off rate under Basel III 30-day scenario (0.03 to 1.00). Represents percentage of deposits assumed to be withdrawn during liquidity stress. Core input for LCR denominator calculation. Lower rates = more stable funding. |
| COUNTERPARTY_TYPE | TEXT | Counterparty classification: RETAIL (individuals), CORPORATE (businesses), FINANCIAL_INSTITUTION (banks/insurers). Determines regulatory treatment, concentration limits, and reporting segmentation. Critical for Basel III compliance. |
| SNB_COORDINATE | TEXT | Swiss National Bank reporting coordinate for outflow classification (e.g., 1.2.1.1 for retail stable). Required for monthly LCR regulatory submission to SNB and data quality reconciliation with FINMA templates. |
| ELIGIBILITY_CRITERIA | TEXT | Business rules for deposit type classification under FINMA Circular 2015/2. Documents criteria such as balance thresholds (CHF 100K), product counts (3+), operational status. Used by Compliance for audit and customer classification validation. |
| ALLOWS_RELATIONSHIP_DISCOUNT | BOOLEAN | Flag indicating whether relationship-based run-off discounts apply (-2% for 3+ products, -1% for direct debit). Used in outflow calculation logic. TRUE for stable retail deposits, FALSE for wholesale/institutional funding. |
| IS_INSURED | BOOLEAN | Flag indicating deposits within CHF 100K deposit insurance limit per Basel III. Insured deposits receive lower run-off rates due to perceived stability. Used for customer segmentation and deposit insurance reporting. |
| IS_OPERATIONAL | BOOLEAN | Flag for operational deposits per Basel III (e.g., payroll, clearing accounts with lower run-off). Corporate operational deposits receive 25% run-off vs 40% for non-operational. Critical for correct LCR calculation. |
| IS_ACTIVE | BOOLEAN | Active status flag for reference data versioning. FALSE when deposit type is deprecated due to regulatory or product changes. Used to filter current classification rules while maintaining audit history. |
| EFFECTIVE_DATE | DATE | Date when this run-off rate rule becomes effective. Supports regulatory change management (e.g., Basel III revisions) and historical LCR recalculation. Critical for audit trail and time-travel analysis. |
| END_DATE | DATE | Date when this rule was superseded or deactivated. NULL for currently active rules. Used with EFFECTIVE_DATE for SCD Type 2 temporal logic and regulatory impact analysis of rate changes. |
| INSERT_TIMESTAMP_UTC | TIMESTAMP_NTZ | UTC timestamp of record creation for data lineage tracking. Used for operational monitoring and compliance audit trails. |

### LIQI_RAW_TB_EVENTS

REP_RAW_V001 · BASE TABLE — Deterministic showcase events injected by the LCR generator (deposit withdrawals, market stress, recovery, loan inflows). Single source of truth for chart annotations, validation tests (TC-VAL-04) and the liquidity agent root-cause narrative. Loaded from lcr_events.csv via stage LIQI_RAW_ST_EVENTS.

| Column | Type | Description |
|---|---|---|
| EVENT_ID | TEXT | Identifier of the showcase liquidity event (EVT_01..EVT_05). |
| EVENT_DATE | DATE | Business date on which the event hits the balance sheet; the LCR change between the previous day and this date is attributed to the event. |
| EVENT_TYPE | TEXT | Event category: DEPOSIT_WITHDRAWAL, MARKET_STRESS, RECOVERY, LOAN_INFLOW. Used for chart annotations and agent narratives. |
| DESCRIPTION | TEXT | Business description of the event as shown in the app and used by the liquidity agent to explain LCR movements. |
| COUNTERPARTY_ID | TEXT | Counterparty driving the event (FK to LIQI_RAW_TB_COUNTERPARTIES). |
| CONTRACT_ID | TEXT | Position (deposit account or contract) expected to be the top client variance driver on EVENT_DATE (ground truth for TC-VAL-04). |
| AMOUNT_CHF | NUMBER | Nominal size of the event in CHF (e.g. 50,000,000 for the Alpenbau withdrawal). |
| EXPECTED_DIRECTION | TEXT | Expected direction of the LCR move on EVENT_DATE: UP or DOWN. Validated by sqlunit/lcr_validation.sql. |
| EXPECTED_MIN_DELTA_PP | NUMBER | Minimum expected absolute day-over-day LCR change in percentage points on EVENT_DATE. Validated by sqlunit/lcr_validation.sql. |
| INSERT_TIMESTAMP_UTC | TIMESTAMP_NTZ | UTC timestamp when the record was loaded; latest load per EVENT_ID wins. |

### LIQI_RAW_TB_HQLA_ELIGIBILITY

REP_RAW_V001 · BASE TABLE — Master reference table for FINMA LCR High-Quality Liquid Assets (HQLA) classification. Defines regulatory levels (L1, L2A, L2B), haircut factors (0%, 15%, 50%), and eligibility criteria per FINMA Circular 2015/2 and Basel III. Used by Treasury for liquidity buffer management, by Risk for LCR monitoring, and by Compliance for SNB regulatory submissions. Supports automated eligibility screening, portfolio composition analysis, and 40% cap rule enforcement. Critical reference data for daily LCR calculation and strategic liquidity planning.

| Column | Type | Description |
|---|---|---|
| ASSET_TYPE | TEXT | Unique HQLA asset type code (e.g., CASH_SNB, GOVT_BOND_CHF). Used to classify treasury holdings and determine regulatory treatment. Key for joining with holdings data. |
| ASSET_NAME | TEXT | Descriptive name of the asset type for reporting and user interface display. Human-readable classification used in Treasury dashboards and FINMA submissions. |
| REGULATORY_LEVEL | TEXT | Basel III HQLA level: L1 (highest quality, 0% haircut), L2A (high quality, 15% haircut), L2B (acceptable quality, 50% haircut). Critical for LCR calculation and 40% cap rule application. |
| HAIRCUT_PCT | NUMBER | Regulatory haircut percentage (0%, 15%, or 50%) applied to market value per Basel III. Displayed in reports and used for compliance documentation. User-friendly representation of haircut factor. |
| HAIRCUT_FACTOR | NUMBER | Haircut multiplier for HQLA stock calculation (1.0000 for L1, 0.8500 for L2A, 0.5000 for L2B). Applied directly to market values: Weighted_Value = Market_Value × Haircut_Factor. Core calculation field for LCR numerator. |
| SNB_COORDINATE | TEXT | Swiss National Bank reporting coordinate for regulatory submission mapping (e.g., 1.1.1.1 for SNB reserves). Required for monthly LCR reports to SNB and FINMA data quality validation. |
| ELIGIBILITY_CRITERIA | TEXT | Business rules defining asset eligibility under FINMA Circular 2015/2. Used by Compliance for audit trails and by Treasury for pre-trade compliance checks. Documents regulatory requirements for each asset type. |
| MIN_CREDIT_RATING | TEXT | Minimum credit rating required for HQLA eligibility (e.g., AA- for foreign govt bonds). Used for automated eligibility screening and counterparty risk assessment. NULL if no rating requirement. |
| REQUIRES_SMI_CONSTITUENT | BOOLEAN | Flag indicating whether security must be in Swiss Market Index (SMI) for L2B equity eligibility. Used for automated equity screening and portfolio rebalancing decisions. TRUE only for EQUITY_SMI type. |
| IS_ACTIVE | BOOLEAN | Active status flag for reference data versioning. FALSE when asset type is deprecated due to regulatory changes. Used to filter current rules for LCR calculation while maintaining audit history. |
| EFFECTIVE_DATE | DATE | Date when this classification rule becomes effective. Supports regulatory change management and historical LCR recalculation. Critical for audit trail and time-travel queries. |
| END_DATE | DATE | Date when this rule was superseded or deactivated. NULL for currently active rules. Used with EFFECTIVE_DATE for SCD Type 2 temporal logic and regulatory impact analysis. |
| INSERT_TIMESTAMP_UTC | TIMESTAMP_NTZ | UTC timestamp of record creation for data lineage tracking. Used for operational monitoring and compliance audit trails. |

### LIQI_RAW_TB_HQLA_HOLDINGS

REP_RAW_V001 · BASE TABLE — Daily snapshot table of High-Quality Liquid Assets (HQLA) held by Treasury for liquidity buffer management per FINMA Circular 2015/2. Contains securities positions with market values, regulatory classifications, and eligibility flags. Source data loaded from treasury management systems via CSV extracts. Used for LCR numerator calculation (HQLA stock with haircuts), portfolio concentration analysis, and SNB monthly regulatory reporting. Supports real-time liquidity monitoring, investment committee reporting, and Basel III compliance. Critical operational data for Treasury decision-making and regulatory supervision.

| Column | Type | Description |
|---|---|---|
| HOLDING_ID | TEXT | Unique identifier for each HQLA holding position. Format: HOLD-YYYYMMDD-XXXXX for daily tracking. Used for position reconciliation, audit trail, and inventory management across treasury systems. |
| AS_OF_DATE | DATE | Reporting date for HQLA position (daily COB snapshot). Used for time-series analysis, historical LCR trends, and regulatory reporting periods. Primary date dimension for LCR calculations. |
| ASSET_TYPE | TEXT | HQLA asset type code linking to eligibility rules (e.g., CASH_SNB, GOVT_BOND_CHF). Determines regulatory level (L1/L2A/L2B) and haircut treatment. Critical for automated HQLA classification and LCR stock calculation. |
| ISIN | TEXT | International Securities Identification Number per ISO 6166 standard. Used for security master reconciliation, price verification, and external reporting. NULL for cash positions. Enables integration with market data providers. |
| SECURITY_NAME | TEXT | Descriptive name of the security for reporting and portfolio analysis. Used in Treasury dashboards, FINMA submissions, and Board reports. Human-readable identifier for investment committee reviews. |
| CURRENCY | TEXT | Original currency of the HQLA holding per ISO 4217 (CHF, EUR, USD, GBP). Used for FX exposure analysis, currency diversification monitoring, and multi-currency liquidity management. Critical for FX risk reporting. |
| QUANTITY | NUMBER | Number of shares or units held (for equities, funds). NULL for bonds (face value) and cash. Used for position size tracking, concentration analysis, and SMI constituent verification. Enables unit-level portfolio analytics. |
| MARKET_VALUE_CCY | NUMBER | Market value in original currency at COB pricing. Source data for FX conversion and P&L attribution. Used for currency-level portfolio analysis and hedge effectiveness monitoring. |
| MARKET_VALUE_CHF | NUMBER | Market value converted to CHF base currency using FX_RATE. Primary value field for LCR calculation, portfolio aggregation, and regulatory reporting. All HQLA calculations performed in CHF per FINMA requirements. |
| FX_RATE | NUMBER | FX conversion rate used for CCY to CHF conversion. Links to REF_RAW_v001.FX rates for reconciliation. Used for P&L attribution, FX sensitivity analysis, and audit trail. NULL when CURRENCY=CHF. |
| MATURITY_DATE | DATE | Final maturity date for bonds and fixed-term instruments. Used for duration analysis, rollover planning, and maturity ladder reporting. NULL for equities and perpetual instruments. Critical for term structure management. |
| CREDIT_RATING | TEXT | Current credit rating from S&P, Moody's, or Fitch. Used for eligibility screening (min AA- for some asset types), credit risk monitoring, and rating migration analysis. Critical for maintaining HQLA status. |
| SMI_CONSTITUENT | BOOLEAN | Flag indicating whether security is Swiss Market Index constituent. Required TRUE for EQUITY_SMI type eligibility. Used for automated L2B equity screening and SMI rebalancing impact analysis. |
| HQLA_ELIGIBLE | BOOLEAN | Flag indicating whether holding passes all FINMA eligibility criteria. FALSE triggers exclusion from LCR calculation and alerts Treasury. Used for pre-trade compliance checks and portfolio optimization. |
| INELIGIBILITY_REASON | TEXT | Explanation for HQLA exclusion when HQLA_ELIGIBLE=FALSE. Documents rating downgrades, maturity mismatches, or regulatory breaches. Used for audit trail, Compliance investigations, and remediation tracking. |
| PORTFOLIO_CODE | TEXT | Treasury portfolio identifier (e.g., TREASURY_LIQ, ALM_BUFFER). Used for portfolio segmentation, mandate compliance, and investment committee reporting. Links to portfolio strategy and risk limits. |
| CUSTODIAN | TEXT | Institution holding the securities (e.g., SIX SIS, Euroclear, Clearstream). Used for custodian risk concentration monitoring, settlement tracking, and operational due diligence. Critical for counterparty exposure management. |
| INSERT_TIMESTAMP_UTC | TIMESTAMP_NTZ | UTC timestamp when record was loaded into Snowflake. Used for data lineage, SLA monitoring, and identifying late-arriving data. Critical for operational dashboards and data quality tracking. |
| BUSINESS_UNIT | TEXT | Owning business unit of the HQLA position (always TREASURY for the liquidity buffer). Used for the REQ-UI-02 business-unit filter and for allocating the liquidity buffer to the treasury book in drill-downs. |
| ISSUER_ID | TEXT | Issuer / obligor of the security or cash balance (FK to LIQI_RAW_TB_COUNTERPARTIES, e.g. ISS_SNB, ISS_CH, ISS_PFB). Provides the counterparty level of the REQ-ANL-02 drill-down path Asset Class -> Product -> Counterparty -> Contract for HQLA positions. |

### LIQI_RAW_TB_MATURITY_BUCKETS

REP_RAW_V001 · BASE TABLE — Standardised maturity buckets for the cashflow engine and liquidity gap analysis (REQ-ENG-01): Overnight, 2-7D, 8-30D, 1-3M, 3-6M, 6-12M, >1Y. Seeded by post_deploy.sql.

| Column | Type | Description |
|---|---|---|
| BUCKET_CODE | TEXT | Maturity bucket code: ON, D2_7, D8_30, M1_3, M3_6, M6_12, Y1P. |
| BUCKET_NAME | TEXT | Display name of the bucket (Overnight, 2-7 days, 8-30 days, 1-3 months, 3-6 months, 6-12 months, >1 year). |
| FROM_DAY | NUMBER | Inclusive lower bound in days from the snapshot date. |
| TO_DAY | NUMBER | Inclusive upper bound in days from the snapshot date. |
| SORT_ORDER | NUMBER | Display order of the bucket in maturity ladders and gap charts. |
| IN_LCR_HORIZON | BOOLEAN | TRUE when the bucket lies within the 30-day LCR stress horizon. |

### LIQI_RAW_TB_PRODUCT_MAPPING

REP_RAW_V001 · BASE TABLE — Multi-taxonomy mapping of source product codes to LCR asset classes, product groups and LCR treatment (REQ-MAP-01). Any product code without an active mapping is routed to the exceptions table as UNMAPPED_ASSET_CLASS (REQ-DQ-01). Seeded by post_deploy.sql.

| Column | Type | Description |
|---|---|---|
| PRODUCT_TYPE | TEXT | Source product / asset / deposit type code as delivered by the source systems (holdings ASSET_TYPE, deposits DEPOSIT_TYPE, contracts PRODUCT_TYPE). |
| SOURCE_SYSTEM | TEXT | Source feed the code belongs to: HOLDINGS, DEPOSITS or CONTRACTS. |
| ASSET_CLASS | TEXT | Multi-level LCR asset class (REQ-MAP-01): HQLA_L1_CASH, HQLA_L1_SOVEREIGN, HQLA_L2A, HQLA_L2B, RETAIL_DEPOSITS, WHOLESALE_DEPOSITS, INTERBANK_FUNDING, LOANS, INTERBANK_PLACEMENTS, COMMITTED_FACILITIES. Top level of the drill-down path. |
| PRODUCT_GROUP | TEXT | Second drill-down level (e.g. SNB reserves, Government bonds, Covered bonds, Retail stable deposits, Corporate loans). Business-friendly grouping of product types. |
| LCR_TREATMENT | TEXT | How the product enters the LCR: HQLA (numerator), OUTFLOW (run-off), INFLOW (30-day contractual inflow), FACILITY (undrawn drawdown outflow) or NONE. |
| HQLA_LEVEL | TEXT | Basel III HQLA level for HQLA products (L1, L2A, L2B); NULL otherwise. |
| DESCRIPTION | TEXT | Business description of the mapping rule and its regulatory basis (FINMA LiqV / Circular 2015/2). |
| IS_ACTIVE | BOOLEAN | Active flag; inactive mappings are treated as unmapped and quarantined. |

### LIQI_RAW_TB_SCENARIOS

REP_RAW_V001 · BASE TABLE — Catalogue of liquidity stress scenarios evaluated simultaneously by the LCR engine (REQ-ENG-05). Seeded by post_deploy.sql.

| Column | Type | Description |
|---|---|---|
| SCENARIO_CODE | TEXT | Scenario code used across the LCR engine: BASE, BANK_STRESS, MARKET_STRESS, COMBINED. |
| SCENARIO_NAME | TEXT | Display name of the scenario in the app, semantic views and agent answers. |
| DESCRIPTION | TEXT | Business narrative of the scenario assumptions. |
| IS_REGULATORY | BOOLEAN | TRUE for the regulatory FINMA LCR (BASE); FALSE for internal stress tests. |
| SORT_ORDER | NUMBER | Display order of the scenario. |

### LCRS_AGG_VW_ALERTS_ACTIVE

REP_AGG_V001 · VIEW — Active LCR compliance alerts filtered for dashboard display. Pre-filtered to show only unresolved alerts requiring attention. Used for notebook queries and real-time monitoring dashboards. Includes severity classification and recommended actions for Treasury escalation.

| Column | Type | Description |
|---|---|---|
| ALERT_DATE | DATE | Reporting date on which the alert was raised. |
| ALERT_TYPE | TEXT | Alert type (LCR_BREACH_CRITICAL, LCR_BREACH, LCR_WARNING, HIGH_VOLATILITY, MODERATE_VOLATILITY, L2_CAP_APPLIED). |
| SEVERITY | TEXT | Severity / colour of the status (RED, YELLOW, GREEN) or alert severity (CRITICAL, HIGH, MEDIUM, INFO). |
| DESCRIPTION | TEXT | Alert message describing the condition. |
| CURRENT_LCR_RATIO | NUMBER | LCR in percent on the alert date. |
| ALERT_THRESHOLD | NUMBER | Threshold that triggered the alert (95, 100 or 66.67); NULL for volatility alerts. |
| RECOMMENDED_ACTION | TEXT | Recommended action for Treasury / Compliance. |
| SEVERITY_PRIORITY | NUMBER | Sort priority of the severity (1 = CRITICAL ... 4 = INFO). |
| DAYS_SINCE_ALERT | NUMBER | Days between the alert date and today. |
| ALERT_AGE | TEXT | Alert age bucket: Today, Yesterday, This Week, Older. |
| ALERT_CATEGORY | TEXT | Alert category: Compliance, Risk, Portfolio or Operational Alert. |
| ESCALATION_REQUIRED | BOOLEAN | TRUE when an LCR breach has not been resolved within 24 hours. |
| ALERT_GENERATED_UTC | TIMESTAMP_LTZ | UTC timestamp when the alert was generated. |

### LCRS_AGG_VW_HQLA_BREAKDOWN

REP_AGG_V001 · VIEW — HQLA composition by level (L1/L2A/L2B) and asset type with market value, haircuts, and percentage of total. Used for portfolio analysis, rebalancing decisions, and notebook queries about asset composition. Includes all HQLA holdings for latest reporting date.

| Column | Type | Description |
|---|---|---|
| REPORTING_DATE | DATE | Reporting (cut-off) date of the LCR figures (daily COB snapshot). |
| ASSET_LEVEL | TEXT | Basel III HQLA level of the asset type (L1, L2A, L2B). |
| ASSET_TYPE | TEXT | HQLA asset type code (e.g. CASH_SNB, GOVT_BOND_CHF, CANTON_BOND, EQUITY_SMI). |
| MARKET_VALUE_CHF | NUMBER | Gross market value in CHF before regulatory haircuts. |
| MARKET_VALUE_BILLIONS | NUMBER | Gross market value in CHF billions (display convenience). |
| HAIRCUT_PCT | NUMBER | Regulatory haircut in percent (0 for L1, 15 for L2A, 50 for L2B). |
| STANDARD_HAIRCUT_PCT | NUMBER | Standard Basel III haircut of the HQLA level in percent (0, 15, 50). |
| HQLA_VALUE_CHF | NUMBER | Post-haircut HQLA value in CHF before the L2 caps. |
| HQLA_VALUE_BILLIONS | NUMBER | Post-haircut HQLA value in CHF billions (display convenience). |
| HOLDINGS_COUNT | NUMBER | Number of eligible HQLA positions in the group. |
| PCT_OF_TOTAL_HQLA | NUMBER | Share of the group in total HQLA in percent. |
| LEVEL_SORT_ORDER | NUMBER | Display order of the HQLA level (1 = L1, 2 = L2A, 3 = L2B). |
| QUALITY_DESCRIPTION | TEXT | Business description of the HQLA quality level and its haircut. |

### LCRS_AGG_VW_LCR_CURRENT

REP_AGG_V001 · VIEW — Current LCR status - single row with all key metrics for executive dashboard. Returns latest available LCR calculation with compliance status, HQLA composition, outflow breakdown, and buffer analysis. Optimized for fast retrieval and notebook queries.

| Column | Type | Description |
|---|---|---|
| REPORTING_DATE | DATE | Reporting (cut-off) date of the LCR figures (daily COB snapshot). |
| BANK_ID | TEXT | Regulatory bank identifier used in SNB submissions (SYNTH-CH-999). |
| LCR_RATIO_PCT | NUMBER | Liquidity Coverage Ratio in percent (HQLA / net cash outflows x 100). |
| COMPLIANCE_STATUS | TEXT | Status band: FAIL (<100%), WARNING (100% to <110%), PASS (>=110%). |
| STATUS_COLOR | TEXT | Dashboard colour of the status: RED, YELLOW, GREEN. |
| HQLA_TOTAL_CHF | NUMBER | Stock of HQLA in CHF after the L2B and L2 caps (LCR numerator). |
| LEVEL1_ASSETS_CHF | NUMBER | Level 1 HQLA in CHF (0% haircut). |
| LEVEL2A_ASSETS_CHF | NUMBER | Level 2A HQLA in CHF after 15% haircut. |
| LEVEL2B_ASSETS_CHF | NUMBER | Level 2B HQLA in CHF after 50% haircut, before the 15% cap. |
| LEVEL2_ASSETS_AFTER_CAP_CHF | NUMBER | Level 2 HQLA in CHF after the L2B 15% and L2 40% caps. |
| LEVEL2_ASSETS_BEFORE_CAP_CHF | NUMBER | Level 2 HQLA in CHF (L2A + L2B) before any cap. |
| LEVEL1_PCT_OF_HQLA | NUMBER | Share of Level 1 in the HQLA stock in percent. |
| LEVEL2_PCT_OF_HQLA | NUMBER | Share of capped Level 2 in the HQLA stock in percent. |
| IS_40PCT_CAP_APPLIED | BOOLEAN | TRUE when the L2B 15% or L2 40% cap reduced the HQLA stock. |
| DISCARDED_LEVEL2_CHF | NUMBER | Level 2 assets in CHF excluded by the caps. |
| CAP_BUFFER_PCT | NUMBER | Headroom to the 40% L2 cap in percent of the cap (negative = cap binding). |
| NET_CASH_OUTFLOWS_CHF | NUMBER | Net cash outflows in CHF = gross outflows - inflows capped at 75% (LCR denominator). |
| RETAIL_OUTFLOWS_CHF | NUMBER | Stressed 30-day retail deposit outflows in CHF. |
| CORPORATE_OUTFLOWS_CHF | NUMBER | Stressed 30-day corporate / SME deposit outflows in CHF. |
| FINANCIAL_INST_OUTFLOWS_CHF | NUMBER | Stressed 30-day financial-institution deposit and interbank borrowing outflows in CHF. |
| RETAIL_PCT_OF_OUTFLOWS | NUMBER | Share of retail outflows in total gross outflows in percent. |
| CORPORATE_PCT_OF_OUTFLOWS | NUMBER | Share of corporate outflows in total gross outflows in percent. |
| FI_PCT_OF_OUTFLOWS | NUMBER | Share of financial-institution outflows in total gross outflows in percent. |
| LIQUIDITY_BUFFER_CHF | NUMBER | Liquidity buffer in CHF = HQLA - net cash outflows. |
| BUFFER_PCT_OF_OUTFLOWS | NUMBER | Liquidity buffer in percent of net cash outflows. |
| BUFFER_STRENGTH | TEXT | Buffer classification: STRONG, ADEQUATE, MODERATE, MINIMAL, BREACH. |
| HQLA_SECURITIES_COUNT | NUMBER | Number of eligible HQLA positions. |
| DEPOSIT_ACCOUNTS_COUNT | NUMBER | Number of deposit and borrowing positions in the calculation. |
| LAST_CALCULATED_UTC | TIMESTAMP_NTZ | UTC timestamp of the last LCR calculation (dynamic table refresh). |
| CALCULATION_AGE_MINUTES | NUMBER | Minutes since the last LCR calculation. |
| DATA_FRESHNESS | TEXT | Freshness flag of the calculation: FRESH (<=60 min), RECENT (<=120 min), STALE. |

### LCRS_AGG_VW_OUTFLOW_BREAKDOWN

REP_AGG_V001 · VIEW — Deposit outflow breakdown by counterparty type showing balances, run-off rates, and outflow amounts. Used for funding strategy analysis and notebook queries about deposit composition. Includes percentage breakdowns for executive reporting and waterfall chart visualization.

| Column | Type | Description |
|---|---|---|
| REPORTING_DATE | DATE | Reporting (cut-off) date of the LCR figures (daily COB snapshot). |
| COUNTERPARTY_TYPE | TEXT | Outflow segment / counterparty type (RETAIL, CORPORATE, FINANCIAL_INSTITUTION, COMMITTED_FACILITIES). |
| DEPOSIT_BALANCE_CHF | NUMBER | Gross amount of the outflow segment in CHF (balances, borrowings or undrawn facilities). |
| DEPOSIT_BALANCE_BILLIONS | NUMBER | Gross amount of the outflow segment in CHF billions. |
| AVG_RUN_OFF_RATE_PCT | NUMBER | Average effective run-off rate of the segment in percent. |
| MIN_RUN_OFF_RATE_PCT | NUMBER | Minimum effective run-off rate of the segment in percent. |
| MAX_RUN_OFF_RATE_PCT | NUMBER | Maximum effective run-off rate of the segment in percent. |
| OUTFLOW_AMOUNT_CHF | NUMBER | Stressed 30-day outflow in CHF (balance x run-off rate). |
| OUTFLOW_AMOUNT_BILLIONS | NUMBER | Stressed 30-day outflow in CHF billions (display convenience). |
| ACCOUNT_COUNT | NUMBER | Number of positions (accounts, borrowings, facilities) in the segment. |
| PCT_OF_TOTAL_OUTFLOWS | NUMBER | Share of the segment in total stressed outflows in percent. |
| PCT_OF_TOTAL_DEPOSITS | NUMBER | Share of the segment in the total gross amount in percent. |
| EFFECTIVE_RUN_OFF_PCT | NUMBER | Effective run-off rate of the segment in percent (outflow / gross amount). |
| COUNTERPARTY_CATEGORY | TEXT | Business label of the outflow segment (Retail, Corporate, Wholesale Funding, Committed Facilities). |
| STABILITY_RATING | TEXT | Funding stability rating derived from the average run-off rate. |
| DISPLAY_ORDER | NUMBER | Display order of the outflow segment in charts and tables. |

### LCRS_AGG_VW_TREND_90DAY

REP_AGG_V001 · VIEW — 90-day LCR historical trend with 7-day and 30-day moving averages. Used for volatility analysis, compliance tracking, and notebook queries about historical performance. Includes day-over-day change calculations and breach detection for management escalation.

| Column | Type | Description |
|---|---|---|
| REPORTING_DATE | DATE | Reporting (cut-off) date of the LCR figures (daily COB snapshot). |
| LCR_RATIO_PCT | NUMBER | Liquidity Coverage Ratio in percent (HQLA / net cash outflows x 100). |
| COMPLIANCE_STATUS | TEXT | Status band: FAIL (<100%), WARNING (100% to <110%), PASS (>=110%). |
| STATUS_COLOR | TEXT | Dashboard colour of the status: RED, YELLOW, GREEN. |
| HQLA_TOTAL_CHF | NUMBER | Stock of HQLA in CHF after the L2B and L2 caps (LCR numerator). |
| OUTFLOW_TOTAL_CHF | NUMBER | Total gross stressed 30-day outflows in CHF incl. facility drawdowns. |
| LIQUIDITY_BUFFER_CHF | NUMBER | Liquidity buffer in CHF = HQLA - net cash outflows. |
| MA7_LCR_RATIO | NUMBER | 7-day moving average of the LCR in percent. |
| MA30_LCR_RATIO | NUMBER | 30-day moving average of the LCR in percent. |
| DAY_OVER_DAY_CHANGE_PCT | NUMBER | Day-over-day LCR change in percentage points. |
| DAY_OVER_DAY_CHANGE_PERCENT | NUMBER | Day-over-day LCR change relative to the prior LCR in percent. |
| VOLATILITY_RATING | TEXT | Daily volatility rating of the LCR change (Stable, Moderate, High, Very High). |
| IS_BREACH | BOOLEAN | TRUE when the LCR is below the 100% regulatory minimum. |
| IS_WARNING | BOOLEAN | TRUE when the LCR is between 100% and 110% (internal warning band). |
| CONSECUTIVE_BREACH_DAYS_3DAY | NUMBER | Number of days below 100% within the last 3 days. |
| TREND_DIRECTION | TEXT | Direction of the day-over-day move: Improving, Declining or Stable. |
| DISTANCE_FROM_MINIMUM_PCT | NUMBER | LCR minus the 100% regulatory minimum in percentage points. |
| DISTANCE_FROM_TARGET_PCT | NUMBER | LCR minus the 110% internal PASS threshold in percentage points. |
| LAST_CALCULATED_UTC | TIMESTAMP_NTZ | UTC timestamp of the last LCR calculation (dynamic table refresh). |
| IS_40PCT_CAP_APPLIED | BOOLEAN | TRUE when the L2B 15% or L2 40% cap reduced the HQLA stock. |

### REPP_AGG_DT_LCR_CUTOFF_DELTAS

REP_AGG_V001 · DYNAMIC TABLE — Snapshot comparison of LCR components between cut-off dates (REQ-ANL-01): day-over-day, week-over-week and month-over-month deltas of HQLA, outflows and inflows per asset class, plus a TOTAL row with the LCR change. Calendar-day comparisons (data exists for every calendar day).

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Current cut-off date (T). |
| COMPARISON_TYPE | TEXT | Comparison window: DOD (T vs T-1), WOW (T vs T-7), MOM (T vs T-30) (REQ-ANL-01). |
| PRIOR_DATE | DATE | Prior cut-off date compared against. |
| STRESS_SCENARIO | TEXT | Scenario code: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS or COMBINED. |
| ASSET_CLASS | TEXT | LCR asset class compared; TOTAL for the whole-bank row carrying the LCR ratio. |
| HQLA_CHF | NUMBER | Post-haircut HQLA at T (pre-cap for asset-class rows, capped for TOTAL). |
| HQLA_PRIOR_CHF | NUMBER | Post-haircut HQLA at the prior date. |
| HQLA_DELTA_CHF | NUMBER | Change in HQLA (T - prior). |
| OUTFLOW_CHF | NUMBER | Stressed outflows at T. |
| OUTFLOW_PRIOR_CHF | NUMBER | Stressed outflows at the prior date. |
| OUTFLOW_DELTA_CHF | NUMBER | Change in stressed outflows (T - prior). |
| INFLOW_CHF | NUMBER | Weighted inflows at T (capped for TOTAL). |
| INFLOW_PRIOR_CHF | NUMBER | Weighted inflows at the prior date. |
| INFLOW_DELTA_CHF | NUMBER | Change in weighted inflows (T - prior). |
| LCR_RATIO | NUMBER | LCR at T in percent (TOTAL rows only). |
| LCR_PRIOR | NUMBER | LCR at the prior date in percent (TOTAL rows only). |
| LCR_DELTA_PP | NUMBER | LCR change in percentage points (TOTAL rows only). |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |

### REPP_AGG_DT_LCR_DAILY

REP_AGG_V001 · DYNAMIC TABLE — Daily Liquidity Coverage Ratio (LCR) calculation per FINMA Circular 2015/2. Combines HQLA stock and 30-day net cash outflows to calculate the primary Basel III liquidity metric. Built from the BASE rows of REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC incl. L2B 15% cap, L2 40% cap and 75% inflow cap). Determines status FAIL (<100%), WARNING (100-110%), PASS (>=110%) and the liquidity buffer. Critical metric monitored daily by Treasury, reported monthly to Swiss National Bank (SNB), and used for strategic liquidity planning. Regulatory minimum 100% must be maintained at all times. Updated hourly for real-time compliance monitoring and management escalation.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting date for LCR calculation (daily COB snapshot). Primary time dimension for regulatory reporting to SNB and intraday liquidity monitoring. Used for time-series compliance tracking and historical breach analysis. |
| BANK_ID | TEXT | Bank identifier for regulatory reporting (SYNTH-CH-999). Used for multi-entity consolidation in group structures and SNB submission identification. Static identifier for this synthetic bank. |
| L1_TOTAL | NUMBER | Total Level 1 HQLA value in CHF (0% haircut). Highest quality liquid assets including SNB reserves, cash, and government bonds rated AA- or higher. Core component of liquidity buffer providing maximum regulatory benefit. |
| L2A_TOTAL | NUMBER | Total Level 2A HQLA value in CHF (15% haircut). Swiss canton bonds and covered bonds. High-quality assets with minor liquidity discount. Used for yield optimization while maintaining strong LCR contribution. |
| L2B_TOTAL | NUMBER | Total Level 2B HQLA value in CHF (50% haircut). SMI equities and AA- corporate bonds. Lower-quality liquid assets with significant haircut. Subject to 40% cap rule and rebalancing constraints. |
| L2_UNCAPPED | NUMBER | Total Level 2 assets before 40% cap enforcement (L2A + L2B). Used for cap proximity monitoring and pre-emptive portfolio rebalancing decisions. Gap between uncapped and capped indicates regulatory constraint. |
| L2_CAPPED | NUMBER | Total Level 2 assets after 40% cap rule (max 2/3 of L1). Final L2 value included in HQLA numerator. When cap applied, excess L2 is discarded and provides no LCR benefit. |
| HQLA_TOTAL | NUMBER | Final total HQLA stock in CHF (L1 + L2_capped). The numerator in LCR formula: LCR = (HQLA / Outflows) × 100. Must maintain minimum CHF 2B for systemic banks. Board-level metric for liquidity risk appetite. |
| CAP_APPLIED | BOOLEAN | Flag indicating 40% Basel III cap was triggered (TRUE = L2 exceeded 2/3 of L1). When TRUE, triggers Treasury alert to rebalance portfolio toward Level 1 assets. Monitored daily by ALM. |
| DISCARDED_L2 | NUMBER | CHF amount of Level 2 assets excluded due to cap breach. Represents opportunity cost - unutilized liquidity buffer. High values trigger strategic review to optimize portfolio composition. |
| TOTAL_HOLDINGS | NUMBER | Total number of HQLA securities across all levels. Portfolio complexity metric for operational management and custody arrangements. Used for diversification analysis and concentration risk assessment. |
| OUTFLOW_RETAIL | NUMBER | Total expected 30-day retail deposit outflows in CHF (3-10% base rates with discounts). Most stable funding source due to deposit insurance and relationship banking. Critical for retail funding strategy. |
| OUTFLOW_CORP | NUMBER | Total expected 30-day corporate deposit outflows in CHF (25-40% rates). Operational vs non-operational designation drives run-off assumptions. Used for commercial banking funding strategy. |
| OUTFLOW_FI | NUMBER | Total expected 30-day financial institution deposit outflows in CHF (100% assumption). Most volatile wholesale funding requiring immediate liquidity coverage. Monitored for counterparty concentration limits. |
| OUTFLOW_TOTAL | NUMBER | Total gross stressed 30-day outflows in CHF incl. facility drawdowns. The LCR denominator is NET_OUTFLOW (outflows minus capped inflows). |
| TOTAL_DEPOSIT_ACCOUNTS | NUMBER | Total number of active deposit accounts across all counterparty types. Deposit base breadth metric for diversification assessment and operational complexity monitoring. |
| LCR_RATIO | NUMBER | Liquidity Coverage Ratio: (HQLA_Total / Net_Outflow) × 100. Primary Basel III liquidity metric. Regulatory minimum 100% per FINMA Circular 2015/2. Values over 9000% indicate no stressed outflows (exceptional case). |
| LCR_STATUS | TEXT | Regulatory compliance status: FAIL (<100%), WARNING (100% to <110% internal buffer), PASS (≥110%). Used for automated alert generation, management escalation, and FINMA breach reporting. |
| SEVERITY | TEXT | Color-coded severity level for dashboards: RED (<100%), YELLOW (100-110%), GREEN (≥110%). Visual indicator for Treasury operations and executive dashboards. |
| LCR_BUFFER_CHF | NUMBER | Absolute liquidity buffer in CHF (HQLA_Total - Net_Outflow). Excess HQLA beyond minimum regulatory requirement. Positive buffer provides cushion for market stress; negative indicates breach requiring immediate action. |
| LCR_BUFFER_PCT | NUMBER | Liquidity buffer as percentage of net outflows ((HQLA - Net_Outflow) / Net_Outflow × 100). Indicates buffer size relative to stressed outflows. Target typically 20-50% buffer for operational flexibility and stress resilience. |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp when LCR calculation was executed. Used for data lineage, audit trail, and SLA monitoring. Critical for validating calculation freshness (must be within 60 minutes for intraday reporting). |
| OUTFLOW_FACILITIES | NUMBER | Drawdown outflows on undrawn committed credit and liquidity facilities (5% retail, 10% corporate credit, 30% corporate liquidity, 40% banks). |
| INFLOW_TOTAL | NUMBER | Weighted 30-day contractual inflows before the cap (50% non-financial, 100% financial, 0% non-performing). |
| INFLOW_CAPPED | NUMBER | Inflows after the 75% cap: min(INFLOW_TOTAL, 0.75 x OUTFLOW_TOTAL). |
| NET_OUTFLOW | NUMBER | Net cash outflows = OUTFLOW_TOTAL - INFLOW_CAPPED. LCR denominator. |
| L2B_CAP_APPLIED | BOOLEAN | TRUE when the 15% Level 2B cap (15/85 of L1 + L2A) reduced the HQLA stock. |
| INFLOW_CAP_APPLIED | BOOLEAN | TRUE when the 75% inflow cap was binding. |

### REPP_AGG_DT_LCR_EXCEPTIONS

REP_AGG_V001 · DYNAMIC TABLE — Quarantine of invalid LCR source records (REQ-DQ-01, spec 3.2). Records that fail mapping, FX or date validation are excluded from all LCR calculations and listed here with an error code and explanation, so the run never fails and stewards can remediate (TC-VAL-03).

| Column | Type | Description |
|---|---|---|
| SNAPSHOT_DATE | DATE | Snapshot date on which the invalid record was received (spec 3.2 LIQUIDITY_EXCEPTIONS). |
| CONTRACT_ID | TEXT | Quarantined position key (contract id, account id or holding id). |
| SOURCE_SYSTEM | TEXT | Source feed of the invalid record: HOLDINGS, DEPOSITS or CONTRACTS. |
| PRODUCT_TYPE | TEXT | Source product code of the invalid record. |
| COUNTERPARTY_ID | TEXT | Counterparty of the invalid record. |
| CURRENCY | TEXT | Currency of the invalid record. |
| AMOUNT_CCY | NUMBER | Exposure in original currency excluded from the LCR because of the error. |
| ERROR_CODE | TEXT | Error code: UNMAPPED_ASSET_CLASS, MISSING_FX, INVALID_DATE or MISSING_RULE. |
| ERROR_DETAIL | TEXT | Human-readable explanation used by data stewards to remediate the record. |
| LOGGED_AT | TIMESTAMP_NTZ | UTC timestamp when the exception was logged (dynamic table refresh time). |

### REPP_AGG_DT_LCR_FX_DAILY

REP_AGG_V001 · DYNAMIC TABLE — Daily end-of-day CCY->CHF conversion rates derived from REFI_RAW_TB_FX_RATES (USD-based quotes crossed via USD). One row per calendar date and currency so the LCR engine converts all positions with a plain equality join (REQ-ING-03). Currencies without a rate on a date are absent and surface as MISSING_FX exceptions.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Calendar date of the end-of-day FX rate (one row per calendar day incl. weekends, which carry the last business-day close). Joined by equality to the AS_OF_DATE of LCR snapshots. |
| CURRENCY | TEXT | ISO 4217 currency converted to CHF (CHF itself has rate 1). |
| RATE_TO_CHF | NUMBER | End-of-day conversion rate: 1 unit of CURRENCY = RATE_TO_CHF CHF. Cross rate via USD: mid(USD->CHF) / mid(USD->CURRENCY). |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh, used for lineage and freshness monitoring. |

### REPP_AGG_DT_LCR_HQLA

REP_AGG_V001 · DYNAMIC TABLE — Daily regulatory (BASE scenario) HQLA holdings aggregated by HQLA level and asset type: holding counts, gross market value, haircut and post-haircut value. Built from REPP_AGG_DT_LCR_SCENARIO_POSITIONS so the composition always matches the LCR numerator. Used by semantic views, composition dashboards and the liquidity agent.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting date for HQLA positions (daily COB snapshot). Primary time dimension for portfolio composition and asset allocation trends. |
| HQLA_LEVEL | TEXT | Basel III HQLA level: L1 (0% haircut), L2A (15% haircut), L2B (50% haircut). Primary grouping for regulatory reporting and cap monitoring. |
| ASSET_TYPE | TEXT | HQLA asset type code (e.g. CASH_SNB, GOVT_BOND_CHF, CANTON_BOND, CORPORATE_BOND_AA, CORPORATE_BOND_A_BBB, EQUITY_SMI). Links to LIQI_RAW_TB_HQLA_ELIGIBILITY. |
| HOLDINGS_COUNT | NUMBER | Number of eligible positions of this asset type on the date; diversification / concentration indicator. |
| MARKET_VALUE_CHF | NUMBER | Gross market value in CHF (end-of-day FX) before regulatory haircuts. |
| HAIRCUT_PCT | NUMBER | Regulatory haircut percentage of the asset type (0, 15 or 50) from LIQI_RAW_TB_HQLA_ELIGIBILITY. |
| HQLA_VALUE_CHF | NUMBER | HQLA value after the regulatory haircut (Market_Value x Haircut_Factor), before the L2B and L2 caps. |
| PCT_OF_TOTAL_HQLA | NUMBER | Share of this asset type in total post-haircut (pre-cap) HQLA in percent; concentration indicator. |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh for lineage and freshness monitoring. |

### REPP_AGG_DT_LCR_HQLA_CALCULATION

REP_AGG_V001 · DYNAMIC TABLE — Daily regulatory (BASE) HQLA stock with both Basel III / FINMA caps: L2B limited to 15/85 of (L1 + L2A) and L2 limited to 2/3 of L1. LCR numerator. Built from REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC).

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting date (daily COB snapshot) of the HQLA stock calculation. |
| L1_TOTAL | NUMBER | Level 1 HQLA in CHF (0% haircut): SNB reserves, vault cash, Swiss Confederation and 0%-risk-weight foreign government bonds. |
| L2A_TOTAL | NUMBER | Level 2A HQLA in CHF after 15% haircut: canton bonds, covered bonds (Pfandbriefe), AA- or better corporate bonds. |
| L2B_TOTAL | NUMBER | Level 2B HQLA in CHF after 50% haircut, before the 15% cap: SMI equities and A+/BBB- corporate bonds. |
| L2_UNCAPPED | NUMBER | L2A + L2B before any regulatory cap. |
| L2_CAPPED | NUMBER | Level 2 included in HQLA after the L2B 15% cap and the L2 40% cap (max 2/3 of L1). |
| HQLA_TOTAL | NUMBER | Stock of HQLA = L1_TOTAL + L2_CAPPED (LCR numerator). |
| L1_COUNT | NUMBER | Number of Level 1 positions. |
| L2A_COUNT | NUMBER | Number of Level 2A positions. |
| L2B_COUNT | NUMBER | Number of Level 2B positions. |
| TOTAL_HOLDINGS | NUMBER | Total number of eligible HQLA positions. |
| CAP_APPLIED | BOOLEAN | TRUE when either the L2B 15% cap or the L2 40% cap reduced the HQLA stock. |
| DISCARDED_L2 | NUMBER | Level 2 assets excluded by the caps (L2_UNCAPPED - L2_CAPPED). |
| MAX_L2_ALLOWED | NUMBER | Maximum Level 2 allowed under the 40% cap: 2/3 x L1_TOTAL. |
| L2_TO_L1_PCT | NUMBER | L2_CAPPED / L1_TOTAL in percent (regulatory maximum 66.67%). |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |
| L2B_CAPPED | NUMBER | Level 2B after the 15% cap: min(L2B, 15/85 x (L1 + L2A)). |
| L2B_CAP_APPLIED | BOOLEAN | TRUE when the 15% L2B cap was binding. |
| L2B_DISCARDED | NUMBER | Level 2B excluded by the 15% cap (L2B_TOTAL - L2B_CAPPED). |

### REPP_AGG_DT_LCR_LIQUIDITY_GAP

REP_AGG_V001 · DYNAMIC TABLE — Maturity ladder / liquidity gap per date, scenario and standard bucket (REQ-ENG-01, REQ-ENG-02): contractual cashflows from LIQI_RAW_TB_CASHFLOWS plus behavioral decay of non-maturity deposits (linear over the scenario decay horizon), with counterbalancing HQLA and cumulative gap. Feeds the gap chart and the behavioral heatmap.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Snapshot (cut-off) date of the maturity ladder. |
| STRESS_SCENARIO | TEXT | Scenario whose behavioral decay profile is applied to non-maturity deposits. |
| BUCKET_CODE | TEXT | Maturity bucket code (ON, D2_7, D8_30, M1_3, M3_6, M6_12, Y1P). |
| BUCKET_NAME | TEXT | Display name of the maturity bucket. |
| SORT_ORDER | NUMBER | Display order of the bucket. |
| IN_LCR_HORIZON | BOOLEAN | TRUE for buckets within the 30-day LCR horizon. |
| CONTRACTUAL_INFLOW_CHF | NUMBER | Contractual cash inflows (principal + interest) of valid contracts falling into the bucket, in CHF (unweighted). |
| CONTRACTUAL_OUTFLOW_CHF | NUMBER | Contractual cash outflows (interbank borrowing repayments and interest) falling into the bucket, in CHF. |
| BEHAVIORAL_OUTFLOW_CHF | NUMBER | Behavioral run-down of non-maturity deposits allocated to the bucket: balance spread linearly over DECAY_PROFILE_DAYS of the scenario rule (REQ-ENG-02). |
| COUNTERBALANCING_CHF | NUMBER | Post-haircut HQLA (pre-cap) available as counterbalancing capacity, shown in the overnight bucket only. |
| NET_GAP_CHF | NUMBER | Bucket gap = contractual inflows - contractual outflows - behavioral outflows. |
| CUMULATIVE_GAP_CHF | NUMBER | Running sum of NET_GAP_CHF plus counterbalancing capacity from the overnight bucket onward; negative values indicate a funding shortfall at that horizon. |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |

### REPP_AGG_DT_LCR_OUTFLOW

REP_AGG_V001 · DYNAMIC TABLE — Daily regulatory (BASE scenario) stressed outflows by segment: retail, corporate, financial institutions (deposits + interbank borrowing) and committed facilities. Gross amounts, effective run-off rates, outflows and shares. Built from REPP_AGG_DT_LCR_SCENARIO_POSITIONS so totals equal OUTFLOW_TOTAL of REPP_AGG_DT_LCR_DAILY.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting date (daily COB snapshot). Primary time dimension for funding composition and run-off trends. |
| COUNTERPARTY_TYPE | TEXT | Outflow segment: RETAIL, CORPORATE (incl. SME), FINANCIAL_INSTITUTION (FI deposits and interbank borrowing) or COMMITTED_FACILITIES (undrawn credit / liquidity facility drawdowns). |
| ACCOUNT_COUNT | NUMBER | Number of positions (deposit accounts, borrowings or facilities) in the segment. |
| CUSTOMER_COUNT | NUMBER | Number of distinct counterparties in the segment. |
| BALANCE_CHF | NUMBER | Gross amount the run-off applies to in CHF: deposit balances, 30-day borrowing repayments or undrawn facility amounts. |
| RUN_OFF_RATE | NUMBER | Weighted effective run-off / drawdown rate (decimal) = OUTFLOW_AMOUNT_CHF / BALANCE_CHF, after account-level adjustments. |
| OUTFLOW_AMOUNT_CHF | NUMBER | Stressed 30-day outflow of the segment in CHF (regulatory BASE scenario). |
| PCT_OF_TOTAL_OUTFLOWS | NUMBER | Share of the segment in total gross outflows in percent. |
| PCT_OF_TOTAL_DEPOSITS | NUMBER | Share of the segment in the total gross amount (BALANCE_CHF) in percent. |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |

### REPP_AGG_DT_LCR_OUTFLOW_CALCULATION

REP_AGG_V001 · DYNAMIC TABLE — Daily regulatory (BASE) net cash outflow calculation: stressed outflows by segment incl. facility drawdowns, weighted contractual inflows with the 75% cap, and net outflows (LCR denominator). Built from REPP_AGG_DT_LCR_SCENARIO_DAILY (shared formula REPP_AGG_FN_LCR_CALC).

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting date (daily COB snapshot) of the net cash outflow calculation. |
| OUTFLOW_RETAIL | NUMBER | Stressed 30-day retail deposit outflows (3-10% base rates with relationship discounts and tenure penalty). |
| OUTFLOW_CORP | NUMBER | Stressed 30-day corporate / SME deposit outflows (25% operational, 40% non-operational). |
| OUTFLOW_FI | NUMBER | Stressed 30-day outflows from FI deposits and interbank borrowing maturing within 30 days (100%). |
| OUTFLOW_TOTAL | NUMBER | Total gross stressed 30-day outflows incl. facility drawdowns (OUTFLOW_RETAIL + OUTFLOW_CORP + OUTFLOW_FI + OUTFLOW_FACILITIES). The LCR denominator is NET_OUTFLOW. |
| RETAIL_ACCOUNTS | NUMBER | Number of active retail deposit accounts. |
| CORP_ACCOUNTS | NUMBER | Number of active corporate / SME deposit accounts. |
| FI_ACCOUNTS | NUMBER | Number of FI deposit and interbank borrowing positions. |
| TOTAL_ACCOUNTS | NUMBER | Total number of deposit and borrowing positions. |
| RETAIL_EFFECTIVE_RUN_OFF_PCT | NUMBER | Effective retail run-off rate in percent (OUTFLOW_RETAIL / retail balance). |
| CORP_EFFECTIVE_RUN_OFF_PCT | NUMBER | Effective corporate run-off rate in percent (OUTFLOW_CORP / corporate balance). |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |
| OUTFLOW_FACILITIES | NUMBER | Drawdown outflows on undrawn committed credit and liquidity facilities. |
| INFLOW_TOTAL | NUMBER | Weighted 30-day contractual inflows before the 75% cap (50% non-financial, 100% financial, 0% non-performing). |
| INFLOW_CAPPED | NUMBER | Inflows after the 75% cap: min(INFLOW_TOTAL, 0.75 x OUTFLOW_TOTAL). |
| NET_OUTFLOW | NUMBER | Net cash outflows = OUTFLOW_TOTAL - INFLOW_CAPPED (LCR denominator). |

### REPP_AGG_DT_LCR_POSITIONS

REP_AGG_V001 · DYNAMIC TABLE — Unified, CHF-converted position layer of the LCR engine: HQLA holdings, deposits and contracts in one grain (AS_OF_DATE x POSITION_ID) enriched with counterparty, product mapping (REQ-MAP-01), FX (REQ-ING-03) and data-quality validation (REQ-DQ-01). All downstream scenario, attribution and drill-down objects read from here; only DQ_STATUS = OK and INCLUDED_IN_LCR rows enter the LCR.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Snapshot (cut-off) date of the position (daily COB). |
| POSITION_ID | TEXT | Atomic position key: HOLDING_ID, deposit ACCOUNT_ID or CONTRACT_ID. Lowest level of the drill-down and driver key in variance attribution. |
| SOURCE_SYSTEM | TEXT | Source feed of the position: HOLDINGS, DEPOSITS or CONTRACTS. |
| PRODUCT_TYPE | TEXT | Source product code (asset type, deposit type or contract product type). |
| POSITION_NAME | TEXT | Display name of the position (security name, account id or contract id). |
| COUNTERPARTY_ID | TEXT | Counterparty / issuer of the position (FK to LIQI_RAW_TB_COUNTERPARTIES). |
| COUNTERPARTY_NAME | TEXT | Counterparty name from the counterparty master (falls back to the id). |
| COUNTERPARTY_SECTOR | TEXT | Rule sector used to look up behavioral rules: deposit COUNTERPARTY_TYPE, contract COUNTERPARTY_SECTOR, or ALL for HQLA. |
| BUSINESS_UNIT | TEXT | Owning business unit (RETAIL, PRIVATE_BANKING, CORPORATE, TREASURY) for filtering (REQ-UI-02). |
| CURRENCY | TEXT | Original currency of the position (ISO 4217). |
| RATING | TEXT | Rating of the instrument or counterparty (S&P scale) for rating filters. |
| ASSET_CLASS | TEXT | Level-1 LCR taxonomy from LIQI_RAW_TB_PRODUCT_MAPPING (e.g. HQLA_L2A, RETAIL_DEPOSITS, LOANS). NULL when unmapped. |
| PRODUCT_GROUP | TEXT | Level-2 taxonomy (product group) from the product mapping. |
| LCR_TREATMENT | TEXT | How the position enters the LCR: HQLA, OUTFLOW, INFLOW or FACILITY. |
| HQLA_LEVEL | TEXT | HQLA level L1 / L2A / L2B for HQLA positions (from LIQI_RAW_TB_HQLA_ELIGIBILITY). |
| AMOUNT_CCY | NUMBER | Exposure in original currency: market value (holdings), balance (deposits) or drawn outstanding (contracts). |
| FX_RATE_TO_CHF | NUMBER | End-of-day CCY->CHF rate of AS_OF_DATE (equality join on REPP_AGG_DT_LCR_FX_DAILY). NULL means missing FX. |
| AMOUNT_CHF | NUMBER | Exposure converted to CHF (AMOUNT_CCY x FX_RATE_TO_CHF). |
| UNDRAWN_CHF | NUMBER | Undrawn committed facility amount in CHF (contracts only, basis for drawdown outflows). |
| START_DATE | DATE | Contract start / last roll-over date (contracts only). |
| MATURITY_DATE | DATE | Contractual maturity of the position (bonds, contracts). |
| HAIRCUT_FACTOR | NUMBER | Regulatory haircut multiplier for HQLA (1.00 L1, 0.85 L2A, 0.50 L2B). |
| RUNOFF_ADJUSTMENT | NUMBER | Account-level run-off adjustment for deposits: -2pp for 3+ products and -1pp for direct debit (where the deposit type allows relationship discounts), +5pp for tenure under 18 months. |
| IS_PERFORMING | BOOLEAN | Performing flag (contracts); non-performing exposures get 0% inflow. |
| INCLUDED_IN_LCR | BOOLEAN | TRUE when the position is in scope of the LCR: HQLA-eligible holdings, ACTIVE deposit accounts, all contracts. |
| ERROR_CODE | TEXT | Data-quality error: UNMAPPED_ASSET_CLASS, MISSING_FX, INVALID_DATE or MISSING_RULE. NULL when the position is valid. |
| ERROR_DETAIL | TEXT | Human-readable explanation of the data-quality error. |
| DQ_STATUS | TEXT | OK = position used in calculations; EXCEPTION = quarantined to REPP_AGG_DT_LCR_EXCEPTIONS (REQ-DQ-01). |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |

### REPP_AGG_DT_LCR_SCENARIO_COMPONENTS

REP_AGG_V001 · DYNAMIC TABLE — Compact LCR component vectors per date and scenario (a few dozen rows per day). Input for REPP_AGG_DT_LCR_SCENARIO_DAILY, the scenario simulator procedure and the agent what-if tool, so what-if recalculations run in milliseconds without touching position data.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Snapshot (cut-off) date. |
| STRESS_SCENARIO | TEXT | Scenario code (BASE, BANK_STRESS, MARKET_STRESS, COMBINED). |
| COMPONENT_TYPE | TEXT | LCR side of the component: HQLA (numerator), OUTFLOW (incl. facility drawdowns) or INFLOW. |
| SIM_CATEGORY | TEXT | Simulator component category (e.g. L1_CASH, L2B_EQUITY, RETAIL_STABLE, FI_FUNDING, FACILITIES, INFLOW_FI). |
| HQLA_LEVEL | TEXT | HQLA level for HQLA components (L1, L2A, L2B); NULL for flows. |
| GROSS_AMOUNT_CHF | NUMBER | Sum of gross amounts the scenario factors apply to (market value, balance, undrawn, 30-day contractual flow). |
| WEIGHTED_AMOUNT_CHF | NUMBER | Sum of weighted amounts (post-haircut HQLA, stressed outflow or weighted inflow) before LCR caps. |
| POSITION_COUNT | NUMBER | Number of positions in the component. |
| COUNTERPARTY_COUNT | NUMBER | Number of distinct counterparties in the component. |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |

### REPP_AGG_DT_LCR_SCENARIO_DAILY

REP_AGG_V001 · DYNAMIC TABLE — Daily LCR result per scenario (REQ-ENG-05): HQLA by level with both regulatory caps, outflows by segment, inflows with the 75% cap, net outflows, LCR ratio and status (FAIL < 100%, WARNING 100-110%, PASS >= 110%). All values computed with the shared REPP_AGG_FN_LCR_CALC formula. BASE rows feed the regulatory LCR dynamic tables.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Snapshot (cut-off) date. |
| STRESS_SCENARIO | TEXT | Scenario code; BASE is the regulatory FINMA LCR. |
| SCENARIO_NAME | TEXT | Scenario display name. |
| IS_REGULATORY | BOOLEAN | TRUE for the regulatory BASE scenario. |
| L1_TOTAL | NUMBER | Level 1 HQLA after haircut (0% regulatory; stress add-ons in stress scenarios). |
| L2A_TOTAL | NUMBER | Level 2A HQLA after 15% haircut (plus stress add-on) before caps. |
| L2B_TOTAL | NUMBER | Level 2B HQLA after 50% haircut (plus stress add-on) before caps. |
| L2B_CAPPED | NUMBER | Level 2B after the 15% cap: min(L2B, 15/85 x (L1 + L2A)). |
| L2_UNCAPPED | NUMBER | L2A + L2B before any cap. |
| L2_CAPPED | NUMBER | Level 2 after the 40% cap: min(L2A + L2B_CAPPED, 2/3 x L1). |
| HQLA_TOTAL | NUMBER | Stock of HQLA = L1 + L2_CAPPED (LCR numerator). |
| L2B_CAP_APPLIED | BOOLEAN | TRUE when the 15% L2B cap reduced the stock. |
| L2_CAP_APPLIED | BOOLEAN | TRUE when the 40% L2 cap reduced the stock. |
| OUTFLOW_RETAIL | NUMBER | Stressed 30-day outflows from retail deposits. |
| OUTFLOW_CORP | NUMBER | Stressed 30-day outflows from corporate / SME deposits. |
| OUTFLOW_FI | NUMBER | Stressed 30-day outflows from financial-institution deposits and interbank borrowing. |
| OUTFLOW_FACILITIES | NUMBER | Drawdown outflows on undrawn committed credit and liquidity facilities. |
| OUTFLOW_TOTAL | NUMBER | Total gross stressed 30-day outflows (retail + corporate + FI + facilities). |
| INFLOW_NONFIN | NUMBER | Weighted 30-day inflows from retail, SME and corporate loans. |
| INFLOW_FI | NUMBER | Weighted 30-day inflows from interbank placements. |
| INFLOW_TOTAL | NUMBER | Total weighted 30-day inflows before the cap. |
| INFLOW_CAPPED | NUMBER | Inflows after the 75% cap: min(INFLOW_TOTAL, 0.75 x OUTFLOW_TOTAL). |
| INFLOW_CAP_APPLIED | BOOLEAN | TRUE when the 75% inflow cap was binding. |
| NET_OUTFLOW | NUMBER | Net cash outflows = OUTFLOW_TOTAL - INFLOW_CAPPED (LCR denominator). |
| LCR_RATIO | NUMBER | LCR in percent = HQLA_TOTAL / NET_OUTFLOW x 100 (9999.99 if no net outflows). |
| LCR_STATUS | TEXT | FAIL (< 100%), WARNING (100% to < 110%), PASS (>= 110%). |
| SEVERITY | TEXT | Dashboard colour for LCR_STATUS: RED, YELLOW, GREEN. |
| LCR_BUFFER_CHF | NUMBER | HQLA_TOTAL - NET_OUTFLOW: liquidity buffer above the 100% minimum. |
| RETAIL_ACCOUNTS | NUMBER | Number of retail deposit positions in the calculation. |
| CORP_ACCOUNTS | NUMBER | Number of corporate / SME deposit positions in the calculation. |
| FI_ACCOUNTS | NUMBER | Number of FI deposit and interbank borrowing positions in the calculation. |
| L1_COUNT | NUMBER | Number of Level 1 HQLA positions. |
| L2A_COUNT | NUMBER | Number of Level 2A HQLA positions. |
| L2B_COUNT | NUMBER | Number of Level 2B HQLA positions. |
| RETAIL_BALANCE_CHF | NUMBER | Gross retail deposit balance (basis for the effective retail run-off rate). |
| CORP_BALANCE_CHF | NUMBER | Gross corporate deposit balance (basis for the effective corporate run-off rate). |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |

### REPP_AGG_DT_LCR_SCENARIO_POSITIONS

REP_AGG_V001 · DYNAMIC TABLE — Position-level LCR contributions for every scenario (REQ-ENG-03/04/05): each valid in-scope position from REPP_AGG_DT_LCR_POSITIONS is combined with the scenario-specific behavioral rule (run-off, inflow, drawdown, haircut add-on) to produce weighted HQLA, outflow and inflow amounts. Single source for scenario components, variance attribution, drill-down and the simulator.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Snapshot (cut-off) date of the position (daily COB). |
| STRESS_SCENARIO | TEXT | Scenario the position is evaluated in: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS, COMBINED (REQ-ENG-05). |
| POSITION_ID | TEXT | Atomic position key (holding id, deposit account id or contract id); driver key for variance attribution and drill-down. |
| SOURCE_SYSTEM | TEXT | Source feed of the position: HOLDINGS, DEPOSITS or CONTRACTS. |
| PRODUCT_TYPE | TEXT | Source product code (asset type, deposit type or contract product type). |
| POSITION_NAME | TEXT | Display name of the position (security name, account id or contract id). |
| PRODUCT_GROUP | TEXT | Level-2 taxonomy (product group) from LIQI_RAW_TB_PRODUCT_MAPPING. |
| ASSET_CLASS | TEXT | Level-1 LCR taxonomy (e.g. HQLA_L1_CASH, RETAIL_DEPOSITS, LOANS) from the product mapping. |
| LCR_TREATMENT | TEXT | How the position enters the LCR: HQLA, OUTFLOW, INFLOW or FACILITY. |
| HQLA_LEVEL | TEXT | HQLA level L1 / L2A / L2B for HQLA positions; NULL otherwise. |
| COUNTERPARTY_ID | TEXT | Counterparty / issuer of the position. |
| COUNTERPARTY_NAME | TEXT | Counterparty name shown in drill-downs and Top-10 driver lists. |
| COUNTERPARTY_SECTOR | TEXT | Rule sector used for the behavioral rule lookup (deposit counterparty type, contract sector, ALL for HQLA). |
| BUSINESS_UNIT | TEXT | Owning business unit for filtering (REQ-UI-02). |
| CURRENCY | TEXT | Original currency of the position (ISO 4217). |
| RATING | TEXT | Instrument / counterparty rating for rating filters. |
| SIM_CATEGORY | TEXT | Simulator component the position belongs to (e.g. RETAIL_STABLE, CORP_NON_OPERATIONAL, FI_FUNDING, FACILITIES, INFLOW_NONFIN, INFLOW_FI, L1_CASH, L2A). Target of what-if multipliers. |
| EXPOSURE_CHF | NUMBER | Exposure in CHF: market value (HQLA), balance (deposits) or drawn outstanding (contracts). |
| UNDRAWN_CHF | NUMBER | Undrawn committed facility amount in CHF (basis of drawdown outflows). |
| CF_IN_30D_CHF | NUMBER | Contractual cash inflows (principal + interest) due within 30 days in CHF. |
| CF_OUT_30D_CHF | NUMBER | Contractual cash outflows (e.g. interbank borrowing repayments) due within 30 days in CHF. |
| COMPONENT_GROSS_CHF | NUMBER | Gross amount the scenario factor is applied to: market value (HQLA), balance (deposits), 30-day outflow (interbank borrowing), undrawn amount (facilities), 30-day performing inflow (loans). |
| APPLIED_FACTOR | NUMBER | Scenario factor applied to COMPONENT_GROSS_CHF: haircut multiplier (HQLA, incl. stress add-on), run-off rate (deposits incl. account adjustments, clamped to 3%-100%), drawdown rate (facilities) or inflow rate (0 for non-performing). |
| HQLA_WEIGHTED_CHF | NUMBER | Post-haircut HQLA value in CHF before the L2 caps (numerator contribution). |
| OUTFLOW_CHF | NUMBER | Stressed 30-day outflow in CHF (denominator contribution). |
| INFLOW_CHF | NUMBER | Weighted 30-day inflow in CHF before the 75% inflow cap. |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the dynamic table refresh. |

### REPP_AGG_DT_LCR_TREND

REP_AGG_V001 · DYNAMIC TABLE — Rolling trend analysis of LCR ratio with statistical metrics and automated alert detection. Calculates 7/30/90-day moving averages, volatility measures, day-over-day changes, and consecutive breach tracking. Enables proactive liquidity risk management by identifying adverse trends before regulatory breaches occur. Triggers Treasury alerts for high volatility (>10% daily change), sustained breaches (3+ consecutive days below 100%), and critical breaches (<95%). Used for Board reporting, management dashboards, and early warning system integration.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting date for trend analysis (daily COB snapshot). Time dimension for historical trend visualization, volatility monitoring, and predictive analytics. Used for identifying adverse liquidity patterns before breaches occur. |
| LCR_RATIO | NUMBER | Daily LCR ratio snapshot from REPP_AGG_DT_LCR_DAILY. Point-in-time value used for calculating rolling averages, volatility measures, and trend analysis. Primary metric for time-series charting. |
| LCR_7D_AVG | NUMBER | 7-day rolling average LCR ratio. Short-term trend indicator for weekly liquidity patterns and intraweek volatility smoothing. Used for identifying immediate trend reversals and operational issues. |
| LCR_30D_AVG | NUMBER | 30-day rolling average LCR ratio. Medium-term trend indicator for monthly compliance averaging and business cycle patterns. Used for SNB monthly submissions and management reporting. |
| LCR_90D_AVG | NUMBER | 90-day rolling average LCR ratio. Long-term strategic trend indicator for quarterly performance assessment and seasonal pattern identification. Used for Board reporting and strategic liquidity planning. |
| LCR_30D_VOLATILITY | NUMBER | 30-day rolling standard deviation of LCR ratio. Statistical measure of LCR stability and predictability. High volatility (>5) indicates unstable liquidity position requiring investigation. Used for risk appetite monitoring. |
| LCR_30D_MIN | NUMBER | Minimum LCR ratio in past 30 days. Identifies worst-case liquidity stress point within monthly window. Used for stress testing validation and regulatory buffer adequacy assessment. |
| LCR_30D_MAX | NUMBER | Maximum LCR ratio in past 30 days. Identifies peak liquidity position within monthly window. Large min-max range indicates high volatility. Used for capacity planning and buffer optimization. |
| LCR_DOD_CHANGE | NUMBER | Day-over-day LCR ratio change in percentage points. Daily velocity metric for intraday monitoring and sudden movement detection. Absolute changes >10pp trigger high volatility alerts requiring immediate investigation. |
| LCR_STATUS | TEXT | Daily compliance status (PASS/WARNING/FAIL/N/A). Used for consecutive breach tracking and sustained stress identification. Drives automated escalation workflows and management notifications. |
| SEVERITY | TEXT | Color-coded severity level (GREEN/YELLOW/RED/GRAY). Visual indicator for dashboard alerts and operational monitoring. RED severity triggers immediate Treasury escalation and breach protocols. |
| CONSECUTIVE_BREACHES_3D | NUMBER | Count of days below 100% threshold in past 3 days (rolling window). Identifies sustained breach patterns requiring escalated regulatory action. Values ≥3 trigger FINMA notification and remediation plan requirement. |
| HIGH_VOLATILITY_ALERT | BOOLEAN | Flag for excessive daily volatility (\|LCR_DOD_Change\| >10pp). Indicates significant intraday HQLA or deposit movements requiring investigation. Triggers Treasury alert to identify root cause (large withdrawals, asset sales, etc.). |
| SUSTAINED_BREACH_ALERT | BOOLEAN | Flag for persistent compliance failure (3+ consecutive days below 100%). Indicates structural liquidity problem requiring immediate corrective action. Triggers executive escalation, FINMA notification, and remediation plan development. |
| CRITICAL_BREACH_ALERT | BOOLEAN | Flag for severe regulatory breach (LCR <95%). Indicates critical liquidity crisis requiring emergency measures. Triggers Board notification, regulatory reporting, and potential public disclosure depending on severity and duration. |
| CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp when trend analysis was calculated. Used for data lineage, audit trail, and calculation freshness validation. Ensures trend metrics reflect latest daily LCR calculations within 60-minute SLA. |

### REPP_AGG_VW_LCR_ALERTS

REP_AGG_V001 · VIEW — Automated alert generation view for LCR compliance monitoring with structured alert messages and severity classification. Generates real-time alerts for regulatory breaches (LCR <100%), critical breaches (LCR <95%), high volatility (>10% daily change), sustained breaches (3+ consecutive days), and 40% cap violations. Each alert includes severity level (CRITICAL/HIGH/MEDIUM/INFO), alert type, descriptive message, and recommended action. Used by Treasury operations for real-time monitoring, by Compliance for breach documentation, and integrated with notification systems for management escalation. Critical for FINMA reporting obligations and audit trail maintenance.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting (cut-off) date of the record (daily COB snapshot). |
| LCR_RATIO | NUMBER | Liquidity Coverage Ratio in percent (HQLA / net cash outflows x 100). |
| LCR_STATUS | TEXT | Status band: FAIL (<100%), WARNING (100% to <110%), PASS (>=110%). |
| SEVERITY | TEXT | Severity / colour of the status (RED, YELLOW, GREEN) or alert severity (CRITICAL, HIGH, MEDIUM, INFO). |
| ALL_ALERTS | ARRAY | Array of alert objects (severity, type, message, action) for the latest date. |
| TOTAL_ALERT_COUNT | NUMBER | Number of alerts raised for the latest date. |
| HIGHEST_SEVERITY | TEXT | Highest alert severity of the latest date (CRITICAL, HIGH, MEDIUM, INFO, NONE). |
| ALERT_TIMESTAMP | TIMESTAMP_LTZ | UTC timestamp when the alerts were evaluated. |

### REPP_AGG_VW_LCR_DEPOSIT_BALANCES_DETAIL

REP_AGG_V001 · VIEW — Account-level detail view of deposit balances with run-off rate calculations and relationship-based discount logic. Provides drill-down capability from aggregate outflow metrics to individual accounts for funding stability analysis, customer relationship quality assessment, and deposit retention strategy optimization. Includes base run-off rates, relationship discounts (product count, direct debit), tenure penalties, and final calculated outflow amounts. Used by Treasury for funding planning and by Retail Banking for customer relationship management.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting (cut-off) date of the record (daily COB snapshot). |
| ACCOUNT_ID | TEXT | Deposit account identifier (links to core banking accounts). |
| CUSTOMER_ID | TEXT | Customer / counterparty identifier of the deposit (links to CRM). |
| DEPOSIT_TYPE | TEXT | Deposit run-off class (e.g. RETAIL_STABLE_INSURED, CORPORATE_OPERATIONAL). |
| DEPOSIT_NAME | TEXT | Descriptive name of the deposit run-off class. |
| COUNTERPARTY_TYPE | TEXT | Outflow segment / counterparty type (RETAIL, CORPORATE, FINANCIAL_INSTITUTION, COMMITTED_FACILITIES). |
| CUSTOMER_SEGMENT | TEXT | Customer segment of the depositor (MASS, AFFLUENT, PRIVATE, CORPORATE). |
| CURRENCY | TEXT | Original currency of the position (ISO 4217). |
| BALANCE_CCY | NUMBER | Deposit balance in original currency. |
| BALANCE_CHF | NUMBER | Deposit balance converted to CHF. |
| BASE_RUN_OFF_RATE | NUMBER | Base regulatory run-off rate of the deposit type (decimal). |
| DISCOUNT_STEP1 | NUMBER | Run-off rate after the -2pp discount for 3+ products (where allowed). |
| DISCOUNT_STEP2 | NUMBER | Run-off rate after the additional -1pp direct-debit discount (where allowed). |
| PENALTY_APPLIED | NUMBER | Run-off rate after the +5pp penalty for accounts younger than 18 months. |
| FINAL_RUN_OFF_RATE | NUMBER | Final run-off rate after adjustments, clamped to 3%-100% (decimal). |
| OUTFLOW_AMOUNT_CHF | NUMBER | Stressed 30-day outflow in CHF (balance x run-off rate). |
| IS_INSURED | BOOLEAN | TRUE when the balance is within the CHF 100k deposit insurance limit. |
| PRODUCT_COUNT | NUMBER | Number of products the customer holds (3+ qualifies for a run-off discount). |
| ACCOUNT_TENURE_DAYS | NUMBER | Days since account opening (under 540 days adds a +5pp penalty). |
| HAS_DIRECT_DEBIT | BOOLEAN | TRUE when the account has a direct-debit mandate (-1pp discount). |
| IS_OPERATIONAL | BOOLEAN | TRUE for operational deposits (payroll, clearing) with lower run-off. |
| ACCOUNT_STATUS | TEXT | Account status; only ACTIVE accounts enter the LCR. |
| SNB_COORDINATE | TEXT | SNB LCR reporting template coordinate for regulatory submission mapping. |

### REPP_AGG_VW_LCR_DRILLDOWN

REP_AGG_V001 · VIEW — Five-level LCR drill-down from asset class to product group, product, counterparty and individual contract (REQ-ANL-02) with business unit, currency and rating filter attributes (REQ-UI-02). Treemap and table source in the LCR page; filter by AS_OF_DATE and STRESS_SCENARIO.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Snapshot (cut-off) date. |
| STRESS_SCENARIO | TEXT | Scenario code: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS or COMBINED. |
| ASSET_CLASS | TEXT | Drill-down level 1: LCR asset class (REQ-ANL-02). |
| PRODUCT_GROUP | TEXT | Drill-down level 2: product group. |
| PRODUCT_TYPE | TEXT | Drill-down level 3: source product code. |
| COUNTERPARTY_ID | TEXT | Drill-down level 4: counterparty id. |
| COUNTERPARTY_NAME | TEXT | Drill-down level 4: counterparty name. |
| POSITION_ID | TEXT | Drill-down level 5: atomic contract / account / holding id. |
| POSITION_NAME | TEXT | Display name of the position. |
| LCR_TREATMENT | TEXT | HQLA, OUTFLOW, INFLOW or FACILITY. |
| HQLA_LEVEL | TEXT | HQLA level for HQLA positions. |
| BUSINESS_UNIT | TEXT | Business unit filter attribute (REQ-UI-02). |
| CURRENCY | TEXT | Currency filter attribute. |
| RATING | TEXT | Rating filter attribute. |
| EXPOSURE_CHF | NUMBER | Exposure in CHF (market value, balance or outstanding). |
| WEIGHTED_CHF | NUMBER | Weighted LCR amount (post-haircut HQLA, stressed outflow or weighted inflow). |
| LCR_IMPACT_CHF | NUMBER | Signed impact on the liquidity buffer: +HQLA, -outflow, +inflow (pre-cap). |

### REPP_AGG_VW_LCR_HQLA_HOLDINGS_DETAIL

REP_AGG_V001 · VIEW — Security-level detail view of HQLA holdings with regulatory level classification and haircut calculations. Provides drill-down capability from aggregate HQLA metrics to individual securities for portfolio analysis, concentration risk monitoring, and regulatory reporting. Includes market values, weighted values after haircuts, credit ratings, and SNB coordinate mappings. Used by Treasury for portfolio rebalancing decisions and by Compliance for regulatory submission preparation.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Reporting (cut-off) date of the record (daily COB snapshot). |
| HOLDING_ID | TEXT | Persistent HQLA position identifier from the treasury system. |
| ASSET_TYPE | TEXT | HQLA asset type code (e.g. CASH_SNB, GOVT_BOND_CHF, CANTON_BOND, EQUITY_SMI). |
| ASSET_NAME | TEXT | Descriptive name of the HQLA asset type. |
| REGULATORY_LEVEL | TEXT | Basel III HQLA level (L1, L2A, L2B) from LIQI_RAW_TB_HQLA_ELIGIBILITY. |
| ISIN | TEXT | ISO 6166 security identifier; NULL for cash positions. |
| SECURITY_NAME | TEXT | Descriptive name of the security or cash balance. |
| CURRENCY | TEXT | Original currency of the position (ISO 4217). |
| QUANTITY | NUMBER | Number of shares held (equities only). |
| MARKET_VALUE_CCY | NUMBER | Market value in original currency at COB prices. |
| MARKET_VALUE_CHF | NUMBER | Gross market value in CHF before regulatory haircuts. |
| HAIRCUT_PCT | NUMBER | Regulatory haircut in percent (0 for L1, 15 for L2A, 50 for L2B). |
| HAIRCUT_FACTOR | NUMBER | Haircut multiplier applied to market value (1.00, 0.85, 0.50). |
| WEIGHTED_VALUE_CHF | NUMBER | Post-haircut value in CHF (market value x haircut factor), before caps. |
| MATURITY_DATE | DATE | Final maturity date of the security; NULL for cash and equities. |
| CREDIT_RATING | TEXT | Credit rating of the security (S&P scale). |
| SMI_CONSTITUENT | BOOLEAN | TRUE when the equity is a Swiss Market Index constituent (L2B eligibility). |
| HQLA_ELIGIBLE | BOOLEAN | TRUE when the holding passes all FINMA eligibility criteria and counts as HQLA. |
| INELIGIBILITY_REASON | TEXT | Reason why the holding is excluded from HQLA (e.g. rating downgrade). |
| PORTFOLIO_CODE | TEXT | Treasury portfolio of the position (TREASURY_LIQ, ALM_BUFFER, TREASURY_INV). |
| CUSTODIAN | TEXT | Institution holding the security (SNB, SIX SIS, Euroclear, Clearstream). |
| SNB_COORDINATE | TEXT | SNB LCR reporting template coordinate for regulatory submission mapping. |

### REPP_AGG_VW_LCR_MONITORING

REP_AGG_V001 · VIEW — Consolidated LCR monitoring dashboard view combining all key liquidity metrics in a single row. Integrates latest daily LCR ratio, compliance status, HQLA breakdown (L1/L2A/L2B), deposit outflow analysis (Retail/Corporate/FI), trend metrics (7/30/90-day averages), volatility measures, and automated alert flags. Optimized for Treasury dashboards, executive reporting, and downstream application integration. Single source of truth for current liquidity position and regulatory compliance status. Updated in real-time via underlying dynamic tables for operational decision-making and management escalation.

| Column | Type | Description |
|---|---|---|
| REPORTING_DATE | DATE | Reporting (cut-off) date of the LCR figures (daily COB snapshot). |
| BANK_ID | TEXT | Regulatory bank identifier used in SNB submissions (SYNTH-CH-999). |
| LCR_RATIO | NUMBER | Liquidity Coverage Ratio in percent (HQLA / net cash outflows x 100). |
| LCR_STATUS | TEXT | Status band: FAIL (<100%), WARNING (100% to <110%), PASS (>=110%). |
| SEVERITY | TEXT | Severity / colour of the status (RED, YELLOW, GREEN) or alert severity (CRITICAL, HIGH, MEDIUM, INFO). |
| HQLA_TOTAL | NUMBER | Stock of HQLA in CHF after the L2B and L2 caps (LCR numerator). |
| L1_TOTAL | NUMBER | Level 1 HQLA in CHF (0% haircut). |
| L2A_TOTAL | NUMBER | Level 2A HQLA in CHF after 15% haircut. |
| L2B_TOTAL | NUMBER | Level 2B HQLA in CHF after 50% haircut, before the 15% cap. |
| L2_CAPPED | NUMBER | Level 2 HQLA in CHF after the L2B 15% and L2 40% caps. |
| CAP_APPLIED | BOOLEAN | TRUE when the L2B 15% or L2 40% cap reduced the HQLA stock. |
| DISCARDED_L2 | NUMBER | Level 2 assets in CHF excluded by the caps. |
| OUTFLOW_TOTAL | NUMBER | Total gross stressed 30-day outflows in CHF incl. facility drawdowns. |
| OUTFLOW_RETAIL | NUMBER | Stressed 30-day retail deposit outflows in CHF. |
| OUTFLOW_CORP | NUMBER | Stressed 30-day corporate / SME deposit outflows in CHF. |
| OUTFLOW_FI | NUMBER | Stressed 30-day financial-institution deposit and interbank borrowing outflows in CHF. |
| LCR_BUFFER_CHF | NUMBER | Liquidity buffer in CHF = HQLA - net cash outflows. |
| LCR_BUFFER_PCT | NUMBER | Liquidity buffer in percent of net cash outflows. |
| LCR_7D_AVG | NUMBER | 7-day rolling average LCR in percent. |
| LCR_30D_AVG | NUMBER | 30-day rolling average LCR in percent. |
| LCR_90D_AVG | NUMBER | 90-day rolling average LCR in percent. |
| LCR_30D_VOLATILITY | NUMBER | 30-day rolling standard deviation of the LCR. |
| LCR_30D_MIN | NUMBER | Minimum LCR of the last 30 days in percent. |
| LCR_30D_MAX | NUMBER | Maximum LCR of the last 30 days in percent. |
| LCR_DOD_CHANGE | NUMBER | Day-over-day LCR change in percentage points. |
| CONSECUTIVE_BREACHES_3D | NUMBER | Number of days below 100% within the last 3 days. |
| HIGH_VOLATILITY_ALERT | BOOLEAN | TRUE when the absolute day-over-day change exceeds 10pp. |
| SUSTAINED_BREACH_ALERT | BOOLEAN | TRUE when the LCR was below 100% on 3 consecutive days. |
| CRITICAL_BREACH_ALERT | BOOLEAN | TRUE when the LCR is below 95%. |
| L1_HOLDING_COUNT | NUMBER | Number of eligible Level 1 positions on the latest date. |
| L1_GROSS_VALUE | NUMBER | Gross market value of eligible Level 1 positions in CHF. |
| L1_CURRENCIES | TEXT | Currencies of the eligible Level 1 positions. |
| L2A_HOLDING_COUNT | NUMBER | Number of eligible Level 2A positions on the latest date. |
| L2A_GROSS_VALUE | NUMBER | Gross market value of eligible Level 2A positions in CHF. |
| L2B_HOLDING_COUNT | NUMBER | Number of eligible Level 2B positions on the latest date. |
| L2B_GROSS_VALUE | NUMBER | Gross market value of eligible Level 2B positions in CHF. |
| RETAIL_ACCOUNT_COUNT | NUMBER | Number of active retail deposit accounts on the latest date. |
| RETAIL_CUSTOMER_COUNT | NUMBER | Number of distinct retail depositors on the latest date. |
| RETAIL_BALANCE_CHF | NUMBER | Total active retail deposit balance in CHF. |
| RETAIL_AVG_RUN_OFF_PCT | NUMBER | Average final run-off rate of retail accounts in percent. |
| CORP_ACCOUNT_COUNT | NUMBER | Number of active corporate deposit accounts on the latest date. |
| CORP_CUSTOMER_COUNT | NUMBER | Number of distinct corporate depositors on the latest date. |
| CORP_BALANCE_CHF | NUMBER | Total active corporate deposit balance in CHF. |
| CORP_AVG_RUN_OFF_PCT | NUMBER | Average final run-off rate of corporate accounts in percent. |
| LCR_CALCULATION_TIMESTAMP | TIMESTAMP_NTZ | UTC timestamp of the underlying LCR calculation. |
| VIEW_QUERY_TIMESTAMP | TIMESTAMP_LTZ | UTC timestamp when this view was queried. |

### REPP_AGG_VW_LCR_MONTHLY_SUMMARY

REP_AGG_V001 · VIEW — Monthly summary of LCR metrics for Swiss National Bank (SNB) regulatory reporting per FINMA Circular 2015/2. Aggregates daily LCR ratios by month with summary statistics (average, min, max, volatility), breach day counts (FAIL/WARNING/PASS), and compliance rates. Used by Compliance team for monthly regulatory submissions, by Treasury for performance reporting, and by Executive Management for Board reporting. Critical for demonstrating sustained compliance with Basel III liquidity requirements and trend analysis over time.

| Column | Type | Description |
|---|---|---|
| REPORTING_MONTH | DATE | First day of the reporting month (monthly SNB / FINMA reporting period). |
| TRADING_DAYS | NUMBER | Number of reporting days in the month. |
| LCR_AVG | NUMBER | Average LCR of the month in percent. |
| LCR_MIN | NUMBER | Minimum LCR of the month in percent. |
| LCR_MAX | NUMBER | Maximum LCR of the month in percent. |
| LCR_VOLATILITY | FLOAT | Standard deviation of the daily LCR within the month. |
| AVG_HQLA_TOTAL | NUMBER | Average HQLA stock of the month in CHF. |
| AVG_OUTFLOW_TOTAL | NUMBER | Average gross stressed outflows of the month in CHF. |
| BREACH_DAYS | NUMBER | Number of FAIL days (LCR < 100%) in the month. |
| WARNING_DAYS | NUMBER | Number of WARNING days (100% to <110%) in the month. |
| COMPLIANT_DAYS | NUMBER | Number of PASS days (>= 110%) in the month. |
| BREACH_RATE_PCT | NUMBER | Share of FAIL days in the month in percent. |

### REPP_AGG_VW_LCR_VARIANCE_ATTRIBUTION

REP_AGG_V001 · VIEW — Exact, additive attribution of LCR changes to individual positions (REQ-ANL-03, TC-VAL-04). Uses the decomposition dLCR = dH/N1 + H0 x (1/N1 - 1/N0), split by position, with separate lines for the L2 cap and the 75% inflow cap effects, so contributions always sum to the reported LCR change. Filter by AS_OF_DATE, COMPARISON_TYPE and STRESS_SCENARIO.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Current cut-off date (T). |
| PRIOR_DATE | DATE | Prior cut-off date (T-1, T-7 or T-30). |
| COMPARISON_TYPE | TEXT | Comparison window: DOD (T vs T-1), WOW (T vs T-7) or MOM (T vs T-30). |
| STRESS_SCENARIO | TEXT | Scenario code: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS or COMBINED. |
| DRIVER_TYPE | TEXT | CLIENT (deposits, loans, facilities, interbank contracts), TREASURY (HQLA holdings) or ADJUSTMENT (effect of the L2 caps and of the 75% inflow cap). |
| POSITION_ID | TEXT | Driver position (contract / account / holding id); CAP_ADJUSTMENT or INFLOW_CAP_ADJUSTMENT for adjustment lines. |
| POSITION_NAME | TEXT | Display name of the driver position. |
| SOURCE_SYSTEM | TEXT | Source feed of the driver position. |
| PRODUCT_TYPE | TEXT | Product code of the driver position. |
| ASSET_CLASS | TEXT | LCR asset class of the driver position. |
| PRODUCT_GROUP | TEXT | Product group of the driver position. |
| COUNTERPARTY_ID | TEXT | Counterparty of the driver position. |
| COUNTERPARTY_NAME | TEXT | Client / issuer name of the driver position. |
| BUSINESS_UNIT | TEXT | Business unit of the driver position. |
| DELTA_HQLA_CHF | NUMBER | Change in post-haircut (pre-cap) HQLA of the position in CHF. |
| DELTA_OUTFLOW_CHF | NUMBER | Change in stressed outflow of the position in CHF. |
| DELTA_INFLOW_CHF | NUMBER | Change in weighted (pre-cap) inflow of the position in CHF. |
| CONTRIBUTION_PP | NUMBER | Exact contribution of the position to the LCR change in percentage points. HQLA: dH/N1; outflow: -H0 x dO/(N0 x N1); inflow: +H0 x dI/(N0 x N1). All contributions incl. adjustment lines sum exactly to LCR_DELTA_PP. |
| LCR_PRIOR | NUMBER | LCR at the prior date in percent (unrounded). |
| LCR_CURRENT | NUMBER | LCR at T in percent (unrounded). |
| LCR_DELTA_PP | NUMBER | Total LCR change in percentage points (unrounded) that the contributions explain. |
| DELTA_EXPOSURE_CHF | NUMBER | Change in exposure of the position in CHF (deposit balance, drawn outstanding or market value). |
| SETTLEMENT_IMPACT_PP | NUMBER | LCR effect of the cash settlement of the client move in percentage points: deposit withdrawals and borrowing repayments reduce SNB reserves, loan repayments increase them (delta cash / N1 x 100). 0 for HQLA and adjustment lines. |
| CLIENT_IMPACT_PP | NUMBER | Funding-adjusted LCR impact of the client move in percentage points = CONTRIBUTION_PP + SETTLEMENT_IMPACT_PP. A CHF 50M corporate withdrawal shows its full negative effect here (cash leaves the bank) although its own outflow line falls. Used to rank root causes. |

### REPP_AGG_VW_LCR_VARIANCE_DRIVERS

REP_AGG_V001 · VIEW — Top-10 client variance drivers per cut-off comparison and scenario (REQ-UI-03, REQ-ANL-03): the client positions (deposits, loans, facilities, interbank contracts) with the largest absolute exact contribution to the LCR change, with contract id and client name. Treasury HQLA moves and cap adjustments are excluded so the root cause (e.g. a client withdrawal) ranks first.

| Column | Type | Description |
|---|---|---|
| AS_OF_DATE | DATE | Current cut-off date (T). |
| PRIOR_DATE | DATE | Prior cut-off date the current date is compared against. |
| COMPARISON_TYPE | TEXT | Comparison window: DOD (T vs T-1), WOW (T vs T-7) or MOM (T vs T-30). |
| STRESS_SCENARIO | TEXT | Scenario code: BASE (regulatory FINMA LCR), BANK_STRESS, MARKET_STRESS or COMBINED. |
| CLIENT_RANK | NUMBER | Rank of the driver among CLIENT positions by absolute funding-adjusted impact CLIENT_IMPACT_PP (1 = largest). |
| POSITION_ID | TEXT | Driver contract / account id. |
| POSITION_NAME | TEXT | Display name of the driver position. |
| PRODUCT_TYPE | TEXT | Product code of the driver. |
| ASSET_CLASS | TEXT | LCR asset class of the driver. |
| COUNTERPARTY_ID | TEXT | Counterparty of the driver. |
| COUNTERPARTY_NAME | TEXT | Client name of the driver (REQ-UI-03). |
| BUSINESS_UNIT | TEXT | Business unit of the driver. |
| DELTA_OUTFLOW_CHF | NUMBER | Change in stressed outflow of the driver in CHF. |
| DELTA_INFLOW_CHF | NUMBER | Change in weighted inflow of the driver in CHF. |
| CONTRIBUTION_PP | NUMBER | Exact contribution of the driver to the LCR change in percentage points. |
| SHARE_OF_DELTA_PCT | NUMBER | Funding-adjusted impact (CLIENT_IMPACT_PP) as a share of the total LCR change in percent. |
| LCR_DELTA_PP | NUMBER | Total LCR change in percentage points. |
| CLIENT_IMPACT_PP | NUMBER | Funding-adjusted LCR impact in percentage points (exact contribution plus the cash settlement of the client move). Negative = the client move lowered the LCR. Use this to explain root causes. |
| SETTLEMENT_IMPACT_PP | NUMBER | Part of CLIENT_IMPACT_PP caused by the cash settlement (SNB reserves) of the client move, in percentage points. |
