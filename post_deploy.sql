-- ============================================================
-- post_deploy.sql - runs after `snow dcm deploy` (see deploy.sh)
-- Holds only what DCM Projects cannot manage (yet). Everything else -
-- schemas, tables, DTs, tasks (STARTED), streams, file formats, procedures,
-- views, semantic views, masking policies, tags, DMF attachments, grants - is in sources/definitions.
--
--   1. Seed data        MERGE / INSERT reference rows (DCM has no DML)
--   2. Cortex Agents    CREATE AGENT + Snowflake Intelligence registration
--                       (ADD AGENT wrapped so the script is re-runnable)
--   3. Shares/listings  owned by ACCOUNTADMIN, account-wide names, agent grants
-- ============================================================

USE DATABASE {{ db }};
USE SCHEMA {{ rep_raw }};

-- ============================================================
-- SEED DATA
-- ============================================================

-- Idempotent MERGE seeds (re-runnable on every deploy)

-- >>> BEGIN GENERATED LIQI REFERENCE SEEDS (operation/render_reference_seeds.py, source reference_data/liqi) - do not edit

MERGE INTO LIQI_RAW_TB_HQLA_ELIGIBILITY t USING (SELECT * FROM VALUES
    ('CASH_SNB', 'Swiss National Bank Reserves', 'L1', 0.0, 1.0, '1.1.1.1', 'Overnight sight deposits with SNB', NULL, FALSE, '2024-01-01'),
    ('CASH_VAULT', 'Physical Cash (Vault)', 'L1', 0.0, 1.0, '1.1.1.2', 'Banknotes and coins in vault', NULL, FALSE, '2024-01-01'),
    ('GOVT_BOND_CHF', 'Swiss Confederation Bonds', 'L1', 0.0, 1.0, '1.1.1.3', 'Swiss federal government debt', NULL, FALSE, '2024-01-01'),
    ('GOVT_BOND_FOREIGN', 'Foreign Government Bonds (0% RW)', 'L1', 0.0, 1.0, '1.1.1.4', 'Foreign sovereign debt with 0% risk weight', 'AA-', FALSE, '2024-01-01'),
    ('CANTON_BOND', 'Swiss Canton Bonds', 'L2A', 15.0, 0.85, '1.1.2.1.1', 'Cantonal government debt (20% risk weight)', NULL, FALSE, '2024-01-01'),
    ('COVERED_BOND', 'Covered Bonds (Pfandbriefe)', 'L2A', 15.0, 0.85, '1.1.2.1.2', 'Swiss Pfandbriefe rated AA- or better', 'AA-', FALSE, '2024-01-01'),
    ('CORPORATE_BOND_AA', 'Corporate Bonds (AA- or better)', 'L2A', 15.0, 0.85, '1.1.2.1.3', 'Non-financial corporate debt rated AA- or better (Level 2A per Basel III para 52)', 'AA-', FALSE, '2024-01-01'),
    ('CORPORATE_BOND_A_BBB', 'Corporate Bonds (A+ to BBB-)', 'L2B', 50.0, 0.5, '1.1.2.2.2', 'Non-financial corporate debt rated A+ to BBB- (Level 2B per Basel III para 54)', 'BBB-', FALSE, '2024-01-01'),
    ('EQUITY_SMI', 'Swiss Market Index (SMI) Equities', 'L2B', 50.0, 0.5, '1.1.2.2.1', 'SMI constituent non-financial equities', NULL, TRUE, '2024-01-01')
) s (ASSET_TYPE, ASSET_NAME, REGULATORY_LEVEL, HAIRCUT_PCT, HAIRCUT_FACTOR, SNB_COORDINATE, ELIGIBILITY_CRITERIA, MIN_CREDIT_RATING, REQUIRES_SMI_CONSTITUENT, EFFECTIVE_DATE)
ON t.ASSET_TYPE = s.ASSET_TYPE
WHEN MATCHED THEN UPDATE SET t.ASSET_NAME = s.ASSET_NAME, t.REGULATORY_LEVEL = s.REGULATORY_LEVEL, t.HAIRCUT_PCT = s.HAIRCUT_PCT, t.HAIRCUT_FACTOR = s.HAIRCUT_FACTOR, t.SNB_COORDINATE = s.SNB_COORDINATE, t.ELIGIBILITY_CRITERIA = s.ELIGIBILITY_CRITERIA, t.MIN_CREDIT_RATING = s.MIN_CREDIT_RATING, t.REQUIRES_SMI_CONSTITUENT = s.REQUIRES_SMI_CONSTITUENT, t.EFFECTIVE_DATE = s.EFFECTIVE_DATE
WHEN NOT MATCHED THEN INSERT (ASSET_TYPE, ASSET_NAME, REGULATORY_LEVEL, HAIRCUT_PCT, HAIRCUT_FACTOR, SNB_COORDINATE, ELIGIBILITY_CRITERIA, MIN_CREDIT_RATING, REQUIRES_SMI_CONSTITUENT, EFFECTIVE_DATE) VALUES (s.ASSET_TYPE, s.ASSET_NAME, s.REGULATORY_LEVEL, s.HAIRCUT_PCT, s.HAIRCUT_FACTOR, s.SNB_COORDINATE, s.ELIGIBILITY_CRITERIA, s.MIN_CREDIT_RATING, s.REQUIRES_SMI_CONSTITUENT, s.EFFECTIVE_DATE);

MERGE INTO LIQI_RAW_TB_DEPOSIT_TYPES t USING (SELECT * FROM VALUES
    ('RETAIL_STABLE_INSURED', 'Retail Stable (Insured, 3+ Products)', 0.03, 'RETAIL', '1.2.1.1', 'Insured balance under 100K CHF, established relationship', TRUE, TRUE, FALSE, '2024-01-01'),
    ('RETAIL_STABLE', 'Retail Stable', 0.05, 'RETAIL', '1.2.1.2', 'Insured balance under 100K CHF', TRUE, TRUE, FALSE, '2024-01-01'),
    ('RETAIL_LESS_STABLE', 'Retail Less Stable', 0.1, 'RETAIL', '1.2.1.3', 'Balance over 100K CHF or single product', FALSE, FALSE, FALSE, '2024-01-01'),
    ('CORPORATE_OPERATIONAL', 'Corporate Operational', 0.25, 'CORPORATE', '1.2.2.1', 'Payroll, clearing, cash management accounts', FALSE, FALSE, TRUE, '2024-01-01'),
    ('CORPORATE_NON_OPERATIONAL', 'Corporate Non-Operational', 0.4, 'CORPORATE', '1.2.2.2', 'Treasury deposits, sweep accounts', FALSE, FALSE, FALSE, '2024-01-01'),
    ('FINANCIAL_INSTITUTION', 'Financial Institution Deposits', 1.0, 'FINANCIAL_INSTITUTION', '1.2.2.3', 'Banks, insurers, other financials (100% outflow)', FALSE, FALSE, FALSE, '2024-01-01'),
    ('UNSECURED_WHOLESALE', 'Unsecured Wholesale Funding', 1.0, 'CORPORATE', '1.2.3', 'Non-operational wholesale funding', FALSE, FALSE, FALSE, '2024-01-01')
) s (DEPOSIT_TYPE, DEPOSIT_NAME, BASE_RUN_OFF_RATE, COUNTERPARTY_TYPE, SNB_COORDINATE, ELIGIBILITY_CRITERIA, ALLOWS_RELATIONSHIP_DISCOUNT, IS_INSURED, IS_OPERATIONAL, EFFECTIVE_DATE)
ON t.DEPOSIT_TYPE = s.DEPOSIT_TYPE
WHEN MATCHED THEN UPDATE SET t.DEPOSIT_NAME = s.DEPOSIT_NAME, t.BASE_RUN_OFF_RATE = s.BASE_RUN_OFF_RATE, t.COUNTERPARTY_TYPE = s.COUNTERPARTY_TYPE, t.SNB_COORDINATE = s.SNB_COORDINATE, t.ELIGIBILITY_CRITERIA = s.ELIGIBILITY_CRITERIA, t.ALLOWS_RELATIONSHIP_DISCOUNT = s.ALLOWS_RELATIONSHIP_DISCOUNT, t.IS_INSURED = s.IS_INSURED, t.IS_OPERATIONAL = s.IS_OPERATIONAL, t.EFFECTIVE_DATE = s.EFFECTIVE_DATE
WHEN NOT MATCHED THEN INSERT (DEPOSIT_TYPE, DEPOSIT_NAME, BASE_RUN_OFF_RATE, COUNTERPARTY_TYPE, SNB_COORDINATE, ELIGIBILITY_CRITERIA, ALLOWS_RELATIONSHIP_DISCOUNT, IS_INSURED, IS_OPERATIONAL, EFFECTIVE_DATE) VALUES (s.DEPOSIT_TYPE, s.DEPOSIT_NAME, s.BASE_RUN_OFF_RATE, s.COUNTERPARTY_TYPE, s.SNB_COORDINATE, s.ELIGIBILITY_CRITERIA, s.ALLOWS_RELATIONSHIP_DISCOUNT, s.IS_INSURED, s.IS_OPERATIONAL, s.EFFECTIVE_DATE);

MERGE INTO LIQI_RAW_TB_PRODUCT_MAPPING t USING (SELECT * FROM VALUES
    ('CASH_SNB', 'HOLDINGS', 'HQLA_L1_CASH', 'SNB reserves', 'HQLA', 'L1', 'CASH_SNB from holdings mapped to HQLA_L1_CASH / SNB reserves (HQLA treatment, FINMA LiqV)', TRUE),
    ('CASH_VAULT', 'HOLDINGS', 'HQLA_L1_CASH', 'Vault cash', 'HQLA', 'L1', 'CASH_VAULT from holdings mapped to HQLA_L1_CASH / Vault cash (HQLA treatment, FINMA LiqV)', TRUE),
    ('GOVT_BOND_CHF', 'HOLDINGS', 'HQLA_L1_SOVEREIGN', 'Government bonds', 'HQLA', 'L1', 'GOVT_BOND_CHF from holdings mapped to HQLA_L1_SOVEREIGN / Government bonds (HQLA treatment, FINMA LiqV)', TRUE),
    ('GOVT_BOND_FOREIGN', 'HOLDINGS', 'HQLA_L1_SOVEREIGN', 'Government bonds', 'HQLA', 'L1', 'GOVT_BOND_FOREIGN from holdings mapped to HQLA_L1_SOVEREIGN / Government bonds (HQLA treatment, FINMA LiqV)', TRUE),
    ('CANTON_BOND', 'HOLDINGS', 'HQLA_L2A', 'Public sector bonds', 'HQLA', 'L2A', 'CANTON_BOND from holdings mapped to HQLA_L2A / Public sector bonds (HQLA treatment, FINMA LiqV)', TRUE),
    ('COVERED_BOND', 'HOLDINGS', 'HQLA_L2A', 'Covered bonds', 'HQLA', 'L2A', 'COVERED_BOND from holdings mapped to HQLA_L2A / Covered bonds (HQLA treatment, FINMA LiqV)', TRUE),
    ('CORPORATE_BOND_AA', 'HOLDINGS', 'HQLA_L2A', 'Corporate bonds', 'HQLA', 'L2A', 'CORPORATE_BOND_AA from holdings mapped to HQLA_L2A / Corporate bonds (HQLA treatment, FINMA LiqV)', TRUE),
    ('CORPORATE_BOND_A_BBB', 'HOLDINGS', 'HQLA_L2B', 'Corporate bonds', 'HQLA', 'L2B', 'CORPORATE_BOND_A_BBB from holdings mapped to HQLA_L2B / Corporate bonds (HQLA treatment, FINMA LiqV)', TRUE),
    ('EQUITY_SMI', 'HOLDINGS', 'HQLA_L2B', 'Equities', 'HQLA', 'L2B', 'EQUITY_SMI from holdings mapped to HQLA_L2B / Equities (HQLA treatment, FINMA LiqV)', TRUE),
    ('RETAIL_STABLE_INSURED', 'DEPOSITS', 'RETAIL_DEPOSITS', 'Retail stable deposits', 'OUTFLOW', NULL, 'RETAIL_STABLE_INSURED from deposits mapped to RETAIL_DEPOSITS / Retail stable deposits (OUTFLOW treatment, FINMA LiqV)', TRUE),
    ('RETAIL_STABLE', 'DEPOSITS', 'RETAIL_DEPOSITS', 'Retail stable deposits', 'OUTFLOW', NULL, 'RETAIL_STABLE from deposits mapped to RETAIL_DEPOSITS / Retail stable deposits (OUTFLOW treatment, FINMA LiqV)', TRUE),
    ('RETAIL_LESS_STABLE', 'DEPOSITS', 'RETAIL_DEPOSITS', 'Retail less stable deposits', 'OUTFLOW', NULL, 'RETAIL_LESS_STABLE from deposits mapped to RETAIL_DEPOSITS / Retail less stable deposits (OUTFLOW treatment, FINMA LiqV)', TRUE),
    ('CORPORATE_OPERATIONAL', 'DEPOSITS', 'WHOLESALE_DEPOSITS', 'Operational deposits', 'OUTFLOW', NULL, 'CORPORATE_OPERATIONAL from deposits mapped to WHOLESALE_DEPOSITS / Operational deposits (OUTFLOW treatment, FINMA LiqV)', TRUE),
    ('CORPORATE_NON_OPERATIONAL', 'DEPOSITS', 'WHOLESALE_DEPOSITS', 'Non-operational deposits', 'OUTFLOW', NULL, 'CORPORATE_NON_OPERATIONAL from deposits mapped to WHOLESALE_DEPOSITS / Non-operational deposits (OUTFLOW treatment, FINMA LiqV)', TRUE),
    ('UNSECURED_WHOLESALE', 'DEPOSITS', 'WHOLESALE_DEPOSITS', 'Non-operational deposits', 'OUTFLOW', NULL, 'UNSECURED_WHOLESALE from deposits mapped to WHOLESALE_DEPOSITS / Non-operational deposits (OUTFLOW treatment, FINMA LiqV)', TRUE),
    ('FINANCIAL_INSTITUTION', 'DEPOSITS', 'INTERBANK_FUNDING', 'Financial institution deposits', 'OUTFLOW', NULL, 'FINANCIAL_INSTITUTION from deposits mapped to INTERBANK_FUNDING / Financial institution deposits (OUTFLOW treatment, FINMA LiqV)', TRUE),
    ('INTERBANK_BORROWING', 'CONTRACTS', 'INTERBANK_FUNDING', 'Interbank borrowing', 'OUTFLOW', NULL, 'INTERBANK_BORROWING from contracts mapped to INTERBANK_FUNDING / Interbank borrowing (OUTFLOW treatment, FINMA LiqV)', TRUE),
    ('LOAN_RETAIL', 'CONTRACTS', 'LOANS', 'Retail loans', 'INFLOW', NULL, 'LOAN_RETAIL from contracts mapped to LOANS / Retail loans (INFLOW treatment, FINMA LiqV)', TRUE),
    ('LOAN_CORP', 'CONTRACTS', 'LOANS', 'Corporate loans', 'INFLOW', NULL, 'LOAN_CORP from contracts mapped to LOANS / Corporate loans (INFLOW treatment, FINMA LiqV)', TRUE),
    ('INTERBANK_LOAN', 'CONTRACTS', 'INTERBANK_PLACEMENTS', 'Interbank placements', 'INFLOW', NULL, 'INTERBANK_LOAN from contracts mapped to INTERBANK_PLACEMENTS / Interbank placements (INFLOW treatment, FINMA LiqV)', TRUE),
    ('CREDIT_FACILITY', 'CONTRACTS', 'COMMITTED_FACILITIES', 'Credit facilities', 'FACILITY', NULL, 'CREDIT_FACILITY from contracts mapped to COMMITTED_FACILITIES / Credit facilities (FACILITY treatment, FINMA LiqV)', TRUE),
    ('LIQUIDITY_FACILITY', 'CONTRACTS', 'COMMITTED_FACILITIES', 'Liquidity facilities', 'FACILITY', NULL, 'LIQUIDITY_FACILITY from contracts mapped to COMMITTED_FACILITIES / Liquidity facilities (FACILITY treatment, FINMA LiqV)', TRUE)
) s (PRODUCT_TYPE, SOURCE_SYSTEM, ASSET_CLASS, PRODUCT_GROUP, LCR_TREATMENT, HQLA_LEVEL, DESCRIPTION, IS_ACTIVE)
ON t.PRODUCT_TYPE = s.PRODUCT_TYPE AND t.SOURCE_SYSTEM = s.SOURCE_SYSTEM
WHEN MATCHED THEN UPDATE SET t.ASSET_CLASS = s.ASSET_CLASS, t.PRODUCT_GROUP = s.PRODUCT_GROUP, t.LCR_TREATMENT = s.LCR_TREATMENT, t.HQLA_LEVEL = s.HQLA_LEVEL, t.DESCRIPTION = s.DESCRIPTION, t.IS_ACTIVE = s.IS_ACTIVE
WHEN NOT MATCHED THEN INSERT (PRODUCT_TYPE, SOURCE_SYSTEM, ASSET_CLASS, PRODUCT_GROUP, LCR_TREATMENT, HQLA_LEVEL, DESCRIPTION, IS_ACTIVE) VALUES (s.PRODUCT_TYPE, s.SOURCE_SYSTEM, s.ASSET_CLASS, s.PRODUCT_GROUP, s.LCR_TREATMENT, s.HQLA_LEVEL, s.DESCRIPTION, s.IS_ACTIVE);

MERGE INTO LIQI_RAW_TB_BEHAVIORAL_RULES t USING (SELECT * FROM VALUES
    ('RETAIL_STABLE_INSURED', 'RETAIL', 'BASE', 0.03, NULL, NULL, 0.0, 365, 'LINEAR', 'RETAIL_STABLE', 'BASE: RETAIL_STABLE_INSURED run-off 3% before account adjustments; linear decay over 365 days', TRUE, '2024-01-01'),
    ('RETAIL_STABLE', 'RETAIL', 'BASE', 0.05, NULL, NULL, 0.0, 365, 'LINEAR', 'RETAIL_STABLE', 'BASE: RETAIL_STABLE run-off 5% before account adjustments; linear decay over 365 days', TRUE, '2024-01-01'),
    ('RETAIL_LESS_STABLE', 'RETAIL', 'BASE', 0.1, NULL, NULL, 0.0, 180, 'LINEAR', 'RETAIL_LESS_STABLE', 'BASE: RETAIL_LESS_STABLE run-off 10% before account adjustments; linear decay over 180 days', TRUE, '2024-01-01'),
    ('CORPORATE_OPERATIONAL', 'CORPORATE', 'BASE', 0.25, NULL, NULL, 0.0, 90, 'LINEAR', 'CORP_OPERATIONAL', 'BASE: CORPORATE_OPERATIONAL run-off 25% before account adjustments; linear decay over 90 days', TRUE, '2024-01-01'),
    ('CORPORATE_NON_OPERATIONAL', 'CORPORATE', 'BASE', 0.4, NULL, NULL, 0.0, 30, 'LINEAR', 'CORP_NON_OPERATIONAL', 'BASE: CORPORATE_NON_OPERATIONAL run-off 40% before account adjustments; linear decay over 30 days', TRUE, '2024-01-01'),
    ('FINANCIAL_INSTITUTION', 'FINANCIAL_INSTITUTION', 'BASE', 1.0, NULL, NULL, 0.0, 7, 'LINEAR', 'FI_FUNDING', 'BASE: FINANCIAL_INSTITUTION run-off 100% before account adjustments; linear decay over 7 days', TRUE, '2024-01-01'),
    ('UNSECURED_WHOLESALE', 'CORPORATE', 'BASE', 1.0, NULL, NULL, 0.0, 30, 'LINEAR', 'CORP_NON_OPERATIONAL', 'BASE: UNSECURED_WHOLESALE run-off 100% before account adjustments; linear decay over 30 days', TRUE, '2024-01-01'),
    ('CASH_SNB', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_CASH', 'BASE: CASH_SNB regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CASH_VAULT', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_CASH', 'BASE: CASH_VAULT regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('GOVT_BOND_CHF', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_BOND', 'BASE: GOVT_BOND_CHF regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('GOVT_BOND_FOREIGN', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_BOND', 'BASE: GOVT_BOND_FOREIGN regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CANTON_BOND', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2A', 'BASE: CANTON_BOND regulatory haircut 15% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('COVERED_BOND', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2A', 'BASE: COVERED_BOND regulatory haircut 15% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CORPORATE_BOND_AA', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2A', 'BASE: CORPORATE_BOND_AA regulatory haircut 15% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CORPORATE_BOND_A_BBB', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2B_BOND', 'BASE: CORPORATE_BOND_A_BBB regulatory haircut 50% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('EQUITY_SMI', 'ALL', 'BASE', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2B_EQUITY', 'BASE: EQUITY_SMI regulatory haircut 50% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('LOAN_RETAIL', 'RETAIL', 'BASE', NULL, 0.5, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'BASE: LOAN_RETAIL / RETAIL inflow rate 50% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('LOAN_CORP', 'CORPORATE', 'BASE', NULL, 0.5, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'BASE: LOAN_CORP / CORPORATE inflow rate 50% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('LOAN_CORP', 'SME', 'BASE', NULL, 0.5, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'BASE: LOAN_CORP / SME inflow rate 50% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('INTERBANK_LOAN', 'FINANCIAL', 'BASE', NULL, 1.0, NULL, 0.0, NULL, 'NONE', 'INFLOW_FI', 'BASE: INTERBANK_LOAN / FINANCIAL inflow rate 100% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'RETAIL', 'BASE', NULL, NULL, 0.05, 0.0, NULL, 'NONE', 'FACILITIES', 'BASE: CREDIT_FACILITY / RETAIL drawdown 5% of undrawn commitment', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'CORPORATE', 'BASE', NULL, NULL, 0.1, 0.0, NULL, 'NONE', 'FACILITIES', 'BASE: CREDIT_FACILITY / CORPORATE drawdown 10% of undrawn commitment', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'SME', 'BASE', NULL, NULL, 0.1, 0.0, NULL, 'NONE', 'FACILITIES', 'BASE: CREDIT_FACILITY / SME drawdown 10% of undrawn commitment', TRUE, '2024-01-01'),
    ('LIQUIDITY_FACILITY', 'CORPORATE', 'BASE', NULL, NULL, 0.3, 0.0, NULL, 'NONE', 'FACILITIES', 'BASE: LIQUIDITY_FACILITY / CORPORATE drawdown 30% of undrawn commitment', TRUE, '2024-01-01'),
    ('LIQUIDITY_FACILITY', 'FINANCIAL', 'BASE', NULL, NULL, 0.4, 0.0, NULL, 'NONE', 'FACILITIES', 'BASE: LIQUIDITY_FACILITY / FINANCIAL drawdown 40% of undrawn commitment', TRUE, '2024-01-01'),
    ('INTERBANK_BORROWING', 'FINANCIAL', 'BASE', 1.0, NULL, NULL, 0.0, NULL, 'NONE', 'FI_FUNDING', 'BASE: INTERBANK_BORROWING / FINANCIAL 100% of contractual repayments within 30 days', TRUE, '2024-01-01'),
    ('RETAIL_STABLE_INSURED', 'RETAIL', 'BANK_STRESS', 0.045, NULL, NULL, 0.0, 182, 'LINEAR', 'RETAIL_STABLE', 'BANK_STRESS: RETAIL_STABLE_INSURED run-off 4% before account adjustments; linear decay over 182 days', TRUE, '2024-01-01'),
    ('RETAIL_STABLE', 'RETAIL', 'BANK_STRESS', 0.075, NULL, NULL, 0.0, 182, 'LINEAR', 'RETAIL_STABLE', 'BANK_STRESS: RETAIL_STABLE run-off 8% before account adjustments; linear decay over 182 days', TRUE, '2024-01-01'),
    ('RETAIL_LESS_STABLE', 'RETAIL', 'BANK_STRESS', 0.15, NULL, NULL, 0.0, 90, 'LINEAR', 'RETAIL_LESS_STABLE', 'BANK_STRESS: RETAIL_LESS_STABLE run-off 15% before account adjustments; linear decay over 90 days', TRUE, '2024-01-01'),
    ('CORPORATE_OPERATIONAL', 'CORPORATE', 'BANK_STRESS', 0.35, NULL, NULL, 0.0, 45, 'LINEAR', 'CORP_OPERATIONAL', 'BANK_STRESS: CORPORATE_OPERATIONAL run-off 35% before account adjustments; linear decay over 45 days', TRUE, '2024-01-01'),
    ('CORPORATE_NON_OPERATIONAL', 'CORPORATE', 'BANK_STRESS', 0.55, NULL, NULL, 0.0, 15, 'LINEAR', 'CORP_NON_OPERATIONAL', 'BANK_STRESS: CORPORATE_NON_OPERATIONAL run-off 55% before account adjustments; linear decay over 15 days', TRUE, '2024-01-01'),
    ('FINANCIAL_INSTITUTION', 'FINANCIAL_INSTITUTION', 'BANK_STRESS', 1.0, NULL, NULL, 0.0, 3, 'LINEAR', 'FI_FUNDING', 'BANK_STRESS: FINANCIAL_INSTITUTION run-off 100% before account adjustments; linear decay over 3 days', TRUE, '2024-01-01'),
    ('UNSECURED_WHOLESALE', 'CORPORATE', 'BANK_STRESS', 1.0, NULL, NULL, 0.0, 15, 'LINEAR', 'CORP_NON_OPERATIONAL', 'BANK_STRESS: UNSECURED_WHOLESALE run-off 100% before account adjustments; linear decay over 15 days', TRUE, '2024-01-01'),
    ('CASH_SNB', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_CASH', 'BANK_STRESS: CASH_SNB regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CASH_VAULT', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_CASH', 'BANK_STRESS: CASH_VAULT regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('GOVT_BOND_CHF', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_BOND', 'BANK_STRESS: GOVT_BOND_CHF regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('GOVT_BOND_FOREIGN', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_BOND', 'BANK_STRESS: GOVT_BOND_FOREIGN regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CANTON_BOND', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2A', 'BANK_STRESS: CANTON_BOND regulatory haircut 15% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('COVERED_BOND', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2A', 'BANK_STRESS: COVERED_BOND regulatory haircut 15% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CORPORATE_BOND_AA', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2A', 'BANK_STRESS: CORPORATE_BOND_AA regulatory haircut 15% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CORPORATE_BOND_A_BBB', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2B_BOND', 'BANK_STRESS: CORPORATE_BOND_A_BBB regulatory haircut 50% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('EQUITY_SMI', 'ALL', 'BANK_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L2B_EQUITY', 'BANK_STRESS: EQUITY_SMI regulatory haircut 50% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('LOAN_RETAIL', 'RETAIL', 'BANK_STRESS', NULL, 0.4, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'BANK_STRESS: LOAN_RETAIL / RETAIL inflow rate 40% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('LOAN_CORP', 'CORPORATE', 'BANK_STRESS', NULL, 0.4, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'BANK_STRESS: LOAN_CORP / CORPORATE inflow rate 40% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('LOAN_CORP', 'SME', 'BANK_STRESS', NULL, 0.4, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'BANK_STRESS: LOAN_CORP / SME inflow rate 40% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('INTERBANK_LOAN', 'FINANCIAL', 'BANK_STRESS', NULL, 0.8, NULL, 0.0, NULL, 'NONE', 'INFLOW_FI', 'BANK_STRESS: INTERBANK_LOAN / FINANCIAL inflow rate 80% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'RETAIL', 'BANK_STRESS', NULL, NULL, 0.075, 0.0, NULL, 'NONE', 'FACILITIES', 'BANK_STRESS: CREDIT_FACILITY / RETAIL drawdown 8% of undrawn commitment', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'CORPORATE', 'BANK_STRESS', NULL, NULL, 0.15, 0.0, NULL, 'NONE', 'FACILITIES', 'BANK_STRESS: CREDIT_FACILITY / CORPORATE drawdown 15% of undrawn commitment', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'SME', 'BANK_STRESS', NULL, NULL, 0.15, 0.0, NULL, 'NONE', 'FACILITIES', 'BANK_STRESS: CREDIT_FACILITY / SME drawdown 15% of undrawn commitment', TRUE, '2024-01-01'),
    ('LIQUIDITY_FACILITY', 'CORPORATE', 'BANK_STRESS', NULL, NULL, 0.45, 0.0, NULL, 'NONE', 'FACILITIES', 'BANK_STRESS: LIQUIDITY_FACILITY / CORPORATE drawdown 45% of undrawn commitment', TRUE, '2024-01-01'),
    ('LIQUIDITY_FACILITY', 'FINANCIAL', 'BANK_STRESS', NULL, NULL, 0.6, 0.0, NULL, 'NONE', 'FACILITIES', 'BANK_STRESS: LIQUIDITY_FACILITY / FINANCIAL drawdown 60% of undrawn commitment', TRUE, '2024-01-01'),
    ('INTERBANK_BORROWING', 'FINANCIAL', 'BANK_STRESS', 1.0, NULL, NULL, 0.0, NULL, 'NONE', 'FI_FUNDING', 'BANK_STRESS: INTERBANK_BORROWING / FINANCIAL 100% of contractual repayments within 30 days', TRUE, '2024-01-01'),
    ('RETAIL_STABLE_INSURED', 'RETAIL', 'MARKET_STRESS', 0.033, NULL, NULL, 0.0, 182, 'LINEAR', 'RETAIL_STABLE', 'MARKET_STRESS: RETAIL_STABLE_INSURED run-off 3% before account adjustments; linear decay over 182 days', TRUE, '2024-01-01'),
    ('RETAIL_STABLE', 'RETAIL', 'MARKET_STRESS', 0.055, NULL, NULL, 0.0, 182, 'LINEAR', 'RETAIL_STABLE', 'MARKET_STRESS: RETAIL_STABLE run-off 6% before account adjustments; linear decay over 182 days', TRUE, '2024-01-01'),
    ('RETAIL_LESS_STABLE', 'RETAIL', 'MARKET_STRESS', 0.11, NULL, NULL, 0.0, 90, 'LINEAR', 'RETAIL_LESS_STABLE', 'MARKET_STRESS: RETAIL_LESS_STABLE run-off 11% before account adjustments; linear decay over 90 days', TRUE, '2024-01-01'),
    ('CORPORATE_OPERATIONAL', 'CORPORATE', 'MARKET_STRESS', 0.3, NULL, NULL, 0.0, 45, 'LINEAR', 'CORP_OPERATIONAL', 'MARKET_STRESS: CORPORATE_OPERATIONAL run-off 30% before account adjustments; linear decay over 45 days', TRUE, '2024-01-01'),
    ('CORPORATE_NON_OPERATIONAL', 'CORPORATE', 'MARKET_STRESS', 0.45, NULL, NULL, 0.0, 15, 'LINEAR', 'CORP_NON_OPERATIONAL', 'MARKET_STRESS: CORPORATE_NON_OPERATIONAL run-off 45% before account adjustments; linear decay over 15 days', TRUE, '2024-01-01'),
    ('FINANCIAL_INSTITUTION', 'FINANCIAL_INSTITUTION', 'MARKET_STRESS', 1.0, NULL, NULL, 0.0, 3, 'LINEAR', 'FI_FUNDING', 'MARKET_STRESS: FINANCIAL_INSTITUTION run-off 100% before account adjustments; linear decay over 3 days', TRUE, '2024-01-01'),
    ('UNSECURED_WHOLESALE', 'CORPORATE', 'MARKET_STRESS', 1.0, NULL, NULL, 0.0, 15, 'LINEAR', 'CORP_NON_OPERATIONAL', 'MARKET_STRESS: UNSECURED_WHOLESALE run-off 100% before account adjustments; linear decay over 15 days', TRUE, '2024-01-01'),
    ('CASH_SNB', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_CASH', 'MARKET_STRESS: CASH_SNB regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CASH_VAULT', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_CASH', 'MARKET_STRESS: CASH_VAULT regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('GOVT_BOND_CHF', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 1.0, NULL, 'NONE', 'L1_BOND', 'MARKET_STRESS: GOVT_BOND_CHF regulatory haircut 0% plus 1pp stress add-on', TRUE, '2024-01-01'),
    ('GOVT_BOND_FOREIGN', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 2.0, NULL, 'NONE', 'L1_BOND', 'MARKET_STRESS: GOVT_BOND_FOREIGN regulatory haircut 0% plus 2pp stress add-on', TRUE, '2024-01-01'),
    ('CANTON_BOND', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 5.0, NULL, 'NONE', 'L2A', 'MARKET_STRESS: CANTON_BOND regulatory haircut 15% plus 5pp stress add-on', TRUE, '2024-01-01'),
    ('COVERED_BOND', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 5.0, NULL, 'NONE', 'L2A', 'MARKET_STRESS: COVERED_BOND regulatory haircut 15% plus 5pp stress add-on', TRUE, '2024-01-01'),
    ('CORPORATE_BOND_AA', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 7.0, NULL, 'NONE', 'L2A', 'MARKET_STRESS: CORPORATE_BOND_AA regulatory haircut 15% plus 7pp stress add-on', TRUE, '2024-01-01'),
    ('CORPORATE_BOND_A_BBB', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 15.0, NULL, 'NONE', 'L2B_BOND', 'MARKET_STRESS: CORPORATE_BOND_A_BBB regulatory haircut 50% plus 15pp stress add-on', TRUE, '2024-01-01'),
    ('EQUITY_SMI', 'ALL', 'MARKET_STRESS', NULL, NULL, NULL, 15.0, NULL, 'NONE', 'L2B_EQUITY', 'MARKET_STRESS: EQUITY_SMI regulatory haircut 50% plus 15pp stress add-on', TRUE, '2024-01-01'),
    ('LOAN_RETAIL', 'RETAIL', 'MARKET_STRESS', NULL, 0.45, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'MARKET_STRESS: LOAN_RETAIL / RETAIL inflow rate 45% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('LOAN_CORP', 'CORPORATE', 'MARKET_STRESS', NULL, 0.45, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'MARKET_STRESS: LOAN_CORP / CORPORATE inflow rate 45% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('LOAN_CORP', 'SME', 'MARKET_STRESS', NULL, 0.45, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'MARKET_STRESS: LOAN_CORP / SME inflow rate 45% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('INTERBANK_LOAN', 'FINANCIAL', 'MARKET_STRESS', NULL, 0.9, NULL, 0.0, NULL, 'NONE', 'INFLOW_FI', 'MARKET_STRESS: INTERBANK_LOAN / FINANCIAL inflow rate 90% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'RETAIL', 'MARKET_STRESS', NULL, NULL, 0.065, 0.0, NULL, 'NONE', 'FACILITIES', 'MARKET_STRESS: CREDIT_FACILITY / RETAIL drawdown 6% of undrawn commitment', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'CORPORATE', 'MARKET_STRESS', NULL, NULL, 0.13, 0.0, NULL, 'NONE', 'FACILITIES', 'MARKET_STRESS: CREDIT_FACILITY / CORPORATE drawdown 13% of undrawn commitment', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'SME', 'MARKET_STRESS', NULL, NULL, 0.13, 0.0, NULL, 'NONE', 'FACILITIES', 'MARKET_STRESS: CREDIT_FACILITY / SME drawdown 13% of undrawn commitment', TRUE, '2024-01-01'),
    ('LIQUIDITY_FACILITY', 'CORPORATE', 'MARKET_STRESS', NULL, NULL, 0.39, 0.0, NULL, 'NONE', 'FACILITIES', 'MARKET_STRESS: LIQUIDITY_FACILITY / CORPORATE drawdown 39% of undrawn commitment', TRUE, '2024-01-01'),
    ('LIQUIDITY_FACILITY', 'FINANCIAL', 'MARKET_STRESS', NULL, NULL, 0.52, 0.0, NULL, 'NONE', 'FACILITIES', 'MARKET_STRESS: LIQUIDITY_FACILITY / FINANCIAL drawdown 52% of undrawn commitment', TRUE, '2024-01-01'),
    ('INTERBANK_BORROWING', 'FINANCIAL', 'MARKET_STRESS', 1.0, NULL, NULL, 0.0, NULL, 'NONE', 'FI_FUNDING', 'MARKET_STRESS: INTERBANK_BORROWING / FINANCIAL 100% of contractual repayments within 30 days', TRUE, '2024-01-01'),
    ('RETAIL_STABLE_INSURED', 'RETAIL', 'COMBINED', 0.048, NULL, NULL, 0.0, 182, 'LINEAR', 'RETAIL_STABLE', 'COMBINED: RETAIL_STABLE_INSURED run-off 5% before account adjustments; linear decay over 182 days', TRUE, '2024-01-01'),
    ('RETAIL_STABLE', 'RETAIL', 'COMBINED', 0.08, NULL, NULL, 0.0, 182, 'LINEAR', 'RETAIL_STABLE', 'COMBINED: RETAIL_STABLE run-off 8% before account adjustments; linear decay over 182 days', TRUE, '2024-01-01'),
    ('RETAIL_LESS_STABLE', 'RETAIL', 'COMBINED', 0.16, NULL, NULL, 0.0, 90, 'LINEAR', 'RETAIL_LESS_STABLE', 'COMBINED: RETAIL_LESS_STABLE run-off 16% before account adjustments; linear decay over 90 days', TRUE, '2024-01-01'),
    ('CORPORATE_OPERATIONAL', 'CORPORATE', 'COMBINED', 0.4, NULL, NULL, 0.0, 45, 'LINEAR', 'CORP_OPERATIONAL', 'COMBINED: CORPORATE_OPERATIONAL run-off 40% before account adjustments; linear decay over 45 days', TRUE, '2024-01-01'),
    ('CORPORATE_NON_OPERATIONAL', 'CORPORATE', 'COMBINED', 0.6, NULL, NULL, 0.0, 15, 'LINEAR', 'CORP_NON_OPERATIONAL', 'COMBINED: CORPORATE_NON_OPERATIONAL run-off 60% before account adjustments; linear decay over 15 days', TRUE, '2024-01-01'),
    ('FINANCIAL_INSTITUTION', 'FINANCIAL_INSTITUTION', 'COMBINED', 1.0, NULL, NULL, 0.0, 3, 'LINEAR', 'FI_FUNDING', 'COMBINED: FINANCIAL_INSTITUTION run-off 100% before account adjustments; linear decay over 3 days', TRUE, '2024-01-01'),
    ('UNSECURED_WHOLESALE', 'CORPORATE', 'COMBINED', 1.0, NULL, NULL, 0.0, 15, 'LINEAR', 'CORP_NON_OPERATIONAL', 'COMBINED: UNSECURED_WHOLESALE run-off 100% before account adjustments; linear decay over 15 days', TRUE, '2024-01-01'),
    ('CASH_SNB', 'ALL', 'COMBINED', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_CASH', 'COMBINED: CASH_SNB regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('CASH_VAULT', 'ALL', 'COMBINED', NULL, NULL, NULL, 0.0, NULL, 'NONE', 'L1_CASH', 'COMBINED: CASH_VAULT regulatory haircut 0% plus 0pp stress add-on', TRUE, '2024-01-01'),
    ('GOVT_BOND_CHF', 'ALL', 'COMBINED', NULL, NULL, NULL, 1.0, NULL, 'NONE', 'L1_BOND', 'COMBINED: GOVT_BOND_CHF regulatory haircut 0% plus 1pp stress add-on', TRUE, '2024-01-01'),
    ('GOVT_BOND_FOREIGN', 'ALL', 'COMBINED', NULL, NULL, NULL, 2.0, NULL, 'NONE', 'L1_BOND', 'COMBINED: GOVT_BOND_FOREIGN regulatory haircut 0% plus 2pp stress add-on', TRUE, '2024-01-01'),
    ('CANTON_BOND', 'ALL', 'COMBINED', NULL, NULL, NULL, 5.0, NULL, 'NONE', 'L2A', 'COMBINED: CANTON_BOND regulatory haircut 15% plus 5pp stress add-on', TRUE, '2024-01-01'),
    ('COVERED_BOND', 'ALL', 'COMBINED', NULL, NULL, NULL, 5.0, NULL, 'NONE', 'L2A', 'COMBINED: COVERED_BOND regulatory haircut 15% plus 5pp stress add-on', TRUE, '2024-01-01'),
    ('CORPORATE_BOND_AA', 'ALL', 'COMBINED', NULL, NULL, NULL, 7.0, NULL, 'NONE', 'L2A', 'COMBINED: CORPORATE_BOND_AA regulatory haircut 15% plus 7pp stress add-on', TRUE, '2024-01-01'),
    ('CORPORATE_BOND_A_BBB', 'ALL', 'COMBINED', NULL, NULL, NULL, 15.0, NULL, 'NONE', 'L2B_BOND', 'COMBINED: CORPORATE_BOND_A_BBB regulatory haircut 50% plus 15pp stress add-on', TRUE, '2024-01-01'),
    ('EQUITY_SMI', 'ALL', 'COMBINED', NULL, NULL, NULL, 15.0, NULL, 'NONE', 'L2B_EQUITY', 'COMBINED: EQUITY_SMI regulatory haircut 50% plus 15pp stress add-on', TRUE, '2024-01-01'),
    ('LOAN_RETAIL', 'RETAIL', 'COMBINED', NULL, 0.35, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'COMBINED: LOAN_RETAIL / RETAIL inflow rate 35% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('LOAN_CORP', 'CORPORATE', 'COMBINED', NULL, 0.35, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'COMBINED: LOAN_CORP / CORPORATE inflow rate 35% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('LOAN_CORP', 'SME', 'COMBINED', NULL, 0.35, NULL, 0.0, NULL, 'NONE', 'INFLOW_NONFIN', 'COMBINED: LOAN_CORP / SME inflow rate 35% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('INTERBANK_LOAN', 'FINANCIAL', 'COMBINED', NULL, 0.7, NULL, 0.0, NULL, 'NONE', 'INFLOW_FI', 'COMBINED: INTERBANK_LOAN / FINANCIAL inflow rate 70% on 30-day performing contractual inflows', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'RETAIL', 'COMBINED', NULL, NULL, 0.09, 0.0, NULL, 'NONE', 'FACILITIES', 'COMBINED: CREDIT_FACILITY / RETAIL drawdown 9% of undrawn commitment', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'CORPORATE', 'COMBINED', NULL, NULL, 0.18, 0.0, NULL, 'NONE', 'FACILITIES', 'COMBINED: CREDIT_FACILITY / CORPORATE drawdown 18% of undrawn commitment', TRUE, '2024-01-01'),
    ('CREDIT_FACILITY', 'SME', 'COMBINED', NULL, NULL, 0.18, 0.0, NULL, 'NONE', 'FACILITIES', 'COMBINED: CREDIT_FACILITY / SME drawdown 18% of undrawn commitment', TRUE, '2024-01-01'),
    ('LIQUIDITY_FACILITY', 'CORPORATE', 'COMBINED', NULL, NULL, 0.54, 0.0, NULL, 'NONE', 'FACILITIES', 'COMBINED: LIQUIDITY_FACILITY / CORPORATE drawdown 54% of undrawn commitment', TRUE, '2024-01-01'),
    ('LIQUIDITY_FACILITY', 'FINANCIAL', 'COMBINED', NULL, NULL, 0.72, 0.0, NULL, 'NONE', 'FACILITIES', 'COMBINED: LIQUIDITY_FACILITY / FINANCIAL drawdown 72% of undrawn commitment', TRUE, '2024-01-01'),
    ('INTERBANK_BORROWING', 'FINANCIAL', 'COMBINED', 1.0, NULL, NULL, 0.0, NULL, 'NONE', 'FI_FUNDING', 'COMBINED: INTERBANK_BORROWING / FINANCIAL 100% of contractual repayments within 30 days', TRUE, '2024-01-01')
) s (PRODUCT_TYPE, COUNTERPARTY_SECTOR, STRESS_SCENARIO, RUNOFF_RATE, INFLOW_RATE, DRAWDOWN_RATE, HAIRCUT_ADDON_PCT, DECAY_PROFILE_DAYS, DECAY_METHOD, SIM_CATEGORY, RULE_DESCRIPTION, IS_ACTIVE, EFFECTIVE_DATE)
ON t.PRODUCT_TYPE = s.PRODUCT_TYPE AND t.COUNTERPARTY_SECTOR = s.COUNTERPARTY_SECTOR AND t.STRESS_SCENARIO = s.STRESS_SCENARIO
WHEN MATCHED THEN UPDATE SET t.RUNOFF_RATE = s.RUNOFF_RATE, t.INFLOW_RATE = s.INFLOW_RATE, t.DRAWDOWN_RATE = s.DRAWDOWN_RATE, t.HAIRCUT_ADDON_PCT = s.HAIRCUT_ADDON_PCT, t.DECAY_PROFILE_DAYS = s.DECAY_PROFILE_DAYS, t.DECAY_METHOD = s.DECAY_METHOD, t.SIM_CATEGORY = s.SIM_CATEGORY, t.RULE_DESCRIPTION = s.RULE_DESCRIPTION, t.IS_ACTIVE = s.IS_ACTIVE, t.EFFECTIVE_DATE = s.EFFECTIVE_DATE
WHEN NOT MATCHED THEN INSERT (PRODUCT_TYPE, COUNTERPARTY_SECTOR, STRESS_SCENARIO, RUNOFF_RATE, INFLOW_RATE, DRAWDOWN_RATE, HAIRCUT_ADDON_PCT, DECAY_PROFILE_DAYS, DECAY_METHOD, SIM_CATEGORY, RULE_DESCRIPTION, IS_ACTIVE, EFFECTIVE_DATE) VALUES (s.PRODUCT_TYPE, s.COUNTERPARTY_SECTOR, s.STRESS_SCENARIO, s.RUNOFF_RATE, s.INFLOW_RATE, s.DRAWDOWN_RATE, s.HAIRCUT_ADDON_PCT, s.DECAY_PROFILE_DAYS, s.DECAY_METHOD, s.SIM_CATEGORY, s.RULE_DESCRIPTION, s.IS_ACTIVE, s.EFFECTIVE_DATE);

MERGE INTO LIQI_RAW_TB_MATURITY_BUCKETS t USING (SELECT * FROM VALUES
    ('ON', 'Overnight', 0, 1, 1, TRUE),
    ('D2_7', '2-7 days', 2, 7, 2, TRUE),
    ('D8_30', '8-30 days', 8, 30, 3, TRUE),
    ('M1_3', '1-3 months', 31, 90, 4, FALSE),
    ('M3_6', '3-6 months', 91, 180, 5, FALSE),
    ('M6_12', '6-12 months', 181, 365, 6, FALSE),
    ('Y1P', '> 1 year', 366, 36500, 7, FALSE)
) s (BUCKET_CODE, BUCKET_NAME, FROM_DAY, TO_DAY, SORT_ORDER, IN_LCR_HORIZON)
ON t.BUCKET_CODE = s.BUCKET_CODE
WHEN MATCHED THEN UPDATE SET t.BUCKET_NAME = s.BUCKET_NAME, t.FROM_DAY = s.FROM_DAY, t.TO_DAY = s.TO_DAY, t.SORT_ORDER = s.SORT_ORDER, t.IN_LCR_HORIZON = s.IN_LCR_HORIZON
WHEN NOT MATCHED THEN INSERT (BUCKET_CODE, BUCKET_NAME, FROM_DAY, TO_DAY, SORT_ORDER, IN_LCR_HORIZON) VALUES (s.BUCKET_CODE, s.BUCKET_NAME, s.FROM_DAY, s.TO_DAY, s.SORT_ORDER, s.IN_LCR_HORIZON);

MERGE INTO LIQI_RAW_TB_SCENARIOS t USING (SELECT * FROM VALUES
    ('BASE', 'Regulatory LCR (FINMA)', 'Regulatory FINMA LiqV / Basel III LCR assumptions', TRUE, 1),
    ('BANK_STRESS', 'Idiosyncratic bank stress', 'Bank-specific stress: higher retail/corporate run-off, more facility drawdowns, lower inflows', FALSE, 2),
    ('MARKET_STRESS', 'Market-wide stress', 'Systemic stress: additional HQLA haircuts, moderate run-off increase, lower inflows', FALSE, 3),
    ('COMBINED', 'Combined stress', 'Combination of idiosyncratic and market-wide stress', FALSE, 4)
) s (SCENARIO_CODE, SCENARIO_NAME, DESCRIPTION, IS_REGULATORY, SORT_ORDER)
ON t.SCENARIO_CODE = s.SCENARIO_CODE
WHEN MATCHED THEN UPDATE SET t.SCENARIO_NAME = s.SCENARIO_NAME, t.DESCRIPTION = s.DESCRIPTION, t.IS_REGULATORY = s.IS_REGULATORY, t.SORT_ORDER = s.SORT_ORDER
WHEN NOT MATCHED THEN INSERT (SCENARIO_CODE, SCENARIO_NAME, DESCRIPTION, IS_REGULATORY, SORT_ORDER) VALUES (s.SCENARIO_CODE, s.SCENARIO_NAME, s.DESCRIPTION, s.IS_REGULATORY, s.SORT_ORDER);

-- DtI / Stichtagsvergleich (UC2) reference data (061_LIQI_dti_ingestion.sql)
MERGE INTO LIQI_RAW_TB_DTI_ENTITIES t USING (SELECT * FROM VALUES
    ('LE_01', 'Legal Entity 1', 1, TRUE, 0.30),
    ('LE_02', 'Legal Entity 2', 2, TRUE, 0.18),
    ('LE_03', 'Legal Entity 3', 3, TRUE, 0.14),
    ('LE_04', 'Legal Entity 4', 4, FALSE, 0.09),
    ('LE_05', 'Legal Entity 5', 5, FALSE, 0.08),
    ('LE_06', 'Legal Entity 6', 6, FALSE, 0.07),
    ('LE_07', 'Legal Entity 7', 7, FALSE, 0.06),
    ('LE_08', 'Legal Entity 8', 8, FALSE, 0.04),
    ('LE_09', 'Legal Entity 9', 9, FALSE, 0.04)
) s (ENTITY_ID, ENTITY_NAME, SORT_ORDER, IS_DEFAULT, CB_BALANCE_SHARE)
ON t.ENTITY_ID = s.ENTITY_ID
WHEN MATCHED THEN UPDATE SET t.ENTITY_NAME = s.ENTITY_NAME, t.SORT_ORDER = s.SORT_ORDER, t.IS_DEFAULT = s.IS_DEFAULT, t.CB_BALANCE_SHARE = s.CB_BALANCE_SHARE
WHEN NOT MATCHED THEN INSERT (ENTITY_ID, ENTITY_NAME, SORT_ORDER, IS_DEFAULT, CB_BALANCE_SHARE) VALUES (s.ENTITY_ID, s.ENTITY_NAME, s.SORT_ORDER, s.IS_DEFAULT, s.CB_BALANCE_SHARE);

MERGE INTO LIQI_RAW_TB_DTI_LST_POSITIONS t USING (SELECT * FROM VALUES
    ('POOL', 'Collateral Pool', NULL, 1, 1, FALSE, TRUE, FALSE),
    ('POOL_CB', 'Central bank and cash', 'POOL', 2, 1, FALSE, TRUE, FALSE),
    ('POOL_CB_BALANCE', 'Central bank balance', 'POOL_CB', 3, 1, TRUE, TRUE, FALSE),
    ('POOL_VAULT_CASH', 'Vault cash', 'POOL_CB', 3, 2, TRUE, TRUE, FALSE),
    ('POOL_SEC', 'Securities', 'POOL', 2, 2, FALSE, TRUE, FALSE),
    ('POOL_SEC_L1', 'Level 1 securities', 'POOL_SEC', 3, 1, TRUE, TRUE, FALSE),
    ('POOL_SEC_L2A', 'Level 2A securities', 'POOL_SEC', 3, 2, TRUE, TRUE, FALSE),
    ('POOL_SEC_L2B', 'Level 2B securities', 'POOL_SEC', 3, 3, TRUE, TRUE, FALSE),
    ('DET', 'Deterministic Cash Flows', NULL, 1, 2, FALSE, TRUE, TRUE),
    ('DET_FIX', 'Fixed cash flows', 'DET', 2, 1, FALSE, TRUE, TRUE),
    ('DET_FIX_LOANS', 'Loans', 'DET_FIX', 3, 1, FALSE, TRUE, TRUE),
    ('DET_FIX_LOANS_RETAIL', 'Mortgages and retail loans', 'DET_FIX_LOANS', 4, 1, TRUE, TRUE, TRUE),
    ('DET_FIX_LOANS_CORP', 'Corporate loans', 'DET_FIX_LOANS', 4, 2, TRUE, TRUE, TRUE),
    ('DET_FIX_MM', 'Money market', 'DET_FIX', 3, 2, FALSE, TRUE, TRUE),
    ('DET_FIX_MM_PLACE', 'Interbank placements', 'DET_FIX_MM', 4, 1, TRUE, TRUE, TRUE),
    ('DET_FIX_MM_BORROW', 'Interbank borrowing', 'DET_FIX_MM', 4, 2, TRUE, TRUE, TRUE),
    ('DET_FIX_SEC', 'Security maturities', 'DET_FIX', 3, 3, TRUE, TRUE, TRUE),
    ('DET_PLAN', 'Planning positions', 'DET', 2, 2, FALSE, FALSE, TRUE),
    ('DET_PLAN_NEW_LENDING', 'New lending (Kreditneugeschaeft)', 'DET_PLAN', 3, 1, TRUE, FALSE, TRUE),
    ('DET_PLAN_FUNDING', 'Funding plan (Fundingplanung)', 'DET_PLAN', 3, 2, TRUE, FALSE, TRUE),
    ('INF', 'Inflows', NULL, 1, 3, FALSE, TRUE, FALSE),
    ('INF_MODEL', 'Modelled inflows', 'INF', 2, 1, FALSE, TRUE, FALSE),
    ('INF_REVERSE_REPO', 'Maturing reverse repos', 'INF_MODEL', 3, 1, TRUE, TRUE, FALSE),
    ('INF_LOAN_RENEWAL', 'Stressed loan repayments', 'INF_MODEL', 3, 2, TRUE, TRUE, FALSE),
    ('OUT', 'Outflows', NULL, 1, 4, FALSE, TRUE, FALSE),
    ('OUT_DEP', 'Deposit run-off', 'OUT', 2, 1, FALSE, TRUE, FALSE),
    ('OUT_DEP_RETAIL', 'Retail deposits', 'OUT_DEP', 3, 1, TRUE, TRUE, FALSE),
    ('OUT_DEP_CORPORATE', 'Corporate deposits', 'OUT_DEP', 3, 2, TRUE, TRUE, FALSE),
    ('OUT_DEP_FI', 'Financial institution deposits', 'OUT_DEP', 3, 3, TRUE, TRUE, FALSE),
    ('OUT_FAC', 'Facility drawdowns', 'OUT', 2, 2, FALSE, TRUE, FALSE),
    ('OUT_FAC_CREDIT', 'Credit facilities', 'OUT_FAC', 3, 1, TRUE, TRUE, FALSE),
    ('OUT_FAC_LIQUIDITY', 'Liquidity facilities', 'OUT_FAC', 3, 2, TRUE, TRUE, FALSE),
    ('OUT_OTHER', 'Other outflows', 'OUT', 2, 3, FALSE, TRUE, FALSE),
    ('OUT_COLLATERAL_CALLS', 'Collateral calls on repos', 'OUT_OTHER', 3, 1, TRUE, TRUE, FALSE)
) s (POSITION_CODE, POSITION_NAME, PARENT_CODE, LEVEL_NO, SORT_ORDER, IS_LEAF, IS_DRILLABLE, IS_DETERMINISTIC)
ON t.POSITION_CODE = s.POSITION_CODE
WHEN MATCHED THEN UPDATE SET t.POSITION_NAME = s.POSITION_NAME, t.PARENT_CODE = s.PARENT_CODE, t.LEVEL_NO = s.LEVEL_NO, t.SORT_ORDER = s.SORT_ORDER, t.IS_LEAF = s.IS_LEAF, t.IS_DRILLABLE = s.IS_DRILLABLE, t.IS_DETERMINISTIC = s.IS_DETERMINISTIC
WHEN NOT MATCHED THEN INSERT (POSITION_CODE, POSITION_NAME, PARENT_CODE, LEVEL_NO, SORT_ORDER, IS_LEAF, IS_DRILLABLE, IS_DETERMINISTIC) VALUES (s.POSITION_CODE, s.POSITION_NAME, s.PARENT_CODE, s.LEVEL_NO, s.SORT_ORDER, s.IS_LEAF, s.IS_DRILLABLE, s.IS_DETERMINISTIC);

MERGE INTO LIQI_RAW_TB_DTI_PRODUCT_CLUSTERS t USING (SELECT * FROM VALUES
    ('PC_CB', 'Central bank and cash', 'Liquidity reserves', 1),
    ('PC_GOV', 'Government bonds', 'Securities', 2),
    ('PC_PUBLIC', 'Canton and covered bonds', 'Securities', 3),
    ('PC_CORP_BOND', 'Corporate bonds and equities', 'Securities', 4),
    ('PC_MORTGAGE', 'Mortgages and retail loans', 'Lending', 5),
    ('PC_CORP_LOAN', 'Corporate loans', 'Lending', 6),
    ('PC_MM_PLACEMENT', 'Interbank placements', 'Money market', 7),
    ('PC_MM_BORROWING', 'Interbank borrowing', 'Money market', 8),
    ('PC_REPO', 'Repos and reverse repos', 'Money market', 9),
    ('PC_RETAIL_DEP', 'Retail deposits', 'Deposits', 10),
    ('PC_CORP_DEP', 'Corporate deposits', 'Deposits', 11),
    ('PC_FI_DEP', 'Financial institution deposits', 'Deposits', 12),
    ('PC_CREDIT_LINES', 'Credit facilities', 'Commitments', 13),
    ('PC_LIQ_LINES', 'Liquidity facilities', 'Commitments', 14),
    ('PC_PLAN', 'Planning positions', 'Planning', 15)
) s (PRODUCT_CLUSTER, CLUSTER_NAME, PRODUCT_GROUP, SORT_ORDER)
ON t.PRODUCT_CLUSTER = s.PRODUCT_CLUSTER
WHEN MATCHED THEN UPDATE SET t.CLUSTER_NAME = s.CLUSTER_NAME, t.PRODUCT_GROUP = s.PRODUCT_GROUP, t.SORT_ORDER = s.SORT_ORDER
WHEN NOT MATCHED THEN INSERT (PRODUCT_CLUSTER, CLUSTER_NAME, PRODUCT_GROUP, SORT_ORDER) VALUES (s.PRODUCT_CLUSTER, s.CLUSTER_NAME, s.PRODUCT_GROUP, s.SORT_ORDER);

MERGE INTO LIQI_RAW_TB_DTI_LST_ASSIGNMENT t USING (SELECT * FROM VALUES
    ('HOLDINGS', 'CASH_SNB', '*', 'POOL', 'POOL_CB_BALANCE', 'PC_CB'),
    ('HOLDINGS', 'CASH_VAULT', '*', 'POOL', 'POOL_VAULT_CASH', 'PC_CB'),
    ('HOLDINGS', 'GOVT_BOND_CHF', '*', 'POOL', 'POOL_SEC_L1', 'PC_GOV'),
    ('HOLDINGS', 'GOVT_BOND_FOREIGN', '*', 'POOL', 'POOL_SEC_L1', 'PC_GOV'),
    ('HOLDINGS', 'CANTON_BOND', '*', 'POOL', 'POOL_SEC_L2A', 'PC_PUBLIC'),
    ('HOLDINGS', 'COVERED_BOND', '*', 'POOL', 'POOL_SEC_L2A', 'PC_PUBLIC'),
    ('HOLDINGS', 'CORPORATE_BOND_AA', '*', 'POOL', 'POOL_SEC_L2A', 'PC_CORP_BOND'),
    ('HOLDINGS', 'CORPORATE_BOND_A_BBB', '*', 'POOL', 'POOL_SEC_L2B', 'PC_CORP_BOND'),
    ('HOLDINGS', 'EQUITY_SMI', '*', 'POOL', 'POOL_SEC_L2B', 'PC_CORP_BOND'),
    ('HOLDINGS', '*', '*', 'CONTRACTUAL', 'DET_FIX_SEC', NULL),
    ('CASHFLOWS', 'LOAN_RETAIL', '*', 'CONTRACTUAL', 'DET_FIX_LOANS_RETAIL', 'PC_MORTGAGE'),
    ('CASHFLOWS', 'LOAN_CORP', '*', 'CONTRACTUAL', 'DET_FIX_LOANS_CORP', 'PC_CORP_LOAN'),
    ('CASHFLOWS', 'INTERBANK_LOAN', '*', 'CONTRACTUAL', 'DET_FIX_MM_PLACE', 'PC_MM_PLACEMENT'),
    ('CASHFLOWS', 'INTERBANK_BORROWING', '*', 'CONTRACTUAL', 'DET_FIX_MM_BORROW', 'PC_MM_BORROWING'),
    ('CASHFLOWS', 'LOAN_CORP', '*', 'INFLOW', 'INF_LOAN_RENEWAL', 'PC_CORP_LOAN'),
    ('DEPOSITS', 'RETAIL_STABLE_INSURED', '*', 'RUNOFF', 'OUT_DEP_RETAIL', 'PC_RETAIL_DEP'),
    ('DEPOSITS', 'RETAIL_STABLE', '*', 'RUNOFF', 'OUT_DEP_RETAIL', 'PC_RETAIL_DEP'),
    ('DEPOSITS', 'RETAIL_LESS_STABLE', '*', 'RUNOFF', 'OUT_DEP_RETAIL', 'PC_RETAIL_DEP'),
    ('DEPOSITS', 'CORPORATE_OPERATIONAL', '*', 'RUNOFF', 'OUT_DEP_CORPORATE', 'PC_CORP_DEP'),
    ('DEPOSITS', 'CORPORATE_NON_OPERATIONAL', '*', 'RUNOFF', 'OUT_DEP_CORPORATE', 'PC_CORP_DEP'),
    ('DEPOSITS', 'FINANCIAL_INSTITUTION', '*', 'RUNOFF', 'OUT_DEP_FI', 'PC_FI_DEP'),
    ('CONTRACTS', 'CREDIT_FACILITY', '*', 'DRAWDOWN', 'OUT_FAC_CREDIT', 'PC_CREDIT_LINES'),
    ('CONTRACTS', 'LIQUIDITY_FACILITY', '*', 'DRAWDOWN', 'OUT_FAC_LIQUIDITY', 'PC_LIQ_LINES'),
    ('SECURED_FUNDING', 'REVERSE_REPO', '*', 'INFLOW', 'INF_REVERSE_REPO', 'PC_REPO'),
    ('SECURED_FUNDING', 'BILATERAL_REPO', '*', 'RUNOFF', 'OUT_COLLATERAL_CALLS', 'PC_REPO'),
    ('PLANNING', 'NEW_LENDING', '*', 'CONTRACTUAL', 'DET_PLAN_NEW_LENDING', 'PC_PLAN'),
    ('PLANNING', 'FUNDING_PLAN', '*', 'CONTRACTUAL', 'DET_PLAN_FUNDING', 'PC_PLAN')
) s (SOURCE_SYSTEM, PRODUCT_TYPE, COUNTERPARTY_SECTOR, FLOW_KIND, POSITION_CODE, PRODUCT_CLUSTER)
ON t.SOURCE_SYSTEM = s.SOURCE_SYSTEM AND t.PRODUCT_TYPE = s.PRODUCT_TYPE AND t.COUNTERPARTY_SECTOR = s.COUNTERPARTY_SECTOR AND t.FLOW_KIND = s.FLOW_KIND
WHEN MATCHED THEN UPDATE SET t.POSITION_CODE = s.POSITION_CODE, t.PRODUCT_CLUSTER = s.PRODUCT_CLUSTER
WHEN NOT MATCHED THEN INSERT (SOURCE_SYSTEM, PRODUCT_TYPE, COUNTERPARTY_SECTOR, FLOW_KIND, POSITION_CODE, PRODUCT_CLUSTER) VALUES (s.SOURCE_SYSTEM, s.PRODUCT_TYPE, s.COUNTERPARTY_SECTOR, s.FLOW_KIND, s.POSITION_CODE, s.PRODUCT_CLUSTER);

MERGE INTO LIQI_RAW_TB_DTI_APPLICATIONS t USING (SELECT * FROM VALUES
    ('HYP', 'Mortgages and retail loans', 1),
    ('KRE', 'Corporate lending', 2),
    ('GEL', 'Money and capital market', 3),
    ('WPA', 'Securities', 4),
    ('EIN', 'Deposits', 5),
    ('DER', 'Derivatives', 6)
) s (APPLICATION_ID, APPLICATION_NAME, SORT_ORDER)
ON t.APPLICATION_ID = s.APPLICATION_ID
WHEN MATCHED THEN UPDATE SET t.APPLICATION_NAME = s.APPLICATION_NAME, t.SORT_ORDER = s.SORT_ORDER
WHEN NOT MATCHED THEN INSERT (APPLICATION_ID, APPLICATION_NAME, SORT_ORDER) VALUES (s.APPLICATION_ID, s.APPLICATION_NAME, s.SORT_ORDER);

MERGE INTO LIQI_RAW_TB_DTI_WLK_CLUSTERS t USING (SELECT * FROM VALUES
    ('I', 'Central bank balances and cash', 0.00, 'L1', 1),
    ('II', 'Swiss Confederation bonds', 0.02, 'L1', 2),
    ('III', 'Other AAA sovereign bonds', 0.03, 'L1', 3),
    ('IV', 'Canton and municipal bonds', 0.05, 'L2A', 4),
    ('V', 'Covered bonds (Pfandbriefe)', 0.07, 'L2A', 5),
    ('VI', 'Corporate bonds AA', 0.10, 'L2A', 6),
    ('VII', 'Corporate bonds A to BBB', 0.20, 'L2B', 7),
    ('VIII', 'SMI equities', 0.30, 'L2B', 8),
    ('IX', 'Not eligible', 1.00, 'NONE', 9),
    ('X', 'Credit claims', 0.40, 'NONE', 10),
    ('XIa', 'EUR government bonds', 0.03, 'L1', 11),
    ('XIb', 'USD government bonds', 0.04, 'L1', 12)
) s (WLK_CLUSTER, CLUSTER_NAME, HAIRCUT, HQLA_CATEGORY, SORT_ORDER)
ON t.WLK_CLUSTER = s.WLK_CLUSTER
WHEN MATCHED THEN UPDATE SET t.CLUSTER_NAME = s.CLUSTER_NAME, t.HAIRCUT = s.HAIRCUT, t.HQLA_CATEGORY = s.HQLA_CATEGORY, t.SORT_ORDER = s.SORT_ORDER
WHEN NOT MATCHED THEN INSERT (WLK_CLUSTER, CLUSTER_NAME, HAIRCUT, HQLA_CATEGORY, SORT_ORDER) VALUES (s.WLK_CLUSTER, s.CLUSTER_NAME, s.HAIRCUT, s.HQLA_CATEGORY, s.SORT_ORDER);

MERGE INTO LIQI_RAW_TB_DTI_BUCKETS t USING (SELECT * FROM VALUES
    ('W1', '1 week', 'DAY', 7, 1),
    ('W2', '2 weeks', 'DAY', 14, 2),
    ('W3', '3 weeks', 'DAY', 21, 3),
    ('M01', '1 month', 'MONTH', 1, 4),
    ('M02', '2 months', 'MONTH', 2, 5),
    ('M03', '3 months', 'MONTH', 3, 6),
    ('M04', '4 months', 'MONTH', 4, 7),
    ('M05', '5 months', 'MONTH', 5, 8),
    ('M06', '6 months', 'MONTH', 6, 9),
    ('M07', '7 months', 'MONTH', 7, 10),
    ('M08', '8 months', 'MONTH', 8, 11),
    ('M09', '9 months', 'MONTH', 9, 12),
    ('M10', '10 months', 'MONTH', 10, 13),
    ('M11', '11 months', 'MONTH', 11, 14),
    ('M12', '12 months', 'MONTH', 12, 15),
    ('GT12M', '> 12 months', 'MONTH', NULL, 16)
) s (BUCKET_CODE, BUCKET_NAME, OFFSET_UNIT, OFFSET_VALUE, SORT_ORDER)
ON t.BUCKET_CODE = s.BUCKET_CODE
WHEN MATCHED THEN UPDATE SET t.BUCKET_NAME = s.BUCKET_NAME, t.OFFSET_UNIT = s.OFFSET_UNIT, t.OFFSET_VALUE = s.OFFSET_VALUE, t.SORT_ORDER = s.SORT_ORDER
WHEN NOT MATCHED THEN INSERT (BUCKET_CODE, BUCKET_NAME, OFFSET_UNIT, OFFSET_VALUE, SORT_ORDER) VALUES (s.BUCKET_CODE, s.BUCKET_NAME, s.OFFSET_UNIT, s.OFFSET_VALUE, s.SORT_ORDER);

-- Zurich bank holidays (switching the calendar = editing reference_data/liqi/dti_holidays.csv)
MERGE INTO LIQI_RAW_TB_DTI_HOLIDAYS t USING (SELECT * FROM VALUES
    ('2025-01-01'::DATE, 'New Year'),
    ('2025-01-02'::DATE, 'Berchtoldstag'),
    ('2025-04-18'::DATE, 'Good Friday'),
    ('2025-04-21'::DATE, 'Easter Monday'),
    ('2025-05-01'::DATE, 'Labour Day'),
    ('2025-05-29'::DATE, 'Ascension Day'),
    ('2025-06-09'::DATE, 'Whit Monday'),
    ('2025-08-01'::DATE, 'Swiss National Day'),
    ('2025-12-25'::DATE, 'Christmas Day'),
    ('2025-12-26'::DATE, 'St. Stephen''s Day'),
    ('2026-01-01'::DATE, 'New Year'),
    ('2026-01-02'::DATE, 'Berchtoldstag'),
    ('2026-04-03'::DATE, 'Good Friday'),
    ('2026-04-06'::DATE, 'Easter Monday'),
    ('2026-05-01'::DATE, 'Labour Day'),
    ('2026-05-14'::DATE, 'Ascension Day'),
    ('2026-05-25'::DATE, 'Whit Monday'),
    ('2026-08-01'::DATE, 'Swiss National Day'),
    ('2026-12-25'::DATE, 'Christmas Day'),
    ('2026-12-26'::DATE, 'St. Stephen''s Day'),
    ('2027-01-01'::DATE, 'New Year'),
    ('2027-01-02'::DATE, 'Berchtoldstag'),
    ('2027-03-26'::DATE, 'Good Friday'),
    ('2027-03-29'::DATE, 'Easter Monday'),
    ('2027-05-01'::DATE, 'Labour Day'),
    ('2027-05-06'::DATE, 'Ascension Day'),
    ('2027-05-17'::DATE, 'Whit Monday'),
    ('2027-08-01'::DATE, 'Swiss National Day'),
    ('2027-12-25'::DATE, 'Christmas Day'),
    ('2027-12-26'::DATE, 'St. Stephen''s Day')
) s (HOLIDAY_DATE, HOLIDAY_NAME)
ON t.HOLIDAY_DATE = s.HOLIDAY_DATE
WHEN MATCHED THEN UPDATE SET t.HOLIDAY_NAME = s.HOLIDAY_NAME
WHEN NOT MATCHED THEN INSERT (HOLIDAY_DATE, HOLIDAY_NAME) VALUES (s.HOLIDAY_DATE, s.HOLIDAY_NAME);

-- insert-only: thresholds are maintained by the DtI owner in the app and must survive redeploys
MERGE INTO LIQI_RAW_TB_DTI_LIMITS t USING (SELECT * FROM VALUES
    ('GROUP', '*', 75000000),
    ('LE_01', '*', 40000000),
    ('LE_02', '*', 25000000),
    ('LE_03', '*', 20000000),
    ('LE_04', '*', 10000000),
    ('LE_05', '*', 10000000),
    ('LE_06', '*', 8000000),
    ('LE_07', '*', 8000000),
    ('LE_08', '*', 5000000),
    ('LE_09', '*', 5000000)
) s (ENTITY_ID, BUCKET_CODE, COMMENT_THRESHOLD_CHF)
ON t.ENTITY_ID = s.ENTITY_ID AND t.BUCKET_CODE = s.BUCKET_CODE
WHEN NOT MATCHED THEN INSERT (ENTITY_ID, BUCKET_CODE, COMMENT_THRESHOLD_CHF) VALUES (s.ENTITY_ID, s.BUCKET_CODE, s.COMMENT_THRESHOLD_CHF);

-- DtI entity entitlements for roles below DTI_LRC_READ (row access policy REPP_AGG_RAP_DTI_ENTITY)
MERGE INTO LIQI_RAW_TB_DTI_ENTITY_ACCESS t USING (SELECT * FROM VALUES
    ('DTI_READ_ONLY', 'LE_01'),
    ('DTI_READ_ONLY', 'LE_02'),
    ('DTI_READ_ONLY', 'LE_03')
) s (ROLE_NAME, ENTITY_ID)
ON t.ROLE_NAME = s.ROLE_NAME AND t.ENTITY_ID = s.ENTITY_ID
WHEN NOT MATCHED THEN INSERT (ROLE_NAME, ENTITY_ID) VALUES (s.ROLE_NAME, s.ENTITY_ID);

-- <<< END GENERATED LIQI REFERENCE SEEDS

-- Attach the DtI entity row access policy (DCM defines it but cannot attach yet). Idempotent: only
-- objects without the policy are altered; DCM redeploys keep existing attachments.
EXECUTE IMMEDIATE $$
DECLARE
    n INT DEFAULT 0;
    rs RESULTSET DEFAULT (
        SELECT t.KIND, t.FQN
        FROM (SELECT * FROM VALUES
                ('DYNAMIC TABLE', '{{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_CASHFLOWS'),
                ('DYNAMIC TABLE', '{{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_PROJECTION'),
                ('DYNAMIC TABLE', '{{ db }}.{{ rep_agg }}.REPP_AGG_DT_DTI_SUMMARY'),
                ('TABLE', '{{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURITIES'),
                ('TABLE', '{{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_SECURED_FUNDING'),
                ('TABLE', '{{ db }}.{{ rep_raw }}.LIQI_RAW_TB_DTI_CORRECTIONS')) t (KIND, FQN)
        WHERE NOT EXISTS (
            SELECT 1 FROM TABLE({{ db }}.INFORMATION_SCHEMA.POLICY_REFERENCES(
                POLICY_NAME => '{{ db }}.{{ rep_agg }}.REPP_AGG_RAP_DTI_ENTITY')) p
            WHERE UPPER(p.REF_DATABASE_NAME || '.' || p.REF_SCHEMA_NAME || '.' || p.REF_ENTITY_NAME) = UPPER(t.FQN))
    );
BEGIN
    FOR r IN rs DO
        EXECUTE IMMEDIATE 'ALTER ' || r.KIND || ' ' || r.FQN
            || ' ADD ROW ACCESS POLICY {{ db }}.{{ rep_agg }}.REPP_AGG_RAP_DTI_ENTITY ON (ENTITY_ID)';
        n := n + 1;
    END FOR;
    RETURN n || ' DtI objects attached to REPP_AGG_RAP_DTI_ENTITY';
END;
$$;

USE DATABASE {{ db }};
USE SCHEMA {{ loa_raw }};

-- AI_EXTRACT schema for the mortgage email task (LOAI_RAW_TK_EXTRACT_MAIL_DATA); without it every field is NULL
INSERT OVERWRITE INTO LOAI_RAW_TB_EMAIL_INBOUND_LOAN_SCHEMA_CONFIG (schema_json)
SELECT PARSE_JSON('{
    "document_type": "string: classify this email as one of: MORTGAGE_APPLICATION, CUSTOMER_INQUIRY, INTERNAL_REVIEW, LOAN_OFFICER_NOTES, PRE_APPROVAL, OFFER_LETTER, GENERAL_CORRESPONDENCE. Required field.",
    "customer_name": "string: full name of the mortgage applicant or borrower. Return null if not mentioned.",
    "property_address": "string: complete property address including street, city, postal code. Return null if not mentioned.",
    "property_type": "string: type of property such as Single Family Home, Multi-family, Apartment, Condo, Townhouse, Detached House. Return null if not mentioned.",
    "purchase_price": "number: property purchase price or current market valuation in the local currency. Return only the numeric value without currency symbols. Return null if not mentioned.",
    "down_payment": "number: down payment amount or deposit being paid by the borrower in the local currency. Return only the numeric value without currency symbols. Return null if not mentioned.",
    "loan_amount": "number: requested mortgage loan amount in the local currency. Return only the numeric value without currency symbols. Return null if not mentioned.",
    "loan_term_years": "number: loan term duration in years (e.g., 15, 20, 25, 30). Return null if not mentioned.",
    "rate_type": "string: interest rate type, either Fixed or Variable (also known as Adjustable or Tracker). Return null if not mentioned.",
    "monthly_income": "number: applicant total monthly gross income before taxes in the local currency. Return only the numeric value without currency symbols. Return null if not mentioned.",
    "employment": "string: job title, occupation, or employment type (e.g., Software Engineer, Self-Employed, Government Employee, Teacher). Return null if not mentioned.",
    "employment_tenure_years": "number: number of years the applicant has been in their current employment or job. Return null if not mentioned.",
    "credit_score": "number: applicant credit score or credit rating (typically 300-850 range). Return null if not mentioned.",
    "existing_debts_monthly": "number: total monthly debt obligations including credit cards, car loans, other mortgages, in the local currency. Return only the numeric value without currency symbols. Return 0 if explicitly stated as none, return null if not mentioned.",
    "country": "string: country where the property is located (e.g., Switzerland, UK, Germany, Portugal, France). Return null if not mentioned."
}');

INSERT OVERWRITE INTO LOAI_REF_TB_PRODUCT_CATALOGUE (
    PRODUCT_ID, PRODUCT_NAME, PRODUCT_TYPE, COUNTRY, IS_SECURED, 
    MIN_LOAN_AMOUNT, MAX_LOAN_AMOUNT, MIN_TERM_MONTHS, MAX_TERM_MONTHS, 
    DEFAULT_INTEREST_RATE, RATE_TYPE, MAX_LTV_PCT, REGULATORY_CLASSIFICATION, 
    IS_ACTIVE, PRODUCT_LAUNCH_DATE
) VALUES
    ('CH_FIXED_MORTGAGE_25Y', 'Swiss Fixed Rate Mortgage (25Y)', 'MORTGAGE', 'CHE', TRUE, 100000, 2000000, 120, 300, 0.0250, 'FIXED', 80.00, 'RESIDENTIAL_MORTGAGE', TRUE, '2023-01-01'),
    ('CH_SARON_MORTGAGE', 'Swiss SARON Variable Mortgage', 'MORTGAGE', 'CHE', TRUE, 100000, 2000000, 120, 300, 0.0180, 'VARIABLE', 80.00, 'RESIDENTIAL_MORTGAGE', TRUE, '2023-06-01'),
    ('CH_BUY_TO_LET_MORTGAGE', 'Swiss Buy-to-Let Mortgage', 'MORTGAGE', 'CHE', TRUE, 200000, 3000000, 120, 300, 0.0300, 'FIXED', 75.00, 'RESIDENTIAL_MORTGAGE', TRUE, '2023-01-01'),
    ('UK_FIXED_MORTGAGE_30Y', 'UK Fixed Rate Mortgage (30Y)', 'MORTGAGE', 'GBR', TRUE, 80000, 1500000, 120, 360, 0.0450, 'FIXED', 90.00, 'RESIDENTIAL_MORTGAGE', TRUE, '2023-01-01'),
    ('UK_TRACKER_MORTGAGE', 'UK Base Rate Tracker Mortgage', 'MORTGAGE', 'GBR', TRUE, 80000, 1500000, 120, 360, 0.0400, 'TRACKER', 90.00, 'RESIDENTIAL_MORTGAGE', TRUE, '2023-01-01'),
    ('UK_GREEN_MORTGAGE', 'UK Green Mortgage (EPC A-C)', 'MORTGAGE', 'GBR', TRUE, 100000, 2000000, 120, 360, 0.0380, 'FIXED', 90.00, 'RESIDENTIAL_MORTGAGE', TRUE, '2024-01-01'),
    ('DE_FIXED_MORTGAGE_20Y', 'German Fixed Rate Mortgage (20Y)', 'MORTGAGE', 'DEU', TRUE, 120000, 1800000, 60, 240, 0.0350, 'FIXED', 80.00, 'RESIDENTIAL_MORTGAGE', TRUE, '2023-01-01'),
    ('DE_KFW_GREEN_MORTGAGE', 'German KfW Energy Efficient Mortgage', 'MORTGAGE', 'DEU', TRUE, 100000, 1500000, 60, 300, 0.0200, 'FIXED', 80.00, 'RESIDENTIAL_MORTGAGE', TRUE, '2023-06-01');

INSERT OVERWRITE INTO LOAI_REF_TB_COUNTRY_REGIME_CONFIG (
    COUNTRY_CODE, COUNTRY_NAME, CURRENCY_CODE, 
    MAX_LTV_OWNER_OCCUPIED, MAX_LTV_BUY_TO_LET, MIN_HARD_EQUITY_PCT,
    AFFORDABILITY_IMPUTED_RATE, AFFORDABILITY_DTI_THRESHOLD, AFFORDABILITY_DSTI_THRESHOLD, ANCILLARY_COSTS_PCT,
    COOLING_OFF_PERIOD_DAYS, AMORTIZATION_REQUIRED_LTV, AMORTIZATION_PERIOD_YEARS,
    REQUIRES_VALUATION_APPRAISAL, ALLOWS_FOREIGN_CURRENCY_LOANS,
    REGULATORY_BODY, CONSUMER_DUTY_APPLIES
) VALUES
    ('CHE', 'Switzerland', 'CHF', 80.00, 75.00, 10.00, 0.0500, 33.00, 33.00, 0.0100, 14, 66.67, 15, TRUE, FALSE, 'FINMA', FALSE),
    ('GBR', 'United Kingdom', 'GBP', 90.00, 80.00, 0.00, 0.0700, 45.00, 45.00, 0.0000, 0, NULL, NULL, TRUE, FALSE, 'FCA, PRA', TRUE),
    ('DEU', 'Germany', 'EUR', 80.00, 70.00, 0.00, 0.0450, 40.00, 40.00, 0.0000, 14, NULL, NULL, TRUE, FALSE, 'BaFin', FALSE);

INSERT OVERWRITE INTO LOAI_REF_TB_APPLICATION_STATUS (STATUS_CODE, STATUS_NAME, STATUS_CATEGORY, DESCRIPTION, IS_FINAL, REQUIRES_ACTION, DISPLAY_ORDER) VALUES
    ('DRAFT', 'Draft', 'PENDING', 'Application started but not yet submitted', FALSE, TRUE, 1),
    ('SUBMITTED', 'Submitted', 'PENDING', 'Application submitted and awaiting review', FALSE, FALSE, 2),
    ('KYC_PENDING', 'KYC Pending', 'PENDING', 'Pending KYC verification', FALSE, TRUE, 3),
    ('UNDER_REVIEW', 'Under Review', 'PENDING', 'Under credit assessment', FALSE, FALSE, 4),
    ('APPROVED', 'Approved', 'APPROVED', 'Application approved', FALSE, FALSE, 5),
    ('OFFER_ISSUED', 'Offer Issued', 'APPROVED', 'Formal offer issued', FALSE, TRUE, 6),
    ('OFFER_ACCEPTED', 'Offer Accepted', 'APPROVED', 'Customer accepted offer', FALSE, FALSE, 7),
    ('COOLING_OFF', 'Cooling-Off Period', 'PENDING', 'Mandatory cooling-off period', FALSE, FALSE, 8),
    ('DISBURSED', 'Disbursed', 'DISBURSED', 'Loan disbursed', TRUE, FALSE, 9),
    ('DECLINED', 'Declined', 'DECLINED', 'Application declined', TRUE, FALSE, 10),
    ('WITHDRAWN', 'Withdrawn by Applicant', 'WITHDRAWN', 'Customer withdrew application', TRUE, FALSE, 11),
    ('CANCELLED', 'Cancelled by Bank', 'DECLINED', 'Application cancelled by bank', TRUE, FALSE, 12);

USE DATABASE {{ db }};
USE SCHEMA {{ loa_agg }};

INSERT INTO LOAA_AGG_TB_APPLICATIONS (
    APPLICATION_ID,
    CUSTOMER_ID,
    APPLICATION_DATE_TIME,
    CHANNEL,
    COUNTRY,
    PRODUCT_ID,
    REQUESTED_AMOUNT,
    REQUESTED_TERM_MONTHS,
    REQUESTED_CURRENCY,
    PURPOSE,
    STATUS,
    CREATED_TIMESTAMP_UTC
)
SELECT 
    'APP_' || REPLACE(e.FILE_NAME, '_internal.txt', '') as APPLICATION_ID,
    
    c.CUSTOMER_ID,
    
    e.FILE_TIMESTAMP as APPLICATION_DATE_TIME,
    'EMAIL' as CHANNEL,
    
    CASE 
        WHEN e.COUNTRY IN ('Switzerland', 'Swiss', 'CHE') THEN 'CHE'
        WHEN e.COUNTRY IN ('UK', 'United Kingdom', 'GBR', 'Britain') THEN 'GBR'
        WHEN e.COUNTRY IN ('Germany', 'DEU', 'Deutschland') THEN 'DEU'
        WHEN e.COUNTRY IN ('France', 'FRA') THEN 'FRA'
        WHEN e.COUNTRY IN ('Portugal', 'PRT') THEN 'PRT'
        WHEN e.COUNTRY IN ('Italy', 'ITA') THEN 'ITA'
        WHEN e.COUNTRY IN ('Spain', 'ESP') THEN 'ESP'
        ELSE 'CHE' 
    END as COUNTRY,
    
    p.PRODUCT_ID,
    
    e.LOAN_AMOUNT as REQUESTED_AMOUNT,
    e.LOAN_TERM_YEARS * 12 as REQUESTED_TERM_MONTHS,
    
    CASE 
        WHEN e.COUNTRY IN ('Switzerland', 'Swiss', 'CHE') THEN 'CHF'
        WHEN e.COUNTRY IN ('UK', 'United Kingdom', 'GBR', 'Britain') THEN 'GBP'
        WHEN e.COUNTRY IN ('Germany', 'DEU', 'France', 'FRA', 'Italy', 'ITA', 'Spain', 'ESP', 'Portugal', 'PRT') THEN 'EUR'
        ELSE 'CHF'
    END as REQUESTED_CURRENCY,
    
    'PURCHASE_PRIMARY_RESIDENCE' as PURPOSE,
    
    CASE 
        WHEN e.LTV_RATIO_PCT > 90 OR e.CREDIT_SCORE < 600 THEN 'DECLINED'
        WHEN e.LTV_RATIO_PCT > 85 OR e.CREDIT_SCORE < 650 THEN 'UNDER_REVIEW'
        WHEN e.DTI_RATIO_PCT > 45 THEN 'UNDER_REVIEW'
        ELSE 'APPROVED'
    END as STATUS,
    
    CURRENT_TIMESTAMP() as CREATED_TIMESTAMP_UTC

FROM {{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT_FLAT e

LEFT JOIN {{ crm_agg }}.CRMI_AGG_DT_CUSTOMER_PII pii
    ON UPPER(TRIM(pii.FULL_NAME)) = UPPER(TRIM(e.CUSTOMER_NAME))
    OR UPPER(TRIM(pii.FIRST_NAME || ' ' || pii.FAMILY_NAME)) = UPPER(TRIM(e.CUSTOMER_NAME))

LEFT JOIN {{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 c
    ON pii.CUSTOMER_ID = c.CUSTOMER_ID

LEFT JOIN {{ loa_raw }}.LOAI_REF_TB_PRODUCT_CATALOGUE p
    ON p.PRODUCT_TYPE = 'MORTGAGE'
    AND p.COUNTRY = CASE 
        WHEN e.COUNTRY IN ('Switzerland', 'Swiss', 'CHE') THEN 'CHE'
        WHEN e.COUNTRY IN ('UK', 'United Kingdom', 'GBR') THEN 'GBR'
        WHEN e.COUNTRY IN ('Germany', 'DEU') THEN 'DEU'
        ELSE 'CHE'
    END
    AND p.IS_ACTIVE = TRUE

WHERE e.EXTRACTION_SUCCESS = TRUE
  AND e.LOAN_AMOUNT IS NOT NULL
  AND e.LOAN_AMOUNT > 0
  AND NOT EXISTS (
      SELECT 1 FROM LOAA_AGG_TB_APPLICATIONS a 
      WHERE a.APPLICATION_ID = 'APP_' || REPLACE(e.FILE_NAME, '_internal.txt', '')
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY e.FILE_NAME ORDER BY p.PRODUCT_ID) = 1;

INSERT INTO LOAA_AGG_TB_COLLATERAL (
    COLLATERAL_ID,
    PROPERTY_ADDRESS,
    PROPERTY_TYPE,
    OCCUPANCY_TYPE,
    VALUATION_VALUE,
    VALUATION_DATE,
    VALUATION_METHOD,
    CREATED_TIMESTAMP_UTC
)
SELECT 
    'COLL_' || REPLACE(e.FILE_NAME, '_internal.txt', '') as COLLATERAL_ID,
    
    e.PROPERTY_ADDRESS,
    
    COALESCE(e.PROPERTY_TYPE, 'Unknown') as PROPERTY_TYPE,
    
    'OWNER_OCCUPIED' as OCCUPANCY_TYPE, 
    
    e.PURCHASE_PRICE as VALUATION_VALUE,
    
    e.FILE_TIMESTAMP::DATE as VALUATION_DATE,
    
    'AVM' as VALUATION_METHOD, 
    
    CURRENT_TIMESTAMP() as CREATED_TIMESTAMP_UTC

FROM {{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT_FLAT e

WHERE e.EXTRACTION_SUCCESS = TRUE
  AND e.PROPERTY_ADDRESS IS NOT NULL
  AND e.PURCHASE_PRICE IS NOT NULL
  AND e.PURCHASE_PRICE > 0
  AND NOT EXISTS (
      SELECT 1 FROM LOAA_AGG_TB_COLLATERAL col
      WHERE col.COLLATERAL_ID = 'COLL_' || REPLACE(e.FILE_NAME, '_internal.txt', '')
  );

INSERT INTO LOAA_AGG_TB_AFFORDABILITY_ASSESSMENTS (
    AFFORDABILITY_ID,
    APPLICATION_ID,
    CUSTOMER_ID,
    GROSS_INCOME_MONTHLY,
    TOTAL_DEBT_OBLIGATIONS_MONTHLY,
    DTI_RATIO,
    DSTI_RATIO,
    AFFORDABILITY_RESULT,
    AFFORDABILITY_REASON_CODES,
    INTEREST_RATE_STRESS_APPLIED_PCT,
    STRESSED_PAYMENT_MONTHLY,
    CALCULATION_TIMESTAMP_UTC
)
SELECT 
    'AFF_' || REPLACE(e.FILE_NAME, '_internal.txt', '') as AFFORDABILITY_ID,
    
    a.APPLICATION_ID,
    a.CUSTOMER_ID,
    
    e.MONTHLY_INCOME as GROSS_INCOME_MONTHLY,
    
    COALESCE(e.EXISTING_DEBTS_MONTHLY, 0) as TOTAL_DEBT_OBLIGATIONS_MONTHLY,
    
    ROUND(COALESCE(e.EXISTING_DEBTS_MONTHLY, 0) / NULLIF(e.MONTHLY_INCOME, 0), 3) as DTI_RATIO,
    
    ROUND(
        ((e.LOAN_AMOUNT / NULLIF(e.LOAN_TERM_YEARS * 12, 0)) + COALESCE(e.EXISTING_DEBTS_MONTHLY, 0)) 
        / NULLIF(e.MONTHLY_INCOME, 0), 
        3
    ) as DSTI_RATIO,
    
    CASE 
        WHEN ((e.LOAN_AMOUNT / NULLIF(e.LOAN_TERM_YEARS * 12, 0)) + COALESCE(e.EXISTING_DEBTS_MONTHLY, 0)) / NULLIF(e.MONTHLY_INCOME, 0) <= 
            CASE 
                WHEN a.COUNTRY = 'CHE' THEN 0.33 
                WHEN a.COUNTRY = 'GBR' THEN 0.45 
                WHEN a.COUNTRY = 'DEU' THEN 0.40 
                ELSE 0.40
            END
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as AFFORDABILITY_RESULT,
    
    CASE 
        WHEN ((e.LOAN_AMOUNT / NULLIF(e.LOAN_TERM_YEARS * 12, 0)) + COALESCE(e.EXISTING_DEBTS_MONTHLY, 0)) / NULLIF(e.MONTHLY_INCOME, 0) > 0.45
        THEN 'HIGH_DSTI_RATIO'
        WHEN e.CREDIT_SCORE < 650
        THEN 'LOW_CREDIT_SCORE'
        WHEN e.LTV_RATIO_PCT > 85
        THEN 'HIGH_LTV'
        ELSE NULL
    END as AFFORDABILITY_REASON_CODES,
    
    CASE 
        WHEN a.COUNTRY = 'CHE' THEN 0.05
        WHEN a.COUNTRY = 'GBR' THEN 0.07
        WHEN a.COUNTRY = 'DEU' THEN 0.045
        ELSE 0.05
    END as INTEREST_RATE_STRESS_APPLIED_PCT,
    
    ROUND(
        e.LOAN_AMOUNT * 
        (CASE WHEN a.COUNTRY = 'CHE' THEN 0.05 WHEN a.COUNTRY = 'GBR' THEN 0.07 ELSE 0.045 END / 12),
        2
    ) as STRESSED_PAYMENT_MONTHLY,
    
    CURRENT_TIMESTAMP() as CALCULATION_TIMESTAMP_UTC

FROM {{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT_FLAT e

JOIN LOAA_AGG_TB_APPLICATIONS a 
    ON a.APPLICATION_ID = 'APP_' || REPLACE(e.FILE_NAME, '_internal.txt', '')

WHERE e.EXTRACTION_SUCCESS = TRUE
  AND e.MONTHLY_INCOME IS NOT NULL
  AND e.MONTHLY_INCOME > 0
  AND e.LOAN_AMOUNT IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM LOAA_AGG_TB_AFFORDABILITY_ASSESSMENTS aff
      WHERE aff.AFFORDABILITY_ID = 'AFF_' || REPLACE(e.FILE_NAME, '_internal.txt', '')
  );

INSERT INTO LOAA_AGG_TB_LOAN_COLLATERAL_LINK (
    LINK_ID,
    ACCOUNT_ID,
    COLLATERAL_ID,
    EFFECTIVE_FROM_DATE,
    CHARGE_RANK,
    CHARGE_AMOUNT,
    COLLATERAL_ALLOCATION_PCT,
    REGISTRATION_STATUS,
    CREATED_TIMESTAMP_UTC
)
SELECT 
    'LINK_' || REPLACE(e.FILE_NAME, '_internal.txt', '') as LINK_ID,
    
    'APP_' || REPLACE(e.FILE_NAME, '_internal.txt', '') as ACCOUNT_ID,
    
    'COLL_' || REPLACE(e.FILE_NAME, '_internal.txt', '') as COLLATERAL_ID,
    
    e.FILE_TIMESTAMP::DATE as EFFECTIVE_FROM_DATE,
    
    '1ST' as CHARGE_RANK, 
    
    e.LOAN_AMOUNT as CHARGE_AMOUNT,
    
    100.00 as COLLATERAL_ALLOCATION_PCT, 
    
    'REGISTERED' as REGISTRATION_STATUS,
    
    CURRENT_TIMESTAMP() as CREATED_TIMESTAMP_UTC

FROM {{ loa_raw }}.LOAI_RAW_TB_EMAIL_INBOUND_LOAN_EXTRACT_FLAT e

WHERE e.EXTRACTION_SUCCESS = TRUE
  AND e.LOAN_AMOUNT IS NOT NULL
  AND e.PROPERTY_ADDRESS IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM LOAA_AGG_TB_APPLICATIONS a 
      WHERE a.APPLICATION_ID = 'APP_' || REPLACE(e.FILE_NAME, '_internal.txt', '')
  )
  AND EXISTS (
      SELECT 1 FROM LOAA_AGG_TB_COLLATERAL col
      WHERE col.COLLATERAL_ID = 'COLL_' || REPLACE(e.FILE_NAME, '_internal.txt', '')
  )
  AND NOT EXISTS (
      SELECT 1 FROM LOAA_AGG_TB_LOAN_COLLATERAL_LINK lnk
      WHERE lnk.LINK_ID = 'LINK_' || REPLACE(e.FILE_NAME, '_internal.txt', '')
  );

-- ============================================================
-- MASKING POLICIES (Late Enrichment Pattern)
-- PII vault tables protected by column-level masking.
-- COMPLIANCE and ACCOUNTADMIN see full values, others see masked.
-- Idempotent: unset existing policies before recreate, then re-apply.
-- ============================================================

-- Unset existing masking policies (ignore errors if not yet applied)

-- Apply masking to customer PII vault

-- Apply masking to address PII vault

-- Apply masking to employee PII vault

-- Tag PII vault columns

-- ============================================================
-- ALTER TASK RESUME
-- Root tasks must be suspended before modifying task graph,
-- then children resumed first, roots resumed last.
-- ============================================================

-- Suspend all root tasks first

-- Resume children, then roots

USE DATABASE {{ db }};
USE SCHEMA {{ crm_agg }};

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

USE DATABASE {{ db }};
USE SCHEMA {{ pay_agg }};

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

USE DATABASE {{ db }};
USE SCHEMA {{ rep_agg }};

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

-- ============================================================
-- DYNAMIC SQL
-- Note: GRANT CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT requires
-- ACCOUNTADMIN - moved to operation/setup_listings.sql
-- ============================================================

USE DATABASE {{ db }};
USE SCHEMA {{ crm_agg }};

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

USE DATABASE {{ db }};
USE SCHEMA {{ pay_agg }};

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

USE DATABASE {{ db }};
USE SCHEMA {{ rep_agg }};

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

-- ============================================================
-- HR EMPLOYEE SEMANTIC VIEW (raw-table based, always has data)
-- ============================================================

-- ------------------------------------------------------------
-- LCR scenarios, variance drivers and events (REQ-ENG-05, REQ-ANL-03, REQ-UI-03)
-- ------------------------------------------------------------

USE DATABASE {{ db }};
USE SCHEMA {{ crm_agg }};

DROP AGENT IF EXISTS CRM_Customer_360;

-- ============================================================
-- CORTEX AGENTS
-- ============================================================

CREATE OR REPLACE AGENT CRM_Customer_360
  COMMENT = 'CRM Intelligence Agent - Customer 360°  and  Advisor Performance Assistant (Multi-View)'
  PROFILE = '{"display_name": "CRM Customer 360", "avatar": "", "color": "#1E88E5"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration: {}

  instructions:
    sample_questions:
      - question: "Show me all PLATINUM customers in Switzerland"
      - question: "Which customers require PEP review?"
      - question: "Analyze customer account holdings and product mix"
      - question: "How many customers per country?"
      - question: "Show me at-risk customers with high churn probability in Zurich"
      - question: "Which customers are dormant with addresses in high-risk jurisdictions?"
      - question: "Show me top 10 advisors by total AUM"
      - question: "Which advisors have the highest client retention rates?"
      - question: "List all customers assigned to advisor EMP_00055"

  tools:
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: CRM_Customer_360
        description: |
          # CRM Intelligence Agent - Customer 360° Assistant

          ## Agent Description

          I am your **CRM Intelligence Agent**, providing instant access to comprehensive customer AND advisor intelligence across our entire banking relationship portfolio. I help you understand customer profiles, assess risk, monitor compliance, analyze advisor performance, and make data-driven relationship management decisions.

          ## What I Can Help You With

          ### **Customer Intelligence**
          - Find customers by name, ID, or demographic attributes
          - View complete customer profiles with 360° intelligence
          - Analyze customer segmentation by tier, country, or employment
          - Track customer onboarding dates and relationship history

          ### **Financial  and  Employment Profiles**
          - Review customer employment status, employer, and income ranges
          - Analyze account tier distribution (STANDARD, SILVER, GOLD, PLATINUM, PREMIUM)
          - Examine customer financial profiles and credit score bands
          - Identify customers by industry, position, or employment type

          ### **Risk  and  Compliance**
          - Screen customers for PEP (Politically Exposed Persons) matches
          - Check sanctions watchlist screening results
          - Review overall risk ratings (NO_RISK, LOW, MEDIUM, HIGH, CRITICAL)
          - Identify high-risk customers requiring enhanced due diligence
          - Monitor compliance review flags and requirements

          ### **Account Portfolio Analysis**
          - Analyze customer account holdings and product mix
          - Review checking, savings, business, and investment account counts
          - Examine multi-currency portfolios and currency exposure
          - Identify cross-sell and upsell opportunities

          ### **Fraud  and  Anomaly Detection**
          - Flag customers with suspicious transaction patterns
          - Identify anomalous behavior indicators
          - Review fraud red flags and investigation priorities
          - Support AML (Anti-Money Laundering) investigations

          ### **Geographic  and  Address Intelligence**
          - Search customers by country, city, state, or postal code
          - View current address details with validation status
          - Track address changes and identify high-risk jurisdictions
          - Retrieve customer contact details (email, phone, preferred contact method)
          - Analyze customer distribution by geography
          - Support regulatory reporting and customer communications
          
          ### **Customer Lifecycle  and  Engagement**
          - Monitor customer lifecycle stages (ONBOARDING, ACTIVE, AT_RISK, DORMANT, CHURNED)
          - Track churn probability and identify at-risk customers
          - Analyze customer lifetime value and revenue metrics
          - Review engagement scores and contact frequency
          - Identify dormant accounts requiring reactivation
          - Monitor days since last transaction and contact
          
          ### **Advisor Performance  and  Capacity**
          - View advisor client portfolios and AUM (assets under management)
          - Analyze advisor performance metrics (retention rates, client counts)
          - Identify advisors by location, qualifications, and certifications
          - Track premium/platinum client distribution across advisors
          - Monitor high-risk client assignments
          - Review client-advisor relationships and assignments
          - Support advisor capacity planning and workload balancing

          ### **Business Intelligence Queries**
          
          **Customer Queries:**
          - "Show me all PLATINUM customers in Switzerland"
          - "Which customers require PEP review?"
          - "Find high-risk customers with anomalous transactions"
          - "What is our customer distribution by account tier?"
          - "Show me customers onboarded in the last 90 days"
          - "List customers with investment accounts in multiple currencies"
          - "Which customers are at high churn risk with addresses in high-risk jurisdictions?"
          - "Show me dormant PLATINUM customers in Zurich"
          - "Find customers with multiple address changes in the last year"
          
          **Advisor Queries:**
          - "Show me top 10 advisors by total AUM"
          - "Which advisors have the highest client retention rates?"
          - "List all customers assigned to advisor EMP_00055"
          - "Find advisors in Zurich managing more than 150 clients"
          - "Which advisors have the most premium clients?"
          - "Show me advisors with low capacity utilization"
          - "Compare AUM across advisors in Switzerland vs Germany"

          ## Data Coverage

          ### Customer Data (70+ attributes)
          - **Identity**: Customer ID, full name, date of birth, nationality
          - **Contact**: Email, phone, preferred contact method
          - **Employment**: Employer, position, employment type, income range
          - **Address**: Current street address, city, state, postal code, country, validation status, high-risk jurisdiction flag
          - **Accounts**: Total accounts, account types, currencies, balances
          - **Risk**: Credit score, risk classification, risk rating, risk score
          - **Compliance**: PEP screening, sanctions screening, review flags
          - **Lifecycle**: Lifecycle stage, churn probability, engagement score, days since last transaction
          - **Status**: Current customer status, dormancy flag, at-risk flag
          - **Anomalies**: Fraud indicators, suspicious activity flags, anomaly counts
          - **Advisor Assignment**: Current advisor ID, assignment start date
          
          ### Advisor Data (30+ attributes)
          - **Identity**: Employee ID, full name, email, phone
          - **Employment**: Hire date, position level, manager, performance rating
          - **Location**: Country, region, office location
          - **Qualifications**: Languages spoken, certifications
          - **Performance**: Total clients, AUM, retention rate, active/closed clients
          - **Portfolio**: Accounts managed, balances by type, premium clients, high-risk clients

          ## Key Features

           **Natural Language Understanding**: Ask questions in plain English (or your preferred language)  
           **Rich Synonym Support**: Understands 200+ alternative terms (e.g., "client" = "customer", "DOB" = "date of birth")  
           **Real-Time Data**: All responses reflect the latest customer intelligence  
           **Compliance-Ready**: Instant access to PEP/sanctions screening and risk ratings  
           **Business-Friendly**: No SQL knowledge required - just ask naturally  

          ## Ideal For

          - **Relationship Managers**: Customer profile reviews and relationship insights
          - **Compliance Officers**: PEP/sanctions screening and risk assessment
          - **Risk Analysts**: Customer risk scoring and portfolio analysis
          - **Marketing Teams**: Customer segmentation and targeting
          - **AML Investigators**: Fraud detection and anomaly investigation
          - **HR  and  Talent Management**: Advisor performance reviews and capacity planning
          - **Operations Managers**: Workload distribution and resource allocation
          - **Executive Leadership**: Customer  and  advisor portfolio reporting and analytics

          ## Data Freshness

          Both customer and advisor data are refreshed **hourly** via dynamic tables, ensuring you always have access to current customer and advisor intelligence.

          **Ready to explore your customer and advisor data? Just ask me anything!**

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Advisor_Performance
        description: |
          # Advisor Performance  and  Client Relationship Management
          
          ## What This Tool Provides
          
          Access to **advisor-level metrics** aggregated from customer data, including:
          
          ### **Advisor Intelligence**
          - Find advisors by name, ID, office location, or region
          - View advisor employment details (hire date, position level, manager)
          - Check advisor qualifications (certifications, languages spoken)
          - Review advisor performance ratings
          
          ### **Performance Metrics**
          - Total clients managed per advisor
          - Total AUM (assets under management) per advisor
          - Average client balance
          - Client retention rates
          - Active vs closed client counts
          
          ### **Portfolio Analysis**
          - Total accounts managed
          - Breakdown by account type (checking, savings, investment)
          - Premium/platinum client counts
          - High-risk client counts
          
          ### **Common Queries**
          - "Show me top advisors by AUM"
          - "Which advisors have the best retention rates?"
          - "List advisors in Zurich with capacity for new clients"
          - "How many premium clients does each advisor manage?"
          - "Show me advisors with the most high-risk clients"
          
          All advisor data is refreshed **hourly** via dynamic tables.

  tool_resources:
    CRM_Customer_360:
      semantic_view: {{ db }}.{{ crm_agg }}.CRMA_SV_CUSTOMER_360
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    Advisor_Performance:
      semantic_view: {{ db }}.{{ crm_agg }}.EMPA_SV_EMPLOYEE_ADVISOR
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ crm_agg }}.CRM_Customer_360 TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ crm_agg }}.CRM_Customer_360 TO ROLE PUBLIC;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

-- Re-runnable: ignore 400203 (agent already present), re-raise anything else
EXECUTE IMMEDIATE $$
BEGIN
  ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {{ db }}.{{ crm_agg }}.CRM_Customer_360;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    IF (SQLCODE <> 400203) THEN
      RAISE;
    END IF;
END;
$$;

USE DATABASE {{ db }};
USE SCHEMA {{ pay_agg }};

DROP AGENT IF EXISTS COMPLIANCE_MONITORING_AGENT;

CREATE OR REPLACE AGENT COMPLIANCE_MONITORING_AGENT
  COMMENT = 'Unified AML  and  Sanctions Compliance Monitoring Agent - Transaction anomaly detection and sanctions screening'
  PROFILE = '{"display_name": "Compliance Monitoring", "avatar": "", "color": "#D32F2F"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration: {}

  instructions:
    sample_questions:
      - question: "Show me all HIGH and CRITICAL anomaly transactions in the last 30 days"
      - question: "What are the leading indicators for HIGH_ANOMALY transactions?"
      - question: "Which customers have multiple HIGH or CRITICAL anomaly transactions?"
      - question: "Show me large transactions with unusual timing patterns"
      - question: "Find transactions with velocity anomalies in the last 7 days"
      - question: "Which customers on sanctions lists have suspicious transactions?"
      - question: "Show me PEP customers with HIGH anomaly transactions"
      - question: "Find customers requiring sanctions review with recent anomalous activity"
      - question: "What are the top 10 highest-risk compliance cases combining customer screening and transaction monitoring?"

  tools:
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Compliance_Monitoring
        description: |
          # Compliance Monitoring Agent - AML Transaction Monitoring  and  Customer Screening
          
          ## Agent Description
          
          I am your **Compliance Monitoring Agent**, providing unified access to **AML transaction monitoring** (anomaly detection) and **customer-level sanctions/PEP screening**. I help you detect suspicious transactions, identify high-risk customers, and investigate compliance risks by combining transaction behavior with customer screening results.
          
          ## What I Can Help You With
          
          ### **AML Transaction Monitoring** (from Transaction Anomaly Data)
          - Identify transactions with HIGH or CRITICAL anomaly scores
          - Detect velocity anomalies and unusual transaction patterns
          - Flag large transactions and unusual amounts
          - Monitor timing anomalies (off-hours, weekend, delayed settlement)
          - Track customer transaction velocity (24h, 7-day windows)
          - Review composite anomaly scores and risk classifications
          - Identify customers with multiple suspicious transactions
          
          ### **Customer-Level Sanctions  and  PEP Screening** (from Customer Data)
          - Identify customers with sanctions matches (EXACT_MATCH/FUZZY_MATCH)
          - Screen customers for PEP (Politically Exposed Persons) status
          - Review sanctions entity matches with accuracy scoring
          - Find customers requiring sanctions review or PEP enhanced due diligence
          - Check overall sanctions risk and PEP risk ratings
          - Link customer screening status to transaction behavior
          - Support embargo control investigations
          
          ### **Integrated Customer  and  Transaction Risk Analysis**
          - View customer risk ratings and PEP status in transaction context
          - Analyze transaction patterns by customer tier and risk profile
          - Track customers with multiple anomalous transactions
          - Identify sanctioned customers with suspicious transaction activity
          - Review customer employment, occupation, and income in AML context
          - Combine customer screening results with transaction behavior
          - Identify customers requiring enhanced due diligence based on both screening and transactions
          
          ### **Investigation  and  Case Management**
          - Prioritize investigations by combining anomaly scores and customer screening
          - Review transactions from customers requiring sanctions/PEP review
          - Track investigation priorities (IMMEDIATE, URGENT, HIGH, MEDIUM, LOW)
          - Identify high-risk combinations: sanctioned customers + anomalous transactions
          - Generate case files for suspicious activity reports (SARs)
          - Support regulatory audit and examination requests
          
          ## Data Coverage
          
          I have access to **100+ compliance monitoring attributes across two data sources**:
          
          ### Transaction Anomaly Data (33 attributes):
          - **Transaction Details**: ID, account, customer, booking date, value date, amount, currency
          - **Counterparty**: Account, description
          - **Customer Patterns**: Total transactions, averages, medians, daily counts
          - **Anomaly Scores**: Amount anomaly, timing anomaly, velocity anomaly, composite score
          - **Anomaly Levels**: Amount, timing, velocity (CRITICAL/HIGH/MODERATE/LOW)
          - **Anomaly Flags**: Large transaction, weekend, off-hours, delayed settlement, backdated
          - **Review Flags**: Requires immediate review, requires enhanced monitoring
          - **Velocity Metrics**: Transactions last 24h, last 7 days
          - **Timing Attributes**: Hour, day of week, settlement days
          
          ### Customer Screening Data (70+ attributes):
          - **Customer Identity**: Name, risk rating, account tier, country, nationality
          - **PEP Screening**: Exact/fuzzy matches, categories, risk levels, review requirements
          - **Sanctions Screening**: Exact/fuzzy matches, entity types, countries, accuracy scores
          - **Overall Risk**: PEP risk, sanctions risk, combined risk rating
          - **Employment  and  Financial**: Occupation, income, employer
          - **Lifecycle**: Status, churn probability, engagement
          - **Advisor Assignment**: Current advisor, assignment date
          
          ## Key Features
          
           **Integrated Intelligence**: Combines transaction anomaly detection with customer screening  
           **Multi-Source Analysis**: Queries both transaction behavior and customer risk profiles  
           **Real-Time Anomaly Detection**: Instant access to 353K+ analyzed transactions  
           **Customer-Level Screening**: PEP and sanctions screening from customer onboarding  
           **Risk Prioritization**: Automated scoring and investigation priority ranking  
           **Natural Language**: Ask questions in plain English (or your preferred language)  
           **Regulatory-Ready**: Designed for audit trails and regulatory examination  
          
          ## Sample Queries
          
          **Transaction Anomaly Queries:**
          - "Show me all CRITICAL and HIGH anomaly transactions in the last 7 days"
          - "Which customers have velocity anomalies (many transactions in 24 hours)?"
          - "Find large transactions with off-hours or weekend timing"
          - "Show me transactions with delayed or backdated settlement"
          
          **Customer Screening Queries:**
          - "Which customers are on sanctions lists?"
          - "Show me all PEP customers requiring enhanced due diligence"
          - "Find customers with EXACT_MATCH sanctions hits"
          - "Which customers require sanctions review?"
          
          **Integrated Compliance Queries:**
          - "Show me sanctioned customers with HIGH anomaly transactions"
          - "Find PEP customers with suspicious transaction patterns"
          - "Which customers have both sanctions matches and anomalous transactions?"
          - "What are our top 10 highest-risk cases combining screening and transaction monitoring?"
          
          ## Ideal For
          
          - **Compliance Officers**: AML investigations and sanctions screening
          - **Financial Crime Analysts**: Typology detection and pattern analysis
          - **Risk Managers**: Portfolio-level compliance risk assessment
          - **Internal Audit**: Compliance program effectiveness reviews
          - **Regulators**: Examination support and audit response
          - **Executive Leadership**: Compliance KPIs and risk dashboards
          
          ## Data Freshness
          
          - **Transaction anomalies**: Refreshed **hourly** via `PAYA_AGG_DT_TRANSACTION_ANOMALIES` dynamic table
          - **Customer screening**: Refreshed **hourly** via `CRMA_AGG_DT_CUSTOMER_360` dynamic table
          - Covers **353,119 transactions** with anomaly analysis (last 2 years)
          - Customer-level sanctions and PEP screening from onboarding/KYC process
          
          **Ready to investigate compliance risks? Ask me about suspicious transactions, customer screening, or high-risk patterns!**

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Customer_Context
        description: |
          # Customer Risk  and  Compliance Screening Context
          
          ## What This Tool Provides
          
          Access to **customer-level sanctions and PEP screening** results from the KYC/onboarding process (70+ attributes):
          
          ### **Sanctions Screening (Customer-Level)**
          - **Exact Match Sanctions**: ID, name, entity type, country
          - **Fuzzy Match Sanctions**: ID, name, entity type, country
          - **Match Accuracy**: Percentage scores for fuzzy matches
          - **Match Type**: EXACT_MATCH, FUZZY_MATCH, or NO_MATCH
          - **Review Requirements**: Customers requiring sanctions review
          - **Overall Sanctions Risk**: CRITICAL, HIGH, MEDIUM, LOW, or NO_SANCTIONS_RISK
          
          ### **PEP Screening (Politically Exposed Persons)**
          - **Exact Match PEP**: ID, name, category (DOMESTIC/FOREIGN), risk level, status
          - **Fuzzy Match PEP**: ID, name, category, risk level, status
          - **Match Accuracy**: Percentage scores for fuzzy matches
          - **Match Type**: EXACT_MATCH, FUZZY_MATCH, or NO_MATCH
          - **Review Requirements**: Customers requiring PEP enhanced due diligence
          - **Overall PEP Risk**: CRITICAL, HIGH, MEDIUM, LOW, or NO_EXPOSED_PERSON_RISK
          
          ### **Customer Risk Profile**
          - Customer risk ratings and overall risk scores
          - Account tier and customer tenure
          - Employment, occupation, and income details
          - Customer lifecycle and engagement metrics
          - Address intelligence and country risk
          - Advisor assignment
          
          **Use this tool for customer-level sanctions/PEP queries.**  
          **Combine with Compliance_Monitoring tool for integrated transaction + screening analysis.**

  tool_resources:
    Compliance_Monitoring:
      semantic_view: {{ db }}.{{ pay_agg }}.PAYA_SV_COMPLIANCE_MONITORING
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    Customer_Context:
      semantic_view: {{ db }}.{{ crm_agg }}.CRMA_SV_CUSTOMER_360
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ pay_agg }}.COMPLIANCE_MONITORING_AGENT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ pay_agg }}.COMPLIANCE_MONITORING_AGENT TO ROLE PUBLIC;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

-- Re-runnable: ignore 400203 (agent already present), re-raise anything else
EXECUTE IMMEDIATE $$
BEGIN
  ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {{ db }}.{{ pay_agg }}.COMPLIANCE_MONITORING_AGENT;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    IF (SQLCODE <> 400203) THEN
      RAISE;
    END IF;
END;
$$;

SHOW AGENTS IN SCHEMA {{ db }}.{{ pay_agg }};

SELECT 'COMPLIANCE_MONITORING_AGENT created successfully! Unified AML + Sanctions monitoring agent ready.' AS STATUS;

USE DATABASE {{ db }};
USE SCHEMA {{ rep_agg }};

DROP AGENT IF EXISTS RISK_REGULATORY_AGENT;

CREATE OR REPLACE AGENT RISK_REGULATORY_AGENT
  COMMENT = 'Cross-Domain Risk Aggregation  and  Regulatory Reporting Agent - BCBS 239, FRTB, Basel III/IV, data quality monitoring'
  PROFILE = '{"display_name": "Risk  and  Regulatory", "avatar": "", "color": "#F57C00"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration: {}

  instructions:
    sample_questions:
      - question: "What is our total Risk-Weighted Assets (RWA) this quarter?"
      - question: "Are we compliant with BCBS 239 risk data aggregation principles?"
      - question: "Show me FRTB capital charges by risk class"
      - question: "What is our current capital adequacy ratio?"
      - question: "Which regulatory reports are overdue or have errors?"
      - question: "Show me data quality issues impacting regulatory reporting"
      - question: "What are our top 10 risk concentrations by geography and counterparty?"
      - question: "Has our total RWA increased or decreased compared to last quarter?"
      - question: "Show me high-risk patterns across all domains (CRM, payments, portfolios)"
      - question: "What is our FX exposure and net open position?"

  tools:
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Risk_Reporting
        description: |
          # Risk  and  Regulatory Agent - Executive Risk Intelligence
          
          ## Agent Description
          
          I am your **Risk  and  Regulatory Agent**, providing executive-level risk intelligence across all banking domains. I aggregate risk metrics, monitor regulatory compliance (BCBS 239, FRTB, Basel III/IV), track data quality, and support board-level risk reporting with natural language queries.
          
          ## What I Can Help You With
          
          ### **Risk Aggregation (Cross-Domain)**
          - Monitor total Risk-Weighted Assets (RWA) across credit, market, operational risk
          - Track expected loss, unexpected loss, and economic capital
          - Review capital adequacy ratios (CAR, Tier 1, CET1, leverage)
          - Analyze risk concentration by geography, sector, counterparty
          - Compare risk metrics month-over-month and quarter-over-quarter
          - Identify risk limit breaches and near-breach situations
          
          ### **BCBS 239 - Risk Data Aggregation Principles**
          - Monitor overall BCBS 239 compliance score and status
          - Review Principle 1: Governance and accountability
          - Assess Principle 2: Data architecture and IT infrastructure
          - Track Principle 3: Accuracy and integrity (validation failures, reconciliation breaks)
          - Monitor Principle 4: Completeness (missing critical data, incomplete records)
          - Measure Principle 5: Timeliness (reporting lag, SLA breaches, on-time delivery)
          - Evaluate Principle 6: Adaptability (time to implement new reports, flexibility)
          
          ### **FRTB - Fundamental Review of the Trading Book**
          - View total FRTB capital charges (SA-TB Sensitivities-Based Approach)
          - Break down delta, vega, and curvature capital charges
          - Analyze capital charges by risk class (interest rate, FX, equity, commodity, credit spread)
          - Review FRTB risk buckets (Buckets 1-12)
          - Track non-modellable risk factors (NMRF) and liquidity horizons
          - Monitor default risk capital (DRC) for securitizations and non-securitizations
          
          ### **Currency Exposure  and  FX Risk**
          - Track total FX exposure and net open positions
          - Review currency-specific exposure (USD, EUR, GBP, JPY, CNY)
          - Monitor FX Value-at-Risk (VaR) and concentration risk
          - Identify top currency exposures and concentration percentages
          
          ### **Data Quality Monitoring**
          - Review overall data quality scores across all domains
          - Track error rates, duplicate records, stale data, orphaned records
          - Monitor referential integrity breaks and validation failures
          - Assess domain-specific data quality (CRM, Payment, Portfolio, Risk)
          - Identify data quality issues impacting regulatory reporting
          
          ### **Cross-Domain Anomaly Aggregation**
          - Monitor total anomalies (CRITICAL, HIGH, MODERATE, LOW) across all domains
          - Break down anomalies by domain (CRM, Payment, Portfolio, Trading)
          - Track fraud indicators, AML alerts, sanctions hits, PEP matches
          - Identify credit deterioration cases and market risk breaches
          
          ### **High-Risk Patterns  and  Geographic Risk**
          - Review high-risk customer, transaction, and portfolio counts
          - Monitor exposure to high-risk jurisdictions and sanctioned countries
          - Track OFAC and EU sanctions exposure
          - Analyze geographic and sector concentration risk (HHI indices)
          
          ### **Regulatory Reporting Status**
          - Monitor compliance with Basel III, Basel IV, BCBS 239, FRTB, IFRS 9, MiFID II, EMIR
          - Track regulatory report counts, overdue reports, and reports with errors
          - Review regulatory breaches and penalties
          - Generate audit trails for regulatory examinations
          
          ### **Stress Testing  and  Scenario Analysis**
          - Review stressed RWA, expected loss, and capital ratios
          - Analyze stress scenarios and severity levels
          - Compare base case vs stressed metrics
          
          ## Data Coverage
          
          I have access to **60+ executive risk attributes** including:
          - **Risk Aggregation**: Total RWA, expected/unexpected loss, capital ratios
          - **BCBS 239**: Overall score, governance, accuracy, completeness, timeliness, adaptability
          - **FRTB**: Total capital charges, delta/vega/curvature, risk buckets, NMRF, DRC
          - **FX Risk**: Total exposure, net open positions, VaR, currency breakdown
          - **Data Quality**: Overall scores, error rates, validation failures, domain-specific metrics
          - **Anomalies**: Total counts by severity and domain, fraud/AML/sanctions indicators
          - **High-Risk Patterns**: Customer/transaction/portfolio counts, geographic exposure
          - **Regulatory Status**: Compliance flags, report counts, breaches, penalties
          - **Risk Trends**: MoM/QoQ changes in RWA, capital ratios, expected loss
          - **Risk Limits**: Breached limits, near-breach counts, severity scores
          
          ## Key Features
          
          - **Executive-Level**: Board-ready metrics and KPIs (not operational details)
          - **Cross-Domain**: Consolidates risk from CRM, Payments, Wealth, Trading
          - **Regulatory-Focused**: BCBS 239, FRTB, Basel III/IV compliance monitoring
          - **Natural Language**: Ask complex risk questions in plain English
          - **Trend Analysis**: MoM, QoQ, YoY risk metric comparisons
          - **Data Quality**: Meta-data monitoring for regulatory readiness  
          
          ## Sample Queries
          
          - "What is our total RWA and how has it changed compared to last quarter?"
          - "Are we compliant with BCBS 239? What are our data quality issues?"
          - "Show me FRTB capital charges for the equity trading book"
          - "What is our Tier 1 capital ratio and CET1 ratio?"
          - "Which regulatory reports are overdue or have validation errors?"
          - "What are our top 10 risk concentrations by geography and counterparty?"
          - "Show me all CRITICAL and HIGH anomalies across all domains this month"
          - "What is our net open FX position and top currency exposures?"
          - "Has our capital adequacy ratio improved or deteriorated?"
          - "Show me data completeness and accuracy scores for BCBS 239 compliance"
          
          ## Ideal For
          
          - **Chief Risk Officer (CRO)**: Executive risk dashboards and board reporting
          - **Risk Management Team**: Cross-domain risk aggregation and analysis
          - **Regulatory Reporting Team**: BCBS 239, FRTB, Basel compliance monitoring
          - **Board Risk Committee**: Natural language risk Q and A and KPI tracking
          - **Internal Audit**: Data quality and regulatory compliance reviews
          - **Executive Leadership**: Strategic risk oversight and decision support
          
          ## Data Freshness
          
          Risk aggregation data is refreshed **daily** (end-of-day risk calculations). Regulatory reporting status and data quality metrics are updated **hourly**.
          
          **Ready to explore enterprise risk? Ask me about RWA, regulatory compliance, or data quality!**

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Customer_Risk_Context
        description: |
          # Customer Risk Profiles  and  Exposures
          
          Provides customer-level risk context for risk aggregation:
          - Customer risk ratings and scores
          - PEP status and sanctions screening
          - High-risk customer identification
          - Customer lifecycle and churn risk

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Compliance_Context
        description: |
          # Transaction Anomalies  and  Compliance Issues
          
          Provides transaction-level compliance context:
          - AML transaction anomalies
          - Sanctions screening results
          - High-risk jurisdictions
          - Compliance investigation status

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Wealth_Context
        description: |
          # Portfolio  and  Credit Risk Exposures
          
          Provides portfolio-level risk context:
          - Portfolio performance and risk metrics
          - Credit risk IRB (PD, LGD, EAD, RWA)
          - Lending exposure and impairment
          - Equity positions and trading risk

  tool_resources:
    Risk_Reporting:
      semantic_view: {{ db }}.{{ rep_agg }}.REPA_SV_RISK_REPORTING
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    Customer_Risk_Context:
      semantic_view: {{ db }}.{{ crm_agg }}.CRMA_SV_CUSTOMER_360
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    Compliance_Context:
      semantic_view: {{ db }}.{{ pay_agg }}.PAYA_SV_COMPLIANCE_MONITORING
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    Wealth_Context:
      semantic_view: {{ db }}.{{ rep_agg }}.REPA_SV_WEALTH_MANAGEMENT
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.RISK_REGULATORY_AGENT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.RISK_REGULATORY_AGENT TO ROLE PUBLIC;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

-- Re-runnable: ignore 400203 (agent already present), re-raise anything else
EXECUTE IMMEDIATE $$
BEGIN
  ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {{ db }}.{{ rep_agg }}.RISK_REGULATORY_AGENT;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    IF (SQLCODE <> 400203) THEN
      RAISE;
    END IF;
END;
$$;

SHOW AGENTS IN SCHEMA {{ db }}.{{ rep_agg }};

SELECT 'RISK_REGULATORY_AGENT created successfully! Cross-domain risk aggregation  and  regulatory compliance agent ready.' AS STATUS;

USE DATABASE {{ db }};
USE SCHEMA {{ rep_agg }};

DROP AGENT IF EXISTS WEALTH_ADVISOR_AGENT;

CREATE OR REPLACE AGENT WEALTH_ADVISOR_AGENT
  COMMENT = 'Unified Wealth Management and Advisory Agent - Portfolio performance, credit risk IRB, and equity trading intelligence'
  PROFILE = '{"display_name": "Wealth Advisor", "avatar": "", "color": "#388E3C"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration: {}

  instructions:
    sample_questions:
      - question: "Show me PLATINUM clients with EXCELLENT_PERFORMANCE portfolios"
      - question: "Which clients have HIGH risk category but GOOD_PERFORMANCE returns?"
      - question: "Find portfolios with POOR_PERFORMANCE or NEGATIVE_PERFORMANCE requiring rebalancing"
      - question: "Show me clients with total portfolio value over CHF 5 million"
      - question: "Which PREMIUM account type clients have the highest returns?"
      - question: "Analyze equity positions for my top 10 clients by total AUM"
      - question: "Show me INVESTMENT accounts with EXCELLENT_PERFORMANCE in the last measurement period"
      - question: "Find clients with high Sharpe ratio and low volatility"

  tools:
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Wealth_Management
        description: |
          # Wealth Advisor Agent - Portfolio, Credit and Equity Intelligence
          
          ## Agent Description
          
          I am your **Wealth Advisor Agent**, providing comprehensive wealth management intelligence across portfolio performance, credit risk, equity trading, and advisor relationships. I help you make informed investment decisions, assess client risk, optimize portfolios, and manage advisor capacity.
          
          ## What I Can Help You With
          
          ### **Portfolio Performance Analytics**
          - View portfolio values, returns, and performance metrics
          - Analyze time-weighted returns (TWR), annualized returns, YTD/MTD/QTD performance
          - Review Sharpe ratio, Sortino ratio, max drawdown, volatility
          - Track beta, alpha, tracking error, information ratio
          - Monitor asset allocation (equity, fixed income, commodities, cash, alternatives)
          - Identify portfolios requiring rebalancing
          - Categorize performance (EXCELLENT_PERFORMANCE, GOOD_PERFORMANCE, NEUTRAL_PERFORMANCE, POOR_PERFORMANCE, NEGATIVE_PERFORMANCE)
          
          ### **Credit Risk and Lending Exposure**
          - Review IRB (Internal Ratings-Based) credit ratings and scores
          - Analyze probability of default (PD), loss given default (LGD), exposure at default (EAD)
          - Monitor risk-weighted assets (RWA) and capital requirements
          - Track expected loss and unexpected loss metrics
          - Review secured vs unsecured lending exposure
          - Identify impaired loans and provisioning requirements
          - Monitor loan-to-value (LTV) ratios and collateral values
          - Track credit utilization and days past due
          
          ### **Equity Positions and Trading**
          - View equity holdings by symbol, ISIN, and security name
          - Monitor net positions, unrealized and realized P and L
          - Track buy/sell activity, average prices, current valuations
          - Analyze trading volumes, commissions, and trading frequency
          - Review equity concentration and diversification
          - Identify stale positions requiring review
          
          ### **Client Segmentation and Prioritization**
          - Analyze clients by tier (STANDARD, GOLD, PLATINUM, PREMIUM)
          - Calculate total client value (portfolio + lending)
          - Prioritize clients (VIP, HIGH, MEDIUM, STANDARD) by total exposure
          - Review client risk profiles and compliance status
          - Track customer tenure and relationship history
          
          ### **Advisor Relationship Management**
          - View advisor assignments and client portfolios
          - Monitor advisor capacity and client load
          - Track last contact dates and review schedules
          - Identify clients requiring advisor attention
          - Optimize advisor workload and capacity planning
          
          ### **Risk Aggregation and Alerts**
          - Identify clients with HIGH or MODERATE risk category
          - Flag portfolios with NEGATIVE_PERFORMANCE or POOR_PERFORMANCE
          - Monitor clients with EXCELLENT_PERFORMANCE but HIGH risk category
          - Track portfolio concentration and asset allocation
          - Review clients with poor performance requiring rebalancing
          
          ## Data Coverage
          
          I have access to **100+ wealth management attributes** including:
          - **Portfolio Metrics**: Value, returns, Sharpe, max drawdown, asset allocation
          - **Credit Risk IRB**: PD, LGD, EAD, RWA, credit rating, expected loss
          - **Lending**: Total exposure, secured/unsecured, loan balance, utilization
          - **Equity Positions**: Holdings, P and L, trading activity, commissions
          - **Customer Context**: Name, tier, risk rating, PEP status, income
          - **Account Details**: Type, status, balance, currency
          - **Advisor Info**: Primary advisor, team, region, contact frequency
          - **Consolidated Metrics**: Total client value, overall risk category, client priority
          
          ## Key Features
          
          - **Unified View**: Portfolio + Credit + Equity in one query (complete client picture)  
          - **Multi-Asset Coverage**: Equities, fixed income, commodities, cash, alternatives  
          - **IRB Compliant**: Full Basel III credit risk metrics  
          - **Natural Language**: Ask questions in plain English  
          - **Real-Time Data**: Hourly refreshed portfolio and position data  
          - **Advisor-Friendly**: Designed for relationship managers and wealth advisors  
          
          ## Sample Queries
          
          - "Show me my top 10 clients by total portfolio value"
          - "Which INVESTMENT accounts have EXCELLENT_PERFORMANCE but HIGH risk category?"
          - "Find portfolios with max drawdown exceeding -20%"
          - "Show me all portfolios with NEGATIVE_PERFORMANCE"
          - "Which clients have GOOD_PERFORMANCE with Sharpe ratio above 1.5?"
          - "Find CHECKING accounts with POOR_PERFORMANCE"
          - "Show me all INVESTMENT accounts with total value over CHF 1 million"
          - "Which portfolios have the highest annualized returns?"
          - "Find accounts with high equity allocation (over 80%)"
          
          ## Ideal For
          
          - **Wealth Advisors**: Client portfolio reviews and investment recommendations
          - **Relationship Managers**: Complete client relationship oversight
          - **Credit Risk Officers**: Lending exposure and IRB monitoring
          - **Portfolio Managers**: Multi-asset portfolio optimization
          - **Private Bankers**: High-net-worth client management
          - **Executive Leadership**: Wealth management KPIs and AUM tracking
          
          ## Data Freshness
          
          Portfolio performance is refreshed **daily** (end-of-day valuations). Equity positions and credit risk metrics are updated **hourly** via dynamic tables.
          
          **Ready to optimize your client portfolios? Ask me about performance, risk, or client prioritization!**

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Customer_Profile
        description: |
          # Customer Profile and Demographics
          
          Provides additional customer context for wealth advisory:
          - Customer demographics and contact information
          - Account tier and risk profile
          - PEP status and compliance flags
          - Customer tenure and relationship history
          - Employment, occupation, and income details

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Advisor_Relationships
        description: |
          # Advisor Assignments and Capacity
          
          Provides advisor relationship and capacity management:
          - Advisor-customer assignments and primary advisors
          - Advisor capacity utilization and client load
          - Advisor performance metrics and revenue
          - Team structure and regional organization
          - Contact frequency and relationship tenure

  tool_resources:
    Wealth_Management:
      semantic_view: {{ db }}.{{ rep_agg }}.REPA_SV_WEALTH_MANAGEMENT
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    Customer_Profile:
      semantic_view: {{ db }}.{{ crm_agg }}.CRMA_SV_CUSTOMER_360
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    Advisor_Relationships:
      semantic_view: {{ db }}.{{ crm_agg }}.EMPA_SV_EMPLOYEE_ADVISOR
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.WEALTH_ADVISOR_AGENT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.WEALTH_ADVISOR_AGENT TO ROLE PUBLIC;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

-- Re-runnable: ignore 400203 (agent already present), re-raise anything else
EXECUTE IMMEDIATE $$
BEGIN
  ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {{ db }}.{{ rep_agg }}.WEALTH_ADVISOR_AGENT;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    IF (SQLCODE <> 400203) THEN
      RAISE;
    END IF;
END;
$$;

SHOW AGENTS IN SCHEMA {{ db }}.{{ rep_agg }};

SELECT 'WEALTH_ADVISOR_AGENT created successfully! Unified wealth management (portfolio + credit + equity) agent ready.' AS STATUS;

USE DATABASE {{ db }};
USE SCHEMA {{ rep_agg }};

DROP AGENT IF EXISTS LIQUIDITY_RISK_AGENT;

CREATE OR REPLACE AGENT LIQUIDITY_RISK_AGENT
  COMMENT = 'Treasury  and  Liquidity Risk Agent - LCR monitoring, HQLA management, deposit analysis, FINMA Circular 2015/2 compliance, SNB reporting'
  PROFILE = '{"display_name": "Treasury  and  Liquidity", "avatar": "", "color": "#00897B"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration: {}

  instructions:
    sample_questions:
      - question: "What is our current LCR ratio and compliance status?"
      - question: "Show me HQLA breakdown by level (L1, L2A, L2B)"
      - question: "Is the 40% cap applied today? How close are we to the limit?"
      - question: "What are our deposit outflows by counterparty type?"
      - question: "Show me the 90-day LCR trend with moving averages"
      - question: "Are there any active liquidity alerts or breach warnings?"
      - question: "What is our liquidity buffer in CHF and as a percentage?"
      - question: "How much HQLA do we have in government bonds?"
      - question: "Which counterparty type contributes most to our stressed outflows?"
      - question: "Has LCR been stable or volatile over the last 30 days?"
      - question: "Show me SNB reserves and cash holdings"
      - question: "What is our weighted average deposit run-off rate?"
      - question: "Why did the LCR drop on 26 August? Which client caused it?"
      - question: "How low does the LCR go under the combined stress scenario?"
      - question: "What happens to the LCR if Alpenbau withdraws another CHF 100M?"
      - question: "How much additional corporate withdrawal can we absorb before breaching 100%?"
      - question: "Which liquidity events explain the September breach?"
      - question: "What is the Distance-to-Illiquidity per legal entity on the latest reporting date?"
      - question: "Which LST positions changed most between the latest and the previous reporting date?"

  tools:
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: LCR_Current_Status
        description: |
          # LCR Current Status - Real-Time Liquidity Monitoring
          
          ## Agent Description
          
          I am your **Treasury  and  Liquidity Risk Agent**, providing real-time monitoring of the Liquidity Coverage Ratio (LCR) for compliance with **FINMA Circular 2015/2** and **Basel III** liquidity standards. I help Treasury teams monitor daily liquidity positions, manage HQLA portfolios, analyze deposit run-offs, and prepare regulatory submissions to the Swiss National Bank (SNB).
          
          ## What I Can Help You With
          
          ### **LCR Compliance Monitoring**
          - Monitor current LCR ratio against 100% minimum (FINMA requirement)
          - Track compliance status: FAIL (<100%), WARNING (100% to <110%), PASS (≥110%)
          - Calculate liquidity buffer (HQLA - Net Outflows)
          - Monitor distance from regulatory minimum and internal target (110%)
          - Review calculation freshness and data quality
          - Generate daily compliance reports for management
          
          ### **HQLA Portfolio Management**
          - View total High-Quality Liquid Assets (HQLA) in CHF
          - Break down HQLA by level (L1, L2A, L2B) with haircuts
          - Analyze HQLA composition by asset type:
            - **Level 1** (0% haircut): SNB reserves, cash, government bonds
            - **Level 2A** (15% haircut): Canton bonds, covered bonds (Pfandbriefe)
            - **Level 2B** (50% haircut): SMI equities, AA- corporate bonds
          - Monitor portfolio concentration and diversification
          - Track holdings count and average position size
          
          ### **40% Cap Rule Monitoring**
          - Check if 40% cap is applied (Level 2 assets ≤ 40% of total HQLA)
          - Calculate distance to cap breach (buffer in percentage points)
          - Identify discarded Level 2 assets due to cap enforcement
          - Monitor Level 2 percentage of total HQLA
          - Support portfolio rebalancing decisions toward Level 1 assets
          - Alert when approaching cap threshold (e.g., >35%)
          
          ### **Deposit Outflow Analysis**
          - Review total stressed net cash outflows (30-day scenario)
          - Break down outflows by counterparty type:
            - **Retail Stable** (3-5% run-off): Salary accounts, long-term relationships
            - **Retail Less Stable** (10% run-off): High-balance savings
            - **Retail Insured** (3% run-off): Deposit insurance protected
            - **Corporate Operational** (25% run-off): Payroll and clearing accounts
            - **Corporate Non-Operational** (40% run-off): Treasury deposits
            - **Financial Institutions** (100% run-off): Bank-to-bank funding
          - Calculate weighted average run-off rate
          - Identify most stable vs most volatile deposit types
          - Support funding strategy and relationship banking initiatives
          
          ### **Trend Analysis  and  Volatility**
          - Track 90-day historical LCR trend
          - Calculate 7-day and 30-day moving averages
          - Measure day-over-day LCR changes (volatility)
          - Identify periods of stability vs stress
          - Detect sustained breaches (3+ consecutive days below 100%)
          - Compare current LCR to historical min/max/average
          
          ### **Compliance Alerts  and  Risk Mitigation**
          - Monitor active compliance alerts and warnings
          - Review breach alerts (LCR < 100%)
          - Track volatility alerts (daily swing > 10%)
          - Monitor cap alerts (40% limit triggered)
          - Assess alert severity (RED/YELLOW/GREEN)
          - Recommend actions for Treasury team
          - Generate escalation reports for management
          
          ### **SNB Regulatory Reporting**
          - Prepare monthly LCR summary for internal reporting
          - Generate quarterly SNB LCR_P survey data
          - Calculate monthly min/max/average LCR ratios
          - Track breach days per month for regulatory disclosure
          - Support audit trail and regulatory examinations
          - Validate data completeness for submissions
          
          ## Data Coverage
          
          ### Current LCR Status (Single-Row Summary)
          - **Primary Metric**: LCR ratio (%), compliance status, status color
          - **HQLA Numerator**: Total HQLA, L1/L2A/L2B breakdown, percentages
          - **40% Cap Analysis**: Cap applied flag, discarded L2, buffer to cap
          - **Net Outflows**: Total outflows, retail/corporate/FI breakdown, percentages
          - **Liquidity Buffer**: Buffer in CHF and % of outflows, strength rating
          - **Portfolio Metrics**: HQLA securities count, deposit accounts count
          - **Data Quality**: Last calculated timestamp, calculation age, freshness
          
          ### HQLA Composition (Asset-Level Detail)
          - **Asset Classification**: Level (L1/L2A/L2B), asset type, quality description
          - **Market Values**: Market value in CHF and billions
          - **Haircuts**: Applied haircut %, standard haircut by level
          - **HQLA Values**: HQLA value after haircut in CHF and billions
          - **Portfolio Metrics**: Holdings count per asset type
          - **Composition**: % of total HQLA, sort order for charts
          
          ### Deposit Outflows (Counterparty-Level Detail)
          - **Counterparty Type**: Retail stable/less stable/insured, corporate op/non-op, FI
          - **Balances**: Deposit balance in CHF and billions
          - **Run-off Rates**: Average/min/max run-off rates by type
          - **Outflows**: Outflow amount in CHF and billions
          - **Composition**: % of total outflows, % of total deposits
          - **Risk Rating**: Stability rating (very stable to very high risk)
          - **Implied Metrics**: Effective run-off %, display order for charts
          
          ### Historical Trend (90-Day Time Series)
          - **Daily Metrics**: LCR ratio, compliance status, HQLA, outflows, buffer
          - **Moving Averages**: 7-day MA, 30-day MA for trend smoothing
          - **Volatility**: Day-over-day change (absolute and %), volatility rating
          - **Breach Detection**: Breach flag, warning flag, consecutive breach days
          - **Trend Direction**: Improving/declining/stable classification
          - **Thresholds**: Distance from 100% minimum, distance from 110% target
          
          ### Active Alerts (Filtered for Action)
          - **Alert Details**: Alert date, type, severity, description
          - **Current Values**: LCR ratio, threshold value triggering alert
          - **Recommended Actions**: Treasury actions for each alert type
          - **Severity Priority**: RED first (critical), then YELLOW (warning)
          - **Time Criticality**: Days since alert, alert age classification
          - **Alert Category**: Compliance/risk/portfolio/operational
          - **Escalation**: Escalation required flag for overdue alerts
          
          ## Key Features
          
          - **Real-Time Monitoring**: 60-minute data refresh for operational decisions
          - **Natural Language**: Ask questions in plain English, no SQL required
          - **Regulatory Aligned**: FINMA Circular 2015/2 and Basel III LCR framework
          - **Treasury Focused**: Daily operational metrics for funding decisions
          - **Risk Mitigation**: Proactive alerts before regulatory breaches occur
          - **Audit Ready**: Complete data lineage for regulatory examinations
          - **SNB Ready**: Monthly summaries prepared for quarterly submissions
          
          ## Sample Queries
          
          ### Current Status Queries
          - "What is our LCR ratio today?"
          - "Are we compliant with FINMA 100% minimum?"
          - "Show me our liquidity buffer in CHF"
          - "What is our current compliance status?"
          - "How fresh is the LCR calculation?"
          
          ### HQLA Portfolio Queries
          - "What is our total HQLA?"
          - "Show me HQLA breakdown by level"
          - "How much do we have in Level 1 assets?"
          - "What percentage of HQLA is government bonds?"
          - "List all Level 2B assets with haircuts"
          - "How many HQLA securities do we hold?"
          
          ### 40% Cap Rule Queries
          - "Is the 40% cap applied today?"
          - "How close are we to the 40% cap?"
          - "Show me discarded Level 2 assets"
          - "What is our Level 2 as percentage of total HQLA?"
          - "How much buffer do we have before hitting the cap?"
          
          ### Deposit Outflow Queries
          - "What are our total stressed outflows?"
          - "Show me deposit outflows by counterparty type"
          - "Which deposit type has the highest run-off rate?"
          - "How much comes from retail vs corporate deposits?"
          - "What is our weighted average run-off rate?"
          - "Which counterparty type is most stable?"
          
          ### Trend  and  Volatility Queries
          - "Show me the 90-day LCR trend"
          - "Has LCR been stable or volatile this month?"
          - "What is the 7-day moving average?"
          - "When was the last time we breached 100%?"
          - "Show me day-over-day LCR changes"
          - "What is the LCR min/max/average over 90 days?"
          
          ### Alert  and  Compliance Queries
          - "Are there any active liquidity alerts?"
          - "Show me all breach alerts"
          - "Which alerts are most critical?"
          - "What actions are recommended for current alerts?"
          - "Have we had any sustained breaches (3+ days)?"
          
          ### SNB Reporting Queries
          - "Prepare monthly LCR summary for SNB submission"
          - "What was our average LCR last month?"
          - "How many breach days did we have this quarter?"
          - "Show me min/max LCR ratios by month"
          
          ## Ideal For
          
          - **Treasury Department**: Daily liquidity monitoring and funding decisions
          - **Chief Financial Officer (CFO)**: Strategic liquidity planning
          - **Asset-Liability Management (ALM)**: Portfolio optimization
          - **Risk Management**: Liquidity risk oversight and stress testing
          - **Regulatory Reporting Team**: SNB quarterly submissions
          - **Finance Committee**: Board-level liquidity reporting
          - **Internal Audit**: LCR compliance validation
          - **Executive Management**: Regulatory breach prevention
          
          ## Data Freshness
          
          LCR data is refreshed **every 60 minutes** via dynamic tables, providing near real-time liquidity monitoring for operational decision-making. This ensures Treasury has access to current positions for intraday liquidity management and regulatory compliance.
          
          **Ready to monitor liquidity risk? Ask me about LCR, HQLA, deposit outflows, or compliance alerts!**

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: HQLA_Portfolio
        description: |
          # HQLA Portfolio Analysis - Asset Composition  and  Quality
          
          ## What This Tool Provides
          
          Detailed analysis of **High-Quality Liquid Assets (HQLA)** portfolio:
          
          ### **Asset-Level Intelligence**
          - View all HQLA holdings by level (L1, L2A, L2B)
          - Break down by asset type (government bonds, canton bonds, SMI equities, etc.)
          - Compare market value vs HQLA value (after haircuts)
          - Monitor portfolio concentration and diversification
          
          ### **Haircut Analysis**
          - Review applied haircuts by asset level (0%, 15%, 50%)
          - Calculate haircut impact on HQLA contribution
          - Compare standard vs actual haircuts
          
          ### **Portfolio Composition**
          - Analyze percentage contribution to total HQLA
          - Track holdings count per asset type
          - Identify largest HQLA positions
          - Support rebalancing decisions
          
          ### **Common Queries**
          - "Show me all Level 1 assets with their HQLA values"
          - "What percentage of HQLA is in government bonds?"
          - "List all assets subject to 50% haircut (Level 2B)"
          - "How many canton bonds do we hold?"
          - "What is the average HQLA value per holding?"
          
          Data refreshed **hourly** for portfolio management decisions.

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Deposit_Outflows
        description: |
          # Deposit Outflow Analysis - Funding Stability  and  Run-off Rates
          
          ## What This Tool Provides
          
          Analysis of **deposit balances and stressed outflows** by counterparty:
          
          ### **Counterparty Intelligence**
          - View deposits by type (retail stable, corporate operational, FI, etc.)
          - Review actual deposit balances in CHF
          - Analyze run-off rate assumptions by counterparty
          - Calculate stressed outflows for 30-day scenario
          
          ### **Funding Strategy Insights**
          - Identify most stable funding sources
          - Monitor concentration in volatile funding types
          - Calculate weighted average run-off rate
          - Support relationship banking initiatives
          
          ### **Run-off Rate Analysis**
          - Compare base run-off rates by counterparty type
          - Review min/max/average rates for each segment
          - Validate effective run-off percentages
          - Identify opportunities for rate improvements (relationship discounts)
          
          ### **Common Queries**
          - "What is our total retail deposit base?"
          - "Show me deposits with 100% run-off rate (most volatile)"
          - "Which counterparty type contributes most to outflows?"
          - "How stable are our corporate operational deposits?"
          - "What percentage of deposits are retail insured?"
          
          Data refreshed **hourly** for funding strategy decisions.

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: LCR_Trend
        description: |
          # LCR Historical Trend - 90-Day Monitoring  and  Volatility Analysis
          
          ## What This Tool Provides
          
          **Historical LCR performance** over 90 days with trend analysis:
          
          ### **Trend Intelligence**
          - Track daily LCR ratios over 90 days
          - Calculate 7-day and 30-day moving averages
          - Measure day-over-day volatility
          - Identify trend direction (improving/declining/stable)
          
          ### **Breach Detection**
          - Highlight days with LCR < 100% (regulatory breach)
          - Monitor warning periods (LCR 100-110%)
          - Detect sustained breaches (3+ consecutive days)
          - Track compliance status over time
          
          ### **Statistical Analysis**
          - Calculate min/max/average LCR over period
          - Measure standard deviation (volatility indicator)
          - Compare current LCR to historical range
          - Identify distance from regulatory thresholds
          
          ### **Common Queries**
          - "Show me the 30-day LCR trend"
          - "Has LCR improved or declined this month?"
          - "What is the volatility over the last 90 days?"
          - "When was the last breach?"
          - "Show me the 7-day moving average"
          - "Compare current LCR to 90-day average"
          
          Data refreshed **hourly** for trend monitoring.

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Compliance_Alerts
        description: |
          # Compliance Alerts - Real-Time Breach Detection  and  Risk Mitigation
          
          ## What This Tool Provides
          
          **Active liquidity alerts** requiring Treasury attention:
          
          ### **Alert Intelligence**
          - View all active/unresolved alerts (last 30 days)
          - Review alert types:
            - **BREACH**: LCR < 100% (FINMA violation)
            - **WARNING**: LCR 100-110% (below internal buffer target)
            - **VOLATILITY**: Daily swing > 10%
            - **CAP_TRIGGERED**: 40% L2 cap applied
          - Assess severity (RED/YELLOW/GREEN)
          - Monitor alert age and time criticality
          
          ### **Action Recommendations**
          - **BREACH** → Report to FINMA within 24 hours
          - **WARNING** → Initiate liquidity sourcing plan
          - **VOLATILITY** → Investigate root cause
          - **CAP_TRIGGERED** → Rebalance portfolio toward L1
          
          ### **Escalation Management**
          - Identify overdue alerts requiring management escalation
          - Track days since alert generation
          - Prioritize by severity and criticality
          - Support regulatory examination preparation
          
          ### **Common Queries**
          - "Are there any active alerts today?"
          - "Show me all breach alerts"
          - "Which alerts are most critical?"
          - "What actions are recommended?"
          - "Are there any overdue alerts?"
          - "Show me volatility alerts from this week"
          
          Data refreshed **hourly** for real-time monitoring.

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: LCR_Scenarios
        description: |
          Daily LCR under four scenarios: BASE (regulatory FINMA LCR), BANK_STRESS (idiosyncratic),
          MARKET_STRESS (market-wide, extra HQLA haircuts) and COMBINED. Use for "how low would the LCR go
          under stress", "which scenario breaches 100%", comparing stressed vs regulatory LCR, buffers and FAIL days.
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: LCR_Variance_Drivers
        description: |
          Root cause of LCR changes. Top-10 client positions (deposits, loans, facilities, interbank contracts)
          driving the LCR move on a date, with contract id, client name and CLIENT_IMPACT_PP (funding-adjusted impact in
          percentage points; negative = the client move lowered the LCR, incl. the cash paid out from SNB reserves).
          Always explain root causes with CLIENT_IMPACT_PP, not CONTRIBUTION_PP.
          Comparison windows DOD (default), WOW, MOM; use STRESS_SCENARIO = 'BASE' for the regulatory LCR.
          Use for "why did the LCR drop on <date>", "who caused the change", "top drivers this week".
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: LCR_Events
        description: |
          Timeline of known liquidity events (client deposit withdrawals, market stress, recovery actions,
          large loan maturities) with date, description, client and amount. Combine with LCR_Variance_Drivers
          to explain LCR movements in business terms.
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: DTI_Compare
        description: |
          UC2 Distance-to-Illiquidity (DtI) and Stichtagsvergleich: DtI in days per legal entity and reporting
          date (stressed and base), and the signed LST flows behind it by LST position, product cluster,
          application and product. Use it to compare two reporting dates (default: latest vs previous business
          day), find the largest position or product deltas, and explain why the DtI moved. Answers only cover the
          entities the user may see (row access by role). Use LCR tools for the 30-day regulatory LCR instead.
    - tool_spec:
        type: generic
        name: LCR_What_If
        description: |
          What-if LCR simulator. Recalculates the regulatory LCR with the official formula (L2B 15% cap,
          L2 40% cap, 75% inflow cap) after applying levers and returns baseline, simulated LCR, status,
          a step-by-step waterfall and the breakeven extra corporate withdrawal that would bring the LCR to 100%.
          p_params is a JSON string with optional keys: preset (BASE|BANK_STRESS|MARKET_STRESS|COMBINED),
          retail_stable_mult, retail_less_stable_mult, corp_operational_mult, corp_non_operational_mult, fi_mult,
          facilities_mult, inflow_mult (1.0 = unchanged, 1.5 = +50%), haircut_addon_l1_pp, haircut_addon_l2a_pp,
          haircut_addon_l2b_pp (extra haircut in percentage points), price_shock_pct (market value drop in %),
          withdrawal_chf (corporate deposit withdrawal in CHF), inflow_cap_pct (default 0.75).
          Example: {"corp_non_operational_mult": 1.5, "withdrawal_chf": 100000000}. Use {} for "what is the breakeven".
        input_schema:
          type: object
          properties:
            p_params:
              type: string
              description: 'JSON object with the what-if levers, e.g. {"withdrawal_chf": 100000000}'
            p_as_of_date:
              type: string
              description: 'Reporting date YYYY-MM-DD; empty string for the latest date'
          required: [p_params, p_as_of_date]
  tool_resources:
    LCR_Current_Status:
      semantic_view: {{ db }}.{{ rep_agg }}.LCRS_SV_LCR_CURRENT
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    HQLA_Portfolio:
      semantic_view: {{ db }}.{{ rep_agg }}.LCRS_SV_HQLA_BREAKDOWN
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    Deposit_Outflows:
      semantic_view: {{ db }}.{{ rep_agg }}.LCRS_SV_OUTFLOW_BREAKDOWN
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    LCR_Trend:
      semantic_view: {{ db }}.{{ rep_agg }}.LCRS_SV_TREND_90DAY
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    Compliance_Alerts:
      semantic_view: {{ db }}.{{ rep_agg }}.LCRS_SV_ALERTS_ACTIVE
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    LCR_Scenarios:
      semantic_view: {{ db }}.{{ rep_agg }}.LCRS_SV_LCR_SCENARIOS
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    LCR_Variance_Drivers:
      semantic_view: {{ db }}.{{ rep_agg }}.LCRS_SV_LCR_DRIVERS
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 60
    LCR_Events:
      semantic_view: {{ db }}.{{ rep_agg }}.LCRS_SV_LCR_EVENTS
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    DTI_Compare:
      semantic_view: {{ db }}.{{ rep_agg }}.DTIS_SV_DTI_COMPARISON
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 60
    LCR_What_If:
      identifier: "{{ db }}.{{ rep_agg }}.REPP_AGG_SP_LCR_WHAT_IF"
      name: "REPP_AGG_SP_LCR_WHAT_IF(VARCHAR, VARCHAR)"
      type: "procedure"
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 120
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.LIQUIDITY_RISK_AGENT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.LIQUIDITY_RISK_AGENT TO ROLE PUBLIC;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

-- Re-runnable: ignore 400203 (agent already present), re-raise anything else
EXECUTE IMMEDIATE $$
BEGIN
  ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {{ db }}.{{ rep_agg }}.LIQUIDITY_RISK_AGENT;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    IF (SQLCODE <> 400203) THEN
      RAISE;
    END IF;
END;
$$;

SHOW AGENTS IN SCHEMA {{ db }}.{{ rep_agg }};

SELECT 'LIQUIDITY_RISK_AGENT created successfully! Treasury  and  Liquidity monitoring agent ready.' AS STATUS;

USE DATABASE {{ db }};
USE SCHEMA {{ rep_agg }};

DROP AGENT IF EXISTS LOAN_PORTFOLIO_AGENT;

CREATE OR REPLACE AGENT LOAN_PORTFOLIO_AGENT
  COMMENT = 'Retail Loans  and  Mortgages Portfolio Agent - Portfolio monitoring, LTV analysis, application funnel, affordability assessment, compliance screening'
  PROFILE = '{"display_name": "Loan Portfolio", "avatar": "", "color": "#1565C0"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration: {}

  instructions:
    sample_questions:
      - question: "What is our total loan exposure and average loan amount?"
      - question: "Show me the LTV distribution across our portfolio"
      - question: "How many loans are in the high LTV bucket (>80%)?"
      - question: "What is our application approval rate by country?"
      - question: "Show me the application funnel by product type"
      - question: "What is the average DTI ratio for approved loans?"
      - question: "How many applications are on compliance hold?"
      - question: "Show me all loans with vulnerable customer flags"
      - question: "Which country has the highest approval rate?"
      - question: "What is the affordability pass rate for UK mortgages?"
      - question: "Are there any applications with sanctions or PEP hits?"
      - question: "Show me portfolio breakdown by country and product"

  tools:
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Portfolio_Summary
        description: |
          # Loan Portfolio Summary - Real-Time Portfolio Monitoring
          
          ## Agent Description
          
          I am your **Retail Loans  and  Mortgages Portfolio Agent**, providing real-time monitoring of loan origination and portfolio performance across Switzerland, UK, and Germany. I help Retail Banking teams monitor application volumes, manage credit risk, track LTV concentration, assess affordability compliance, and integrate with sanctions/PEP screening for regulatory compliance.
          
          ## What I Can Help You With
          
          ### **Portfolio Monitoring**
          - Monitor total loan exposure by product type (mortgages, personal loans)
          - Track loan counts and average loan amounts
          - Analyze portfolio composition by country and product
          - Review application status distribution (approved, declined, under review)
          - Monitor portfolio growth and origination trends
          - Identify largest and smallest loan exposures
          
          ### **Product Analysis**
          - Compare mortgage vs personal loan volumes
          - Analyze average loan terms by product type
          - Review product mix across different markets
          - Track product-specific approval rates
          - Monitor product concentration risk
          
          ### **Country/Market Analysis**
          - Compare portfolio across Switzerland (CHE), UK (GBR), Germany (DEU), Portugal (PRT)
          - Analyze market-specific lending patterns
          - Review country-specific approval rates
          - Monitor regulatory exposure by jurisdiction
          - Track cross-border portfolio diversification
          
          ### **Application Status Tracking**
          - Monitor approved, declined, and under-review applications
          - Track application volumes over time
          - Analyze status distribution by product and country
          - Identify bottlenecks in application processing
          - Support operational capacity planning
          
          ## Data Coverage
          
          ### Portfolio Metrics (Aggregated by Product/Country/Status)
          - **Volume Metrics**: Loan count, total exposure, average/min/max loan amounts
          - **Product Dimensions**: Product type (MORTGAGE, PERSONAL_LOAN)
          - **Geographic Dimensions**: Country codes (CHE, GBR, DEU, PRT)
          - **Status Dimensions**: Application status (APPROVED, DECLINED, UNDER_REVIEW)
          - **Term Metrics**: Average loan term in months
          - **Data Quality**: Calculation timestamp (60-minute refresh)
          
          ## Sample Queries
          
          ### Portfolio Overview
          - "What is our total loan exposure?"
          - "How many loans do we have in the portfolio?"
          - "Show me portfolio breakdown by product type"
          - "What is the average loan amount?"
          - "Show me the largest loan in the portfolio"
          
          ### Country Analysis
          - "Show me portfolio by country"
          - "How many loans do we have in Switzerland?"
          - "Compare loan volumes between UK and Germany"
          - "What is the average loan amount in CHE?"
          
          ### Product Analysis
          - "Show me mortgage vs personal loan volumes"
          - "What is the average term for mortgages?"
          - "How much exposure do we have in mortgages?"
          
          ### Status Analysis
          - "How many approved applications do we have?"
          - "Show me declined applications by country"
          - "What percentage of applications are under review?"
          
          ## Ideal For
          
          - **Retail Banking Leadership**: Portfolio performance and strategic planning
          - **Credit Risk Management**: Exposure monitoring and risk assessment
          - **Loan Operations**: Application pipeline management
          - **Product Management**: Product mix analysis and optimization
          - **Country Managers**: Market-specific performance tracking
          - **Finance/Treasury**: Balance sheet planning and forecasting
          
          ## Data Freshness
          
          Portfolio data is refreshed **every 60 minutes** via dynamic tables, providing near real-time monitoring for operational decision-making and risk management.
          
          **Ready to monitor loan portfolio? Ask me about exposures, product mix, or country performance!**

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: LTV_Risk_Analysis
        description: |
          # LTV Distribution  and  Risk Concentration - Credit Risk Monitoring
          
          ## What This Tool Provides
          
          Detailed **Loan-to-Value (LTV) distribution** analysis for credit risk management:
          
          ### **LTV Risk Intelligence**
          - View LTV distribution across 6 risk buckets (0-50%, 50-60%, 60-70%, 70-80%, 80-90%, >90%)
          - Monitor high LTV concentration (>80% LTV = higher credit risk)
          - Analyze average LTV by bucket
          - Track collateral values supporting loan portfolio
          - Calculate portfolio concentration in each LTV band
          
          ### **Credit Risk Assessment**
          - **0-50% LTV**: Very low risk - strong equity cushion
          - **50-60% LTV**: Low risk - adequate collateral coverage
          - **60-70% LTV**: Moderate risk - standard lending
          - **70-80% LTV**: Elevated risk - requires monitoring
          - **80-90% LTV**: High risk - regulatory thresholds (FINMA 80% owner-occupied)
          - **>90% LTV**: Very high risk - may breach regulatory limits
          
          ### **Regulatory Compliance**
          - **Switzerland (FINMA)**: Max 80% LTV for owner-occupied, 75% for buy-to-let
          - **UK (FCA)**: Max 90% LTV for owner-occupied (Consumer Duty)
          - **Germany (BaFin)**: Max 85% LTV typical for residential mortgages
          - Monitor portfolio concentration above regulatory thresholds
          - Support stress testing and scenario analysis
          
          ### **Portfolio Risk Metrics**
          - Total loan amount in each LTV bucket
          - Total collateral value by bucket
          - Average LTV within each band
          - Percentage of loans in each bucket (concentration risk)
          - Number of loans requiring additional monitoring (high LTV)
          
          ### **Common Queries**
          - "Show me LTV distribution across the portfolio"
          - "How many loans are in the high LTV bucket (>80%)?"
          - "What is our average LTV?"
          - "How much exposure do we have above 80% LTV?"
          - "Show me collateral values by LTV bucket"
          - "What percentage of loans are in the 70-80% LTV range?"
          - "Which LTV bucket has the most concentration?"
          
          Data refreshed **hourly** for credit risk monitoring.

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Application_Funnel
        description: |
          # Application Funnel  and  Conversion Metrics - Origination Performance
          
          ## What This Tool Provides
          
          **Application pipeline and conversion analysis** by product, country, and channel:
          
          ### **Funnel Intelligence**
          - Track total applications received
          - Monitor approved, declined, and under-review volumes
          - Calculate approval and decline rates
          - Analyze conversion by product type
          - Compare performance across countries
          - Evaluate channel effectiveness (email, portal, branch, broker)
          
          ### **Conversion Metrics**
          - **Approval Rate**: Approved applications / Total applications
          - **Decline Rate**: Declined applications / Total applications
          - **Pending Rate**: Under review / Total applications
          - Average requested loan amount by status
          - Identify bottlenecks in application processing
          
          ### **Channel Performance**
          - Compare EMAIL vs PORTAL vs BRANCH vs BROKER channels
          - Analyze channel-specific approval rates
          - Monitor average loan size by channel
          - Support channel strategy and marketing decisions
          - Optimize broker partnerships based on conversion
          
          ### **Market Performance**
          - Compare approval rates across Switzerland, UK, Germany, Portugal
          - Identify market-specific credit quality patterns
          - Monitor country-specific decline reasons
          - Support country-level credit policy adjustments
          
          ### **Product Performance**
          - Compare mortgage vs personal loan conversion rates
          - Analyze product-specific application volumes
          - Monitor average loan amounts by product
          - Support product strategy and risk appetite decisions
          
          ### **Common Queries**
          - "What is our overall approval rate?"
          - "Show me the application funnel by country"
          - "Which channel has the highest approval rate?"
          - "How many applications are currently under review?"
          - "Compare approval rates for mortgages vs personal loans"
          - "Show me declined applications by country"
          - "What is the approval rate for UK mortgages?"
          - "Which country has the most applications?"
          
          Data refreshed **hourly** for operational monitoring.

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Affordability_Assessment
        description: |
          # Affordability Analysis - DTI/DSTI Compliance Monitoring
          
          ## What This Tool Provides
          
          **Affordability assessment outcomes** and DTI/DSTI analysis by country:
          
          ### **Affordability Intelligence**
          - Monitor affordability pass/fail rates by country
          - Analyze average Debt-to-Income (DTI) ratios
          - Track Debt Service-to-Income (DSTI) metrics
          - Review average gross monthly income
          - Monitor average monthly debt obligations
          - Support responsible lending compliance
          
          ### **Regulatory Thresholds**
          - **Switzerland (FINMA)**: Max 33% DSTI with 5% imputed rate stress test
          - **UK (FCA)**: Max 45% DTI for affordability (Consumer Duty)
          - **Germany (BaFin)**: Max 40% DSTI typical for residential mortgages
          - Monitor portfolio compliance with country-specific rules
          - Support stress testing and scenario analysis
          
          ### **DTI vs DSTI**
          - **DTI (Debt-to-Income)**: Total debt obligations / Gross income
          - **DSTI (Debt Service-to-Income)**: Loan payment + existing debts / Gross income
          - DSTI includes hypothetical loan payment for affordability test
          - Used for responsible lending and consumer protection
          
          ### **Assessment Metrics**
          - Number of affordability assessments performed
          - Pass vs fail counts by country
          - Average DTI and DSTI ratios
          - Average income levels by country
          - Average existing debt obligations
          - Affordability pass rate percentage
          
          ### **Responsible Lending**
          - Monitor adherence to affordability rules
          - Identify customers with high debt burdens
          - Support vulnerable customer identification
          - Demonstrate compliance with Consumer Duty (UK FCA)
          - Prepare for regulatory examinations
          
          ### **Common Queries**
          - "What is our affordability pass rate?"
          - "Show me average DTI ratio by country"
          - "How many affordability assessments have we performed?"
          - "What is the average DSTI for UK applications?"
          - "Show me affordability pass/fail breakdown by country"
          - "What is the average monthly income for approved loans?"
          - "How many assessments failed affordability?"
          - "Compare affordability pass rates across countries"
          
          Data refreshed **hourly** for compliance monitoring.

    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: Compliance_Screening
        description: |
          # Compliance  and  Screening Integration - Sanctions/PEP Monitoring
          
          ## What This Tool Provides
          
          **Loan applications linked to sanctions and PEP screening** for compliance:
          
          ### **Compliance Intelligence**
          - View all loan applications with customer compliance status
          - Monitor sanctions screening hits (OFAC, EU, UN)
          - Track Politically Exposed Persons (PEP) flags
          - Review overall risk ratings (CRITICAL, HIGH, MEDIUM, LOW)
          - Identify vulnerable customer flags
          - Monitor applications on compliance hold
          - Support KYC/CDD requirements
          
          ### **Sanctions Screening**
          - Identify customers with sanctions matches
          - Review sanctioning authorities (OFAC, EU, UN)
          - Monitor sanctions risk scores
          - Track sanctioned entity names
          - Support sanctions compliance and regulatory reporting
          - Prevent prohibited lending to sanctioned individuals
          
          ### **PEP Screening**
          - Identify Politically Exposed Persons in loan portfolio
          - Review PEP categories (DOMESTIC, FOREIGN, INTERNATIONAL)
          - Monitor PEP risk levels (CRITICAL, HIGH, MEDIUM, LOW)
          - Track exact vs fuzzy PEP matches
          - Support enhanced due diligence (EDD) requirements
          - Demonstrate compliance with AML regulations
          
          ### **Vulnerable Customers**
          - Identify customers with vulnerability flags
          - Support UK FCA Consumer Duty requirements
          - Monitor vulnerable customer treatment
          - Ensure appropriate lending decisions
          - Demonstrate fair treatment of vulnerable borrowers
          
          ### **Compliance Hold Management**
          - View applications blocked for compliance review
          - Monitor hold reasons (sanctions, PEP, high risk)
          - Track compliance status (SANCTIONS_REVIEW, PEP_REVIEW, CLEAR)
          - Support escalation to compliance team
          - Maintain audit trail for regulatory examinations
          
          ### **Integration with Customer 360**
          - Loan applications automatically linked to Customer 360
          - Real-time sanctions/PEP status from screening views
          - Overall customer risk rating incorporated
          - Comprehensive customer profile for credit decisions
          - Demonstrate holistic risk assessment
          
          ### **Common Queries**
          - "Are there any applications with sanctions hits?"
          - "Show me loans with PEP flags"
          - "How many applications are on compliance hold?"
          - "Show me all high risk applications"
          - "Which applications have vulnerable customer flags?"
          - "Are there any OFAC sanctions matches?"
          - "Show me applications with CRITICAL risk rating"
          - "How many applications have CLEAR compliance status?"
          - "Show me all PEP matches by risk level"
          - "Which customers require enhanced due diligence?"
          
          Data refreshed **hourly** for compliance monitoring.

  tool_resources:
    Portfolio_Summary:
      semantic_view: {{ db }}.{{ rep_agg }}.LOAS_SV_PORTFOLIO_CURRENT
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    LTV_Risk_Analysis:
      semantic_view: {{ db }}.{{ rep_agg }}.LOAS_SV_LTV_DISTRIBUTION
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    Application_Funnel:
      semantic_view: {{ db }}.{{ rep_agg }}.LOAS_SV_APPLICATION_FUNNEL
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    Affordability_Assessment:
      semantic_view: {{ db }}.{{ rep_agg }}.LOAS_SV_AFFORDABILITY_ANALYSIS
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
    
    Compliance_Screening:
      semantic_view: {{ db }}.{{ rep_agg }}.LOAS_SV_COMPLIANCE_SCREENING
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.LOAN_PORTFOLIO_AGENT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.LOAN_PORTFOLIO_AGENT TO ROLE PUBLIC;

CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

-- Re-runnable: ignore 400203 (agent already present), re-raise anything else
EXECUTE IMMEDIATE $$
BEGIN
  ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {{ db }}.{{ rep_agg }}.LOAN_PORTFOLIO_AGENT;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    IF (SQLCODE <> 400203) THEN
      RAISE;
    END IF;
END;
$$;

SHOW AGENTS IN SCHEMA {{ db }}.{{ rep_agg }};

SELECT 'LOAN_PORTFOLIO_AGENT created successfully! Retail Loans  and  Mortgages monitoring agent ready.' AS STATUS;

USE DATABASE {{ db }};
USE SCHEMA {{ crm_agg }};

CREATE OR REPLACE AGENT HR_EMPLOYEE_AGENT
  COMMENT = 'HR Employee Intelligence Agent - workforce headcount, org structure, hierarchy, tenure, offices, regions, performance ratings, languages, certifications'
  PROFILE = '{"display_name": "HR Employee", "avatar": "SparklesAgentIcon", "color": "#7B1FA2"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration: {}

  instructions:
    sample_questions:
      - question: "How many employees do we have?"
      - question: "Show me headcount by region"
      - question: "What is our org chart structure?"
      - question: "Which offices have the most staff?"
      - question: "What languages do our employees speak?"
      - question: "Average tenure of our team leaders?"
      - question: "List all employees in Germany"

    system: |
      You are the HR Employee Intelligence Agent for Synthetic Bank. You answer questions about the workforce: headcount, organizational structure, hierarchy, tenure, office locations, regions, performance ratings, languages spoken, and professional certifications.

      DATA SCOPE:
      - You have data on ALL bank employees: Client Advisors, Team Leaders, and Super Team Leaders
      - Employees span 12 countries across multiple European regions
      - The data includes names, hire dates, offices, performance ratings, languages, and certifications
      - Manager relationships enable org chart and hierarchy queries

      RESPONSE GUIDELINES:
      - Always provide specific numbers and breakdowns
      - When asked about "employees" or "staff", include ALL position levels unless a specific level is requested
      - For org chart questions, show the hierarchy: Super Team Leader -> Team Leaders -> Client Advisors
      - Include country and region context for geographic questions
      - Format headcount data in clear tables when multiple groupings are involved

  tools:
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: HR_Employee_Data
        description: |
          HR Employee Intelligence - workforce data covering all bank employees across 3 hierarchy levels.
          Answers: headcount (total, by country, region, office, position level), org chart and reporting structure,
          tenure analysis (hire dates, years of service), performance ratings, languages spoken, certifications,
          office and geographic distribution.
          Position Levels: SUPER_TEAM_LEADER, TEAM_LEADER, CLIENT_ADVISOR.

  tool_resources:
    HR_Employee_Data:
      semantic_view: {{ db }}.{{ crm_agg }}.EMPA_SV_HR_EMPLOYEE
      execution_environment:
        type: warehouse
        warehouse: MD_TEST_WH
        query_timeout: 30
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ crm_agg }}.HR_EMPLOYEE_AGENT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ crm_agg }}.HR_EMPLOYEE_AGENT TO ROLE PUBLIC;

-- Re-runnable: ignore 400203 (agent already present), re-raise anything else
EXECUTE IMMEDIATE $$
BEGIN
  ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {{ db }}.{{ crm_agg }}.HR_EMPLOYEE_AGENT;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    IF (SQLCODE <> 400203) THEN
      RAISE;
    END IF;
END;
$$;

SELECT 'HR_EMPLOYEE_AGENT created successfully! HR workforce intelligence agent ready.' AS STATUS;

USE DATABASE {{ db }};
USE SCHEMA {{ rep_agg }};

CREATE OR REPLACE AGENT UBER_AGENT
  COMMENT = 'Synthetic Bank Uber Agent - routes requests across Loan Portfolio, Liquidity Risk, Wealth Advisor, CRM Customer 360, and HR Employee sub-agents with smart domain-based routing.'
  PROFILE = '{"display_name": "Uber Agent", "avatar": "SparklesAgentIcon", "color": "#6A1B9A"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration:
    budget:
      seconds: 60
      tokens: 32000
    tool_not_accessible: accept

  instructions:
    sample_questions:
      - question: "What is our total loan exposure across all products?"
      - question: "What is the current LCR ratio and are we FINMA compliant?"
      - question: "Show me my top 10 wealth clients by AUM"
      - question: "Which customers are at high churn risk?"
      - question: "How many employees do we have?"
      - question: "Show me headcount by region or office"
      - question: "How many mortgage applications are under review?"
      - question: "Show me HQLA breakdown by level"
      - question: "Which advisors have the highest client retention?"
      - question: "Are there any loans with sanctions or PEP hits?"

    response: |
      Respond in the same language the user writes in. Structure responses clearly:
      1. Direct answer with key metrics
      2. Analysis and context (trends, comparisons, concentrations)
      3. Cross-domain insights when relevant (e.g., flag if a high-LTV borrower also has compliance issues)

      Use appropriate currency formatting. When presenting risk metrics, include the regulatory context (FINMA, FCA, BaFin, Basel III). If data suggests potential compliance concerns, note them proactively.

    orchestration: |
      Route every request to exactly one specialist agent first. Use this decision tree:

      1. **loan_portfolio** - Retail lending and mortgage origination
         Keywords: loan, mortgage, LTV, loan-to-value, application funnel, approval rate, decline rate, affordability, DTI, DSTI, sanctions screening, PEP screening (in loan context), compliance hold, collateral, origination, personal loan, loan exposure, application status

      2. **liquidity_risk** - Treasury, liquidity, and LCR
         Keywords: LCR, liquidity coverage ratio, HQLA, high-quality liquid assets, deposit outflows, run-off rate, FINMA liquidity, Basel III liquidity, 40% cap, Level 1/2A/2B assets, SNB reporting, liquidity buffer, treasury, funding stability, canton bonds, government bonds (in HQLA context), liquidity alerts

      3. **wealth_advisor** - Wealth management, portfolio performance, credit risk IRB, equity trading
         Keywords: portfolio performance, AUM, returns, Sharpe ratio, Sortino, drawdown, alpha, beta, asset allocation, IRB, probability of default, PD, LGD, EAD, risk-weighted assets, RWA, equity positions, P&L, trading, ISIN, investment account, wealth, private banking, rebalancing, client tier (PLATINUM/GOLD/PREMIUM in wealth context)

      4. **crm_customer_360** - Customer profiles, demographics, lifecycle, advisor-client relationships
         Keywords: customer 360, customer profile, demographics, contact details, address, onboarding, lifecycle stage, churn probability, engagement score, dormant customers, at-risk customers, customer segmentation, advisor performance, advisor capacity, advisor AUM, client retention, PEP/sanctions (in customer screening context), fraud flags, anomaly detection, KYC

      5. **hr_employee** - HR, workforce, org structure, employee master data
         Keywords: employee, employees, staff, headcount, HR, workforce, team size, hire date, tenure, team leader, manager, employment status, office, office location, region (in staff context), org chart, hierarchy, certifications, languages, position level, super team leader, how many people, workforce size, team structure, reporting structure

      **Overlap rules:**
      - "Customer risk + loan" -> loan_portfolio first, then crm_customer_360 if needed
      - "Customer risk + portfolio" -> wealth_advisor first, then crm_customer_360 if needed
      - "Advisor + portfolio" -> wealth_advisor (has advisor relationships built in)
      - "Advisor performance + client counts" -> crm_customer_360 (has detailed advisor metrics)
      - "Employee + advisor capacity" -> hr_employee first, then crm_customer_360 if needed
      - "Employee + client portfolio" -> crm_customer_360 (has advisor-client relationships)
      - "Regulatory compliance + loans" -> loan_portfolio
      - "Regulatory compliance + liquidity" -> liquidity_risk

      If the question is ambiguous or spans 3+ domains, ask the user to clarify which area they are most interested in.

    system: |
      You are the Synthetic Bank Uber Agent - a senior banking intelligence coordinator. You route each user request to the most appropriate specialist agent based on the domain. When a question spans multiple domains, call the primary domain first, then supplement with others. Always provide context-rich answers with regulatory awareness.

      CRITICAL RULE: Never refuse a question by saying you lack data without first trying the most relevant sub-agent. If the closest available data is a partial match, query it and explain what data scope is covered. For example, if asked about "employees", route to hr_employee which has complete workforce data. If asked about "advisors and their clients", route to crm_customer_360 which has advisor-client relationships.

  tools:
    - tool_spec:
        type: web_search
        name: Web Search
    - tool_spec:
        type: code_execution
        name: code_execution
    - tool_spec:
        type: agent_toolset
        name: loan_portfolio
    - tool_spec:
        type: agent_toolset
        name: liquidity_risk
    - tool_spec:
        type: agent_toolset
        name: wealth_advisor
    - tool_spec:
        type: agent_toolset
        name: crm_customer_360
    - tool_spec:
        type: agent_toolset
        name: hr_employee

  tool_resources:
    loan_portfolio:
      agent_name: {{ db }}.{{ rep_agg }}.LOAN_PORTFOLIO_AGENT
    liquidity_risk:
      agent_name: {{ db }}.{{ rep_agg }}.LIQUIDITY_RISK_AGENT
    wealth_advisor:
      agent_name: {{ db }}.{{ rep_agg }}.WEALTH_ADVISOR_AGENT
    crm_customer_360:
      agent_name: {{ db }}.{{ crm_agg }}.CRM_CUSTOMER_360
    hr_employee:
      agent_name: {{ db }}.{{ crm_agg }}.HR_EMPLOYEE_AGENT
  $$;

GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.UBER_AGENT TO ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT {{ db }}.{{ rep_agg }}.UBER_AGENT TO ROLE PUBLIC;

-- Re-runnable: ignore 400203 (agent already present), re-raise anything else
EXECUTE IMMEDIATE $$
BEGIN
  ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {{ db }}.{{ rep_agg }}.UBER_AGENT;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    IF (SQLCODE <> 400203) THEN
      RAISE;
    END IF;
END;
$$;

SELECT 'UBER_AGENT created successfully! Orchestrator agent with 7 sub-agents ready.' AS STATUS;

-- MASTER_AGENT: managed separately in post_deploy_master-agent.sql
-- Deploy via: ./deploy.sh DEV (or PROD)

USE DATABASE {{ db }};

USE SCHEMA AAA_DCM;

-- Share and listing names are per environment: DEV keeps SH_* / LS_*, PROD uses SH_PROD_* / LS_PROD_*
-- (account-level objects, so DEV and PROD in the same account must not share a name).
{% set is_prod = '_PRD_' in db %}
{% set sh = 'SH_PROD_' if is_prod else 'SH_' %}
{% set ls = 'LS_PROD_' if is_prod else 'LS_' %}
{% set title_suffix = ' (PROD)' if is_prod else '' %}

-- ============================================================
-- DATA PRODUCT SHARES
-- ============================================================

-- Product 1: Customer PII Vault (RESTRICTED)
CREATE SHARE IF NOT EXISTS {{ sh }}CUSTOMER_PII_VAULT
    COMMENT = 'Restricted: Customer PII vault with column-level masking policies. Only ACCOUNTADMIN/COMPLIANCE see unmasked values.';

GRANT USAGE ON DATABASE {{ db }} TO SHARE {{ sh }}CUSTOMER_PII_VAULT;
GRANT USAGE ON SCHEMA {{ db }}.{{ crm_agg }} TO SHARE {{ sh }}CUSTOMER_PII_VAULT;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.CRMI_AGG_DT_CUSTOMER_PII TO SHARE {{ sh }}CUSTOMER_PII_VAULT;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.CRMI_AGG_DT_ADDRESS_PII TO SHARE {{ sh }}CUSTOMER_PII_VAULT;

SELECT 'Share {{ sh }}CUSTOMER_PII_VAULT created.' AS STATUS;

-- Product 2: Employee PII Vault (RESTRICTED)
CREATE SHARE IF NOT EXISTS {{ sh }}EMPLOYEE_PII_VAULT
    COMMENT = 'Restricted: Employee PII vault with column-level masking policies. Only ACCOUNTADMIN/COMPLIANCE see unmasked values.';

GRANT USAGE ON DATABASE {{ db }} TO SHARE {{ sh }}EMPLOYEE_PII_VAULT;
GRANT USAGE ON SCHEMA {{ db }}.{{ crm_agg }} TO SHARE {{ sh }}EMPLOYEE_PII_VAULT;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.EMPI_AGG_DT_EMPLOYEE_PII TO SHARE {{ sh }}EMPLOYEE_PII_VAULT;

SELECT 'Share {{ sh }}EMPLOYEE_PII_VAULT created.' AS STATUS;

-- Product 3: Customer 360 & KYC (AI-READY)
CREATE SHARE IF NOT EXISTS {{ sh }}CUSTOMER_360_KYC
    COMMENT = 'Customer 360 & KYC data product - AI-ready with semantic views and Cortex Agents. Comprehensive customer profiles, AML/KYC screening, advisor performance.';

ALTER SHARE {{ sh }}CUSTOMER_360_KYC SET SECURE_OBJECTS_ONLY = FALSE;

GRANT USAGE ON DATABASE {{ db }} TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT USAGE ON SCHEMA {{ db }}.{{ crm_agg }} TO SHARE {{ sh }}CUSTOMER_360_KYC;

-- Customer 360 core
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_360 TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.CRMA_AGG_VW_CUSTOMER_360_ENRICHED TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_CURRENT TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_HISTORY TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_CUSTOMER_LIFECYCLE TO SHARE {{ sh }}CUSTOMER_360_KYC;

-- Address metadata (no PII)
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_ADDRESSES_CURRENT TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.CRMA_AGG_DT_ADDRESSES_HISTORY TO SHARE {{ sh }}CUSTOMER_360_KYC;

-- Accounts
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.ACCA_AGG_DT_ACCOUNTS TO SHARE {{ sh }}CUSTOMER_360_KYC;

-- Risk & compliance views (excluding cross-database sanctions views)
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.CRMA_AGG_VW_CUSTOMER_RISK_PROFILE TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.CRMA_AGG_VW_SCREENING_STATUS TO SHARE {{ sh }}CUSTOMER_360_KYC;

-- Employee / advisor analytics
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.EMPA_AGG_DT_ADVISOR_PERFORMANCE TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.EMPA_AGG_DT_PORTFOLIO_BY_ADVISOR TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON TABLE {{ db }}.{{ crm_agg }}.EMPA_AGG_DT_TEAM_LEADER_DASHBOARD TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ADVISORS TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ADVISOR_PERFORMANCE_ENRICHED TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_TEAM_DASHBOARD_ENRICHED TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_EMPLOYEE_HIERARCHY TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ORGANIZATIONAL_CHART TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_CURRENT_ASSIGNMENTS TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_ASSIGNMENT_HISTORY TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON VIEW {{ db }}.{{ crm_agg }}.EMPA_AGG_VW_WORKLOAD_DISTRIBUTION TO SHARE {{ sh }}CUSTOMER_360_KYC;

-- Semantic views (AI-ready)
GRANT SELECT ON SEMANTIC VIEW {{ db }}.{{ crm_agg }}.CRMA_SV_CUSTOMER_360 TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT REFERENCES ON SEMANTIC VIEW {{ db }}.{{ crm_agg }}.CRMA_SV_CUSTOMER_360 TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON SEMANTIC VIEW {{ db }}.{{ crm_agg }}.EMPA_SV_EMPLOYEE_ADVISOR TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT REFERENCES ON SEMANTIC VIEW {{ db }}.{{ crm_agg }}.EMPA_SV_EMPLOYEE_ADVISOR TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT SELECT ON SEMANTIC VIEW {{ db }}.{{ crm_agg }}.EMPA_SV_HR_EMPLOYEE TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT REFERENCES ON SEMANTIC VIEW {{ db }}.{{ crm_agg }}.EMPA_SV_HR_EMPLOYEE TO SHARE {{ sh }}CUSTOMER_360_KYC;

-- Cortex Agents (AI-ready)
GRANT USAGE ON AGENT {{ db }}.{{ crm_agg }}.CRM_Customer_360 TO SHARE {{ sh }}CUSTOMER_360_KYC;
GRANT USAGE ON AGENT {{ db }}.{{ crm_agg }}.HR_EMPLOYEE_AGENT TO SHARE {{ sh }}CUSTOMER_360_KYC;

SELECT 'Share {{ sh }}CUSTOMER_360_KYC created with AI objects.' AS STATUS;

-- ============================================================
-- ORGANIZATION LISTINGS
-- ============================================================

-- Product 1: Customer PII Vault
CREATE ORGANIZATION LISTING IF NOT EXISTS {{ ls }}CUSTOMER_PII_VAULT
SHARE {{ sh }}CUSTOMER_PII_VAULT AS
$$
title: "Customer PII Vault{{ title_suffix }}"
subtitle: "Restricted personal data with column-level masking and GDPR support"
description: "Restricted PII vault containing customer personal data protected by column-level masking policies. Only ACCOUNTADMIN and COMPLIANCE roles see unmasked values; all other roles receive masked output. Implements the Late Enrichment pattern: downstream analytics carry only CUSTOMER_ID, with PII joined back at display time. Includes customer addresses in a separate address PII table. All columns tagged with SENSITIVITY_LEVEL = restricted. Supports GDPR right-to-erasure. Quality gates: ROW_COUNT and NULL_PERCENT monitored via Snowflake Data Metric Functions."
organization_profile: "INTERNAL"
organization_targets:
  discovery:
    - all_internal_accounts: true
  access:
    - all_internal_accounts: true
support_contact: "marcel.daeppen@snowflake.com"
approver_contact: "marcel.daeppen@snowflake.com"
locations:
  access_regions:
    - name: "PUBLIC.AWS_EU_CENTRAL_1"
$$;

SELECT 'Listing {{ ls }}CUSTOMER_PII_VAULT published.' AS STATUS;

-- Product 2: Employee PII Vault
CREATE ORGANIZATION LISTING IF NOT EXISTS {{ ls }}EMPLOYEE_PII_VAULT
SHARE {{ sh }}EMPLOYEE_PII_VAULT AS
$$
title: "Employee PII Vault{{ title_suffix }}"
subtitle: "Restricted employee personal data with column-level masking"
description: "Restricted employee PII vault containing personal data for all bank staff including client advisors, team leaders, and super team leaders. Protected by column-level masking policies (only ACCOUNTADMIN and COMPLIANCE roles see unmasked values). All columns tagged with SENSITIVITY_LEVEL = restricted. Implements the Late Enrichment pattern for HR analytics and advisor performance reporting. Downstream tables carry only EMPLOYEE_ID; this vault is joined at display time for name resolution. Quality gates: ROW_COUNT and NULL_PERCENT monitored via Snowflake Data Metric Functions."
organization_profile: "INTERNAL"
organization_targets:
  discovery:
    - all_internal_accounts: true
  access:
    - all_internal_accounts: true
support_contact: "marcel.daeppen@snowflake.com"
approver_contact: "marcel.daeppen@snowflake.com"
locations:
  access_regions:
    - name: "PUBLIC.AWS_EU_CENTRAL_1"
$$;

SELECT 'Listing {{ ls }}EMPLOYEE_PII_VAULT published.' AS STATUS;

-- Product 3: Customer 360 & KYC (AI-Ready)
CREATE ORGANIZATION LISTING IF NOT EXISTS {{ ls }}CUSTOMER_360_KYC
SHARE {{ sh }}CUSTOMER_360_KYC AS
$$
title: "Customer 360 & KYC{{ title_suffix }}"
subtitle: "AI-ready customer profiles with AML/KYC screening and advisor analytics"
description: "Centralized customer master data product with comprehensive 360-degree profiles, SCD Type 2 history tracking, and Late Enrichment PII pattern. Core capabilities: Customer 360 profiles with 70+ attributes (identity, accounts, risk, compliance); PEP (Politically Exposed Persons) checks with confidence scoring; lifecycle engagement scoring, churn prediction, and vulnerability flags; consolidated risk profiles (NO_RISK through CRITICAL); account master data with multi-currency support; SCD Type 2 history for customer attributes and addresses. Advisor analytics: client advisor performance metrics (AUM, client counts, retention), team leader dashboards, organizational hierarchy, and workload distribution. AI-ready: 3 Semantic Views (CRMA_SV_CUSTOMER_360, EMPA_SV_EMPLOYEE_ADVISOR, EMPA_SV_HR_EMPLOYEE) for natural language querying via Cortex Analyst; 2 Cortex Agents (CRM_Customer_360, HR_EMPLOYEE_AGENT) for conversational data exploration; 200+ synonym mappings for business-friendly queries. Quality gates: ROW_COUNT, FRESHNESS, and NULL_PERCENT monitored via Snowflake Data Metric Functions. Data freshness: hourly refresh via dynamic tables (60-minute target lag). 25+ objects included across customer profiles, risk views, advisor performance, and organizational structure."
organization_profile: "INTERNAL"
organization_targets:
  discovery:
    - all_internal_accounts: true
  access:
    - all_internal_accounts: true
support_contact: "marcel.daeppen@snowflake.com"
approver_contact: "marcel.daeppen@snowflake.com"
locations:
  access_regions:
    - name: "PUBLIC.AWS_EU_CENTRAL_1"
$$;

SELECT 'Listing {{ ls }}CUSTOMER_360_KYC published with AI-ready tag.' AS STATUS;
