/*
 * 580_FCOA_consolidation.sql
 * Financial Control showcase: management group consolidation in CHF (illustrative, synthetic data).
 *
 *   FCOA_AGG_VW_GROUP_STRUCTURE        fictional group: 9 entities, functional currency, ownership, P&L drivers (inline)
 *   FCOA_AGG_VW_FS_LINES               mini chart of accounts: balance sheet and P&L lines with annual rates (inline)
 *   FCOA_AGG_VW_FS_LINE_MAPPING        source product type -> FS line (inline)
 *   FCOA_AGG_VW_IC_PAIRS               intercompany funding pairs (inline)
 *   FCOA_AGG_DT_FX_PERIODS             closing / average / historical CCY->CHF rate per reporting month
 *   FCOA_AGG_DT_ENTITY_POSITIONS       liquidity book positions per month, entity, FS line, product and currency
 *   FCOA_AGG_DT_ENTITY_BALANCE_SHEET   entity balance sheet in functional currency and CHF (incl. IC, equity, CTA)
 *   FCOA_AGG_DT_ENTITY_PNL             entity monthly management P&L derived from balances and drivers
 *   FCOA_AGG_VW_FX_TRANSLATION         translation of the foreign entities (closing / average / historical rate, CTA)
 *   FCOA_AGG_VW_IC_RECONCILIATION      IC pairs side A vs side B, incl. the opt-in demo exception
 *   FCOA_AGG_DT_GROUP_CONSOLIDATION    entity, IC elimination, NCI and group columns per FS line
 *   FCOA_AGG_VW_CONSOLIDATION_BRIDGE   month-over-month bridge: entity performance, FX, IC eliminations, NCI
 *   FCOA_AGG_VW_CONSOLIDATION_TRAIL    answer trail from source positions to the group value
 *   FCOA_AGG_VW_CONSOLIDATION_CHECKS   controls: balance, IC net-to-zero, CTA, source completeness, freshness
 *
 * Balances come from the shared liquidity book (REPP_AGG_DT_LIQ_POSITIONS, valid records only) at the last
 * snapshot of each month; FX rates from REPP_AGG_DT_LCR_FX_DAILY (USD quotes crossed via USD). Mappings,
 * drivers and IC pairs are inline VALUES: no new tables. Everything is illustrative, not IFRS.
 */

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_GROUP_STRUCTURE(
    ENTITY_ID COMMENT 'Legal entity code (LE_01..LE_09), same codes as the liquidity book.',
    DISPLAY_NAME COMMENT 'Fictional display name of the entity (illustrative).',
    COUNTRY COMMENT 'Country of the entity (ISO 3166 alpha-2).',
    FUNCTIONAL_CCY COMMENT 'Functional currency of the entity; the group reports in CHF.',
    PARENT_ID COMMENT 'Parent entity; NULL for the group holding.',
    OWNERSHIP_PCT COMMENT 'Group ownership share (1 = 100 %); the remainder is non-controlling interest (NCI).',
    EQUITY_RATIO COMMENT 'Equity as share of total assets (management assumption).',
    FEE_BPS COMMENT 'Annual fee and commission income in basis points of loans and securities.',
    COST_INCOME_RATIO COMMENT 'Operating expenses as share of operating income (60 % personnel, 40 % other).',
    TAX_RATE COMMENT 'Income tax rate on positive profit before tax.',
    SORT_ORDER COMMENT 'Display order of the entity columns.'
) COMMENT = 'Fictional group structure of the Financial Control showcase: display names, functional currency, ownership and the management drivers of the illustrative P&L. Inline values, no table.'
AS
SELECT ENTITY_ID::VARCHAR(10), DISPLAY_NAME::VARCHAR(50), COUNTRY::VARCHAR(2), FUNCTIONAL_CCY::VARCHAR(3), PARENT_ID::VARCHAR(10),
       OWNERSHIP_PCT::NUMBER(5,4), EQUITY_RATIO::NUMBER(5,4), FEE_BPS::NUMBER(6,2), COST_INCOME_RATIO::NUMBER(5,4),
       TAX_RATE::NUMBER(5,4), SORT_ORDER::INTEGER
FROM VALUES
    ('LE_01', 'Group Holding CH', 'CH', 'CHF', NULL,    1.00, 0.12, 15, 0.55, 0.19, 1),
    ('LE_02', 'Bank CH',          'CH', 'CHF', 'LE_01', 1.00, 0.08, 25, 0.62, 0.19, 2),
    ('LE_03', 'Custody CH',       'CH', 'CHF', 'LE_01', 1.00, 0.07, 45, 0.68, 0.19, 3),
    ('LE_04', 'Markets CH',       'CH', 'CHF', 'LE_01', 1.00, 0.09, 35, 0.66, 0.19, 4),
    ('LE_05', 'Services CH',      'CH', 'CHF', 'LE_01', 1.00, 0.08, 20, 0.70, 0.19, 5),
    ('LE_06', 'Wealth CH',        'CH', 'CHF', 'LE_01', 0.80, 0.09, 60, 0.64, 0.19, 6),
    ('LE_07', 'Bank EU (DE)',     'DE', 'EUR', 'LE_01', 1.00, 0.08, 25, 0.71, 0.30, 7),
    ('LE_08', 'Fund Services LU', 'LU', 'EUR', 'LE_01', 0.75, 0.10, 55, 0.69, 0.25, 8),
    ('LE_09', 'Brokerage US',     'US', 'USD', 'LE_01', 1.00, 0.09, 50, 1.04, 0.21, 9)
    AS v(ENTITY_ID, DISPLAY_NAME, COUNTRY, FUNCTIONAL_CCY, PARENT_ID, OWNERSHIP_PCT, EQUITY_RATIO, FEE_BPS,
         COST_INCOME_RATIO, TAX_RATE, SORT_ORDER);

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES(
    FS_LINE COMMENT 'Financial statement line code.',
    STATEMENT COMMENT 'BS = balance sheet, PL = profit and loss.',
    SECTION COMMENT 'ASSETS, LIABILITIES, EQUITY or PNL.',
    LABEL COMMENT 'Display label of the line.',
    SORT_ORDER COMMENT 'Display order within the statement.',
    ANNUAL_RATE COMMENT 'Annual interest rate of the line (income on assets, expense on liabilities); NULL when not interest bearing.',
    IS_INTERCOMPANY COMMENT 'TRUE for intercompany lines that are eliminated in the group column.'
) COMMENT = 'Mini chart of accounts of the consolidation showcase (balance sheet and P&L lines, interest rate assumptions). Inline values, no table.'
AS
SELECT FS_LINE::VARCHAR(30), STATEMENT::VARCHAR(2), SECTION::VARCHAR(12), LABEL::VARCHAR(60), SORT_ORDER::INTEGER,
       ANNUAL_RATE::NUMBER(6,4), IS_INTERCOMPANY::BOOLEAN
FROM VALUES
    ('CASH_CB',               'BS', 'ASSETS',      'Cash & central bank',             10, 0.0040, FALSE),
    ('INTERBANK_ASSETS',      'BS', 'ASSETS',      'Due from banks',                  20, 0.0100, FALSE),
    ('CLIENT_LOANS',          'BS', 'ASSETS',      'Loans to clients',                30, 0.0260, FALSE),
    ('SECURITIES',            'BS', 'ASSETS',      'Securities',                      40, 0.0120, FALSE),
    ('IC_RECEIVABLES',        'BS', 'ASSETS',      'Intercompany receivables',        50, NULL,   TRUE),
    ('OTHER_ASSETS',          'BS', 'ASSETS',      'Other assets',                    60, NULL,   FALSE),
    ('CLIENT_DEPOSITS',       'BS', 'LIABILITIES', 'Client deposits',                110, 0.0045, FALSE),
    ('INTERBANK_LIABILITIES', 'BS', 'LIABILITIES', 'Due to banks',                   120, 0.0110, FALSE),
    ('IC_PAYABLES',           'BS', 'LIABILITIES', 'Intercompany payables',          130, NULL,   TRUE),
    ('OTHER_LIABILITIES',     'BS', 'LIABILITIES', 'Debt issued & other liabilities',140, 0.0200, FALSE),
    ('SHARE_CAPITAL',         'BS', 'EQUITY',      'Share capital',                  210, NULL,   FALSE),
    ('RESERVES',              'BS', 'EQUITY',      'Reserves & retained earnings',   220, NULL,   FALSE),
    ('CTA',                   'BS', 'EQUITY',      'Currency translation reserve',   230, NULL,   FALSE),
    ('NCI',                   'BS', 'EQUITY',      'Non-controlling interests',      240, NULL,   FALSE),
    ('INTEREST_INCOME',       'PL', 'PNL',         'Interest income',                310, NULL,   FALSE),
    ('INTEREST_EXPENSE',      'PL', 'PNL',         'Interest expense',               320, NULL,   FALSE),
    ('IC_INTEREST',           'PL', 'PNL',         'Intercompany interest',          330, NULL,   TRUE),
    ('FEE_INCOME',            'PL', 'PNL',         'Fee & commission income',        340, NULL,   FALSE),
    ('IC_SERVICE_FEE',        'PL', 'PNL',         'Intercompany service fees',      350, NULL,   TRUE),
    ('PERSONNEL',             'PL', 'PNL',         'Personnel expenses',             360, NULL,   FALSE),
    ('OTHER_OPEX',            'PL', 'PNL',         'Other operating expenses',       370, NULL,   FALSE),
    ('TAX',                   'PL', 'PNL',         'Income taxes',                   380, NULL,   FALSE),
    ('NCI_SHARE',             'PL', 'PNL',         'Profit attributable to NCI',     390, NULL,   FALSE)
    AS v(FS_LINE, STATEMENT, SECTION, LABEL, SORT_ORDER, ANNUAL_RATE, IS_INTERCOMPANY);

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINE_MAPPING(
    PRODUCT_TYPE COMMENT 'Source product type of the liquidity book (REPP_AGG_DT_LIQ_POSITIONS.PRODUCT_TYPE).',
    AKTIV_PASSIV COMMENT 'Balance sheet side of the source position: A = asset, P = liability.',
    FS_LINE COMMENT 'Target financial statement line (FCOA_AGG_VW_FS_LINES).'
) COMMENT = 'Mapping rules from source product types to balance sheet lines. Unmapped products fall back to OTHER_ASSETS / OTHER_LIABILITIES and are reported by the source completeness control. Inline values, no table.'
AS
SELECT PRODUCT_TYPE::VARCHAR(50), AKTIV_PASSIV::VARCHAR(1), FS_LINE::VARCHAR(30)
FROM VALUES
    ('CASH_SNB',                  'A', 'CASH_CB'),
    ('CASH_VAULT',                'A', 'CASH_CB'),
    ('INTERBANK_LOAN',            'A', 'INTERBANK_ASSETS'),
    ('LOAN_CORP',                 'A', 'CLIENT_LOANS'),
    ('LOAN_RETAIL',               'A', 'CLIENT_LOANS'),
    ('CREDIT_FACILITY',           'A', 'CLIENT_LOANS'),
    ('LIQUIDITY_FACILITY',        'A', 'CLIENT_LOANS'),
    ('GOVT_BOND_CHF',             'A', 'SECURITIES'),
    ('GOVT_BOND_FOREIGN',         'A', 'SECURITIES'),
    ('CANTON_BOND',               'A', 'SECURITIES'),
    ('COVERED_BOND',              'A', 'SECURITIES'),
    ('CORPORATE_BOND_AA',         'A', 'SECURITIES'),
    ('CORPORATE_BOND_A_BBB',      'A', 'SECURITIES'),
    ('EQUITY_SMI',                'A', 'SECURITIES'),
    ('RETAIL_STABLE_INSURED',     'P', 'CLIENT_DEPOSITS'),
    ('RETAIL_STABLE',             'P', 'CLIENT_DEPOSITS'),
    ('RETAIL_LESS_STABLE',        'P', 'CLIENT_DEPOSITS'),
    ('CORPORATE_OPERATIONAL',     'P', 'CLIENT_DEPOSITS'),
    ('CORPORATE_NON_OPERATIONAL', 'P', 'CLIENT_DEPOSITS'),
    ('FINANCIAL_INSTITUTION',     'P', 'INTERBANK_LIABILITIES'),
    ('INTERBANK_BORROWING',       'P', 'INTERBANK_LIABILITIES')
    AS v(PRODUCT_TYPE, AKTIV_PASSIV, FS_LINE);

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_IC_PAIRS(
    PAIR_ID COMMENT 'Intercompany pair identifier.',
    LENDER_ID COMMENT 'Entity holding the intercompany receivable (side A).',
    BORROWER_ID COMMENT 'Entity holding the intercompany payable (side B); the pair is denominated in its functional currency.',
    IC_TYPE COMMENT 'IC_LOAN (intragroup funding) or IC_DEPOSIT (intragroup cash placement).',
    SHARE_OF_BORROWER_ASSETS COMMENT 'Pair amount as share of the borrower''s external assets in its functional currency.',
    ANNUAL_RATE COMMENT 'Annual intercompany interest rate of the pair.',
    DESCRIPTION COMMENT 'Business description of the pair.',
    IS_DEMO_EXCEPTION COMMENT 'TRUE for the pair used by the opt-in demo exception (borrower side not revalued at the closing rate).'
) COMMENT = 'Intercompany positions of the consolidation showcase, defined as shares of the borrower''s balance sheet so they scale with the data. Both sides are booked at the same closing rate and net to zero; the demo exception is applied only in FCOA_AGG_VW_IC_RECONCILIATION. Inline values, no table.'
AS
SELECT PAIR_ID::VARCHAR(10), LENDER_ID::VARCHAR(10), BORROWER_ID::VARCHAR(10), IC_TYPE::VARCHAR(20),
       SHARE_OF_BORROWER_ASSETS::NUMBER(6,4), ANNUAL_RATE::NUMBER(6,4), DESCRIPTION::VARCHAR(200), IS_DEMO_EXCEPTION::BOOLEAN
FROM VALUES
    ('IC01', 'LE_01', 'LE_07', 'IC_LOAN',    0.10, 0.0150, 'Intragroup funding loan to Bank EU (EUR)',            FALSE),
    ('IC02', 'LE_01', 'LE_09', 'IC_LOAN',    0.12, 0.0200, 'Intragroup funding loan to Brokerage US (USD)',       TRUE),
    ('IC03', 'LE_01', 'LE_08', 'IC_LOAN',    0.08, 0.0150, 'Intragroup funding loan to Fund Services LU (EUR)',   FALSE),
    ('IC04', 'LE_01', 'LE_04', 'IC_LOAN',    0.06, 0.0120, 'Intragroup funding loan to Markets CH (CHF)',         FALSE),
    ('IC05', 'LE_03', 'LE_02', 'IC_DEPOSIT', 0.05, 0.0080, 'Custody CH places client cash at Bank CH (CHF)',      FALSE)
    AS v(PAIR_ID, LENDER_ID, BORROWER_ID, IC_TYPE, SHARE_OF_BORROWER_ASSETS, ANNUAL_RATE, DESCRIPTION, IS_DEMO_EXCEPTION);

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_FX_PERIODS(
    PERIOD_MONTH DATE COMMENT 'Reporting month (first day of the month).',
    PERIOD_DATE DATE COMMENT 'Reporting date: last liquidity book snapshot of the month.',
    CURRENCY VARCHAR(3) COMMENT 'Currency converted to CHF (CHF, EUR, USD).',
    CLOSING_RATE NUMBER(18,8) COMMENT 'Closing rate on PERIOD_DATE: 1 unit of CURRENCY in CHF (balance sheet translation).',
    AVERAGE_RATE NUMBER(18,8) COMMENT 'Average daily rate from the first day of the month to PERIOD_DATE (P&L translation).',
    HISTORICAL_RATE NUMBER(18,8) COMMENT 'Closing rate of the first reporting month (share capital translation).',
    PREV_PERIOD_MONTH DATE COMMENT 'Previous reporting month (NULL for the first month).',
    PREV_CLOSING_RATE NUMBER(18,8) COMMENT 'Closing rate of the previous reporting month (bridge FX effect, demo exception).',
    PREV_AVERAGE_RATE NUMBER(18,8) COMMENT 'Average rate of the previous reporting month (bridge FX effect on profit).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh.'
) COMMENT = 'Translation rates of the consolidation showcase per reporting month: closing, average and historical CCY->CHF rate (from REPP_AGG_DT_LCR_FX_DAILY, USD quotes crossed via USD).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
REFRESH_MODE = FULL
AS
WITH per AS (
    SELECT DATE_TRUNC(month, AS_OF_DATE)::DATE AS PERIOD_MONTH, MAX(AS_OF_DATE) AS PERIOD_DATE
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS
    WHERE DQ_STATUS = 'OK'
    GROUP BY 1
),
r AS (
    SELECT p.PERIOD_MONTH, p.PERIOD_DATE, f.CURRENCY,
           MAX(IFF(f.AS_OF_DATE = p.PERIOD_DATE, f.RATE_TO_CHF, NULL)) AS CLOSING_RATE,
           AVG(f.RATE_TO_CHF) AS AVERAGE_RATE
    FROM per p
    JOIN {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LCR_FX_DAILY f
      ON f.AS_OF_DATE BETWEEN p.PERIOD_MONTH AND p.PERIOD_DATE
    WHERE f.CURRENCY IN ('CHF', 'EUR', 'USD')
    GROUP BY 1, 2, 3
)
SELECT PERIOD_MONTH, PERIOD_DATE, CURRENCY::VARCHAR(3) AS CURRENCY,
       CLOSING_RATE::NUMBER(18,8) AS CLOSING_RATE,
       AVERAGE_RATE::NUMBER(18,8) AS AVERAGE_RATE,
       FIRST_VALUE(CLOSING_RATE) OVER (PARTITION BY CURRENCY ORDER BY PERIOD_MONTH)::NUMBER(18,8) AS HISTORICAL_RATE,
       LAG(PERIOD_MONTH) OVER (PARTITION BY CURRENCY ORDER BY PERIOD_MONTH) AS PREV_PERIOD_MONTH,
       LAG(CLOSING_RATE) OVER (PARTITION BY CURRENCY ORDER BY PERIOD_MONTH)::NUMBER(18,8) AS PREV_CLOSING_RATE,
       LAG(AVERAGE_RATE) OVER (PARTITION BY CURRENCY ORDER BY PERIOD_MONTH)::NUMBER(18,8) AS PREV_AVERAGE_RATE,
       CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM r
WHERE CLOSING_RATE IS NOT NULL;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_POSITIONS(
    PERIOD_MONTH DATE COMMENT 'Reporting month (first day of the month).',
    PERIOD_DATE DATE COMMENT 'Reporting date: last liquidity book snapshot of the month.',
    ENTITY_ID VARCHAR(10) COMMENT 'Legal entity (LE_01..LE_09).',
    AKTIV_PASSIV VARCHAR(1) COMMENT 'Balance sheet side: A = asset, P = liability.',
    FS_LINE VARCHAR(30) COMMENT 'Financial statement line from FCOA_AGG_VW_FS_LINE_MAPPING (fallback OTHER_ASSETS / OTHER_LIABILITIES).',
    PRODUCT_TYPE VARCHAR(50) COMMENT 'Source product type.',
    IS_MAPPED BOOLEAN COMMENT 'FALSE when the product type has no mapping rule and fell back to an OTHER line.',
    CURRENCY VARCHAR(3) COMMENT 'Original currency of the positions.',
    POSITION_COUNT NUMBER(18,0) COMMENT 'Number of source positions (REPP_AGG_DT_LIQ_POSITIONS rows).',
    AMOUNT_CCY NUMBER(18,2) COMMENT 'Balance in original currency (positive on both sides).',
    AMOUNT_CHF NUMBER(18,2) COMMENT 'Balance in CHF at the closing rate of PERIOD_DATE (as converted by the liquidity book).',
    CALCULATION_TIMESTAMP TIMESTAMP_NTZ COMMENT 'UTC timestamp of the dynamic table refresh (data freshness control).'
) COMMENT = 'External balances of the consolidation showcase: valid liquidity book positions (DQ_STATUS = OK) at the last snapshot of each month, mapped to balance sheet lines, per entity, product and currency.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
REFRESH_MODE = FULL
AS
WITH per AS (
    SELECT DATE_TRUNC(month, AS_OF_DATE)::DATE AS PERIOD_MONTH, MAX(AS_OF_DATE) AS PERIOD_DATE
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS
    WHERE DQ_STATUS = 'OK'
    GROUP BY 1
)
SELECT p.PERIOD_MONTH, p.PERIOD_DATE, x.ENTITY_ID, x.AKTIV_PASSIV,
       COALESCE(m.FS_LINE, IFF(x.AKTIV_PASSIV = 'A', 'OTHER_ASSETS', 'OTHER_LIABILITIES'))::VARCHAR(30) AS FS_LINE,
       x.PRODUCT_TYPE, (m.FS_LINE IS NOT NULL) AS IS_MAPPED, x.CURRENCY,
       COUNT(*) AS POSITION_COUNT,
       SUM(x.AMOUNT_CCY)::NUMBER(18,2) AS AMOUNT_CCY,
       SUM(x.AMOUNT_CHF)::NUMBER(18,2) AS AMOUNT_CHF,
       CURRENT_TIMESTAMP() AS CALCULATION_TIMESTAMP
FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS x
JOIN per p ON x.AS_OF_DATE = p.PERIOD_DATE
LEFT JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINE_MAPPING m
  ON m.PRODUCT_TYPE = x.PRODUCT_TYPE AND m.AKTIV_PASSIV = x.AKTIV_PASSIV
WHERE x.DQ_STATUS = 'OK'
GROUP BY p.PERIOD_MONTH, p.PERIOD_DATE, x.ENTITY_ID, x.AKTIV_PASSIV, 5, x.PRODUCT_TYPE, 7, x.CURRENCY;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_BALANCE_SHEET(
    PERIOD_MONTH DATE COMMENT 'Reporting month (first day of the month).',
    PERIOD_DATE DATE COMMENT 'Reporting date of the balances.',
    ENTITY_ID VARCHAR(10) COMMENT 'Legal entity.',
    FS_LINE VARCHAR(30) COMMENT 'Balance sheet line (FCOA_AGG_VW_FS_LINES).',
    BASIS_CCY VARCHAR(3) COMMENT 'Currency the amount is held in: position currency for external balances, borrower currency for IC pairs, functional currency otherwise.',
    AMOUNT_BASIS NUMBER(18,2) COMMENT 'Amount in BASIS_CCY.',
    FUNCTIONAL_CCY VARCHAR(3) COMMENT 'Functional currency of the entity.',
    AMOUNT_LCY NUMBER(18,2) COMMENT 'Amount in the functional currency of the entity (local currency view).',
    RATE_TYPE VARCHAR(12) COMMENT 'Translation rate applied to reach CHF: CLOSING, HISTORICAL (share capital), AVERAGE (reserves) or RESIDUAL (CTA).',
    TRANSLATION_RATE NUMBER(18,8) COMMENT 'Functional currency -> CHF rate applied (NULL for the CTA residual).',
    AMOUNT_CHF NUMBER(18,2) COMMENT 'Amount in CHF (group reporting currency).'
) COMMENT = 'Entity balance sheets of the consolidation showcase in functional currency and CHF: external balances, intercompany receivables and payables, the funding residual (debt issued / other assets), equity split into share capital (historical rate), reserves (average rate) and the currency translation reserve (CTA residual). Every entity balances by construction; the checks view proves it. Materialized so the reporting views stay flat (fast compile).'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
REFRESH_MODE = FULL
AS
WITH gs AS (SELECT * FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_GROUP_STRUCTURE),
fx AS (SELECT * FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_FX_PERIODS),
ext AS (
    SELECT e.PERIOD_MONTH, e.PERIOD_DATE, e.ENTITY_ID, e.FS_LINE, e.CURRENCY AS BASIS_CCY,
           SUM(e.AMOUNT_CCY) AS AMOUNT_BASIS, SUM(e.AMOUNT_CHF) AS AMOUNT_CHF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_POSITIONS e
    GROUP BY 1, 2, 3, 4, 5
),
ext_assets AS (
    SELECT PERIOD_MONTH, ENTITY_ID, SUM(AMOUNT_CHF) AS ASSETS_CHF
    FROM ext WHERE FS_LINE IN ('CASH_CB', 'INTERBANK_ASSETS', 'CLIENT_LOANS', 'SECURITIES', 'OTHER_ASSETS')
    GROUP BY 1, 2
),
-- IC pair amount in the borrower's functional currency, booked at the same closing rate on both sides
ic AS (
    SELECT a.PERIOD_MONTH, p.PAIR_ID, p.LENDER_ID, p.BORROWER_ID, b.FUNCTIONAL_CCY AS PAIR_CCY,
           p.SHARE_OF_BORROWER_ASSETS * a.ASSETS_CHF / f.CLOSING_RATE AS AMOUNT_PAIR_CCY,
           p.SHARE_OF_BORROWER_ASSETS * a.ASSETS_CHF AS AMOUNT_CHF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_IC_PAIRS p
    JOIN gs b ON b.ENTITY_ID = p.BORROWER_ID
    JOIN ext_assets a ON a.ENTITY_ID = p.BORROWER_ID
    JOIN fx f ON f.PERIOD_MONTH = a.PERIOD_MONTH AND f.CURRENCY = b.FUNCTIONAL_CCY
),
lines AS (
    SELECT PERIOD_MONTH, ENTITY_ID, FS_LINE, BASIS_CCY, AMOUNT_BASIS, AMOUNT_CHF FROM ext
    UNION ALL
    SELECT PERIOD_MONTH, LENDER_ID, 'IC_RECEIVABLES', PAIR_CCY, SUM(AMOUNT_PAIR_CCY), SUM(AMOUNT_CHF) FROM ic GROUP BY 1, 2, 4
    UNION ALL
    SELECT PERIOD_MONTH, BORROWER_ID, 'IC_PAYABLES', PAIR_CCY, SUM(AMOUNT_PAIR_CCY), SUM(AMOUNT_CHF) FROM ic GROUP BY 1, 2, 4
),
lcy AS (
    SELECT l.PERIOD_MONTH, f.PERIOD_DATE, l.ENTITY_ID, l.FS_LINE, l.BASIS_CCY, l.AMOUNT_BASIS, g.FUNCTIONAL_CCY,
           l.AMOUNT_CHF / f.CLOSING_RATE AS AMOUNT_LCY, f.CLOSING_RATE, l.AMOUNT_CHF, s.SECTION
    FROM lines l
    JOIN gs g ON g.ENTITY_ID = l.ENTITY_ID
    JOIN fx f ON f.PERIOD_MONTH = l.PERIOD_MONTH AND f.CURRENCY = g.FUNCTIONAL_CCY
    JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES s ON s.FS_LINE = l.FS_LINE
),
-- equity = equity ratio x assets; the residual is funded by debt issued (or held as other assets)
tot AS (
    SELECT l.PERIOD_MONTH, l.PERIOD_DATE, l.ENTITY_ID, l.FUNCTIONAL_CCY, l.CLOSING_RATE, g.EQUITY_RATIO,
           SUM(IFF(l.SECTION = 'ASSETS', l.AMOUNT_LCY, 0)) AS ASSETS_LCY,
           SUM(IFF(l.SECTION = 'LIABILITIES', l.AMOUNT_LCY, 0)) AS LIAB_LCY
    FROM lcy l JOIN gs g ON g.ENTITY_ID = l.ENTITY_ID
    GROUP BY 1, 2, 3, 4, 5, 6
),
eq AS (
    SELECT t.*, t.EQUITY_RATIO * t.ASSETS_LCY AS EQUITY_LCY,
           t.ASSETS_LCY - t.LIAB_LCY - t.EQUITY_RATIO * t.ASSETS_LCY AS RESIDUAL_LCY,
           f.AVERAGE_RATE, f.HISTORICAL_RATE
    FROM tot t
    JOIN fx f ON f.PERIOD_MONTH = t.PERIOD_MONTH AND f.CURRENCY = t.FUNCTIONAL_CCY
),
eq2 AS (
    -- share capital = 80 % of the first month's equity, so reserves stay positive when the balance sheet shrinks
    SELECT eq.*, 0.8 * FIRST_VALUE(EQUITY_LCY) OVER (PARTITION BY ENTITY_ID ORDER BY PERIOD_MONTH) AS CAPITAL_LCY
    FROM eq
)
SELECT PERIOD_MONTH, PERIOD_DATE, ENTITY_ID, FS_LINE::VARCHAR(30) AS FS_LINE, BASIS_CCY::VARCHAR(3) AS BASIS_CCY,
       AMOUNT_BASIS::NUMBER(18,2), FUNCTIONAL_CCY, AMOUNT_LCY::NUMBER(18,2), 'CLOSING'::VARCHAR(12),
       CLOSING_RATE::NUMBER(18,8), AMOUNT_CHF::NUMBER(18,2)
FROM lcy
UNION ALL
SELECT PERIOD_MONTH, PERIOD_DATE, ENTITY_ID, IFF(RESIDUAL_LCY >= 0, 'OTHER_LIABILITIES', 'OTHER_ASSETS'), FUNCTIONAL_CCY,
       ABS(RESIDUAL_LCY), FUNCTIONAL_CCY, ABS(RESIDUAL_LCY), 'CLOSING', CLOSING_RATE, ABS(RESIDUAL_LCY) * CLOSING_RATE
FROM eq2
UNION ALL
SELECT PERIOD_MONTH, PERIOD_DATE, ENTITY_ID, 'SHARE_CAPITAL', FUNCTIONAL_CCY, CAPITAL_LCY, FUNCTIONAL_CCY, CAPITAL_LCY,
       'HISTORICAL', HISTORICAL_RATE, CAPITAL_LCY * HISTORICAL_RATE
FROM eq2
UNION ALL
SELECT PERIOD_MONTH, PERIOD_DATE, ENTITY_ID, 'RESERVES', FUNCTIONAL_CCY, EQUITY_LCY - CAPITAL_LCY, FUNCTIONAL_CCY,
       EQUITY_LCY - CAPITAL_LCY, 'AVERAGE', AVERAGE_RATE, (EQUITY_LCY - CAPITAL_LCY) * AVERAGE_RATE
FROM eq2
UNION ALL
SELECT PERIOD_MONTH, PERIOD_DATE, ENTITY_ID, 'CTA', FUNCTIONAL_CCY, 0, FUNCTIONAL_CCY, 0, 'RESIDUAL', NULL,
       EQUITY_LCY * CLOSING_RATE - CAPITAL_LCY * HISTORICAL_RATE - (EQUITY_LCY - CAPITAL_LCY) * AVERAGE_RATE
FROM eq2;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_PNL(
    PERIOD_MONTH DATE COMMENT 'Reporting month (first day of the month).',
    ENTITY_ID VARCHAR(10) COMMENT 'Legal entity.',
    FS_LINE VARCHAR(30) COMMENT 'P&L line (FCOA_AGG_VW_FS_LINES); income positive, expenses negative.',
    FUNCTIONAL_CCY VARCHAR(3) COMMENT 'Functional currency of the entity.',
    AMOUNT_LCY NUMBER(18,2) COMMENT 'Monthly amount in functional currency.',
    AVERAGE_RATE NUMBER(18,8) COMMENT 'Average functional currency -> CHF rate of the month.',
    AMOUNT_CHF NUMBER(18,2) COMMENT 'Monthly amount in CHF at the average rate.',
    DRIVER VARCHAR(100) COMMENT 'How the amount is derived (balance x rate, fee bps, cost-income ratio, tax rate, IC pair).'
) COMMENT = 'Illustrative monthly management P&L per entity, derived from the closing balances and the drivers of FCOA_AGG_VW_GROUP_STRUCTURE / FCOA_AGG_VW_FS_LINES: interest income and expense, intercompany interest and service fees (net to zero in the group), fees, personnel and other opex, taxes. Translated at the monthly average rate.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
REFRESH_MODE = FULL
AS
WITH gs AS (SELECT * FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_GROUP_STRUCTURE),
fx AS (SELECT * FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_FX_PERIODS),
bs AS (
    SELECT b.PERIOD_MONTH, b.ENTITY_ID, b.FS_LINE, b.FUNCTIONAL_CCY, SUM(b.AMOUNT_LCY) AS AMOUNT_LCY, SUM(b.AMOUNT_CHF) AS AMOUNT_CHF,
           s.SECTION, s.ANNUAL_RATE
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_BALANCE_SHEET b
    JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES s ON s.FS_LINE = b.FS_LINE
    GROUP BY 1, 2, 3, 4, 7, 8
),
ext AS (
    SELECT b.PERIOD_MONTH, b.ENTITY_ID, b.FUNCTIONAL_CCY,
           SUM(IFF(b.SECTION = 'ASSETS', b.AMOUNT_LCY * COALESCE(b.ANNUAL_RATE, 0), 0)) / 12 AS II,
           -SUM(IFF(b.SECTION = 'LIABILITIES', b.AMOUNT_LCY * COALESCE(b.ANNUAL_RATE, 0), 0)) / 12 AS IE,
           SUM(IFF(b.FS_LINE IN ('CLIENT_LOANS', 'SECURITIES'), b.AMOUNT_LCY, 0)) * g.FEE_BPS / 10000 / 12 AS FEE,
           SUM(IFF(b.SECTION = 'ASSETS' AND b.FS_LINE <> 'IC_RECEIVABLES', b.AMOUNT_CHF, 0)) AS EXT_ASSETS_CHF,
           g.COST_INCOME_RATIO, g.TAX_RATE
    FROM bs b JOIN gs g ON g.ENTITY_ID = b.ENTITY_ID
    GROUP BY 1, 2, 3, g.FEE_BPS, g.COST_INCOME_RATIO, g.TAX_RATE
),
-- IC flows are fixed in CHF first so both sides translate back to the same CHF amount;
-- interest uses the booked IC payable (each borrower has one pair)
ic_int AS (
    SELECT b.PERIOD_MONTH, p.LENDER_ID, p.BORROWER_ID, b.AMOUNT_CHF * p.ANNUAL_RATE / 12 AS AMOUNT_CHF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_IC_PAIRS p
    JOIN bs b ON b.ENTITY_ID = p.BORROWER_ID AND b.FS_LINE = 'IC_PAYABLES'
),
ic_svc AS (
    SELECT e.PERIOD_MONTH, 'LE_01' AS LENDER_ID, e.ENTITY_ID AS BORROWER_ID, e.EXT_ASSETS_CHF * 0.0004 / 12 AS AMOUNT_CHF
    FROM ext e WHERE e.ENTITY_ID <> 'LE_01'
),
ic AS (
    SELECT PERIOD_MONTH, LENDER_ID AS ENTITY_ID, 'IC_INTEREST' AS FS_LINE, AMOUNT_CHF FROM ic_int
    UNION ALL SELECT PERIOD_MONTH, BORROWER_ID, 'IC_INTEREST', -AMOUNT_CHF FROM ic_int
    UNION ALL SELECT PERIOD_MONTH, LENDER_ID, 'IC_SERVICE_FEE', AMOUNT_CHF FROM ic_svc
    UNION ALL SELECT PERIOD_MONTH, BORROWER_ID, 'IC_SERVICE_FEE', -AMOUNT_CHF FROM ic_svc
),
ic_lcy AS (
    SELECT i.PERIOD_MONTH, i.ENTITY_ID, i.FS_LINE, SUM(i.AMOUNT_CHF) / MAX(f.AVERAGE_RATE) AS AMOUNT_LCY
    FROM ic i
    JOIN gs g ON g.ENTITY_ID = i.ENTITY_ID
    JOIN fx f ON f.PERIOD_MONTH = i.PERIOD_MONTH AND f.CURRENCY = g.FUNCTIONAL_CCY
    GROUP BY 1, 2, 3
),
pre AS (
    SELECT e.*,
           COALESCE(ii.AMOUNT_LCY, 0) AS IC_INT,
           COALESCE(sv.AMOUNT_LCY, 0) AS IC_SVC,
           GREATEST(e.II + e.IE + e.FEE, 0) * e.COST_INCOME_RATIO AS OPEX
    FROM ext e
    LEFT JOIN ic_lcy ii ON ii.PERIOD_MONTH = e.PERIOD_MONTH AND ii.ENTITY_ID = e.ENTITY_ID AND ii.FS_LINE = 'IC_INTEREST'
    LEFT JOIN ic_lcy sv ON sv.PERIOD_MONTH = e.PERIOD_MONTH AND sv.ENTITY_ID = e.ENTITY_ID AND sv.FS_LINE = 'IC_SERVICE_FEE'
),
pl AS (
    SELECT PERIOD_MONTH, ENTITY_ID, FUNCTIONAL_CCY, 'INTEREST_INCOME' AS FS_LINE, II AS AMOUNT_LCY,
           'Closing asset balances x annual rate / 12' AS DRIVER FROM pre
    UNION ALL SELECT PERIOD_MONTH, ENTITY_ID, FUNCTIONAL_CCY, 'INTEREST_EXPENSE', IE,
           'Closing liability balances x annual rate / 12' FROM pre
    UNION ALL SELECT PERIOD_MONTH, ENTITY_ID, FUNCTIONAL_CCY, 'IC_INTEREST', IC_INT,
           'IC pair amount x IC rate / 12 (eliminated in the group)' FROM pre
    UNION ALL SELECT PERIOD_MONTH, ENTITY_ID, FUNCTIONAL_CCY, 'FEE_INCOME', FEE,
           'Loans and securities x entity fee bps / 12' FROM pre
    UNION ALL SELECT PERIOD_MONTH, ENTITY_ID, FUNCTIONAL_CCY, 'IC_SERVICE_FEE', IC_SVC,
           'Holding charges 4 bp p.a. of subsidiary assets (eliminated in the group)' FROM pre
    UNION ALL SELECT PERIOD_MONTH, ENTITY_ID, FUNCTIONAL_CCY, 'PERSONNEL', -0.6 * OPEX,
           '60 % of cost-income ratio x operating income' FROM pre
    UNION ALL SELECT PERIOD_MONTH, ENTITY_ID, FUNCTIONAL_CCY, 'OTHER_OPEX', -0.4 * OPEX,
           '40 % of cost-income ratio x operating income' FROM pre
    UNION ALL SELECT PERIOD_MONTH, ENTITY_ID, FUNCTIONAL_CCY, 'TAX', -TAX_RATE * GREATEST(II + IE + FEE + IC_INT + IC_SVC - OPEX, 0),
           'Tax rate x positive profit before tax' FROM pre
)
SELECT p.PERIOD_MONTH, p.ENTITY_ID, p.FS_LINE::VARCHAR(30), p.FUNCTIONAL_CCY, p.AMOUNT_LCY::NUMBER(18,2),
       f.AVERAGE_RATE, (p.AMOUNT_LCY * f.AVERAGE_RATE)::NUMBER(18,2), p.DRIVER::VARCHAR(100)
FROM pl p
JOIN fx f ON f.PERIOD_MONTH = p.PERIOD_MONTH AND f.CURRENCY = p.FUNCTIONAL_CCY;

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FX_TRANSLATION(
    PERIOD_MONTH COMMENT 'Reporting month (first day of the month).',
    PERIOD_DATE COMMENT 'Reporting date.',
    ENTITY_ID COMMENT 'Legal entity.',
    FUNCTIONAL_CCY COMMENT 'Functional currency of the entity.',
    CLOSING_RATE COMMENT 'Closing rate (assets, liabilities).',
    AVERAGE_RATE COMMENT 'Average rate of the month (P&L, reserves).',
    HISTORICAL_RATE COMMENT 'Historical rate (share capital).',
    TOTAL_ASSETS_LCY COMMENT 'Total assets in functional currency.',
    TOTAL_ASSETS_CHF COMMENT 'Total assets in CHF at the closing rate.',
    NET_ASSETS_LCY COMMENT 'Equity (net assets) in functional currency.',
    NET_ASSETS_CHF COMMENT 'Net assets in CHF at the closing rate.',
    SHARE_CAPITAL_CHF COMMENT 'Share capital in CHF at the historical rate.',
    RESERVES_CHF COMMENT 'Reserves in CHF at the average rate.',
    CTA_CHF COMMENT 'Currency translation reserve: net assets at closing rate minus capital at historical and reserves at average rate.',
    CTA_MOVEMENT_CHF COMMENT 'Change of the CTA against the previous month.',
    NET_PROFIT_LCY COMMENT 'Monthly net profit in functional currency.',
    NET_PROFIT_CHF COMMENT 'Monthly net profit in CHF at the average rate.'
) COMMENT = 'FX translation of every entity into the CHF group currency (closing rate for the balance sheet, average rate for the P&L and reserves, historical rate for share capital) with the resulting currency translation reserve (CTA). CHF entities show rate 1 and CTA 0.'
AS
WITH bs AS (
    SELECT b.PERIOD_MONTH, b.PERIOD_DATE, b.ENTITY_ID, b.FUNCTIONAL_CCY,
           SUM(IFF(s.SECTION = 'ASSETS', b.AMOUNT_LCY, 0)) AS TOTAL_ASSETS_LCY,
           SUM(IFF(s.SECTION = 'ASSETS', b.AMOUNT_CHF, 0)) AS TOTAL_ASSETS_CHF,
           SUM(IFF(s.SECTION = 'EQUITY', b.AMOUNT_LCY, 0)) AS NET_ASSETS_LCY,
           SUM(IFF(s.SECTION = 'EQUITY', b.AMOUNT_CHF, 0)) AS NET_ASSETS_CHF,
           SUM(IFF(b.FS_LINE = 'SHARE_CAPITAL', b.AMOUNT_CHF, 0)) AS SHARE_CAPITAL_CHF,
           SUM(IFF(b.FS_LINE = 'RESERVES', b.AMOUNT_CHF, 0)) AS RESERVES_CHF,
           SUM(IFF(b.FS_LINE = 'CTA', b.AMOUNT_CHF, 0)) AS CTA_CHF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_BALANCE_SHEET b
    JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES s ON s.FS_LINE = b.FS_LINE
    GROUP BY 1, 2, 3, 4
),
pl AS (
    SELECT PERIOD_MONTH, ENTITY_ID, SUM(AMOUNT_LCY) AS NET_PROFIT_LCY, SUM(AMOUNT_CHF) AS NET_PROFIT_CHF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_PNL
    GROUP BY 1, 2
)
SELECT bs.PERIOD_MONTH, bs.PERIOD_DATE, bs.ENTITY_ID, bs.FUNCTIONAL_CCY, f.CLOSING_RATE, f.AVERAGE_RATE, f.HISTORICAL_RATE,
       bs.TOTAL_ASSETS_LCY::NUMBER(18,2), bs.TOTAL_ASSETS_CHF::NUMBER(18,2), bs.NET_ASSETS_LCY::NUMBER(18,2),
       bs.NET_ASSETS_CHF::NUMBER(18,2), bs.SHARE_CAPITAL_CHF::NUMBER(18,2), bs.RESERVES_CHF::NUMBER(18,2), bs.CTA_CHF::NUMBER(18,2),
       (bs.CTA_CHF - LAG(bs.CTA_CHF) OVER (PARTITION BY bs.ENTITY_ID ORDER BY bs.PERIOD_MONTH))::NUMBER(18,2),
       pl.NET_PROFIT_LCY::NUMBER(18,2), pl.NET_PROFIT_CHF::NUMBER(18,2)
FROM bs
JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_FX_PERIODS f ON f.PERIOD_MONTH = bs.PERIOD_MONTH AND f.CURRENCY = bs.FUNCTIONAL_CCY
LEFT JOIN pl ON pl.PERIOD_MONTH = bs.PERIOD_MONTH AND pl.ENTITY_ID = bs.ENTITY_ID;

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_IC_RECONCILIATION(
    PERIOD_MONTH COMMENT 'Reporting month (first day of the month).',
    PAIR_ID COMMENT 'Intercompany pair.',
    IC_TYPE COMMENT 'IC_LOAN or IC_DEPOSIT.',
    DESCRIPTION COMMENT 'Business description of the pair.',
    LENDER_ID COMMENT 'Side A entity (receivable).',
    BORROWER_ID COMMENT 'Side B entity (payable).',
    PAIR_CCY COMMENT 'Currency of the pair (borrower functional currency).',
    AMOUNT_PAIR_CCY COMMENT 'Pair amount in pair currency.',
    SIDE_A_CHF COMMENT 'Receivable of side A in CHF at the closing rate.',
    SIDE_B_CHF COMMENT 'Payable of side B in CHF at the closing rate.',
    DIFFERENCE_CHF COMMENT 'Side A minus side B (0 when both sides are booked at the closing rate).',
    IS_DEMO_EXCEPTION COMMENT 'TRUE for the pair used by the opt-in demo exception.',
    DEMO_SIDE_B_CHF COMMENT 'Side B in CHF if the borrower kept the pair at the previous month-end rate (not revalued).',
    DEMO_DIFFERENCE_CHF COMMENT 'Side A minus DEMO_SIDE_B_CHF for the demo pair, else 0.',
    RESOLUTION_HINT COMMENT 'How Finance resolves a difference on this pair.'
) COMMENT = 'Intercompany reconciliation of the consolidation showcase: each pair side A (receivable) vs side B (payable) in CHF. The demo columns show the exception the app can switch on: the borrower books the USD loan at last month''s rate, so the pair does not net to zero.'
AS
SELECT b.PERIOD_MONTH, p.PAIR_ID, p.IC_TYPE, p.DESCRIPTION, p.LENDER_ID, p.BORROWER_ID, b.BASIS_CCY AS PAIR_CCY,
       b.AMOUNT_BASIS::NUMBER(38,12) AS AMOUNT_PAIR_CCY, b.AMOUNT_CHF::NUMBER(38,12) AS SIDE_A_CHF,
       b.AMOUNT_CHF::NUMBER(38,12) AS SIDE_B_CHF, 0::NUMBER(18,2) AS DIFFERENCE_CHF,
       p.IS_DEMO_EXCEPTION,
       IFF(p.IS_DEMO_EXCEPTION AND f.PREV_CLOSING_RATE IS NOT NULL, b.AMOUNT_BASIS * f.PREV_CLOSING_RATE, b.AMOUNT_CHF)::NUMBER(18,2),
       (b.AMOUNT_CHF - IFF(p.IS_DEMO_EXCEPTION AND f.PREV_CLOSING_RATE IS NOT NULL, b.AMOUNT_BASIS * f.PREV_CLOSING_RATE, b.AMOUNT_CHF))::NUMBER(18,2),
       IFF(p.IS_DEMO_EXCEPTION, 'Revalue the payable at the month-end closing rate in the borrower ledger and confirm the balance with the lender.',
           'Balances agree; no action.')::VARCHAR(200)
FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_IC_PAIRS p
JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_BALANCE_SHEET b ON b.ENTITY_ID = p.BORROWER_ID AND b.FS_LINE = 'IC_PAYABLES'
JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_FX_PERIODS f ON f.PERIOD_MONTH = b.PERIOD_MONTH AND f.CURRENCY = b.BASIS_CCY;

DEFINE DYNAMIC TABLE {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_GROUP_CONSOLIDATION(
    PERIOD_MONTH DATE COMMENT 'Reporting month (first day of the month).',
    PERIOD_DATE DATE COMMENT 'Reporting date.',
    STATEMENT VARCHAR(2) COMMENT 'BS or PL.',
    SECTION VARCHAR(12) COMMENT 'ASSETS, LIABILITIES, EQUITY or PNL.',
    FS_LINE VARCHAR(30) COMMENT 'Financial statement line.',
    LABEL VARCHAR(60) COMMENT 'Display label of the line.',
    SORT_ORDER INTEGER COMMENT 'Display order of the line.',
    COLUMN_TYPE VARCHAR(10) COMMENT 'ENTITY (translated entity value), IC_ELIM (intercompany elimination), NCI (non-controlling interest reclassification) or GROUP (consolidated value).',
    ENTITY_ID VARCHAR(10) COMMENT 'Originating entity of the row; GROUP for the consolidated rows.',
    AMOUNT_CHF NUMBER(18,2) COMMENT 'Amount in CHF. GROUP = sum of ENTITY + IC_ELIM + NCI.'
) COMMENT = 'Consolidation sheet in CHF: translated entity values, intercompany eliminations (receivables/payables, IC interest and service fees), NCI reclassification (equity share and profit share of the 80 % / 75 % owned entities) and the consolidated group value per FS line and month.'
TARGET_LAG = '{{ lag }}' WAREHOUSE = {{ wh }}
REFRESH_MODE = FULL
AS
WITH gs AS (SELECT * FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_GROUP_STRUCTURE),
ent AS (
    SELECT PERIOD_MONTH, ENTITY_ID, FS_LINE, SUM(AMOUNT_CHF) AS AMOUNT_CHF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_BALANCE_SHEET GROUP BY 1, 2, 3
    UNION ALL
    SELECT PERIOD_MONTH, ENTITY_ID, FS_LINE, SUM(AMOUNT_CHF)
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_PNL GROUP BY 1, 2, 3
),
rows_all AS (
    SELECT e.PERIOD_MONTH, e.ENTITY_ID, e.FS_LINE, 'ENTITY' AS COLUMN_TYPE, e.AMOUNT_CHF FROM ent e
    UNION ALL
    SELECT e.PERIOD_MONTH, e.ENTITY_ID, e.FS_LINE, 'IC_ELIM', -e.AMOUNT_CHF
    FROM ent e JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES s ON s.FS_LINE = e.FS_LINE
    WHERE s.IS_INTERCOMPANY
    UNION ALL
    -- NCI: the minority share of equity moves to the NCI line
    SELECT e.PERIOD_MONTH, e.ENTITY_ID, e.FS_LINE, 'NCI', -(1 - g.OWNERSHIP_PCT) * e.AMOUNT_CHF
    FROM ent e JOIN gs g ON g.ENTITY_ID = e.ENTITY_ID
    WHERE g.OWNERSHIP_PCT < 1 AND e.FS_LINE IN ('SHARE_CAPITAL', 'RESERVES', 'CTA')
    UNION ALL
    SELECT e.PERIOD_MONTH, e.ENTITY_ID, 'NCI', 'NCI', SUM((1 - g.OWNERSHIP_PCT) * e.AMOUNT_CHF)
    FROM ent e JOIN gs g ON g.ENTITY_ID = e.ENTITY_ID
    WHERE g.OWNERSHIP_PCT < 1 AND e.FS_LINE IN ('SHARE_CAPITAL', 'RESERVES', 'CTA')
    GROUP BY 1, 2
    UNION ALL
    -- NCI: the minority share of net profit is shown as a deduction
    SELECT e.PERIOD_MONTH, e.ENTITY_ID, 'NCI_SHARE', 'NCI', -SUM((1 - g.OWNERSHIP_PCT) * e.AMOUNT_CHF)
    FROM ent e
    JOIN gs g ON g.ENTITY_ID = e.ENTITY_ID
    JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES s ON s.FS_LINE = e.FS_LINE
    WHERE g.OWNERSHIP_PCT < 1 AND s.STATEMENT = 'PL'
    GROUP BY 1, 2
),
withgroup AS (
    SELECT PERIOD_MONTH, ENTITY_ID, FS_LINE, COLUMN_TYPE, AMOUNT_CHF FROM rows_all
    UNION ALL
    SELECT PERIOD_MONTH, 'GROUP', FS_LINE, 'GROUP', SUM(AMOUNT_CHF) FROM rows_all GROUP BY 1, 3
)
SELECT w.PERIOD_MONTH, f.PERIOD_DATE, s.STATEMENT, s.SECTION, w.FS_LINE, s.LABEL, s.SORT_ORDER,
       w.COLUMN_TYPE::VARCHAR(10), w.ENTITY_ID::VARCHAR(10), w.AMOUNT_CHF::NUMBER(18,2)
FROM withgroup w
JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES s ON s.FS_LINE = w.FS_LINE
JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_FX_PERIODS f ON f.PERIOD_MONTH = w.PERIOD_MONTH AND f.CURRENCY = 'CHF';

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_CONSOLIDATION_BRIDGE(
    PERIOD_MONTH COMMENT 'Reporting month (first day of the month).',
    PREV_PERIOD_MONTH COMMENT 'Comparison month.',
    METRIC COMMENT 'TOTAL_ASSETS (group balance sheet total) or NET_PROFIT (monthly profit attributable to the shareholders of the group).',
    COMPONENT COMMENT 'OPENING, ENTITY_PERFORMANCE (change at constant FX rates), FX_EFFECT (rate change on the current balances / profit), IC_ELIMINATION, NCI or CLOSING.',
    ENTITY_ID COMMENT 'Entity for ENTITY_PERFORMANCE and FX_EFFECT; GROUP for the other components.',
    SORT_ORDER COMMENT 'Display order of the component.',
    AMOUNT_CHF COMMENT 'Amount in CHF; OPENING + all movements = CLOSING.'
) COMMENT = 'Month-over-month bridge of group total assets and group net profit: what came from entity performance (at constant rates), from FX and from intercompany eliminations and NCI. Answers "what drove the change versus last month".'
AS
WITH fx AS (SELECT * FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_FX_PERIODS),
-- total assets per entity, with the FX effect of the rate change on the current balances (per holding currency)
ta AS (
    SELECT b.PERIOD_MONTH, b.ENTITY_ID,
           SUM(b.AMOUNT_CHF) AS CHF,
           SUM(b.AMOUNT_BASIS * (f.CLOSING_RATE - COALESCE(f.PREV_CLOSING_RATE, f.CLOSING_RATE))) AS FX,
           SUM(IFF(b.FS_LINE = 'IC_RECEIVABLES', b.AMOUNT_CHF, 0)) AS IC
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_BALANCE_SHEET b
    JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES s ON s.FS_LINE = b.FS_LINE AND s.SECTION = 'ASSETS'
    JOIN fx f ON f.PERIOD_MONTH = b.PERIOD_MONTH AND f.CURRENCY = b.BASIS_CCY
    GROUP BY 1, 2
),
np AS (
    SELECT p.PERIOD_MONTH, p.ENTITY_ID,
           SUM(p.AMOUNT_CHF) AS CHF,
           SUM(p.AMOUNT_LCY) * (MAX(f.AVERAGE_RATE) - COALESCE(MAX(f.PREV_AVERAGE_RATE), MAX(f.AVERAGE_RATE))) AS FX,
           SUM(IFF(p.FS_LINE IN ('IC_INTEREST', 'IC_SERVICE_FEE'), p.AMOUNT_CHF, 0)) AS IC
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_PNL p
    JOIN fx f ON f.PERIOD_MONTH = p.PERIOD_MONTH AND f.CURRENCY = p.FUNCTIONAL_CCY
    GROUP BY 1, 2
),
nci AS (
    SELECT PERIOD_MONTH, SUM(AMOUNT_CHF) AS NCI
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_GROUP_CONSOLIDATION
    WHERE COLUMN_TYPE = 'NCI' AND FS_LINE = 'NCI_SHARE'
    GROUP BY 1
),
ent AS (
    SELECT 'TOTAL_ASSETS' AS METRIC, PERIOD_MONTH, ENTITY_ID, CHF, FX, IC, 0 AS NCI FROM ta
    UNION ALL
    SELECT 'NET_PROFIT', n.PERIOD_MONTH, n.ENTITY_ID, n.CHF, n.FX, n.IC, 0 FROM np n
),
ent2 AS (
    SELECT e.*, p.PREV_PERIOD_MONTH,
           LAG(e.CHF) OVER (PARTITION BY e.METRIC, e.ENTITY_ID ORDER BY e.PERIOD_MONTH) AS PREV_CHF,
           LAG(e.IC) OVER (PARTITION BY e.METRIC, e.ENTITY_ID ORDER BY e.PERIOD_MONTH) AS PREV_IC
    FROM ent e
    JOIN fx p ON p.PERIOD_MONTH = e.PERIOD_MONTH AND p.CURRENCY = 'CHF'
),
grp AS (
    SELECT g.METRIC, g.PERIOD_MONTH, MAX(g.PREV_PERIOD_MONTH) AS PREV_PERIOD_MONTH,
           SUM(g.CHF) - SUM(g.IC) + IFF(g.METRIC = 'NET_PROFIT', COALESCE(MAX(n.NCI), 0), 0) AS CLOSING,
           SUM(g.IC) AS IC,
           IFF(g.METRIC = 'NET_PROFIT', COALESCE(MAX(n.NCI), 0), 0) AS NCI
    FROM ent2 g
    LEFT JOIN nci n ON n.PERIOD_MONTH = g.PERIOD_MONTH
    GROUP BY 1, 2
),
grp2 AS (
    SELECT g.*, LAG(CLOSING) OVER (PARTITION BY METRIC ORDER BY PERIOD_MONTH) AS OPENING,
           LAG(IC) OVER (PARTITION BY METRIC ORDER BY PERIOD_MONTH) AS PREV_IC,
           LAG(NCI) OVER (PARTITION BY METRIC ORDER BY PERIOD_MONTH) AS PREV_NCI
    FROM grp g
),
out AS (
    SELECT PERIOD_MONTH, PREV_PERIOD_MONTH, METRIC, 'OPENING' AS COMPONENT, 'GROUP' AS ENTITY_ID, 1 AS SORT_ORDER, OPENING AS AMOUNT_CHF
    FROM grp2 WHERE OPENING IS NOT NULL
    UNION ALL
    SELECT PERIOD_MONTH, PREV_PERIOD_MONTH, METRIC, 'ENTITY_PERFORMANCE', ENTITY_ID, 2, (CHF - PREV_CHF) - FX
    FROM ent2 WHERE PREV_CHF IS NOT NULL
    UNION ALL
    SELECT PERIOD_MONTH, PREV_PERIOD_MONTH, METRIC, 'FX_EFFECT', ENTITY_ID, 3, FX
    FROM ent2 WHERE PREV_CHF IS NOT NULL
    UNION ALL
    SELECT PERIOD_MONTH, PREV_PERIOD_MONTH, METRIC, 'IC_ELIMINATION', 'GROUP', 4, -(IC - PREV_IC)
    FROM grp2 WHERE OPENING IS NOT NULL
    UNION ALL
    SELECT PERIOD_MONTH, PREV_PERIOD_MONTH, METRIC, 'NCI', 'GROUP', 5, NCI - PREV_NCI
    FROM grp2 WHERE OPENING IS NOT NULL AND METRIC = 'NET_PROFIT'
    UNION ALL
    SELECT PERIOD_MONTH, PREV_PERIOD_MONTH, METRIC, 'CLOSING', 'GROUP', 6, CLOSING
    FROM grp2 WHERE OPENING IS NOT NULL
)
SELECT PERIOD_MONTH, PREV_PERIOD_MONTH, METRIC::VARCHAR(20), COMPONENT::VARCHAR(20), ENTITY_ID::VARCHAR(10), SORT_ORDER::INTEGER,
       AMOUNT_CHF::NUMBER(18,2)
FROM out;

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_CONSOLIDATION_TRAIL(
    PERIOD_MONTH COMMENT 'Reporting month (first day of the month).',
    STATEMENT COMMENT 'BS or PL.',
    FS_LINE COMMENT 'Financial statement line.',
    LABEL COMMENT 'Display label of the line.',
    ENTITY_ID COMMENT 'Legal entity.',
    DISPLAY_NAME COMMENT 'Fictional entity name.',
    SOURCE_POSITIONS COMMENT 'Number of source positions in the liquidity book behind the line (0 for derived lines).',
    SOURCE_PRODUCTS COMMENT 'Source product types mapped to the line, or the derivation rule for derived lines.',
    HOLDING_CCYS COMMENT 'Currencies the underlying amounts are held in.',
    FUNCTIONAL_CCY COMMENT 'Functional currency of the entity.',
    AMOUNT_LCY COMMENT 'Entity amount in functional currency.',
    RATE_TYPE COMMENT 'Translation rate type: CLOSING, AVERAGE, HISTORICAL or RESIDUAL.',
    TRANSLATION_RATE COMMENT 'Functional currency -> CHF rate applied.',
    ENTITY_CHF COMMENT 'Entity amount in CHF.',
    IC_ELIM_CHF COMMENT 'Intercompany elimination of the entity amount.',
    NCI_CHF COMMENT 'NCI reclassification of the entity amount.',
    GROUP_CONTRIBUTION_CHF COMMENT 'Contribution of the entity to the group value (ENTITY + IC_ELIM + NCI).'
) COMMENT = 'Answer trail of the consolidation showcase: for every group figure, the chain from the source positions and mapping rule over the functional currency amount, the FX rate and the elimination / NCI step to the contribution to the group value.'
AS
WITH c AS (
    SELECT PERIOD_MONTH, STATEMENT, FS_LINE, LABEL, ENTITY_ID,
           SUM(IFF(COLUMN_TYPE = 'ENTITY', AMOUNT_CHF, 0)) AS ENTITY_CHF,
           SUM(IFF(COLUMN_TYPE = 'IC_ELIM', AMOUNT_CHF, 0)) AS IC_ELIM_CHF,
           SUM(IFF(COLUMN_TYPE = 'NCI', AMOUNT_CHF, 0)) AS NCI_CHF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_GROUP_CONSOLIDATION
    WHERE COLUMN_TYPE <> 'GROUP'
    GROUP BY 1, 2, 3, 4, 5
),
bs AS (
    SELECT PERIOD_MONTH, ENTITY_ID, FS_LINE, FUNCTIONAL_CCY, SUM(AMOUNT_LCY) AS AMOUNT_LCY, MAX(RATE_TYPE) AS RATE_TYPE,
           MAX(TRANSLATION_RATE) AS TRANSLATION_RATE, LISTAGG(DISTINCT BASIS_CCY, ', ') WITHIN GROUP (ORDER BY BASIS_CCY) AS HOLDING_CCYS
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_BALANCE_SHEET
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT PERIOD_MONTH, ENTITY_ID, FS_LINE, FUNCTIONAL_CCY, SUM(AMOUNT_LCY), 'AVERAGE', MAX(AVERAGE_RATE), FUNCTIONAL_CCY
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_PNL
    GROUP BY 1, 2, 3, 4
),
src AS (
    SELECT PERIOD_MONTH, ENTITY_ID, FS_LINE, SUM(POSITION_COUNT) AS SOURCE_POSITIONS,
           LISTAGG(DISTINCT PRODUCT_TYPE, ', ') WITHIN GROUP (ORDER BY PRODUCT_TYPE) AS SOURCE_PRODUCTS
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_POSITIONS
    GROUP BY 1, 2, 3
),
drv AS (
    SELECT PERIOD_MONTH, ENTITY_ID, FS_LINE, MAX(DRIVER) AS DRIVER
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_PNL
    GROUP BY 1, 2, 3
)
SELECT c.PERIOD_MONTH, c.STATEMENT, c.FS_LINE, c.LABEL, c.ENTITY_ID, g.DISPLAY_NAME,
       COALESCE(s.SOURCE_POSITIONS, 0)::NUMBER(18,0),
       COALESCE(s.SOURCE_PRODUCTS, d.DRIVER,
           CASE c.FS_LINE
               WHEN 'IC_RECEIVABLES' THEN 'Intercompany pairs: agreed share of the borrower''s external assets'
               WHEN 'IC_PAYABLES' THEN 'Intercompany pairs: agreed share of the borrower''s external assets'
               WHEN 'OTHER_LIABILITIES' THEN 'Funding residual: assets - liabilities - equity'
               WHEN 'OTHER_ASSETS' THEN 'Funding residual: assets - liabilities - equity'
               WHEN 'SHARE_CAPITAL' THEN '80 % of the equity (equity ratio x assets) of the first reporting month'
               WHEN 'RESERVES' THEN 'Equity change since the first reporting month'
               WHEN 'CTA' THEN 'Net assets at closing rate - capital at historical rate - reserves at average rate'
               WHEN 'NCI' THEN 'Minority share of equity (1 - ownership)'
               WHEN 'NCI_SHARE' THEN 'Minority share of net profit (1 - ownership)'
           END)::VARCHAR(500),
       b.HOLDING_CCYS::VARCHAR(50), g.FUNCTIONAL_CCY, b.AMOUNT_LCY::NUMBER(18,2), b.RATE_TYPE::VARCHAR(12), b.TRANSLATION_RATE::NUMBER(18,8),
       c.ENTITY_CHF::NUMBER(18,2), c.IC_ELIM_CHF::NUMBER(18,2), c.NCI_CHF::NUMBER(18,2),
       (c.ENTITY_CHF + c.IC_ELIM_CHF + c.NCI_CHF)::NUMBER(18,2)
FROM c
JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_GROUP_STRUCTURE g ON g.ENTITY_ID = c.ENTITY_ID
LEFT JOIN bs b ON b.PERIOD_MONTH = c.PERIOD_MONTH AND b.ENTITY_ID = c.ENTITY_ID AND b.FS_LINE = c.FS_LINE
LEFT JOIN src s ON s.PERIOD_MONTH = c.PERIOD_MONTH AND s.ENTITY_ID = c.ENTITY_ID AND s.FS_LINE = c.FS_LINE
LEFT JOIN drv d ON d.PERIOD_MONTH = c.PERIOD_MONTH AND d.ENTITY_ID = c.ENTITY_ID AND d.FS_LINE = c.FS_LINE;

DEFINE VIEW {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_CONSOLIDATION_CHECKS(
    PERIOD_MONTH COMMENT 'Reporting month (first day of the month).',
    CHECK_ID COMMENT 'Control identifier (FC-C1..FC-C5).',
    CHECK_NAME COMMENT 'Control name.',
    DESCRIPTION COMMENT 'What the control proves.',
    TOLERANCE COMMENT 'Tolerance of the control (CHF, or hours for freshness).',
    DIFFERENCE COMMENT 'Measured difference (CHF, or hours since the last refresh for freshness).',
    STATUS COMMENT 'PASS or FAIL.',
    AFFECTED COMMENT 'Objects behind a failure (entities, pairs, products).',
    RESOLUTION_HINT COMMENT 'How Finance resolves a failure.',
    DEMO_DIFFERENCE COMMENT 'Difference with the opt-in demo exception switched on.',
    DEMO_STATUS COMMENT 'Status with the demo exception switched on.',
    DEMO_AFFECTED COMMENT 'Affected objects with the demo exception switched on.'
) COMMENT = 'Consolidation controls per month: entity and group balance check, intercompany net-to-zero, CTA reconciliation, source completeness against the liquidity book and data freshness. The DEMO_ columns show the same controls with the IC demo exception switched on.'
AS
WITH gs AS (SELECT ENTITY_ID, DISPLAY_NAME FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_GROUP_STRUCTURE),
bs AS (
    SELECT b.PERIOD_MONTH, b.ENTITY_ID,
           SUM(IFF(s.SECTION = 'ASSETS', b.AMOUNT_CHF, 0)) AS A,
           SUM(IFF(s.SECTION = 'LIABILITIES', b.AMOUNT_CHF, 0)) AS L,
           SUM(IFF(s.SECTION = 'EQUITY', b.AMOUNT_CHF, 0)) AS E,
           SUM(IFF(b.FS_LINE IN ('SHARE_CAPITAL', 'RESERVES'), b.AMOUNT_CHF, 0)) AS CAP_RES,
           SUM(IFF(b.FS_LINE = 'CTA', b.AMOUNT_CHF, 0)) AS CTA
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_BALANCE_SHEET b
    JOIN {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_FS_LINES s ON s.FS_LINE = b.FS_LINE
    GROUP BY 1, 2
),
grp AS (
    SELECT PERIOD_MONTH,
           SUM(IFF(SECTION = 'ASSETS', AMOUNT_CHF, 0)) - SUM(IFF(SECTION IN ('LIABILITIES', 'EQUITY'), AMOUNT_CHF, 0)) AS DIFF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_GROUP_CONSOLIDATION
    WHERE COLUMN_TYPE = 'GROUP' AND STATEMENT = 'BS'
    GROUP BY 1
),
c1 AS (
    SELECT b.PERIOD_MONTH, GREATEST(MAX(ABS(b.A - b.L - b.E)), MAX(ABS(g.DIFF))) AS DIFF,
           LISTAGG(IFF(ABS(b.A - b.L - b.E) > 1, n.DISPLAY_NAME, NULL), ', ') AS AFFECTED
    FROM bs b JOIN grp g ON g.PERIOD_MONTH = b.PERIOD_MONTH JOIN gs n ON n.ENTITY_ID = b.ENTITY_ID
    GROUP BY 1
),
ic AS (
    SELECT PERIOD_MONTH,
           ABS(SUM(IFF(FS_LINE = 'IC_RECEIVABLES', AMOUNT_CHF, 0)) - SUM(IFF(FS_LINE = 'IC_PAYABLES', AMOUNT_CHF, 0)))
             + ABS(SUM(IFF(FS_LINE IN ('IC_INTEREST', 'IC_SERVICE_FEE'), AMOUNT_CHF, 0))) AS DIFF
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_GROUP_CONSOLIDATION
    WHERE COLUMN_TYPE = 'ENTITY'
    GROUP BY 1
),
icd AS (
    SELECT r.PERIOD_MONTH, SUM(ABS(r.DEMO_DIFFERENCE_CHF)) AS DEMO_DIFF,
           LISTAGG(IFF(ABS(r.DEMO_DIFFERENCE_CHF) > 1, r.PAIR_ID || ' ' || a.DISPLAY_NAME || ' / ' || b.DISPLAY_NAME, NULL), ', ') AS DEMO_AFFECTED
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_VW_IC_RECONCILIATION r
    JOIN gs a ON a.ENTITY_ID = r.LENDER_ID
    JOIN gs b ON b.ENTITY_ID = r.BORROWER_ID
    GROUP BY 1
),
c3 AS (
    SELECT b.PERIOD_MONTH, SUM(ABS((b.A - b.L) - b.CAP_RES - b.CTA)) AS DIFF,
           LISTAGG(IFF(ABS((b.A - b.L) - b.CAP_RES - b.CTA) > 1, n.DISPLAY_NAME, NULL), ', ') AS AFFECTED
    FROM bs b JOIN gs n ON n.ENTITY_ID = b.ENTITY_ID
    GROUP BY 1
),
srcd AS (
    SELECT p.PERIOD_MONTH, SUM(x.AMOUNT_CHF) AS CHF
    FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS x
    JOIN (SELECT DISTINCT PERIOD_MONTH, PERIOD_DATE FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_FX_PERIODS) p ON p.PERIOD_DATE = x.AS_OF_DATE
    WHERE x.DQ_STATUS = 'OK'
    GROUP BY 1
),
dtd AS (
    SELECT PERIOD_MONTH, SUM(AMOUNT_CHF) AS CHF, SUM(IFF(IS_MAPPED, 0, AMOUNT_CHF)) AS UNMAPPED,
           LISTAGG(DISTINCT IFF(IS_MAPPED, NULL, PRODUCT_TYPE), ', ') AS AFFECTED,
           MIN(CALCULATION_TIMESTAMP) AS REFRESHED
    FROM {{ db }}.{{ rep_agg }}.FCOA_AGG_DT_ENTITY_POSITIONS
    GROUP BY 1
),
c4 AS (
    SELECT d.PERIOD_MONTH, ABS(COALESCE(s.CHF, 0) - d.CHF) + ABS(d.UNMAPPED) AS DIFF, d.AFFECTED
    FROM dtd d LEFT JOIN srcd s ON s.PERIOD_MONTH = d.PERIOD_MONTH
),
-- freshness = lag of the consolidation behind its source (not wall-clock age): a quiet source is not stale
src_ts AS (
    SELECT MAX(CALCULATION_TIMESTAMP) AS SRC_REFRESHED FROM {{ db }}.{{ rep_agg }}.REPP_AGG_DT_LIQ_POSITIONS
),
c5 AS (
    SELECT d.PERIOD_MONTH, GREATEST(DATEDIFF(minute, d.REFRESHED, s.SRC_REFRESHED), 0) / 60 AS DIFF
    FROM dtd d CROSS JOIN src_ts s
),
chk AS (
    SELECT c1.PERIOD_MONTH, 'FC-C1' AS CHECK_ID, 'Balance sheet balances' AS CHECK_NAME,
           'Assets = liabilities + equity for every entity and for the group.' AS DESCRIPTION,
           1 AS TOLERANCE, c1.DIFF, c1.AFFECTED,
           'Check the mapping of new product types and the funding residual of the affected entity.' AS HINT,
           c1.DIFF AS DEMO_DIFF, c1.AFFECTED AS DEMO_AFFECTED
    FROM c1
    UNION ALL
    SELECT ic.PERIOD_MONTH, 'FC-C2', 'Intercompany nets to zero',
           'IC receivables = IC payables and IC interest / service fees net to zero across all pairs.',
           1, ic.DIFF, NULL,
           'Match the open pair with the counterparty entity, revalue at the closing rate and rebook the difference.',
           ic.DIFF + COALESCE(icd.DEMO_DIFF, 0), icd.DEMO_AFFECTED
    FROM ic LEFT JOIN icd ON icd.PERIOD_MONTH = ic.PERIOD_MONTH
    UNION ALL
    SELECT PERIOD_MONTH, 'FC-C3', 'CTA reconciles',
           'Currency translation reserve = net assets at closing rate - capital at historical - reserves at average rate.',
           1, DIFF, AFFECTED, 'Check the FX rates of the period (closing, average, historical) of the affected entity.', DIFF, AFFECTED
    FROM c3
    UNION ALL
    SELECT PERIOD_MONTH, 'FC-C4', 'Source completeness',
           'Every valid liquidity book position is in the consolidation and mapped to a named FS line.',
           1, DIFF, AFFECTED, 'Add a mapping rule for the listed product types to the FS line mapping.', DIFF, AFFECTED
    FROM c4
    UNION ALL
    SELECT PERIOD_MONTH, 'FC-C5', 'Data freshness',
           'The consolidation is up to date with its source: at most 2 hours behind the latest liquidity book refresh.',
           2, DIFF, NULL, 'Check the refresh of the consolidation dynamic tables (target lag, warehouse).', DIFF, NULL
    FROM c5
)
SELECT PERIOD_MONTH, CHECK_ID::VARCHAR(10), CHECK_NAME::VARCHAR(60), DESCRIPTION::VARCHAR(200), TOLERANCE::NUMBER(18,2),
       DIFF::NUMBER(18,2), IFF(DIFF <= TOLERANCE, 'PASS', 'FAIL')::VARCHAR(4), NULLIF(AFFECTED, '')::VARCHAR(500),
       HINT::VARCHAR(200), DEMO_DIFF::NUMBER(18,2), IFF(DEMO_DIFF <= TOLERANCE, 'PASS', 'FAIL')::VARCHAR(4),
       NULLIF(DEMO_AFFECTED, '')::VARCHAR(500)
FROM chk;

-- Read access for the bank app (caller's rights, same pattern as 900_access.sql)
{% for t in ['FCOA_AGG_DT_FX_PERIODS', 'FCOA_AGG_DT_ENTITY_POSITIONS', 'FCOA_AGG_DT_ENTITY_BALANCE_SHEET', 'FCOA_AGG_DT_ENTITY_PNL',
             'FCOA_AGG_DT_GROUP_CONSOLIDATION'] %}
GRANT SELECT ON DYNAMIC TABLE {{ db }}.{{ rep_agg }}.{{ t }} TO ROLE PUBLIC;
{% endfor %}
{% for v in ['FCOA_AGG_VW_GROUP_STRUCTURE', 'FCOA_AGG_VW_FS_LINES', 'FCOA_AGG_VW_FS_LINE_MAPPING', 'FCOA_AGG_VW_IC_PAIRS',
             'FCOA_AGG_VW_FX_TRANSLATION', 'FCOA_AGG_VW_IC_RECONCILIATION', 'FCOA_AGG_VW_CONSOLIDATION_BRIDGE',
             'FCOA_AGG_VW_CONSOLIDATION_TRAIL', 'FCOA_AGG_VW_CONSOLIDATION_CHECKS'] %}
GRANT SELECT ON VIEW {{ db }}.{{ rep_agg }}.{{ v }} TO ROLE PUBLIC;
{% endfor %}
