# Demo Storyline: Group Consolidation for Financial Control

## Purpose

A 15–20 minute walkthrough of the **Group Consolidation** page of the `bank-app`
(`/finance/consolidation`) for a C-level Financial Control audience (CFO / COO,
group controlling, accounting, regulatory reporting).

It starts from a **finance decision**, not from the architecture:

> *What drove the change in group profit and assets versus last month, and how much
> came from entity performance, FX and intercompany eliminations?*

The key message: **one governed data foundation answers that question in minutes,
with every figure traceable to its source and every control visible.** The same
data later supports controlling self-service, unit economics and Treasury / NII
analysis.

> **Illustrative status.** All data is **synthetic** (generated, not anonymized
> customer data). Entity names, ownership shares, rates and cost assumptions are
> **fictional**. The page is a **management consolidation**, not an IFRS statement.
> The banner on the page and the note in every CSV export say the same; keep it
> visible on screen throughout the demo.

## Cast (personas)

| Persona | Role in the story |
| --- | --- |
| **CFO** | Asks the decision question, wants to trust the answer |
| **Head of Group Controlling** | Walks the bridge and the answer trail |
| **Head of Accounting / Consolidation** | Owns the controls and resolves exceptions |

## The fictional group (reporting currency CHF)

| Entity | Display name | Functional currency | Ownership |
| --- | --- | --- | --- |
| LE_01 | Group Holding CH (parent, treasury hub) | CHF | 100 % |
| LE_02 – LE_05 | Bank CH, Custody CH, Markets CH, Services CH | CHF | 100 % |
| LE_06 | Wealth CH | CHF | 80 % (NCI 20 %) |
| LE_07 | Bank EU (DE) | EUR | 100 % |
| LE_08 | Fund Services LU | EUR | 75 % (NCI 25 %) |
| LE_09 | Brokerage US (loss-making build-out) | USD | 100 % |

## Verified figures (October 2026, reporting date 2026-10-23)

| Figure | Value |
| --- | --- |
| Group total assets (after eliminations) | CHF 5,983.6 M (September CHF 6,134.8 M, −151.2 M) |
| of which entity performance at constant FX | −160.3 M (Group Holding CH −103.4 M, Custody CH −55.6 M, Markets CH +28.4 M) |
| of which FX effect | +9.3 M |
| of which change in IC eliminations | −0.2 M |
| IC eliminated | CHF 151.2 M (receivables = payables) |
| Group net profit attributable (month) | CHF 1.50 M (NCI share −0.03 M) |
| Currency translation reserve (CTA) | CHF −8.6 M (Brokerage US −3.6 M, Bank EU −3.1 M, Fund Services LU −2.5 M) |
| NCI in equity | CHF 9.3 M |
| Controls | 5 / 5 pass; with the demo exception 4 / 5 (FC-C2 fails by CHF 0.43 M on pair IC02) |

> Figures are indicative; always quote what the app shows on the day of the demo
> (the page and the gate read the same objects). Note that October is a partial
> month (data as of 23 October).

---

## Pre-demo checklist

1. Data is current: `sqlunit/fco_validation.sql` returns **FCO-01..07 PASS**
   (`snow sql -c <conn> -f sqlunit/fco_validation.sql`).
2. App runs: `HOST_PORT=8088 docker compose up -d --build` in `bank-app`, open
   `http://localhost:8088/finance/consolidation`, month **Oct 2026**.
3. "Simulate exception" on the Controls tab is **off** (reload resets it).
4. Agree the success measures with Finance beforehand (see the last section);
   do not quote savings in the demo.

---

## Act 1 – The decision question (Bridge tab, 4 min)

**Say:** "Last month's group close: assets went down by CHF 151 M. Is that a
business problem, an FX effect or just intercompany noise?"

**Show:**
- The header question and the KPI row: total assets **−151.2 M vs last month**,
  FX effect **+9.3 M**, controls **5 / 5**.
- The waterfall (*Total assets*): last month → entities **−160.3 M** → FX **+9.3 M**
  → IC elim. **−0.2 M** → this month. The sentence above the chart states it in words.
- *Contribution by entity*: the decrease sits in **Group Holding CH** and
  **Custody CH**; **Markets CH** grew. FX helped slightly.
- Switch to **Net profit**: profit is stable (CHF 1.48 M → 1.50 M). The loss-making
  entity (Brokerage US, cost-income ratio above 100 %) shows in Act 2 on the P&L sheet.

**Message:** the answer comes first, decomposed the way a controller explains it,
not as a data dump.

## Act 2 – Trust: the answer trail (Consolidation sheet, 5 min)

**Say:** "Can I trust that number? Where does it come from?"

**Show:**
- The sheet: nine entity columns, **Σ Entities, IC Elim, NCI, Group**. Total
  assets of the entities CHF 6,134.8 M, IC receivables eliminated to **0.0** in the
  group column, group CHF 5,983.6 M.
- Click **Group** on *Loans to clients*: the drawer lists every entity's
  contribution and adds up to the group value.
- Click **Brokerage US** on *Loans to clients*: the trail shows
  1. source positions and holding currencies,
  2. the mapping rule (product types → line),
  3. the USD amount,
  4. the closing rate USD/CHF and the CHF value,
  5. elimination, 6. NCI, 7. contribution to the group.
- Click **Brokerage US** on *Intercompany payables*: the derivation rule, the USD
  amount at the closing rate, and the full elimination against Group Holding CH.
- Toggle **Local currency**: the EUR and USD entities show their own currency;
  eliminations and the group stay in CHF. Toggle **P&L** for the monthly result and
  *Net profit attributable to shareholders*.
- **Export CSV**: the file carries the illustrative note.

**Message:** every group figure is explainable down to the source rows; no
spreadsheet in between.

## Act 3 – Controls and exceptions (Controls tab, 4 min)

**Say:** "Before sign-off, which controls ran, and what happens when one fails?"

**Show:**
- Five controls, all **PASS**:
  - **FC-C1** balance sheet balances (every entity and the group),
  - **FC-C2** intercompany nets to zero (balances, IC interest, service fees),
  - **FC-C3** CTA reconciles,
  - **FC-C4** source completeness (every valid source position is consolidated and
    mapped),
  - **FC-C5** data freshness.
- Click **Simulate exception**: Brokerage US kept its USD funding loan from
  Group Holding CH at **last month's rate** (not revalued). **FC-C2 turns red**:
  difference **CHF 0.43 M**, affected pair *IC02 Group Holding CH / Brokerage US*,
  resolution *revalue at the closing rate, confirm with the counterparty,
  rebook*. The KPI turns to *exception open*; the IC reconciliation table
  highlights the pair.
- Click **Reset exception**: back to 5 / 5.

**Message:** controls are part of the data product, run on every refresh, and
exceptions come with the affected objects and the resolution path.

## Act 4 – Accounting assumptions (Method tab, 3 min)

**Say:** "How exactly are the foreign entities translated?"

**Show** (October figures):
- *Brokerage US*: closing rate USD/CHF **0.6873** (balance sheet), average
  **0.6838** (P&L and reserves), historical **0.8222** (share capital). Total
  assets USD 454.9 M → CHF 312.7 M. Net assets CHF 28.1 M − capital CHF 22.2 M −
  reserves CHF 9.5 M = **CTA CHF −3.6 M**.
- *EUR cross rate*: rates arrive as USD quotes; EUR/CHF = USD/CHF ÷ USD/EUR
  (**0.7721** closing).
- *Ownership and NCI*: Wealth CH 80 %, Fund Services LU 75 %; 100 % consolidated,
  the minority share of equity shown as NCI, the minority share of profit
  deducted.
- *Where the numbers come from*: balances are the synthetic liquidity book
  positions (held in CHF, EUR and USD); functional-currency amounts are derived
  from them at the closing rate; the P&L is a management view derived from the
  balances and stated rate, fee and cost assumptions.

**Message:** the method is transparent and explainable to auditors; assumptions
are stated, not hidden.

## Act 5 – A reusable finance data foundation (2 min)

**Say:** "Consolidation is the first use. The same governed data feeds the next
questions."

- Controlling self-service: ask questions in natural language (Ask AI page) on
  governed data.
- Next showcases on the same foundation: **partner and product unit economics**
  (contribution margin per partner / asset class) and **Treasury / NII
  sensitivity** (link to the LCR and DtI pages of this app).
- *Speaker note:* position this against the customer's own finance-data initiatives
  only where the presenter has confirmed them; do not name internal programmes
  from public sources without checking.

## Success measures to agree with Finance (before claiming benefits)

Establish a baseline with the Finance team; the showcase illustrates the intended
improvement but does not prove a saving.

| Measure | Baseline to collect |
| --- | --- |
| Close / reporting effort | Person-days per monthly group close |
| Manual reconciliations | Number of spreadsheet reconciliations (IC, FX, mapping) |
| Exception resolution | Time from detection to resolution of an IC break |
| Time to answer | Time to answer "what drove the change vs last month?" |
| Control coverage | Controls automated vs. manual, evidence for audit |

---

## Appendix – technical detail (only if asked)

- **Objects** (DCM, `sources/definitions/580_FCOA_consolidation.sql`, schema `REP_AGG_V001`):
  - Inline mapping views (no new tables): `FCOA_AGG_VW_GROUP_STRUCTURE`, `FCOA_AGG_VW_FS_LINES`,
    `FCOA_AGG_VW_FS_LINE_MAPPING`, `FCOA_AGG_VW_IC_PAIRS`.
  - Dynamic tables: `FCOA_AGG_DT_FX_PERIODS`, `FCOA_AGG_DT_ENTITY_POSITIONS`,
    `FCOA_AGG_DT_ENTITY_BALANCE_SHEET`, `FCOA_AGG_DT_ENTITY_PNL`, `FCOA_AGG_DT_GROUP_CONSOLIDATION`.
  - Reporting views: `FCOA_AGG_VW_FX_TRANSLATION`, `FCOA_AGG_VW_IC_RECONCILIATION`,
    `FCOA_AGG_VW_CONSOLIDATION_BRIDGE`, `FCOA_AGG_VW_CONSOLIDATION_TRAIL`, `FCOA_AGG_VW_CONSOLIDATION_CHECKS`.
- **Sources:** `REPP_AGG_DT_LIQ_POSITIONS` (valid positions, last snapshot per month) and
  `REPP_AGG_DT_LCR_FX_DAILY` (USD quotes crossed via USD). No new data feeds, no changes to
  existing tables.
- **Modelling choices:** equity = equity ratio × assets per entity, the funding residual is
  shown as debt issued / other assets; share capital = 80 % of the first month's equity at the
  historical rate; reserves at the average rate; CTA as residual. IC pairs are agreed shares of
  the borrower's external assets, booked at the same closing rate on both sides; the demo
  exception values the borrower side at the previous month-end rate.
- **Tests:** `sqlunit/fco_validation.sql` (FCO-01..07, part of the `deploy.sh` gate),
  `sqlunit/tests.sqltest` (object counts), `bank-app/e2e/finance-consolidation.spec.ts`.
