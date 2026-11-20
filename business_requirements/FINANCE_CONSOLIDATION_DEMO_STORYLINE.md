# Demo Storyline: Group Consolidation for Financial Control

## Purpose

A 15–20 minute walkthrough of the **Group Consolidation** page of the `bank-app`
(`/finance/consolidation`) for a C-level Financial Control audience (CFO / COO,
group controlling, accounting, regulatory reporting).

It starts from a **finance decision**, not from the architecture:

> *What drove the change in group assets versus last month, and how much came from
> entity performance, FX and intercompany eliminations? And what makes up this
> month's profit?*

The key message: **one governed data foundation answers that question in minutes,
and every figure can be followed through this demo's calculation lineage**
(source positions → mapping → FX → elimination → group). The same data later
supports controlling self-service, unit economics and Treasury / NII analysis.

> **Illustrative status.** All data is **synthetic** (generated, not anonymized
> customer data). Entity names, ownership shares, rates and cost assumptions are
> **fictional**. The page is a **management consolidation**, not an IFRS statement.
> The banner on the page and the note in every CSV export say the same; keep it
> visible on screen throughout the demo.

### What this demo is not

Say this early, and again if someone asks about production use:

- **Not a production consolidation or an audit-ready close.** The accounting model is deliberately simplified (see the appendix). Equity is derived from an asset ratio, the funding gap is a residual line, the CTA is a residual and the P&L is a management view derived from stated assumptions.
- **Not evidence of production-grade controls.** The controls show *how* checks and exceptions can be part of the data product. They are not a statement about any bank's control framework.
- **Not a benefit case.** Benefits are agreed with Finance against a baseline (last section); the demo shows the intended way of working.

## Cast (personas)

| Persona | Role in the story |
| --- | --- |
| **CFO** | Asks the decision question, wants to trust the answer |
| **Head of Group Controlling** | Walks the bridge and the calculation lineage |
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

## Glossary (speaker notes, explain on first use)

| Term | One-line explanation |
| --- | --- |
| **IC (intercompany)** | Balances and flows between two group entities (loans, deposits, service fees). They cancel out in the group view. |
| **IC elimination** | Removing both sides of an IC position so the group shows only business with third parties. |
| **NCI (non-controlling interest)** | The share of a subsidiary owned by outside shareholders (here 20 % of Wealth CH, 25 % of Fund Services LU). |
| **Functional currency** | The currency an entity keeps its books in (CHF, EUR or USD). |
| **Closing rate** | FX rate at the reporting date; used for assets and liabilities. |
| **Average rate** | Average FX rate of the month; used for the P&L. |
| **Historical rate** | FX rate when the capital was paid in; used for share capital. |
| **CTA (currency translation reserve)** | The equity difference that arises because balance sheet, P&L and capital are translated at different rates. |

## Demo month: September 2026 (latest complete month)

The page opens on the **latest complete month**. October 2026 is selectable but
marked *partial* (data only up to 23 October); do not use it as the headline.

| Figure | Value (Sep 2026, reporting date 2026-09-30) |
| --- | --- |
| Group total assets (after eliminations) | CHF 6,134.8 M (August CHF 6,357.3 M, **−222.5 M**) |
| of which entity performance at constant FX | **−210.5 M** (Group Holding CH −72.0 M, Custody CH −53.6 M, Bank CH −39.4 M, Markets CH −33.6 M; Wealth CH +13.9 M) |
| of which FX effect | **−14.0 M** (stronger CHF against EUR and USD) |
| of which change in IC eliminations | **+2.1 M** |
| Σ entity assets / IC eliminated | CHF 6,285.8 M / CHF 151.0 M |
| Net profit before NCI (month) | CHF 1.51 M |
| NCI share of profit | CHF −0.03 M |
| **Profit to shareholders** (month) | **CHF 1.48 M** (August CHF 1.49 M) |
| Profit composition | interest income +9.16 M, interest expense −4.75 M, fees +1.26 M, personnel −2.26 M, other opex −1.51 M, tax −0.39 M |
| Currency translation reserve (CTA) | CHF −9.0 M |
| NCI in equity | CHF 9.4 M |
| Controls | 5 / 5 pass; with the demo exception 4 / 5 (FC-C2 fails by CHF 0.77 M on pair IC02) |

> Figures are indicative; always quote what the app shows on the day of the demo
> (the page and the gate read the same objects).

---

## Pre-demo checklist

1. Data is current: `sqlunit/fco_validation.sql` returns **FCO-01..07 PASS**
   (`snow sql -c <conn> -f sqlunit/fco_validation.sql`).
2. App runs: `HOST_PORT=8088 docker compose up -d --build` in `bank-app`, open
   `http://localhost:8088/finance/consolidation`; it opens on **Sep 2026**.
3. "Simulate exception" on the Controls tab is **off** (reload resets it).
4. Agree the success measures with Finance beforehand (see the last section);
   do not quote savings in the demo.

---

## Act 1 – The decision question (Bridge tab, 5 min)

**Say:** "Group assets went down by CHF 222 M last month. Is that a business
movement, an FX effect or intercompany noise? And how did we earn this month's
profit?"

**Show (assets):**
- KPI row: total assets **−222.5 M vs last month**, FX effect **−14.0 M**,
  controls **5 / 5**.
- Waterfall: last month CHF 6,357.3 M → entities **−210.5 M** → FX **−14.0 M** →
  IC elim. **+2.1 M** → this month CHF 6,134.8 M. The sentence above the chart states
  it in words.
- *Contribution by entity*: the decrease is broad-based (Group Holding CH, Custody CH,
  Bank CH, Markets CH); only Wealth CH grew. FX explains a small part, because
  most of the book is in CHF.

**Show (profit):**
- *What makes up this month's profit*: interest income +9.16 M, interest expense
  −4.75 M, fees +1.26 M, personnel −2.26 M, other opex −1.51 M, tax −0.39 M →
  **net profit before NCI 1.51 M** → NCI −0.03 M → **profit to shareholders 1.48 M**.
  Intercompany interest and fees net to zero.
- Profit is stable month on month (1.49 M → 1.48 M); the story of the month is in
  the balance sheet, not in the P&L.

**Message:** the answer comes first, decomposed the way a controller explains it.

## Act 2 – Trust: the calculation lineage (Consolidation sheet, 5 min)

**Say:** "Can I trust that number? Where does it come from?"

**Show:**
- The sheet: nine entity columns, **Σ Entities, IC Elim, NCI, Group**. Σ entity
  assets CHF 6,285.8 M, IC receivables eliminated to **0.0** in the group column
  (CHF 151.0 M), group CHF 6,134.8 M.
- Click **Group** on *Loans to clients*: the drawer lists every entity's
  contribution and adds up to the group value.
- Click **Brokerage US** on *Loans to clients*: the trail shows
  1. source positions and holding currencies,
  2. the mapping rule (product types → line),
  3. the USD amount,
  4. the closing rate USD/CHF and the CHF value,
  5. elimination, 6. NCI, 7. contribution to the group.
- Toggle **Local currency**: EUR and USD entities show their own currency;
  eliminations and the group stay in CHF. Toggle **P&L**: *Net profit before NCI*,
  the NCI line and *Net profit attributable to shareholders*; Brokerage US is the
  only loss-maker (−0.09 M).
- **Export CSV**: the file carries the illustrative note.

**Message:** every group figure can be followed through this demo's calculation
lineage down to the source rows, with no spreadsheet in between. *(Avoid "audit-proof"
or "production-ready".)*

## Act 3 – Controls and exceptions (Controls tab, 4 min)

**Say:** "Before sign-off, which controls ran, and what happens when one fails?"

**Show:**
- Five controls, all **PASS** (FC-C1 … FC-C5, table below).
- Click **Simulate exception**: Brokerage US kept its USD funding loan from
  Group Holding CH at **last month's rate** (not revalued). **FC-C2 turns red**:
  difference **CHF 0.77 M**, affected pair *IC02 Group Holding CH / Brokerage US*,
  resolution *revalue at the closing rate, confirm with the counterparty,
  rebook*. The KPI shows *exception open*, and the IC reconciliation table
  highlights the pair.
- Click **Reset exception**: back to 5 / 5.

**Message:** controls can be part of the data product. They run on every refresh,
and exceptions come with the affected objects and the resolution path.

### Controls on screen vs. the deployment checks

The **five controls** are what Finance sees on the page. The **seven FCO checks**
(`sqlunit/fco_validation.sql`) are the engineering gate that runs on every
deployment: they verify the controls themselves and the parts of the page that
are not controls.

| FCO check (deployment gate) | What it verifies | UI control |
| --- | --- | --- |
| FCO-01 balance sheet balances | FC-C1 passes in every month | FC-C1 |
| FCO-02 IC nets to zero | FC-C2 passes in every month (without the demo exception) | FC-C2 |
| FCO-03 demo exception | the exception turns exactly FC-C2 red and names pair IC02 | FC-C2 (demo) |
| FCO-04 CTA reconciles | FC-C3 passes; CHF entities have CTA 0 | FC-C3 |
| FCO-05 source completeness | FC-C4 passes in every month | FC-C4 |
| FCO-06 bridge adds up | opening + movements = closing for every month (Bridge tab) | – |
| FCO-07 trail ends at group | the calculation lineage adds up to the group value (Consolidation sheet) | – |
| – | data freshness is checked live on the page | FC-C5 |

## Act 4 – Accounting assumptions (Method tab, 3 min, optional detail)

**Say:** "How exactly are the foreign entities translated?" *Keep this short
unless the audience asks; use the glossary.*

**Show** (September figures):
- *Brokerage US*: closing rate USD/CHF **0.6793** (balance sheet), average
  **0.6753** (P&L and reserves), historical **0.8222** (share capital). Total
  assets USD 478.9 M → CHF 325.3 M. Net assets CHF 29.3 M − capital CHF 22.2 M −
  reserves CHF 10.8 M = **CTA CHF −3.8 M**.
- *EUR cross rate*: rates arrive as USD quotes; EUR/CHF = USD/CHF ÷ USD/EUR
  (**0.7658** closing).
- *Ownership and NCI*: Wealth CH 80 %, Fund Services LU 75 %. They are 100 % consolidated; the minority share of equity is shown as NCI, and the minority share of profit is deducted.
- *Where the numbers come from*: balances are the synthetic liquidity book
  positions (held in CHF, EUR and USD); functional-currency amounts are derived
  from them at the closing rate; the P&L is a management view derived from the
  balances and stated rate, fee and cost assumptions.

**Message:** the method is stated openly. It is a simplified demonstration model, not an accounting policy.

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
- **Simplified modelling choices (demonstration only, not accounting policy):**
  - Equity = equity ratio × assets per entity; the funding residual is shown as debt issued / other assets.
  - Share capital = 80 % of the first month's equity, at the historical rate; reserves at the average rate; the CTA is the residual.
  - The P&L is derived from closing balances × stated rates, fee bps and cost-income ratios.
  - IC pairs are agreed shares of the borrower's external assets, booked at the same closing rate on both sides.
  - The demo exception values the borrower side at the previous month-end rate.
- **Why October looks special:** in October (partial month) Σ entity assets happen to equal
  September's group total (CHF 6,134.8 M), so the October IC elimination (151.2 M) equals the
  month-on-month decline. This is a coincidence of the synthetic data, not a calculation link; it
  does not occur in any other month, which is why the demo uses September.
- **Tests:** `sqlunit/fco_validation.sql` (FCO-01..07, part of the `deploy.sh` gate),
  `sqlunit/tests.sqltest` (object counts), `bank-app/e2e/finance-consolidation.spec.ts`.
