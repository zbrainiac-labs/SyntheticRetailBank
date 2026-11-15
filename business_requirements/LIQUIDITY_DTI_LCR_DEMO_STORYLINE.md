# Demo Storyline: One Liquidity Book – LCR & Days-to-Illiquidity in the Bank App

## Purpose

A guided 30–40 minute walkthrough of the liquidity capabilities of the new
`bank-app` (Next.js on Snowflake App Runtime / SPCS). It shows how **one
liquidity book** — one securities master, one deposit/contract/cash-flow book,
one generation run — feeds two different steering views:

| View | Question it answers | Audience |
| --- | --- | --- |
| **LCR Monitoring** (`/liquidity/lcr`) | Do we hold enough HQLA to survive 30 days of regulatory stress (FINMA LiqV / Circ. 2015/2)? | Treasury, ALM, Regulatory Reporting |
| **DtI comparison** (`/dti/*`) | How many days until we run out of liquidity, per legal entity, and *why did it change* since yesterday? | Liquidity Risk Control, Treasury desk |

The key message: **both numbers come from the same rows** — no reconciliation
spreadsheet, governed by Snowflake roles and masking, refreshed by tasks and
dynamic tables.

## Cast (personas)

| Persona | Role in the story | App instance |
| --- | --- | --- |
| **Anna – Head of Treasury** | Owns the LCR, asks "are we OK?" | owner (`:8088`) |
| **Marco – Liquidity Risk Controller** | Explains DtI deltas, enters corrections, comments | owner (`:8088`) |
| **Sara – Internal Audit** | Read-only reviewer, must not change anything | `DTI_READ_ONLY` (`:8089`) |

## The scenario baked into the data

The generator scenario `generators/scenarios/lcr_dti_showcase.json` writes a
deterministic, story-driven history (one simulation from 2025-04-30 to the
as-of date). Offsets are relative to the as-of date, so the storyline works
for any regeneration.

Verified on run `LIQ-6B6B9414C571` (generator `liq-book-4`), as-of 2026-10-23
(the generator scales the configured amounts with the book, so the app shows
larger CHF figures than the JSON). The event table is visible on the LCR
**Overview** tab; the events that move both engines are also markers on the
DtI **Summary** (*DtI history and events*, DTI_08–DTI_10).

| Event | Date (verified) | Event | LCR effect (verified) |
| --- | --- | --- | --- |
| EVT_01 / DTI_01 | 2026-08-24 | Alpenbau Holding AG withdraws CHF 147 M non-operational deposit (paid from SNB reserves) | 139.1 % → 134.4 % (−4.7 pp, top driver −4.1 pp) |
| EVT_02 / DTI_08 | 2026-09-08 | **Funding cliff** in a market stress: CHF 547 M interbank funding from Zentralbank Partner AG (`CTR_002564`) falls inside 30 days, plus bond/equity price shock and corporate deposit run | 136.7 % → 98.6 % (−38.2 pp, −35.1 pp from CTR_002564); low 90.3 % on 09-14; 5 business days (7 calendar days) below 100 % |
| EVT_03 / DTI_09 | 2026-09-15 | Recovery: treasury rolls the funding to 6 months **in three tranches** (`CTR_002564-R1..R3`, 40 / 35 / 25 %) and buys SNB reserves with term funding | 90.3 → 104.3 → 119.9 → 132.6 % (09-15 … 09-17) |
| EVT_04 / DTI_02 | 2026-10-05 | CHF 188 M corporate loans (5) mature within 30 days | 133.6 % → 142.3 % (+8.7 pp) |
| EVT_05 / DTI_10 | 2026-10-20 | Rhein Logistik Group AG withdraws CHF 162 M | 142.3 % → 138.5 % (−3.7 pp, top driver −5.1 pp) |
| DTI_06 | −20 business days | Security in WLK VI downgraded to VII | DtI Free Assets / Securities |
| DTI_05 | −12 business days | LE_01 buys CHF 90 M WLK II security (unencumbered) from SNB reserves | DtI pool shifts from cash to securities |
| DTI_03 | reference date | CORR-0001: LE_01 central bank balance −11.5 M (30 % of its balance, late SIC settlement) | DtI Manual Inputs |
| — | previous date | CORR-0002: LE_03 corporate outflow −2.5 M (announced withdrawal) | DtI Manual Inputs |
| DTI_04 | reference date | LE_07 delivers **preliminary** data | DtI Summary "preliminary" |
| DTI_07 | +10 days | A WLK IV security matures | DtI liquidity projection |

Verified headline figures on 2026-10-23: regulatory LCR **135.0 % PASS**
(HQLA CHF 1.87 bn, net outflows CHF 1.39 bn, buffer CHF 485 M); internal
scenarios Bank stress 111.2 %, Market stress 115.9 %, Combined stress
**101.7 %** (all ≥ 100 % today, all < 100 % in the stress window); free
pool CHF 1,898 M on both engines. DtI (stressed, 10-23 vs 10-22) per entity:
LE_01 75 / 70, LE_02 55 / 56, LE_03 33 / 31, LE_04 28 / 28, LE_05 52 / 55,
LE_06 20 / 22, LE_07 22 (PRELIMINARY) / 19, LE_08 21 / 19, LE_09 42 / 42;
default group (LE_01–03) 56 / 56 days, all entities 41 / 40 days. On the
stress date 2026-09-08 the group DtI falls 51 → 20 days — the same contract
that breaks the LCR.

> Figures in this document are indicative; always quote what the app shows
> on the day of the demo.

---

## Pre-demo checklist

1. Data generated and checked **before upload**: `./data_generator.sh`
   (optionally `LIQ_SEED`, `AS_OF_DATE`), then the offline demo gate
   `venv/bin/python generators/liquidity_checks.py generated_data/lcr`
   (DEMO-01..08 must PASS), then `./upload-data.sh`.
2. Engines refreshed: `operation/execute_all_tasks_and_refresh_dts.sql`.
3. **Suspend LIQI tasks after any DCM deploy** (deploy resumes them):
   `snow sql -c zs28104-svc_mdaeppen --role CICD -f operation/tasks_and_dts.sql --enable-templating JINJA -D db=AAA_DEV_SYNTHETIC_BANK -D action=suspend -D task_like=LIQI_RAW_TK_% -D dt_like=-`
4. SQL gates green: `sqlunit/data_check.sql` (DATA-LIQ-01..08),
   `sqlunit/dti_validation.sql`, `sqlunit/lcr_validation.sql`.
5. **`make demo-up`** (in `bank-app/`): starts the owner app on `:8088` and
   `DTI_READ_ONLY` on `:8089` (production build), waits for both health
   checks and runs the DEV demo gate `operation/demo_check.sql`
   (DEMO-01..09 must PASS).
6. Optional dress rehearsal: `npx playwright test e2e/demo-storyline.spec.ts`
   with `BASE_URL=http://localhost:8088 RO_URL=http://localhost:8089` —
   walks acts 1–8 and leaves screenshots in `test-results/demo/`.
7. Open both apps in separate browser windows, owner window on
   `/liquidity/lcr`. Saved DtI filters reset automatically when the book's
   as-of date changes (no local-storage clean-up needed).
8. **After the demo / a rehearsal**: `operation/demo_reset.sql` removes the
   corrections entered live, refreshes the DtI tables and suspends the LIQI
   tasks.

---

## Act 1 – "One book, one run" (3 min)

**Story:** Anna starts her day. Before looking at any number she wants to
know *which data* she is looking at.

1. Open **LCR Monitoring** (`/liquidity/lcr`).
2. Point at the **liquidity book badge** (top of page): generation run ID,
   as-of date, latest LCR and latest DtI pool.
3. Switch to **DtI → Summary** (`/dti`): the same badge, same run, same as-of
   date. Both pages default to the run's as-of date.

**Talking points**
- One raw book (holdings = securities master, one deposit book), one
  `RUN_ID`; the badge flags a mismatch if the two engines read different runs.
- The free collateral pool shown in DtI equals the unencumbered HQLA in the
  LCR — encumbered securities are not HQLA-eligible, credit claims are
  excluded from LCR.

## Act 2 – LCR: are we compliant today? (6 min)

1. **Overview** tab: headline LCR vs the 100 % minimum and 110 % internal
   target; status pill; 90-day trend line with both reference lines.
2. Point at the event markers EVT_01–EVT_05 on the 90-day chart and the
   *Liquidity events* table below it; show the stress window
   2026-09-08 … 09-14 below 100 % and the recovery on 09-15.
3. *(Scenarios tab: the internal Combined stress is 101.7 % today — tight
   but covered; Bank 111.2 %, Market 115.9 %. All three were below 100 % in
   the stress window.)*
4. **HQLA & Caps**: Level 1 CHF 1,300 M, Level 2a CHF 471 M, Level 2b
   CHF 98 M after haircuts; Level 2 share ≈ 30 % (max 40 %) — caps not
   binding.
5. **Flows & Inflow Cap**: outflows CHF 1,537 M (corporate 777 M, financial
   institutions 422 M, facilities 324 M), weighted inflows CHF 152 M — 75 %
   inflow cap far from binding.
6. **Behavioral**: run-off rates per deposit class (retail stable / less
   stable, corporate operational / non-operational).

**Talking points:** regulatory logic lives in Snowflake (dynamic tables
`REPP_AGG_DT_LCR_*`), the app only renders. Rules come from reference tables
(`reference_data/liqi/*.csv`) versioned in git.

## Act 3 – LCR: what happened during the stress? (6 min)

1. **Scenarios** tab: Regulatory (BASE) vs Bank stress, Market stress,
   Combined stress for the same date.
2. **Variance** tab, day over day, set the reporting date to:
   - **2026-08-24** → −4.7 pp, top driver Alpenbau Holding AG
     `CP_CORP_EVT1_DEP_01` (−4.1 pp).
   - **2026-09-08** → −38.2 pp, top driver Zentralbank Partner AG
     `CTR_002564` (−35.1 pp, CHF 547 M into the 30-day window).
   - **2026-09-15** → +14.0 pp, top driver the first rolled tranche
     `CTR_002564-R1` (+11.7 pp); 09-16 and 09-17 roll the rest
     (+15.6 pp, +12.7 pp).
   Message: *the breach was a funding concentration, not the deposit run —
   and treasury fixed it over three days, not overnight.*
3. **Drill-down**: treemap asset class → product group → counterparty
   (13k positions aggregated in < 1 s) and the contract table.
4. **Currency**: HQLA per outflow CHF 1.48, USD 3.42, EUR 1.17 — EUR is the
   tightest currency.
5. **Data Quality**: 7 quarantined records (2 invalid date, 2 missing SEK
   FX, 3 unmapped product `NEW_DERIVATIVE_X`), excluded from all figures.

**Talking point:** root cause in three clicks, from ratio to the deposit
account, all on governed data.

## Act 4 – LCR what-if simulator & reporting (5 min)

**Story:** Anna asks: "What if tomorrow looks like the stress window again?"

1. **Simulator** tab: apply a price shock (%), extra run-off and haircut
   add-ons for L1 / L2a / L2b (pp). Show the **Simulated LCR** next to the
   actual one.
2. Show "Extra withdrawal to 100 %": CHF 618 M today (sqlunit ENG-06:
   that withdrawal gives 99.98 %). Then add a market price shock and higher
   corporate run-off and compare the simulated LCR with the actual one.
   Note: the simulator cannot reproduce the September breach from shocks
   alone, because that breach was the CHF 547 M funding maturity.
3. **Reporting** tab: monthly SNB summary — September 2026 FAIL (7 days
   below 100 %, min 90.26 %), July/August/October PASS.

## Act 5 – DtI: how long can we survive, per entity? (6 min)

**Story:** Marco prepares the daily DtI comparison: reference date (today)
vs comparison date (previous business day).

1. **DtI → Filter**: set global defaults — entities (group = sum of the
   selected entities), reference date, comparison date, value type,
   "include preliminary". Show the data-status table for both dates.
2. **Summary** (`/dti`):
   - KPI tiles: collateral pool, deterministic cash flows, inflows,
     outflows, total liquidity balance — reference, comparison, change.
   - **Liquidity projection** chart: cumulative balance over 365 days; the
     day it crosses zero is the DtI. Point out the step at +10 days (WLK IV
     maturity).
   - **DtI per entity**: default selection is LE_01–03 (group 56 vs 56
     days; LE_01 75, LE_02 55, LE_03 33). Add LE_07 → shows *preliminary*;
     toggle "include preliminary" to reveal 22 days. Every entity sits
     between 20 and 75 days — each entity holds its own buffer, sized to
     its stressed outflows.
   - **DtI history and events**: group DtI over the last 100 days with the
     event markers. Point at **DTI_08 (2026-09-08)**: the group DtI drops
     from ~51 to ~20 days on the same day the LCR breaks — *same contract,
     same book, two views*. DTI_09 shows the tranche roll; events between
     comparison and reference date are flagged *in this comparison*
     (today: CORR-0001, the preliminary LE_07 run, the WLK IV maturity).
3. **Parameters**: every provision the calculation uses — legal entities and
   central bank shares, LST position scheme, applications, product groups /
   clusters, WLK clusters, commenting thresholds. All read from the
   reference tables, nothing hard-coded in the app.

**Talking point:** the pool counts free securities as **repo capacity from
day 1** at stressed collateral value — same securities as the LCR HQLA.

## Act 6 – DtI: explain the delta, top-down (7 min)

**Story:** Group DtI changed vs yesterday. Marco must explain it before the
morning call.

1. **LST Positions**: pivot of reference / comparison / delta per position
   and time bucket. Deltas above the commenting threshold are highlighted.
2. Click a highlighted cell → **Deal Level**: the deals behind the number,
   with delta.
3. **Applications**: same delta by source application, expand/collapse
   Aktiva and Passiva.
4. **Product Groups** → click a cluster header → **Single Products** of that
   cluster.
5. **Free Assets**: collateral value vs nominal vs market value at t+1 and
   t+365; collateralised business; breakdown of unencumbered assets by
   category; mobilisation status. WLK I shows the central bank and vault
   cash of the selected entities (fixed central bank share) — the same cash
   the LCR counts as Level 1. Show the CHF 90 M WLK II purchase of LE_01
   (−12 business days) and the WLK VI→VII downgrade (−20 business days).
6. **Securities**: master data per WLK cluster — the same securities that
   are LCR holdings.

## Act 7 – Manual inputs, governance & audit (5 min)

1. **Manual Inputs → Corrections**: CORR-0001 (LE_01 central bank balance
   −11.5 M, late SIC settlement, by LRCU_ANALYST at 22:15, *in engine*) on
   the reference date — sized from the balance, so the central bank balance
   never goes negative (DATA-LIQ-08); CORR-0002 (LE_03 corporate outflow
   −2.5 M) on the comparison date. Show who / when / comment. Add a
   correction live (rehearsed in `e2e/demo-storyline.spec.ts`), refresh,
   and run `operation/demo_reset.sql` afterwards.
2. **Commenting thresholds**: entity vs group limits per bucket.
3. **Export CSV** on the Summary page (owner only; verified 200).
4. Switch to **Sara's window (`:8089`, role `DTI_READ_ONLY`)**:
   - Same DtI numbers; `canEdit=false`, `canExport=false`. A correction
     POST is rejected with *"Role DTI_READ_ONLY may not edit DtI manual
     inputs"* (app-level role check, verified).
   - LCR is not granted to this role — Snowflake rejects the LCR query and
     the book badge reports "not comparable" instead of a false mismatch.
   - Customer PII is masked (masking policies), while the owner sees clear
     values.

**Talking point:** caller's rights — the app runs queries as the viewer,
so row access and masking policies apply per user without app code.

## Act 8 – Ask the data (optional, 3 min)

Open **AI → Ask** (`/ai/ask`, agent `REP_AGG_V001.MASTER_AGENT`) and ask:
- "Why did the regulatory LCR fall below 100 % between 2026-09-08 and
  2026-09-14?" — verified: answers with the CHF 549 M Zentralbank Partner
  funding (−35.1 pp), the 93.68 % low and the 09-15 roll, and states which
  parts it verified (≈ 1–2 min response).
- "Which legal entity has the lowest stressed DtI today?" (not verified)

## Closing message (1 min)

- **One book**: LCR and DtI reconcile by construction (DATA-LIQ checks),
  not by spreadsheet.
- **Explainable**: from ratio / days down to deal and deposit in a few
  clicks.
- **Governed**: roles, masking, read-only audit access, corrections with
  comments.
- **Automated**: generation run → stage → tasks → dynamic tables → app;
  CI with unit, e2e and SQL tests.

---

## Appendix C – Dry-run findings (2026-11-15) and their resolution

All 8 acts were executed against `:8088` (owner) and `:8089`
(`DTI_READ_ONLY`). The ten findings below were fixed in run
`LIQ-6B6B9414C571` (plan `liquidity-demo-realism-fixes`) and are guarded by
the demo gate (offline `generators/liquidity_checks.py`, DEV
`operation/demo_check.sql`, both PASS on 2026-11-15):

| # | Finding | Resolution | Guard |
| --- | --- | --- | --- |
| 1 | LE_01 DtI 36 → 0, CB balance −9.1 M after CORR-0001 | CORR-0001 = 30 % of LE_01's balance (−11.5 M); cash split by the fixed central bank share | DEMO-07, DATA-LIQ-08 (min +5.0 M) |
| 2 | Entity DtI jumps (LE_08 102 → 192), LE_04/06 at 8–9 days | Securities booked per entity by stressed 60-day need, daily intra-group rebalancing, rolling business-day planning grid | DEMO-06 (p95 move 13 days, all entities ≥ 20 days) |
| 3 | Weekly LCR saw-tooth, EVT_05 not visible | Fixed treasury-bill ladder, maturities on business days only; EVT_05 enlarged | DEMO-02 (≤ 0.3 pp), DEMO-04 (−3.7 pp) |
| 4 | Recovery +44.9 pp in one day | Funding rolled in three tranches over three business days | DEMO-03 (max step 15.6 pp) |
| 5 | "Market stress" was really a funding cliff | Story names it a funding cliff; price shock and deposit run doubled | DEMO-01 |
| 6 | Internal stress FAIL on the as-of date | Internal BANK / MARKET / COMBINED rates recalibrated (BASE unchanged): Combined 101.7 % | DEMO-05 |
| 7 | Saved DtI filter overrode the as-of date | Saved dates expire when the book's as-of date changes | vitest `filters.test.ts` |
| 8 | Float noise on LCR axis labels | Whole-percent ticks on multiples of 10 % | vitest `format.test.ts` |
| 9 | DtI WLK I = 0 while LCR counts CHF 44 M cash | Free Assets WLK I shows central bank + vault cash | e2e `liquidity-book.spec.ts` |
| 10 | Read-only container not running | `make demo-up` starts both apps and runs the gate | checklist 5 |

The September LCR breach is preserved (−38.2 pp, 5 business days below
100 %, low 90.3 %). Verdict: **both parts are demo-ready**; the remaining
day-over-day DtI moves are single deals maturing or intra-group collateral
transfers and can be explained on the Deal Level page.

<details><summary>Original findings (before the fix)</summary>

| # | Severity | Finding | Recommendation |
| --- | --- | --- | --- |
| 1 | High | **LE_01 DtI 36 → 0 days** day over day. CORR-0001 (−15 M) exceeds LE_01's central bank balance (5.5 M), so the SNB balance goes to −9.1 M on day 0. A negative SNB sight balance is not realistic. | Scale CORR-0001 to the entity's balance (e.g. −3 M) or raise LE_01's CB share. |
| 2 | High | **Unrealistic DtI jumps**: LE_08 102 → 192, LE_09 161 → 129 days in one day without an event; LE_04 at 8 days, LE_06 at 9 days while group LCR is 134 %. | Smooth entity-level flows / pool allocation; target entity DtI ≥ 30 days except a scripted one. |
| 3 | Medium | **Weekly saw-tooth in LCR**: weekdays drift down (≈ −1.5 pp/day), Saturday +3.6 pp on average. Looks like a calendar artifact. EVT_05 (−2.4 pp) is not distinguishable from it. | Investigate weekend handling in the generator (deposits/flows on non-business days); make EVT_05 bigger or drop it from the story. |
| 4 | Medium | **Recovery +44.9 pp in one day** (94.4 → 139.3 %). Plausible mechanically (funding roll), but a real treasury would recover over several days. | Optional: spread the roll over 2–3 days. |
| 5 | Medium | "Market stress" event is really a **funding cliff**: the price shock and deposit run contribute < 1 pp; one CHF 549 M contract is −35 pp. | Rename EVT_02 in the story (done above) or make the price shock visible. |
| 6 | Medium | **Internal stress scenarios FAIL on the as-of date** (Bank 99.1 %, Combined 84.8 %) while BASE is 134 %. Defensible, but the audience will ask. | Brief presenter, or calibrate Combined ≥ 100 %. |
| 7 | Low | Saved DtI filter in localStorage overrides the as-of default. | Pre-demo step 7 added; consider expiring saved dates when the run changes. |
| 8 | Low | Chart y-axis labels with float noise (`47.66999999999999%`) on LCR Overview / Scenarios. | Use `formatTick` / round domain. |
| 9 | Low | DtI Free Assets WLK I (central bank cash) = 0 while LCR shows CHF 44 M cash L1. | Check whether cash is intentionally outside the securities pool; explain in Act 1 otherwise. |
| 10 | Low | Read-only container was stopped. | Pre-demo step 8 added. |

</details>

## Appendix A – Click path cheat-sheet

| Step | URL | Tab / element |
| --- | --- | --- |
| 1 | `/liquidity/lcr` | Book badge |
| 2 | `/liquidity/lcr` | Overview, HQLA & Caps, Flows & Inflow Cap, Behavioral |
| 3 | `/liquidity/lcr` | Scenarios, Variance, Drill-down, Currency, Data Quality |
| 4 | `/liquidity/lcr` | Simulator, Reporting |
| 5 | `/dti/filter`, `/dti`, `/dti/parameters` | Filters, Summary (incl. DtI history and events), Parameters |
| 6 | `/dti/positions` → `/dti/deals`, `/dti/applications`, `/dti/product-groups` → `/dti/products`, `/dti/free-assets`, `/dti/securities` | Delta drill-down |
| 7 | `/dti/manual-inputs`, `:8089/dti` | Corrections, thresholds, read-only |
| 8 | `/ai/ask` | Natural language |

## Appendix B – Fallbacks

| Symptom | Fix |
| --- | --- |
| "No LCR data" | Run `operation/execute_all_tasks_and_refresh_dts.sql` |
| Book badge mismatch | Engines read different runs – refresh DTs, re-check DATA-LIQ-07 |
| Demo figures look off | `operation/demo_check.sql` shows which storyline value moved (DEMO-01..09) |
| Leftover live corrections | `operation/demo_reset.sql` |
| DtI entity shows "preliminary" unexpectedly | Expected for LE_07; toggle "include preliminary" |
| Numbers change during demo | LIQI tasks were resumed by a deploy – suspend them (checklist 3) |
| e2e/dev reloads break the page | Use `next start` production build |
