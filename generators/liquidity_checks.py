"""
Offline demo-readiness checks of the LCR + DtI liquidity book (no Snowflake).

Reads one generator output directory (generated_data/lcr or a scratch copy) and recomputes what the
Snowflake engines show in the bank app:
  - scenario LCR per reporting date (port of 360_LIQA_analytics: rule x gross, run-off adjustments,
    haircut add-ons, L2B / L2 caps, 75% inflow cap) for BASE, BANK_STRESS, MARKET_STRESS, COMBINED
  - DtI per legal entity (port of 365_LIQA_dti_engine: contractual flows, new loans, collateral pool,
    cash allocation, run-off, renewals, drawdowns, secured funding, planning, corrections)
and evaluates the demo storyline acceptance values (DEMO-01..08, see operation/demo_check.sql for the
DEV twin incl. DEMO-09). Ports are checked against the engines by the DEV gate, not here.

Run: venv/bin/python generators/liquidity_checks.py generated_data/lcr [--csv out.csv]
Exit code 0 = all checks pass.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from functools import cache
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reference_loader as ref

SCENARIOS = ("BASE", "BANK_STRESS", "MARKET_STRESS", "COMBINED")
INFLOW_CAP, L2B_CAP_RATIO, L2_CAP_RATIO = 0.75, 15.0 / 85.0, 2.0 / 3.0
HORIZON = 365
LOAN_RENEWAL = {"BASE": 0.20, "STRESS": 0.60}  # 365 engine, renewal CTE
COLLATERAL_CALL = 0.10                          # 365 engine, secured CTE
INVALID_PRODUCTS, INVALID_CCY = {"NEW_DERIVATIVE_X"}, {"SEK"}

# Acceptance values of the demo storyline (business_requirements/LIQUIDITY_DTI_LCR_DEMO_STORYLINE.md)
LIMITS = {
    "breach_min_drop_pp": 30.0, "breach_min_days": 5, "breach_low": (90.0, 97.0),
    "weekday_bias_pp": 0.5, "recovery_max_step_pp": 25.0, "evt05_min_pp": 3.0,
    "combined_asof": (100.0, 105.0), "base_asof": (132.0, 138.0),
    "dti_jump_p95_days": 15, "dti_jump_max_days": 60, "dti_min_days_asof": 20,
}


# --------------------------------------------------------------------------- input
@dataclass(frozen=True)
class Book:
    out: Path

    def csv(self, name: str) -> pd.DataFrame:
        return pd.read_csv(self.out / name)

    def snapshot(self, kind: str, d: date) -> pd.DataFrame:
        return _read_snapshot(self.out, kind, d)

    def lcr_dates(self) -> list[date]:
        return sorted(date.fromisoformat(str(x)[:10]) for x in self.csv("_generator_lcr_estimate.csv").AS_OF_DATE)

    def dti_dates(self) -> list[date]:
        return sorted(date.fromisoformat(f.name[-12:-4][:4] + "-" + f.name[-8:-6] + "-" + f.name[-6:-4])
                      for f in self.out.glob("dti_securities_*.csv"))


@cache
def _read_snapshot(out: Path, kind: str, d: date) -> pd.DataFrame:
    """One snapshot file of a generator output directory (cached: every check reads the same files)."""
    f = out / f"{kind}_{d:%Y%m%d}.csv"
    return pd.read_csv(f) if f.exists() else pd.DataFrame()


@cache
def _rules() -> pd.DataFrame:
    r = pd.DataFrame(ref._rows("behavioral_rules"))
    for c in ("RUNOFF_RATE", "INFLOW_RATE", "DRAWDOWN_RATE", "HAIRCUT_ADDON_PCT", "DECAY_PROFILE_DAYS"):
        r[c] = pd.to_numeric(r[c], errors="coerce")
    return r[r.IS_ACTIVE == "TRUE"]


@cache
def _haircut() -> dict[str, tuple[str, float]]:
    return ref.hqla_types()


class Fx:
    """CCY -> CHF from the deposit snapshot of the same day (generator FX), CHF = 1."""

    def __init__(self, deps: pd.DataFrame):
        rates = deps[deps.CURRENCY != "CHF"].dropna(subset=["FX_RATE"]).groupby("CURRENCY").FX_RATE.first()
        self.rate = {"CHF": 1.0, **rates.to_dict()}

    def __call__(self, ccy: pd.Series) -> pd.Series:
        return ccy.map(self.rate).fillna(0.0)


def _valid_contracts(ctr: pd.DataFrame) -> pd.DataFrame:
    ok = (~ctr.PRODUCT_TYPE.isin(INVALID_PRODUCTS) & ~ctr.CURRENCY.isin(INVALID_CCY)
          & (pd.to_datetime(ctr.START_DATE) <= pd.to_datetime(ctr.MATURITY_DATE)))
    return ctr[ok]


def _runoff_adjustment(dep: pd.DataFrame) -> pd.Series:
    """Account level run-off adjustment of 360 (relationship discounts, tenure penalty)."""
    allows = dep.DEPOSIT_TYPE.map({k: v[2] for k, v in ref.deposit_types().items()}).fillna(False).astype(bool)
    adj = -0.02 * (allows & (dep.PRODUCT_COUNT >= 3)) - 0.01 * (allows & dep.HAS_DIRECT_DEBIT.astype(bool))
    return adj + 0.05 * (dep.ACCOUNT_TENURE_DAYS < 540)


# --------------------------------------------------------------------- LCR port
def scenario_lcr(book: Book, d: date) -> dict[str, float]:
    """LCR in % per scenario on date d."""
    hold, dep, ctr, cfs = (book.snapshot(k, d) for k in ("hqla_holdings", "deposit_balances", "contracts", "cashflows"))
    fx = Fx(dep)
    rules = _rules()
    dep = dep[dep.ACCOUNT_STATUS == "ACTIVE"]
    ctr = _valid_contracts(ctr)
    end = pd.Timestamp(d + timedelta(days=30))
    cf = cfs[(pd.to_datetime(cfs.CF_DATE) > pd.Timestamp(d)) & (pd.to_datetime(cfs.CF_DATE) <= end)]
    cf = cf.assign(CHF=cf.AMOUNT_CCY * fx(cf.CURRENCY))
    cf30 = cf.pivot_table(index="CONTRACT_ID", columns="DIRECTION", values="CHF", aggfunc="sum").fillna(0.0)
    ctr = ctr.join(cf30, on="CONTRACT_ID").fillna({"IN": 0.0, "OUT": 0.0})
    return {s: _lcr_one(rules[rules.STRESS_SCENARIO == s], hold, dep, ctr, fx) for s in SCENARIOS}


def _lcr_one(r: pd.DataFrame, hold: pd.DataFrame, dep: pd.DataFrame, ctr: pd.DataFrame, fx: Fx) -> float:
    h = hold[hold.HQLA_ELIGIBLE.astype(bool)]
    addon = r[r.COUNTERPARTY_SECTOR == "ALL"].set_index("PRODUCT_TYPE").HAIRCUT_ADDON_PCT.fillna(0.0)
    lvl = h.ASSET_TYPE.map(lambda t: _haircut()[t][0])
    factor = h.ASSET_TYPE.map(lambda t: _haircut()[t][1]) - h.ASSET_TYPE.map(addon).fillna(0.0) / 100
    w = h.MARKET_VALUE_CHF * factor.clip(lower=0)
    l1, l2a, l2b = (w[lvl == x].sum() for x in ("L1", "L2A", "L2B"))
    key = ["PRODUCT_TYPE", "COUNTERPARTY_SECTOR"]
    dr = r.rename(columns={"PRODUCT_TYPE": "DEPOSIT_TYPE", "COUNTERPARTY_SECTOR": "COUNTERPARTY_TYPE"})
    dm = dep.merge(dr[["DEPOSIT_TYPE", "COUNTERPARTY_TYPE", "RUNOFF_RATE"]], on=["DEPOSIT_TYPE", "COUNTERPARTY_TYPE"])
    outflow = (dm.BALANCE_CHF * (dm.RUNOFF_RATE + _runoff_adjustment(dm)).clip(0.03, 1.0)).sum()
    cm = ctr.merge(r[key + ["RUNOFF_RATE", "INFLOW_RATE", "DRAWDOWN_RATE"]], on=key)
    cfx = fx(cm.CURRENCY)
    outflow += (cm.OUT * cm.RUNOFF_RATE).where(cm.RUNOFF_RATE.notna(), 0).sum()
    outflow += (cm.UNDRAWN_CCY * cfx * cm.DRAWDOWN_RATE).where(cm.DRAWDOWN_RATE.notna(), 0).sum()
    inflow = (cm.IN * cm.INFLOW_RATE * cm.IS_PERFORMING.astype(bool)).where(cm.INFLOW_RATE.notna(), 0).sum()
    l2b_c = min(l2b, L2B_CAP_RATIO * (l1 + l2a))
    hqla = l1 + min(l2a + l2b_c, L2_CAP_RATIO * l1)
    net = outflow - min(inflow, INFLOW_CAP * outflow)
    return float(hqla / net * 100) if net > 0 else float("nan")


# --------------------------------------------------------------------- DtI port
@cache
def _assignment() -> pd.DataFrame:
    return pd.DataFrame(ref._rows("dti_lst_assignment"))


@cache
def _roots() -> dict[str, str]:
    pos = {r["POSITION_CODE"]: r["PARENT_CODE"] for r in ref._rows("dti_lst_positions")}

    def root(p: str) -> str:
        while pos.get(p):
            p = pos[p]
        return p
    return {p: root(p) for p in pos}


@cache
def _dti_rules() -> pd.DataFrame:
    r = _rules().assign(RATE=lambda x: x.RUNOFF_RATE.fillna(x.DRAWDOWN_RATE))
    base = r[r.STRESS_SCENARIO == "BASE"].set_index(["PRODUCT_TYPE", "COUNTERPARTY_SECTOR"])
    stress = r[r.STRESS_SCENARIO == "COMBINED"].set_index(["PRODUCT_TYPE", "COUNTERPARTY_SECTOR"])
    return pd.DataFrame({"RATE_BASE": base.RATE, "RATE_STRESS": stress.RATE,
                         "DAYS_BASE": base.DECAY_PROFILE_DAYS, "DAYS_STRESS": stress.DECAY_PROFILE_DAYS}).reset_index()


def _assigned(source: str, flow_kind: str) -> set[str]:
    a = _assignment()
    return set(a[(a.SOURCE_SYSTEM == source) & (a.FLOW_KIND == flow_kind)].PRODUCT_TYPE)


def _flow(entity, day, root, base, stress) -> pd.DataFrame:
    return pd.DataFrame({"ENTITY_ID": entity, "DAY": day, "ROOT": root, "BASE": base, "STRESS": stress})


def dti_flows(book: Book, d: date, cash_share: dict[str, float] | None = None) -> pd.DataFrame:
    """All signed DtI flows of reporting date d (ENTITY_ID, DAY offset, Level I ROOT, BASE, STRESS)."""
    parts = [_contractual(book, d), _pool(book, d, cash_share), _runoff(book, d), _drawdown(book, d),
             _secured(book, d), _planning(book, d), _corrections(book, d)]
    f = pd.concat([p for p in parts if len(p)], ignore_index=True)
    return f[f.DAY <= HORIZON].assign(DAY=lambda x: x.DAY.clip(lower=0))


def _days(series: pd.Series, d: date) -> pd.Series:
    return (pd.to_datetime(series) - pd.Timestamp(d)).dt.days


def _contractual(book: Book, d: date) -> pd.DataFrame:
    dep = book.snapshot("deposit_balances", d)
    fx = Fx(dep)
    ctr = _valid_contracts(book.snapshot("contracts", d))
    cfs = book.snapshot("cashflows", d)
    m = cfs.merge(ctr[["CONTRACT_ID", "PRODUCT_TYPE", "ENTITY_ID"]], on="CONTRACT_ID")
    m = m[m.PRODUCT_TYPE.isin(_assigned("CASHFLOWS", "CONTRACTUAL"))]
    m = m.assign(DAY=_days(m.CF_DATE, d), AMT=m.AMOUNT_CCY * fx(m.CURRENCY) * m.DIRECTION.map({"OUT": -1, "IN": 1}))
    m = m[m.DAY > 0]
    tx = book.snapshot("dti_transactions", d)
    nl = tx[(tx.TXN_TYPE == "NEW_LOAN") & (pd.to_datetime(tx.END_DATE) > pd.Timestamp(d))] if len(tx) else tx
    if len(nl):
        tenor = (pd.to_datetime(nl.END_DATE) - pd.to_datetime(nl.BOOK_DATE)).dt.days
        nl = nl.assign(DAY=_days(nl.END_DATE, d), AMT=nl.AMOUNT_CHF * (1 + nl.INTEREST_RATE * tenor / 365))
        m = pd.concat([m[["ENTITY_ID", "DAY", "AMT", "PRODUCT_TYPE"]], nl[["ENTITY_ID", "DAY", "AMT", "PRODUCT_TYPE"]]])
    out = _flow(m.ENTITY_ID, m.DAY, "DET", m.AMT, m.AMT)
    ren = m[m.PRODUCT_TYPE.isin(_assigned("CASHFLOWS", "INFLOW")) & (m.AMT > 0)]
    renew = _flow(ren.ENTITY_ID, ren.DAY, "INF", -LOAN_RENEWAL["BASE"] * ren.AMT, -LOAN_RENEWAL["STRESS"] * ren.AMT)
    return pd.concat([out, renew], ignore_index=True)


def cash_shares(book: Book, d: date, fixed: bool) -> dict[str, float]:
    """Entity share of central bank / vault cash: fixed CB_BALANCE_SHARE or current active-deposit share."""
    if fixed:
        return {e: w / 100 for e, w in ref.entity_shares()}
    dep = book.snapshot("deposit_balances", d)
    s = dep[dep.ACCOUNT_STATUS == "ACTIVE"].groupby("ENTITY_ID").BALANCE_CHF.sum()
    return (s / s.sum()).to_dict()


def _pool(book: Book, d: date, cash_share: dict[str, float] | None) -> pd.DataFrame:
    sec = book.snapshot("dti_securities", d)
    sec = sec[~sec.IS_ENCUMBERED.astype(bool) & sec.HQLA_CATEGORY.isin(["L1", "L2A", "L2B"])]
    pool = _flow(sec.ENTITY_ID, 1, "POOL", sec.MARKET_VALUE_CHF, sec.COLLATERAL_VALUE_CHF)
    hold = book.snapshot("hqla_holdings", d)
    cash = hold[hold.ASSET_TYPE.str.startswith("CASH")].MARKET_VALUE_CHF.sum() if len(hold) else 0.0
    share = cash_share or cash_shares(book, d, fixed=False)
    ents = list(share)
    amt = [cash * share[e] for e in ents]
    return pd.concat([pool, _flow(ents, 0, "POOL", amt, amt)], ignore_index=True)


def _expand(df: pd.DataFrame, days_col: str, amount: pd.Series, d_max: int) -> pd.DataFrame:
    """Spread amount linearly over days 1..days (vectorised LATERAL FLATTEN of the engine)."""
    rows = []
    for k in range(1, d_max + 1):
        on = df[days_col] >= k
        rows.append(pd.DataFrame({"ENTITY_ID": df.ENTITY_ID[on], "DAY": k, "AMT": (amount / df[days_col])[on]}))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["ENTITY_ID", "DAY", "AMT"])


def _runoff(book: Book, d: date) -> pd.DataFrame:
    dep = book.snapshot("deposit_balances", d)
    dep = dep[dep.ACCOUNT_STATUS == "ACTIVE"][["ENTITY_ID", "DEPOSIT_TYPE", "COUNTERPARTY_TYPE", "BALANCE_CHF"]]
    tx = book.snapshot("dti_transactions", d)
    if len(tx):
        mv = tx[tx.TXN_TYPE == "DEPOSIT_MOVEMENT"].rename(columns={"PRODUCT_TYPE": "DEPOSIT_TYPE", "AMOUNT_CHF": "BALANCE_CHF"})
        dep = pd.concat([dep, mv[["ENTITY_ID", "DEPOSIT_TYPE", "COUNTERPARTY_TYPE", "BALANCE_CHF"]]])
    g = dep.groupby(["ENTITY_ID", "DEPOSIT_TYPE", "COUNTERPARTY_TYPE"], as_index=False).BALANCE_CHF.sum()
    g = g[g.DEPOSIT_TYPE.isin(_assigned("DEPOSITS", "RUNOFF"))]
    g = g.merge(_dti_rules(), left_on=["DEPOSIT_TYPE", "COUNTERPARTY_TYPE"], right_on=["PRODUCT_TYPE", "COUNTERPARTY_SECTOR"])
    b = _expand(g, "DAYS_BASE", -g.BALANCE_CHF * g.RATE_BASE, int(g.DAYS_BASE.max()))
    s = _expand(g, "DAYS_STRESS", -g.BALANCE_CHF * g.RATE_STRESS, int(g.DAYS_STRESS.max()))
    return pd.concat([_flow(b.ENTITY_ID, b.DAY, "OUT", b.AMT, 0.0), _flow(s.ENTITY_ID, s.DAY, "OUT", 0.0, s.AMT)],
                     ignore_index=True)


def _drawdown(book: Book, d: date) -> pd.DataFrame:
    fx = Fx(book.snapshot("deposit_balances", d))
    c = _valid_contracts(book.snapshot("contracts", d))
    c = c[(c.UNDRAWN_CCY > 0) & c.PRODUCT_TYPE.isin(_assigned("CONTRACTS", "DRAWDOWN"))]
    c = c.assign(U=c.UNDRAWN_CCY * fx(c.CURRENCY)).merge(
        _dti_rules(), on=["PRODUCT_TYPE", "COUNTERPARTY_SECTOR"])
    per = c.groupby("ENTITY_ID").apply(lambda x: pd.Series({"B": (x.U * x.RATE_BASE).sum() / 30,
                                                             "S": (x.U * x.RATE_STRESS).sum() / 30}),
                                       include_groups=False)
    rows = [_flow(per.index, k, "OUT", -per.B.values, -per.S.values) for k in range(1, 31)]
    return pd.concat(rows, ignore_index=True)


def _secured(book: Book, d: date) -> pd.DataFrame:
    sf = book.snapshot("dti_secured_funding", d)
    rr = sf[sf.TRANSACTION_TYPE == "REVERSE_REPO"]
    br = sf[sf.TRANSACTION_TYPE == "BILATERAL_REPO"]
    return pd.concat([_flow(rr.ENTITY_ID, _days(rr.MATURITY_DATE, d), "INF", rr.NOMINAL_CHF, rr.NOMINAL_CHF),
                      _flow(br.ENTITY_ID, 1, "OUT", 0.0, -COLLATERAL_CALL * br.NOMINAL_CHF)], ignore_index=True)


@cache
def _plan_all(out: Path) -> pd.DataFrame:
    return pd.read_csv(out / "dti_planning.csv")


def _planning(book: Book, d: date) -> pd.DataFrame:
    p = _plan_all(book.out)
    p = p[p.AS_OF_DATE == d.isoformat()]
    stress = p.AMOUNT_CHF.where(p.PLAN_TYPE != "FUNDING_PLAN", 0.0)
    return _flow(p.ENTITY_ID, _days(p.VALUE_DATE, d), "DET", p.AMOUNT_CHF, stress)


def _corrections(book: Book, d: date) -> pd.DataFrame:
    k = book.csv("dti_corrections.csv")
    k = k[k.AS_OF_DATE == d.isoformat()]
    roots = k.LST_POSITION.map(_roots())
    return _flow(k.ENTITY_ID, _days(k.VALUE_DATE, d), roots, k.AMOUNT_CHF, k.AMOUNT_CHF)


def dti_days(flows: pd.DataFrame, entities: list[str] | None = None) -> int | None:
    """First projection day with cumulative stressed balance <= 0 (None = liquid for 365 days)."""
    f = flows if entities is None else flows[flows.ENTITY_ID.isin(entities)]
    daily = f.groupby("DAY").STRESS.sum().reindex(range(HORIZON + 1), fill_value=0.0).cumsum()
    hit = daily[daily <= 0]
    return int(hit.index[0]) if len(hit) else None


def dti_matrix(book: Book, dates: list[date], fixed_share: bool) -> pd.DataFrame:
    """Stressed DtI per entity (and GROUP) and reporting date; None -> 366."""
    rows = []
    ents = [e for e, _ in ref.entity_shares()]
    for d in dates:
        f = dti_flows(book, d, cash_shares(book, d, fixed_share))
        row = {"AS_OF_DATE": d, **{e: dti_days(f, [e]) for e in ents}, "GROUP": dti_days(f)}
        rows.append({k: (HORIZON + 1 if v is None else v) for k, v in row.items()})
    return pd.DataFrame(rows).set_index("AS_OF_DATE")


def cb_balance_after_corrections(book: Book, d: date, fixed_share: bool) -> dict[str, float]:
    """Central bank balance per entity on day 0 after POOL_CB_BALANCE corrections."""
    hold = book.snapshot("hqla_holdings", d)
    snb = hold[hold.ASSET_TYPE == "CASH_SNB"].MARKET_VALUE_CHF.sum()
    bal = {e: snb * s for e, s in cash_shares(book, d, fixed_share).items()}
    k = book.csv("dti_corrections.csv")
    for _, c in k[(k.AS_OF_DATE == d.isoformat()) & (k.LST_POSITION == "POOL_CB_BALANCE")].iterrows():
        bal[c.ENTITY_ID] = bal.get(c.ENTITY_ID, 0.0) + c.AMOUNT_CHF
    return bal


# ----------------------------------------------------------------------- checks
@dataclass
class Result:
    check_id: str
    title: str
    expected: str
    actual: str
    passed: bool


def _lcr_series(book: Book) -> pd.Series:
    est = book.csv("_generator_lcr_estimate.csv")
    return pd.Series(est.LCR.values, index=pd.to_datetime(est.AS_OF_DATE).dt.date).sort_index()


def _events(book: Book) -> dict[str, date]:
    ev = book.csv("lcr_events.csv")
    return {r.EVENT_ID: date.fromisoformat(r.EVENT_DATE) for r in ev.itertuples()}


def check_breach(lcr: pd.Series, events: dict[str, date]) -> Result:
    """DEMO-01: the September stress breach (funding cliff) is preserved and the only breach window."""
    s = events["EVT_02"]
    drop = lcr[s] - lcr[lcr.index < s].iloc[-1]
    below = lcr[lcr < 100]
    bdays = [x for x in below.index if x.weekday() < 5]
    window_ok = all(s <= x < events["EVT_03"] + timedelta(days=7) for x in below.index)
    lo, hi = LIMITS["breach_low"]
    ok = (-drop >= LIMITS["breach_min_drop_pp"] and len(bdays) >= LIMITS["breach_min_days"]
          and lo <= below.min() <= hi and window_ok)
    return Result("DEMO-01", "LCR breach preserved", f"drop>={LIMITS['breach_min_drop_pp']}pp, >={LIMITS['breach_min_days']} bdays<100, min {lo}-{hi}, one window",
                  f"drop {drop:.1f}pp, {len(bdays)} bdays<100, min {below.min() if len(below) else float('nan'):.1f}, one window {window_ok}", ok)


def _event_period(events: dict[str, date]) -> set[date]:
    """Event days and the two days after, plus the whole stress-to-recovery period (scripted, not noise)."""
    ev = set(events.values()) | {e + timedelta(days=k) for e in events.values() for k in range(1, 3)}
    return ev | {events["EVT_02"] + timedelta(days=k) for k in range((events["EVT_03"] - events["EVT_02"]).days + 7)}


def check_weekday_bias(lcr: pd.Series, events: dict[str, date]) -> Result:
    """DEMO-02: mean day-over-day change per weekday (event period excluded) within +-limit."""
    ev = _event_period(events)
    delta = lcr.diff().dropna()
    delta = delta[[d not in ev for d in delta.index]]
    by = delta.groupby([d.weekday() for d in delta.index]).mean()
    worst = by.abs().max()
    return Result("DEMO-02", "No weekday pattern in LCR", f"|mean d/d per weekday| <= {LIMITS['weekday_bias_pp']}pp",
                  " ".join(f"{'MTWTFSS'[k]}{v:+.2f}" for k, v in by.items()), worst <= LIMITS["weekday_bias_pp"])


def check_recovery(lcr: pd.Series, events: dict[str, date]) -> Result:
    """DEMO-03: recovery over several business days, no single step above the limit."""
    r = events["EVT_03"]
    steps = lcr.diff()[(lcr.index >= r) & (lcr.index <= r + timedelta(days=7))]
    return Result("DEMO-03", "Gradual recovery", f"max step <= {LIMITS['recovery_max_step_pp']}pp",
                  f"max step {steps.max():.1f}pp", steps.max() <= LIMITS["recovery_max_step_pp"])


def check_evt05(lcr: pd.Series, events: dict[str, date]) -> Result:
    """DEMO-04: the second withdrawal is a visible step."""
    e = events["EVT_05"]
    step = lcr[e] - lcr[lcr.index < e].iloc[-1]
    return Result("DEMO-04", "EVT_05 visible", f"step <= -{LIMITS['evt05_min_pp']}pp", f"step {step:+.1f}pp",
                  step <= -LIMITS["evt05_min_pp"])


def check_scenarios(book: Book, events: dict[str, date], as_of: date) -> Result:
    """DEMO-05: on the as-of date BASE ~135, Combined 100-105, stress scenarios >= 100; all < 100 in the breach."""
    now = scenario_lcr(book, as_of)
    breach = scenario_lcr(book, events["EVT_02"] + timedelta(days=3))
    clo, chi = LIMITS["combined_asof"]
    blo, bhi = LIMITS["base_asof"]
    ok = (blo <= now["BASE"] <= bhi and clo <= now["COMBINED"] <= chi
          and min(now.values()) >= 100 and max(breach.values()) < 100)
    fmt = " ".join(f"{k[:4]} {v:.1f}" for k, v in now.items())
    return Result("DEMO-05", "Scenario LCR on as-of date", f"BASE {blo}-{bhi}, COMB {clo}-{chi}, all>=100; breach all<100",
                  f"{fmt} | breach max {max(breach.values()):.1f}", ok)


def check_dti_stability(m: pd.DataFrame, dti_event_dates: set[date]) -> Result:
    """DEMO-06: entity DtI is stable day over day on reporting dates without an event (95th percentile and
    maximum of the absolute change), and no entity below the floor on the as-of date. DtI is a first-crossing
    measure: a single deal maturing can move it by weeks where the curve is flat, so the check bounds the
    distribution instead of every single move."""
    ents = [c for c in m.columns if c != "GROUP"]
    moves, worst = [], (0, "none")
    for prev, cur in zip(m.index, m.index[1:]):
        if cur in dti_event_dates or cur < m.index.max() - timedelta(days=90):
            continue
        for e in ents:
            jump = abs(int(m.at[cur, e]) - int(m.at[prev, e]))
            moves.append(jump)
            if jump > worst[0]:
                worst = (jump, f"{e} {prev}->{cur} {m.at[prev, e]}->{m.at[cur, e]}")
    s = pd.Series(moves, dtype=float)
    last = m.iloc[-1][ents]
    ok = (s.quantile(0.95) <= LIMITS["dti_jump_p95_days"] and s.max() <= LIMITS["dti_jump_max_days"]
          and (last >= LIMITS["dti_min_days_asof"]).all())
    return Result("DEMO-06", "Entity DtI stable",
                  f"p95 move <= {LIMITS['dti_jump_p95_days']}d, max <= {LIMITS['dti_jump_max_days']}d, as-of >= {LIMITS['dti_min_days_asof']}d",
                  f"p95 {s.quantile(0.95):.0f}d, max {worst[1]}; as-of min {int(last.min())}d ({last.idxmin()})", ok)


def check_cb_non_negative(book: Book, dates: list[date], fixed_share: bool) -> Result:
    """DEMO-07: central bank balance per entity after corrections >= 0."""
    worst = min((v, e, d) for d in dates for e, v in cb_balance_after_corrections(book, d, fixed_share).items())
    return Result("DEMO-07", "CB balance >= 0 after corrections", ">= 0", f"min {worst[0] / 1e6:.1f}M ({worst[1]} {worst[2]})",
                  worst[0] >= 0)


def check_one_book(book: Book, dates: list[date]) -> Result:
    """DEMO-08: LCR holdings and DtI securities are the same rows (ids, values) on every shared date."""
    bad = []
    for d in dates:
        h, s = book.snapshot("hqla_holdings", d), book.snapshot("dti_securities", d)
        if not len(h):
            continue
        hs = h[~h.ASSET_TYPE.str.startswith("CASH")].set_index("HOLDING_ID").MARKET_VALUE_CHF
        ss = s[s.WLK_CLUSTER != "X"].set_index("SECURITY_ID").MARKET_VALUE_CHF
        if set(hs.index) != set(ss.index) or (hs - ss.reindex(hs.index)).abs().max() > 0.01:
            bad.append(d)
    return Result("DEMO-08", "LCR holdings == DtI securities", "0 mismatching dates", f"{len(bad)} mismatching", not bad)


def run_checks(out: Path, fixed_share: bool = True) -> tuple[list[Result], pd.DataFrame]:
    book = Book(out)
    lcr, events = _lcr_series(book), _events(book)
    as_of = date.fromisoformat(book.csv("generation_run.csv").AS_OF_DATE.iloc[0])
    dti_dates = [d for d in book.dti_dates() if d >= as_of - timedelta(days=100)]
    m = dti_matrix(book, dti_dates, fixed_share)
    dti_ev = {date.fromisoformat(x) for x in book.csv("dti_events.csv").EVENT_DATE} | _event_period(events)
    results = [check_breach(lcr, events), check_weekday_bias(lcr, events), check_recovery(lcr, events),
               check_evt05(lcr, events), check_scenarios(book, events, as_of), check_dti_stability(m, dti_ev),
               check_cb_non_negative(book, dti_dates, fixed_share), check_one_book(book, dti_dates)]
    return results, m


def main() -> int:
    p = argparse.ArgumentParser(description="Offline demo-readiness checks of the liquidity book")
    p.add_argument("out_dir", type=Path)
    p.add_argument("--deposit-share", action="store_true", help="allocate cash by deposit share (engine before fix)")
    p.add_argument("--matrix", type=Path, help="write the DtI entity matrix to this CSV")
    a = p.parse_args()
    results, m = run_checks(a.out_dir, fixed_share=not a.deposit_share)
    if a.matrix:
        m.to_csv(a.matrix)
    for r in results:
        print(f"{'PASS' if r.passed else 'FAIL'}  {r.check_id}  {r.title:<34} expected {r.expected:<55} actual {r.actual}")
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
