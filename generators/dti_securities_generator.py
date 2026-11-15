"""
Collateral securities book of the bank (simulated ANUBIS APEL feed) - the ONE
securities master of the liquidity book. The LCR HQLA holdings (unencumbered
L1/L2A/L2B securities) and the DtI collateral pool are both derived from it,
so both engines value exactly the same positions.

Follows the Excel prototype (Daten_Wertpapiere / Daten_Zeitreihen):
- 15-40 securities per WLK cluster, booked at the legal entities.
- The liquidity value of a security is spread over its own cashflow dates
  (coupons, redemption), valued with the price of the data set:
      value_i = market value * cashflow_i / sum(cashflows)
  Basis = market value, stressed = basis * (1 - haircut). Cashflows beyond
  13 months are collapsed into one LATER row at maturity (bucket GT12M).
- Business-day dynamics: cluster price random walk, cash-neutral switch trades,
  maturities replaced by new paper, encumbrance changes, three showcase events.
  Purchases settle against the SNB reserve account (cash_delta, applied by the
  LCR book generator).

Also writes DtI business-day transactions (large corporate deposit movements,
loan new business), sized with the calibrated book scale (flow_scale).
"""

from __future__ import annotations

import math
import zlib
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import reference_loader as ref

PUBLIC_SECTOR = "Public sector"

# Issuer groups that several tables below refer to
COVERED_BONDS, EQUITIES, CREDIT_CLAIMS = "Covered bond issuers", "Listed equities", "Credit claims"

# WLK cluster -> generator parameters (issuer groups, coupon, daily price vol); HQLA category and haircut
# come from the reference data (LIQI_RAW_TB_DTI_WLK_CLUSTERS)
_WLK_PARAMS = {
    "II": (["Sovereigns"], 0.010, 0.0012),
    "III": (["Sovereigns", "Supranationals"], 0.015, 0.0015),
    "IV": (["Sub-sovereigns"], 0.012, 0.0018),
    "V": ([COVERED_BONDS], 0.014, 0.0020),
    "VI": (["Corporates"], 0.018, 0.0025),
    "VII": (["Corporates"], 0.026, 0.0030),
    "VIII": ([EQUITIES], 0.0, 0.0090),
    "IX": (["Corporates"], 0.040, 0.0040),
    "X": ([CREDIT_CLAIMS], 0.030, 0.0005),
    "XIa": (["Sovereigns"], 0.025, 0.0016),
    "XIb": (["Sovereigns"], 0.040, 0.0018),
}
_REF_WLK = ref.wlk_clusters()
# WLK cluster -> (HQLA category, haircut, issuer groups, coupon, daily price vol)
WLK = {w: _REF_WLK[w] + p for w, p in _WLK_PARAMS.items()}
WLK_COUNT = {"II": (25, 40), "III": (15, 22), "IV": (20, 35), "V": (18, 30), "VI": (15, 28), "VII": (15, 25),
             "VIII": (15, 20), "IX": (3, 5), "X": (4, 6), "XIa": (15, 25), "XIb": (15, 25)}
ISSUERS = {
    "Sovereigns": ["Swiss Confederation", "Federal Republic of Germany", "Republic of Austria", "Kingdom of Netherlands",
                   "United States Treasury"],
    "Supranationals": ["European Investment Bank", "World Bank", "Council of Europe Bank"],
    "Sub-sovereigns": ["Canton of Zurich", "Canton of Bern", "Canton of Geneva", "City of Zurich", "Canton of Vaud"],
    COVERED_BONDS: ["Pfandbriefbank schweiz. Hypothekarinstitute", "Pfandbriefzentrale der Kantonalbanken"],
    "Corporates": ["Nestle Finance", "Roche Holdings", "Novartis Finance", "ABB Finance", "Swisscom", "Holcim Finance"],
    EQUITIES: ["SMI constituent"],
    CREDIT_CLAIMS: ["SME loan pool"],
}
INDUSTRY = {"Sovereigns": PUBLIC_SECTOR, "Supranationals": PUBLIC_SECTOR, "Sub-sovereigns": PUBLIC_SECTOR,
            COVERED_BONDS: "Financials", EQUITIES: "Industrials", CREDIT_CLAIMS: "Financials"}
CORP_INDUSTRIES = ["Consumer", "Healthcare", "Healthcare", "Industrials", "Telecom", "Materials"]
CCY = {"XIa": "EUR", "XIb": "USD"}

ENTITY_SHARES = ref.entity_shares()

# Calibration (group totals in CHF). Short-term liquidity comes from treasury bill ladders (market value,
# length in calendar days): a fixed number of rungs (the next business days), rolled to the far end at
# maturity, so the pool size and inflow profile are the same on every reporting date and weekday.
# Every rung is booked at all legal entities by their share. Long bonds (13-120 months) mostly land in GT12M.
LADDERS = [(600e6, 20), (150e6, 10)]
LADDER_BILLS = 3  # bills maturing per business day and ladder (different WLK clusters)
LADDER_CLUSTERS = ["II", "IV", "III", "XIa", "V", "II"]
LONG_MV_CHF = 1_250e6
HORIZON_DAYS = 400  # cashflows beyond this are collapsed into one LATER row
REBALANCE_MOVES = 1  # intra-group collateral transfers per business day
REBALANCE_BAND = 0.10  # rebalance only when an entity is more than 10% off its buffer target


def crc(key: str) -> int:
    return zlib.crc32(key.encode("utf-8"))


def entity_for(key: str) -> str:
    x, acc = crc(key) % 100, 0
    for e, w in ENTITY_SHARES:
        acc += w
        if x < acc:
            return e
    return ENTITY_SHARES[-1][0]


def is_business_day(d: date, holidays: set) -> bool:
    return d.weekday() < 5 and d not in holidays


def next_business_day(d: date, holidays: set) -> date:
    """d itself when it is a business day, else the following business day (maturities never fall on holidays)."""
    while not is_business_day(d, holidays):
        d += timedelta(days=1)
    return d


ENTITY_WEIGHT = {e: w / 100.0 for e, w in ENTITY_SHARES}


def add_months(d: date, months: int) -> date:
    m = d.month - 1 + months
    y, m = d.year + m // 12, m % 12 + 1
    last = [31, 29 if y % 4 == 0 and (y % 100 or y % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    return date(y, m, min(d.day, last))


@dataclass
class Security:
    sid: str
    wlk: str
    entity: str
    issuer_group: str
    issuer: str
    currency: str
    nominal: float
    coupon: float
    freq: int  # coupons per year (0 = zero coupon / equity)
    maturity: date | None
    short: bool
    idio: float = 1.0
    encumbered: bool = False
    mobilised: bool = False
    isin: str = ""
    name: str = ""
    events: list[str] = field(default_factory=list)


class DtiSecuritiesGenerator:
    def __init__(self, first: date, last: date, holidays: set, seed: int = 4242, scenario: dict | None = None):
        self.rng = np.random.default_rng(seed)
        self.first, self.last = first, last
        self.holidays = holidays
        self.cfg = (scenario or ref.scenario())["dti"]
        self.seq = 0
        self.price: dict[str, float] = dict.fromkeys(WLK, 1.0)
        self.secs: list[Security] = []
        self.txns: list[dict] = []
        self.txn_seq = 0
        self.events: list[dict] = []
        self.cash_delta = 0.0   # settlement of purchases against SNB reserves, taken by the LCR book
        self.flow_scale = 1.0   # calibrated book scale for the DtI transaction amounts
        # booking share of the securities book per legal entity (default: CB share; the LCR book rebooks it to
        # the entities' liquidity need, see rebook)
        self.book_shares: list[tuple[str, float]] = [(e, w / 100.0) for e, w in ENTITY_SHARES]
        self.cash = 0.0  # central bank + vault cash of the group (allocated by the fixed CB share)
        self.planted = False
        bdays = [first + timedelta(days=i) for i in range((last - first).days + 1)]
        self.bdays = [d for d in bdays if is_business_day(d, holidays)]
        # showcase event dates (business days near the end of the window)
        self.evt_purchase = self.bdays[-self.cfg["securities_purchase"]["offset_business_days"]]
        self.evt_downgrade = self.bdays[-self.cfg["wlk_downgrade"]["offset_business_days"]]
        self._build(first)

    # ----------------------------------------------------------------- build
    def _new(self, wlk: str, d: date, nominal: float | None = None, entity: str | None = None) -> Security:
        rng = self.rng
        self.seq += 1
        sid = f"SEC-{self.seq:05d}"
        group = issuer_group_for(wlk, sid)
        issuer = ISSUERS[group][crc(sid) % len(ISSUERS[group])]
        if wlk == "VIII":
            mat, freq, coupon = None, 0, 0.0
        else:
            mat = next_business_day(add_months(d, int(rng.integers(13, 121))), self.holidays)
            freq = int(rng.choice([1, 1, 1, 2, 12]))
            coupon = max(0.0, WLK[wlk][3] + 0.004 * rng.standard_normal())
        if nominal is None:
            nominal = float(rng.lognormal(math.log(10e6), 0.6))
        sec = Security(sid, wlk, entity or self._weighted_entity(sid), group, issuer, CCY.get(wlk, "CHF"), nominal,
                       coupon, freq, mat, False, idio=float(1 + 0.01 * rng.standard_normal()))
        k = crc(sid + "enc") % 100
        sec.encumbered = wlk not in ("IX", "X") and k < 12
        sec.mobilised = not sec.encumbered and wlk not in ("IX", "X") and k < 45
        prefix = {"EUR": "DE", "USD": "US"}.get(sec.currency, "CH")
        sec.isin = f"{prefix}{crc(sid + 'isin') % 10**10:010d}"
        kind = "Share" if wlk == "VIII" else "Bond"
        sec.name = f"{issuer} {kind} {sec.coupon * 100:.2f}% {mat.isoformat() if mat else ''}".strip()
        self.secs.append(sec)
        return sec

    def _build(self, d: date):
        rng = self.rng
        for wlk, (lo, hi) in WLK_COUNT.items():
            for _ in range(int(rng.integers(lo, hi + 1))):
                self._new(wlk, d)
        self._scale()

    def _scale(self):
        """Scale the bond nominals so that the bond portfolio hits its calibrated market value."""
        group = [s for s in self.secs if s.wlk not in ("IX", "X")]
        mv = sum(s.nominal * self.price[s.wlk] * s.idio for s in group)
        for s in group:
            s.nominal *= LONG_MV_CHF / mv

    # -------------------------------------------------------------- dynamics
    def set_targets(self, need: dict[str, float], cash: float = 0.0):
        """Current buffer target per entity (share of the stressed outflow), used by the daily rebalancing. The
        target covers the whole buffer: central bank cash (allocated by the fixed CB share) plus free securities."""
        total = sum(need.values())
        self.book_shares = [(e, need.get(e, 0.0) / total) for e, _ in ENTITY_SHARES]
        self.cash = cash

    def rebook(self, need: dict[str, float], cash: float = 0.0):
        """Book the securities at the legal entities by their liquidity need (share of the stressed 30-day
        outflow): every entity holds its own buffer, so entity DtI follows the group instead of an id hash.
        Deterministic greedy fill (largest position first, to the entity with the largest gap to its target);
        positions pinned by a showcase event keep their entity."""
        self.set_targets(need, cash)
        free = sorted(self._free_book(), key=lambda s: (-self.mv(s), s.sid))
        gap = self._gaps(free)
        for s in free:
            gap[s.entity] += self.mv(s)
        for s in free:
            s.entity = _largest(gap)
            gap[s.entity] -= self.mv(s)

    def _free_book(self) -> list[Security]:
        """Free (unencumbered, HQLA-eligible) persistent securities that treasury may move between entities."""
        return [s for s in self.secs if not s.events and not s.encumbered and s.wlk not in ("IX", "X")]

    def _gaps(self, free: list[Security]) -> dict[str, float]:
        """Target minus booked buffer (CB cash share + free securities) per entity (positive = below target)."""
        pool = sum(self.mv(s) for s in free)
        gap = {e: w * (pool + self.cash) - ENTITY_WEIGHT[e] * self.cash for e, w in self.book_shares}
        for s in free:
            gap[s.entity] -= self.mv(s)
        return gap

    def _rebalance(self, moves: int = REBALANCE_MOVES):
        """Intra-group collateral management: treasury transfers up to `moves` free securities per business day
        from the entity furthest above its buffer target to the one furthest below, but only when that entity
        is more than REBALANCE_BAND off its target and the transfer reduces both gaps (no daily churn), so
        every entity keeps its own buffer through maturities and encumbrance changes."""
        for _ in range(moves):
            free = self._free_book()
            gap = self._gaps(free)
            pool = sum(self.mv(s) for s in free)
            give, take = _smallest(gap), _largest(gap)
            target = dict(self.book_shares)
            band = REBALANCE_BAND * (pool + self.cash)
            if gap[take] <= band * target[take] and -gap[give] <= band * target[give]:
                return
            fits = [s for s in free if s.entity == give and self.mv(s) <= min(-gap[give], gap[take])]
            if not fits:
                return
            max(fits, key=lambda s: (self.mv(s), s.sid)).entity = take

    def _weighted_entity(self, key: str) -> str:
        x, acc = (crc(key + "book") % 10_000) / 10_000, 0.0
        for e, w in self.book_shares:
            acc += w
            if x < acc:
                return e
        return self.book_shares[-1][0]

    def evolve(self, d: date, latest: date):
        """Business-day step of the securities book (called by the LCR book generator)."""
        self._evolve(d)
        if not self.planted and d >= latest - timedelta(days=40):
            self.plant_maturity(latest)
            self.planted = True

    def take_cash(self) -> float:
        """Cash settlement accumulated since the last call (positive = cash received)."""
        c, self.cash_delta = self.cash_delta, 0.0
        return c

    def mv(self, s: Security) -> float:
        return s.nominal * (s.idio if s.short else self.price[s.wlk] * s.idio)

    def valued(self, d: date):
        """All positions on date d (persistent book + treasury bill ladders) with market value in CHF."""
        return [(s, self.mv(s)) for s in self.secs + self._ladder(d)]

    def shock_prices(self, shock_by_wlk: dict[str, float]):
        for w, x in shock_by_wlk.items():
            self.price[w] *= 1 - x

    def recover_prices(self, share: float):
        for w in self.price:
            self.price[w] = 1 - (1 - self.price[w]) * share

    def _evolve(self, d: date):
        rng = self.rng
        for w, (_, _, _, _, vol) in WLK.items():
            self.price[w] *= 1 + vol * rng.standard_normal()
        for s in self.secs:
            s.idio *= 1 + 0.0008 * rng.standard_normal()
        # maturities: drop and replace with new paper of the same cluster and sleeve (stationary profile)
        matured = [s for s in self.secs if s.maturity and s.maturity <= d]
        for s in matured:
            self.secs.remove(s)
            self._new(s.wlk, d, nominal=s.nominal, entity=s.entity)
        # switch trades: 1-2 per business day, sell a position and reinvest the proceeds in another cluster
        # (cash-neutral, so the pool size is stationary and the LCR calibration holds)
        tradable = [w for w in WLK if w not in ("IX", "X")]
        for _ in range(int(rng.integers(1, 3))):
            sell_wlk = str(rng.choice(tradable))
            buy_wlk = str(rng.choice(tradable))
            pool = [s for s in self.secs if s.wlk == sell_wlk and not s.events]
            if len(pool) <= 15:
                continue
            old = pool[int(rng.integers(0, len(pool)))]
            proceeds = self.mv(old)
            self.secs.remove(old)
            # the booking entity keeps its collateral: the switch is a trade, not a transfer between entities
            new = self._new(buy_wlk, d, nominal=proceeds / self.price[buy_wlk], entity=old.entity)
            self.cash_delta += proceeds - self.mv(new)
        # encumbrance changes: one security pledged (repo) and one released per day, so the share stays stable;
        # independent of the entity booking (the daily rebalancing keeps every entity's free buffer on target)
        eligible = [s for s in self.secs if s.wlk not in ("IX", "X") and not s.events]
        for pledge in (True, False):
            cand = [s for s in eligible if s.encumbered != pledge]
            if cand:
                s = cand[int(rng.integers(0, len(cand)))]
                s.encumbered = pledge
                s.mobilised = s.mobilised and not pledge
        self._rebalance()
        self._apply_events(d)
        self._deposit_and_loan_txns(d)

    def _apply_events(self, d: date):
        if d == self.evt_purchase:
            p = self.cfg["securities_purchase"]
            # configured size, not scaled with the book: one visible purchase, not a restructuring of an entity
            s = self._new(p["wlk"], d, nominal=p["nominal_chf"], entity=p["entity"])
            s.events.append("DTI_05")
            s.encumbered = s.mobilised = False  # bought for the liquidity buffer: free collateral
            amt = self.mv(s)
            self.cash_delta -= amt  # paid from SNB reserves: HQLA unchanged, cash moves into WLK II
            self.events.append({"EVENT_ID": "DTI_05", "EVENT_DATE": d.isoformat(), "ENTITY_ID": p["entity"],
                                    "EVENT_TYPE": "SECURITY_PURCHASE", "POSITION_ID": s.sid, "AMOUNT_CHF": round(amt, 2),
                                    "DESCRIPTION": f"Treasury buys CHF {amt / 1e6:.0f}M {s.issuer} bonds (WLK {p['wlk']}) "
                                                "from SNB reserves: largest securities delta"})
        if d == self.evt_downgrade:
            g = self.cfg["wlk_downgrade"]
            cand = sorted([s for s in self.secs if s.wlk == g["from"]], key=lambda s: -s.nominal)
            s = cand[0]
            s.wlk = g["to"]
            s.events.append("DTI_06")
            self.events.append({"EVENT_ID": "DTI_06", "EVENT_DATE": d.isoformat(), "ENTITY_ID": s.entity,
                                    "EVENT_TYPE": "WLK_DOWNGRADE", "POSITION_ID": s.sid, "AMOUNT_CHF": round(s.nominal, 2),
                                    "DESCRIPTION": f"{s.issuer} downgraded: security moves from WLK {g['from']} to {g['to']} "
                                                f"(haircut {WLK[g['from']][1]:.0%} -> {WLK[g['to']][1]:.0%})"})

    def plant_maturity(self, latest: date):
        """DTI_07: one canton bond redeems inside W2 of the latest reporting date (earlier dates keep the old maturity)."""
        m = self.cfg["security_maturity"]
        cand = sorted([s for s in self.secs if s.wlk == m["wlk"] and not s.events], key=lambda s: -s.nominal)
        s = cand[0]
        s.events.append("DTI_07")
        s.maturity_override = latest + timedelta(days=m["days_after_latest"])  # type: ignore[attr-defined]
        s.override_from = latest  # type: ignore[attr-defined]
        self.events.append({"EVENT_ID": "DTI_07", "EVENT_DATE": latest.isoformat(), "ENTITY_ID": s.entity,
                                "EVENT_TYPE": "SECURITY_MATURITY", "POSITION_ID": s.sid, "AMOUNT_CHF": round(s.nominal, 2),
                                "DESCRIPTION": f"{s.issuer} bond (WLK IV) redeems on {s.maturity_override.isoformat()}: "
                                            "liquidity value moves from > 12 months into bucket W2"})
        return s

    # ---------------------------------------------------- DtI transactions
    def _txn_entity(self) -> tuple[str, float]:
        """Booking entity of a business-day transaction and its size factor: tickets scale with the entity share,
        so a movement is a few percent of the entity's book (a CHF 40M ticket never hits the smallest entity)."""
        x = crc(f"TXN-{self.txn_seq:06d}") % 100
        acc = 0
        for e, w in ENTITY_SHARES:
            acc += w
            if x < acc:
                return e, ENTITY_WEIGHT[e]
        e = ENTITY_SHARES[-1][0]
        return e, ENTITY_WEIGHT[e]

    def _deposit_and_loan_txns(self, d: date):
        """Large corporate deposit movements (temporary) and loan new business on each business day."""
        rng = self.rng
        for _ in range(int(rng.integers(3, 7))):
            self.txn_seq += 1
            entity, size = self._txn_entity()
            amt = float(rng.uniform(2e6, 15e6)) * (1 if rng.random() < 0.5 else -1) * self.flow_scale * size
            dtype = str(rng.choice(["CORPORATE_OPERATIONAL", "CORPORATE_NON_OPERATIONAL", "FINANCIAL_INSTITUTION"]))
            self.txns.append({"TXN_ID": f"TXN-{self.txn_seq:06d}", "TXN_TYPE": "DEPOSIT_MOVEMENT",
                                  "ENTITY_ID": entity, "PRODUCT_TYPE": dtype,
                                  "COUNTERPARTY_TYPE": "FINANCIAL_INSTITUTION" if dtype == "FINANCIAL_INSTITUTION" else "CORPORATE",
                                  "APPLICATION_ID": "EIN", "AKTIV_PASSIV": "P", "BOOK_DATE": d,
                                  "END_DATE": d + timedelta(days=int(rng.integers(7, 45))), "AMOUNT_CHF": amt,
                                  "RATE": 0.0, "IS_NEW_BUSINESS": False})
        for _ in range(int(rng.integers(0, 3))):
            self.txn_seq += 1
            entity, size = self._txn_entity()
            corp = rng.random() < 0.6
            self.txns.append({"TXN_ID": f"TXN-{self.txn_seq:06d}", "TXN_TYPE": "NEW_LOAN",
                                  "ENTITY_ID": entity,
                                  "PRODUCT_TYPE": "LOAN_CORP" if corp else "LOAN_RETAIL",
                                  "COUNTERPARTY_TYPE": "CORPORATE" if corp else "RETAIL",
                                  "APPLICATION_ID": "KRE" if corp else "HYP", "AKTIV_PASSIV": "A", "BOOK_DATE": d,
                                  "END_DATE": d + timedelta(days=int(rng.integers(30, 181))),
                                  "AMOUNT_CHF": float(rng.uniform(1e6, 5e6) if corp else rng.uniform(0.2e6, 1e6))
                                  * self.flow_scale * size * len(ENTITY_SHARES),
                                  "RATE": float(0.02 + 0.01 * rng.random()), "IS_NEW_BUSINESS": True})

    def txn_rows(self, d: date) -> list[dict]:
        """Transactions alive on reporting date d (booked on or before d, not yet ended)."""
        rows = []
        for t in self.txns:
            if t["BOOK_DATE"] <= d < t["END_DATE"]:
                rows.append({"TXN_ID": t["TXN_ID"], "AS_OF_DATE": d.isoformat(), "ENTITY_ID": t["ENTITY_ID"],
                                 "TXN_TYPE": t["TXN_TYPE"], "PRODUCT_TYPE": t["PRODUCT_TYPE"],
                                 "COUNTERPARTY_TYPE": t["COUNTERPARTY_TYPE"], "APPLICATION_ID": t["APPLICATION_ID"],
                                 "AKTIV_PASSIV": t["AKTIV_PASSIV"], "BOOK_DATE": t["BOOK_DATE"].isoformat(),
                                 "END_DATE": t["END_DATE"].isoformat(), "AMOUNT_CHF": round(t["AMOUNT_CHF"], 2),
                                 "INTEREST_RATE": round(t["RATE"], 5), "IS_NEW_BUSINESS": t["IS_NEW_BUSINESS"]})
        return rows

    # ------------------------------------------------------------- snapshots
    @staticmethod
    def _maturity(s: Security, d: date) -> date | None:
        o = getattr(s, "maturity_override", None)
        return o if o and d >= s.override_from else s.maturity

    def _cashflows(self, s: Security, d: date):
        mat = self._maturity(s, d)
        if mat is None:  # equity: liquid at the end of the horizon via sale, shown as LATER
            return [(d + timedelta(days=HORIZON_DAYS + 1), "LATER", 1.0)]
        flows = []
        if s.freq:
            step = 12 // s.freq
            cd = mat
            while cd > d:
                flows.append((cd, "COUPON", s.nominal * s.coupon / s.freq))
                cd = add_months(cd, -step)
        flows.append((mat, "REDEMPTION", s.nominal))
        flows = sorted(f for f in flows if f[0] > d)
        horizon = d + timedelta(days=HORIZON_DAYS)
        near = [f for f in flows if f[0] <= horizon]
        far = sum(f[2] for f in flows if f[0] > horizon)
        if far:
            near.append((mat, "LATER", far))
        return near

    def _rungs(self, d: date, n: int) -> list[date]:
        """The next n business days after d (maturity dates of the ladder rungs)."""
        out, m = [], d
        while len(out) < n:
            m += timedelta(days=1)
            if is_business_day(m, self.holidays):
                out.append(m)
        return out

    def _ladder(self, d: date) -> list[Security]:
        """Treasury bill ladders on reporting date d: a fixed number of rungs (the next business days, about
        5/7 of the ladder length), LADDER_BILLS bills per rung (one per cluster), each booked at every legal
        entity by its share. Ids are keyed by maturity date and entity, so a bill keeps its id, entity and nominal
        on every reporting date until it matures - the ladder rolls without changing size, shape or entity mix
        on any weekday."""
        out = []
        for li, (mv, days) in enumerate(LADDERS):
            rungs = self._rungs(d, round(days * 5 / 7))
            for m in rungs:
                for k in range(LADDER_BILLS):
                    out += self._bills(li, k, m, d, mv / len(rungs) / LADDER_BILLS)
        return out

    def _bills(self, li: int, k: int, m: date, d: date, nominal: float) -> list[Security]:
        wlk = LADDER_CLUSTERS[(k + li) % len(LADDER_CLUSTERS)]
        key = f"BIL-{li}{k}-{m:%Y%m%d}"
        group = issuer_group_for(wlk, key)
        issuer = ISSUERS[group][crc(key) % len(ISSUERS[group])]
        out = []
        for e, w in self.book_shares:
            s = Security(f"{key}-{e[-2:]}", wlk, e, group, issuer, CCY.get(wlk, "CHF"), nominal * w, 0.0, 0, m, True)
            s.idio = 1 - 0.005 * (m - d).days / 365  # discount paper, pulls to par
            s.isin = f"CH{crc(key + 'isin') % 10**10:010d}"
            s.name = f"{issuer} Bill {m.isoformat()}"
            out.append(s)
        return out

    def snapshot(self, d: date, run_id: str = ""):
        sec_rows, cf_rows = [], []
        for s, mv in self.valued(d):
            hq, hc = WLK[s.wlk][0], WLK[s.wlk][1]
            px = mv / s.nominal if s.nominal else 0.0
            mat = self._maturity(s, d)
            sec_rows.append({"SECURITY_ID": s.sid, "AS_OF_DATE": d.isoformat(), "ENTITY_ID": s.entity, "ISIN": s.isin,
                                 "SECURITY_NAME": s.name, "ISSUER": s.issuer, "ISSUER_GROUP": s.issuer_group,
                                 "INDUSTRY": INDUSTRY.get(s.issuer_group, CORP_INDUSTRIES[crc(s.sid) % len(CORP_INDUSTRIES)]),
                                 "CURRENCY": s.currency, "WLK_CLUSTER": s.wlk, "HQLA_CATEGORY": hq, "HAIRCUT": hc,
                                 "NOMINAL_CHF": round(s.nominal, 2), "COUPON_RATE": round(s.coupon, 5), "COUPON_FREQ": s.freq,
                                 "MATURITY_DATE": mat.isoformat() if mat else None, "PRICE": round(px * 100, 4),
                                 "MARKET_VALUE_CHF": round(mv, 2), "COLLATERAL_VALUE_CHF": round(mv * (1 - hc), 2),
                                 "IS_ENCUMBERED": s.encumbered, "IS_MOBILISED": s.mobilised,
                                 "IS_SHORT_TERM": s.short, "RUN_ID": run_id})
            flows = self._cashflows(s, d)
            total = sum(f[2] for f in flows) or 1.0
            for cf_date, cf_type, amt in flows:
                base = mv * amt / total
                cf_rows.append({"AS_OF_DATE": d.isoformat(), "SECURITY_ID": s.sid, "CF_DATE": cf_date.isoformat(),
                                    "CF_TYPE": cf_type, "CF_NOMINAL_CHF": round(amt, 2), "BASE_VALUE_CHF": round(base, 2),
                                    "STRESSED_VALUE_CHF": round(base * (1 - hc), 2)})
        return sec_rows, cf_rows

    def write(self, d: date, out_dir: Path, run_id: str = "") -> dict:
        """Write the securities, security cashflows and DtI transactions snapshot of reporting date d."""
        tag = d.strftime("%Y%m%d")
        sec_rows, cf_rows = self.snapshot(d, run_id)
        txn = self.txn_rows(d)
        pd.DataFrame(sec_rows).to_csv(out_dir / f"dti_securities_{tag}.csv", index=False)
        pd.DataFrame(cf_rows).to_csv(out_dir / f"dti_security_cashflows_{tag}.csv", index=False)
        pd.DataFrame(txn, columns=["TXN_ID", "AS_OF_DATE", "ENTITY_ID", "TXN_TYPE", "PRODUCT_TYPE",
                                   "COUNTERPARTY_TYPE", "APPLICATION_ID", "AKTIV_PASSIV", "BOOK_DATE",
                                   "END_DATE", "AMOUNT_CHF", "INTEREST_RATE", "IS_NEW_BUSINESS"]
                     ).to_csv(out_dir / f"dti_transactions_{tag}.csv", index=False)
        return {"securities": len(sec_rows), "cashflows": len(cf_rows), "transactions": len(txn)}


def issuer_group_for(wlk: str, sid: str) -> str:
    groups = WLK[wlk][2]
    return groups[crc(sid) % len(groups)]


def _largest(gap: dict[str, float]) -> str:
    """Entity with the largest gap to its buffer target (ties: highest id, deterministic)."""
    return max(gap.items(), key=lambda kv: (kv[1], kv[0]))[0]


def _smallest(gap: dict[str, float]) -> str:
    """Entity furthest above its buffer target (ties: lowest id, deterministic)."""
    return min(gap.items(), key=lambda kv: (kv[1], kv[0]))[0]
