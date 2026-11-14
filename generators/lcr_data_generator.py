#!/usr/bin/env python3
"""
FINMA LCR + DtI Liquidity Book Generator (one persistent, calibrated book)
==========================================================================
Generates a deterministic liquidity book for a Swiss bank and writes daily
snapshots for the LCR showcase and the DtI / Stichtagsvergleich (UC2).

ONE book feeds both engines:
- Counterparties, deposits and contracts are created once and evolve day by day.
- The securities book (dti_securities_generator.py) is the only collateral book:
  the LCR HQLA holdings are its unencumbered L1/L2A/L2B securities (same ids,
  same market values) plus SNB reserves and vault cash; the DtI collateral pool
  values the very same rows. Encumbered securities are not HQLA in either view.
- One simulation runs from the first DtI month-end to --as-of-date, so the DtI
  month-end history and the LCR window are snapshots of the same book.

Dynamics
--------
- Deposits follow a mean-reverting AR(1) process; net deposit flows and security
  purchases settle against the SNB reserve account (balance-sheet identity).
- Security prices follow small random walks; switch trades are cash-neutral.
- Loans and interbank contracts roll over at maturity.
- Weekends are flat; FX uses the daily end-of-day rate.

Calibration: the wholesale client book (corporate / FI deposits and contracts)
is scaled so the BASE LCR on the first simulated day equals --target-lcr on the
given securities pool. Event amounts scale with the book.

Showcase events and DtI control feeds are configured in
generators/scenarios/lcr_dti_showcase.json; reference data (holidays, haircuts,
run-off rates, entities) comes from reference_data/liqi (also seeded by
post_deploy.sql). Same --seed + --as-of-date => byte-identical output.

Regulatory basis: FINMA Liquidity Ordinance (LiqV) / Circular 2015/2, Basel III LCR.
"""

import argparse
import bisect
import hashlib
import json
import math
import re
import zlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import reference_loader as ref
from dti_securities_generator import CORP_INDUSTRIES as SEC_CORP_INDUSTRIES
from dti_securities_generator import (
    COVERED_BONDS,
    CREDIT_CLAIMS,
    ISSUERS,
    WLK,
    DtiSecuritiesGenerator,
    is_business_day,
)
from dti_securities_generator import INDUSTRY as SEC_INDUSTRY

# ---------------------------------------------------------------------------
# Reference data (single source reference_data/liqi, also seeded by post_deploy.sql)
# ---------------------------------------------------------------------------
ZURICH_HOLIDAYS = ref.holidays()
HQLA_TYPES = ref.hqla_types()            # asset_type: (level, haircut_factor)
DEPOSIT_TYPES = ref.deposit_types()      # deposit_type: (counterparty_type, base_run_off, allows_discount, is_operational)
CONTRACT_RULES = ref.contract_rules()    # (product, sector): (ASSET | FACILITY | LIABILITY, BASE rate)

INFLOW_CAP = 0.75
L2B_CAP_RATIO = 15.0 / 85.0
L2_CAP_RATIO = 2.0 / 3.0

FALLBACK_FX_TO_CHF = {"CHF": 1.0, "EUR": 0.94, "USD": 0.80, "GBP": 1.07}
GENERATOR_VERSION = "liq-book-3"

# ---------------------------------------------------------------------------
# DtI / Stichtagsvergleich (UC2) attributes - simulated ANUBIS / FRISCO / APEL /
# SPOT fields, derived deterministically from ids (crc32).
# ---------------------------------------------------------------------------
ENTITY_SHARES = ref.entity_shares()
ENTITY_WEIGHT = {e: w / 100.0 for e, w in ENTITY_SHARES}

# product -> (application, originating front system)
CONTRACT_APPS = {"LOAN_RETAIL": ("HYP", "RETAIL_CORE"), "LOAN_CORP": ("KRE", "CORP_LENDING"),
                 "CREDIT_FACILITY": ("KRE", "CORP_LENDING"), "LIQUIDITY_FACILITY": ("KRE", "CORP_LENDING"),
                 "INTERBANK_LOAN": ("GEL", "TREASURY_TMS"), "INTERBANK_BORROWING": ("GEL", "TREASURY_TMS"),
                 "NEW_DERIVATIVE_X": ("DER", "TREASURY_TMS")}

# WLK cluster (internal collateral cluster, APEL) -> haircut
WLK_HAIRCUT = {w: hc for w, (_, hc) in ref.wlk_clusters().items()}
# WLK cluster of a pool security -> LCR asset type (LIQI_RAW_TB_HQLA_ELIGIBILITY) and indicative rating.
# Credit claims (WLK X) are SNB collateral but not securities: they stay out of the LCR holdings.
SEC_ASSET_TYPE = {"II": "GOVT_BOND_CHF", "III": "GOVT_BOND_FOREIGN", "XIa": "GOVT_BOND_FOREIGN",
                  "XIb": "GOVT_BOND_FOREIGN", "IV": "CANTON_BOND", "V": "COVERED_BOND", "VI": "CORPORATE_BOND_AA",
                  "VII": "CORPORATE_BOND_A_BBB", "VIII": "EQUITY_SMI", "IX": "CORPORATE_BOND_A_BBB"}
SEC_RATING = {"II": "AAA", "III": "AAA", "XIa": "AA+", "XIb": "AA+", "IV": "AA", "V": "AAA", "VI": "AA-",
              "VII": "BBB+", "VIII": None, "IX": "BB+"}
SECURED_TYPE = {"II": "SNB_REPO", "IV": "SNB_REPO", "V": "GC_POOLING", "VI": "BILATERAL_REPO",
                "VII": "BILATERAL_REPO", "XIa": "BILATERAL_REPO", "XIb": "BILATERAL_REPO"}
SECURED_ELIGIBLE = {"SNB_REPO": "WLK I-V (SNB GC basket)", "GC_POOLING": "WLK II-VI (SIX GC basket)",
                    "BILATERAL_REPO": "WLK II-VII and XIa/XIb", "REVERSE_REPO": "WLK II received"}
PORTFOLIOS = ["TREASURY_LIQ", "ALM_BUFFER", "TREASURY_INV"]
CUSTODIAN = {"EUR": "EUROCLEAR", "USD": "CLEARSTREAM"}  # others: SIX SIS

# consistency of the two reference views: WLK category == regulatory level of the mapped asset type
for _w, _t in SEC_ASSET_TYPE.items():
    assert _w == "IX" or WLK[_w][0] == HQLA_TYPES[_t][0], (_w, _t)


def crc(key: str) -> int:
    return zlib.crc32(key.encode("utf-8"))


def entity_for(key: str) -> str:
    x, acc = crc(key) % 100, 0
    for e, w in ENTITY_SHARES:
        acc += w
        if x < acc:
            return e
    return ENTITY_SHARES[-1][0]


def issuer_id(name: str) -> str:
    return "ISS_" + re.sub(r"[^A-Z0-9]+", "_", name.upper()).strip("_")


CORP_PREFIX = ["Alpen", "Rhein", "Helvetia", "Jura", "Limmat", "Aare", "Gotthard", "Saentis",
               "Pilatus", "Rigi", "Emme", "Reuss", "Thur", "Linth", "Bodensee", "Leman",
               "Ticino", "Engadin", "Matterhorn", "Eiger"]
CORP_SUFFIX = ["Bau", "Logistik", "Pharma", "Tech", "Energie", "Immobilien", "Handel",
               "Maschinen", "Uhren", "Food", "Chemie", "Transport", "Medtech", "Textil",
               "Druck", "Holz", "Metall", "Agrar", "Software", "Verpackung"]
CORP_FORM = ["AG", "Holding AG", "GmbH", "SA", "Group AG"]
BANK_NAMES = ["Kantonalbank Nord", "Kantonalbank Sued", "Privatbank Zuerich", "Bank am See",
              "Regionalbank Mittelland", "Handelsbank Basel", "Genfer Privatbank",
              "Alpine Clearing Bank", "Euro Interbank AG", "Atlantic Securities Bank",
              "Raiffeisen Region West", "Hypothekarbank Ost", "Lombard Bank Lugano",
              "Stadtbank Bern", "Nordic Liquidity Bank", "Banque Romande",
              "Merchant Bank London", "Frankfurter Geschaeftsbank", "Zentralbank Partner AG",
              "Valiant-Style Regionalbank", "Insurance Treasury AG", "Pension Fund Alpha",
              "Asset Manager Beta", "Securities Dealer Gamma", "Clearing House Delta"]


@dataclass
class Event:
    event_id: str
    event_date: date
    event_type: str
    description: str
    counterparty_id: str | None = None
    contract_id: str | None = None
    amount_chf: float = 0.0
    expected_direction: str = "DOWN"
    expected_min_delta_pp: float = 1.0


def next_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


class FXTable:
    """CCY->CHF rates derived from the generated fx_rates files (USD base, cross via USD)."""

    def __init__(self, fx_dir: Path | None):
        self.by_date: dict[date, dict[str, float]] = {}
        if fx_dir and fx_dir.exists():
            for f in sorted(fx_dir.glob("fx_rates_*.csv")):
                df = pd.read_csv(f)
                usd = df[df["from_currency"] == "USD"].set_index("to_currency")["mid_rate"].to_dict()
                if "CHF" not in usd:
                    continue
                d = datetime.strptime(str(df["date"].iloc[0]), "%Y-%m-%d").date()
                rates = {"CHF": 1.0, "USD": float(usd["CHF"])}
                for ccy, mid in usd.items():
                    if ccy not in ("CHF",):
                        rates[ccy] = float(usd["CHF"]) / float(mid)
                self.by_date[d] = rates
        self.dates = sorted(self.by_date)

    def rate(self, ccy: str, d: date) -> float | None:
        if ccy == "CHF":
            return 1.0
        if not self.dates:
            return FALLBACK_FX_TO_CHF.get(ccy)
        # Daily EOD rates exist for every date; fallback to last date <= d only for gaps
        i = bisect.bisect_right(self.dates, d)
        rates = self.by_date[self.dates[i - 1]] if i else self.by_date[self.dates[0]]
        return rates.get(ccy, FALLBACK_FX_TO_CHF.get(ccy))


class LCRBookGenerator:
    """Persistent, calibrated liquidity book (client book + securities book) with deterministic events."""

    def __init__(self, customer_ids: list[str], start: date, end: date, seed: int, target_lcr: float, fx: FXTable,
                 sec: DtiSecuritiesGenerator, scenario: dict, with_events: bool = True, scale_adjust: float = 1.0):
        self.rng = np.random.default_rng(seed)
        self.scale_adjust = scale_adjust
        self.customer_ids = customer_ids
        self.start = start
        self.end = end
        self.days = (end - start).days + 1
        self.target_lcr = target_lcr
        self.fx = fx
        self.sec = sec
        self.cfg = scenario
        self.ev_cfg = scenario["lcr_events"]
        self.with_events = with_events
        self.counterparties: list[dict] = []
        self.deposits: list[dict] = []
        self.holdings: list[dict] = []   # cash only; securities come from the securities book
        self.contracts: list[dict] = []
        self.events: list[Event] = []
        self.scale = 1.0
        self._build_book()
        self._plan_events()
        self._calibrate()

    # ------------------------------------------------------------------ book
    def _build_book(self):
        rng = self.rng
        # Retail counterparties = real customers
        for cid in self.customer_ids:
            self.counterparties.append({"COUNTERPARTY_ID": cid, "COUNTERPARTY_NAME": f"Customer {cid}",
                                            "SECTOR": "RETAIL", "COUNTRY": "CH", "RATING": None,
                                            "CUSTOMER_ID": cid, "BUSINESS_UNIT": "RETAIL"})
        # Corporates / SMEs
        names = set()
        for i in range(260):
            while True:
                n = f"{rng.choice(CORP_PREFIX)} {rng.choice(CORP_SUFFIX)} {rng.choice(CORP_FORM)}"
                if n not in names:
                    names.add(n)
                    break
            sector = "CORPORATE" if i < 160 else "SME"
            self.counterparties.append({"COUNTERPARTY_ID": f"CP_CORP_{i+1:04d}", "COUNTERPARTY_NAME": n,
                                            "SECTOR": sector, "COUNTRY": str(rng.choice(["CH"] * 8 + ["DE", "AT"])),
                                            "RATING": str(rng.choice(["A", "A-", "BBB+", "BBB", "BBB-", "BB+"])),
                                            "CUSTOMER_ID": None, "BUSINESS_UNIT": "CORPORATE"})
        # Event counterparties with fixed names
        self.counterparties.append({"COUNTERPARTY_ID": "CP_CORP_EVT1", "COUNTERPARTY_NAME": "Alpenbau Holding AG",
                                        "SECTOR": "CORPORATE", "COUNTRY": "CH", "RATING": "BBB+", "CUSTOMER_ID": None,
                                        "BUSINESS_UNIT": "CORPORATE"})
        self.counterparties.append({"COUNTERPARTY_ID": "CP_CORP_EVT2", "COUNTERPARTY_NAME": "Rhein Logistik Group AG",
                                        "SECTOR": "CORPORATE", "COUNTRY": "CH", "RATING": "A-", "CUSTOMER_ID": None,
                                        "BUSINESS_UNIT": "CORPORATE"})
        # Financial institutions
        for i, n in enumerate(BANK_NAMES):
            self.counterparties.append({"COUNTERPARTY_ID": f"CP_FI_{i+1:03d}", "COUNTERPARTY_NAME": n,
                                            "SECTOR": "FINANCIAL", "COUNTRY": str(rng.choice(["CH"] * 6 + ["DE", "GB", "FR", "LU"])),
                                            "RATING": str(rng.choice(["AA", "AA-", "A+", "A", "A-"])),
                                            "CUSTOMER_ID": None, "BUSINESS_UNIT": "TREASURY"})
        # Issuers: central bank, own vault and every issuer of the securities book
        self.counterparties.append({"COUNTERPARTY_ID": "ISS_SNB", "COUNTERPARTY_NAME": "Swiss National Bank",
                                        "SECTOR": "CENTRAL_BANK", "COUNTRY": "CH", "RATING": None, "CUSTOMER_ID": None,
                                        "BUSINESS_UNIT": "TREASURY"})
        self.counterparties.append({"COUNTERPARTY_ID": "ISS_VAULT", "COUNTERPARTY_NAME": "Own vault cash",
                                        "SECTOR": "CENTRAL_BANK", "COUNTRY": "CH", "RATING": None, "CUSTOMER_ID": None,
                                        "BUSINESS_UNIT": "TREASURY"})
        sector = {"Sovereigns": "SOVEREIGN", "Supranationals": "SOVEREIGN", "Sub-sovereigns": "SOVEREIGN",
                  COVERED_BONDS: "FINANCIAL", CREDIT_CLAIMS: "FINANCIAL"}
        for group, issuers in ISSUERS.items():
            for n in issuers:
                self.counterparties.append({"COUNTERPARTY_ID": issuer_id(n), "COUNTERPARTY_NAME": n,
                                                "SECTOR": sector.get(group, "CORPORATE"), "COUNTRY": "CH", "RATING": None,
                                                "CUSTOMER_ID": None, "BUSINESS_UNIT": "TREASURY"})

        # ---- deposits: retail (savings + current), corporate, FI
        for cid in self.customer_ids:
            n_acc = int(rng.choice([1, 2, 2, 3]))
            for k in range(n_acc):
                bal = float(rng.lognormal(math.log(25_000), 0.9))
                bal = min(bal, 2_000_000)
                insured = bal <= 100_000
                dtype = ("RETAIL_STABLE_INSURED" if insured and rng.random() < 0.7 else
                         "RETAIL_STABLE" if rng.random() < 0.75 else "RETAIL_LESS_STABLE")
                seg = "MASS" if bal < 50_000 else "AFFLUENT" if bal < 250_000 else "PRIVATE"
                bu = "PRIVATE_BANKING" if seg == "PRIVATE" else "RETAIL"
                self.deposits.append(self._new_deposit(f"{cid}_DEP_{k+1:02d}", cid, dtype,
                                                       str(rng.choice(["CHF"] * 17 + ["EUR"] * 2 + ["USD"])),
                                                       bal, seg, bu, sigma=0.004))
        corp_cps = [c for c in self.counterparties if c["SECTOR"] in ("CORPORATE", "SME") and c["COUNTERPARTY_ID"].startswith("CP_CORP_")
                    and "EVT" not in c["COUNTERPARTY_ID"]]
        for c in corp_cps:
            for k, dtype in enumerate(["CORPORATE_OPERATIONAL", "CORPORATE_NON_OPERATIONAL"]):
                if c["SECTOR"] == "SME" and k == 1 and rng.random() < 0.5:
                    continue
                bal = float(rng.lognormal(math.log(1_800_000 if c["SECTOR"] == "CORPORATE" else 600_000), 0.8))
                self.deposits.append(self._new_deposit(f"{c['COUNTERPARTY_ID']}_DEP_{k+1:02d}", c["COUNTERPARTY_ID"],
                                                       dtype, str(rng.choice(["CHF"] * 8 + ["EUR", "USD"])),
                                                       bal, "CORPORATE", "CORPORATE", sigma=0.012))
        # event counterparties: large balances (scenario)
        for e in self.cfg["event_deposits"]:
            self.deposits.append(self._new_deposit(e["account"], e["counterparty"], e["deposit_type"], "CHF",
                                                   e["balance_chf"], "CORPORATE", "CORPORATE", sigma=e["sigma"]))
        for c in [c for c in self.counterparties if c["SECTOR"] == "FINANCIAL" and c["COUNTERPARTY_ID"].startswith("CP_FI")]:
            if rng.random() < 0.6:
                bal = float(rng.lognormal(math.log(8_000_000), 0.6))
                self.deposits.append(self._new_deposit(f"{c['COUNTERPARTY_ID']}_DEP_01", c["COUNTERPARTY_ID"],
                                                       "FINANCIAL_INSTITUTION", str(rng.choice(["CHF", "CHF", "EUR"])),
                                                       bal, "CORPORATE", "TREASURY", sigma=0.02))

        # ---- contracts
        cno = 0

        def add_contract(cp, product, sector, bu, ccy, outstanding, undrawn, tenor_days, rate, rating=None,
                         performing=True, start=None):
            nonlocal cno
            cno += 1
            st = start or (self.start - timedelta(days=int(rng.integers(30, 1500))))
            mat = self.start + timedelta(days=int(rng.integers(1, tenor_days)))
            self.contracts.append({"CONTRACT_ID": f"CTR_{cno:06d}", "COUNTERPARTY_ID": cp, "PRODUCT_TYPE": product,
                                       "SECTOR": sector, "BUSINESS_UNIT": bu, "CURRENCY": ccy, "RATING": rating,
                                       "START_DATE": st, "MATURITY_DATE": mat, "OUTSTANDING_CCY": outstanding,
                                       "UNDRAWN_CCY": undrawn, "INTEREST_RATE": rate, "IS_PERFORMING": performing,
                                       "TENOR_DAYS": tenor_days, "AMORTIZING": product == "LOAN_RETAIL"})
            return self.contracts[-1]

        retail_sample = rng.choice(self.customer_ids, size=min(1500, len(self.customer_ids)), replace=False)
        for cid in retail_sample:
            add_contract(cid, "LOAN_RETAIL", "RETAIL", "RETAIL", "CHF",
                         float(rng.lognormal(math.log(180_000), 0.7)), 0.0, 3650, float(rng.uniform(0.015, 0.035)),
                         performing=rng.random() > 0.02)
        for cid in rng.choice(self.customer_ids, size=min(400, len(self.customer_ids)), replace=False):
            add_contract(cid, "CREDIT_FACILITY", "RETAIL", "RETAIL", "CHF", 0.0,
                         float(rng.uniform(5_000, 50_000)), 1825, 0.06)
        for c in corp_cps:
            for _ in range(int(rng.integers(1, 3))):
                add_contract(c["COUNTERPARTY_ID"], "LOAN_CORP", c["SECTOR"], "CORPORATE",
                             str(rng.choice(["CHF"] * 8 + ["EUR", "USD"])),
                             float(rng.lognormal(math.log(2_500_000 if c["SECTOR"] == "CORPORATE" else 700_000), 0.8)),
                             0.0, 1825, float(rng.uniform(0.02, 0.05)), rating=c["RATING"], performing=rng.random() > 0.03)
            if rng.random() < 0.6:
                add_contract(c["COUNTERPARTY_ID"], "CREDIT_FACILITY", c["SECTOR"], "CORPORATE", "CHF",
                             0.0, float(rng.lognormal(math.log(2_000_000), 0.6)), 1095, 0.0, rating=c["RATING"])
            if c["SECTOR"] == "CORPORATE" and rng.random() < 0.25:
                add_contract(c["COUNTERPARTY_ID"], "LIQUIDITY_FACILITY", "CORPORATE", "CORPORATE", "CHF",
                             0.0, float(rng.lognormal(math.log(3_000_000), 0.5)), 1095, 0.0, rating=c["RATING"])
        fis = [c for c in self.counterparties if c["COUNTERPARTY_ID"].startswith("CP_FI")]
        for c in fis:
            add_contract(c["COUNTERPARTY_ID"], "INTERBANK_LOAN", "FINANCIAL", "TREASURY",
                         str(rng.choice(["CHF", "CHF", "EUR", "USD"])),
                         float(rng.uniform(1_000_000, 5_000_000)), 0.0, 180, float(rng.uniform(0.005, 0.015)),
                         rating=c["RATING"])
            add_contract(c["COUNTERPARTY_ID"], "INTERBANK_BORROWING", "FINANCIAL", "TREASURY", "CHF",
                         float(rng.uniform(1_000_000, 4_000_000)), 0.0, 270, float(rng.uniform(0.006, 0.016)),
                         rating=c["RATING"])
            if rng.random() < 0.3:
                add_contract(c["COUNTERPARTY_ID"], "LIQUIDITY_FACILITY", "FINANCIAL", "TREASURY", "CHF",
                             0.0, float(rng.uniform(5_000_000, 20_000_000)), 730, 0.0, rating=c["RATING"])
        # Event contracts (maturities set by the event plan, amounts by _size_events)
        self.evt_funding = add_contract("CP_FI_019", "INTERBANK_BORROWING", "FINANCIAL", "TREASURY", "CHF",
                                        250_000_000, 0.0, 400, 0.012, rating="A")
        self.evt_loans = [add_contract(c["COUNTERPARTY_ID"], "LOAN_CORP", "CORPORATE", "CORPORATE", "CHF",
                                       v, 0.0, 400, 0.03, rating=c["RATING"])
                          for c, v in zip(corp_cps[:5], [55e6, 45e6, 40e6, 35e6, 30e6])]
        # Data quality injection (~0.1% of rows)
        dq_cp = corp_cps[-1]["COUNTERPARTY_ID"]
        for k in range(3):
            add_contract(dq_cp, "NEW_DERIVATIVE_X", "CORPORATE", "TREASURY", "CHF",
                         float(rng.uniform(1e6, 5e6)), 0.0, 365, 0.0)
        for k in range(2):
            add_contract(corp_cps[-2]["COUNTERPARTY_ID"], "LOAN_CORP", "CORPORATE", "CORPORATE", "SEK",
                         float(rng.uniform(5e6, 20e6)), 0.0, 900, 0.03)
        for k in range(2):
            c = add_contract(corp_cps[-3]["COUNTERPARTY_ID"], "LOAN_CORP", "CORPORATE", "CORPORATE", "CHF",
                             float(rng.uniform(1e6, 4e6)), 0.0, 900, 0.03)
            c["START_DATE"] = c["MATURITY_DATE"] + timedelta(days=120)
            c["DQ_INVALID_DATE"] = True

        # ---- central bank reserves and vault cash (the securities come from the securities book)
        self.snb = {"HOLDING_ID": "HOLD-00001", "ASSET_TYPE": "CASH_SNB", "ISSUER_ID": "ISS_SNB",
                        "SECURITY_NAME": "SNB sight deposits", "MV_CHF": 600e6, "CUSTODIAN": "SNB"}
        self.holdings = [self.snb, {"HOLDING_ID": "HOLD-00002", "ASSET_TYPE": "CASH_VAULT", "ISSUER_ID": "ISS_VAULT",
                                        "SECURITY_NAME": "Vault cash", "MV_CHF": 25e6, "CUSTODIAN": "OWN"}]

    def _new_deposit(self, acc, cust, dtype, ccy, bal, seg, bu, sigma):
        cpt, _, allows, oper = DEPOSIT_TYPES[dtype]
        return {"ACCOUNT_ID": acc, "CUSTOMER_ID": cust, "DEPOSIT_TYPE": dtype, "CURRENCY": ccy, "BASE_BAL": bal, "BAL": bal,
                    "SIGMA": sigma, "IS_INSURED": cpt == "RETAIL" and bal <= 100_000,
                    "PRODUCT_COUNT": int(self.rng.integers(1, 6)) if allows else 1,
                    "TENURE": int(self.rng.integers(30, 3650)),
                    "HAS_DIRECT_DEBIT": bool(self.rng.random() < (0.7 if cpt == "RETAIL" else 0.3)),
                    "IS_OPERATIONAL": oper, "COUNTERPARTY_TYPE": cpt, "CUSTOMER_SEGMENT": seg,
                    "ACCOUNT_STATUS": "ACTIVE" if self.rng.random() < 0.98 or "EVT" in acc else "DORMANT",
                    "BUSINESS_UNIT": bu}

    # --------------------------------------------------------------- events
    def _plan_events(self):
        if not self.with_events:
            return
        t, ev = self.end, self.ev_cfg
        w2 = t - timedelta(days=ev["withdrawal_2"]["offset_days"])
        self.evt_dates = {
            "WITHDRAWAL_1": next_weekday(t - timedelta(days=ev["withdrawal_1"]["offset_days"])),
            "STRESS_START": next_weekday(t - timedelta(days=ev["stress"]["offset_days"])),
            "RECOVERY": next_weekday(t - timedelta(days=ev["recovery"]["offset_days"])),
            "INFLOW": next_weekday(t - timedelta(days=ev["inflow"]["offset_days"])),
            "WITHDRAWAL_2": w2 - timedelta(days=max(0, w2.weekday() - 4)),
        }
        stress_days = []
        d = self.evt_dates["STRESS_START"]
        while len(stress_days) < ev["stress"]["days"]:
            if d.weekday() < 5:
                stress_days.append(d)
            d += timedelta(days=1)
        self.stress_days = stress_days
        # Event funding: matures far out; enters 30d window on STRESS_START, rolled on RECOVERY
        self.evt_funding["MATURITY_DATE"] = self.evt_dates["STRESS_START"] + timedelta(days=ev["stress"]["funding_days_to_maturity"])
        self.evt_funding["PRE_EVENT_MATURITY"] = self.evt_dates["STRESS_START"] + timedelta(days=200)
        # Event loans: maturity pulled inside 30d window on INFLOW date
        for ln in self.evt_loans:
            ln["MATURITY_DATE"] = self.evt_dates["INFLOW"] + timedelta(days=ev["inflow"]["days_to_maturity"])
            ln["PRE_EVENT_MATURITY"] = self.evt_dates["INFLOW"] + timedelta(days=380)

    # ---------------------------------------------------------- LCR (BASE)
    @staticmethod
    def deposit_rate(d) -> float:
        _, base, allows, _ = DEPOSIT_TYPES[d["DEPOSIT_TYPE"]]
        r = base
        if allows and d["PRODUCT_COUNT"] >= 3:
            r -= 0.02
        if allows and d["HAS_DIRECT_DEBIT"]:
            r -= 0.01
        if d["TENURE"] < 540:
            r += 0.05
        return max(0.03, min(1.0, r))

    def _contract_valid(self, c) -> bool:
        return (c["PRODUCT_TYPE"] != "NEW_DERIVATIVE_X" and c["CURRENCY"] != "SEK"
                and not c.get("DQ_INVALID_DATE"))

    def _cashflows(self, c, d: date) -> list[dict]:
        """Contractual cashflows of a contract within the next 365 days."""
        if c["PRODUCT_TYPE"] in ("CREDIT_FACILITY", "LIQUIDITY_FACILITY") or c["OUTSTANDING_CCY"] <= 0:
            return []
        if c.get("DQ_INVALID_DATE"):
            return []
        horizon = d + timedelta(days=365)
        direction = "OUT" if c["PRODUCT_TYPE"] == "INTERBANK_BORROWING" else "IN"
        if c["AMORTIZING"]:
            return self._amortizing_flows(c, d, horizon, direction)
        return self._bullet_flows(c, d, horizon, direction)

    @staticmethod
    def _amortizing_flows(c, d: date, horizon: date, direction: str) -> list[dict]:
        """Quarterly amortization (1.25% per quarter) + interest, remaining balance at maturity."""
        out = []
        mat = c["MATURITY_DATE"]
        q = date(d.year, ((d.month - 1) // 3) * 3 + 1, 1)
        bal = c["OUTSTANDING_CCY"]
        while True:
            q = (q.replace(day=1) + timedelta(days=95)).replace(day=1)
            if q > horizon or q > mat:
                break
            prin = bal * 0.0125
            out.append({"CF_DATE": q, "CF_TYPE": "PRINCIPAL", "DIRECTION": direction, "AMOUNT_CCY": prin})
            out.append({"CF_DATE": q, "CF_TYPE": "INTEREST", "DIRECTION": direction, "AMOUNT_CCY": bal * c["INTEREST_RATE"] / 4})
            bal -= prin
        if mat <= horizon:
            out.append({"CF_DATE": mat, "CF_TYPE": "PRINCIPAL", "DIRECTION": direction, "AMOUNT_CCY": bal})
        return out

    @staticmethod
    def _bullet_flows(c, d: date, horizon: date, direction: str) -> list[dict]:
        """Principal at maturity, quarterly interest until maturity plus the broken period at maturity."""
        out = []
        mat = c["MATURITY_DATE"]
        if mat <= horizon:
            out.append({"CF_DATE": mat, "CF_TYPE": "PRINCIPAL", "DIRECTION": direction, "AMOUNT_CCY": c["OUTSTANDING_CCY"]})
        k = 1
        while True:
            q = d + timedelta(days=91 * k)
            if q > min(mat, horizon):
                break
            out.append({"CF_DATE": q, "CF_TYPE": "INTEREST", "DIRECTION": direction,
                        "AMOUNT_CCY": c["OUTSTANDING_CCY"] * c["INTEREST_RATE"] / 4})
            k += 1
        if mat <= horizon and c["INTEREST_RATE"] > 0:
            out.append({"CF_DATE": mat, "CF_TYPE": "INTEREST", "DIRECTION": direction,
                        "AMOUNT_CCY": c["OUTSTANDING_CCY"] * c["INTEREST_RATE"] * ((mat - d).days % 91) / 365})
        return out

    def hqla_positions(self, d: date):
        """(asset_type, market value CHF, eligible) of cash and every security of the book on date d."""
        out = [(h["ASSET_TYPE"], h["MV_CHF"], True) for h in self.holdings]
        for s, mv in self.sec.valued(d):
            if s.wlk in SEC_ASSET_TYPE:
                out.append((SEC_ASSET_TYPE[s.wlk], mv, WLK[s.wlk][0] != "NONE" and not s.encumbered))
        return out

    def _flows(self, d: date) -> dict:
        """30-day BASE outflows / inflows split into retail and wholesale (the wholesale part is calibrated)."""
        f = {"out_r": 0.0, "out_w": 0.0, "in_r": 0.0, "in_w": 0.0}
        for dep in self.deposits:
            if dep["ACCOUNT_STATUS"] != "ACTIVE":
                continue
            k = "out_r" if dep["COUNTERPARTY_TYPE"] == "RETAIL" else "out_w"
            f[k] += dep["BAL"] * (self.fx.rate(dep["CURRENCY"], d) or 0) * self.deposit_rate(dep)
        for c in self.contracts:
            self._add_contract_flows(f, c, d)
        return f

    def _add_contract_flows(self, f: dict, c: dict, d: date):
        """Add one contract's 30-day BASE flows to f: facility drawdown, or its cashflows within 30 days."""
        key = (c["PRODUCT_TYPE"], c["SECTOR"])
        if not self._contract_valid(c) or key not in CONTRACT_RULES:
            return
        fx = self.fx.rate(c["CURRENCY"], d) or 0
        kind, rate = CONTRACT_RULES[key]
        part = "r" if c["SECTOR"] == "RETAIL" else "w"
        if kind == "FACILITY":
            f["out_" + part] += c["UNDRAWN_CCY"] * fx * rate
            return
        side = "out_" if kind == "LIABILITY" else "in_"
        if side == "in_" and not c["IS_PERFORMING"]:
            return
        for cf in self._cashflows(c, d):
            if d < cf["CF_DATE"] <= d + timedelta(days=30):
                f[side + part] += cf["AMOUNT_CCY"] * fx * rate

    def compute_lcr(self, d: date) -> dict:
        l1 = l2a = l2b = 0.0
        for asset_type, mv, eligible in self.hqla_positions(d):
            if not eligible:
                continue
            lvl, f = HQLA_TYPES[asset_type]
            if lvl == "L1":
                l1 += mv * f
            elif lvl == "L2A":
                l2a += mv * f
            else:
                l2b += mv * f
        fl = self._flows(d)
        out, inflow = fl["out_r"] + fl["out_w"], fl["in_r"] + fl["in_w"]
        l2b_c = min(l2b, L2B_CAP_RATIO * (l1 + l2a))
        l2 = min(l2a + l2b_c, L2_CAP_RATIO * l1)
        hqla = l1 + l2
        inflow_c = min(inflow, INFLOW_CAP * out)
        net = out - inflow_c
        return {"L1": l1, "L2A": l2a, "L2B": l2b, "HQLA": hqla, "OUTFLOW": out, "INFLOW": inflow, "NET": net,
                    "LCR": hqla / net * 100 if net > 0 else float("nan"), "FLOWS": fl}

    def _calibrate(self):
        """Scale the wholesale client book so the day-0 BASE LCR equals the target on the given securities pool."""
        self._pre_event_state(self.start)
        base = self.compute_lcr(self.start)
        fl = base["FLOWS"]
        target_net = base["HQLA"] / (self.target_lcr / 100.0)
        k = 1.0
        for _ in range(20):  # fixed point, the inflow cap makes NET piecewise linear in k
            out = fl["out_r"] + k * fl["out_w"]
            inflow = min(fl["in_r"] + k * fl["in_w"], INFLOW_CAP * out)
            net = out - inflow
            k *= target_net / net
        k *= self.scale_adjust
        self.scale = k
        for dep in self.deposits:
            if dep["COUNTERPARTY_TYPE"] != "RETAIL":
                dep["BAL"] *= k
                dep["BASE_BAL"] *= k
        for c in self.contracts:
            if c["SECTOR"] != "RETAIL":
                c["OUTSTANDING_CCY"] *= k
                c["UNDRAWN_CCY"] *= k
        self.sec.flow_scale = k
        if self.with_events:
            self._size_events(self.compute_lcr(self.start)["NET"])

    def _size_events(self, n0: float):
        """Scale event amounts to the size of the book (day-0 net cash outflows)."""
        ev = self.ev_cfg
        self.n0 = n0
        self.evt_funding["OUTSTANDING_CCY"] = round(ev["stress"]["funding_share_of_net_outflow"] * n0, -6)
        loan_total = round(ev["inflow"]["loan_share_of_net_outflow"] * n0, -6)
        for ln, w in zip(self.evt_loans, ev["inflow"]["loan_weights"]):
            ln["OUTSTANDING_CCY"] = round(loan_total * w, -5)
        self.recovery_reserves = round(ev["recovery"]["reserves_share_of_net_outflow"] * n0, -6)
        self.withdrawal_1 = round(ev["withdrawal_1"]["amount_chf"] * self.scale, -6)
        self.withdrawal_2 = round(ev["withdrawal_2"]["amount_chf"] * self.scale, -6)
        fund = self.evt_funding["OUTSTANDING_CCY"]
        loans = sum(ln["OUTSTANDING_CCY"] for ln in self.evt_loans)
        dates = self.evt_dates
        self.events = [
            Event("EVT_01", dates["WITHDRAWAL_1"], "DEPOSIT_WITHDRAWAL",
                  f"Alpenbau Holding AG withdraws CHF {self.withdrawal_1/1e6:.0f}M from its non-operational deposit "
                  "(settled from SNB reserves)", "CP_CORP_EVT1", ev["withdrawal_1"]["account"], self.withdrawal_1,
                  "DOWN", ev["withdrawal_1"]["min_delta_pp"]),
            Event("EVT_02", dates["STRESS_START"], "MARKET_STRESS",
                  f"Market stress begins: bond/equity price shock, corporate deposit run and CHF {fund/1e6:.0f}M "
                  "short-term interbank funding from Zentralbank Partner AG falls inside the 30-day window",
                  "CP_FI_019", self.evt_funding["CONTRACT_ID"], fund, "DOWN", ev["stress"]["min_delta_pp"]),
            Event("EVT_03", dates["RECOVERY"], "RECOVERY",
                  f"Treasury rolls the CHF {fund/1e6:.0f}M interbank funding to 6 months and buys CHF "
                  f"{self.recovery_reserves/1e6:.0f}M SNB reserves with new term funding",
                  "CP_FI_019", self.evt_funding["CONTRACT_ID"], fund, "UP", ev["recovery"]["min_delta_pp"]),
            Event("EVT_04", dates["INFLOW"], "LOAN_INFLOW",
                  f"CHF {loans/1e6:.0f}M corporate loan book (5 loans) scheduled to mature within 30 days - "
                  "contractual inflows", self.evt_loans[0]["COUNTERPARTY_ID"], self.evt_loans[0]["CONTRACT_ID"],
                  loans, "UP", ev["inflow"]["min_delta_pp"]),
            Event("EVT_05", dates["WITHDRAWAL_2"], "DEPOSIT_WITHDRAWAL",
                  f"Rhein Logistik Group AG withdraws CHF {self.withdrawal_2/1e6:.0f}M from its non-operational deposit",
                  "CP_CORP_EVT2", ev["withdrawal_2"]["account"], self.withdrawal_2, "DOWN",
                  ev["withdrawal_2"]["min_delta_pp"]),
        ]

    # ---------------------------------------------------------------- daily
    def _evolve(self, d: date):
        rng = self.rng
        business_day = d.weekday() < 5
        if not business_day:
            for dep in self.deposits:
                dep["TENURE"] += 1
            return
        net_flow = 0.0
        for dep in self.deposits:
            dep["TENURE"] += 1
            old = dep["BAL"]
            new = old + 0.1 * (dep["BASE_BAL"] - old) + dep["SIGMA"] * dep["BASE_BAL"] * rng.standard_normal()
            dep["BAL"] = max(new, 0.0)
            if dep["ACCOUNT_STATUS"] == "ACTIVE":
                net_flow += (dep["BAL"] - old) * (self.fx.rate(dep["CURRENCY"], d) or 1.0)
        self.snb["MV_CHF"] += net_flow
        for c in self.contracts:
            if c["MATURITY_DATE"] <= d and not c.get("DQ_INVALID_DATE"):
                if c in self.evt_loans:
                    # event loans repay into SNB reserves and are not rolled
                    if c["OUTSTANDING_CCY"] > 0:
                        self.snb["MV_CHF"] += c["OUTSTANDING_CCY"]
                        c["OUTSTANDING_CCY"] = 0.0
                    continue
                # roll over: same notional, new tenor
                c["START_DATE"] = d
                c["MATURITY_DATE"] = d + timedelta(days=int(rng.integers(30, c["TENOR_DAYS"] + 30)))
            if c["AMORTIZING"] and d.day == 1 and d.month in (1, 4, 7, 10):
                c["OUTSTANDING_CCY"] *= (1 - 0.0125)

    def _withdraw(self, account: str, amount: float):
        a = next(x for x in self.deposits if x["ACCOUNT_ID"] == account)
        a["BAL"] -= amount
        a["BASE_BAL"] -= amount
        self.snb["MV_CHF"] -= amount

    def _apply_events(self, d: date):
        if not self.with_events:
            return
        ev, dates = self.ev_cfg, self.evt_dates
        if d == dates["WITHDRAWAL_1"]:
            self._withdraw(ev["withdrawal_1"]["account"], self.withdrawal_1)
        if d in self.stress_days:
            shock = ev["stress"]["price_shock"]
            self.sec.shock_prices({w: shock[t] for w, t in SEC_ASSET_TYPE.items() if t in shock})
            # corporate operational deposits run each stress day
            run = 0.0
            for a in self.deposits:
                if a["DEPOSIT_TYPE"] == "CORPORATE_OPERATIONAL" and a["ACCOUNT_STATUS"] == "ACTIVE":
                    w = a["BAL"] * ev["stress"]["corporate_operational_run_per_day"]
                    a["BAL"] -= w
                    a["BASE_BAL"] -= w
                    run += w * (self.fx.rate(a["CURRENCY"], d) or 1.0)
            self.snb["MV_CHF"] -= run
        if d == dates["RECOVERY"]:
            self.evt_funding["START_DATE"] = d
            self.evt_funding["MATURITY_DATE"] = d + timedelta(days=ev["recovery"]["funding_roll_days"])
            self.snb["MV_CHF"] += self.recovery_reserves
            self.sec.recover_prices(ev["recovery"]["price_recovery_share"])
        if d == dates["WITHDRAWAL_2"]:
            self._withdraw(ev["withdrawal_2"]["account"], self.withdrawal_2)

    def _pre_event_state(self, d: date):
        """Before their event dates, event contracts sit outside the 30-day window."""
        if not self.with_events:
            return
        ev = self.evt_dates
        f = self.evt_funding
        if d < ev["STRESS_START"]:
            f["MATURITY_DATE"] = f["PRE_EVENT_MATURITY"]
        elif d < ev["RECOVERY"]:
            f["MATURITY_DATE"] = ev["STRESS_START"] + timedelta(days=self.ev_cfg["stress"]["funding_days_to_maturity"])
        for ln in self.evt_loans:
            if d < ev["INFLOW"]:
                ln["MATURITY_DATE"] = ln["PRE_EVENT_MATURITY"]
            elif ln["OUTSTANDING_CCY"] > 0:
                ln["MATURITY_DATE"] = ev["INFLOW"] + timedelta(days=self.ev_cfg["inflow"]["days_to_maturity"])

    # ---------------------------------------------------------------- write
    def _dti_cp_entity(self, cp_id: str) -> str:
        """Booking entity of a counterparty; showcase event clients are pinned."""
        return {"CP_CORP_EVT1": "LE_02", "CP_CORP_EVT2": "LE_03"}.get(cp_id, entity_for(cp_id))

    def _holding_rows(self, d: date, run_id: str) -> list[dict]:
        """LCR holdings = SNB reserves + vault cash + every security of the securities book (same ids and values)."""
        ds = d.isoformat()
        rows = [self._cash_row(h, ds, run_id) for h in self.holdings]
        rows += [self._security_row(s, mv, d, run_id) for s, mv in self.sec.valued(d) if s.wlk in SEC_ASSET_TYPE]
        return rows

    @staticmethod
    def _cash_row(h: dict, ds: str, run_id: str) -> dict:
        mv = round(h["MV_CHF"], 2)
        return {"HOLDING_ID": h["HOLDING_ID"], "AS_OF_DATE": ds, "ASSET_TYPE": h["ASSET_TYPE"], "ISIN": None,
                "SECURITY_NAME": h["SECURITY_NAME"], "CURRENCY": "CHF", "QUANTITY": None,
                "MARKET_VALUE_CCY": mv, "MARKET_VALUE_CHF": mv,
                "FX_RATE": None, "MATURITY_DATE": None, "CREDIT_RATING": None, "SMI_CONSTITUENT": False,
                "HQLA_ELIGIBLE": True, "PORTFOLIO_CODE": "TREASURY_LIQ", "CUSTODIAN": h["CUSTODIAN"],
                "INELIGIBILITY_REASON": None, "BUSINESS_UNIT": "TREASURY", "ISSUER_ID": h["ISSUER_ID"],
                "ENTITY_ID": "LE_01", "ISSUER_GROUP": "Central banks" if h["ASSET_TYPE"] == "CASH_SNB" else "Own cash",
                "INDUSTRY": "Public sector", "WLK_CLUSTER": "I", "WLK_HAIRCUT": WLK_HAIRCUT["I"],
                "NOMINAL_CHF": mv, "COLLATERAL_VALUE_CHF": mv,
                "IS_ENCUMBERED": False, "IS_MOBILISED": False, "RUN_ID": run_id}

    @staticmethod
    def _ineligibility(encumbered: bool, hqla_category: str) -> str | None:
        """Why a security is not LCR HQLA: below BBB- (WLK IX) or pledged in secured funding; None = eligible."""
        if hqla_category == "NONE":
            return "Rating below BBB- (not HQLA eligible)"
        if encumbered:
            return "Encumbered: pledged as collateral in secured funding"
        return None

    def _security_row(self, s, mv: float, d: date, run_id: str) -> dict:
        fx = self.fx.rate(s.currency, d) or 1.0
        hq, hc = WLK[s.wlk][0], WLK[s.wlk][1]
        mat = self.sec._maturity(s, d)
        reason = self._ineligibility(s.encumbered, hq)
        k = crc(s.sid)
        return {"HOLDING_ID": s.sid, "AS_OF_DATE": d.isoformat(), "ASSET_TYPE": SEC_ASSET_TYPE[s.wlk], "ISIN": s.isin,
                "SECURITY_NAME": s.name, "CURRENCY": s.currency,
                "QUANTITY": round(mv / 250.0, 0) if s.wlk == "VIII" else None,
                "MARKET_VALUE_CCY": round(mv / fx, 2), "MARKET_VALUE_CHF": round(mv, 2),
                "FX_RATE": None if s.currency == "CHF" else round(fx, 6),
                "MATURITY_DATE": mat.isoformat() if mat else None, "CREDIT_RATING": SEC_RATING[s.wlk],
                "SMI_CONSTITUENT": s.wlk == "VIII", "HQLA_ELIGIBLE": reason is None,
                "PORTFOLIO_CODE": "TREASURY_LIQ" if s.short else PORTFOLIOS[k % len(PORTFOLIOS)],
                "CUSTODIAN": CUSTODIAN.get(s.currency, "SIX SIS"),
                "INELIGIBILITY_REASON": reason, "BUSINESS_UNIT": "TREASURY", "ISSUER_ID": issuer_id(s.issuer),
                "ENTITY_ID": s.entity, "ISSUER_GROUP": s.issuer_group,
                "INDUSTRY": SEC_INDUSTRY.get(s.issuer_group, SEC_CORP_INDUSTRIES[k % len(SEC_CORP_INDUSTRIES)]),
                "WLK_CLUSTER": s.wlk, "WLK_HAIRCUT": hc, "NOMINAL_CHF": round(s.nominal, 2),
                "COLLATERAL_VALUE_CHF": round(mv * (1 - hc), 2), "IS_ENCUMBERED": s.encumbered,
                "IS_MOBILISED": s.mobilised, "RUN_ID": run_id}

    def _rows(self, d: date, run_id: str = ""):
        ds = d.isoformat()
        hold = self._holding_rows(d, run_id)
        deps = []
        for a in self.deposits:
            fx = self.fx.rate(a["CURRENCY"], d) or 1.0
            deps.append({"ACCOUNT_ID": a["ACCOUNT_ID"], "AS_OF_DATE": ds, "CUSTOMER_ID": a["CUSTOMER_ID"],
                             "DEPOSIT_TYPE": a["DEPOSIT_TYPE"], "CURRENCY": a["CURRENCY"],
                             "BALANCE_CCY": round(a["BAL"], 2), "BALANCE_CHF": round(a["BAL"] * fx, 2),
                             "FX_RATE": None if a["CURRENCY"] == "CHF" else round(fx, 6),
                             "IS_INSURED": a["IS_INSURED"], "PRODUCT_COUNT": a["PRODUCT_COUNT"],
                             "ACCOUNT_TENURE_DAYS": a["TENURE"], "HAS_DIRECT_DEBIT": a["HAS_DIRECT_DEBIT"],
                             "IS_OPERATIONAL": a["IS_OPERATIONAL"], "COUNTERPARTY_TYPE": a["COUNTERPARTY_TYPE"],
                             "CUSTOMER_SEGMENT": a["CUSTOMER_SEGMENT"], "ACCOUNT_STATUS": a["ACCOUNT_STATUS"],
                             "BUSINESS_UNIT": a["BUSINESS_UNIT"], "COUNTERPARTY_ID": a["CUSTOMER_ID"],
                             # DtI (FRISCO-like) attributes - deposits are liabilities of application EIN
                             "ENTITY_ID": self._dti_cp_entity(a["CUSTOMER_ID"]), "APPLICATION_ID": "EIN",
                             "AKTIV_PASSIV": "P",
                             "ORIG_APPLICATION": "RETAIL_CORE" if a["COUNTERPARTY_TYPE"] == "RETAIL" else "CORP_LENDING",
                             "IS_NEW_BUSINESS": a["TENURE"] < 90, "RUN_ID": run_id})
        ctrs, cfs = [], []
        for c in self.contracts:
            if c["OUTSTANDING_CCY"] <= 0 and c["UNDRAWN_CCY"] <= 0:
                continue
            ctrs.append({"CONTRACT_ID": c["CONTRACT_ID"], "AS_OF_DATE": ds, "COUNTERPARTY_ID": c["COUNTERPARTY_ID"],
                             "PRODUCT_TYPE": c["PRODUCT_TYPE"], "BUSINESS_UNIT": c["BUSINESS_UNIT"], "CURRENCY": c["CURRENCY"],
                             "RATING": c["RATING"], "START_DATE": c["START_DATE"].isoformat(),
                             "MATURITY_DATE": c["MATURITY_DATE"].isoformat(),
                             "OUTSTANDING_CCY": round(c["OUTSTANDING_CCY"], 2), "UNDRAWN_CCY": round(c["UNDRAWN_CCY"], 2),
                             "INTEREST_RATE": round(c["INTEREST_RATE"], 5), "IS_PERFORMING": c["IS_PERFORMING"],
                             "COUNTERPARTY_SECTOR": c["SECTOR"],
                             # DtI (FRISCO-like) attributes
                             "ENTITY_ID": self._dti_cp_entity(c["COUNTERPARTY_ID"]),
                             "APPLICATION_ID": CONTRACT_APPS.get(c["PRODUCT_TYPE"], ("SON", "OTHER"))[0],
                             "AKTIV_PASSIV": "P" if c["PRODUCT_TYPE"] == "INTERBANK_BORROWING" else "A",
                             "ORIG_APPLICATION": CONTRACT_APPS.get(c["PRODUCT_TYPE"], ("SON", "OTHER"))[1],
                             "IS_NEW_BUSINESS": (d - c["START_DATE"]).days <= 30})
            for cf in self._cashflows(c, d):
                cfs.append({"AS_OF_DATE": ds, "CONTRACT_ID": c["CONTRACT_ID"], "CF_DATE": cf["CF_DATE"].isoformat(),
                                "CF_TYPE": cf["CF_TYPE"], "DIRECTION": cf["DIRECTION"], "CURRENCY": c["CURRENCY"],
                                "AMOUNT_CCY": round(cf["AMOUNT_CCY"], 2)})
        return hold, deps, ctrs, cfs

    # ------------------------------------------------------- DtI (UC2) feeds
    def _secured_funding(self, d: date, hold: list[dict]) -> list[dict]:
        """SPOT-like secured funding: one deal per encumbered security, plus reverse repos."""
        ds, out = d.isoformat(), []
        fis = [c["COUNTERPARTY_ID"] for c in self.counterparties if c["COUNTERPARTY_ID"].startswith("CP_FI")]
        for h in hold:
            if not h["IS_ENCUMBERED"]:
                continue
            kind = SECURED_TYPE.get(h["WLK_CLUSTER"], "BILATERAL_REPO")
            k = crc(h["HOLDING_ID"])
            out.append({"DEAL_ID": f"SF-{h['HOLDING_ID']}", "AS_OF_DATE": ds, "ENTITY_ID": h["ENTITY_ID"],
                            "TRANSACTION_TYPE": kind,
                            "COUNTERPARTY_ID": "ISS_SNB" if kind == "SNB_REPO" else fis[k % len(fis)],
                            "COLLATERAL_HOLDING_ID": h["HOLDING_ID"], "ELIGIBLE_COLLATERAL": SECURED_ELIGIBLE[kind],
                            "NOMINAL_CHF": h["COLLATERAL_VALUE_CHF"],
                            "MATURITY_DATE": (d + timedelta(days=1 + k % 60)).isoformat(), "DIRECTION": "BORROW"})
        for i, ent in enumerate(["LE_01", "LE_02", "LE_03"]):
            out.append({"DEAL_ID": f"SF-RR-{ent}", "AS_OF_DATE": ds, "ENTITY_ID": ent, "TRANSACTION_TYPE": "REVERSE_REPO",
                            "COUNTERPARTY_ID": fis[i], "COLLATERAL_HOLDING_ID": None,
                            "ELIGIBLE_COLLATERAL": SECURED_ELIGIBLE["REVERSE_REPO"],
                            "NOMINAL_CHF": round(20e6 * self.scale * ENTITY_WEIGHT[ent] / 0.3, -5),
                            "MATURITY_DATE": (d + timedelta(days=7)).isoformat(), "DIRECTION": "LEND"})
        return out

    def _write_dti_feeds(self, out_dir: Path, dates: list[date]):
        """Report status, manual corrections, planning and DtI showcase events (ground truth for tests)."""
        cfg = self.cfg["dti"]
        latest, prev = dates[-1], dates[-2]
        pe = cfg["preliminary_entity"]
        status = [{"AS_OF_DATE": d.isoformat(), "ENTITY_ID": e,
                       "DATA_STATUS": "PRELIMINARY" if (d == latest and e == pe) else "FINAL",
                       "LOCKED_BY": None if (d == latest and e == pe) else "LST_RUN",
                       "LOCKED_AT_UTC": None if (d == latest and e == pe) else f"{d.isoformat()} 21:00:00"}
                  for d in dates for e, _ in ENTITY_SHARES]
        pd.DataFrame(status).to_csv(out_dir / "dti_report_status.csv", index=False)
        corr = []
        for c in cfg["corrections"]:
            d = latest if c["date"] == "latest" else prev
            corr.append({"CORRECTION_ID": c["id"], "AS_OF_DATE": d.isoformat(), "ENTITY_ID": c["entity"],
                             "LST_POSITION": c["position"], "VALUE_DATE": (d + timedelta(days=c["value_offset_days"])).isoformat(),
                             "AMOUNT_CHF": c["amount_chf"], "COMMENT": c["comment"], "CREATED_BY": "LRCU_ANALYST",
                             "CREATED_AT_UTC": f"{d.isoformat()} {c['time']}"})
        pd.DataFrame(corr).to_csv(out_dir / "dti_corrections.csv", index=False)
        pd.DataFrame(self._planning_rows(cfg["planning"], dates)).to_csv(out_dir / "dti_planning.csv", index=False)
        evts = self._dti_event_rows(cfg, latest) if self.with_events else []
        evts += self.sec.events
        pd.DataFrame(evts, columns=["EVENT_ID", "EVENT_DATE", "ENTITY_ID", "EVENT_TYPE", "POSITION_ID",
                                    "AMOUNT_CHF", "DESCRIPTION"]).to_csv(out_dir / "dti_events.csv", index=False)

    def _planning_rows(self, p: dict, dates: list[date]) -> list[dict]:
        """Planning positions (no deal level): monthly new lending (outflow) and planned funding (inflow)."""
        plan = []
        for d in dates:
            for e, w in ENTITY_SHARES:
                for m in range(1, p["months"] + 1):
                    vd = next_weekday(date(d.year + (d.month - 1 + m) // 12, (d.month - 1 + m) % 12 + 1, 1))
                    plan.append({"AS_OF_DATE": d.isoformat(), "ENTITY_ID": e, "PLAN_TYPE": "NEW_LENDING", "VALUE_DATE": vd.isoformat(),
                                 "AMOUNT_CHF": round(p["new_lending_group_chf"] * self.scale * w / 100
                                                     * (1 + p["new_lending_monthly_growth"] * m), -3)})
                    plan.append({"AS_OF_DATE": d.isoformat(), "ENTITY_ID": e, "PLAN_TYPE": "FUNDING_PLAN", "VALUE_DATE": vd.isoformat(),
                                 "AMOUNT_CHF": round(p["funding_plan_group_chf"] * self.scale * w / 100, -3)})
        return plan

    def _dti_event_rows(self, cfg: dict, latest: date) -> list[dict]:
        """DtI showcase events (ground truth for the DtI tests)."""
        ev = self.evt_dates
        c1 = cfg["corrections"][0]
        pe = cfg["preliminary_entity"]
        return [
                {"EVENT_ID": "DTI_01", "EVENT_DATE": ev["WITHDRAWAL_1"].isoformat(), "ENTITY_ID": self._dti_cp_entity("CP_CORP_EVT1"),
                     "EVENT_TYPE": "DEPOSIT_WITHDRAWAL", "POSITION_ID": self.ev_cfg["withdrawal_1"]["account"],
                     "AMOUNT_CHF": self.withdrawal_1,
                     "DESCRIPTION": f"Alpenbau Holding AG withdraws CHF {self.withdrawal_1/1e6:.0f}M at Legal Entity 2: "
                                 "largest delta vs. the previous day"},
                {"EVENT_ID": "DTI_02", "EVENT_DATE": ev["INFLOW"].isoformat(),
                     "ENTITY_ID": self._dti_cp_entity(self.evt_loans[0]["COUNTERPARTY_ID"]),
                     "EVENT_TYPE": "BUCKET_SHIFT", "POSITION_ID": self.evt_loans[0]["CONTRACT_ID"],
                     "AMOUNT_CHF": sum(ln["OUTSTANDING_CCY"] for ln in self.evt_loans),
                     "DESCRIPTION": "Corporate loan book maturity moves from > 12 months into the 1-month bucket"},
                {"EVENT_ID": "DTI_03", "EVENT_DATE": latest.isoformat(), "ENTITY_ID": c1["entity"],
                     "EVENT_TYPE": "CORRECTION", "POSITION_ID": c1["id"], "AMOUNT_CHF": float(c1["amount_chf"]),
                     "DESCRIPTION": f"Manual correction of the central bank balance (CHF {c1['amount_chf']/1e6:.0f}M)"},
                {"EVENT_ID": "DTI_04", "EVENT_DATE": latest.isoformat(), "ENTITY_ID": pe,
                     "EVENT_TYPE": "PRELIMINARY_DATA", "POSITION_ID": None, "AMOUNT_CHF": 0.0,
                     "DESCRIPTION": f"Legal Entity {int(pe[-2:])} LST run not yet locked: data is PRELIMINARY"},
            ]

    def _step(self, i: int, d: date, latest: date):
        self._pre_event_state(d)
        if i > 0:
            self._evolve(d)
            if is_business_day(d, ZURICH_HOLIDAYS):
                self.sec.evolve(d, latest)
            self.snb["MV_CHF"] += self.sec.take_cash()
        self._apply_events(d)

    def probe_lcr(self, d_probe: date, latest: date) -> float:
        """Simulate (without output) up to d_probe and return the BASE LCR there (calibration pass)."""
        for i in range((d_probe - self.start).days + 1):
            self._step(i, self.start + timedelta(days=i), latest)
        return self.compute_lcr(d_probe)["LCR"]

    def _write_snapshot(self, d: date, daily: bool, out_dir: Path, run_id: str, summary: list[dict]):
        """Holdings, deposits, contracts, cashflows and secured funding of one snapshot date. SNAPSHOT_SCOPE (last
        column) is DAILY in the LCR window and MONTH_END for the history; daily dates also get an LCR estimate."""
        tag = d.strftime("%Y%m%d")
        hold, deps, ctrs, cfs = self._rows(d, run_id)
        scope = "DAILY" if daily else "MONTH_END"
        for rows in (hold, deps, ctrs, cfs):
            for r in rows:
                r["SNAPSHOT_SCOPE"] = scope
        pd.DataFrame(hold).to_csv(out_dir / f"hqla_holdings_{tag}.csv", index=False)
        pd.DataFrame(deps).to_csv(out_dir / f"deposit_balances_{tag}.csv", index=False)
        pd.DataFrame(ctrs).to_csv(out_dir / f"contracts_{tag}.csv", index=False)
        pd.DataFrame(cfs).to_csv(out_dir / f"cashflows_{tag}.csv", index=False)
        sf = self._secured_funding(d, hold)
        pd.DataFrame(sf).to_csv(out_dir / f"dti_secured_funding_{tag}.csv", index=False)
        for k, rows in (("h", hold), ("d", deps), ("c", ctrs), ("f", cfs), ("s", sf)):
            self.totals[k] += len(rows)
        if not daily:
            self.totals["hist"] += 1
            return
        m = self.compute_lcr(d)
        m.pop("FLOWS")
        m["AS_OF_DATE"] = d
        summary.append(m)

    def run(self, out_dir: Path, window_start: date, hist_dates: list[date], report_dates: list[date],
            run_id: str) -> pd.DataFrame:
        """One simulation from self.start to self.end. Writes the daily LCR window and the month-end history
        (hist_dates) into the same files, marked by SNAPSHOT_SCOPE (DAILY / MONTH_END, last column), and the
        securities snapshots on every DtI reporting date."""
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.counterparties).to_csv(out_dir / "counterparties.csv", index=False)
        hist, report, latest = set(hist_dates), set(report_dates), max(report_dates)
        summary = []
        self.totals = {"h": 0, "d": 0, "c": 0, "f": 0, "s": 0, "hist": 0, "securities": 0, "security_cashflows": 0, "transactions": 0}
        for i in range(self.days):
            d = self.start + timedelta(days=i)
            self._step(i, d, latest)
            if d >= window_start or d in hist:
                self._write_snapshot(d, d >= window_start, out_dir, run_id, summary)
            if d in report:
                t = self.sec.write(d, out_dir, run_id)
                self.totals["securities"] += t["securities"]
                self.totals["security_cashflows"] += t["cashflows"]
                self.totals["transactions"] += t["transactions"]
        ev = pd.DataFrame([{"EVENT_ID": e.event_id, "EVENT_DATE": e.event_date.isoformat(), "EVENT_TYPE": e.event_type,
                                "DESCRIPTION": e.description, "COUNTERPARTY_ID": e.counterparty_id,
                                "CONTRACT_ID": e.contract_id, "AMOUNT_CHF": e.amount_chf,
                                "EXPECTED_DIRECTION": e.expected_direction,
                                "EXPECTED_MIN_DELTA_PP": e.expected_min_delta_pp} for e in self.events],
                          columns=["EVENT_ID", "EVENT_DATE", "EVENT_TYPE", "DESCRIPTION", "COUNTERPARTY_ID",
                                   "CONTRACT_ID", "AMOUNT_CHF", "EXPECTED_DIRECTION", "EXPECTED_MIN_DELTA_PP"])
        ev.to_csv(out_dir / "lcr_events.csv", index=False)
        self._write_dti_feeds(out_dir, report_dates)
        write_data_dictionary(out_dir)
        return pd.DataFrame(summary)


def write_data_dictionary(out_dir: Path):
    (out_dir / "README.md").write_text("""# LCR + DtI generated data - data dictionary

One book feeds both engines. All amounts in original currency (`*_CCY`) or Swiss francs (`*_CHF`).
One file per snapshot date (`YYYYMMDD`). RUN_ID (holdings, deposits, securities) = generation_run.csv RUN_ID.

## generation_run.csv
RUN_ID (deterministic hash of the generator inputs), AS_OF_DATE (latest reporting date of LCR and DtI),
WINDOW_START (first LCR date), HISTORY_START (first DtI month-end), SEED, TARGET_LCR, BOOK_SCALE (calibrated
wholesale scale), SCENARIO, CUSTOMERS, GENERATOR_VERSION.

## _manifest.csv
TABLE_NAME, ROW_COUNT: rows written per target table (checked by the SQL gate after the load).

## counterparties.csv (static)
COUNTERPARTY_ID (retail = CRM customer_id, CP_CORP_*, CP_FI_*, ISS_* issuers of the securities book),
COUNTERPARTY_NAME, SECTOR, COUNTRY, RATING, CUSTOMER_ID, BUSINESS_UNIT.

## hqla_holdings_YYYYMMDD.csv (daily, LCR window)
SNB reserves (HOLD-00001), vault cash (HOLD-00002) and every security of the securities book: HOLDING_ID =
SECURITY_ID of dti_securities, same MARKET_VALUE_CHF. ASSET_TYPE mapped from the WLK cluster. HQLA_ELIGIBLE is
FALSE for encumbered securities and WLK IX (INELIGIBILITY_REASON). Credit claims (WLK X) are not included.
Also ENTITY_ID, ISSUER_GROUP, INDUSTRY, WLK_CLUSTER, WLK_HAIRCUT, NOMINAL_CHF, COLLATERAL_VALUE_CHF,
IS_ENCUMBERED, IS_MOBILISED, RUN_ID.

## deposit_balances_YYYYMMDD.csv
ACCOUNT_ID, AS_OF_DATE, CUSTOMER_ID, DEPOSIT_TYPE, CURRENCY, BALANCE_CCY, BALANCE_CHF, FX_RATE, IS_INSURED,
PRODUCT_COUNT, ACCOUNT_TENURE_DAYS, HAS_DIRECT_DEBIT, IS_OPERATIONAL, COUNTERPARTY_TYPE, CUSTOMER_SEGMENT,
ACCOUNT_STATUS, BUSINESS_UNIT, COUNTERPARTY_ID, ENTITY_ID, APPLICATION_ID, AKTIV_PASSIV, ORIG_APPLICATION,
IS_NEW_BUSINESS, RUN_ID.

## contracts_YYYYMMDD.csv / cashflows_YYYYMMDD.csv
Loans, facilities and interbank contracts with their contractual cashflows (next 365 days).
Injected data quality issues: NEW_DERIVATIVE_X (unmapped), SEK (no FX), start after maturity.

## SNAPSHOT_SCOPE (last column of holdings, deposit balances, contracts and cashflows)
DAILY = LCR window (every calendar day), MONTH_END = month-end history of the same simulation before the
window (DtI comparison). Same files, same columns; the LCR engine reads DAILY only.

## lcr_events.csv / dti_events.csv
Showcase events (ground truth for tests), generated from generators/scenarios/lcr_dti_showcase.json.

## dti_securities_YYYYMMDD.csv / dti_security_cashflows_YYYYMMDD.csv (DtI reporting dates)
The securities book (APEL-like): SECURITY_ID, ENTITY_ID, ISIN, WLK_CLUSTER, HQLA_CATEGORY, HAIRCUT, NOMINAL_CHF,
PRICE, MARKET_VALUE_CHF, COLLATERAL_VALUE_CHF, IS_ENCUMBERED, IS_MOBILISED, IS_SHORT_TERM, RUN_ID; cashflow values
spread the market value over coupons / redemption (LATER = beyond 400 days).

## dti_transactions, dti_secured_funding, dti_report_status, dti_corrections, dti_planning
DtI business-day movements, secured funding (one deal per encumbered security), lock status, manual corrections
and planning positions.

## Reporting dates
LCR: every calendar day of the window ending AS_OF_DATE. DtI: Zurich business days of the window plus the last
business day of the months before it. Both end on AS_OF_DATE.
""")


def month_end_business_days(start: date, n: int, holidays: set) -> list[date]:
    """Last business day of each of the n months before the month of `start`."""
    out = []
    first_of_month = start.replace(day=1)
    for _ in range(n):
        d = first_of_month - timedelta(days=1)
        while not is_business_day(d, holidays):
            d -= timedelta(days=1)
        out.append(d)
        first_of_month = d.replace(day=1)
    return sorted(out)


def default_as_of(fx: FXTable) -> date:
    """Last Zurich business day covered by FX rates (or today when no FX files are available)."""
    d = min(date.today(), fx.dates[-1]) if fx.dates else date.today()
    while not is_business_day(d, ZURICH_HOLIDAYS):
        d -= timedelta(days=1)
    return d


OUTPUT_PATTERNS = ["hqla_holdings_*.csv", "deposit_balances_*.csv", "contracts_*.csv", "cashflows_*.csv",
                   "dti_*.csv", "lcr_events.csv", "counterparties.csv", "generation_run.csv", "_manifest.csv",
                   "_generator_lcr_estimate.csv"]


def write_manifest(out_dir: Path):
    tables = {"hqla_holdings_": "LIQI_RAW_TB_HQLA_HOLDINGS", "deposit_balances_": "LIQI_RAW_TB_DEPOSIT_BALANCES",
              "contracts_": "LIQI_RAW_TB_CONTRACTS", "cashflows_": "LIQI_RAW_TB_CASHFLOWS",
              "dti_securities_": "LIQI_RAW_TB_DTI_SECURITIES", "dti_security_cashflows_": "LIQI_RAW_TB_DTI_SECURITY_CASHFLOWS",
              "dti_transactions_": "LIQI_RAW_TB_DTI_TRANSACTIONS", "dti_secured_funding_": "LIQI_RAW_TB_DTI_SECURED_FUNDING",
              "dti_planning": "LIQI_RAW_TB_DTI_PLANNING", "dti_report_status": "LIQI_RAW_TB_DTI_REPORT_STATUS",
              "dti_corrections": "LIQI_RAW_TB_DTI_CORRECTIONS", "dti_events": "LIQI_RAW_TB_DTI_EVENTS",
              "lcr_events": "LIQI_RAW_TB_EVENTS", "counterparties": "LIQI_RAW_TB_COUNTERPARTIES",
              "generation_run": "LIQI_RAW_TB_GENERATION_RUN"}
    counts = {}
    for f in sorted(out_dir.glob("*.csv")):
        for prefix, table in sorted(tables.items(), key=lambda x: -len(x[0])):
            if f.name.startswith(prefix):
                with open(f) as fh:
                    counts[table] = counts.get(table, 0) + max(0, sum(1 for _ in fh) - 1)
                break
    pd.DataFrame(sorted(counts.items()), columns=["TABLE_NAME", "ROW_COUNT"]).to_csv(out_dir / "_manifest.csv", index=False)


def main():
    p = argparse.ArgumentParser(description="Generate the persistent, calibrated LCR + DtI liquidity book")
    p.add_argument("--days", type=int, default=90, help="LCR window length in calendar days ending --as-of-date")
    p.add_argument("--as-of-date", type=str, default=None,
                   help="YYYY-MM-DD latest reporting date (default: last Zurich business day covered by FX)")
    p.add_argument("--customers", type=int, default=1000, help="Synthetic customers if no --customer-file")
    p.add_argument("--output-dir", type=str, required=True)
    p.add_argument("--customer-file", type=str, default=None)
    p.add_argument("--fx-dir", type=str, default=None, help="Directory with fx_rates_*.csv (default: <output-dir>/../fx_rates)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--target-lcr", type=float, default=135.0)
    p.add_argument("--scenario", type=str, default="lcr_dti_showcase", help="generators/scenarios/<name>.json")
    p.add_argument("--no-events", action="store_true", help="Disable the deterministic showcase events")
    p.add_argument("--history-month-ends", type=int, default=15,
                   help="DtI month-end history snapshots before the window (0 = none)")
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    fx_dir = Path(args.fx_dir) if args.fx_dir else out_dir.parent / "fx_rates"
    fx = FXTable(fx_dir)
    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else default_as_of(fx)
    if not is_business_day(as_of, ZURICH_HOLIDAYS):
        p.error(f"--as-of-date {as_of} is not a Zurich business day")
    window_start = as_of - timedelta(days=args.days - 1)
    hist_dates = month_end_business_days(window_start, args.history_month_ends, ZURICH_HOLIDAYS)
    window_bdays = [d for d in (window_start + timedelta(days=i) for i in range(args.days))
                    if is_business_day(d, ZURICH_HOLIDAYS)]
    report_dates = hist_dates + window_bdays
    start = hist_dates[0] if hist_dates else window_start
    if args.customer_file and Path(args.customer_file).exists():
        customer_ids = pd.read_csv(args.customer_file)["customer_id"].astype(str).tolist()
    else:
        customer_ids = [f"CUST_{i+1:05d}" for i in range(args.customers)]
    scenario = ref.scenario(args.scenario)
    run_key = json.dumps({"v": GENERATOR_VERSION, "seed": args.seed, "as_of": as_of.isoformat(), "days": args.days,
                              "hist": args.history_month_ends, "target": args.target_lcr, "scenario": scenario,
                              "events": not args.no_events, "customers": customer_ids}, sort_keys=True)
    run_id = "LIQ-" + hashlib.sha256(run_key.encode()).hexdigest()[:12].upper()

    out_dir.mkdir(parents=True, exist_ok=True)
    for pattern in OUTPUT_PATTERNS:  # drop files of earlier runs (other as-of dates) so they are never uploaded
        for f in out_dir.glob(pattern):
            f.unlink()

    def book(adjust: float):
        s = DtiSecuritiesGenerator(start, as_of, ZURICH_HOLIDAYS, seed=args.seed + 11, scenario=scenario)
        return LCRBookGenerator(customer_ids, start, as_of, args.seed, args.target_lcr, fx, s, scenario,
                                not args.no_events, scale_adjust=adjust)

    # calibration pass: the BASE LCR is pinned to the target on the as-of date (the book drifts
    # between the first month-end and the window); NET scales ~linearly with the wholesale book
    adjust = book(1.0).probe_lcr(as_of, as_of) / args.target_lcr
    gen = book(adjust)
    summary = gen.run(out_dir, window_start, hist_dates, report_dates, run_id)
    pd.DataFrame([{"RUN_ID": run_id, "AS_OF_DATE": as_of.isoformat(), "WINDOW_START": window_start.isoformat(),
                       "HISTORY_START": start.isoformat(), "SEED": args.seed, "TARGET_LCR": args.target_lcr,
                       "BOOK_SCALE": round(gen.scale, 6), "SCENARIO": args.scenario, "CUSTOMERS": len(customer_ids),
                       "GENERATOR_VERSION": GENERATOR_VERSION}]).to_csv(out_dir / "generation_run.csv", index=False)
    summary.to_csv(out_dir / "_generator_lcr_estimate.csv", index=False)
    write_manifest(out_dir)

    lcr = summary["LCR"]
    t = gen.totals
    print("✅ LCR + DtI book generation: SUCCESS")
    print(f"   - Run {run_id}: as-of {as_of}, LCR window {window_start} .. {as_of}, history from {start}")
    print(f"   - Book scale (wholesale): {gen.scale:.3f} | counterparties {len(gen.counterparties):,} | "
          f"deposits {len(gen.deposits):,} | contracts {len(gen.contracts):,}")
    print(f"   - Rows: holdings {t['h']:,}, deposits {t['d']:,}, contracts {t['c']:,}, cashflows {t['f']:,}, "
          f"secured funding {t['s']:,}")
    print(f"   - DtI: {t['hist']} month-ends, {len(report_dates)} reporting dates, securities {t['securities']:,}, "
          f"cashflows {t['security_cashflows']:,}, transactions {t['transactions']:,}")
    print(f"   - FX source: {'generated fx_rates (' + str(len(fx.dates)) + ' days)' if fx.dates else 'fallback constants'}")
    print(f"   - BASE LCR (generator estimate): min {lcr.min():.1f}% / avg {lcr.mean():.1f}% / max {lcr.max():.1f}% | "
          f"latest {lcr.iloc[-1]:.1f}% | days < 100%: {(lcr < 100).sum()}")
    for e in gen.events:
        row = summary[summary["AS_OF_DATE"] == e.event_date]
        prev = summary[summary["AS_OF_DATE"] < e.event_date].tail(1)
        if len(row) and len(prev):
            print(f"   - {e.event_id} {e.event_date} {e.event_type}: LCR {prev['LCR'].iloc[0]:.1f}% -> {row['LCR'].iloc[0]:.1f}%")


if __name__ == "__main__":
    main()
