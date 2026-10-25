#!/usr/bin/env python3
"""
FINMA LCR Data Generator (persistent book, calibrated, event-driven)
====================================================================
Generates a realistic, deterministic liquidity book for a Swiss regional bank
(balance sheet ~CHF 4-6bn) and writes daily snapshots for the LCR showcase.

Design
------
- Persistent book: counterparties, deposit accounts, HQLA holdings and
  contracts are created ONCE and evolve day by day (no daily re-draw).
    * Deposits follow a mean-reverting AR(1) process; net deposit flows settle
      against the SNB reserve account (balance-sheet identity).
    * Bond / equity prices follow small random walks.
    * Loans, interbank loans/borrowings roll over at maturity, so contracts
      naturally enter / leave the 30-day LCR window.
    * Weekends are flat (no business activity), FX uses the daily end-of-day rate (one row per calendar day).
- Calibration: day-0 HQLA is scaled so the BASE LCR equals --target-lcr.
- Deterministic showcase events (also written to lcr_events.csv):
    T-60  corporate deposit withdrawal (CHF 50M)           -> LCR drop
    T-45..T-40 market stress (price shock, corporate run,
               short-term interbank funding enters window) -> breach < 100%
    T-38  recovery (funding rolled, SNB reserves purchased) -> bump
    T-20  large corporate loan book matures within 30 days -> inflow bump
    T-3   second corporate withdrawal                       -> recent drop
- Data quality injection (~0.1% of contracts): unknown product
  NEW_DERIVATIVE_X, currency without FX rate (SEK), maturity before start.

Output files (in --output-dir)
------------------------------
  counterparties.csv               static counterparty master
  hqla_holdings_YYYYMMDD.csv       daily HQLA positions
  deposit_balances_YYYYMMDD.csv    daily deposit balances
  contracts_YYYYMMDD.csv           daily loans / facilities / interbank contracts
  cashflows_YYYYMMDD.csv           daily contractual cashflow schedule (next 365 days)
  lcr_events.csv                   injected showcase events (ground truth for tests)
  README.md                        data dictionary of all files

Regulatory basis: FINMA Liquidity Ordinance (LiqV) / Circular 2015/2, Basel III LCR.
"""

import argparse
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Regulatory parameters (BASE scenario) - mirrored by the SQL engine seeds
# ---------------------------------------------------------------------------
HQLA_TYPES = {
    # asset_type: (level, haircut_factor)
    "CASH_SNB": ("L1", 1.00),
    "CASH_VAULT": ("L1", 1.00),
    "GOVT_BOND_CHF": ("L1", 1.00),
    "GOVT_BOND_FOREIGN": ("L1", 1.00),
    "CANTON_BOND": ("L2A", 0.85),
    "COVERED_BOND": ("L2A", 0.85),
    "CORPORATE_BOND_AA": ("L2A", 0.85),
    "CORPORATE_BOND_A_BBB": ("L2B", 0.50),
    "EQUITY_SMI": ("L2B", 0.50),
}

DEPOSIT_TYPES = {
    # deposit_type: (counterparty_type, base_run_off, allows_discount, is_operational)
    "RETAIL_STABLE_INSURED": ("RETAIL", 0.03, True, True),
    "RETAIL_STABLE": ("RETAIL", 0.05, True, True),
    "RETAIL_LESS_STABLE": ("RETAIL", 0.10, False, False),
    "CORPORATE_OPERATIONAL": ("CORPORATE", 0.25, False, True),
    "CORPORATE_NON_OPERATIONAL": ("CORPORATE", 0.40, False, False),
    "FINANCIAL_INSTITUTION": ("FINANCIAL_INSTITUTION", 1.00, False, False),
}

# contract product: (direction, sector, rate) - rate = inflow / drawdown / run-off
CONTRACT_RULES = {
    ("LOAN_RETAIL", "RETAIL"): ("ASSET", 0.50),
    ("LOAN_CORP", "CORPORATE"): ("ASSET", 0.50),
    ("LOAN_CORP", "SME"): ("ASSET", 0.50),
    ("INTERBANK_LOAN", "FINANCIAL"): ("ASSET", 1.00),
    ("CREDIT_FACILITY", "RETAIL"): ("FACILITY", 0.05),
    ("CREDIT_FACILITY", "CORPORATE"): ("FACILITY", 0.10),
    ("CREDIT_FACILITY", "SME"): ("FACILITY", 0.10),
    ("LIQUIDITY_FACILITY", "CORPORATE"): ("FACILITY", 0.30),
    ("LIQUIDITY_FACILITY", "FINANCIAL"): ("FACILITY", 0.40),
    ("INTERBANK_BORROWING", "FINANCIAL"): ("LIABILITY", 1.00),
}

INFLOW_CAP = 0.75
L2B_CAP_RATIO = 15.0 / 85.0
L2_CAP_RATIO = 2.0 / 3.0

FALLBACK_FX_TO_CHF = {"CHF": 1.0, "EUR": 0.94, "USD": 0.80, "GBP": 1.07}

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
    counterparty_id: Optional[str] = None
    contract_id: Optional[str] = None
    amount_chf: float = 0.0
    expected_direction: str = "DOWN"
    expected_min_delta_pp: float = 1.0


def next_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


class FXTable:
    """CCY->CHF rates derived from the generated fx_rates files (USD base, cross via USD)."""

    def __init__(self, fx_dir: Optional[Path]):
        self.by_date: Dict[date, Dict[str, float]] = {}
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

    def rate(self, ccy: str, d: date) -> Optional[float]:
        if ccy == "CHF":
            return 1.0
        if not self.dates:
            return FALLBACK_FX_TO_CHF.get(ccy)
        # Daily EOD rates exist for every date; fallback to last date <= d only for gaps
        candidates = [x for x in self.dates if x <= d]
        rates = self.by_date[candidates[-1]] if candidates else self.by_date[self.dates[0]]
        return rates.get(ccy, FALLBACK_FX_TO_CHF.get(ccy))


class LCRBookGenerator:
    """Persistent, calibrated liquidity book with deterministic events."""

    def __init__(self, customer_ids: List[str], start: date, days: int, seed: int,
                 target_lcr: float, fx: FXTable, with_events: bool = True):
        self.rng = np.random.default_rng(seed)
        self.customer_ids = customer_ids
        self.start = start
        self.days = days
        self.end = start + timedelta(days=days - 1)
        self.target_lcr = target_lcr
        self.fx = fx
        self.with_events = with_events
        self.counterparties: List[dict] = []
        self.deposits: List[dict] = []
        self.holdings: List[dict] = []
        self.contracts: List[dict] = []
        self.events: List[Event] = []
        self._build_book()
        self._plan_events()
        self._calibrate()

    # ------------------------------------------------------------------ book
    def _build_book(self):
        rng = self.rng
        # Retail counterparties = real customers
        for cid in self.customer_ids:
            self.counterparties.append(dict(COUNTERPARTY_ID=cid, COUNTERPARTY_NAME=f"Customer {cid}",
                                            SECTOR="RETAIL", COUNTRY="CH", RATING=None,
                                            CUSTOMER_ID=cid, BUSINESS_UNIT="RETAIL"))
        # Corporates / SMEs
        names = set()
        for i in range(260):
            while True:
                n = f"{rng.choice(CORP_PREFIX)} {rng.choice(CORP_SUFFIX)} {rng.choice(CORP_FORM)}"
                if n not in names:
                    names.add(n)
                    break
            sector = "CORPORATE" if i < 160 else "SME"
            self.counterparties.append(dict(COUNTERPARTY_ID=f"CP_CORP_{i+1:04d}", COUNTERPARTY_NAME=n,
                                            SECTOR=sector, COUNTRY=str(rng.choice(["CH"] * 8 + ["DE", "AT"])),
                                            RATING=str(rng.choice(["A", "A-", "BBB+", "BBB", "BBB-", "BB+"])),
                                            CUSTOMER_ID=None, BUSINESS_UNIT="CORPORATE"))
        # Event counterparties with fixed names
        self.counterparties.append(dict(COUNTERPARTY_ID="CP_CORP_EVT1", COUNTERPARTY_NAME="Alpenbau Holding AG",
                                        SECTOR="CORPORATE", COUNTRY="CH", RATING="BBB+", CUSTOMER_ID=None,
                                        BUSINESS_UNIT="CORPORATE"))
        self.counterparties.append(dict(COUNTERPARTY_ID="CP_CORP_EVT2", COUNTERPARTY_NAME="Rhein Logistik Group AG",
                                        SECTOR="CORPORATE", COUNTRY="CH", RATING="A-", CUSTOMER_ID=None,
                                        BUSINESS_UNIT="CORPORATE"))
        # Financial institutions
        for i, n in enumerate(BANK_NAMES):
            self.counterparties.append(dict(COUNTERPARTY_ID=f"CP_FI_{i+1:03d}", COUNTERPARTY_NAME=n,
                                            SECTOR="FINANCIAL", COUNTRY=str(rng.choice(["CH"] * 6 + ["DE", "GB", "FR", "LU"])),
                                            RATING=str(rng.choice(["AA", "AA-", "A+", "A", "A-"])),
                                            CUSTOMER_ID=None, BUSINESS_UNIT="TREASURY"))
        # Issuers for HQLA
        issuers = [("ISS_SNB", "Swiss National Bank", "CENTRAL_BANK"), ("ISS_VAULT", "Own vault cash", "CENTRAL_BANK"),
                   ("ISS_CH", "Swiss Confederation", "SOVEREIGN"), ("ISS_DE", "Federal Republic of Germany", "SOVEREIGN"),
                   ("ISS_US", "United States Treasury", "SOVEREIGN"), ("ISS_KT", "Swiss Cantons", "SOVEREIGN"),
                   ("ISS_PFB", "Pfandbriefbank schweizerischer Hypothekarinstitute", "FINANCIAL"),
                   ("ISS_CORP_AA", "AA-rated Corporate Issuers", "CORPORATE"),
                   ("ISS_CORP_BBB", "A/BBB-rated Corporate Issuers", "CORPORATE"),
                   ("ISS_SMI", "SMI Constituents", "CORPORATE")]
        for iid, n, s in issuers:
            self.counterparties.append(dict(COUNTERPARTY_ID=iid, COUNTERPARTY_NAME=n, SECTOR=s, COUNTRY="CH",
                                            RATING=None, CUSTOMER_ID=None, BUSINESS_UNIT="TREASURY"))

        # ---- deposits: retail (savings + current), corporate, FI
        acct_no = 0
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
                acct_no += 1
        corp_cps = [c for c in self.counterparties if c["SECTOR"] in ("CORPORATE", "SME") and "EVT" not in c["COUNTERPARTY_ID"]]
        for c in corp_cps:
            for k, dtype in enumerate(["CORPORATE_OPERATIONAL", "CORPORATE_NON_OPERATIONAL"]):
                if c["SECTOR"] == "SME" and k == 1 and rng.random() < 0.5:
                    continue
                bal = float(rng.lognormal(math.log(1_800_000 if c["SECTOR"] == "CORPORATE" else 600_000), 0.8))
                self.deposits.append(self._new_deposit(f"{c['COUNTERPARTY_ID']}_DEP_{k+1:02d}", c["COUNTERPARTY_ID"],
                                                       dtype, str(rng.choice(["CHF"] * 8 + ["EUR", "USD"])),
                                                       bal, "CORPORATE", "CORPORATE", sigma=0.012))
        # event counterparties: large non-operational balances
        self.deposits.append(self._new_deposit("CP_CORP_EVT1_DEP_01", "CP_CORP_EVT1", "CORPORATE_NON_OPERATIONAL",
                                               "CHF", 72_000_000, "CORPORATE", "CORPORATE", sigma=0.002))
        self.deposits.append(self._new_deposit("CP_CORP_EVT2_DEP_01", "CP_CORP_EVT2", "CORPORATE_NON_OPERATIONAL",
                                               "CHF", 58_000_000, "CORPORATE", "CORPORATE", sigma=0.002))
        self.deposits.append(self._new_deposit("CP_CORP_EVT2_DEP_02", "CP_CORP_EVT2", "CORPORATE_OPERATIONAL",
                                               "CHF", 25_000_000, "CORPORATE", "CORPORATE", sigma=0.004))
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
            self.contracts.append(dict(CONTRACT_ID=f"CTR_{cno:06d}", COUNTERPARTY_ID=cp, PRODUCT_TYPE=product,
                                       SECTOR=sector, BUSINESS_UNIT=bu, CURRENCY=ccy, RATING=rating,
                                       START_DATE=st, MATURITY_DATE=mat, OUTSTANDING_CCY=outstanding,
                                       UNDRAWN_CCY=undrawn, INTEREST_RATE=rate, IS_PERFORMING=performing,
                                       TENOR_DAYS=tenor_days, AMORTIZING=product == "LOAN_RETAIL"))
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
        # Event contracts (maturities set by the event plan)
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

        # ---- HQLA holdings (sized by calibration)
        hno = 0

        def add_holding(asset_type, issuer, name, ccy, mv_chf, rating=None, maturity_days=None, isin=None,
                        eligible=True, reason=None, vol=0.001):
            nonlocal hno
            hno += 1
            self.holdings.append(dict(HOLDING_ID=f"HOLD-{hno:05d}", ASSET_TYPE=asset_type, ISSUER_ID=issuer,
                                      SECURITY_NAME=name, CURRENCY=ccy, BASE_MV_CHF=mv_chf, MV_CHF=mv_chf, PRICE_IDX=1.0,
                                      CREDIT_RATING=rating, ISIN=isin, VOL=vol,
                                      MATURITY_DATE=(self.start + timedelta(days=maturity_days)) if maturity_days else None,
                                      SMI_CONSTITUENT=asset_type == "EQUITY_SMI", HQLA_ELIGIBLE=eligible,
                                      INELIGIBILITY_REASON=reason,
                                      PORTFOLIO_CODE="TREASURY_LIQ" if asset_type.startswith("CASH") else
                                      str(rng.choice(["TREASURY_LIQ", "ALM_BUFFER", "TREASURY_INV"])),
                                      CUSTODIAN="SNB" if asset_type == "CASH_SNB" else "OWN" if asset_type == "CASH_VAULT"
                                      else str(rng.choice(["SIX SIS", "SIX SIS", "EUROCLEAR", "CLEARSTREAM"]))))
            return self.holdings[-1]

        self.snb = add_holding("CASH_SNB", "ISS_SNB", "SNB sight deposits", "CHF", 600e6, vol=0.0)
        add_holding("CASH_VAULT", "ISS_VAULT", "Vault cash", "CHF", 25e6, vol=0.0)
        for i in range(12):
            add_holding("GOVT_BOND_CHF", "ISS_CH", f"Swiss Confederation {i+1:02d}", "CHF", 30e6, "AAA",
                        int(rng.integers(400, 3650)), f"CH{rng.integers(10**9, 10**10-1):010d}", vol=0.0015)
        for i in range(6):
            iss, ccy = (("ISS_DE", "EUR") if i % 2 == 0 else ("ISS_US", "USD"))
            add_holding("GOVT_BOND_FOREIGN", iss, f"{'Bund' if ccy == 'EUR' else 'UST'} {i+1:02d}", ccy, 25e6, "AA+",
                        int(rng.integers(400, 3650)), f"{'DE' if ccy == 'EUR' else 'US'}{rng.integers(10**9, 10**10-1):010d}",
                        vol=0.002)
        for i in range(10):
            add_holding("CANTON_BOND", "ISS_KT", f"Canton bond {i+1:02d}", "CHF", 18e6, "AA",
                        int(rng.integers(400, 3650)), f"CH{rng.integers(10**9, 10**10-1):010d}", vol=0.002)
        for i in range(10):
            add_holding("COVERED_BOND", "ISS_PFB", f"Pfandbrief {i+1:02d}", "CHF", 15e6, "AAA",
                        int(rng.integers(400, 3650)), f"CH{rng.integers(10**9, 10**10-1):010d}", vol=0.002)
        for i in range(6):
            add_holding("CORPORATE_BOND_AA", "ISS_CORP_AA", f"Corporate AA bond {i+1:02d}", "CHF", 10e6, "AA-",
                        int(rng.integers(400, 2500)), f"CH{rng.integers(10**9, 10**10-1):010d}", vol=0.0025)
        for i in range(5):
            add_holding("CORPORATE_BOND_A_BBB", "ISS_CORP_BBB", f"Corporate BBB bond {i+1:02d}", "CHF", 8e6,
                        str(rng.choice(["A", "A-", "BBB+"])), int(rng.integers(400, 2500)),
                        f"CH{rng.integers(10**9, 10**10-1):010d}", vol=0.003)
        for i, isin in enumerate(["CH0012032048", "CH0244767585", "CH0038863350", "CH0012005267", "CH0012221716",
                                  "CH0024608827", "CH0012138530", "CH0012032113", "CH0025751329", "CH0210483332"]):
            add_holding("EQUITY_SMI", "ISS_SMI", f"SMI equity {isin[-4:]}", "CHF", 6e6, None, None, isin, vol=0.01)
        add_holding("CORPORATE_BOND_A_BBB", "ISS_CORP_BBB", "Corporate bond (downgraded)", "CHF", 7e6, "BB+",
                    900, f"CH{rng.integers(10**9, 10**10-1):010d}", eligible=False,
                    reason="Rating downgraded to BB+ (below BBB- minimum for L2B)")

    def _new_deposit(self, acc, cust, dtype, ccy, bal, seg, bu, sigma):
        cpt, _, allows, oper = DEPOSIT_TYPES[dtype]
        return dict(ACCOUNT_ID=acc, CUSTOMER_ID=cust, DEPOSIT_TYPE=dtype, CURRENCY=ccy, BASE_BAL=bal, BAL=bal,
                    SIGMA=sigma, IS_INSURED=cpt == "RETAIL" and bal <= 100_000,
                    PRODUCT_COUNT=int(self.rng.integers(1, 6)) if allows else 1,
                    TENURE=int(self.rng.integers(30, 3650)),
                    HAS_DIRECT_DEBIT=bool(self.rng.random() < (0.7 if cpt == "RETAIL" else 0.3)),
                    IS_OPERATIONAL=oper, COUNTERPARTY_TYPE=cpt, CUSTOMER_SEGMENT=seg,
                    ACCOUNT_STATUS="ACTIVE" if self.rng.random() < 0.98 or "EVT" in acc else "DORMANT",
                    BUSINESS_UNIT=bu)

    # --------------------------------------------------------------- events
    def _plan_events(self):
        if not self.with_events:
            return
        t = self.end
        self.evt_dates = {
            "WITHDRAWAL_1": next_weekday(t - timedelta(days=60)),
            "STRESS_START": next_weekday(t - timedelta(days=45)),
            "RECOVERY": next_weekday(t - timedelta(days=38)),
            "INFLOW": next_weekday(t - timedelta(days=20)),
            "WITHDRAWAL_2": t - timedelta(days=3 + max(0, (t - timedelta(days=3)).weekday() - 4)),
        }
        stress_days = []
        d = self.evt_dates["STRESS_START"]
        while len(stress_days) < 5:
            if d.weekday() < 5:
                stress_days.append(d)
            d += timedelta(days=1)
        self.stress_days = stress_days
        # Event funding: matures far out; enters 30d window on STRESS_START, rolled on RECOVERY
        self.evt_funding["MATURITY_DATE"] = self.evt_dates["STRESS_START"] + timedelta(days=20)
        self.evt_funding["PRE_EVENT_MATURITY"] = self.evt_dates["STRESS_START"] + timedelta(days=200)
        # Event loans: maturity pulled inside 30d window on INFLOW date (mature 25 days later)
        for ln in self.evt_loans:
            ln["MATURITY_DATE"] = self.evt_dates["INFLOW"] + timedelta(days=25)
            ln["PRE_EVENT_MATURITY"] = self.evt_dates["INFLOW"] + timedelta(days=380)
        ev = self.evt_dates
        self.events = [
            Event("EVT_01", ev["WITHDRAWAL_1"], "DEPOSIT_WITHDRAWAL",
                  "Alpenbau Holding AG withdraws CHF 50M from its non-operational deposit (settled from SNB reserves)",
                  "CP_CORP_EVT1", "CP_CORP_EVT1_DEP_01", 50e6, "DOWN", 1.0),
            Event("EVT_02", ev["STRESS_START"], "MARKET_STRESS",
                  "Market stress begins: bond/equity price shock, corporate deposit run and CHF 250M short-term "
                  "interbank funding from Zentralbank Partner AG falls inside the 30-day window",
                  "CP_FI_008", self.evt_funding["CONTRACT_ID"], 250e6, "DOWN", 10.0),
            Event("EVT_03", ev["RECOVERY"], "RECOVERY",
                  "Treasury rolls the CHF 250M interbank funding to 6 months and buys CHF 300M SNB reserves "
                  "with new term funding", "CP_FI_008", self.evt_funding["CONTRACT_ID"], 300e6, "UP", 10.0),
            Event("EVT_04", ev["INFLOW"], "LOAN_INFLOW",
                  "CHF 205M corporate loan book (5 loans) scheduled to mature within 30 days - contractual inflows",
                  self.evt_loans[0]["COUNTERPARTY_ID"], self.evt_loans[0]["CONTRACT_ID"], 205e6, "UP", 3.0),
            Event("EVT_05", ev["WITHDRAWAL_2"], "DEPOSIT_WITHDRAWAL",
                  "Rhein Logistik Group AG withdraws CHF 40M from its non-operational deposit",
                  "CP_CORP_EVT2", "CP_CORP_EVT2_DEP_01", 40e6, "DOWN", 0.5),
        ]

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

    def _cashflows(self, c, d: date) -> List[dict]:
        """Contractual cashflows of a contract within the next 365 days."""
        if c["PRODUCT_TYPE"] in ("CREDIT_FACILITY", "LIQUIDITY_FACILITY") or c["OUTSTANDING_CCY"] <= 0:
            return []
        if c.get("DQ_INVALID_DATE"):
            return []
        horizon = d + timedelta(days=365)
        direction = "OUT" if c["PRODUCT_TYPE"] == "INTERBANK_BORROWING" else "IN"
        out = []
        mat = c["MATURITY_DATE"]
        if c["AMORTIZING"]:
            # quarterly amortization (1.25% per quarter) + interest
            q = date(d.year, ((d.month - 1) // 3) * 3 + 1, 1)
            bal = c["OUTSTANDING_CCY"]
            while True:
                q = (q.replace(day=1) + timedelta(days=95)).replace(day=1)
                if q > horizon or q > mat:
                    break
                prin = bal * 0.0125
                out.append(dict(CF_DATE=q, CF_TYPE="PRINCIPAL", DIRECTION=direction, AMOUNT_CCY=prin))
                out.append(dict(CF_DATE=q, CF_TYPE="INTEREST", DIRECTION=direction, AMOUNT_CCY=bal * c["INTEREST_RATE"] / 4))
                bal -= prin
            if mat <= horizon:
                out.append(dict(CF_DATE=mat, CF_TYPE="PRINCIPAL", DIRECTION=direction, AMOUNT_CCY=bal))
        else:
            if mat <= horizon:
                out.append(dict(CF_DATE=mat, CF_TYPE="PRINCIPAL", DIRECTION=direction, AMOUNT_CCY=c["OUTSTANDING_CCY"]))
            # quarterly interest until maturity
            k = 1
            while True:
                q = d + timedelta(days=91 * k)
                if q > min(mat, horizon):
                    break
                out.append(dict(CF_DATE=q, CF_TYPE="INTEREST", DIRECTION=direction,
                                AMOUNT_CCY=c["OUTSTANDING_CCY"] * c["INTEREST_RATE"] / 4))
                k += 1
            if mat <= horizon and c["INTEREST_RATE"] > 0:
                out.append(dict(CF_DATE=mat, CF_TYPE="INTEREST", DIRECTION=direction,
                                AMOUNT_CCY=c["OUTSTANDING_CCY"] * c["INTEREST_RATE"] * ((mat - d).days % 91) / 365))
        return out

    def compute_lcr(self, d: date) -> dict:
        l1 = l2a = l2b = 0.0
        for h in self.holdings:
            if not h["HQLA_ELIGIBLE"]:
                continue
            lvl, f = HQLA_TYPES[h["ASSET_TYPE"]]
            w = h["MV_CHF"] * f
            if lvl == "L1":
                l1 += w
            elif lvl == "L2A":
                l2a += w
            else:
                l2b += w
        out = 0.0
        for dep in self.deposits:
            if dep["ACCOUNT_STATUS"] != "ACTIVE":
                continue
            out += dep["BAL"] * (self.fx.rate(dep["CURRENCY"], d) or 0) * self.deposit_rate(dep)
        inflow = 0.0
        for c in self.contracts:
            if not self._contract_valid(c):
                continue
            fx = self.fx.rate(c["CURRENCY"], d) or 0
            key = (c["PRODUCT_TYPE"], c["SECTOR"])
            if key not in CONTRACT_RULES:
                continue
            kind, rate = CONTRACT_RULES[key]
            if kind == "FACILITY":
                out += c["UNDRAWN_CCY"] * fx * rate
                continue
            for cf in self._cashflows(c, d):
                if d < cf["CF_DATE"] <= d + timedelta(days=30):
                    if kind == "LIABILITY":
                        out += cf["AMOUNT_CCY"] * fx * rate
                    elif c["IS_PERFORMING"]:
                        inflow += cf["AMOUNT_CCY"] * fx * rate
        l2b_c = min(l2b, L2B_CAP_RATIO * (l1 + l2a))
        l2 = min(l2a + l2b_c, L2_CAP_RATIO * l1)
        hqla = l1 + l2
        inflow_c = min(inflow, INFLOW_CAP * out)
        net = out - inflow_c
        return dict(L1=l1, L2A=l2a, L2B=l2b, HQLA=hqla, OUTFLOW=out, INFLOW=inflow, NET=net,
                    LCR=hqla / net * 100 if net > 0 else float("nan"))

    def _calibrate(self):
        # Calibrate on the day-0 state (event contracts outside the 30-day window)
        self._pre_event_state(self.start)
        base = self.compute_lcr(self.start)
        needed = self.target_lcr / 100.0 * base["NET"]
        scale = needed / base["HQLA"]
        for h in self.holdings:
            h["BASE_MV_CHF"] *= scale
            h["MV_CHF"] = h["BASE_MV_CHF"]
        if self.with_events:
            self._size_events(base["NET"])

    def _size_events(self, n0: float):
        """Scale event amounts to the size of the book (day-0 net cash outflows)."""
        self.n0 = n0
        self.evt_funding["OUTSTANDING_CCY"] = round(0.27 * n0, -6)
        loan_total = round(0.11 * n0, -6)
        for ln, w in zip(self.evt_loans, [0.27, 0.22, 0.20, 0.17, 0.14]):
            ln["OUTSTANDING_CCY"] = round(loan_total * w, -5)
        self.recovery_reserves = round(0.08 * n0, -6)
        fund = self.evt_funding["OUTSTANDING_CCY"]
        loans = sum(ln["OUTSTANDING_CCY"] for ln in self.evt_loans)
        ev = self.evt_dates
        self.events = [
            Event("EVT_01", ev["WITHDRAWAL_1"], "DEPOSIT_WITHDRAWAL",
                  "Alpenbau Holding AG withdraws CHF 50M from its non-operational deposit (settled from SNB reserves)",
                  "CP_CORP_EVT1", "CP_CORP_EVT1_DEP_01", 50e6, "DOWN", 1.0),
            Event("EVT_02", ev["STRESS_START"], "MARKET_STRESS",
                  f"Market stress begins: bond/equity price shock, corporate deposit run and CHF {fund/1e6:.0f}M "
                  "short-term interbank funding from Zentralbank Partner AG falls inside the 30-day window",
                  "CP_FI_019", self.evt_funding["CONTRACT_ID"], fund, "DOWN", 10.0),
            Event("EVT_03", ev["RECOVERY"], "RECOVERY",
                  f"Treasury rolls the CHF {fund/1e6:.0f}M interbank funding to 6 months and buys CHF "
                  f"{self.recovery_reserves/1e6:.0f}M SNB reserves with new term funding",
                  "CP_FI_019", self.evt_funding["CONTRACT_ID"], fund, "UP", 10.0),
            Event("EVT_04", ev["INFLOW"], "LOAN_INFLOW",
                  f"CHF {loans/1e6:.0f}M corporate loan book (5 loans) scheduled to mature within 30 days - "
                  "contractual inflows", self.evt_loans[0]["COUNTERPARTY_ID"], self.evt_loans[0]["CONTRACT_ID"],
                  loans, "UP", 3.0),
            Event("EVT_05", ev["WITHDRAWAL_2"], "DEPOSIT_WITHDRAWAL",
                  "Rhein Logistik Group AG withdraws CHF 40M from its non-operational deposit",
                  "CP_CORP_EVT2", "CP_CORP_EVT2_DEP_01", 40e6, "DOWN", 1.0),
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
        for h in self.holdings:
            if h["ASSET_TYPE"].startswith("CASH"):
                continue
            h["PRICE_IDX"] *= (1 + h["VOL"] * rng.standard_normal())
            h["MV_CHF"] = h["BASE_MV_CHF"] * h["PRICE_IDX"]
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

    def _apply_events(self, d: date):
        if not self.with_events:
            return
        ev = self.evt_dates
        dep = {x["ACCOUNT_ID"]: x for x in self.deposits}
        if d == ev["WITHDRAWAL_1"]:
            a = dep["CP_CORP_EVT1_DEP_01"]
            a["BAL"] -= 50e6
            a["BASE_BAL"] -= 50e6
            self.snb["MV_CHF"] -= 50e6
        if d in self.stress_days:
            k = self.stress_days.index(d) + 1
            for h in self.holdings:
                if h["ASSET_TYPE"].startswith("CASH"):
                    continue
                shock = {"GOVT_BOND_CHF": 0.002, "GOVT_BOND_FOREIGN": 0.003, "CANTON_BOND": 0.012,
                         "COVERED_BOND": 0.012, "CORPORATE_BOND_AA": 0.016, "CORPORATE_BOND_A_BBB": 0.02,
                         "EQUITY_SMI": 0.025}.get(h["ASSET_TYPE"], 0.0)
                h["PRICE_IDX"] *= (1 - shock)
                h["MV_CHF"] = h["BASE_MV_CHF"] * h["PRICE_IDX"]
            # corporate operational deposits run 1.5% per day
            run = 0.0
            for a in self.deposits:
                if a["DEPOSIT_TYPE"] == "CORPORATE_OPERATIONAL" and a["ACCOUNT_STATUS"] == "ACTIVE":
                    w = a["BAL"] * 0.035
                    a["BAL"] -= w
                    a["BASE_BAL"] -= w
                    run += w * (self.fx.rate(a["CURRENCY"], d) or 1.0)
            self.snb["MV_CHF"] -= run
        if d == ev["RECOVERY"]:
            self.evt_funding["START_DATE"] = d
            self.evt_funding["MATURITY_DATE"] = d + timedelta(days=182)
            self.snb["MV_CHF"] += self.recovery_reserves
            for h in self.holdings:
                if not h["ASSET_TYPE"].startswith("CASH"):
                    h["PRICE_IDX"] = 1 - (1 - h["PRICE_IDX"]) * 0.5
                    h["MV_CHF"] = h["BASE_MV_CHF"] * h["PRICE_IDX"]
        if d == ev["WITHDRAWAL_2"]:
            a = dep["CP_CORP_EVT2_DEP_01"]
            a["BAL"] -= 40e6
            a["BASE_BAL"] -= 40e6
            self.snb["MV_CHF"] -= 40e6

    def _pre_event_state(self, d: date):
        """Before their event dates, event contracts sit outside the 30-day window."""
        if not self.with_events:
            return
        ev = self.evt_dates
        f = self.evt_funding
        if d < ev["STRESS_START"]:
            f["MATURITY_DATE"] = f["PRE_EVENT_MATURITY"]
        elif d < ev["RECOVERY"]:
            f["MATURITY_DATE"] = ev["STRESS_START"] + timedelta(days=20)
        for ln in self.evt_loans:
            if d < ev["INFLOW"]:
                ln["MATURITY_DATE"] = ln["PRE_EVENT_MATURITY"]
            elif ln["OUTSTANDING_CCY"] > 0:
                ln["MATURITY_DATE"] = ev["INFLOW"] + timedelta(days=25)

    # ---------------------------------------------------------------- write
    def _rows(self, d: date):
        ds = d.isoformat()
        hold = []
        for h in self.holdings:
            fx = self.fx.rate(h["CURRENCY"], d) or 1.0
            hold.append(dict(HOLDING_ID=h["HOLDING_ID"], AS_OF_DATE=ds, ASSET_TYPE=h["ASSET_TYPE"], ISIN=h["ISIN"],
                             SECURITY_NAME=h["SECURITY_NAME"], CURRENCY=h["CURRENCY"],
                             QUANTITY=round(h["MV_CHF"] / 250.0, 0) if h["ASSET_TYPE"] == "EQUITY_SMI" else None,
                             MARKET_VALUE_CCY=round(h["MV_CHF"] / fx, 2), MARKET_VALUE_CHF=round(h["MV_CHF"], 2),
                             FX_RATE=None if h["CURRENCY"] == "CHF" else round(fx, 6),
                             MATURITY_DATE=h["MATURITY_DATE"].isoformat() if h["MATURITY_DATE"] else None,
                             CREDIT_RATING=h["CREDIT_RATING"], SMI_CONSTITUENT=h["SMI_CONSTITUENT"],
                             HQLA_ELIGIBLE=h["HQLA_ELIGIBLE"], PORTFOLIO_CODE=h["PORTFOLIO_CODE"],
                             CUSTODIAN=h["CUSTODIAN"], INELIGIBILITY_REASON=h["INELIGIBILITY_REASON"],
                             BUSINESS_UNIT="TREASURY", ISSUER_ID=h["ISSUER_ID"]))
        deps = []
        for a in self.deposits:
            fx = self.fx.rate(a["CURRENCY"], d) or 1.0
            deps.append(dict(ACCOUNT_ID=a["ACCOUNT_ID"], AS_OF_DATE=ds, CUSTOMER_ID=a["CUSTOMER_ID"],
                             DEPOSIT_TYPE=a["DEPOSIT_TYPE"], CURRENCY=a["CURRENCY"],
                             BALANCE_CCY=round(a["BAL"], 2), BALANCE_CHF=round(a["BAL"] * fx, 2),
                             FX_RATE=None if a["CURRENCY"] == "CHF" else round(fx, 6),
                             IS_INSURED=a["IS_INSURED"], PRODUCT_COUNT=a["PRODUCT_COUNT"],
                             ACCOUNT_TENURE_DAYS=a["TENURE"], HAS_DIRECT_DEBIT=a["HAS_DIRECT_DEBIT"],
                             IS_OPERATIONAL=a["IS_OPERATIONAL"], COUNTERPARTY_TYPE=a["COUNTERPARTY_TYPE"],
                             CUSTOMER_SEGMENT=a["CUSTOMER_SEGMENT"], ACCOUNT_STATUS=a["ACCOUNT_STATUS"],
                             BUSINESS_UNIT=a["BUSINESS_UNIT"], COUNTERPARTY_ID=a["CUSTOMER_ID"]))
        ctrs, cfs = [], []
        for c in self.contracts:
            if c["OUTSTANDING_CCY"] <= 0 and c["UNDRAWN_CCY"] <= 0:
                continue
            ctrs.append(dict(CONTRACT_ID=c["CONTRACT_ID"], AS_OF_DATE=ds, COUNTERPARTY_ID=c["COUNTERPARTY_ID"],
                             PRODUCT_TYPE=c["PRODUCT_TYPE"], BUSINESS_UNIT=c["BUSINESS_UNIT"], CURRENCY=c["CURRENCY"],
                             RATING=c["RATING"], START_DATE=c["START_DATE"].isoformat(),
                             MATURITY_DATE=c["MATURITY_DATE"].isoformat(),
                             OUTSTANDING_CCY=round(c["OUTSTANDING_CCY"], 2), UNDRAWN_CCY=round(c["UNDRAWN_CCY"], 2),
                             INTEREST_RATE=round(c["INTEREST_RATE"], 5), IS_PERFORMING=c["IS_PERFORMING"],
                             COUNTERPARTY_SECTOR=c["SECTOR"]))
            for cf in self._cashflows(c, d):
                cfs.append(dict(AS_OF_DATE=ds, CONTRACT_ID=c["CONTRACT_ID"], CF_DATE=cf["CF_DATE"].isoformat(),
                                CF_TYPE=cf["CF_TYPE"], DIRECTION=cf["DIRECTION"], CURRENCY=c["CURRENCY"],
                                AMOUNT_CCY=round(cf["AMOUNT_CCY"], 2)))
        return hold, deps, ctrs, cfs

    def run(self, out_dir: Path, verbose: bool = True) -> pd.DataFrame:
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self.counterparties).to_csv(out_dir / "counterparties.csv", index=False)
        summary = []
        totals = dict(h=0, d=0, c=0, f=0)
        for i in range(self.days):
            d = self.start + timedelta(days=i)
            self._pre_event_state(d)
            if i > 0:
                self._evolve(d)
            self._apply_events(d)
            hold, deps, ctrs, cfs = self._rows(d)
            tag = d.strftime("%Y%m%d")
            pd.DataFrame(hold).to_csv(out_dir / f"hqla_holdings_{tag}.csv", index=False)
            pd.DataFrame(deps).to_csv(out_dir / f"deposit_balances_{tag}.csv", index=False)
            pd.DataFrame(ctrs).to_csv(out_dir / f"contracts_{tag}.csv", index=False)
            pd.DataFrame(cfs).to_csv(out_dir / f"cashflows_{tag}.csv", index=False)
            totals["h"] += len(hold)
            totals["d"] += len(deps)
            totals["c"] += len(ctrs)
            totals["f"] += len(cfs)
            m = self.compute_lcr(d)
            m["AS_OF_DATE"] = d
            summary.append(m)
        ev = pd.DataFrame([dict(EVENT_ID=e.event_id, EVENT_DATE=e.event_date.isoformat(), EVENT_TYPE=e.event_type,
                                DESCRIPTION=e.description, COUNTERPARTY_ID=e.counterparty_id,
                                CONTRACT_ID=e.contract_id, AMOUNT_CHF=e.amount_chf,
                                EXPECTED_DIRECTION=e.expected_direction,
                                EXPECTED_MIN_DELTA_PP=e.expected_min_delta_pp) for e in self.events])
        ev.to_csv(out_dir / "lcr_events.csv", index=False)
        write_data_dictionary(out_dir)
        self.totals = totals
        return pd.DataFrame(summary)


def write_data_dictionary(out_dir: Path):
    (out_dir / "README.md").write_text("""# LCR generated data - data dictionary

All amounts in original currency (`*_CCY`) or Swiss francs (`*_CHF`). One file per snapshot date (`YYYYMMDD`).

## counterparties.csv (static)
| Column | Description |
|---|---|
| COUNTERPARTY_ID | Counterparty key (retail = CRM customer_id, CP_CORP_*, CP_FI_*, ISS_* issuers) |
| COUNTERPARTY_NAME | Legal / display name |
| SECTOR | RETAIL, SME, CORPORATE, FINANCIAL, SOVEREIGN, CENTRAL_BANK - drives LCR run-off / inflow rates |
| COUNTRY | ISO country code of domicile |
| RATING | External rating (corporates, FIs) |
| CUSTOMER_ID | Link to CRM customer master (retail only) |
| BUSINESS_UNIT | Owning business unit (RETAIL, PRIVATE_BANKING, CORPORATE, TREASURY) |

## hqla_holdings_YYYYMMDD.csv
HOLDING_ID (persistent position id), AS_OF_DATE, ASSET_TYPE (CASH_SNB, CASH_VAULT, GOVT_BOND_CHF, GOVT_BOND_FOREIGN,
CANTON_BOND, COVERED_BOND, CORPORATE_BOND_AA, CORPORATE_BOND_A_BBB, EQUITY_SMI), ISIN, SECURITY_NAME, CURRENCY,
QUANTITY (equities), MARKET_VALUE_CCY, MARKET_VALUE_CHF, FX_RATE (CCY->CHF, empty for CHF), MATURITY_DATE,
CREDIT_RATING, SMI_CONSTITUENT, HQLA_ELIGIBLE, PORTFOLIO_CODE, CUSTODIAN, INELIGIBILITY_REASON,
BUSINESS_UNIT (TREASURY), ISSUER_ID (-> counterparties).

## deposit_balances_YYYYMMDD.csv
ACCOUNT_ID, AS_OF_DATE, CUSTOMER_ID (-> counterparties), DEPOSIT_TYPE (run-off class), CURRENCY, BALANCE_CCY,
BALANCE_CHF, FX_RATE, IS_INSURED (<= CHF 100k), PRODUCT_COUNT, ACCOUNT_TENURE_DAYS, HAS_DIRECT_DEBIT, IS_OPERATIONAL,
COUNTERPARTY_TYPE (RETAIL / CORPORATE / FINANCIAL_INSTITUTION), CUSTOMER_SEGMENT, ACCOUNT_STATUS, BUSINESS_UNIT,
COUNTERPARTY_ID.

## contracts_YYYYMMDD.csv
CONTRACT_ID, AS_OF_DATE, COUNTERPARTY_ID, PRODUCT_TYPE (LOAN_RETAIL, LOAN_CORP, INTERBANK_LOAN, CREDIT_FACILITY,
LIQUIDITY_FACILITY, INTERBANK_BORROWING; NEW_DERIVATIVE_X = injected unmapped product), BUSINESS_UNIT, CURRENCY,
RATING, START_DATE, MATURITY_DATE, OUTSTANDING_CCY (drawn amount), UNDRAWN_CCY (committed undrawn facility),
INTEREST_RATE (annual, decimal), IS_PERFORMING, COUNTERPARTY_SECTOR.

## cashflows_YYYYMMDD.csv
AS_OF_DATE, CONTRACT_ID, CF_DATE, CF_TYPE (PRINCIPAL / INTEREST), DIRECTION (IN = receivable, OUT = payable),
CURRENCY, AMOUNT_CCY. Schedule covers the next 365 days from AS_OF_DATE.

## lcr_events.csv
EVENT_ID, EVENT_DATE, EVENT_TYPE, DESCRIPTION, COUNTERPARTY_ID, CONTRACT_ID (driver position), AMOUNT_CHF,
EXPECTED_DIRECTION (UP / DOWN LCR move), EXPECTED_MIN_DELTA_PP (minimum expected LCR move in percentage points).

## Injected data quality issues (~0.1% of contracts)
- PRODUCT_TYPE = NEW_DERIVATIVE_X (unmapped asset class)
- CURRENCY = SEK (no FX rate available)
- START_DATE after MATURITY_DATE (invalid dates)
""")


def main():
    p = argparse.ArgumentParser(description="Generate the persistent, calibrated FINMA LCR book")
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--customers", type=int, default=1000, help="Synthetic customers if no --customer-file")
    p.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD (default: days-1 before today)")
    p.add_argument("--output-dir", type=str, required=True)
    p.add_argument("--customer-file", type=str, default=None)
    p.add_argument("--fx-dir", type=str, default=None, help="Directory with fx_rates_*.csv (default: <output-dir>/../fx_rates)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--target-lcr", type=float, default=135.0)
    p.add_argument("--no-events", action="store_true", help="Disable the deterministic showcase events")
    args = p.parse_args()

    start = (datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date
             else date.today() - timedelta(days=args.days - 1))
    out_dir = Path(args.output_dir)
    fx_dir = Path(args.fx_dir) if args.fx_dir else out_dir.parent / "fx_rates"
    if args.customer_file and Path(args.customer_file).exists():
        customer_ids = pd.read_csv(args.customer_file)["customer_id"].astype(str).tolist()
    else:
        customer_ids = [f"CUST_{i+1:05d}" for i in range(args.customers)]

    fx = FXTable(fx_dir)
    gen = LCRBookGenerator(customer_ids, start, args.days, args.seed, args.target_lcr, fx, not args.no_events)
    summary = gen.run(out_dir)
    lcr = summary["LCR"]
    print("✅ LCR generation: SUCCESS")
    print(f"   - Date range: {start} to {gen.end} ({args.days} days)")
    print(f"   - Counterparties: {len(gen.counterparties):,} | deposits: {len(gen.deposits):,} | "
          f"holdings: {len(gen.holdings)} | contracts: {len(gen.contracts):,}")
    print(f"   - Rows: holdings {gen.totals['h']:,}, deposits {gen.totals['d']:,}, contracts {gen.totals['c']:,}, "
          f"cashflows {gen.totals['f']:,}")
    print(f"   - FX source: {'generated fx_rates (' + str(len(fx.dates)) + ' days)' if fx.dates else 'fallback constants'}")
    print(f"   - BASE LCR (generator estimate): min {lcr.min():.1f}% / avg {lcr.mean():.1f}% / max {lcr.max():.1f}% | "
          f"days < 100%: {(lcr < 100).sum()}")
    for e in gen.events:
        row = summary[summary["AS_OF_DATE"] == e.event_date]
        prev = summary[summary["AS_OF_DATE"] < e.event_date].tail(1)
        if len(row) and len(prev):
            print(f"   - {e.event_id} {e.event_date} {e.event_type}: LCR {prev['LCR'].iloc[0]:.1f}% -> {row['LCR'].iloc[0]:.1f}%")
    summary.to_csv(out_dir / "_generator_lcr_estimate.csv", index=False)


if __name__ == "__main__":
    main()
