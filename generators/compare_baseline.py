"""
Before/after comparison of two liquidity book outputs (generator directories).

  metrics(out)   key figures per run: daily BASE LCR, scenario LCR on the as-of date, stressed DtI per
                 entity (last 20 reporting dates), free collateral pool and HQLA on the as-of date
  compare(a, b)  per metric: baseline, candidate, delta; flagged INTENDED when listed in --intended

Run: venv/bin/python generators/compare_baseline.py generated_data/_baseline_demo/lcr generated_data/_lcr_check
     [--save generated_data/_baseline_demo/metrics.csv]
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reference_loader as ref
from liquidity_checks import Book, cash_shares, dti_days, dti_flows, scenario_lcr

LAST_DTI_DATES = 20


def metrics(out: Path, fixed_share: bool = True) -> pd.DataFrame:
    book = Book(out)
    as_of = date.fromisoformat(book.csv("generation_run.csv").AS_OF_DATE.iloc[0])
    rows = [("LCR_BASE", r.AS_OF_DATE[:10], r.LCR) for r in book.csv("_generator_lcr_estimate.csv").itertuples()]
    rows += [("LCR_SCENARIO_ASOF", s, v) for s, v in scenario_lcr(book, as_of).items()]
    ents = [e for e, _ in ref.entity_shares()]
    for d in book.dti_dates()[-LAST_DTI_DATES:]:
        f = dti_flows(book, d, cash_shares(book, d, fixed_share))
        rows += [("DTI_STRESSED", f"{d}|{e}", dti_days(f, [e]) or 366) for e in ents]
        rows.append(("DTI_STRESSED", f"{d}|GROUP", dti_days(f) or 366))
    sec = book.snapshot("dti_securities", as_of)
    free = sec[~sec.IS_ENCUMBERED.astype(bool) & sec.HQLA_CATEGORY.isin(["L1", "L2A", "L2B"])]
    rows.append(("FREE_POOL_MV_ASOF", "ALL", free.MARKET_VALUE_CHF.sum()))
    hold = book.snapshot("hqla_holdings", as_of)
    rows.append(("HQLA_MV_ASOF", "ALL", hold[hold.HQLA_ELIGIBLE.astype(bool)].MARKET_VALUE_CHF.sum()))
    return pd.DataFrame(rows, columns=["METRIC", "KEY", "VALUE"])


def compare(base: pd.DataFrame, cand: pd.DataFrame) -> pd.DataFrame:
    m = base.merge(cand, on=["METRIC", "KEY"], how="outer", suffixes=("_BASE", "_CAND"))
    m["DELTA"] = m.VALUE_CAND - m.VALUE_BASE
    return m


def summary(diff: pd.DataFrame) -> pd.DataFrame:
    g = diff.groupby("METRIC").agg(N=("KEY", "size"), MAX_ABS_DELTA=("DELTA", lambda x: x.abs().max()),
                                   MEAN_DELTA=("DELTA", "mean"), MISSING=("DELTA", lambda x: x.isna().sum()))
    return g.round(2)


def main():
    p = argparse.ArgumentParser(description="Compare two liquidity book outputs")
    p.add_argument("baseline", type=Path, help="baseline generator directory or metrics CSV")
    p.add_argument("candidate", type=Path, nargs="?", help="candidate generator directory")
    p.add_argument("--save", type=Path, help="write the baseline metrics to this CSV")
    p.add_argument("--deposit-share", action="store_true", help="baseline cash allocation by deposit share")
    a = p.parse_args()
    base = (pd.read_csv(a.baseline) if a.baseline.suffix == ".csv"
            else metrics(a.baseline, fixed_share=not a.deposit_share))
    if a.save:
        base.to_csv(a.save, index=False)
    if a.candidate is not None:
        diff = compare(base, metrics(a.candidate))
        print(summary(diff).to_string())
        scen = diff[diff.METRIC.isin(["LCR_SCENARIO_ASOF", "FREE_POOL_MV_ASOF", "HQLA_MV_ASOF"])]
        print(scen[["METRIC", "KEY", "VALUE_BASE", "VALUE_CAND", "DELTA"]].round(2).to_string(index=False))


if __name__ == "__main__":
    main()
