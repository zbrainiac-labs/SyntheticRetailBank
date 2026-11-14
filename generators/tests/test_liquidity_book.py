"""
Offline checks of the LCR + DtI liquidity book generator (stdlib unittest, no Snowflake):
  - reference seeds in post_deploy.sql are rendered from reference_data/liqi
  - same seed + as-of date => byte-identical files
  - LCR holdings and DtI securities are one book (same ids, values, encumbrance)
  - both engines end on the as-of date, BASE LCR calibrated to the target there
Run: venv/bin/python -m unittest discover -s generators/tests -v
"""

import csv
import hashlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
GEN = REPO / "generators" / "lcr_data_generator.py"
FX = REPO / "generated_data" / "fx_rates"
AS_OF = "2026-10-23"
ARGS = ["--as-of-date", AS_OF, "--customers", "400", "--seed", "7", "--target-lcr", "135", "--history-month-ends", "3"]

sys.path.insert(0, str(REPO / "generators"))
import reference_loader as ref


def generate(out: Path):
    fx = ["--fx-dir", str(FX)] if FX.exists() else ["--fx-dir", str(out / "_no_fx")]
    subprocess.run([sys.executable, str(GEN), *ARGS, *fx, "--output-dir", str(out)], check=True,
                   capture_output=True, text=True)


def digest(d: Path) -> dict:
    return {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(d.iterdir()) if f.is_file()}


class LiquidityBookTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.a, cls.b = Path(cls.tmp.name) / "a", Path(cls.tmp.name) / "b"
        generate(cls.a)
        generate(cls.b)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_reference_seeds_in_sync(self):
        r = subprocess.run([sys.executable, str(REPO / "tools" / "render_reference_seeds.py"), "--check"], check=False,
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_generator_reads_reference_data(self):
        with open(REPO / "reference_data" / "liqi" / "dti_holidays.csv") as f:
            self.assertEqual(len(ref.holidays()), sum(1 for _ in csv.DictReader(f)))
        self.assertEqual(sum(w for _, w in ref.entity_shares()), 100)

    def test_deterministic(self):
        self.assertEqual(digest(self.a), digest(self.b))

    def test_one_securities_book(self):
        dates = 0
        for f in sorted(self.a.glob("dti_securities_*.csv")):
            h_file = self.a / f"hqla_holdings_{f.name[-12:]}"
            if not h_file.exists():
                continue
            dates += 1
            s, h = pd.read_csv(f), pd.read_csv(h_file)
            s = s[s.WLK_CLUSTER != "X"]
            h_sec = h[~h.ASSET_TYPE.str.startswith("CASH")]
            self.assertEqual(set(s.SECURITY_ID), set(h_sec.HOLDING_ID), f.name)
            m = s.merge(h_sec, left_on="SECURITY_ID", right_on="HOLDING_ID", suffixes=("_s", "_h"))
            self.assertLess((m.MARKET_VALUE_CHF_s - m.MARKET_VALUE_CHF_h).abs().max(), 0.005, f.name)
            self.assertTrue((m.IS_ENCUMBERED_s == m.IS_ENCUMBERED_h).all(), f.name)
            self.assertFalse(m[m.IS_ENCUMBERED_s].HQLA_ELIGIBLE.any(), f"encumbered security counted as HQLA in {f.name}")
        self.assertGreater(dates, 50)

    def test_same_as_of_and_calibration(self):
        run = pd.read_csv(self.a / "generation_run.csv").iloc[0]
        self.assertEqual(run.AS_OF_DATE, AS_OF)
        self.assertEqual(max(f.name[-12:-4] for f in self.a.glob("hqla_holdings_*.csv")), AS_OF.replace("-", ""))
        self.assertEqual(max(f.name[-12:-4] for f in self.a.glob("dti_securities_*.csv")), AS_OF.replace("-", ""))
        est = pd.read_csv(self.a / "_generator_lcr_estimate.csv")
        self.assertAlmostEqual(est.LCR.iloc[-1], 135, delta=3)
        for name in ("hqla_holdings_", "deposit_balances_", "dti_securities_"):
            ids = set(pd.read_csv(next(self.a.glob(f"{name}*.csv"))).RUN_ID)
            self.assertEqual(ids, {run.RUN_ID}, name)

    def test_manifest_matches_files(self):
        m = pd.read_csv(self.a / "_manifest.csv").set_index("TABLE_NAME").ROW_COUNT
        rows = sum(len(pd.read_csv(f)) for f in self.a.glob("hqla_holdings_*.csv"))
        self.assertEqual(m["LIQI_RAW_TB_HQLA_HOLDINGS"], rows)

    def test_one_raw_book_with_snapshot_scope(self):
        """Month-end history lives in the main files (SNAPSHOT_SCOPE = MONTH_END, last column), no side tables."""
        run = pd.read_csv(self.a / "generation_run.csv").iloc[0]
        window_start = run.WINDOW_START.replace("-", "")
        self.assertEqual(list(self.a.glob("dti_hist_*.csv")), [])
        scopes = {}
        for name in ("hqla_holdings_", "deposit_balances_", "contracts_", "cashflows_"):
            for f in sorted(self.a.glob(f"{name}*.csv")):
                df = pd.read_csv(f)
                self.assertEqual(df.columns[-1], "SNAPSHOT_SCOPE", f.name)
                expected = "DAILY" if f.name[-12:-4] >= window_start else "MONTH_END"
                self.assertEqual(set(df.SNAPSHOT_SCOPE), {expected}, f.name)
                scopes.setdefault(name, set()).add(expected)
        for name, s in scopes.items():
            self.assertEqual(s, {"DAILY", "MONTH_END"}, name)
        month_ends = {f.name[-12:-4] for f in self.a.glob("hqla_holdings_*.csv") if f.name[-12:-4] < window_start}
        self.assertEqual(len(month_ends), 3)


if __name__ == "__main__":
    unittest.main()
