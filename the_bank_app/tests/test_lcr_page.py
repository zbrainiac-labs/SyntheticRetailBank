"""
Headless smoke test of the LCR Monitoring page against the DEV LCR engine.

Run from the_bank_app/:  venv/bin/python tests/test_lcr_page.py
Requires the Snowflake connection configured for the app (DEMO_MDAEPPEN by default).
"""
import datetime as dt
import sys

sys.path.insert(0, ".")
from streamlit.testing.v1 import AppTest  # noqa: E402

failures = []


def check(name, cond, detail=""):
    print(f"{'PASS' if cond else 'FAIL'}  {name}  {detail}")
    if not cond:
        failures.append(name)


at = AppTest.from_file("pages/lcr_monitoring.py", default_timeout=300)
at.run()
check("page renders without exceptions", not at.exception, [e.value for e in at.exception])
check("11 tabs", len(at.tabs) == 11, len(at.tabs))
check("charts rendered", len(at.get("plotly_chart")) >= 10, len(at.get("plotly_chart")))
metrics = {m.label: m.value for m in at.metric}
check("KPI row present", all(k in metrics for k in ("LCR", "Status", "HQLA (CHF)", "Outflows (CHF)")))

# Simulator: a corporate withdrawal must lower the simulated LCR
base_lcr = float(metrics["Simulated LCR"].rstrip("%"))
at.number_input[0].set_value(100).run()
sim_lcr = float({m.label: m.value for m in at.metric}["Simulated LCR"].rstrip("%"))
check("simulator withdrawal lowers LCR", sim_lcr < base_lcr, f"{base_lcr}% -> {sim_lcr}%")

# Breach day: the stress window must show FAIL status
fail_dates = [d for d in at.select_slider[0].options if dt.date(2026, 9, 11) <= dt.date.fromisoformat(str(d)) <= dt.date(2026, 9, 16)]
if fail_dates:
    at.select_slider[0].set_value(dt.date.fromisoformat(str(fail_dates[2]))).run()
    status = {m.label: m.value for m in at.metric}.get("Status")
    check("stress window shows FAIL", status == "FAIL", f"{fail_dates[2]} -> {status}")
    check("breach banner shown", any("100% regulatory minimum" in e.value for e in at.error))

print("RESULT:", "FAIL " + ", ".join(failures) if failures else "ALL PASS")
sys.exit(1 if failures else 0)
