"""
Headless render test of every page of the bank app (design-system regression, Phase C).

Run from the_bank_app/:  venv/bin/python tests/test_all_pages.py
Each page must render without uncaught exceptions; st.error messages are reported.
"""
import glob
import sys

sys.path.insert(0, ".")
import utils.ui_helpers  # noqa: E402,F401  (registers the Plotly template like app.py)
from streamlit.testing.v1 import AppTest  # noqa: E402

failures = []
for page in sorted(glob.glob("pages/*.py")):
    at = AppTest.from_file(page, default_timeout=300)
    try:
        at.run()
        exc = [e.value for e in at.exception]
        err = [e.value for e in at.error]
    except Exception as e:  # timeout or harness error
        exc, err = [str(e)], []
    title = at.title[0].value if len(at.title) else "-"
    ok = not exc
    print(f"{'PASS' if ok else 'FAIL'}  {page:<32} title={title!r:<28} charts={len(at.get('plotly_chart')):>2} "
          f"metrics={len(at.metric):>2} errors={len(err)}" + (f"  EXC={exc[0][:120]}" if exc else "")
          + (f"  ERR={err[0][:120]}" if err else ""))
    if not ok:
        failures.append(page)

print("RESULT:", "FAIL " + ", ".join(failures) if failures else "ALL PASS")
sys.exit(1 if failures else 0)
