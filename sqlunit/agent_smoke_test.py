#!/usr/bin/env python3
"""
Regression smoke test: one question per domain agent (via REP_AGG_V001.CALL_SUB_AGENT).

PASS = non-empty answer, no error text, and the answer contains at least one number.
Usage (repo root):  the_bank_app/venv/bin/python sqlunit/agent_smoke_test.py [CONNECTION] [DATABASE]
"""
import re
import sys
import time

import snowflake.connector

CONN = sys.argv[1] if len(sys.argv) > 1 else "DEMO_MDAEPPEN"
DB = sys.argv[2] if len(sys.argv) > 2 else "AAA_DEV_SYNTHETIC_BANK"

CASES = [
    ("CRM_AGG_V001.CRM_CUSTOMER_360", "How many customers do we have by account tier?"),
    ("CRM_AGG_V001.HR_EMPLOYEE_AGENT", "How many active employees do we have per country?"),
    ("PAY_AGG_V001.COMPLIANCE_MONITORING_AGENT", "How many critical anomaly transactions are there?"),
    ("REP_AGG_V001.LIQUIDITY_RISK_AGENT", "What is the current LCR ratio and status?"),
    ("REP_AGG_V001.LOAN_PORTFOLIO_AGENT", "What is the average LTV of the mortgage portfolio?"),
    ("REP_AGG_V001.RISK_REGULATORY_AGENT", "What is the total risk weighted assets?"),
    ("REP_AGG_V001.WEALTH_ADVISOR_AGENT", "What is the total portfolio value under management in CHF?"),
    ("REP_AGG_V001.UBER_AGENT", "How many customers do we have?"),
]

con = snowflake.connector.connect(connection_name=CONN, database=DB, schema="REP_AGG_V001",
                                  warehouse="MD_TEST_WH")
cur = con.cursor()
fails = 0
for agent, question in CASES:
    t0 = time.time()
    try:
        cur.execute("CALL CALL_SUB_AGENT(%s, %s)", (f"{DB}.{agent}", question))
        answer = cur.fetchone()[0] or ""
    except Exception as e:
        answer = f"ERROR: {e}"
    ok = bool(answer.strip()) and not re.match(r"(?i)\s*error", answer) and re.search(r"\d", answer)
    fails += not ok
    snippet = re.sub(r"\s+", " ", answer)[:150]
    print(f"{'PASS' if ok else 'FAIL'}  {agent.split('.')[1]:<28} {time.time() - t0:5.1f}s  {snippet}")

print(f"RESULT: {len(CASES) - fails}/{len(CASES)} PASS")
sys.exit(1 if fails else 0)
