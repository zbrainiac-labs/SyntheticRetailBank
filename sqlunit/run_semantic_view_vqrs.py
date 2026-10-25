#!/usr/bin/env python3
"""
Regression: run every verified query (VQR) of every semantic view in the database.

Each VQR's logical table references (__name) are replaced by the base table from the semantic view
YAML; the query is executed as SELECT COUNT(*) FROM (<sql>). PASS = runs and returns >= 1 row.

Usage (repo root):  the_bank_app/venv/bin/python sqlunit/run_semantic_view_vqrs.py [CONNECTION] [DATABASE]
"""
import re
import sys

import snowflake.connector
import yaml

CONN = sys.argv[1] if len(sys.argv) > 1 else "DEMO_MDAEPPEN"
DB = sys.argv[2] if len(sys.argv) > 2 else "AAA_DEV_SYNTHETIC_BANK"

con = snowflake.connector.connect(connection_name=CONN, database=DB, warehouse="MD_TEST_WH")
cur = con.cursor()
cur.execute(f"SHOW SEMANTIC VIEWS IN DATABASE {DB}")
views = [(r[3], r[1]) for r in cur.fetchall()]  # schema, name

results = []
for schema, name in sorted(views):
    fqn = f"{DB}.{schema}.{name}"
    try:
        cur.execute("SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW(%s)", (fqn,))
        spec = yaml.safe_load(cur.fetchone()[0])
    except Exception as e:
        results.append((fqn, "-", "FAIL", f"YAML: {str(e)[:100]}"))
        continue
    def physical(bt):  # base table, or a SQL definition used as a subquery
        if "definition" in bt:
            return f"({bt['definition']})"
        return ".".join([bt.get("database", DB), bt.get("schema", schema), bt["table"]])

    tables = {t["name"].lower(): physical(t["base_table"]) for t in spec.get("tables", [])}
    vqrs = spec.get("verified_queries") or []
    if not vqrs:
        results.append((fqn, "-", "NO_VQR", "no verified queries"))
    for q in vqrs:
        sql = re.sub(r"\b__(\w+)", lambda m: tables.get(m.group(1).lower(), m.group(0)), q["sql"])
        try:
            cur.execute(f"SELECT COUNT(*) FROM ({sql.rstrip().rstrip(';')})")
            n = cur.fetchone()[0]
            results.append((fqn, q["name"], "PASS" if n > 0 else "FAIL", f"{n} rows"))
        except Exception as e:
            results.append((fqn, q["name"], "FAIL", str(e).replace("\n", " ")[:140]))

for fqn, vq, status, detail in results:
    print(f"{status:<6} {fqn.split('.', 1)[1]:<55} {vq:<40} {detail}")
fails = [r for r in results if r[2] == "FAIL"]
print(f"RESULT: {len(views)} semantic views, {sum(r[1] != '-' for r in results)} VQRs, "
      f"{len(fails)} FAIL, {sum(r[2] == 'NO_VQR' for r in results)} views without VQRs")
sys.exit(1 if fails else 0)
