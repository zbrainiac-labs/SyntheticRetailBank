#!/usr/bin/env python3
"""
Render the LIQI reference seeds of post_deploy.sql from reference_data/liqi/*.csv.

reference_data/liqi is the single source of truth for LCR / DtI reference and
configuration data (holidays, buckets, scenarios, rules, mappings). The
generators read the same CSVs, so seeds and generated data cannot drift apart.

Usage:
  operation/render_reference_seeds.py          rewrite the generated block in post_deploy.sql
  operation/render_reference_seeds.py --check  exit 1 if post_deploy.sql is out of sync
"""
import argparse
import csv
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REF_DIR = REPO / "reference_data" / "liqi"
TARGET = REPO / "post_deploy.sql"
BEGIN = "-- >>> BEGIN GENERATED LIQI REFERENCE SEEDS (operation/render_reference_seeds.py, source reference_data/liqi) - do not edit"
END = "-- <<< END GENERATED LIQI REFERENCE SEEDS"
NUMBER = re.compile(r"^-?\d+(\.\d+)?$")


def literal(value: str, cast: str = None) -> str:
    if value == "":
        return "NULL"
    if value in ("TRUE", "FALSE"):
        return value
    if NUMBER.match(value) and not cast:
        return value
    quoted = "'" + value.replace("'", "''") + "'"
    return f"{quoted}::{cast}" if cast else quoted


def render_table(t: dict) -> str:
    with open(REF_DIR / t["file"], newline="") as f:
        rows = list(csv.reader(f))
    cols, data = rows[0], rows[1:]
    casts = t.get("casts", {})
    values = ",\n".join("    (" + ", ".join(literal(v, casts.get(c)) for c, v in zip(cols, r)) + ")" for r in data)
    keys = t["keys"]
    non_keys = [c for c in cols if c not in keys]
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    insert = (f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
              f"VALUES ({', '.join('s.' + c for c in cols)});")
    out = []
    if t.get("comment"):
        out.append(f"-- {t['comment']}")
    out.append(f"MERGE INTO {t['table']} t USING (SELECT * FROM VALUES\n{values}\n) s ({', '.join(cols)})")
    out.append(f"ON {on}")
    if t["mode"] == "upsert" and non_keys:
        out.append("WHEN MATCHED THEN UPDATE SET " + ", ".join(f"t.{c} = s.{c}" for c in non_keys))
    out.append(insert)
    return "\n".join(out)


def render() -> str:
    tables = json.loads((REF_DIR / "tables.json").read_text())
    body = "\n\n".join(render_table(t) for t in tables)
    return f"{BEGIN}\n\n{body}\n\n{END}"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--check", action="store_true")
    args = p.parse_args()
    sql = TARGET.read_text()
    if BEGIN not in sql or END not in sql:
        print(f"ERROR: markers not found in {TARGET}", file=sys.stderr)
        return 1
    head, rest = sql.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    new = head + render() + tail
    if args.check:
        if new != sql:
            print("post_deploy.sql LIQI seeds are out of sync with reference_data/liqi - run operation/render_reference_seeds.py")
            return 1
        print("post_deploy.sql LIQI seeds in sync with reference_data/liqi")
        return 0
    TARGET.write_text(new)
    print(f"rendered {TARGET.name} from {REF_DIR.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
