#!/usr/bin/env bash
# ============================================================
# operation/full_rebuild_test.sh - full regression from scratch (DEV only)
#
# Drops the DEV database and rebuilds everything from this repository alone:
# bootstrap, generated data, DCM deploy, post-deploy, upload, task runs and
# dynamic table refreshes; then runs every test suite. Proves that the repo is
# the complete source of truth (anything created by hand in DEV is gone after).
#
# Usage:  ./operation/full_rebuild_test.sh DEV --yes [--customers N] [--connection NAME]
#   --yes          required: confirms the DROP DATABASE
#   --customers N  number of generated customers (default 5000)
#   --connection   Snowflake CLI connection (default DEMO_MDAEPPEN)
#   --skip-generate  reuse the files in generated_data/ instead of regenerating
#
# No rollback: the script recreates the database right after the drop, so
# UNDROP DATABASE is only possible if the run fails between step 1 and step 2.
# ============================================================
set -euo pipefail

TARGET="${1:-}"; shift || true
CONFIRM=false; CUSTOMERS=5000; CONNECTION="DEMO_MDAEPPEN"; GENERATE=true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes) CONFIRM=true ;;
    --customers) CUSTOMERS="$2"; shift ;;
    --connection) CONNECTION="$2"; shift ;;
    --skip-generate) GENERATE=false ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
  shift
done
[[ "$TARGET" == "DEV" ]] || { echo "Refusing to run: target must be DEV (got '$TARGET')"; exit 2; }
[[ "$CONFIRM" == "true" ]] || { echo "Refusing to run without --yes (this drops AAA_DEV_SYNTHETIC_BANK)"; exit 2; }

DB="AAA_DEV_SYNTHETIC_BANK"; WH="MD_TEST_WH"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
RUN_DIR="$(mktemp -d -t full_rebuild)"
STEP="init"
trap 'echo ""; echo "FAILED at step: $STEP (logs: $RUN_DIR)"' ERR

VARS=(--variable "db=$DB" --variable "wh=$WH" --variable "lag=60 MINUTE"
      --variable "rep_agg=REP_AGG_v001" --variable "rep_raw=REP_RAW_v001"
      --variable "crm_agg=CRM_AGG_v001" --variable "crm_raw=CRM_RAW_v001"
      --variable "cmd_raw=CMD_RAW_v001" --variable "cmd_agg=CMD_AGG_v001"
      --variable "eqt_raw=EQT_RAW_v001" --variable "eqt_agg=EQT_AGG_v001"
      --variable "fii_raw=FII_RAW_v001" --variable "fii_agg=FII_AGG_v001"
      --variable "loa_raw=LOA_RAW_v001" --variable "loa_agg=LOA_AGG_v001"
      --variable "pay_raw=PAY_RAW_v001" --variable "pay_agg=PAY_AGG_v001"
      --variable "ref_raw=REF_RAW_v001" --variable "ref_agg=REF_AGG_v001")

# snow wrapper: retries when the session drops before the first statement ran
snowq() {
  local log="$1"; shift
  local i
  for i in 1 2 3 4; do
    if snow "$@" -c "$CONNECTION" > "$log" 2>&1; then return 0; fi
    if grep -q "Connection is closed" "$log" && ! grep -q "Statement executed\|^| " "$log"; then
      sleep 10; continue
    fi
    cat "$log" | tail -20; return 1
  done
  tail -20 "$log"; return 1
}

step() { STEP="$1"; echo ""; echo "=== [$(date +%H:%M:%S)] $1"; }

# Row counts and LCR used for the before/after comparison
SNAPSHOT_SQL="
SELECT 'customers' K, COUNT(*) V FROM $DB.CRM_AGG_V001.CRMA_AGG_DT_CUSTOMER_360 UNION ALL
SELECT 'accounts', COUNT(*) FROM $DB.CRM_AGG_V001.ACCA_AGG_DT_ACCOUNTS UNION ALL
SELECT 'transactions', COUNT(*) FROM $DB.PAY_RAW_V001.PAYI_RAW_TB_TRANSACTIONS UNION ALL
SELECT 'equity_trades', COUNT(*) FROM $DB.EQT_RAW_V001.EQTI_RAW_TB_TRADES UNION ALL
SELECT 'fi_trades', COUNT(*) FROM $DB.FII_RAW_V001.FIII_RAW_TB_TRADES UNION ALL
SELECT 'cmd_trades', COUNT(*) FROM $DB.CMD_RAW_V001.CMDI_RAW_TB_TRADES UNION ALL
SELECT 'aum_chf_bn', ROUND(SUM(TOTAL_PORTFOLIO_VALUE_CHF)/1e9, 2) FROM $DB.REP_AGG_V001.REPP_AGG_DT_PORTFOLIO_PERFORMANCE UNION ALL
SELECT 'pep_matches', COUNT_IF(EXPOSED_PERSON_MATCH_TYPE <> 'NO_MATCH') FROM $DB.CRM_AGG_V001.CRMA_AGG_DT_CUSTOMER_360 UNION ALL
SELECT 'lcr_pct', MAX_BY(LCR_RATIO, AS_OF_DATE) FROM $DB.REP_AGG_V001.REPP_AGG_DT_LCR_DAILY"

step "0/8 snapshot before"
snowq "$RUN_DIR/before.log" sql --role CICD --format CSV -q "$SNAPSHOT_SQL" || echo "(no usable before-snapshot)"

step "1/8 drop database $DB"
# A shared database cannot be dropped: detach it from the outbound shares first
# (revoking database usage removes all of a share's grants; post_deploy.sql re-grants them)
snowq "$RUN_DIR/shares.log" sql --role ACCOUNTADMIN --format CSV -q "SHOW SHARES ->> SELECT \"name\" FROM \$1 WHERE \"kind\" = 'OUTBOUND' AND \"database_name\" = '$DB'"
for share in $(grep -E '^SH_[A-Z0-9_]+$' "$RUN_DIR/shares.log"); do
  snowq "$RUN_DIR/revoke_$share.log" sql --role ACCOUNTADMIN -q "REVOKE USAGE ON DATABASE $DB FROM SHARE $share"
  echo "  detached share $share"
done
snowq "$RUN_DIR/drop.log" sql --role CICD -q "DROP DATABASE IF EXISTS $DB"
echo "Rollback: snow sql -c $CONNECTION --role CICD -q \"UNDROP DATABASE $DB\""

step "2/8 bootstrap (pre_deploy.sql as CICD)"
snowq "$RUN_DIR/pre_deploy.log" sql --role CICD -f pre_deploy.sql \
  --variable "db=$DB" --variable "wh=$WH" --enable-templating JINJA

step "3/8 generate all data ($CUSTOMERS customers)"
if [[ "$GENERATE" == "true" ]]; then
  ./data_generator.sh "$CUSTOMERS" --clean > "$RUN_DIR/generate.log" 2>&1 || { tail -30 "$RUN_DIR/generate.log"; false; }
  grep -E "no short|sells" "$RUN_DIR/generate.log" || true
else
  echo "  skipped: reusing generated_data/ ($(find generated_data -type f | wc -l | tr -d ' ') files)"
fi

step "4/8 deploy (DCM + post_deploy + master agent)"
SNOW_CONNECTION="$CONNECTION" ./deploy.sh DEV --skip-tests > "$RUN_DIR/deploy.log" 2>&1 || { tail -30 "$RUN_DIR/deploy.log"; false; }
grep -E "Deployed|Deploy complete" "$RUN_DIR/deploy.log" || true

step "5/8 upload data files"
./upload-data.sh --CONNECTION_NAME="$CONNECTION" > "$RUN_DIR/upload.log" 2>&1 || { tail -30 "$RUN_DIR/upload.log"; false; }

step "6/8 load and refresh (tasks, then dynamic tables)"
snowq "$RUN_DIR/execute1.log" sql --role CICD -f operation/execute_all_tasks_and_refresh_dts.sql "${VARS[@]}" --enable-templating JINJA
# EXECUTE TASK is asynchronous: wait for the loads, then refresh again on the loaded data
for i in $(seq 1 60); do
  snowq "$RUN_DIR/running.log" sql --role CICD --format CSV -q \
    "SELECT COUNT(*) AS N FROM TABLE($DB.INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START => DATEADD(hour, -3, CURRENT_TIMESTAMP()))) WHERE STATE IN ('EXECUTING', 'SCHEDULED') AND QUERY_ID IS NOT NULL"
  running=$(grep -E '^[0-9]+$' "$RUN_DIR/running.log" | tail -1)
  [[ "${running:-0}" == "0" ]] && break
  echo "  $running task runs still executing..."; sleep 20
done
# The loan applications seeded by post_deploy.sql derive from the AI_EXTRACT mail task output,
# which only exists now (on a fresh database the deploy ran before the upload)
snowq "$RUN_DIR/post_deploy_loans.log" sql --role CICD -f post_deploy.sql "${VARS[@]}" --enable-templating JINJA
snowq "$RUN_DIR/execute2.log" sql --role CICD -f operation/execute_all_tasks_and_refresh_dts.sql "${VARS[@]}" --enable-templating JINJA

step "7/8 structural and idempotency checks"
snowq "$RUN_DIR/objects.log" sql --role CICD --format CSV -q "
SELECT 'schemas' K, COUNT(*) V FROM $DB.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC') UNION ALL
SELECT 'dynamic_tables', COUNT(*) FROM $DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND IS_DYNAMIC = 'YES' UNION ALL
SELECT 'views', COUNT(*) FROM $DB.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'"
cat "$RUN_DIR/objects.log" | grep -E "^[a-z_]+," || true
snowq "$RUN_DIR/plan2.log" dcm plan --target DEV
grep -q "No changes detected" "$RUN_DIR/plan2.log" || { echo "Second DCM plan is not empty:"; grep -E "Planned|^(CREATE|ALTER|DROP)" "$RUN_DIR/plan2.log"; false; }
echo "  second DCM plan: no changes"
snowq "$RUN_DIR/post_deploy2.log" sql --role CICD -f post_deploy.sql "${VARS[@]}" --enable-templating JINJA
echo "  second post_deploy.sql run: OK"

step "8/8 test suites"
FAIL=0
: > "$RUN_DIR/gate.log"
for f in sqlunit/lcr_validation.sql sqlunit/metadata_check.sql sqlunit/data_check.sql; do
  snowq "$RUN_DIR/g.tmp" sql --role CICD --format CSV -f "$f" || true
  cat "$RUN_DIR/g.tmp" >> "$RUN_DIR/gate.log"
done
echo "  gate: $(grep -cE '^(TC-|ENG-|META-|DATA-)[^,]*,PASS' "$RUN_DIR/gate.log") PASS, $(grep -cE '^(TC-|ENG-|META-|DATA-)[^,]*,FAIL' "$RUN_DIR/gate.log") FAIL"
grep -qE '^(TC-|ENG-|META-|DATA-)[^,]*,FAIL' "$RUN_DIR/gate.log" && { grep -E ',FAIL' "$RUN_DIR/gate.log"; FAIL=1; }

snowq "$RUN_DIR/dcm_test.log" sql --role CICD -q "EXECUTE DCM PROJECT $DB.AAA_DCM.SYNTHETIC_RETAIL_BANK TEST ALL" || FAIL=1
grep -q '"status":"SUCCESSFUL"' "$RUN_DIR/dcm_test.log" && echo "  DCM TEST ALL: SUCCESSFUL" || { echo "  DCM TEST ALL: not successful"; FAIL=1; }

PY="$ROOT/the_bank_app/venv/bin/python"
"$PY" sqlunit/run_semantic_view_vqrs.py > "$RUN_DIR/vqr.log" 2>&1 || true
echo "  VQRs: $(grep RESULT "$RUN_DIR/vqr.log" || echo 'no result')"
grep -q " 0 FAIL" "$RUN_DIR/vqr.log" || FAIL=1
"$PY" sqlunit/agent_smoke_test.py > "$RUN_DIR/agents.log" 2>&1 || true
echo "  agents: $(grep RESULT "$RUN_DIR/agents.log" || echo 'no result')"
grep -q "RESULT: \([0-9]*\)/\1 PASS" "$RUN_DIR/agents.log" || FAIL=1
snowq "$RUN_DIR/golden.log" sql --role CICD -q "CALL $DB.REP_AGG_V001.RUN_GOLDEN_REGRESSION('FULL')" || FAIL=1
echo "  golden: $(grep -o 'Mode=[A-Z]* Total=[0-9]* Pass=[0-9]* Fail=[0-9]*' "$RUN_DIR/golden.log" || echo 'no result')"
grep -q "Fail=0" "$RUN_DIR/golden.log" || FAIL=1
(cd the_bank_app && venv/bin/python tests/test_lcr_page.py > "$RUN_DIR/app_lcr.log" 2>&1; venv/bin/python tests/test_all_pages.py > "$RUN_DIR/app_all.log" 2>&1) || true
echo "  app: lcr page $(tail -1 "$RUN_DIR/app_lcr.log"), all pages $(tail -1 "$RUN_DIR/app_all.log")"
grep -q "ALL PASS" "$RUN_DIR/app_lcr.log" && grep -q "ALL PASS" "$RUN_DIR/app_all.log" || FAIL=1

step "before / after"
snowq "$RUN_DIR/after.log" sql --role CICD --format CSV -q "$SNAPSHOT_SQL" || true
join -t, -a2 -e '-' -o 0,1.2,2.2 <(grep -E '^[a-z_]+,' "$RUN_DIR/before.log" | sort) <(grep -E '^[a-z_]+,' "$RUN_DIR/after.log" | sort) \
  | awk -F, 'BEGIN{printf "  %-14s %16s %16s\n","metric","before","after"} {printf "  %-14s %16s %16s\n",$1,$2,$3}'

echo ""
if [[ "$FAIL" == "0" ]]; then echo "FULL REBUILD TEST PASSED (logs: $RUN_DIR)"; else echo "FULL REBUILD TEST FAILED (logs: $RUN_DIR)"; exit 1; fi
