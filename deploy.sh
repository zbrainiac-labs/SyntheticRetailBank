#!/usr/bin/env bash
set -euo pipefail

# Usage: ./deploy.sh [DEV|PROD] [--skip-tests]
#   SNOW_CONNECTION=<name> selects the Snowflake CLI connection (default: the CLI default)
#   --skip-tests  skip the post-deploy LCR validation + metadata gate (step 4)
TARGET="DEV"
SKIP_TESTS=false
for arg in "$@"; do
  case "$arg" in
    --skip-tests) SKIP_TESTS=true ;;
    DEV|PROD) TARGET="$arg" ;;
  esac
done

# Resolve variables per target
case "$TARGET" in
  DEV)
    DB="AAA_DEV_SYNTHETIC_BANK"
    WH="MD_TEST_WH"
    LAG="60 MINUTE"
    ;;
  PROD)
    DB="AAA_PRD_SYNTHETIC_BANK"
    WH="MD_TEST_WH"
    LAG="30 MINUTE"
    ;;
  *)
    echo "Unknown target: $TARGET (use DEV or PROD)"
    exit 1
    ;;
esac

REP_AGG="REP_AGG_v001"
CRM_AGG="CRM_AGG_v001"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONN=()
[[ -n "${SNOW_CONNECTION:-}" ]] && CONN=(-c "$SNOW_CONNECTION")

echo "=== Deploying SyntheticRetailBank to $TARGET ==="
echo "  Database: $DB"
echo "  Warehouse: $WH"
echo ""

# Step 1: DCM deploy (DEFINE statements)
echo "[1/4] DCM deploy..."
# First deploy into an empty database: semantic views validate against their base dynamic
# tables, which only exist (initialized) after the first pass. Deploy without them, refresh
# the dynamic tables once, then deploy everything.
DT_COUNT=""
for _ in 1 2 3; do
  DT_COUNT=$(snow sql ${CONN[@]+"${CONN[@]}"} --role CICD --format CSV \
    -q "SELECT COUNT(*) FROM $DB.INFORMATION_SCHEMA.TABLES WHERE IS_DYNAMIC = 'YES'" 2>/dev/null | grep -E '^[0-9]+$' | tail -1 || true)
  [[ -n "$DT_COUNT" ]] && break
  sleep 10
done
# Only an explicit 0 triggers the first pass: on an existing database it would drop the semantic views
if [[ "$DT_COUNT" == "0" ]]; then
  echo "  empty database: first pass without semantic views"
  snow dcm deploy --target "$TARGET" ${CONN[@]+"${CONN[@]}"} -D "semantic_views=false"
  # A manual refresh also refreshes upstream dynamic tables, so the order doesn't matter.
  REFRESH_SQL=""
  for _ in 1 2 3 4 5; do
    REFRESH_SQL=$(snow sql ${CONN[@]+"${CONN[@]}"} --role CICD --format CSV \
      -q "SHOW DYNAMIC TABLES IN DATABASE $DB ->> SELECT 'ALTER DYNAMIC TABLE ' || \"database_name\" || '.' || \"schema_name\" || '.' || \"name\" || ' REFRESH;' FROM \$1" \
      2>/dev/null | grep '^ALTER DYNAMIC TABLE ' || true)
    [[ -n "$REFRESH_SQL" ]] && break
    sleep 10
  done
  [[ -n "$REFRESH_SQL" ]] || { echo "ERROR: no dynamic tables found after the first pass"; exit 1; }
  echo "  refreshing $(printf '%s\n' "$REFRESH_SQL" | grep -c .) dynamic tables"
  snow sql ${CONN[@]+"${CONN[@]}"} --role CICD -q "$REFRESH_SQL" < /dev/null > /dev/null
fi
snow dcm deploy --target "$TARGET" ${CONN[@]+"${CONN[@]}"}
echo ""

# Step 2: Post-deploy (what DCM cannot manage: seeds, agents, DMF attachments, shares, listings)
echo "[2/4] Post-deploy: seeds, agents, shares..."
snow sql ${CONN[@]+"${CONN[@]}"} --role CICD -f "$SCRIPT_DIR/post_deploy.sql" \
  --variable "db=$DB" \
  --variable "wh=$WH" \
  --variable "lag=$LAG" \
  --variable "rep_agg=$REP_AGG" \
  --variable "rep_raw=REP_RAW_v001" \
  --variable "crm_agg=$CRM_AGG" \
  --variable "crm_raw=CRM_RAW_v001" \
  --variable "cmd_raw=CMD_RAW_v001" \
  --variable "cmd_agg=CMD_AGG_v001" \
  --variable "eqt_raw=EQT_RAW_v001" \
  --variable "eqt_agg=EQT_AGG_v001" \
  --variable "fii_raw=FII_RAW_v001" \
  --variable "fii_agg=FII_AGG_v001" \
  --variable "loa_raw=LOA_RAW_v001" \
  --variable "loa_agg=LOA_AGG_v001" \
  --variable "pay_raw=PAY_RAW_v001" \
  --variable "pay_agg=PAY_AGG_v001" \
  --variable "ref_raw=REF_RAW_v001" \
  --variable "ref_agg=REF_AGG_v001" \
  --enable-templating JINJA
echo ""

# Step 3: Post-deploy: master agent (hybrid router, caching, observability, golden regression)
echo "[3/4] Post-deploy: master agent..."
snow sql ${CONN[@]+"${CONN[@]}"} --role CICD -f "$SCRIPT_DIR/post_deploy_master-agent.sql" \
  --variable "db=$DB" \
  --variable "wh=$WH" \
  --variable "rep_agg=$REP_AGG" \
  --variable "crm_agg=$CRM_AGG" \
  --enable-templating JINJA
echo ""

# Step 4: Test gate (LCR validation suite TC-VAL-01..04 / ENG-01..06 + DtI suite DTI-01..08 + metadata completeness + data uniqueness)
if [[ "$SKIP_TESTS" == "true" ]]; then
  echo "[4/4] Test gate skipped (--skip-tests)"
else
  echo "[4/4] Test gate: LCR validation + metadata completeness + data uniqueness..."
  TEST_LOG="$(mktemp)"
  for f in sqlunit/lcr_validation.sql sqlunit/dti_validation.sql sqlunit/metadata_check.sql sqlunit/data_check.sql; do
    snow sql ${CONN[@]+"${CONN[@]}"} --role CICD --format CSV -f "$SCRIPT_DIR/$f" >> "$TEST_LOG" 2>&1 || true
  done
  # DtI row access per role: the role is set at login (PAT sessions cannot USE ROLE)
  for r in DTI_READ_ONLY DTI_LRC_READ; do
    f="sqlunit/dti_governance_$(echo "$r" | sed 's/^DTI_//' | tr '[:upper:]' '[:lower:]').sql"
    snow sql ${CONN[@]+"${CONN[@]}"} --role "$r" --format CSV -f "$SCRIPT_DIR/$f" >> "$TEST_LOG" 2>&1 || true
  done
  grep -E "^(TC-|ENG-|DTI-|META-|DATA-)" "$TEST_LOG" | cut -d, -f1-2
  if grep -qE "^(TC-|ENG-|DTI-|META-|DATA-)[^,]*,FAIL" "$TEST_LOG" || ! grep -qE "^(TC-|ENG-|DTI-|META-|DATA-)" "$TEST_LOG"; then
    echo "❌ Test gate FAILED (details: $TEST_LOG)"
    exit 1
  fi
  echo "✅ Test gate passed"
fi
echo ""

echo "=== Deploy complete ($TARGET) ==="
