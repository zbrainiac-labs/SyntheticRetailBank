#!/usr/bin/env bash
set -euo pipefail

# Usage: ./deploy.sh [DEV|PROD]
TARGET="${1:-DEV}"

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

echo "=== Deploying SyntheticRetailBank to $TARGET ==="
echo "  Database: $DB"
echo "  Warehouse: $WH"
echo ""

# Step 1: DCM deploy (DEFINE statements)
echo "[1/3] DCM deploy..."
snow dcm deploy --target "$TARGET"
echo ""

# Step 2: Post-deploy (data pipeline: file formats, seeds, agents, DMFs, shares, listings)
echo "[2/3] Post-deploy: data pipeline..."
snow sql -f "$SCRIPT_DIR/post_deploy.sql" \
  --variable "db='$DB'" \
  --variable "wh='$WH'" \
  --variable "lag='$LAG'" \
  --variable "rep_agg='$REP_AGG'" \
  --variable "rep_raw='REP_RAW_v001'" \
  --variable "crm_agg='$CRM_AGG'" \
  --variable "crm_raw='CRM_RAW_v001'" \
  --variable "cmd_raw='CMD_RAW_v001'" \
  --variable "cmd_agg='CMD_AGG_v001'" \
  --variable "eqt_raw='EQT_RAW_v001'" \
  --variable "eqt_agg='EQT_AGG_v001'" \
  --variable "fii_raw='FII_RAW_v001'" \
  --variable "fii_agg='FII_AGG_v001'" \
  --variable "loa_raw='LOA_RAW_v001'" \
  --variable "loa_agg='LOA_AGG_v001'" \
  --variable "pay_raw='PAY_RAW_v001'" \
  --variable "pay_agg='PAY_AGG_v001'" \
  --variable "ref_raw='REF_RAW_v001'" \
  --variable "ref_agg='REF_AGG_v001'" \
  --enable-templating JINJA
echo ""

# Step 3: Post-deploy: master agent (hybrid router, caching, observability, golden regression)
echo "[3/3] Post-deploy: master agent..."
snow sql -f "$SCRIPT_DIR/post_deploy_master-agent.sql" \
  --variable "db='$DB'" \
  --variable "wh='$WH'" \
  --variable "rep_agg='$REP_AGG'" \
  --variable "crm_agg='$CRM_AGG'" \
  --enable-templating JINJA
echo ""

echo "=== Deploy complete ($TARGET) ==="
