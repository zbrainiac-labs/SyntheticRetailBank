#!/bin/bash
# Test each DCM definition file individually
# Usage: ./operation/test_dcm_individual.sh [connection_name]
# Default connection: zs28104-svc_cicd

CONN="${1:-zs28104-svc_cicd}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TEST_DIR="/tmp/dcm_test"

rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR/sources/definitions" "$TEST_DIR/sources/macros"
cp "$PROJECT_DIR/manifest.yml" "$TEST_DIR/"
cp "$PROJECT_DIR/sources/macros/common.sql" "$TEST_DIR/sources/macros/"

echo "Testing DCM definitions individually (connection: $CONN)"
echo "========================================================"

PASS=0
FAIL=0

for f in "$PROJECT_DIR"/sources/definitions/*.sql; do
  base=$(basename "$f")
  rm -f "$TEST_DIR/sources/definitions/"*.sql 2>/dev/null
  cp "$f" "$TEST_DIR/sources/definitions/"
  result=$(snow dcm plan -c "$CONN" --from "$TEST_DIR" 2>&1 | tail -5)
  if echo "$result" | grep -q "Error"; then
    err=$(echo "$result" | grep -oE 'Error.*' | head -1 | cut -c1-120)
    echo "FAIL: $base"
    echo "      $err"
    FAIL=$((FAIL + 1))
  else
    echo "OK:   $base"
    PASS=$((PASS + 1))
  fi
done

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS + FAIL)) files"
