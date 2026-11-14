#!/bin/bash
# -----------------------------------------------------------------------------
# Local CI for SyntheticRetailBank -- no commit or push required, all tools in containers.
# Adapted from ../crew-asset-management/local-ci/run.sh.
#
# Stages (in order, fail fast):
#   1. lint      shellcheck, actionlint, ruff (report-only; LINT_STRICT=1 enforces)  local-ci/lint.sh
#   2. test      bank-app typecheck + vitest with coverage + JUnit          local-ci/unit-tests.sh
#   3. sonar     sonar-scanner -> quality gate + new-code issue check        local-ci/sonar-self-scan.sh
#   4. image     arm64 docker build of bank-app + /api/health smoke test     local-ci/image.sh
#   5. e2e       opt-in (--with-e2e): Playwright against the running app containers
#                (owner on :8088, read-only on :8089 -- see bank-app/Makefile)
#   6. sqlunit   opt-in (--with-sqlunit): DtI/LCR SQL gate against Snowflake DEV (sqlunit/*.sql)
#
# Or run the GitHub workflow itself with nektos/act (--act): .github/workflows/ci.yml,
# same scripts, jobs in catthehacker/ubuntu containers; Sonar reuses the local stack.
#
# Usage:
#   local-ci/run.sh [--skip=lint,test,sonar,image] [--with-e2e] [--with-sqlunit]
#                   [--platform=linux/arm64] [--tag=bank-app:local-ci]
#   local-ci/run.sh --act [--job=<job>] [-- <extra act args>]
#
# Requires: docker and a running DataOpsBackbone SonarQube stack (../DataOpsBackbone/start.sh);
#           act for --act; snow CLI + connection for --with-sqlunit.
# Env: SNOWFLAKE_CONNECTION (default zs28104-svc_mdaeppen, --with-sqlunit), ACT_IMAGE
# Logs: test-results/local-ci/<timestamp>.log
# -----------------------------------------------------------------------------
set -Eeuo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKIP=","
WITH_E2E=0
WITH_SQLUNIT=0
USE_ACT=0
ACT_JOB=""
ACT_ARGS=()
PLATFORM="linux/arm64"
TAG="bank-app:local-ci"

while [[ $# -gt 0 ]]; do
  case $1 in
    --skip=*) SKIP=",${1#*=}," ;;
    --with-e2e) WITH_E2E=1 ;;
    --with-sqlunit) WITH_SQLUNIT=1 ;;
    --act) USE_ACT=1 ;;
    --job=*) ACT_JOB="${1#*=}" ;;
    --platform=*) PLATFORM="${1#*=}" ;;
    --tag=*) TAG="${1#*=}" ;;
    --) shift; ACT_ARGS=("$@"); break ;;
    -h|--help) sed -n '2,29p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
  shift
done

command -v docker >/dev/null || { echo "ERROR: docker not found"; exit 1; }
skipped() { [[ "$SKIP" == *",$1,"* ]]; }

LOG_DIR="$REPO/test-results/local-ci"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/$(date +%Y%m%dT%H%M%S).log"
exec > >(tee "$LOG") 2>&1
echo "SyntheticRetailBank local-ci -- log: $LOG"

stage() { echo; echo "=== [$1] $(date +%H:%M:%S) ==="; }

check_log_secrets() {
  if grep -qE 'squ_[A-Za-z0-9]{20,}|gh[op]_[A-Za-z0-9]{20,}|eyJ[A-Za-z0-9_-]{40,}\.' "$LOG"; then
    echo "ERROR: secret-looking value found in log $LOG"
    exit 1
  fi
}

# --- act: run .github/workflows/ci.yml locally
if [[ "$USE_ACT" -eq 1 ]]; then
  command -v act >/dev/null || { echo "ERROR: act not found (brew install act)"; exit 1; }
  NETWORK=$(docker network ls --format '{{.Name}}' | grep -m1 'sonarnet$' || true)
  [[ -n "$NETWORK" ]] || { echo "ERROR: sonarnet network not found -- run ../DataOpsBackbone/start.sh"; exit 1; }
  stage "act ci.yml${ACT_JOB:+ (job $ACT_JOB)}"
  # --bind: the job works on the host checkout itself, so the docker run -v "$REPO:..." mounts
  # in the scripts resolve on the Docker Desktop daemon. The host architecture matches arm64.
  set +e
  act workflow_dispatch -C "$REPO" -W "$REPO/.github/workflows/ci.yml" \
    -P ubuntu-latest="${ACT_IMAGE:-catthehacker/ubuntu:act-latest}" \
    --bind --network "$NETWORK" --container-architecture linux/arm64 \
    ${ACT_JOB:+-j "$ACT_JOB"} ${ACT_ARGS[@]+"${ACT_ARGS[@]}"}
  RC=$?
  set -e
  check_log_secrets
  echo "act exit code: $RC"
  exit "$RC"
fi

# --- 1. Lint
if ! skipped lint; then
  stage lint
  bash "$REPO/local-ci/lint.sh"
fi

# --- 2. Typecheck + unit tests with coverage + JUnit
if ! skipped test; then
  stage test
  bash "$REPO/local-ci/unit-tests.sh"
  # liquidity book generator: reference seeds in sync, determinism, one LCR/DtI book (offline)
  "$REPO/venv/bin/python" -m unittest discover -s "$REPO/generators/tests" -v
fi

# --- 3. Sonar: scan, quality gate, open issues in new code = 0 (coverage from stage 2)
if ! skipped sonar; then
  stage sonar
  bash "$REPO/local-ci/sonar-self-scan.sh" --strict --no-coverage
fi

# --- 4. Container image + smoke test
if ! skipped image; then
  stage image
  bash "$REPO/local-ci/image.sh" --platform="$PLATFORM" --tag="$TAG"
fi

# --- 5. Playwright e2e against the running containers (opt-in)
if [[ "$WITH_E2E" -eq 1 ]]; then
  stage e2e
  (cd "$REPO/bank-app" && BASE_URL="${BASE_URL:-http://localhost:8088}" RO_URL="${RO_URL:-http://localhost:8089}" \
    npx playwright test --reporter=line)
fi

# --- 6. SQL gate against Snowflake DEV (opt-in, read-only checks; same rule as deploy.sh step 4)
if [[ "$WITH_SQLUNIT" -eq 1 ]]; then
  stage sqlunit
  CONN="${SNOWFLAKE_CONNECTION:-zs28104-svc_mdaeppen}"
  SQL_LOG="$LOG_DIR/sqlunit-$(date +%Y%m%dT%H%M%S).csv"
  for f in lcr_validation dti_validation metadata_check data_check; do
    snow sql -c "$CONN" --role CICD --format CSV -f "$REPO/sqlunit/$f.sql" < /dev/null >> "$SQL_LOG" 2>&1 || true
  done
  for r in DTI_READ_ONLY DTI_LRC_READ; do
    f="dti_governance_$(echo "${r#DTI_}" | tr '[:upper:]' '[:lower:]')"
    snow sql -c "$CONN" --role "$r" --format CSV -f "$REPO/sqlunit/$f.sql" < /dev/null >> "$SQL_LOG" 2>&1 || true
  done
  CHECKS='^(TC-|ENG-|DTI-|META-|DATA-)'
  if grep -qE "${CHECKS}[^,]*,FAIL" "$SQL_LOG" || ! grep -qE "$CHECKS" "$SQL_LOG"; then
    grep -E "${CHECKS}[^,]*,FAIL" "$SQL_LOG" || echo "no check results (see $SQL_LOG)"
    echo "ERROR: sqlunit gate failed"; exit 1
  fi
  echo "sqlunit ok: $(grep -cE "$CHECKS" "$SQL_LOG") checks passed"
fi

check_log_secrets
echo; echo "local-ci: all stages passed"
