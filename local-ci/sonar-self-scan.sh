#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Scan this repo with the DataOpsBackbone SonarQube; with --strict fail on scanner
# errors, a failed quality gate, or open issues in new code. Used locally and in CI.
# Adapted from ../crew-asset-management/scripts/sonar-scan.sh (lcov instead of Python coverage).
#
# Reuses the DataOpsBackbone stack:
#   network  *_sonarnet   (scanner reaches http://sonarqube:9000)
#   volume   *_sonar_env  (shared analysis token, written by sonar-init)
#   image    brainiac/local-github-runner (ships sonar-scanner)
#
# Usage: bash local-ci/sonar-self-scan.sh [--no-coverage] [--strict]
#   --no-coverage  reuse the coverage of a previous local-ci/unit-tests.sh run
# Env:   SONAR_HOST (default http://sonarqube:9000), SONAR_PROJECT_KEY
#        (default synthetic-retail-bank-local), RUNNER_IMAGE (default brainiac/local-github-runner)
# -----------------------------------------------------------------------------
set -Eeuo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COVERAGE=1; STRICT=0
for arg in "$@"; do
  case "$arg" in
    --no-coverage) COVERAGE=0 ;;
    --strict)      STRICT=1 ;;
    -h|--help)     sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

NETWORK=$(docker network ls --format '{{.Name}}' | grep -m1 'sonarnet$' || true)
VOLUME=$(docker volume ls --format '{{.Name}}' | grep -m1 'sonar_env$' || true)
[[ -n "$NETWORK" && -n "$VOLUME" ]] || {
  echo "ERROR: DataOpsBackbone SonarQube stack not found (need *_sonarnet network and *_sonar_env volume)." >&2
  echo "       Start it with ../DataOpsBackbone/start.sh" >&2
  exit 1
}
if docker ps -a --format '{{.Names}}' | grep -qx sonarqube && ! docker ps --format '{{.Names}}' | grep -qx sonarqube; then
  echo "==> Starting sonarqube container"
  docker start sonarqube >/dev/null
fi

# --- 1. Unit tests with coverage (same /repo mount as the scanner, so paths match)
if [[ $COVERAGE -eq 1 ]]; then
  echo "==> unit tests with coverage"
  bash "$REPO/local-ci/unit-tests.sh" | tail -n 5
fi

# --- 2. Scan, 3. summary
echo "==> sonar-scanner -> ${SONAR_HOST:-http://sonarqube:9000}"
docker run --rm --network "$NETWORK" \
  -v "$VOLUME:/sonar:ro" -v "$REPO:/repo" -w /repo \
  -e H="${SONAR_HOST:-http://sonarqube:9000}" \
  -e KEY="${SONAR_PROJECT_KEY:-synthetic-retail-bank-local}" \
  -e STRICT="$STRICT" \
  --user root --entrypoint bash "${RUNNER_IMAGE:-brainiac/local-github-runner}" -c '
    set -Eeuo pipefail
    source /sonar/.sonar_env
    git config --global --add safe.directory /repo
    # Sonar starts slowly after docker start; wait for it to report UP.
    for _ in $(seq 1 60); do
      [[ "$(curl -sf "$H/api/system/status" | jq -r .status 2>/dev/null)" == UP ]] && break
      sleep 5
    done
    RC=0
    sonar-scanner -Dsonar.host.url="$H" -Dsonar.token="$SONAR_TOKEN" -Dsonar.projectKey="$KEY" \
      -Dsonar.working.directory=/tmp/.scannerwork -Dsonar.qualitygate.wait=true \
      >/tmp/scanner.log 2>&1 || RC=$?
    grep -E "ERROR|ANALYSIS SUCCESSFUL|QUALITY GATE|EXECUTION" /tmp/scanner.log || true
    if [[ "$RC" -ne 0 ]] && ! grep -q "QUALITY GATE STATUS: FAILED" /tmp/scanner.log; then
      echo "sonar-scanner failed (exit $RC)"; tail -n 40 /tmp/scanner.log; exit "$RC"
    fi

    api() { curl -sf -u "$SONAR_TOKEN:" "$H/api/$1"; }
    echo
    echo "Quality gate: $(api "qualitygates/project_status?projectKey=$KEY" | jq -r .projectStatus.status)"
    api "measures/component?component=$KEY&metricKeys=bugs,vulnerabilities,security_hotspots,code_smells,coverage,duplicated_lines_density,ncloc" \
      | jq -r ".component.measures[] | \"  \(.metric): \(.value)\""
    LEGACY=$(api "issues/search?componentKeys=$KEY&resolved=false&severities=BLOCKER,CRITICAL&ps=1" | jq .total)
    echo "BLOCKER+CRITICAL issues (all code, reported): $LEGACY"
    ISSUES=$(api "issues/search?componentKeys=$KEY&resolved=false&inNewCodePeriod=true&ps=500")
    COUNT=$(jq ".total" <<<"$ISSUES")
    echo "Open issues in new code (enforced with --strict): $COUNT"
    jq -r ".issues[:25][] | [.severity, .rule, (.component|sub(\"^[^:]*:\";\"\")), (.line//0|tostring), .message] | @tsv" <<<"$ISSUES"
    echo "Dashboard: http://localhost:9000/dashboard?id=$KEY"
    if [[ "$STRICT" -eq 1 ]]; then
      [[ "$RC" -eq 0 && "$COUNT" -eq 0 ]]
    fi
  '
