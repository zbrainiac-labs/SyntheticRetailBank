#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Scan this repo with the DataOpsBackbone SonarQube; with --strict fail on scanner
# errors, a failed quality gate, or open issues in new code. Used locally and in CI.
# Adapted from ../crew-asset-management/scripts/sonar-scan.sh (lcov instead of Python coverage).
#
# Inside a DataOpsBackbone runner (self-hosted CI: /sonar/.sonar_env + sonar-scanner present) the
# scan runs inline; otherwise in a scanner container that reuses the DataOpsBackbone stack:
#   network  *_sonarnet   (scanner reaches http://sonarqube:9000)
#   volume   *_sonar_env  (shared analysis token, written by sonar-init)
#   image    brainiac/local-github-runner (ships sonar-scanner)
#
# Usage: bash local-ci/sonar-self-scan.sh [--no-coverage] [--strict]
#   --no-coverage  reuse the coverage of a previous local-ci/unit-tests.sh run (CI: unit job artifact)
# Env:   SONAR_HOST (default http://sonarqube:9000), SONAR_PROJECT_KEY
#        (default SyntheticRetailBank-local; ci.yml sets SyntheticRetailBank-local-pr on pull requests)
#        RUNNER_IMAGE (default brainiac/local-github-runner)
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

KEY="${SONAR_PROJECT_KEY:-SyntheticRetailBank-local}"
case "$KEY" in
  SyntheticRetailBank) NAME="SyntheticRetailBank (main)" ;;
  *-local-pr) NAME="SyntheticRetailBank (pull requests)" ;;
  *) NAME="SyntheticRetailBank (self-scan)" ;;
esac
export H="${SONAR_HOST:-http://sonarqube:9000}" KEY NAME STRICT

# Scan + summary; runs with the repo as working directory (SRC).
# shellcheck disable=SC2016  # expanded by the inner bash
SCAN='
  set -Eeuo pipefail
  source /sonar/.sonar_env
  git config --global --add safe.directory "$SRC"
  WORK=$(mktemp -d)
  # Sonar starts slowly after docker start; wait for it to report UP.
  for _ in $(seq 1 60); do
    [[ "$(curl -sf "$H/api/system/status" | jq -r .status 2>/dev/null)" == UP ]] && break
    sleep 5
  done
  RC=0
  sonar-scanner -Dsonar.host.url="$H" -Dsonar.token="$SONAR_TOKEN" -Dsonar.projectKey="$KEY" -Dsonar.projectName="$NAME" \
    -Dsonar.tests=bank-app/__tests__,bank-app/e2e,sqlunit \
    -Dsonar.working.directory="$WORK/.scannerwork" -Dsonar.qualitygate.wait=true \
    >"$WORK/scanner.log" 2>&1 || RC=$?
  grep -E "ERROR|ANALYSIS SUCCESSFUL|QUALITY GATE|EXECUTION" "$WORK/scanner.log" || true
  if [[ "$RC" -ne 0 ]] && ! grep -q "QUALITY GATE STATUS: FAILED" "$WORK/scanner.log"; then
    echo "sonar-scanner failed (exit $RC)"; tail -n 40 "$WORK/scanner.log"; exit "$RC"
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

# --- inside a DataOpsBackbone runner: the job workspace is not visible to the docker host, scan inline
if [[ -r /sonar/.sonar_env ]] && command -v sonar-scanner >/dev/null; then
  [[ $COVERAGE -eq 0 ]] || { echo "ERROR: inside a runner use --no-coverage (coverage comes from the unit job)" >&2; exit 2; }
  echo "==> sonar-scanner (inline) -> $H  [$NAME]"
  cd "$REPO"
  SRC="$REPO" bash -c "$SCAN"
  exit
fi

# --- local / act: scanner container on the DataOpsBackbone network
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
echo "==> sonar-scanner -> $H  [$NAME]"
docker run --rm --network "$NETWORK" \
  -v "$VOLUME:/sonar:ro" -v "$REPO:/repo" -w /repo \
  -e H -e KEY -e NAME -e STRICT -e SRC=/repo \
  --user root --entrypoint bash "${RUNNER_IMAGE:-brainiac/local-github-runner}" -c "$SCAN"
