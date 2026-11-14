#!/bin/bash
# -----------------------------------------------------------------------------
# Run the bank-app checks in a container: typecheck, vitest with coverage + JUnit.
# Same Node image as bank-app/Dockerfile, so tests run against what is deployed.
# Writes bank-app/reports/junit-unit.xml and bank-app/reports/coverage/lcov.info
# (paths relative to /repo, the same mount the sonar-scanner uses).
#
# Usage: bash local-ci/unit-tests.sh [extra vitest args]
# Env:   NODE_IMAGE (default node:26-slim)
# -----------------------------------------------------------------------------
set -Eeuo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${NODE_IMAGE:-node:26-slim}"

mkdir -p "$REPO/bank-app/reports"
# node_modules lives in a named volume: host modules are macOS builds, the container is linux.
docker run --rm -v "$REPO:/repo" -v bank-app-ci-node-modules:/repo/bank-app/node_modules \
  -w /repo/bank-app -e CI=1 -e CI_REPORTS=1 -e NEXT_TELEMETRY_DISABLED=1 \
  "$IMAGE" bash -c '
    set -Eeuo pipefail
    npm ci --no-audit --no-fund --loglevel=error
    npx tsc --noEmit
    echo "typecheck ok"
    npx vitest run --coverage "$@"
  ' _ "$@"
