#!/bin/bash
# -----------------------------------------------------------------------------
# Build the bank-app image (linux/arm64, same as Docker Desktop and the SPCS pool) and
# smoke-test it: the standalone server starts and /api/health reports the runtime.
# No Snowflake credentials are passed, so only the runtime part of the health check runs.
#
# Usage: bash local-ci/image.sh [--platform=linux/arm64] [--tag=bank-app:local-ci]
# -----------------------------------------------------------------------------
set -Eeuo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLATFORM="linux/arm64"
TAG="bank-app:local-ci"
for arg in "$@"; do
  case "$arg" in
    --platform=*) PLATFORM="${arg#*=}" ;;
    --tag=*)      TAG="${arg#*=}" ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

REV=$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)
docker build --platform "$PLATFORM" --label org.opencontainers.image.revision="$REV" \
  -t "$TAG" "$REPO/bank-app"

NAME="bank-app-smoke-$$"
docker run -d --rm --platform "$PLATFORM" --name "$NAME" "$TAG" >/dev/null
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 30); do
  if docker exec "$NAME" node -e "fetch('http://127.0.0.1:8080/api/health').then(r=>r.json()).then(j=>{console.log(JSON.stringify(j));process.exit(j.ok===false?1:0)}).catch(()=>process.exit(1))"; then
    echo "image smoke test ok ($TAG, $PLATFORM)"
    docker image ls --format '  {{.Repository}}:{{.Tag}}  {{.Size}}' | grep -E "^  ${TAG%%:*}:" || true
    exit 0
  fi
  sleep 2
done
echo "ERROR: /api/health did not answer"; docker logs "$NAME" | tail -n 30
exit 1
