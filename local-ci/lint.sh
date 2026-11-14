#!/bin/bash
# -----------------------------------------------------------------------------
# Lint, all tools in containers: ShellCheck (CI + deploy scripts), actionlint (workflows),
# ruff (Python: generators, the_bank_app, sqlunit -- report-only until the clean-up step,
# LINT_STRICT=1 to enforce).
#
# Usage: bash local-ci/lint.sh
# -----------------------------------------------------------------------------
set -Eeuo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> shellcheck"
docker run --rm -v "$REPO:/mnt" -w /mnt koalaman/shellcheck:stable -S warning local-ci/*.sh

echo "==> actionlint"
docker run --rm -v "$REPO:/repo" -w /repo rhysd/actionlint:latest -color .github/workflows/ci.yml

echo "==> ruff"
RUFF_RC=0
docker run --rm -v "$REPO:/repo" -w /repo ghcr.io/astral-sh/ruff:latest check --output-format=concise \
  --exclude venv,venv.broken-py314,the_bank_app/venv generators the_bank_app sqlunit | tail -n 5 || RUFF_RC=$?
if [[ "$RUFF_RC" -ne 0 ]]; then
  if [[ "${LINT_STRICT:-0}" == 1 ]]; then exit "$RUFF_RC"; fi
  echo "ruff: findings reported (not enforced; set LINT_STRICT=1 to fail)"
fi
echo "lint ok"
