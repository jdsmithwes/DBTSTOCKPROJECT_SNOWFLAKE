#!/usr/bin/env bash
# dbt_run.sh — run dbt then SQLFluff lint as the final step.
#
# Usage:
#   ./dbt_run.sh                        # full run
#   ./dbt_run.sh --select mart_ml_features
#   ./dbt_run.sh --full-refresh --select mart_ml_features
#
# SQLFluff lint always runs last, even on partial --select runs,
# so every commit catches formatting drift before it reaches main.
#
# Exit codes:
#   0  — dbt succeeded AND lint is clean
#   1  — dbt failed (lint skipped)
#   2  — dbt succeeded but lint found violations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load credentials from .env (gitignored)
if [[ -f .env ]]; then
    set -a
    # shellcheck source=.env
    source .env
    set +a
else
    echo "ERROR: .env not found in $SCRIPT_DIR" >&2
    exit 1
fi

DBT="${DBT_BIN:-/Users/jamaalsmith/dbtenv/bin/dbt}"

echo "=== dbt run $* ==="
if ! "$DBT" run --profiles-dir . "$@"; then
    echo "=== dbt run FAILED — skipping lint ===" >&2
    exit 1
fi

echo ""
echo "=== SQLFluff lint (post-run) ==="
if sqlfluff lint models/ --templater jinja --dialect snowflake; then
    echo "=== All checks passed ==="
    exit 0
else
    echo "=== SQLFluff found violations — fix before merging to main ===" >&2
    exit 2
fi
