#!/usr/bin/env bash
# Run the loadtest binary against a running API.
# Usage: ./scripts/loadtest.sh [n] [concurrency] [type]
set -euo pipefail
N="${1:-10000}"
C="${2:-64}"
TYPE="${3:-flaky}"
cd "$(dirname "$0")/.."
go run ./cmd/loadtest -n "$N" -c "$C" -type "$TYPE" -fail-rate 0.05 -max-attempts 5 -wait 120s
