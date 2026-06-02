#!/usr/bin/env bash
# Simulate a worker crash to validate <5s recovery.
# Usage: ./scripts/kill-worker.sh           # picks the first worker container
set -euo pipefail
WID="$(docker compose ps --format '{{.Service}} {{.Name}}' | awk '$1=="worker"{print $2; exit}')"
if [[ -z "$WID" ]]; then
  echo "no worker container found" >&2
  exit 1
fi
echo "killing $WID"
docker kill -s KILL "$WID"
