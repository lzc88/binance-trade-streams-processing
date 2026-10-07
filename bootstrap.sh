#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Syncing ingestion venv (uv)"
cd "$ROOT_DIR"
uv sync

echo "==> Starting Redpanda and Redpanda Console"
docker compose -f "$ROOT_DIR/docker-compose.yml" up -d redpanda console

echo "==> Creating raw_trades topic"
docker exec redpanda rpk cluster health --watch --exit-when-healthy
docker exec redpanda rpk topic describe raw_trades >/dev/null 2>&1 \
    || docker exec redpanda rpk topic create raw_trades -p 3

echo "==> Bootstrap complete"
