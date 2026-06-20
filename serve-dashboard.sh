#!/usr/bin/env bash
# deepest-crawl dashboard server
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST="${1:-0.0.0.0}"
PORT="${2:-8766}"
exec "$DIR/.venv/bin/python" -m deepest.dashboard.__main__ --host "$HOST" --port "$PORT"
