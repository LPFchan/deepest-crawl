#!/usr/bin/env bash
# deepest-crawl brain convenience launcher via MLX-VLM.
# OpenAI-compatible endpoint defaults to http://127.0.0.1:8765/v1
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODEL="${DEEPEST_BRAIN_MODEL:-froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit}"
HOST="${DEEPEST_BRAIN_HOST:-127.0.0.1}"
PORT="${DEEPEST_BRAIN_PORT:-8765}"
exec "$DIR/.venv/bin/python" -m mlx_vlm.server --model "$MODEL" --host "$HOST" --port "$PORT"
