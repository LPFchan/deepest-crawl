#!/usr/bin/env bash
# deepest-crawl brain — vision-capable Qwen3.6-27B (heretic) via MLX-VLM.
# OpenAI-compatible endpoint: http://127.0.0.1:8765/v1
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$DIR/.venv/bin/python" -m mlx_vlm.server --model "froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit" --host 127.0.0.1 --port 8765
