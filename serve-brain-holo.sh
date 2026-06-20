#!/usr/bin/env bash
# deepest-crawl brain — Holo-3.1-9B-MLX vision-language model via MLX-VLM.
# OpenAI-compatible endpoint: http://127.0.0.1:8765/v1
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODEL="$HOME/models/Holo-3.1-9B-mlx"
exec "$DIR/.venv/bin/python" -m mlx_vlm.server --model "$MODEL" --host 127.0.0.1 --port 8765
