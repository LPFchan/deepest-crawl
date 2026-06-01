"""The brain — froggeric/Qwen3.6-27B vision-intact, via the local MLX-VLM server.

Talks to the OpenAI-compatible endpoint that `serve-brain.sh` exposes on
127.0.0.1:8765. Two entry points: summarize text (DOM path) and summarize an
image (vision-fallback path). Same model handles both.
"""
from __future__ import annotations

import base64
import json
import urllib.request

ENDPOINT = "http://127.0.0.1:8765/v1/chat/completions"
MODEL = "froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit"

SYSTEM = (
    "You are the summarization brain of a deep web crawler. You receive the "
    "content of a single web page (as text, or as a screenshot when text "
    "extraction failed). Produce a faithful, self-contained summary of what the "
    "page actually contains. Do not speculate beyond the content. If the page is "
    "an error, login wall, captcha, or empty, say so explicitly in one line."
)


def _post(payload: dict, timeout: float) -> str:
    req = urllib.request.Request(
        ENDPOINT,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = json.loads(r.read())
    return body["choices"][0]["message"]["content"].strip()


def summarize_text(url: str, text: str, max_chars: int = 12000,
                   max_tokens: int = 512, timeout: float = 180.0) -> str:
    text = (text or "").strip()[:max_chars]
    user = f"URL: {url}\n\n--- PAGE TEXT ---\n{text}"
    return _post({
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.2,
    }, timeout)


def summarize_image(url: str, png_bytes: bytes,
                    max_tokens: int = 512, timeout: float = 240.0) -> str:
    b64 = base64.b64encode(png_bytes).decode()
    data_uri = f"data:image/png;base64,{b64}"
    user = [
        {"type": "text",
         "text": f"URL: {url}\nText extraction failed; summarize this screenshot."},
        {"type": "image_url", "image_url": {"url": data_uri}},
    ]
    return _post({
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.2,
    }, timeout)


EXTRACTOR_SYSTEM = (
    "You write tiny, robust Python web-content extractors. You are given a host "
    "and an HTML excerpt of a page that defeated generic text extraction. Output "
    "ONLY a Python function with this exact signature and nothing else:\n\n"
    "    def extract(engine, tab, url):\n"
    "        ...\n        return text\n\n"
    "`engine` exposes: engine.evaluate(tab, js_expr, await_promise=False) -> any, "
    "engine.dom_text(tab) -> str, engine.html(tab) -> str, "
    "engine.cdp(tab, method, params) -> dict. Prefer engine.evaluate with a JS "
    "expression that targets this site's real content containers and returns a "
    "string. Return clean readable text. No imports, no markdown, no prose."
)


def generate_extractor(url: str, host: str, html_excerpt: str, reference: str = "",
                       max_tokens: int = 700, timeout: float = 180.0) -> str:
    ref_block = f"\n\n--- BROWSER-HARNESS EXPERTISE FOR THIS SITE ---\n{reference}" if reference else ""
    user = (f"host: {host}\nurl: {url}{ref_block}\n\n--- HTML EXCERPT ---\n"
            f"{html_excerpt[:16000]}")
    return _post({
        "model": MODEL,
        "messages": [
            {"role": "system", "content": EXTRACTOR_SYSTEM},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.1,
    }, timeout)


def alive(timeout: float = 3.0) -> bool:
    try:
        urllib.request.urlopen("http://127.0.0.1:8765/v1/models", timeout=timeout).close()
        return True
    except OSError:
        return False
