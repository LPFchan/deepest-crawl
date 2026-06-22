"""The brain — MLX-VLM server via OpenAI-compatible endpoint.

Override via env vars:
  DEEPEST_BRAIN_ENDPOINT  (default: http://127.0.0.1:8765/v1/chat/completions)
  DEEPEST_BRAIN_MODEL     (default: ~/models/Holo-3.1-9B-mlx)
  DEEPEST_BRAIN_VISION    (default: '1' — set to '0' for text-only models)
"""
from __future__ import annotations

import base64
import json
import os
from urllib.parse import urlparse, urlunparse
import urllib.request

ENDPOINT = os.environ.get(
    "DEEPEST_BRAIN_ENDPOINT",
    "http://127.0.0.1:8765/v1/chat/completions",
)
MODEL = os.environ.get("DEEPEST_BRAIN_MODEL",
    os.path.expanduser("~/models/Holo-3.1-9B-mlx"))
HAS_VISION = os.environ.get("DEEPEST_BRAIN_VISION", "1") == "1"

SYSTEM = (
    "You are the summarization brain of a deep web crawler. You receive the "
    "content of a single web page (as text, or as a screenshot when text "
    "extraction failed). Produce a faithful, self-contained summary of what the "
    "page actually contains. Do not speculate beyond the content. If the page is "
    "an error, login wall, captcha, or empty, say so explicitly in one line."
)


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


def configure(model: str | None = None, endpoint: str | None = None,
              vision: bool | None = None) -> None:
    global MODEL, ENDPOINT, HAS_VISION
    if model:
        MODEL = model
        os.environ["DEEPEST_BRAIN_MODEL"] = model
    if endpoint:
        ENDPOINT = endpoint
        os.environ["DEEPEST_BRAIN_ENDPOINT"] = endpoint
    if vision is not None:
        HAS_VISION = bool(vision)
        os.environ["DEEPEST_BRAIN_VISION"] = "1" if HAS_VISION else "0"


def current_model() -> str:
    return MODEL


def current_endpoint() -> str:
    return ENDPOINT


def has_vision() -> bool:
    return HAS_VISION


def _post(payload: dict, timeout: float) -> str:
    req = urllib.request.Request(
        current_endpoint(),
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = json.loads(r.read())
    msg = body["choices"][0]["message"]
    content = msg.get("content")
    if isinstance(content, str) and content.strip():
        return content.strip()

    # Some Qwen-style thinking servers return reasoning before final content.
    # Prefer final content, but keep smoke runs from crashing if only reasoning
    # is present because generation stopped early.
    reasoning = msg.get("reasoning") or msg.get("reasoning_content")
    if isinstance(reasoning, str) and reasoning.strip():
        return reasoning.strip()

    raise KeyError("content")


def complete(system: str, user: str, max_tokens: int | None = None,
             temperature: float = 0.2, timeout: float = 180.0) -> str:
    max_tokens = max_tokens or _env_int("DEEPEST_BRAIN_MAX_TOKENS", 512)
    return _post({
        "model": current_model(),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }, timeout)


def complete_with_image(system: str, user: str, png_bytes: bytes,
                        max_tokens: int | None = None, temperature: float = 0.2,
                        timeout: float = 240.0) -> str:
    if not has_vision():
        raise RuntimeError(
            f"complete_with_image called but DEEPEST_BRAIN_VISION=0 "
            f"(model has no vision tower).")
    max_tokens = max_tokens or _env_int("DEEPEST_BRAIN_VISION_MAX_TOKENS", 512)
    b64 = base64.b64encode(png_bytes).decode()
    data_uri = f"data:image/png;base64,{b64}"
    return _post({
        "model": current_model(),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": [
                {"type": "text", "text": user},
                {"type": "image_url", "image_url": {"url": data_uri}},
            ]},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }, timeout)


def summarize_text(url: str, text: str, max_chars: int | None = None,
                   max_tokens: int | None = None, timeout: float = 180.0) -> str:
    max_chars = max_chars or _env_int("DEEPEST_BRAIN_MAX_CHARS", 12000)
    max_tokens = max_tokens or _env_int("DEEPEST_BRAIN_MAX_TOKENS", 512)
    text = (text or "").strip()[:max_chars]
    user = f"URL: {url}\n\n--- PAGE TEXT ---\n{text}"
    return complete(SYSTEM, user, max_tokens=max_tokens, temperature=0.2,
                    timeout=timeout)


def summarize_image(url: str, png_bytes: bytes,
                    max_tokens: int | None = None, timeout: float = 240.0) -> str:
    if not has_vision():
        raise RuntimeError(
            f"summarize_image called but DEEPEST_BRAIN_VISION=0 (model "
            f"has no vision tower). URL: {url}")
    max_tokens = max_tokens or _env_int("DEEPEST_BRAIN_VISION_MAX_TOKENS", 512)
    b64 = base64.b64encode(png_bytes).decode()
    data_uri = f"data:image/png;base64,{b64}"
    user = [
        {"type": "text",
         "text": f"URL: {url}\nText extraction failed; summarize this screenshot."},
        {"type": "image_url", "image_url": {"url": data_uri}},
    ]
    return _post({
        "model": current_model(),
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
                       max_tokens: int | None = None, timeout: float = 180.0) -> str:
    max_tokens = max_tokens or _env_int("DEEPEST_BRAIN_EXTRACTOR_MAX_TOKENS", 700)
    ref_block = f"\n\n--- BROWSER-HARNESS EXPERTISE FOR THIS SITE ---\n{reference}" if reference else ""
    user = (f"host: {host}\nurl: {url}{ref_block}\n\n--- HTML EXCERPT ---\n"
            f"{html_excerpt[:16000]}")
    return _post({
        "model": current_model(),
        "messages": [
            {"role": "system", "content": EXTRACTOR_SYSTEM},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.1,
    }, timeout)


def models_endpoint() -> str:
    parsed = urlparse(current_endpoint())
    path = parsed.path
    marker = "/v1/chat/completions"
    if path.endswith(marker):
        path = path[: -len(marker)] + "/v1/models"
    else:
        path = "/v1/models"
    return urlunparse(parsed._replace(path=path, query="", fragment=""))


def alive(timeout: float = 3.0) -> bool:
    try:
        urllib.request.urlopen(models_endpoint(), timeout=timeout).close()
        return True
    except OSError:
        return False
