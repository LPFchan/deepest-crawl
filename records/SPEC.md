# deepest-crawl Spec

- Project: deepest-crawl
- Project id: deepest-crawl
- Operator: yeowool
- Last updated: 2026-06-02 03-12-45 KST
- Related decisions: none yet

## Project Thesis

deepest-crawl is a real-browser deep-crawl sidecar for Eastself. It takes links
that Eastself marked `unretrievable` or `needs_deep_crawl`, opens them through
the operator's real Chrome profile, extracts page content from DOM text or a
screenshot, and summarizes the result with a local multimodal MLX-VLM server.

## Core Capabilities

- Extract stuck links from Eastself's SQLite `link_cache` into
  `inputs/links.json`.
- Drive the operator's real Chrome via Open Browser Use extension transport.
- Perceive page content through per-site extractors, generic DOM text, or
  screenshot fallback.
- Summarize DOM text or screenshots through a local OpenAI-compatible
  `mlx_vlm.server` on `127.0.0.1:8765`.
- Cache generated per-site extractors under `skills/<host>/extract.py` after
  validation.

## Invariants

- The primary browser engine is the operator's real Chrome profile, not a
  separate headless browser.
- The crawler must treat generated `inputs/` and `outputs/` data as local
  runtime artifacts, not source truth.
- Generated per-site extractor code is inspectable local code and is validated
  before caching, but it is not sandboxed.
- Bulk crawling through the real profile may expose cookies and logged-in
  browser context to target sites, so operational use must remain deliberate.

## Main Surfaces

- `extract_links.py`: Eastself cache export.
- `deepest/runner.py`: crawl loop and output writer.
- `deepest/engines/chrome.py`: real Chrome transport.
- `deepest/perception/policy.py`: DOM-first, screenshot-fallback perception.
- `deepest/brain.py`: local model client.
- `deepest/skills_store/`: generated extractor loading and forging.
- `deepest/dashboard/`: live dashboard scaffolding.
