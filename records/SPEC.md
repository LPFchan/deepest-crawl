# deepest-crawl Spec

- Project: deepest-crawl
- Project id: deepest-crawl
- Operator: yeowool
- Last updated: 2026-06-21 05-39-42 KST
- Related decisions: none yet

## Project Thesis

deepest-crawl is a real-browser deep-crawl sidecar for Eastself. It takes links
that Eastself marked `unretrievable` or `needs_deep_crawl`, opens them through
the operator's real Chrome profile, extracts page content from DOM text or a
screenshot, and summarizes the result with a local multimodal MLX-VLM server.
It also exposes an operator dashboard for inspecting, filtering, selecting, and
running crawls with live browser screenshots and agent trace.

## Core Capabilities

- Extract stuck links from Eastself's SQLite `link_cache` into
  `inputs/links.json`.
- Drive the operator's real Chrome via Open Browser Use extension transport.
- Perceive page content through per-site extractors, generic DOM text, or
  screenshot fallback.
- Summarize DOM text or screenshots through a local OpenAI-compatible
  `mlx_vlm.server` on `127.0.0.1:8765`.
- Launch and monitor local Chrome transport plus the MLX brain from the
  dashboard service layer.
- Run agentic crawls that can navigate redirects, wait through transient
  verification pages, use Internet Archive fallback for content-down pages, and
  preserve per-domain notes/playbooks.
- Extract rendered article content with DOM, viewport-text, BeautifulSoup/lxml,
  and trafilatura-based main-content extraction before model summarization.
- Verify final summaries with a second same-brain verifier pass before
  accepting them, and strip browser/UI/extraction-internal prose from persisted
  results.
- Support dashboard bulk crawl operation with filtered queues, multi-select
  `Crawl Selected`, cancelation, per-job timeout/delay/jitter, status filters,
  and in-place sidebar refreshes.
- Use screenshot vision for agent decisions on security verification pages and
  fall back to visual checkbox detection when DOM access cannot find the widget.
- Cache generated per-site extractors under `skills/<host>/extract.py` after
  validation.

## Invariants

- The primary browser engine is the operator's real Chrome profile, not a
  separate headless browser.
- The dashboard must bind to an externally reachable host when requested
  (`0.0.0.0:8766` in the operator workflow) and remain able to launch/reuse
  Chrome plus the local MLX brain.
- The crawler must treat generated `inputs/` and `outputs/` data as local
  runtime artifacts, not source truth.
- Generated per-site extractor code is inspectable local code and is validated
  before caching, but it is not sandboxed.
- Bulk crawling through the real profile may expose cookies and logged-in
  browser context to target sites, so operational use must remain deliberate.
- Agentic automation may request Bitwarden autofill through the existing Chrome
  extension but must not read, print, or store passwords.
- Crawler-owned tabs should be closed after each job succeeds, fails, or is
  canceled.

## Main Surfaces

- `extract_links.py`: Eastself cache export.
- `deepest/runner.py`: crawl loop and output writer.
- `deepest/engines/chrome.py`: real Chrome transport.
- `deepest/perception/policy.py`: DOM-first, screenshot-fallback perception.
- `deepest/brain.py`: local model client.
- `deepest/skills_store/`: generated extractor loading and forging.
- `deepest/services.py`: local service manager for MLX brain and Chrome
  transport.
- `deepest/dashboard/`: live dashboard, crawl queue, browser preview, agent
  timeline, domain memory editor, service controls, and bulk-crawl controls.
