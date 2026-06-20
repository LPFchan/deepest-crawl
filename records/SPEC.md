# deepest-crawl Spec

- Project: deepest-crawl
- Project id: deepest-crawl
- Operator: local user
- Last updated: 2026-06-21 05-55-00 KST
- Related decisions: none yet

## Project Thesis

deepest-crawl is a local real-browser deep-crawl dashboard. It takes URLs from a
JSON queue, opens them through the operator's real Chrome profile, extracts page
content from DOM text, article text, or a screenshot, and summarizes the result
with a local OpenAI-compatible model server.
It also exposes an operator dashboard for inspecting, filtering, selecting, and
running crawls with live browser screenshots and agent trace.

## Core Capabilities

- Load crawl queues from `inputs/links.json`.
- Optionally extract stuck links from a compatible SQLite `link_cache` source
  into `inputs/links.json`.
- Drive the operator's real Chrome via Open Browser Use extension transport.
- Perceive page content through per-site extractors, generic DOM text, or
  screenshot fallback.
- Summarize DOM text or screenshots through a local OpenAI-compatible model
  endpoint; the bundled service helper defaults to MLX-VLM on
  `127.0.0.1:8765`.
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
- The dashboard must bind to an externally reachable host when requested and
  remain able to launch/reuse Chrome plus the local brain service.
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

## Architecture

The system separates browser connection from crawl intelligence.

- Browser connection: `deepest/engines/chrome.py` drives an already-running
  Chrome profile through the Open Browser Use extension transport.
- Browser mechanics: `deepest/bh/helpers.py` vendors browser-helper mechanics
  and runs them through `deepest/bh/_ipc.py`, a transport shim over the Open
  Browser Use CDP bridge.
- Perception: `deepest/perception/policy.py` prefers per-site extractors and
  rendered text, then escalates to screenshot vision when page state requires it.
- Brain: `deepest/brain.py` talks to a local OpenAI-compatible endpoint. The
  bundled service helper can start `mlx_vlm.server`, but any compatible endpoint
  can be configured with `DEEPEST_BRAIN_ENDPOINT`.
- Dashboard: `deepest/dashboard/` owns the operator workflow: queue filtering,
  selected/bulk crawl jobs, live browser preview, agent/tool timeline, service
  controls, and domain memory.

Per-link flow:

1. Open a crawler-owned tab and navigate to the target URL.
2. Handle redirects, short-link landing pages, transient security verification,
   and login/autofill requests when policy chooses them.
3. Extract rendered page content through DOM text, article extraction, and
   viewport context.
4. Use screenshot vision for visually blocked or verification-like states.
5. Use the Internet Archive only when the page is content-down, removed,
   unavailable, or similarly unrecoverable.
6. Generate a faithful summary and verify it with a second same-brain pass.
7. Persist the result and close the crawler-owned tab.

## Main Surfaces

- `extract_links.py`: optional SQLite `link_cache` export.
- `deepest/runner.py`: crawl loop and output writer.
- `deepest/engines/chrome.py`: real Chrome transport.
- `deepest/perception/policy.py`: DOM-first, screenshot-fallback perception.
- `deepest/brain.py`: local model client.
- `deepest/skills_store/`: generated extractor loading and forging.
- `deepest/services.py`: local service manager for MLX brain and Chrome
  transport.
- `deepest/dashboard/`: live dashboard, crawl queue, browser preview, agent
  timeline, domain memory editor, service controls, and bulk-crawl controls.
