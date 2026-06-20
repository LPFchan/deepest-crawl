# deepest-crawl

deepest-crawl is a local real-browser crawl dashboard for URLs that need more
than a simple HTTP fetch. It drives a normal Chrome profile, extracts rendered
page content, summarizes it through a local OpenAI-compatible model server, and
stores resumable JSON results.

```text
inputs/links.json -> real Chrome -> DOM/article extraction + screenshot vision -> local brain -> outputs/summaries.json
```

## What It Does

- Opens URLs in a real Chrome profile through the Open Browser Use extension
  transport.
- Shows a dashboard with a filterable URL queue, live browser screenshot, merged
  agent/tool timeline, domain notes, and bulk crawl controls.
- Supports status filters, multi-select `Crawl Selected`, cancellation,
  per-job timeout, delay, and jitter.
- Extracts content with rendered DOM text, BeautifulSoup/lxml, trafilatura, and
  screenshot vision when needed.
- Uses a same-brain verifier pass before accepting final summaries.
- Falls back to the Internet Archive for pages that are genuinely down, removed,
  or 404-like.
- Can request Chrome extension autofill for login workflows without reading or
  storing passwords.

## Important Safety Note

This tool intentionally uses a real Chrome profile. Target sites may see that
profile's cookies, logged-in state, extensions, IP address, and browser
fingerprint. Use selected batches, delay/jitter, and explicit cancellation when
crawling unfamiliar or large URL sets.

Bitwarden support is UI-only: the agent may request browser autofill through the
installed extension, but the project should never read, print, or persist
passwords.

## Requirements

- macOS or another environment that can run the configured Chrome transport.
- Python 3.12.
- Chrome with the Open Browser Use extension/native transport installed and
  enabled.
- A local OpenAI-compatible text or vision-language model endpoint. The built-in
  service helper defaults to MLX-VLM on `127.0.0.1:8765`.

## Install

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -U pip
.venv/bin/python -m pip install -e .
cp .env.example .env
```

The project ignores `.env`, `inputs/`, `outputs/`, `.tmp/`, and `.venv/`.

## Configuration

Common environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DEEPEST_BRAIN_ENDPOINT` | `http://127.0.0.1:8765/v1/chat/completions` | OpenAI-compatible chat completions endpoint. |
| `DEEPEST_BRAIN_MODEL` | project default MLX model | Model name or local model path sent to the brain server. |
| `DEEPEST_BRAIN_VISION` | `1` | Set `0` for text-only models. |
| `DEEPEST_BRAIN_AUTOSTART` | `1` | Let the dashboard/runner start the local brain server. |
| `DEEPEST_CHROME_AUTOSTART` | `1` | Let the dashboard/runner launch Chrome and wait for the transport. |
| `DEEPEST_BRAIN_HOST` | `127.0.0.1` | Host used when autostarting MLX-VLM. |
| `DEEPEST_BRAIN_PORT` | `8765` | Port used when autostarting MLX-VLM. |
| `DEEPEST_CHROME_APP` | `Google Chrome` | macOS app name used for Chrome autostart. |

The included `serve-brain.sh` and `serve-brain-holo.sh` are convenience scripts.
They are configurable through environment variables and are not required if you
already run a compatible model server.

## Input Format

Create `inputs/links.json`:

```json
[
  {
    "id": "example-1",
    "url": "https://example.com/article",
    "reason": "needs_deep_crawl",
    "sub_reason": "",
    "has_image": false,
    "prior_len": 0
  }
]
```

Only `url` is strictly required; stable `id` values make resumable result updates
cleaner.

`extract_links.py` is an optional adapter for a local SQLite `link_cache` source.
Set `DEEPEST_LINK_CACHE_DB` or pass explicit paths:

```bash
.venv/bin/python extract_links.py --db /path/to/cache.db --out inputs/links.json
```

## Run

Start the dashboard:

```bash
./serve-dashboard.sh
```

Default dashboard URL:

```text
http://127.0.0.1:8766/
```

For LAN access, the script defaults to binding `0.0.0.0`; pass a different host
or port when needed:

```bash
./serve-dashboard.sh 127.0.0.1 8766
```

Run the CLI crawler:

```bash
.venv/bin/python -m deepest.runner --limit 20
```

## Output

Results are written to `outputs/summaries.json` as JSON objects keyed by source
id and URL, with crawl status, final URL, summary text, and error information
when a crawl fails.

## Repository Notes

- `deepest/bh/` contains vendored browser-helper code adapted to the local
  transport shim.
- `deepest/reference/` contains browser/domain playbooks used as reference
  material for agent decisions.
- Repo-local records live under `records/` and use the provenance rules described
  in `records/REPO.md`.
