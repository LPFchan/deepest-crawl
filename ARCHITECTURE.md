# deepest-crawl architecture

**One engine — your actual real Chrome (your profile, your extensions, your
logins).** On it: DOM-default → vision-fallback, self-amending, per-site skills.
No engine routing.

Why this shape: the four wants split into two kinds of property.

- **"Real Chrome + my profile/extensions"** is a *connection* property. Delivered
  by **open-browser-use's extension transport** — it drives your already-running
  real Chrome via `chrome.debugger`, with no `--remote-debugging-port`, no
  separate/"testing" profile, your extensions and logins intact. (browser-harness
  needs Chrome launched with a debug port, which modern Chrome blocks on the
  default profile — the separate-profile thing we're avoiding.)
- **DOM→vision, self-amending, per-site skills** are *intelligence* properties.
  They're just Python over CDP. We own them. (browser-harness's runtime is NOT
  used; its self-amending / per-site-skill *patterns* were ported here.)

Both cloned repos are consumed as libraries in `.venv`; neither is edited.

## Maximal reuse of browser-harness (copy/paste is a virtue here)

We don't reimplement BH's browser mechanics — we **vendor them verbatim** and run
them on our transport:

- `deepest/bh/helpers.py` — **byte-for-byte BH** (`shasum`-verified identical to
  the source). ~500 lines of field-tested CDP logic: `click_at_xy` (with retina
  dpr handling), `fill_input` (framework-aware), `press_key`, `scroll`,
  `wait_for_load`, `wait_for_element`, `wait_for_network_idle`, `js` (auto-IIFE),
  `capture_screenshot`, `page_info`, `upload_file`, `http_get`, …
- `deepest/bh/_ipc.py` — the **only** thing we wrote: a drop-in transport shim
  implementing BH's daemon wire-protocol on top of OBU `executeCdp`. helpers.py
  does `from . import _ipc as ipc`, so the verbatim file runs on your real Chrome
  with zero edits. (Proven offline: BH's verbatim `wait_for_load`/`js`/`page_info`
  round-trip through the shim.)
- `deepest/reference/` — BH's **expertise as data**, copied verbatim:
  `domain-skills/` (109 per-site playbooks) + `interaction-skills/` (17 mechanics
  guides). The forge step feeds the matching playbook + relevant guides to the
  brain so generated extractors stand on BH's accumulated field knowledge.

Shim degradations (v1, noted in `_ipc.py`): cross-frame session routing for
iframe-targeted `js(target_id=...)` and live event draining (`drain_events`,
which `wait_for_network_idle` uses) are not wired to OBU's notification stream
yet — they degrade safely rather than crash.

```
┌─ L3  BRAIN + SELF-AMENDING        deepest/skills_store/ + deepest/brain.py
│   • per-site skills: skills/<host>/extract.py  (OUR store, owned here)
│   • forge.py: when generic path is thin, brain WRITES an extractor for the
│     host, it's validated by running once, then cached + reused (inspectable)
│   • brain = froggeric/Qwen3.6-27B vision-intact via MLX-VLM @ :8765
├─ L2  PERCEPTION POLICY            deepest/perception/policy.py
│   • per-site skill → generic readable DOM → screenshot+vision fallback
├─ L1  EXECUTION (single engine)    deepest/engines/{base,chrome}.py
│   • BrowserEngine.cdp(tab, method, params) — the one seam
│   • RealChromeEngine → OBU extension transport → YOUR real Chrome
└──────────────────────────────────────────────────────────────────────────
   signals.py  page-state signals (auth-wall/blocked) for self-correction
   runner.py   inputs/links.json → crawl → outputs/summaries.json (resumable)
```

## Per-link flow (all on the one engine)

1. open tab in your real Chrome, navigate, settle
2. **skill** — if `skills/<host>/extract.py` exists, use it
3. **DOM** — else generic readable `innerText`
4. **self-amend** (`--forge`) — if DOM is thin/blocked and no skill yet, brain
   writes one for this host, validate+cache, retry once
5. **vision** — if still thin, screenshot → Qwen3.6 vision brain
6. summarize → append to `outputs/summaries.json`

## Self-amending, safely

Forged extractors are written to `skills/<host>/extract.py`, validated by running
once (must return ≥200 chars), cached, and reused — not regenerated per link.
They're plain inspectable files, so what the (uncensored) brain emits accrues
under review. Trust boundary: generated code is `exec`'d locally in-process —
validation, not a sandbox. This is a local trusted tool.

## Operational caveats (real, not blocking)

- **Your real profile touches every link.** Bulk-crawling 21k `unretrievable`
  links through your daily Chrome sends your logged-in identity/cookies to all of
  them, makes that browser busy while it runs, and is serial (one profile, no
  parallelism). Intended for auth-walled links (twitter needs your login); just
  know it applies to the sketchy ones too.
- OBU's `chrome.debugger` transport shows Chrome's "debugging" banner and
  conflicts with having DevTools open on the controlled tab; the MV3 worker can
  be evicted on long idle.

## Run

```bash
./serve-brain.sh                                   # brain on :8765 (own terminal)
open-browser-use info                              # confirm real-Chrome transport
.venv/bin/python -m deepest.runner --limit 20             # smoke (DOM/vision only)
.venv/bin/python -m deepest.runner --limit 50 --forge     # + self-amending skills
```
