# deepest-crawl

A "deepest" web crawler — a sidecar of **Eastself**. It takes the links Eastself
couldn't retrieve (marked `unretrievable` / `needs_deep_crawl` in `link_cache`),
drives a real browser through them, and emits content summaries.

```
inputs/links.json  ──►  engine (OBU ⇄ BH)  ──►  brain (MLX-VLM)  ──►  outputs/summaries.json
```

## Pieces

| Piece | Choice | Status |
|-------|--------|--------|
| Brain (LLM) | `froggeric/Qwen3.6-27B-Uncensored-Heretic-v2-MLX-4bit` — **vision-intact** (verified: 333 `vision_tower.*` tensors), via MLX-VLM | ⏳ downloading (16.1 GB) |
| Serving | local OpenAI-compatible server (`mlx_vlm.server`) on `127.0.0.1:8765` | ✅ `serve-brain.sh` |
| Engine (single) | **real Chrome** via OBU extension transport (your profile/extensions/logins) | ✅ `engines/chrome.py` |
| Browser mechanics | **browser-harness `helpers.py` vendored verbatim** (sha-identical), run on a 1-file OBU transport shim (`bh/_ipc.py`) | ✅ proven offline |
| Expertise corpus | BH `domain-skills/` (109 sites) + `interaction-skills/` (17 guides) copied in; fed to the forge | ✅ `reference/` |
| Smarts (ours) | DOM→vision + self-amending + per-site skills, all on the one engine | ✅ scaffolded |
| Input | `extract_links.py` reads Eastself `cache.db` → `inputs/links.json` | ✅ 21,112 links |
| Output | JSON summaries | ⏳ engine TODO |

## The brain CAN see

`froggeric/...Heretic-v2-MLX-4bit` keeps the full Qwen3.6 vision tower, so one
model handles both **text/DOM summarization** and **screenshot vision**. Routing
plan: extract readable DOM text first (cheap); fall back to a screenshot through
the same model only when a page is image-only or visually blocks extraction.

(The originally-requested `dawncr0w/...Native-MTP-Preserved-oQ4-MLX` is text-only
— vision tower stripped — so it was rejected.)

## Input

`extract_links.py` (read-only on Eastself's DB) pulls from
`~/Documents/Eastself/data/cache.db` table `link_cache`, filtering
`triage_status IN ('unretrievable','needs_deep_crawl')` → **21,112 unique links**.

```bash
.venv/bin/python extract_links.py            # → inputs/links.json
```
Each record: `{id, url, reason, sub_reason, has_image, prior_len}`.

## Output contract (draft)

`outputs/summaries.json`:
```json
[{ "id": "abc123", "url": "https://…", "status": "ok|failed|blocked",
   "engine": "obu|bh", "used_vision": false, "summary": "…", "error": null }]
```

## Setup done

- `.venv` (Python 3.12): `mlx-lm 0.31.3`, `mlx-vlm 0.5.0`
- HF token from vaultwarden `llm/HF_TOKEN` → `.env` (gitignored)
- `./serve-brain.sh` → `http://127.0.0.1:8765/v1` (OpenAI-compatible)
- `inputs/links.json` (21,112 links)

## TODO

1. Finish brain download; smoke-test text + image inference via mlx_vlm.
2. Decide OBU vs BH primary/fallback (start one engine, measure on a sample).
3. Write the crawl runner: links.json → browser fetch → DOM/vision → brain → summaries.json.
