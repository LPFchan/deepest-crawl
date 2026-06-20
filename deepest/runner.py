"""The crawl runner: links.json -> real Chrome -> perception -> brain -> summaries.

ONE engine (your real Chrome via OBU transport). On it, in order:
  1. per-site skill extractor (skills/<host>/extract.py) if present
  2. generic readable DOM
  3. self-amend: if DOM is thin/blocked and --forge is on, ask the brain to
     write a per-site extractor, validate+cache it, retry once
  4. vision fallback: screenshot -> the same Qwen3.6 vision brain

Resumable: skips ids already in the output file.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from . import brain, services, signals
from .engines.chrome import RealChromeEngine
from .perception.policy import perceive
from .skills_store import forge as forge_mod
from .skills_store.store import SkillStore

ROOT = Path(__file__).resolve().parents[1]


def load_done(out_path: Path) -> dict[str, dict]:
    if not out_path.exists():
        return {}
    try:
        return {r["id"]: r for r in json.loads(out_path.read_text())}
    except Exception:
        return {}


def crawl_one(engine, store, link, do_forge: bool) -> dict:
    url = link["url"]
    rec = {"id": link["id"], "url": url, "engine": engine.name,
           "status": "failed", "mode": None, "used_vision": False,
           "forged": False, "summary": None, "error": None, "note": ""}
    tab = None
    try:
        tab = engine.new_tab(url)
        engine.cdp(tab, "Page.enable")
        time.sleep(1.0)  # crude settle; skills can wait smarter

        p = perceive(engine, tab, url, skill_store=store)

        # self-amend: generic path was thin -> try to forge a per-site skill once
        if do_forge and p.mode == "vision" and not store.has(signals.host_of(url)):
            saved, text = forge_mod.forge(engine, tab, url, store, brain)
            if saved:
                rec["forged"] = True
                p = perceive(engine, tab, url, skill_store=store)  # retry with skill

        rec["mode"] = p.mode
        rec["note"] = p.note
        if p.mode == "vision":
            rec["used_vision"] = True
            rec["summary"] = brain.summarize_image(url, p.image_png)
        else:
            rec["summary"] = brain.summarize_text(url, p.text or "")
        rec["status"] = "ok"
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"
    finally:
        if tab is not None:
            engine.close_tab(tab)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default=str(ROOT / "inputs/links.json"))
    ap.add_argument("--out", dest="out", default=str(ROOT / "outputs/summaries.json"))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--forge", action="store_true",
                    help="enable self-amending: brain writes per-site skills on thin pages")
    a = ap.parse_args()

    try:
        services.ensure_brain(status=lambda msg: print(f"[service] {msg}", flush=True))
        services.ensure_chrome_transport(
            status=lambda msg: print(f"[service] {msg}", flush=True)
        )
    except Exception as e:
        raise SystemExit(f"local service startup failed: {type(e).__name__}: {e}") from e

    links = json.loads(Path(a.inp).read_text())
    if a.limit:
        links = links[: a.limit]
    out_path = Path(a.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    done = load_done(out_path)

    store = SkillStore()
    engine = RealChromeEngine().connect()

    results = list(done.values())
    todo = [l for l in links if l["id"] not in done]
    print(f"{len(todo)} to crawl, {len(done)} already done | forge={a.forge}")

    try:
        for i, link in enumerate(todo, 1):
            rec = crawl_one(engine, store, link, a.forge)
            results.append(rec)
            flag = "👁" if rec["used_vision"] else ("🛠" if rec["forged"] else " ")
            print(f"[{i}/{len(todo)}] {rec['status']:6} {flag} {rec['mode'] or '-':6} "
                  f"{link['url'][:66]}")
            out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    finally:
        engine.close()
        services.shutdown_autostarted()


if __name__ == "__main__":
    main()
