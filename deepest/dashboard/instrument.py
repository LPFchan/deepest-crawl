"""Instrument the crawl pipeline to push state to the dashboard store.

Call `patch()` once to hook into brain, perception, and runner.
"""

from __future__ import annotations

import functools
import time

from .state import STATE, CrawlStep


def patch():
    import deepest.brain as brain_mod
    import deepest.runner as runner_mod
    import deepest.perception.policy as policy_mod

    _patch_brain(brain_mod)
    _patch_perception(policy_mod)
    _patch_runner(runner_mod)


def _patch_brain(brain_mod):
    orig_post = brain_mod._post

    @functools.wraps(orig_post)
    def _post(payload: dict, timeout: float) -> str:
        messages = payload.get("messages", [])
        STATE.update(prompt=repr(messages))
        result = orig_post(payload, timeout)
        STATE.update(response=result)
        return result

    brain_mod._post = _post


def _patch_perception(policy_mod):
    orig_perceive = policy_mod.perceive

    @functools.wraps(orig_perceive)
    def perceive(engine, tab, url, **kw):
        STATE.update(url=url)
        result = orig_perceive(engine, tab, url, **kw)
        STATE.update(
            mode=result.mode,
            dom_text=result.text or "",
            png_bytes=result.image_png,
            note=result.note,
        )
        return result

    policy_mod.perceive = perceive


def _patch_runner(runner_mod):
    orig_crawl_one = runner_mod.crawl_one

    @functools.wraps(orig_crawl_one)
    def crawl_one(engine, store, link, do_forge):
        STATE.current = CrawlStep(
            id=link["id"],
            url=link["url"],
            status="crawling",
        )
        try:
            result = orig_crawl_one(engine, store, link, do_forge)
            STATE.update(
                status=result.get("status", "done"),
                error=result.get("error", ""),
                note=result.get("note", ""),
                response=result.get("summary", "") or result.get("error", ""),
            )
            STATE.complete_current()
            return result
        except Exception as e:
            STATE.update(status="error", error=str(e))
            STATE.complete_current()
            raise

    runner_mod.crawl_one = crawl_one

    orig_main = runner_mod.main

    @functools.wraps(orig_main)
    def main():
        import argparse
        import json
        from pathlib import Path

        ap = argparse.ArgumentParser()
        ap.add_argument("--in", dest="inp", default=str(runner_mod.ROOT / "inputs/links.json"))
        ap.add_argument("--out", dest="out", default=str(runner_mod.ROOT / "outputs/summaries.json"))
        ap.add_argument("--limit", type=int, default=0)
        ap.add_argument("--forge", action="store_true")
        ap.add_argument("--dashboard", action="store_true", help="start dashboard server")
        ap.add_argument("--dashboard-host", default="127.0.0.1")
        ap.add_argument("--dashboard-port", type=int, default=8766)
        a = ap.parse_args()

        if a.dashboard:
            from .server import start as start_dash
            import threading
            threading.Thread(target=start_dash, args=(a.dashboard_host, a.dashboard_port), daemon=True).start()
            print(f"dashboard at http://{a.dashboard_host}:{a.dashboard_port}")

        links = json.loads(Path(a.inp).read_text())
        if a.limit:
            links = links[:a.limit]

        STATE.progress = {"done": 0, "total": len(links)}

        out_path = Path(a.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        from ..engines.chrome import RealChromeEngine
        from ..skills_store.store import SkillStore
        engine = RealChromeEngine().connect()
        store = SkillStore()

        results = []
        try:
            for i, link in enumerate(links, 1):
                rec = crawl_one(engine, store, link, a.forge)
                results.append(rec)
                STATE.progress["done"] = i
                out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2))
        finally:
            engine.close()

    runner_mod.main = main
