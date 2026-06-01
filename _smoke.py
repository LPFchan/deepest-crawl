"""Smoke test: verify imports, brain, Chrome transport, then crawl a few links.

Usage:
    python _smoke.py                        # verify setup only
    python _smoke.py --crawl 3              # verify + crawl N links
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PASS = "  \033[92m✓\033[0m"
FAIL = "  \033[91m✗\033[0m"


def check(label: str, ok: bool, detail: str = "") -> bool:
    icon = PASS if ok else FAIL
    d = f"  ({detail})" if detail else ""
    print(f"{icon} {label}{d}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--crawl", type=int, default=0, help="crawl N links after verification")
    a = ap.parse_args()

    print("\n=== deepest-crawl smoke test ===\n")
    ok = True

    # 1. core imports
    print("Core modules:")
    try:
        from deepest import brain as brain_mod, signals, runner
        from deepest.engines.chrome import RealChromeEngine
        from deepest.perception.policy import perceive
        from deepest.skills_store.store import SkillStore
        from deepest.skills_store.forge import forge
        check("deepest.brain", True)
        check("deepest.signals", True)
        check("deepest.runner", True)
        check("deepest.engines.chrome", True)
        check("deepest.perception.policy", True)
        check("deepest.skills_store.store", True)
        check("deepest.skills_store.forge", True)
    except ImportError as e:
        check("core imports", False, str(e))
        ok = False

    # 2. BH helpers + shim
    print("\nBH transport:")
    try:
        from deepest.bh import helpers, _ipc
        check("deepest.bh.helpers", True)
        check("deepest.bh._ipc (shim)", True)
    except ImportError as e:
        check("BH modules", False, str(e))
        ok = False

    # 3. OBU import
    print("\nOpen Browser Use:")
    try:
        import open_browser_use
        check("open_browser_use SDK", True)
    except ImportError as e:
        check("open_browser_use SDK", False, str(e))
        ok = False

    # 4. Brain health
    print("\nBrain (MLX-VLM @ :8765):")
    try:
        alive = brain_mod.alive()
        check("brain reachable", alive, "start ./serve-brain.sh if needed")
    except Exception as e:
        check("brain reachable", False, str(e))
        alive = False
        ok = False

    # 5. Chrome transport
    print("\nReal Chrome (OBU extension):")
    try:
        engine = RealChromeEngine()
        engine.connect()
        check("chrome connected", True, engine.name)
        tabs = engine.user_tabs()
        check(f"user tabs: {len(tabs)}", True)
        engine.close()
    except Exception as e:
        check("chrome connection", False, str(e))
        ok = False

    # 6. Input data
    print("\nInput data:")
    inp = ROOT / "inputs/links.json"
    if inp.exists():
        import json
        links = json.loads(inp.read_text())
        check(f"links.json: {len(links)} entries", True)
    else:
        check("links.json", False, "run extract_links.py first")
        ok = False

    if not ok:
        print(f"\n{FAIL} some checks failed — fix above before crawling\n")
        sys.exit(1)

    print(f"\n{PASS} all checks passed\n")

    if a.crawl and alive:
        print(f"--- crawling {a.crawl} links ---\n")
        sys.argv = ["runner.py", f"--limit={a.crawl}", "--forge"]
        try:
            runner.main()
        except SystemExit:
            pass


if __name__ == "__main__":
    main()
