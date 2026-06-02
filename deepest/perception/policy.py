"""L2 perception: DOM-first, vision-fallback — running on BH's verbatim helpers.

Order per page (all on the one real-Chrome engine):
  1. per-site skill extractor (skills/<host>/extract.py) if present
  2. generic readable DOM, using browser-harness's field-tested wait + eval
     helpers (wait_for_load / wait_for_network_idle / js) reused verbatim
  3. vision fallback: screenshot (BH capture_screenshot) -> the vision brain

Returns a Perception so the runner records mode/used_vision and can trigger the
self-amending forge when DOM is thin.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

MIN_TEXT_CHARS = 200
VISION_MAX_DIM = int(os.environ.get("DEEPEST_VISION_MAX_DIM", "640"))


@dataclass
class Perception:
    mode: str              # "skill" | "dom" | "vision"
    text: str | None = None
    image_png: bytes | None = None
    note: str = ""


def host_of(url: str) -> str:
    return (urlparse(url).hostname or "").removeprefix("www.")


def perceive(engine, tab, url: str, skill_store=None) -> Perception:
    host = host_of(url)

    # 1) per-site skill (L3)
    if skill_store is not None:
        skill = skill_store.get(host)
        if skill is not None:
            try:
                text = skill.extract(engine, tab, url)
                if text and len(text) >= MIN_TEXT_CHARS:
                    return Perception(mode="skill", text=text, note=f"skill:{host}")
            except Exception as e:
                return _dom_or_vision(engine, tab, fail_note=f"skill_error:{e}")

    # 2 + 3) generic DOM via BH helpers, then vision
    return _dom_or_vision(engine, tab)


def _dom_or_vision(engine, tab, fail_note: str = "") -> Perception:
    h = engine.activate(tab)  # BH's verbatim helper toolkit, bound to this tab

    # BH expertise: wait for load, then settle network (degrades gracefully).
    try:
        h.wait_for_load(timeout=15.0)
        h.wait_for_network_idle(timeout=6.0, idle_ms=500)
    except Exception:
        pass

    try:
        text = h.js("document.body ? document.body.innerText : ''") or ""
    except Exception as e:
        text = ""
        fail_note = fail_note or f"dom_error:{e}"

    if text and len(text.strip()) >= MIN_TEXT_CHARS:
        return Perception(mode="dom", text=text, note=fail_note)

    # thin/empty DOM -> vision fallback (BH's capture_screenshot)
    try:
        path = h.capture_screenshot(max_dim=VISION_MAX_DIM)
        png = open(path, "rb").read()
        return Perception(mode="vision", image_png=png,
                          note=(fail_note or "thin_dom") + ";vision")
    except Exception as e:
        return Perception(mode="dom", text=text or "",
                          note=(fail_note or "thin_dom") + f";vision_failed:{e}")
