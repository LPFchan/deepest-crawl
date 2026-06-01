"""Self-amending: forge a per-site extractor when the generic path fails.

When the generic DOM path produces thin/blocked content for a host, we ask the
brain to WRITE a small `extract(engine, tab, url) -> str` for that host, validate
it by running it once, and — only if it yields real content — cache it to
skills/<host>/extract.py. Cached skills are reused (not regenerated per link) and
are plain inspectable files, so what the (uncensored) brain produces accrues
under review rather than being blind-exec'd on every link.

Trust boundary: generated code is executed locally in-process. v1 gates on
(a) it compiles, (b) it runs without raising, (c) it returns >= MIN chars.
That's the validation, not a sandbox — this is a local trusted tool.
"""
from __future__ import annotations

import importlib.util
import textwrap
from pathlib import Path

from ..reference import corpus
from ..signals import host_of

MIN_SKILL_CHARS = 200


def _validate_and_run(source: str, engine, tab, url) -> tuple[bool, str, str]:
    """Compile + execute generated source, return (ok, text, error)."""
    try:
        compile(source, "<forged-skill>", "exec")
    except SyntaxError as e:
        return False, "", f"syntax: {e}"
    ns: dict = {}
    try:
        exec(source, ns)  # noqa: S102 - trusted local tool, see module docstring
    except Exception as e:
        return False, "", f"exec: {type(e).__name__}: {e}"
    fn = ns.get("extract")
    if not callable(fn):
        return False, "", "no extract()"
    try:
        text = fn(engine, tab, url)
    except Exception as e:
        return False, "", f"run: {type(e).__name__}: {e}"
    text = (text or "").strip()
    if len(text) < MIN_SKILL_CHARS:
        return False, text, f"thin:{len(text)}"
    return True, text, ""


def forge(engine, tab, url, store, brain) -> tuple[bool, str]:
    """Try to create + cache a skill for url's host. Returns (saved, text)."""
    host = host_of(url)
    html = ""
    try:
        html = engine.html(tab)[:20000]
    except Exception:
        pass

    # Hand the brain BH's accumulated expertise for this host: the matching
    # per-site playbook plus the most relevant interaction guides.
    reference = "\n\n".join(filter(None, [
        corpus.domain_playbook(host),
        corpus.interaction_guide("iframes", "shadow-dom", "scrolling", "dropdowns"),
    ]))
    source = brain.generate_extractor(url=url, host=host, html_excerpt=html,
                                      reference=reference)
    source = _strip_fences(source)

    ok, text, err = _validate_and_run(source, engine, tab, url)
    if not ok:
        return False, ""  # caller falls back to vision; skill not cached

    path = store.path_for(host)
    path.parent.mkdir(parents=True, exist_ok=True)
    header = textwrap.dedent(f'''\
        """Forged per-site extractor for {host}. Auto-generated, then validated by
        running once (returned >= {MIN_SKILL_CHARS} chars). Inspect/edit freely."""
    ''')
    path.write_text(header + "\n" + source + "\n")
    store._cache.pop(host, None)  # bust loader cache so next get() reloads
    return True, text


def _strip_fences(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        lines = s.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines)
    return s
