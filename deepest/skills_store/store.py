"""L3 skill store — deepest-crawl's OWN, decoupled per-domain skill registry.

Deliberately NOT browser-harness's domain-skills dir: we treat BH as a library
and keep our learned extractors here, under deepest-crawl/skills/<host>/.

A skill is a small Python module exposing:
    def extract(engine, tab, url) -> str
that returns clean page text for one host. The brain writes/refines these when a
host defeats the generic DOM path (self-amending); they are cached and reused
(not regenerated per link), so what accrues stays inspectable.

This file is the loader/runtime. Generation/refinement (the self-correction loop)
lands in a separate module once the engine path is validated end-to-end.
"""
from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path

SKILLS_DIR = Path(__file__).resolve().parents[2] / "skills"


@dataclass
class Skill:
    host: str
    path: Path
    _fn = None

    def extract(self, engine, tab, url) -> str:
        if self._fn is None:
            spec = importlib.util.spec_from_file_location(
                f"deepest_skill_{self.host.replace('.', '_')}", self.path)
            if not spec or not spec.loader:
                raise RuntimeError(f"cannot load skill {self.path}")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            fn = getattr(mod, "extract", None)
            if not callable(fn):
                raise RuntimeError(f"skill {self.path} has no extract()")
            self._fn = fn
        return self._fn(engine, tab, url)


class SkillStore:
    """Loads skills/<host>/extract.py on demand."""

    def __init__(self, root: Path | None = None):
        self.root = root or SKILLS_DIR
        self._cache: dict[str, Skill | None] = {}

    def get(self, host: str) -> Skill | None:
        if host in self._cache:
            return self._cache[host]
        path = self.root / host / "extract.py"
        skill = Skill(host=host, path=path) if path.exists() else None
        self._cache[host] = skill
        return skill

    def has(self, host: str) -> bool:
        return self.get(host) is not None

    def path_for(self, host: str) -> Path:
        return self.root / host / "extract.py"
