"""Access to BH's copied-in expertise corpus (markdown), for the forge step.

We vendored browser-harness's per-site playbooks and interaction guides verbatim:
  reference/domain-skills/<host>/*.md   — 100+ per-site playbooks
  reference/interaction-skills/*.md     — dialogs, dropdowns, iframes, shadow-dom,
                                          uploads, downloads, scrolling, ...

When the brain forges a per-site extractor, we hand it the matching BH playbook
(if any) plus the relevant interaction guides as ground-truth context — so the
generated skill stands on BH's accumulated field knowledge instead of guessing.
"""
from __future__ import annotations

from pathlib import Path

REF = Path(__file__).resolve().parent
DOMAIN_SKILLS = REF / "domain-skills"
INTERACTION_SKILLS = REF / "interaction-skills"


def domain_playbook(host: str, max_chars: int = 6000) -> str:
    """Concatenated BH playbook(s) for a host, matched loosely by name.

    Matches the leading label of the host (e.g. 'twitter' -> twitter dir, also
    catches 'x' style dirs by substring). Returns '' if none.
    """
    if not DOMAIN_SKILLS.is_dir():
        return ""
    label = host.split(".")[0].lower()
    hits: list[Path] = []
    for d in DOMAIN_SKILLS.iterdir():
        if not d.is_dir():
            continue
        name = d.name.lower()
        if name == label or label in name or name in host.lower():
            hits.extend(sorted(d.rglob("*.md")))
    out, total = [], 0
    for p in hits:
        try:
            t = p.read_text()
        except Exception:
            continue
        out.append(f"# BH playbook: {p.relative_to(DOMAIN_SKILLS)}\n{t}")
        total += len(t)
        if total >= max_chars:
            break
    return "\n\n".join(out)[:max_chars]


def interaction_guide(*names: str, max_chars: int = 4000) -> str:
    """Return named interaction guides (e.g. 'iframes', 'shadow-dom')."""
    if not INTERACTION_SKILLS.is_dir():
        return ""
    out, total = [], 0
    for n in names:
        p = INTERACTION_SKILLS / f"{n}.md"
        if not p.exists():
            continue
        try:
            t = p.read_text()
        except Exception:
            continue
        out.append(f"# BH interaction skill: {n}\n{t}")
        total += len(t)
        if total >= max_chars:
            break
    return "\n\n".join(out)[:max_chars]


def list_domain_hosts() -> list[str]:
    if not DOMAIN_SKILLS.is_dir():
        return []
    return sorted(d.name for d in DOMAIN_SKILLS.iterdir() if d.is_dir())
