"""Runtime signals over a fetched page — NOT engine routing.

There is one engine (real Chrome). These helpers tell the runner/skill-forge
*about the page*: did we hit a login wall, is the page blocked/empty, etc., so the
self-correction loop can react (e.g. claim a logged-in tab, or forge a per-site
skill) — all on the same single engine.
"""
from __future__ import annotations

from urllib.parse import urlparse


def host_of(url: str) -> str:
    return (urlparse(url).hostname or "").removeprefix("www.")


_LOGIN_URL_MARKERS = ("/login", "/i/flow/login", "accounts/login", "signin")
_LOGIN_TEXT_MARKERS = (
    "log in to", "sign in to continue", "create your account",
    "you must log in", "log in to twitter", "log in to x",
)


def looks_auth_walled(text: str | None, final_url: str | None) -> bool:
    if final_url and any(m in final_url for m in _LOGIN_URL_MARKERS):
        return True
    if text and len(text.strip()) < 400:
        low = text.lower()
        if any(m in low for m in _LOGIN_TEXT_MARKERS):
            return True
    return False


def looks_blocked(text: str | None) -> bool:
    if not text:
        return True
    low = text.lower()
    return any(m in low for m in (
        "are you a robot", "verify you are human", "captcha",
        "access denied", "403 forbidden", "rate limit",
    ))
