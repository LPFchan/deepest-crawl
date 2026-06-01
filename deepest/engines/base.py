"""Engine-agnostic browser backend.

Both browser-harness (BH) and open-browser-use (OBU) ultimately drive Chrome via
the Chrome DevTools Protocol. This module defines the single seam everything else
in deepest-crawl is written against: a `cdp(method, params)` call plus a few
convenience wrappers. Swap the backend, keep the whole stack above it unchanged.

Layering (see README):
  L1 = this file + concrete backends  (engines/bh.py, engines/obu.py)
  L2 = perception policy              (perception/)
  L3 = brain loop + skill store       (skills_store/, runner)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TabHandle:
    """Backend-opaque reference to one controlled tab.

    `id` is whatever the backend uses to address the tab (BH: targetId str;
    OBU: chrome tab id int). Callers never interpret it — they pass it back.
    """
    id: Any
    backend: str
    meta: dict[str, Any] = field(default_factory=dict)


class BrowserEngine(ABC):
    """The L1 seam. Concrete backends implement these; nothing else does CDP."""

    name: str = "base"

    @abstractmethod
    def connect(self) -> "BrowserEngine": ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def new_tab(self, url: str | None = None) -> TabHandle: ...

    @abstractmethod
    def cdp(self, tab: TabHandle, method: str, params: dict | None = None) -> dict:
        """Raw CDP escape hatch. Everything below is sugar over this."""

    # ---- convenience sugar (default impls live on the base via cdp) ----

    def navigate(self, tab: TabHandle, url: str) -> dict:
        self.cdp(tab, "Page.enable")
        return self.cdp(tab, "Page.navigate", {"url": url})

    def evaluate(self, tab: TabHandle, expression: str, await_promise: bool = False) -> Any:
        r = self.cdp(tab, "Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": await_promise,
        })
        result = r.get("result", {}) if isinstance(r, dict) else {}
        return result.get("value")

    def dom_text(self, tab: TabHandle) -> str:
        val = self.evaluate(tab, "document.body ? document.body.innerText : ''")
        return val or ""

    def html(self, tab: TabHandle) -> str:
        val = self.evaluate(tab, "document.documentElement ? document.documentElement.outerHTML : ''")
        return val or ""

    def screenshot_png(self, tab: TabHandle, full_page: bool = False) -> bytes:
        import base64
        r = self.cdp(tab, "Page.captureScreenshot",
                     {"format": "png", "captureBeyondViewport": full_page})
        data = r.get("data") if isinstance(r, dict) else None
        if not data:
            raise RuntimeError(f"{self.name}: screenshot returned no data")
        return base64.b64decode(data)

    def current_url(self, tab: TabHandle) -> str:
        return self.evaluate(tab, "location.href") or ""
