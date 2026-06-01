"""Thread-safe live state store that the crawl pipeline pushes into."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class CrawlStep:
    id: str = ""
    url: str = ""
    mode: str = ""
    png_bytes: bytes | None = None
    dom_text: str = ""
    html_excerpt: str = ""
    prompt: str = ""
    response: str = ""
    actions: list[dict] = field(default_factory=list)
    status: str = "pending"
    error: str = ""
    note: str = ""


class DashboardState:
    def __init__(self):
        self._lock = threading.Lock()
        self._current: CrawlStep = CrawlStep()
        self._history: list[CrawlStep] = []
        self._listeners: list[Callable[[CrawlStep], None]] = []
        self.progress = {"done": 0, "total": 0}

    @property
    def current(self) -> CrawlStep:
        with self._lock:
            return self._current

    @current.setter
    def current(self, val: CrawlStep) -> None:
        with self._lock:
            self._current = val
            self._notify(val)

    def push_action(self, action: dict) -> None:
        with self._lock:
            self._current.actions.append(action)
            self._notify(self._current)

    def update(self, **kwargs) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self._current, k, v)
            self._notify(self._current)

    def complete_current(self) -> None:
        with self._lock:
            self._current.status = "done"
            self._history.append(self._current)
            self._current = CrawlStep()
            self._notify(self._current)

    def listen(self, callback: Callable[[CrawlStep], None]) -> Callable:
        self._listeners.append(callback)
        return lambda: self._listeners.remove(callback)

    def _notify(self, step: CrawlStep) -> None:
        for cb in self._listeners:
            try:
                cb(step)
            except Exception:
                pass


STATE = DashboardState()
