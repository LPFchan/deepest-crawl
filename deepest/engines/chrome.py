"""The ONE engine: your actual real Chrome (your profile, extensions, logins).

Transport is open-browser-use's extension path — the only clean way to control
your *already-running* real Chrome with your real profile, without
`--remote-debugging-port`, without a separate/"testing" profile, and with your
installed extensions and logged-in sessions intact. We consume open-browser-use
strictly as a library (the cloned repo is never edited); everything above this
file (perception, self-amending, per-site skills, brain) is our own code, written
against the engine-agnostic `cdp()` seam in base.py.

There is no engine routing. This is the single backend. (browser-harness's
runtime is not used; its self-amending / per-site-skill *patterns* were ported
into deepest-crawl and are owned here.)

Prereqs (one-time, user cooperation): real Chrome open with the Open Browser Use
extension enabled. Verify with `open-browser-use info`.
"""
from __future__ import annotations

import json
import threading
import uuid
from pathlib import Path

from .base import BrowserEngine, TabHandle

ACTIVE_REGISTRY = Path("/tmp/open-browser-use/active.json")


def discover_socket() -> str:
    """Read OBU's active-socket registry (written by the native host when Chrome
    + extension are live)."""
    if not ACTIVE_REGISTRY.exists():
        from .. import services
        services.ensure_chrome_transport()
    if not ACTIVE_REGISTRY.exists():
        raise RuntimeError(
            "Real-Chrome transport not reachable: no OBU socket registry. "
            "Chrome autostart was attempted. Open your real Chrome with the Open Browser Use extension enabled, "
            "then verify with `open-browser-use info`."
        )
    return json.loads(ACTIVE_REGISTRY.read_text())["socketPath"]


class RealChromeEngine(BrowserEngine):
    """Single engine — your real Chrome via the OBU extension transport."""

    name = "chrome"

    def __init__(self, session_id: str | None = None, socket_path: str | None = None,
                 timeout: float = 30.0):
        self.session_id = session_id or f"deepest-{uuid.uuid4().hex[:8]}"
        self._socket_path = socket_path
        self._timeout = timeout
        self._client = None
        self._lock = threading.RLock()

    # ---- lifecycle ----
    def connect(self) -> "RealChromeEngine":
        from open_browser_use.client import OpenBrowserUseClient  # type: ignore
        from ..bh import _ipc as bh_ipc  # bind BH's verbatim helpers to this engine
        sock = self._socket_path or discover_socket()
        self._client = OpenBrowserUseClient(
            socket_path=sock, session_id=self.session_id, timeout=self._timeout,
        ).connect()
        bh_ipc.bind(self)
        return self

    def activate(self, tab: TabHandle):
        """Point BH's verbatim helper toolkit at `tab` and return the helpers
        module. Lets perception/skills call BH's field-tested mechanics directly:
            h = engine.activate(tab); h.wait_for_load(); h.click_at_xy(x, y)
        """
        from ..bh import _ipc as bh_ipc
        from ..bh import helpers as bh  # verbatim browser-harness helpers
        bh_ipc.set_tab(tab)
        return bh

    def close(self) -> None:
        with self._lock:
            if self._client is not None:
                try:
                    self._client.turn_ended()
                finally:
                    self._client.close()
                    self._client = None

    def _c(self):
        if self._client is None:
            self.connect()
        return self._client

    # ---- tabs ----
    def new_tab(self, url: str | None = None) -> TabHandle:
        with self._lock:
            c = self._c()
            created = c.create_tab()
            tab_id = created["id"] if isinstance(created, dict) else created
            c.attach(tab_id)
            handle = TabHandle(id=tab_id, backend=self.name)
            if url:
                self.navigate(handle, url)
            return handle

    def claim_tab(self, tab_id: int) -> TabHandle:
        """Adopt an existing tab from your real session (e.g. an already
        logged-in twitter/x tab) instead of opening a fresh one."""
        with self._lock:
            c = self._c()
            c.claim_user_tab(tab_id)
            c.attach(tab_id)
            return TabHandle(id=tab_id, backend=self.name)

    def user_tabs(self) -> list[dict]:
        with self._lock:
            return self._c().get_user_tabs()

    def session_tabs(self) -> list[dict]:
        with self._lock:
            return self._c().get_tabs()

    def close_tab(self, tab: TabHandle) -> None:
        try:
            self.cdp(tab, "Page.close")
        except Exception:
            pass

    # ---- the one seam ----
    def cdp(self, tab: TabHandle, method: str, params: dict | None = None) -> dict:
        with self._lock:
            return self._c().execute_cdp(tab.id, method, params or {})

    def finalize(self, keep: list[dict] | None = None) -> None:
        with self._lock:
            self._c().finalize_tabs(keep or [])
