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


def _make_client(socket_path: str, session_id: str, timeout: float):
    from open_browser_use import client as obu_client  # type: ignore

    class TolerantOpenBrowserUseClient(obu_client.OpenBrowserUseClient):
        def request(self, method: str, params: obu_client.JsonObject | None = None):
            self.connect()
            if self._socket is None:
                raise RuntimeError("Open Browser Use socket is not connected")
            request_id = self._next_id
            self._next_id += 1
            merged_params: obu_client.JsonObject = {
                "session_id": self.session_id,
                "turn_id": self.turn_id,
            }
            if params:
                merged_params.update(params)
            request = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": merged_params,
            }
            self._socket.sendall(obu_client.encode_frame(request))
            while True:
                response = obu_client.read_frame(self._socket)
                if response.get("id") == request_id:
                    if "error" in response:
                        message = response["error"].get(
                            "message", "Open Browser Use request failed"
                        )
                        raise RuntimeError(message)
                    return response.get("result")
                if "id" not in response and isinstance(response.get("method"), str):
                    self._dispatch_notification(response)
                    continue
                # OBU can emit stale or bridge-owned response ids such as
                # "OBU:2873"; keep reading until this request's response arrives.
                continue

    return TolerantOpenBrowserUseClient(
        socket_path=socket_path,
        session_id=session_id,
        timeout=timeout,
    )


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
    _GLOBAL_LOCK = threading.RLock()

    def __init__(self, session_id: str | None = None, socket_path: str | None = None,
                 timeout: float = 30.0):
        self.session_id = session_id or f"deepest-{uuid.uuid4().hex[:8]}"
        self._socket_path = socket_path
        self._timeout = timeout
        self._client = None
        self._lock = self._GLOBAL_LOCK

    # ---- lifecycle ----
    def connect(self) -> "RealChromeEngine":
        from ..bh import _ipc as bh_ipc  # bind BH's verbatim helpers to this engine
        with self._lock:
            sock = self._socket_path or discover_socket()
            self._client = _make_client(sock, self.session_id, self._timeout).connect()
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

    @staticmethod
    def _is_transport_desync(exc: Exception) -> bool:
        return "unexpected response id" in f"{type(exc).__name__}: {exc}"

    def _reset_client_locked(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None

    # Native JS dialogs (alert/confirm/prompt) open an OS-level modal that freezes the
    # page's JS thread and carries text that never reaches the DOM, so the observer is
    # blind to it. Wrap them in-page to record the message and auto-answer (alert ->
    # undefined, confirm -> true, prompt -> default) instead of blocking.
    _DIALOG_CAPTURE_JS = (
        "(()=>{if(window.__deepestDialogHook)return;window.__deepestDialogHook=true;"
        "window.__deepestDialogs=[];"
        "var rec=function(t,m){try{window.__deepestDialogs.push({type:t,"
        "message:String(m==null?'':m)});"
        "if(window.__deepestDialogs.length>20)window.__deepestDialogs.shift();}catch(e){}};"
        "window.alert=function(m){rec('alert',m);};"
        "window.confirm=function(m){rec('confirm',m);return true;};"
        "window.prompt=function(m,d){rec('prompt',m);return d==null?'':d;};})()"
    )

    def _install_dialog_capture(self, tab: TabHandle) -> None:
        """Register the dialog wrapper before navigation (so it applies to the document
        being loaded and any child frames) and also inject it into whatever is already
        loaded, for adopted tabs. Page.enable first: addScriptToEvaluateOnNewDocument is
        a Page-domain command and is rejected until the domain is enabled (idempotent).
        Best-effort: never let this break tab setup."""
        for method, params in (
            ("Page.enable", {}),
            ("Page.addScriptToEvaluateOnNewDocument", {"source": self._DIALOG_CAPTURE_JS}),
            ("Runtime.evaluate", {"expression": self._DIALOG_CAPTURE_JS}),
        ):
            try:
                self.cdp(tab, method, params)
            except Exception:
                pass

    # ---- tabs ----
    def new_tab(self, url: str | None = None) -> TabHandle:
        with self._lock:
            c = self._c()
            created = c.create_tab()
            tab_id = created["id"] if isinstance(created, dict) else created
            c.attach(tab_id)
            handle = TabHandle(id=tab_id, backend=self.name)
            self._install_dialog_capture(handle)
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
            handle = TabHandle(id=tab_id, backend=self.name)
            self._install_dialog_capture(handle)
            return handle

    def current_url(self, tab: TabHandle) -> str:
        """Resolve the tab URL from the browser-level target list (CDP Target domain),
        which stays responsive even when the page's main thread is blocked, instead of
        an in-page location.href eval that a stuck page can stall on for the full CDP
        budget. Fall back to the eval only if the target list lacks this tab, and turn a
        CDP timeout there into '' rather than letting a wedged page raise."""
        tid = getattr(tab, "id", tab)
        try:
            for getter in (self.session_tabs, self.user_tabs):
                for t in getter() or []:
                    if isinstance(t, dict) and str(t.get("id")) == str(tid) and t.get("url"):
                        return t["url"]
        except Exception:
            pass
        try:
            return self.evaluate(tab, "location.href") or ""
        except Exception as exc:
            if "Timed out" in f"{exc}" and "waiting for CDP command" in f"{exc}":
                return ""
            raise

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
            try:
                return self._c().execute_cdp(tab.id, method, params or {})
            except RuntimeError as exc:
                if not self._is_transport_desync(exc):
                    raise
                self._reset_client_locked()
                return self._c().execute_cdp(tab.id, method, params or {})

    def finalize(self, keep: list[dict] | None = None) -> None:
        with self._lock:
            self._c().finalize_tabs(keep or [])
