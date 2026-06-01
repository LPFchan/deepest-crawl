"""Drop-in transport shim so BH's VERBATIM helpers.py runs on OBU's transport.

browser-harness's helpers.py does `from . import _ipc as ipc` and speaks its
daemon's wire-protocol (`ipc.connect` / `ipc.request`). We keep helpers.py
byte-for-byte BH (max copy/paste fidelity) and reimplement only this transport
module — translating BH's daemon protocol into open-browser-use `executeCdp`
calls against your real Chrome. helpers.py never knows the difference.

Bind once with `bind(engine)`; point it at the active tab with `set_tab(tab)`.
The engine wrapper in engines/chrome.py does this for you.

BH daemon protocol implemented (the subset helpers.py uses):
  {"method","params","session_id"}        -> {"result": <cdp result>}
  {"meta":"drain_events"}                  -> {"events":[...]}     (degraded: see note)
  {"meta":"pending_dialog"}                -> {"dialog": None}     (no JS-dialog intercept yet)
  {"meta":"current_tab"}                   -> {"targetId","url","title"}
  {"meta":"set_session", ...}              -> {}
  {"meta":"session"}                       -> {"session_id": <token>}
"""
from __future__ import annotations

from pathlib import Path

# screenshots / debug-click overlays land here (helpers.py reads ipc._TMP)
_TMP = Path(__file__).resolve().parents[2] / ".tmp"
_TMP.mkdir(parents=True, exist_ok=True)

# module-global binding: the one real-Chrome engine + the active tab handle
_ENGINE = None
_TAB = None
_SESSION_TOKEN = "deepest-bh"


def bind(engine) -> None:
    global _ENGINE
    _ENGINE = engine


def set_tab(tab) -> None:
    global _TAB
    _TAB = tab


def current_tab():
    return _TAB


# ---- BH daemon-compatible surface used by helpers.py ----

def sock_addr(name: str) -> str:  # helpers does SOCK = ipc.sock_addr(NAME) at import
    return f"<obu-shim:{name}>"


class _Conn:
    """Stand-in for BH's daemon socket; carries no state of its own."""
    def close(self) -> None:
        pass


def connect(name: str, timeout: float = 5.0):
    if _ENGINE is None:
        raise RuntimeError(
            "deepest.bh transport not bound — call deepest.bh._ipc.bind(engine) "
            "(engines/chrome.py does this on connect)."
        )
    return _Conn(), _SESSION_TOKEN


def request(conn, token, req: dict) -> dict:
    if "meta" in req:
        return _handle_meta(req)
    # CDP path
    method = req.get("method")
    params = req.get("params") or {}
    session_id = req.get("session_id")
    if session_id:
        # helpers.js(target_id=...) attaches an iframe session via flatten and
        # passes session_id here. OBU's executeCdp is tab-scoped and manages its
        # own sessions, so cross-frame session routing isn't supported in v1.
        # Best-effort: run against the active tab. (Rare for our crawl.)
        pass
    if _TAB is None:
        raise RuntimeError("no active tab — call set_tab(tab) before BH helpers")
    result = _ENGINE.cdp(_TAB, method, params)
    return {"result": result}


def _handle_meta(req: dict) -> dict:
    meta = req["meta"]
    if meta == "drain_events":
        # OBU broadcasts CDP events as JSON-RPC notifications, but we don't buffer
        # them here yet. Returning [] makes wait_for_network_idle fall through to
        # its idle window quickly rather than block. TODO: wire OBU notifications.
        return {"events": []}
    if meta == "pending_dialog":
        return {"dialog": None}
    if meta == "session":
        return {"session_id": _SESSION_TOKEN}
    if meta == "set_session":
        return {}
    if meta == "current_tab":
        if _TAB is None:
            raise RuntimeError("no active tab for current_tab")
        try:
            info = _ENGINE.cdp(_TAB, "Runtime.evaluate", {
                "expression": "JSON.stringify({url:location.href,title:document.title})",
                "returnByValue": True,
            })
            import json as _json
            v = _json.loads(info.get("result", {}).get("value") or "{}")
        except Exception:
            v = {}
        return {"targetId": _TAB.id, "url": v.get("url", ""), "title": v.get("title", "")}
    return {}
