"""FastAPI dashboard server.

Endpoints:
  GET  /                  Dashboard HTML
  GET  /events            SSE stream of state updates
  GET  /screenshot        Current screenshot PNG
  GET  /state             JSON snapshot of current state
  POST /crawl             Submit a URL to crawl (body: {"url": "..."})
  POST /prompt            Send a free-form prompt to the brain (body: {"text": "..."})
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import importlib
import json
import os
import random
import re
import threading
import time
import traceback
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from .. import services
from ..engines.base import TabHandle
from .state import STATE, CrawlStep

try:
    from PIL import Image
except Exception:  # pragma: no cover - Pillow is a runtime dependency
    Image = None

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - optional dependency until env is synced
    BeautifulSoup = None

try:
    import trafilatura
except Exception:  # pragma: no cover - optional dependency until env is synced
    trafilatura = None

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
LINKS_PATH = ROOT / "inputs" / "links.json"
RESULTS_PATH = ROOT / "outputs" / "summaries.json"
DOMAIN_KNOWLEDGE_DIR = ROOT / "outputs" / "domain-knowledge"
_RUN_LOCK = threading.Lock()
_FILE_LOCK = threading.Lock()
_SERVICE_LOCK = threading.Lock()
_SCREENSHOT_LOCK = threading.Lock()
_ACTIVE_JOB_LOCK = threading.Lock()
_CANCEL_EVENT = threading.Event()
_SERVICE_STATE = {"status": "idle", "note": "", "error": ""}
_LINKS_CACHE = {"mtime": None, "data": []}
_RESULTS_CACHE = {"mtime": None, "data": {}}
_DOMAIN_NOTE_COUNT_CACHE: dict[str, tuple[float | None, int]] = {}
_SCREENSHOT_TAB = None
_LAST_SCREENSHOT_BYTES: bytes | None = None
_ACTIVE_JOB = {"engine": None, "tab": None}
app = FastAPI(title="deepest-crawl dashboard")

# Lazy imports — the dashboard can serve static content without the brain/engine loaded
_brain_ready = False
_engine_ready = None
_engine_timeout = None


def _env_float(name: str, default: float) -> float:
    try:
        return max(0.0, float(os.environ.get(name, str(default))))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


def _agent_brain_max_chars() -> int:
    return _env_int("DEEPEST_AGENT_BRAIN_MAX_CHARS", 18000)


def _agent_brain_max_tokens() -> int:
    return _env_int("DEEPEST_AGENT_BRAIN_MAX_TOKENS", 768)


def _agent_vision_max_tokens() -> int:
    return _env_int("DEEPEST_AGENT_VISION_MAX_TOKENS", 768)


def _crawl_timeout_seconds(value: float | None = None) -> float:
    return value if value and value > 0 else _env_float("DEEPEST_CRAWL_TIMEOUT_SECONDS", 300.0)


def _chrome_command_timeout_seconds(value: float | None = None) -> float:
    crawl_timeout = _crawl_timeout_seconds(value)
    chrome_timeout = _env_float("DEEPEST_CHROME_COMMAND_TIMEOUT_SECONDS", 20.0)
    return max(1.0, min(crawl_timeout, chrome_timeout))


def _job_deadline(timeout_seconds: float | None = None) -> float:
    return time.monotonic() + _crawl_timeout_seconds(timeout_seconds)


def _remaining_seconds(deadline: float, default: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Job timed out.")
    return max(0.1, min(default, remaining))


def _check_job_open(deadline: float) -> None:
    if _CANCEL_EVENT.is_set():
        raise InterruptedError("Job canceled.")
    _remaining_seconds(deadline, 0.1)


def _job_sleep(seconds: float, deadline: float) -> None:
    wait = min(seconds, _remaining_seconds(deadline, seconds))
    if not _cancelable_sleep(wait):
        raise InterruptedError("Job canceled.")
    if wait < seconds:
        raise TimeoutError("Job timed out.")


def _bulk_delay_seconds(value: float | None = None) -> float:
    return value if value is not None and value >= 0 else _env_float("DEEPEST_BULK_DELAY_SECONDS", 20.0)


def _bulk_jitter_seconds(value: float | None = None) -> float:
    return value if value is not None and value >= 0 else _env_float("DEEPEST_BULK_JITTER_SECONDS", 10.0)


def _screenshot_timeout_seconds() -> float:
    return _env_float("DEEPEST_SCREENSHOT_TIMEOUT_SECONDS", 1.5)


def _env_enabled(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() not in {"0", "false", "no", "off"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _host_of(url: str) -> str:
    return (urlparse(url).hostname or "").removeprefix("www.").lower()


def _domain_memory_target_from_state() -> tuple[str, str]:
    candidates: list[str] = []
    if STATE.current.url:
        candidates.append(STATE.current.url)
    for event in reversed(STATE.current.trace[-40:]):
        url = event.get("url") or event.get("archive_url")
        if isinstance(url, str) and url:
            candidates.append(url)
    for url in candidates:
        source_url = _wayback_original_url(url)
        host = _host_of(source_url)
        if host and host != "web.archive.org":
            return host, source_url
    return "", ""


def _safe_link_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:12]


def _extract_url(text: str) -> str:
    match = re.search(r"https?://[^\s<>'\")]+", text or "")
    if not match:
        return ""
    return match.group(0).rstrip(".,;:")


def _looks_blocked_or_error(text: str) -> bool:
    sample = (text or "").lower()[:4000]
    if not sample:
        return True
    markers = [
        "access denied",
        "are you a human",
        "captcha",
        "cloudflare",
        "enable javascript",
        "forbidden",
        "login required",
        "not found",
        "page unavailable",
        "please verify",
        "sign in",
        "temporarily unavailable",
        "too many requests",
        "unusual traffic",
    ]
    return any(marker in sample for marker in markers)


def _is_wayback_url(url: str) -> bool:
    return _host_of(url) in {"archive.org", "web.archive.org"}


def _wayback_original_url(url: str) -> str:
    if not _is_wayback_url(url):
        return url
    match = re.search(r"/web/[^/]+/(https?://.+)$", url or "", re.I)
    return match.group(1) if match else ""


def _normal_url_key(url: str) -> str:
    return (url or "").strip().rstrip("/").lower()


def _wayback_snapshot_candidates(
    url: str,
    deadline: float | None = None,
    *,
    exclude_urls: set[str] | None = None,
    limit: int = 12,
) -> list[str]:
    if not _env_enabled("DEEPEST_WAYBACK_ON_CONTENT_DOWN", True):
        return []
    source_url = _wayback_original_url(url)
    if not source_url.startswith(("http://", "https://")) or _is_wayback_url(source_url):
        return []
    timeout = _env_float("DEEPEST_WAYBACK_CDX_TIMEOUT_SECONDS", 15.0)
    if deadline is not None:
        try:
            timeout = _remaining_seconds(deadline, timeout)
        except TimeoutError:
            return []
    excluded = {_normal_url_key(item) for item in (exclude_urls or set())}
    api = (
        "https://web.archive.org/cdx/search/cdx"
        f"?url={quote(source_url, safe='')}"
        "&output=json&fl=timestamp,original,statuscode,mimetype,digest"
        "&filter=statuscode:200"
        "&collapse=digest&sort=reverse"
        f"&limit={max(1, limit)}"
    )
    try:
        req = urllib.request.Request(
            api,
            headers={"User-Agent": "deepest-crawl/0.1 (+wayback fallback)"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as res:
            rows = json.loads(res.read().decode("utf-8", "replace"))
        candidates: list[str] = []
        for row in rows[1:] if isinstance(rows, list) else []:
            if not isinstance(row, list) or len(row) < 2:
                continue
            mimetype = str(row[3]).lower() if len(row) > 3 else ""
            if mimetype and "html" not in mimetype and "text/plain" not in mimetype:
                continue
            ts, original = str(row[0]), str(row[1])
            archive_url = f"https://web.archive.org/web/{ts}/{original}"
            if _normal_url_key(archive_url) in excluded:
                continue
            candidates.append(archive_url)
        return candidates
    except Exception as exc:
        STATE.push_trace({
            "ts": _now(),
            "message": "wayback cdx lookup failed",
            "url": source_url,
            "error": f"{type(exc).__name__}: {exc}",
        })
        return []


_CLOUDFLARE_ORIGIN_DOWN_STATUSES = {520, 521, 522, 523, 524, 525, 526, 527, 530}

_CLOUDFLARE_ORIGIN_DOWN_MARKERS = (
    "cloudflare host error",
    "error 520",
    "error 521",
    "error 522",
    "error 523",
    "error 524",
    "error 525",
    "error 526",
    "error 527",
    "error 530",
    "web server is returning an unknown error",
    "web server is down",
    "web server is not returning a connection",
    "origin is unreachable",
    "connection timed out",
    "ssl handshake failed",
    "invalid ssl certificate",
    "the web server reported a bad gateway error",
    "contact your hosting provider",
)


def _cloudflare_origin_down_reason(text: str, status: int | None = None) -> str:
    if status in _CLOUDFLARE_ORIGIN_DOWN_STATUSES:
        return f"http-{status}"
    sample = " ".join((text or "").lower().split())[:4000]
    if not sample or "cloudflare" not in sample:
        return ""
    for marker in _CLOUDFLARE_ORIGIN_DOWN_MARKERS:
        if marker in sample:
            return marker
    return ""


def _content_down_reason(text: str, status: int | None = None) -> str:
    cloudflare_reason = _cloudflare_origin_down_reason(text, status)
    if cloudflare_reason:
        return cloudflare_reason
    if status in {404, 410, 451, 500, 502, 503, 504}:
        return f"http-{status}"
    sample = " ".join((text or "").lower().split())[:4000]
    if not sample:
        return ""
    markers = (
        "404 not found",
        "404 error",
        "page not found",
        "not found",
        "does not exist",
        "has been removed",
        "no longer available",
        "page unavailable",
        "content unavailable",
        "temporarily unavailable",
        "sorry! something went wrong",
        "the requested url was not found",
        "we can't find the page",
        "we couldn't find the page",
        "this page isn't available",
        "this page is no longer available",
    )
    for marker in markers:
        if marker in sample:
            return marker
    return ""


def _content_down_failure_message(url: str, reason: str) -> str:
    return f"Content unavailable ({reason}) at {url}."


def _transient_verification_reason(text: str, status: int | None = None) -> str:
    if _cloudflare_origin_down_reason(text, status):
        return ""
    sample = " ".join((text or "").lower().split())[:4000]
    if not sample:
        return ""
    markers = (
        "checking your browser",
        "checking if the site connection is secure",
        "verify you are human",
        "verify that you are human",
        "just a moment",
        "please wait while we check your browser",
        "turnstile",
        "ddos-guard",
        "attention required",
        "security check",
        "security verification",
        "performing security verification",
        "not a bot",
        "security service to protect",
        "enable javascript and cookies",
    )
    for marker in markers:
        if marker in sample:
            return f"http-{status}:{marker}" if status else marker
    if "cloudflare" in sample and any(marker in sample for marker in (
        "checking", "verify", "human", "security", "ray id", "challenge",
        "enable javascript", "please wait",
    )):
        return f"http-{status}:cloudflare challenge" if status else "cloudflare challenge"
    return ""


def _page_response_status(engine, tab) -> int | None:
    try:
        status = engine.evaluate(tab, """
            (() => {
              const nav = performance.getEntriesByType('navigation')[0];
              if (nav && Number.isFinite(nav.responseStatus)) return nav.responseStatus;
              return 0;
            })()
        """)
        status = int(status or 0)
        return status or None
    except Exception:
        return None


def _wayback_snapshot_url(url: str, deadline: float | None = None,
                          exclude_urls: set[str] | None = None) -> str:
    if not _env_enabled("DEEPEST_WAYBACK_ON_CONTENT_DOWN", True):
        return ""
    source_url = _wayback_original_url(url)
    if not source_url.startswith(("http://", "https://")) or _is_wayback_url(source_url):
        return ""
    excluded = {_normal_url_key(item) for item in (exclude_urls or set())}
    candidates = _wayback_snapshot_candidates(
        source_url,
        deadline,
        exclude_urls=exclude_urls,
    )
    if candidates:
        return candidates[0]
    timeout = 5.0
    if deadline is not None:
        try:
            timeout = _remaining_seconds(deadline, timeout)
        except TimeoutError:
            return ""
    api = "https://archive.org/wayback/available?url=" + quote(source_url, safe="")
    try:
        req = urllib.request.Request(
            api,
            headers={"User-Agent": "deepest-crawl/0.1 (+wayback fallback)"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as res:
            data = json.loads(res.read().decode("utf-8", "replace"))
        closest = (data.get("archived_snapshots") or {}).get("closest") or {}
        closest_url = str(closest.get("url") or "")
        if closest.get("available") and closest_url and _normal_url_key(closest_url) not in excluded:
            return closest_url
    except Exception as exc:
        STATE.push_trace({
            "ts": _now(),
            "message": "wayback availability lookup failed",
            "url": source_url,
            "error": f"{type(exc).__name__}: {exc}",
        })
    fallback = "https://web.archive.org/web/2/" + source_url
    if _normal_url_key(fallback) in excluded:
        return ""
    STATE.push_trace({
        "ts": _now(),
        "message": "using direct wayback replay fallback",
        "url": source_url,
        "archive_url": fallback,
    })
    return fallback


def _load_json(path: Path, fallback):
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text())
    except Exception:
        return fallback


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def _json_safe(value):
    if isinstance(value, str):
        return "".join(ch if ord(ch) >= 32 else " " for ch in value)
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    return value


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return None


def _invalidate_link_caches() -> None:
    _LINKS_CACHE.update({"mtime": None, "data": []})
    _RESULTS_CACHE.update({"mtime": None, "data": {}})
    _DOMAIN_NOTE_COUNT_CACHE.clear()


def _load_links() -> list[dict]:
    mtime = _mtime(LINKS_PATH)
    if _LINKS_CACHE["mtime"] == mtime:
        return list(_LINKS_CACHE["data"])
    links = _load_json(LINKS_PATH, [])
    if not isinstance(links, list):
        links = []
    _LINKS_CACHE.update({"mtime": mtime, "data": links})
    return list(links)


def _load_results_by_id() -> dict[str, dict]:
    mtime = _mtime(RESULTS_PATH)
    if _RESULTS_CACHE["mtime"] == mtime:
        return dict(_RESULTS_CACHE["data"])
    rows = _load_json(RESULTS_PATH, [])
    if not isinstance(rows, list):
        return {}
    by_id = {str(r.get("id")): r for r in rows if isinstance(r, dict) and r.get("id")}
    _RESULTS_CACHE.update({"mtime": mtime, "data": by_id})
    return dict(by_id)


def _persist_result(rec: dict) -> None:
    with _FILE_LOCK:
        rows = _load_json(RESULTS_PATH, [])
        if not isinstance(rows, list):
            rows = []
        by_id = {str(r.get("id")): i for i, r in enumerate(rows)
                 if isinstance(r, dict) and r.get("id")}
        rec = dict(rec)
        rec["updated_at"] = _now()
        if rec["id"] in by_id:
            rows[by_id[rec["id"]]] = rec
        else:
            rows.append(rec)
        _write_json(RESULTS_PATH, rows)
        _RESULTS_CACHE.update({"mtime": None, "data": {}})


def _knowledge_path(host: str) -> Path:
    clean = "".join(ch if ch.isalnum() or ch in ".-" else "_" for ch in host)
    return DOMAIN_KNOWLEDGE_DIR / f"{clean or 'unknown'}.json"


def _load_domain_knowledge(host: str) -> dict:
    path = _knowledge_path(host)
    data = _load_json(path, {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("host", host)
    data.setdefault("notes", [])
    data.setdefault("playbooks", [])
    data.setdefault("traces", [])
    return data


def _domain_note_count(host: str) -> int:
    path = _knowledge_path(host)
    mtime = _mtime(path)
    cached = _DOMAIN_NOTE_COUNT_CACHE.get(host)
    if cached and cached[0] == mtime:
        return cached[1]
    data = _load_domain_knowledge(host)
    count = len(data.get("notes", []))
    _DOMAIN_NOTE_COUNT_CACHE[host] = (mtime, count)
    return count


def _append_domain_note(host: str, source: str, text: str, url: str = "",
                        trace: list[dict] | None = None) -> dict:
    if not host:
        host = _host_of(url)
    data = _load_domain_knowledge(host)
    normalized = text.strip().lower()
    for note in data.get("notes", [])[-30:]:
        if note.get("text", "").strip().lower() == normalized:
            return data
    data["updated_at"] = _now()
    data["notes"].append({
        "ts": _now(),
        "source": source,
        "url": url,
        "text": text,
    })
    if trace:
        data["traces"].append({
            "ts": _now(),
            "url": url,
            "events": trace[-80:],
        })
        data["traces"] = data["traces"][-20:]
    with _FILE_LOCK:
        _write_json(_knowledge_path(host), data)
    _DOMAIN_NOTE_COUNT_CACHE.pop(host, None)
    return data


def _append_domain_playbook(host: str, source: str, title: str, steps,
                            url: str = "") -> dict:
    if not host:
        host = _host_of(url)
    data = _load_domain_knowledge(host)
    if isinstance(steps, str):
        parsed_steps = [
            line.strip(" -\t")
            for line in steps.splitlines()
            if line.strip(" -\t")
        ]
    else:
        parsed_steps = [str(step).strip() for step in steps if str(step).strip()]
    if not parsed_steps:
        return data
    title = title.strip() or parsed_steps[0][:80]
    normalized = "\n".join(step.lower() for step in parsed_steps)
    for playbook in data.get("playbooks", [])[-30:]:
        existing = "\n".join(str(step).lower() for step in playbook.get("steps", []))
        if existing == normalized:
            return data
    data["updated_at"] = _now()
    data["playbooks"].append({
        "id": hashlib.sha1(f"{host}\n{title}\n{normalized}".encode()).hexdigest()[:12],
        "ts": _now(),
        "source": source,
        "url": url,
        "title": title,
        "steps": parsed_steps[:12],
    })
    data["playbooks"] = data["playbooks"][-40:]
    with _FILE_LOCK:
        _write_json(_knowledge_path(host), data)
    _DOMAIN_NOTE_COUNT_CACHE.pop(host, None)
    return data


def _domain_instruction_text(knowledge: dict) -> str:
    lines: list[str] = []
    playbooks = knowledge.get("playbooks", [])[-5:]
    if playbooks:
        lines.append("Reusable domain playbooks:")
        for playbook in playbooks:
            title = playbook.get("title") or "playbook"
            lines.append(f"- {title}")
            for idx, step in enumerate(playbook.get("steps", [])[:8], 1):
                lines.append(f"  {idx}. {step}")
    notes = [
        n.get("text", "")
        for n in knowledge.get("notes", [])[-8:]
        if n.get("text")
    ]
    if notes:
        lines.append("Domain notes:")
        lines.extend(f"- {note}" for note in notes)
    return "\n".join(lines)


def _operator_memory_command(text: str) -> str:
    normalized = (text or "").strip()
    match = re.match(
        r"(?is)^\s*add\s+to\s+domain\s+(?:memory|playbook)\s*:?\s*(.+)$",
        normalized,
    )
    if not match:
        return ""
    return match.group(1).strip()


def _save_operator_memory_command(text: str) -> dict:
    memory_text = _operator_memory_command(text)
    if not memory_text:
        return {}
    explicit_url = _extract_url(memory_text) or _extract_url(text)
    if explicit_url:
        host = _host_of(_wayback_original_url(explicit_url))
        url = explicit_url
    else:
        host, url = _domain_memory_target_from_state()
    if not host:
        raise ValueError("No active domain to attach memory to.")
    data = _append_domain_note(host, "operator", memory_text, url)
    data = _append_domain_playbook(host, "operator", "Operator workaround", memory_text, url)
    if STATE.current.host in {host, "web.archive.org", ""}:
        STATE.update(
            domain_knowledge=data.get("notes", []),
            domain_playbooks=data.get("playbooks", []),
        )
    STATE.push_trace({
        "ts": _now(),
        "message": "operator domain memory saved",
        "host": host,
        "text": memory_text,
    })
    return {
        "status": "saved",
        "host": host,
        "url": url,
        "notes": data.get("notes", []),
        "playbooks": data.get("playbooks", []),
    }


def _trace(message: str, **fields) -> None:
    event = {"ts": _now(), "message": message, **fields}
    STATE.push_trace(event)
    STATE.update(note=message)


def _publish_screenshot(engine, tab, label: str = "screenshot") -> None:
    global _LAST_SCREENSHOT_BYTES
    try:
        png = engine.screenshot_png(tab)
        _LAST_SCREENSHOT_BYTES = png
        STATE.update(png_bytes=png)
        STATE.push_trace({
            "ts": _now(),
            "message": label,
        })
    except Exception as e:
        STATE.push_trace({
            "ts": _now(),
            "message": f"{label} failed",
            "error": f"{type(e).__name__}: {e}",
        })


def _try_screenshot(engine, tab, attempts: int = 3, delay: float = 0.6) -> bytes | None:
    for attempt in range(attempts):
        try:
            return engine.screenshot_png(tab)
        except Exception:
            if attempt + 1 >= attempts:
                return None
            time.sleep(delay)
    return None


def _set_active_tab(engine=None, tab=None) -> None:
    with _ACTIVE_JOB_LOCK:
        _ACTIVE_JOB["engine"] = engine
        _ACTIVE_JOB["tab"] = tab


def _close_active_tab() -> None:
    with _ACTIVE_JOB_LOCK:
        engine = _ACTIVE_JOB.get("engine")
        tab = _ACTIVE_JOB.get("tab")
    if engine is not None and tab is not None:
        _close_job_tab(engine, tab, reason="active job cleanup")


def _close_job_tab(engine, tab, *, reason: str = "job cleanup") -> None:
    if engine is None or tab is None:
        return
    benign_errors = (
        "Debugger unattached",
        "No tab with id",
        "not part of browser session",
        "No target with given id",
        "Target closed",
    )
    try:
        engine.cdp(tab, "Page.close", {})
        STATE.push_trace({
            "ts": _now(),
            "message": "closed browser tab",
            "reason": reason,
            "tab": getattr(tab, "id", ""),
        })
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        message = "browser tab close failed"
        if any(item in error for item in benign_errors):
            message = "browser tab already closed or detached"
        STATE.push_trace({
            "ts": _now(),
            "message": message,
            "reason": reason,
            "tab": getattr(tab, "id", ""),
            "error": error,
        })


def _live_chrome_screenshot() -> bytes | None:
    global _SCREENSHOT_TAB, _LAST_SCREENSHOT_BYTES
    if not _SCREENSHOT_LOCK.acquire(blocking=False):
        return None
    try:
        engine = _ensure_engine()
        tab = _SCREENSHOT_TAB
        if tab is not None:
            png = _try_screenshot(engine, tab, attempts=1)
            if png:
                _LAST_SCREENSHOT_BYTES = png
                return png
            _SCREENSHOT_TAB = None

        session_tabs = engine.session_tabs()
        if isinstance(session_tabs, list) and session_tabs:
            tab_id = session_tabs[0].get("id")
            if tab_id is not None:
                tab = TabHandle(id=tab_id, backend="chrome")
                png = _try_screenshot(engine, tab, attempts=1)
                if png:
                    _SCREENSHOT_TAB = tab
                    _LAST_SCREENSHOT_BYTES = png
                    return png

        tab = None
        try:
            tabs = engine.user_tabs()
            user_tab = None
            if isinstance(tabs, list) and tabs:
                user_tab = next(
                    (
                        t for t in tabs
                        if ":8766" not in str(t.get("url", ""))
                    ),
                    tabs[0],
                )
            if user_tab and user_tab.get("id") is not None:
                tab = engine.claim_tab(int(user_tab["id"]))
                png = _try_screenshot(engine, tab, attempts=1)
                if png:
                    _SCREENSHOT_TAB = tab
                    _LAST_SCREENSHOT_BYTES = png
                    return png
        except Exception:
            tab = None

        tab = tab or engine.new_tab("about:blank")
        png = _try_screenshot(engine, tab, attempts=1)
        if png:
            _SCREENSHOT_TAB = tab
            _LAST_SCREENSHOT_BYTES = png
            return png
        return None
    except Exception:
        _SCREENSHOT_TAB = None
        return None
    finally:
        _SCREENSHOT_LOCK.release()


def _error_detail(exc: Exception) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


DOMAIN_LEARNING_SYSTEM = (
    "You maintain per-domain operating memory for a browser crawler. "
    "Given one crawl or agent trace, decide whether there is a reusable, "
    "domain-specific workaround worth remembering. Output exactly one concise "
    "imperative note, or output NO_UPDATE. Remember concrete navigation, URL, "
    "search, login-wall, selector, retry, and error-recovery tactics. Do not "
    "save generic transient facts such as one-off network failures, current "
    "timestamps, or page summaries."
)


def _auto_learn_domain(brain_mod, host: str, url: str, outcome: str,
                       trace: list[dict], summary: str = "",
                       error: str = "") -> None:
    if not host or brain_mod is None:
        return
    try:
        trace_text = json.dumps(trace[-40:], ensure_ascii=False, indent=2)
        prompt = (
            f"Host: {host}\nURL: {url}\nOutcome: {outcome}\n"
            f"Error: {error or '-'}\nSummary: {summary[:1200] or '-'}\n\n"
            f"Recent trace:\n{trace_text}\n\n"
            "Reusable domain workaround, or NO_UPDATE:"
        )
        learned = brain_mod.complete(
            DOMAIN_LEARNING_SYSTEM,
            prompt,
            max_tokens=180,
            temperature=0.1,
            timeout=120.0,
        ).strip()
    except Exception as e:
        STATE.push_trace({
            "ts": _now(),
            "message": "domain learning skipped",
            "error": f"{type(e).__name__}: {e}",
        })
        return

    cleaned = learned.strip().strip('"').strip()
    if not cleaned or cleaned.upper().startswith("NO_UPDATE"):
        return
    if len(cleaned) > 800:
        cleaned = cleaned[:800].rstrip()
    data = _append_domain_note(host, "auto-learn", cleaned, url, trace)
    data = _append_domain_playbook(host, "auto-learn", "Auto-learned workaround", cleaned, url)
    if STATE.current.host == host:
        STATE.update(
            domain_knowledge=data.get("notes", []),
            domain_playbooks=data.get("playbooks", []),
        )


def _ensure_brain(timeout_seconds: float | None = None):
    global _brain_ready
    if _brain_ready:
        from .. import brain
        return brain
    brain = services.ensure_brain(
        status=lambda note: STATE.update(status="starting_brain", note=note),
        wait_seconds=_crawl_timeout_seconds(timeout_seconds) if timeout_seconds else None,
    )
    _brain_ready = True
    STATE.update(status="brain_ready", note="")
    return brain


def _call_brain_with_retry(operation, deadline: float, timeout_seconds: float, label: str):
    global _brain_ready
    try:
        return operation(_remaining_seconds(deadline, timeout_seconds))
    except Exception as exc:
        _brain_ready = False
        STATE.push_trace({
            "ts": _now(),
            "message": f"{label} failed; restarting brain once",
            "error": f"{type(exc).__name__}: {exc}",
        })
        _ensure_brain(_remaining_seconds(deadline, timeout_seconds))
        return operation(_remaining_seconds(deadline, timeout_seconds))


def _ensure_engine(timeout_seconds: float | None = None):
    global _engine_ready, _engine_timeout
    timeout = _chrome_command_timeout_seconds(timeout_seconds)
    if _engine_ready is not None and _engine_timeout != timeout:
        try:
            _engine_ready.close()
        except Exception:
            pass
        _engine_ready = None
    if _engine_ready is None:
        services.ensure_chrome_transport(
            status=lambda note: STATE.update(status="starting_chrome", note=note)
        )
        from ..engines.chrome import RealChromeEngine
        _engine_ready = RealChromeEngine(timeout=timeout).connect()
        _engine_timeout = timeout
    return _engine_ready


def _reconnect_engine(timeout_seconds: float | None = None):
    global _engine_ready, _engine_timeout, _SCREENSHOT_TAB
    try:
        if _engine_ready is not None:
            _engine_ready.close()
    except Exception:
        pass
    _engine_ready = None
    _engine_timeout = None
    _SCREENSHOT_TAB = None
    return _ensure_engine(timeout_seconds)


def _new_tab_with_retry(engine, url: str | None, timeout_seconds: float, deadline: float):
    _check_job_open(deadline)
    try:
        return engine, engine.new_tab(url)
    except Exception as exc:
        STATE.push_trace({
            "ts": _now(),
            "message": "opening tab failed; reconnecting Chrome transport",
            "error": f"{type(exc).__name__}: {exc}",
        })
        engine = _reconnect_engine(_remaining_seconds(deadline, timeout_seconds))
        _check_job_open(deadline)
        return engine, engine.new_tab(url)


def _is_tab_session_error(exc: Exception) -> bool:
    message = f"{type(exc).__name__}: {exc}"
    return (
        "not part of browser session" in message
        or "No tab with id" in message
        or "Debugger unattached" in message
        or "unexpected response id" in message
    )


def _navigate_with_tab_recovery(engine, tab, url: str, timeout_seconds: float,
                                deadline: float, *, reason: str,
                                step_no: int | None = None):
    """Navigate, recovering when the OBU session loses ownership of the tab."""
    try:
        engine.navigate(tab, url)
        return engine, tab
    except Exception as exc:
        if not _is_tab_session_error(exc):
            raise
        trace = {
            "ts": _now(),
            "message": "tab detached during navigation; reconnecting",
            "reason": reason,
            "tab": getattr(tab, "id", ""),
            "url": url,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if step_no is not None:
            trace["step"] = step_no
        STATE.push_trace(trace)

    engine = _reconnect_engine(_remaining_seconds(deadline, timeout_seconds))
    try:
        tab = engine.claim_tab(int(getattr(tab, "id")))
        engine.navigate(tab, url)
        _set_active_tab(engine, tab)
        STATE.push_trace({
            "ts": _now(),
            "message": "reclaimed detached browser tab",
            "reason": reason,
            "tab": getattr(tab, "id", ""),
        })
        return engine, tab
    except Exception as exc:
        trace = {
            "ts": _now(),
            "message": "tab reclaim failed; opening replacement tab",
            "reason": reason,
            "tab": getattr(tab, "id", ""),
            "url": url,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if step_no is not None:
            trace["step"] = step_no
        STATE.push_trace(trace)
        engine, tab = _new_tab_with_retry(engine, url, timeout_seconds, deadline)
        _set_active_tab(engine, tab)
        return engine, tab


def _wait_for_load_with_tab_recovery(engine, tab, url: str, timeout_seconds: float,
                                     deadline: float, *, reason: str,
                                     step_no: int | None = None):
    try:
        bh = engine.activate(tab)
        bh.wait_for_load(timeout=_remaining_seconds(deadline, 15))
        return engine, tab, bh
    except Exception as exc:
        if not _is_tab_session_error(exc):
            raise
        trace = {
            "ts": _now(),
            "message": "tab detached while waiting for load; reconnecting",
            "reason": reason,
            "tab": getattr(tab, "id", ""),
            "url": url,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if step_no is not None:
            trace["step"] = step_no
        STATE.push_trace(trace)
        engine, tab = _navigate_with_tab_recovery(
            engine,
            tab,
            url,
            timeout_seconds,
            deadline,
            reason=reason,
            step_no=step_no,
        )
        bh = engine.activate(tab)
        bh.wait_for_load(timeout=_remaining_seconds(deadline, 15))
        return engine, tab, bh


def _serialize(step: CrawlStep) -> dict:
    d = {
        "id": step.id,
        "link_id": step.link_id,
        "url": step.url,
        "host": step.host,
        "mode": step.mode,
        "status": step.status,
        "error": step.error,
        "error_detail": step.error_detail,
        "note": step.note,
        "dom_text": step.dom_text[:2000] if step.dom_text else "",
        "prompt": step.prompt[:2000] if step.prompt else "",
        "response": step.response,
        "actions": step.actions[-20:],
        "trace": step.trace[-80:],
        "domain_knowledge": step.domain_knowledge[-20:],
        "domain_playbooks": step.domain_playbooks[-20:],
        "has_screenshot": step.png_bytes is not None,
        "progress": STATE.progress,
    }
    return _json_safe(d)


async def _event_generator(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    def _on_state(step):
        loop.call_soon_threadsafe(queue.put_nowait, _serialize(step))

    remove = STATE.listen(_on_state)
    try:
        yield {"data": json.dumps(_serialize(STATE.current))}
        while True:
            if await request.is_disconnected():
                break
            try:
                data = await asyncio.wait_for(queue.get(), timeout=5)
                yield {"data": json.dumps(data)}
            except asyncio.TimeoutError:
                yield {"data": '{"ping": true}'}
    finally:
        remove()


@app.get("/events")
async def sse(request: Request):
    return EventSourceResponse(_event_generator(request))


@app.get("/screenshot")
async def screenshot():
    step = STATE.current
    if step.png_bytes:
        return Response(content=step.png_bytes, media_type="image/png")
    if _RUN_LOCK.locked():
        if _LAST_SCREENSHOT_BYTES:
            return Response(content=_LAST_SCREENSHOT_BYTES, media_type="image/png")
        return Response(status_code=204)
    try:
        png = await asyncio.wait_for(
            asyncio.to_thread(_live_chrome_screenshot),
            timeout=_screenshot_timeout_seconds(),
        )
    except asyncio.TimeoutError:
        png = None
    if png:
        return Response(content=png, media_type="image/png")
    if _LAST_SCREENSHOT_BYTES:
        return Response(content=_LAST_SCREENSHOT_BYTES, media_type="image/png")
    return Response(status_code=204)


@app.get("/state")
async def state():
    return _serialize(STATE.current)


@app.post("/activity/clear")
async def clear_activity():
    STATE.clear_activity()
    return {"status": "cleared"}


@app.get("/")
async def index():
    html = (HERE / "index.html").read_text()
    return HTMLResponse(html)


@app.get("/app.css")
async def app_css():
    return Response((HERE / "app.css").read_text(), media_type="text/css")


@app.get("/app.js")
async def app_js():
    return Response((HERE / "app.js").read_text(), media_type="application/javascript")


# ---- interactive endpoints ----

class CrawlRequest(BaseModel):
    url: str
    id: str | None = None
    reason: str | None = None
    sub_reason: str | None = None
    timeout_seconds: float | None = None


class PromptRequest(BaseModel):
    text: str


class AgentRequest(BaseModel):
    instruction: str
    timeout_seconds: float | None = None


class FetchAllRequest(BaseModel):
    ids: list[str] | None = None
    limit: int | None = None
    q: str = ""
    reason: str = ""
    status: str = ""
    confirm_count: int | None = None
    delay_seconds: float | None = None
    jitter_seconds: float | None = None
    timeout_seconds: float | None = None


class RefreshLinksRequest(BaseModel):
    db: str | None = None
    statuses: list[str] | None = None
    limit: int | None = None


class DomainNoteRequest(BaseModel):
    url: str = ""
    host: str = ""
    text: str


class DomainPlaybookRequest(BaseModel):
    url: str = ""
    host: str = ""
    title: str = ""
    steps: str


class ServiceStartRequest(BaseModel):
    brain: bool = True
    chrome: bool = True
    model_id: str | None = None
    model: str | None = None
    restart_brain: bool = False


class BrainConfigRequest(BaseModel):
    model_id: str | None = None
    model: str | None = None
    restart_brain: bool = True


def _result_from_state(link: dict, status: str, summary: str | None = None,
                       error: str | None = None, error_detail: str = "") -> dict:
    current = STATE.current
    rec = {
        "id": link["id"],
        "url": link["url"],
        "reason": link.get("reason"),
        "sub_reason": link.get("sub_reason"),
        "engine": "chrome",
        "status": status,
        "mode": current.mode or None,
        "used_vision": current.mode == "vision",
        "forged": False,
        "summary": summary,
        "error": error,
        "error_detail": error_detail,
        "note": current.note,
        "trace": current.trace[-80:],
        "domain_knowledge": current.domain_knowledge[-20:],
        "domain_playbooks": current.domain_playbooks[-20:],
    }
    if current.url and current.url != link.get("url"):
        rec["final_url"] = current.url
    return rec


def _persist_agent_result(url: str, status: str, summary: str = "",
                          error: str = "", error_detail: str = "",
                          source_link: dict | None = None) -> None:
    effective_url = url if url.startswith(("http://", "https://")) else ""
    if not effective_url and source_link:
        source_url = str(source_link.get("url") or "")
        if source_url.startswith(("http://", "https://")):
            effective_url = source_url
    if not effective_url:
        return
    link = {
        "id": _safe_link_id(effective_url),
        "url": effective_url,
        "reason": "agent",
        "sub_reason": "prompt",
    }
    _persist_result(_result_from_state(
        link,
        status,
        summary=summary or None,
        error=error or None,
        error_detail=error_detail,
    ))
    if source_link and source_link.get("id") and source_link.get("id") != link["id"]:
        source = dict(source_link)
        source.setdefault("url", url)
        source_rec = _result_from_state(
            source,
            status,
            summary=summary or None,
            error=error or None,
            error_detail=error_detail,
        )
        source_rec["final_url"] = effective_url
        _persist_result(source_rec)
    STATE.push_trace({
        "ts": _now(),
        "message": "agent result persisted",
        "url": effective_url,
        "source_id": source_link.get("id") if source_link else "",
        "status": status,
    })


def _should_agentic_fallback(link: dict, perception, knowledge: dict) -> bool:
    if not _env_enabled("DEEPEST_AGENTIC_FALLBACK", True):
        return False
    reason = str(link.get("reason") or "").lower()
    note = str(getattr(perception, "note", "") or "").lower()
    text = getattr(perception, "text", "") or ""
    if knowledge.get("playbooks"):
        return True
    if reason in {"unretrievable", "needs_deep_crawl"}:
        return True
    if _looks_blocked_or_error(text):
        return True
    if "dom_error" in note or "vision_failed" in note:
        return len(text.strip()) < 120
    if "thin_dom" in note:
        return len(text.strip()) < 40
    return False


def _fallback_summary(url: str, text: str, reason: str) -> str:
    clean = " ".join((text or "").split())
    if clean:
        excerpt = clean[:2000]
        return (
            f"Fetched page content, but local brain summarization failed ({reason}).\n\n"
            f"URL: {url}\n\nCaptured text excerpt:\n{excerpt}"
        )
    return (
        f"Reached the page, but local brain summarization failed ({reason}) "
        "and no readable DOM text was captured."
    )


def _is_brain_failure(exc: Exception) -> bool:
    message = f"{type(exc).__name__}: {exc}"
    return any(marker in message for marker in (
        "RemoteDisconnected",
        "Brain server",
        "urlopen",
        "timed out",
        "Connection refused",
        "Connection reset",
    ))


def _is_direct_crawl_instruction(instruction: str, url: str) -> bool:
    if not url:
        return False
    text = (instruction or "").lower()
    crawl_terms = (
        "crawl", "fetch", "scrape", "extract", "summarize", "summarise",
        "read this", "read the", "get content", "page content",
    )
    interactive_terms = (
        "click", "login", "log in", "sign in", "search", "type",
        "fill", "submit", "autofill", "bitwarden", "scroll",
    )
    if any(term in text for term in interactive_terms):
        return False
    return any(term in text for term in crawl_terms)


def _do_crawl(link_or_url, timeout_seconds: float | None = None):
    """Run one crawl in a background thread, pushing state updates."""
    link = link_or_url if isinstance(link_or_url, dict) else {"url": str(link_or_url)}
    url = link["url"]
    link.setdefault("id", _safe_link_id(url))
    host = _host_of(url)
    knowledge = _load_domain_knowledge(host)
    timeout = _crawl_timeout_seconds(timeout_seconds)
    deadline = _job_deadline(timeout)

    STATE.current = CrawlStep(
        id=link["id"],
        link_id=link["id"],
        url=url,
        host=host,
        status="queued",
        prompt=link.get("prompt", ""),
        domain_knowledge=knowledge.get("notes", []),
        domain_playbooks=knowledge.get("playbooks", []),
    )
    tab = None
    brain = None
    try:
        _check_job_open(deadline)
        _trace("connecting to real Chrome transport")
        if link.get("prompt"):
            _trace("direct crawl from agent prompt", instruction=link.get("prompt"))
        engine = _ensure_engine(_remaining_seconds(deadline, timeout))
        STATE.update(status="navigating")
        _trace("opening tab", url=url)
        engine, tab = _new_tab_with_retry(
            engine,
            url,
            timeout,
            deadline,
        )
        _set_active_tab(engine, tab)
        _job_sleep(2, deadline)
        _publish_screenshot(engine, tab, "initial browser screenshot")

        from ..perception.policy import perceive
        _check_job_open(deadline)
        _trace("perceiving page")
        p = perceive(engine, tab, url)
        current_url = engine.current_url(tab) or url
        STATE.update(url=current_url, host=_host_of(current_url))
        _publish_screenshot(engine, tab, "post-perception browser screenshot")
        response_status = _page_response_status(engine, tab)
        down_reason = _content_down_reason(p.text or "", response_status)
        if down_reason and not _is_wayback_url(current_url) and knowledge.get("playbooks"):
            STATE.push_trace({
                "ts": _now(),
                "message": "content down; trying domain playbook before archive",
                "reason": down_reason,
                "url": current_url,
                "playbooks": len(knowledge.get("playbooks", [])),
            })
            instruction = (
                f"Go to {url} and crawl it. Use the domain memory/playbooks if they apply. "
                "The current page appears content-down; try any concrete domain playbook "
                "for this URL before using the Internet Archive. If no playbook applies, "
                "use the Internet Archive / Wayback Machine for the URL."
            )
            _do_agentic(
                instruction,
                initial_url=url,
                timeout_seconds=_remaining_seconds(deadline, timeout),
                source_link=link,
            )
            return
        if down_reason and not _is_wayback_url(current_url):
            archive_url = _wayback_snapshot_url(current_url, deadline) or _wayback_snapshot_url(url, deadline)
            if archive_url:
                STATE.push_trace({
                    "ts": _now(),
                    "message": "content down; trying internet archive",
                    "reason": down_reason,
                    "url": current_url,
                    "archive_url": archive_url,
                })
                STATE.update(status="navigating", note="trying Internet Archive snapshot",
                             url=archive_url, host=_host_of(archive_url))
                engine, tab = _navigate_with_tab_recovery(
                    engine,
                    tab,
                    archive_url,
                    timeout,
                    deadline,
                    reason="direct archive fallback",
                )
                engine, tab, _ = _wait_for_load_with_tab_recovery(
                    engine,
                    tab,
                    archive_url,
                    timeout,
                    deadline,
                    reason="direct archive fallback",
                )
                _job_sleep(1, deadline)
                url = engine.current_url(tab) or archive_url
                STATE.update(url=url, host=_host_of(url))
                p = perceive(engine, tab, url)
                _publish_screenshot(engine, tab, "internet archive browser screenshot")
                response_status = _page_response_status(engine, tab)
                down_reason = _content_down_reason(p.text or "", response_status)
        if down_reason and _is_wayback_url(current_url if not _is_wayback_url(url) else url):
            failed_url = url if _is_wayback_url(url) else current_url
            message = _content_down_failure_message(failed_url, down_reason)
            _trace("archived page unavailable", reason=down_reason, url=failed_url)
            STATE.update(error=message, status="error")
            _persist_result(_result_from_state(link, "failed", error=message))
            return
        if _should_agentic_fallback(link, p, knowledge):
            STATE.push_trace({
                "ts": _now(),
                "message": "agentic fallback triggered",
                "reason": link.get("reason"),
                "mode": p.mode,
                "perception_note": p.note,
                "playbooks": len(knowledge.get("playbooks", [])),
            })
            instruction = (
                f"Go to {url} and crawl it. Use the domain memory/playbooks if they apply. "
                "If the direct page is blocked or empty, navigate like a human to find the real content, "
                "then extract a faithful summary. If the page is a 404, not found, removed, unavailable, "
                "or otherwise content-down, use the Internet Archive / Wayback Machine for the URL."
            )
            _do_agentic(
                instruction,
                initial_url=url,
                timeout_seconds=_remaining_seconds(deadline, timeout),
                source_link=link,
            )
            return
        _check_job_open(deadline)
        STATE.update(mode=p.mode, dom_text=p.text or "", status="summarizing")
        _trace("summarizing page", mode=p.mode, perception_note=p.note)
        STATE.update(prompt=f"Summarize: {url}", status="thinking")
        _publish_screenshot(engine, tab, "pre-summary browser screenshot")
        try:
            _trace("checking local brain")
            brain = _ensure_brain(_remaining_seconds(deadline, timeout))
            if p.mode == "vision" and p.image_png:
                summary = _call_brain_with_retry(
                    lambda call_timeout: brain.summarize_image(
                        url, p.image_png, timeout=call_timeout
                    ),
                    deadline, timeout, "image summary",
                )
            else:
                summary = _call_brain_with_retry(
                    lambda call_timeout: brain.summarize_text(
                        url, p.text or "", timeout=call_timeout
                    ),
                    deadline, timeout, "text summary",
                )
        except Exception as brain_exc:
            if not (p.text or "").strip():
                raise
            message = f"{type(brain_exc).__name__}: {brain_exc}"
            detail = _fallback_summary(url, p.text or "", message)
            down_reason = _content_down_reason(p.text or "", _page_response_status(engine, tab))
            if down_reason:
                failure = _content_down_failure_message(url, down_reason)
                _trace("captured content is unavailable page", reason=down_reason, error=message)
                STATE.update(error=failure, error_detail=message, status="error")
                _persist_result(_result_from_state(
                    link, "failed", error=failure, error_detail=message,
                ))
                return
            STATE.push_trace({
                "ts": _now(),
                "message": "brain summary failed; failing crawl with captured DOM text",
                "error": message,
            })
            failure = f"Local brain summarization failed: {message}"
            STATE.update(error=failure, error_detail=detail, status="error")
            _persist_result(_result_from_state(
                link, "failed", error=failure, error_detail=detail,
            ))
            return
        STATE.update(response=summary)

        _publish_screenshot(engine, tab, "captured final screenshot")
        STATE.update(status="done")
        _persist_result(_result_from_state(link, "ok", summary=summary))
        _append_domain_note(host, "crawl-ok", f"Fetched successfully via {p.mode}.", url,
                            STATE.current.trace)
        _auto_learn_domain(brain, host, url, "ok", STATE.current.trace, summary=summary)
    except Exception as e:
        detail = _error_detail(e)
        message = f"{type(e).__name__}: {e}"
        _trace("crawl failed", error=message)
        STATE.update(error=message, error_detail=detail, status="error")
        _persist_result(_result_from_state(link, "failed", error=message,
                                           error_detail=detail))
        _append_domain_note(host, "crawl-error", message, url, STATE.current.trace)
        _auto_learn_domain(brain, host, url, "failed", STATE.current.trace, error=message)
    finally:
        if tab is not None:
            _close_job_tab(engine, tab, reason="crawl job finished")
        _set_active_tab()


def _do_prompt(text: str):
    """Send a free-form prompt to the brain in a background thread."""
    STATE.current = CrawlStep(id="prompt", url="", status="thinking")
    STATE.update(prompt=text, status="thinking")
    try:
        brain = _ensure_brain()
        # Use summarize_text as a generic prompt — it sends system + user
        response = brain.summarize_text(
            "prompt",
            text,
            max_chars=_env_int("DEEPEST_PROMPT_MAX_CHARS", 10000),
            max_tokens=_env_int("DEEPEST_PROMPT_MAX_TOKENS", 1024),
        )
        STATE.update(response=response, status="done")
    except Exception as e:
        STATE.update(error=f"{type(e).__name__}: {e}", error_detail=_error_detail(e),
                     status="error")


def _run_crawl_job(link: dict, timeout_seconds: float | None = None) -> None:
    if not _RUN_LOCK.acquire(blocking=False):
        STATE.current = CrawlStep(status="error", error="Another crawl job is already running.")
        return
    try:
        _CANCEL_EVENT.clear()
        STATE.progress = {"done": 0, "total": 1}
        _do_crawl(link, timeout_seconds=timeout_seconds)
        STATE.progress = {"done": 1, "total": 1}
    finally:
        _CANCEL_EVENT.clear()
        _RUN_LOCK.release()


def _cancelable_sleep(seconds: float) -> bool:
    deadline = time.time() + max(0.0, seconds)
    while time.time() < deadline:
        if _CANCEL_EVENT.is_set():
            return False
        time.sleep(min(1.0, deadline - time.time()))
    return not _CANCEL_EVENT.is_set()


def _run_fetch_all_job(
    links: list[dict],
    delay_seconds: float | None = None,
    jitter_seconds: float | None = None,
    timeout_seconds: float | None = None,
) -> None:
    if not _RUN_LOCK.acquire(blocking=False):
        STATE.current = CrawlStep(status="error", error="Another crawl job is already running.")
        return
    delay = _bulk_delay_seconds(delay_seconds)
    jitter = _bulk_jitter_seconds(jitter_seconds)
    timeout = _crawl_timeout_seconds(timeout_seconds)
    try:
        _CANCEL_EVENT.clear()
        STATE.progress = {"done": 0, "total": len(links)}
        for i, link in enumerate(links, 1):
            if _CANCEL_EVENT.is_set():
                STATE.current = CrawlStep(
                    id="crawl-all",
                    status="canceled",
                    note=f"Canceled after {i - 1} of {len(links)} URLs.",
                )
                STATE.progress = {"done": i - 1, "total": len(links)}
                return
            _do_crawl(dict(link), timeout_seconds=timeout)
            STATE.progress = {"done": i, "total": len(links)}
            if i < len(links):
                wait = delay + (random.uniform(0, jitter) if jitter else 0)
                STATE.current = CrawlStep(
                    id="crawl-all",
                    status="waiting",
                    note=f"Waiting {wait:.1f}s before next URL.",
                )
                STATE.progress = {"done": i, "total": len(links)}
                if not _cancelable_sleep(wait):
                    STATE.current = CrawlStep(
                        id="crawl-all",
                        status="canceled",
                        note=f"Canceled after {i} of {len(links)} URLs.",
                    )
                    return
        STATE.current = CrawlStep(
            id="crawl-all",
            status="done",
            note=f"Crawl all complete: {len(links)} URLs.",
        )
    finally:
        _CANCEL_EVENT.clear()
        _RUN_LOCK.release()


def _normalize_status_filter(status: str) -> str:
    status_norm = status.strip().lower()
    if status_norm == "success":
        return "ok"
    return status_norm


def _display_status(status: str) -> str:
    return "success" if status == "ok" else status


def _link_status(link: dict, results: dict[str, dict] | None) -> str:
    if not results:
        return "pending"
    return results.get(str(link.get("id")), {}).get("status", "pending")


def _filter_links(
    links: list[dict],
    q: str = "",
    reason: str = "",
    status: str = "",
    results: dict[str, dict] | None = None,
) -> list[dict]:
    q_norm = q.strip().lower()
    reason_norm = reason.strip().lower()
    status_norm = _normalize_status_filter(status)

    def keep(link: dict) -> bool:
        if q_norm and q_norm not in str(link.get("url", "")).lower():
            return False
        if reason_norm and reason_norm != str(link.get("reason", "")).lower():
            return False
        if status_norm and status_norm != _link_status(link, results):
            return False
        return True

    return [link for link in links if keep(link)]


@app.get("/links")
async def links(limit: int = 100, offset: int = 0, q: str = "", reason: str = "", status: str = ""):
    limit = max(1, min(limit, 50000))
    offset = max(0, offset)
    all_links = _load_links()
    results = _load_results_by_id()
    filtered = _filter_links(all_links, q=q, reason=reason, status=status, results=results)
    rows = []
    for link in filtered[offset: offset + limit]:
        result = results.get(str(link.get("id")), {})
        host = _host_of(link.get("url", ""))
        rows.append({
            **link,
            "host": host,
            "status": _display_status(result.get("status", "pending")),
            "mode": result.get("mode"),
            "summary": result.get("summary"),
            "error": result.get("error"),
            "updated_at": result.get("updated_at"),
            "domain_notes": _domain_note_count(host),
        })
    return _json_safe({
        "path": str(LINKS_PATH),
        "total": len(all_links),
        "filtered": len(filtered),
        "offset": offset,
        "limit": limit,
        "rows": rows,
    })


@app.get("/results/{link_id}")
async def result_detail(link_id: str):
    from fastapi.responses import JSONResponse
    links_by_id = {str(link.get("id")): link for link in _load_links()}
    link = links_by_id.get(link_id)
    result = _load_results_by_id().get(link_id)
    if not link and not result:
        return JSONResponse({"error": "Unknown link id."}, status_code=404)

    url = (result or link or {}).get("url", "")
    host = _host_of(url)
    knowledge = _load_domain_knowledge(host)
    return _json_safe({
        "link": link,
        "result": result,
        "host": host,
        "domain_knowledge": knowledge,
    })


@app.post("/fetch-all")
async def fetch_all(req: FetchAllRequest):
    from fastapi.responses import JSONResponse
    if _RUN_LOCK.locked():
        return JSONResponse({"error": "Another crawl job is already running."}, status_code=409)
    all_links = _load_links()
    results = _load_results_by_id()
    by_id = {str(l.get("id")): l for l in all_links}
    if req.ids:
        selected = _filter_links([by_id[i] for i in req.ids if i in by_id],
                                 q=req.q, reason=req.reason, status=req.status,
                                 results=results)
    else:
        selected = _filter_links(all_links, q=req.q, reason=req.reason,
                                 status=req.status, results=results)
    if req.limit:
        selected = selected[:max(0, req.limit)]
    if not selected:
        return JSONResponse({"error": "No links selected."}, status_code=400)
    if req.confirm_count != len(selected):
        return JSONResponse({
            "error": (
                "Bulk crawl confirmation did not match the selected URL count. "
                "Refresh the list and confirm again."
            ),
            "selected_count": len(selected),
            "confirm_count": req.confirm_count,
        }, status_code=409)
    delay = _bulk_delay_seconds(req.delay_seconds)
    jitter = _bulk_jitter_seconds(req.jitter_seconds)
    timeout = _crawl_timeout_seconds(req.timeout_seconds)
    threading.Thread(
        target=_run_fetch_all_job,
        args=(selected, delay, jitter, timeout),
        daemon=True,
    ).start()
    return {
        "status": "accepted",
        "count": len(selected),
        "delay_seconds": delay,
        "jitter_seconds": jitter,
        "timeout_seconds": timeout,
    }


@app.post("/jobs/cancel")
async def cancel_job():
    if _RUN_LOCK.locked():
        _CANCEL_EVENT.set()
        _close_active_tab()
        STATE.update(status="canceling", note="Cancel requested; stopping after current URL.")
        return {"status": "canceling"}
    STATE.current = CrawlStep(id="cancel", status="idle", note="No running crawl job.")
    return {"status": "idle"}


@app.post("/links/refresh")
async def refresh_links(req: RefreshLinksRequest):
    from fastapi.responses import JSONResponse
    try:
        exporter = importlib.import_module("extract_links")
        result = exporter.export_links(
            db=req.db or exporter.DB,
            out=LINKS_PATH,
            statuses=req.statuses or exporter.STATUSES,
            limit=req.limit or 0,
        )
        _invalidate_link_caches()
    except Exception as e:
        detail = _error_detail(e)
        STATE.current = CrawlStep(
            id="links-refresh",
            status="error",
            error=f"{type(e).__name__}: {e}",
            error_detail=detail,
            note="Eastself link refresh failed",
        )
        return JSONResponse({
            "error": f"{type(e).__name__}: {e}",
            "error_detail": detail,
        }, status_code=500)

    STATE.current = CrawlStep(
        id="links-refresh",
        status="done",
        note=f"refreshed {result['count']} Eastself links",
        response=json.dumps(result, ensure_ascii=False),
    )
    return {"status": "ok", **result}


@app.post("/domain-note")
async def domain_note(req: DomainNoteRequest):
    from fastapi.responses import JSONResponse
    text = req.text.strip()
    if not text:
        return JSONResponse({"error": "Empty domain note."}, status_code=400)
    host = (req.host or _host_of(req.url)).strip().lower()
    if not host:
        return JSONResponse({"error": "A URL or host is required."}, status_code=400)
    data = _append_domain_note(host, "operator", text, req.url)
    if STATE.current.host == host:
        STATE.update(
            domain_knowledge=data.get("notes", []),
            domain_playbooks=data.get("playbooks", []),
        )
    return {
        "status": "saved",
        "host": host,
        "notes": data.get("notes", []),
        "playbooks": data.get("playbooks", []),
    }


@app.post("/domain-playbook")
async def domain_playbook(req: DomainPlaybookRequest):
    from fastapi.responses import JSONResponse
    steps = req.steps.strip()
    if not steps:
        return JSONResponse({"error": "Empty playbook."}, status_code=400)
    host = (req.host or _host_of(req.url)).strip().lower()
    if not host:
        return JSONResponse({"error": "A URL or host is required."}, status_code=400)
    data = _append_domain_playbook(host, "operator", req.title, steps, req.url)
    if STATE.current.host == host:
        STATE.update(domain_playbooks=data.get("playbooks", []))
    return {
        "status": "saved",
        "host": host,
        "playbooks": data.get("playbooks", []),
    }


def _set_service_state(status: str, note: str = "", error: str = "") -> None:
    _SERVICE_STATE.update({"status": status, "note": note, "error": error})
    STATE.update(status=f"services_{status}", note=note, error=error)


def _start_services(req: ServiceStartRequest) -> None:
    global _brain_ready
    if not _SERVICE_LOCK.acquire(blocking=False):
        _set_service_state("busy", "service startup already running")
        return
    try:
        _set_service_state("starting", "starting local services")
        if req.brain:
            if req.model_id or req.model or req.restart_brain:
                selected = services.configure_brain(
                    model_id=req.model_id,
                    model=req.model,
                    restart=req.restart_brain,
                )
                _brain_ready = False
                _set_service_state("starting", f"selected brain: {selected['label']}")
            services.ensure_brain(
                status=lambda note: _set_service_state("starting", note)
            )
        if req.chrome:
            services.ensure_chrome_transport(
                status=lambda note: _set_service_state("starting", note)
            )
        _set_service_state("ready", "local services ready")
    except Exception as e:
        _set_service_state("error", "local service startup failed",
                           f"{type(e).__name__}: {e}")
    finally:
        _SERVICE_LOCK.release()


@app.get("/services")
async def service_status():
    return _json_safe({
        **services.status(),
        "startup": dict(_SERVICE_STATE),
    })


@app.post("/services/brain")
async def configure_brain(req: BrainConfigRequest):
    global _brain_ready
    from fastapi.responses import JSONResponse
    if _RUN_LOCK.locked():
        return JSONResponse({"error": "Cannot switch brain model while a crawl job is running."}, status_code=409)
    try:
        selected = services.configure_brain(
            model_id=req.model_id,
            model=req.model,
            restart=req.restart_brain,
        )
        _brain_ready = False
        return _json_safe({
            "status": "configured",
            "selected": selected,
            "services": services.status(),
        })
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=400)


@app.post("/services/start")
async def start_services(req: ServiceStartRequest):
    from fastapi.responses import JSONResponse
    if _SERVICE_LOCK.locked():
        return JSONResponse({
            "error": "Service startup is already running.",
            "startup": dict(_SERVICE_STATE),
        }, status_code=409)
    threading.Thread(target=_start_services, args=(req,), daemon=True).start()
    return {"status": "accepted", "startup": dict(_SERVICE_STATE)}


# ---- agentic loop ----

AGENT_SYSTEM = (
    "You are a browser automation agent. You control a real Chrome browser.\n\n"
    "Given a user instruction and the current page state, output ONE action.\n"
    "Output exactly these formats:\n\n"
    "navigate|URL\n"
    "click|text|visible text of link/button\n"
    "click|css|CSS selector\n"
    "click|xy|x|y\n"
    "type|css|CSS selector|text to type\n"
    "type|xy|x|y|text to type\n"
    "type|text|text to type into the currently focused field\n"
    "press|key\n"
    "submit|css|CSS selector\n"
    "autofill|bitwarden\n"
    "login|bitwarden|CSS selector for form or password field, or auto\n"
    "archive|current\n"
    "archive|original\n"
    "archive|URL\n"
    "scroll|down\n"
    "scroll|up\n"
    "back\n"
    "reload\n"
    "wait|seconds\n"
    "screenshot\n"
    "extract\n"
    "done|detailed content summary\n\n"
    "If the current page already contains enough information to answer the crawl request, "
    "output done| followed by the faithful summary. Do not output plain prose without done|.\n"
    "Be as thorough and detailed as possible. If the summary would be barebones, use scroll|down "
    "or scroll|up to acquire more context before outputting done|.\n"
    "The final summary must describe page content, not browser status. Do not say things like "
    "'landed on the page', 'fully loaded', or 'no need for further navigation'. Explain vague "
    "references with concrete details; for example, if there is 'confusion', explain what caused it. "
    "Do not mention generic site chrome such as navigation links, social media links, search bars, "
    "menus, sidebars, footers, copyright or legal notices, sign-in/login/subscription gating "
    "(for example 'requires sign-in to view lyrics'), or recommendation modules such as "
    "recommended, related, popular, or 'fans also like' items, "
    "or extraction internals such as 'Extracted main content'.\n"
    "Do not invent or call tools/functions outside the formats listed above.\n\n"
    "For visual security pages that require typing, click the field first if needed, "
    "then use type|text|... for the focused field and press|Enter to submit. "
    "Use type|xy|x|y|... when the screenshot shows exactly where to type.\n\n"
    "Global content-down policy: if the page is 404, not found, removed, unavailable, "
    "or otherwise down, first apply any concrete domain playbook that matches "
    "the current URL or page state. If no playbook applies, use the archive tool. "
    "When a domain playbook says to navigate, modify a URL, click, type, or search, "
    "you must output that concrete non-archive action before any archive action. "
    "Cloudflare origin errors such as "
    "520, 521, 522, 523, 524, 525, 526, 527, and 530 are content-down states, "
    "not verification challenges. Emit archive|current for the current page, "
    "archive|original for the user's original URL, or archive|URL for a specific target. "
    "Then crawl the archived content. If an archived snapshot is blank, empty, or has "
    "no readable content, do not output done; use archive|original to search another "
    "capture, or report failure only after archive search is exhausted. Do not treat "
    "transient anti-bot or verification "
    "screens as content-down. If the page says Cloudflare, checking your browser, "
    "security check, or just a moment without a visible control, use wait|5 once "
    "and let Chrome complete the check. If there is a visible 'verify you are human' "
    "control, checkbox, captcha, or security verification widget, click it or "
    "interact with it like a human. Do not keep waiting forever, and do not reload "
    "a verification page just to escape it.\n\n"
    "Examples:\n"
    "navigate|https://example.com\n"
    "click|text|SoundCloud\n"
    "type|css|#searchbox|hello world\n"
    "type|text|123456\n"
    "type|xy|420|315|123456\n"
    "press|Enter\n"
    "submit|css|form[role=search]\n"
    "autofill|bitwarden\n"
    "login|bitwarden|auto\n"
    "archive|current\n"
    "done|The article explains the concrete facts, background, and outcome in detail.\n"
    "Never ask for, print, or store passwords. Use Bitwarden only through Chrome autofill.\n"
)


PLAYBOOK_SYSTEM = (
    "You convert domain playbooks into exactly one concrete browser action. "
    "Output only one action in one of these formats:\n"
    "navigate|URL\n"
    "click|text|visible text of link/button\n"
    "click|css|CSS selector\n"
    "click|xy|x|y\n"
    "type|css|CSS selector|text to type\n"
    "type|xy|x|y|text to type\n"
    "type|text|text to type into the currently focused field\n"
    "press|key\n"
    "submit|css|CSS selector\n"
    "scroll|down\n"
    "scroll|up\n"
    "wait|seconds\n"
    "back\n"
    "reload\n\n"
    "Archive actions are forbidden. Done/final summaries are forbidden. "
    "If a playbook describes changing a URL, output navigate| with the changed URL."
)


def _looks_like_final_summary(text: str) -> bool:
    clean = " ".join(text.strip().split())
    if len(clean) < 80:
        return False
    lower = clean.lower()
    if lower.startswith(("i need to ", "we need to ", "next ", "first ", "let's ")):
        return False
    if any(token in lower for token in (
        "click|", "navigate|", "scroll|", "wait|", "extract|", "done|",
        "call_tool", "tool_use", "function_call",
    )):
        return False
    summary_markers = (
        "the page is", "the page describes", "the page contains",
        "the forum", "the thread", "the article", "the post",
        "summary", "features", "introduces", "discussing",
    )
    return "." in clean and any(marker in lower for marker in summary_markers)


def _clean_final_summary(text: str) -> str:
    clean = " ".join((text or "").strip().split())
    clean = re.sub(r"(?i)^done\|\s*", "", clean).strip()
    clean = re.sub(r"(?i)^landed on the\s+", "The ", clean).strip()
    clean = re.sub(r"(?i)^landed on\s+", "", clean).strip()
    clean = re.sub(r"(?i)^the\s+page\s+is\s+fully\s+loaded[.;]?\s*", "", clean).strip()
    sentences = re.split(r"(?<=[.!?])\s+", clean)
    noise_markers = (
        "fully loaded",
        "no need for further navigation",
        "no further navigation",
        "no need to navigate",
        "current page already contains",
        "browser status",
        "navigation links",
        "social media links",
        "search bar",
        "search box",
        "menu links",
        "site navigation",
        "other sections of the site",
        "extracted main content",
        "main content is extracted",
        "extracted and summarized",
        "browser viewport",
        "visible viewport",
        "page footer",
        "site footer",
        "in the footer",
        "copyright information",
        "copyright notice",
        "all rights reserved",
        "©",
        "requires sign-in",
        "requires sign in",
        "requires a sign",
        "sign in to",
        "sign-in to",
        "log in to",
        "requires a subscription",
        "requires login",
        "to listen to the full",
        "to view the lyrics",
        "to view lyrics",
        "recommended track",
        "recommended release",
        "recommended for you",
        "related track",
        "related release",
        "popular release",
        "popular track",
        "you might also",
        "more like this",
        "fans also",
    )
    kept = [
        sentence.strip()
        for sentence in sentences
        if sentence.strip()
        and not any(marker in sentence.lower() for marker in noise_markers)
    ]
    return " ".join(kept).strip() or clean


def _normalize_extracted_text(text: str, *, limit: int = 20000) -> str:
    lines = []
    for raw in (text or "").splitlines():
        line = " ".join(raw.split())
        if line:
            lines.append(line)
    return "\n".join(lines)[:limit].strip()


def _word_count(text: str) -> int:
    return len(re.findall(r"\w+", text or ""))


def _soup_text(el) -> str:
    return _normalize_extracted_text(el.get_text("\n", strip=True), limit=20000)


def _extract_with_bs4(html: str) -> dict:
    if not html or BeautifulSoup is None:
        return {"text": "", "title": "", "source": "none", "words": 0}
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    for node in soup.select(
        "script,style,noscript,svg,canvas,template,nav,header,footer,aside,"
        "form,iframe,[role=navigation],[role=banner],[role=contentinfo]"
    ):
        node.decompose()

    title = ""
    if soup.title and soup.title.string:
        title = " ".join(soup.title.string.split())
    h1 = soup.find("h1")
    if h1:
        h1_text = _normalize_extracted_text(h1.get_text(" ", strip=True), limit=300)
        title = h1_text or title

    selectors = (
        "article",
        "main",
        "[role=main]",
        ".entry-content",
        ".post-content",
        ".article-content",
        ".td-post-content",
        ".story-content",
        ".post",
    )
    candidates = []
    for selector in selectors:
        for el in soup.select(selector):
            text = _soup_text(el)
            words = _word_count(text)
            para_count = len([p for p in el.find_all(["p", "li", "blockquote"]) if _word_count(p.get_text(" ")) >= 8])
            if words >= 40:
                candidates.append((words + para_count * 20, selector, text))

    if candidates:
        _, selector, text = max(candidates, key=lambda item: item[0])
        return {"text": text, "title": title, "source": f"bs4:{selector}", "words": _word_count(text)}

    paragraphs = []
    for el in soup.find_all(["h1", "h2", "h3", "p", "li", "blockquote"]):
        text = _normalize_extracted_text(el.get_text(" ", strip=True), limit=2000)
        words = _word_count(text)
        if words >= 8 or el.name in {"h1", "h2", "h3"}:
            paragraphs.append(text)
    text = _normalize_extracted_text("\n".join(dict.fromkeys(paragraphs)), limit=20000)
    return {"text": text, "title": title, "source": "bs4:paragraphs", "words": _word_count(text)}


def _extract_main_content(html: str) -> dict:
    if not html:
        return {"text": "", "title": "", "source": "none", "words": 0}
    if trafilatura is not None:
        try:
            text = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=True,
                deduplicate=True,
            )
            text = _normalize_extracted_text(text or "", limit=20000)
            if _word_count(text) >= 40:
                return {
                    "text": text,
                    "title": "",
                    "source": "trafilatura",
                    "words": _word_count(text),
                }
        except Exception as exc:
            STATE.push_trace({
                "ts": _now(),
                "message": "article extraction failed",
                "extractor": "trafilatura",
                "error": f"{type(exc).__name__}: {exc}",
            })
    return _extract_with_bs4(html)


def _best_text_for_extraction(obs: dict, dom: str) -> str:
    extracted = obs.get("extracted") if isinstance(obs, dict) else {}
    article_text = extracted.get("text", "") if isinstance(extracted, dict) else ""
    return article_text or obs.get("viewport_text", "") or dom or ""


def _page_can_scroll_more(obs: dict) -> bool:
    scroll = obs.get("scroll") if isinstance(obs, dict) else {}
    if not isinstance(scroll, dict):
        return False
    try:
        y = float(scroll.get("y") or 0)
        h = float(scroll.get("h") or 0)
        page_h = float(scroll.get("pageH") or 0)
    except (TypeError, ValueError):
        return False
    return page_h > h + 350 and y < page_h - h - 250


def _context_scroll_amount(obs: dict, fraction: float = 0.38, cap: int = 520) -> int:
    scroll = obs.get("scroll") if isinstance(obs, dict) else {}
    try:
        y = float(scroll.get("y") or 0)
        h = float(scroll.get("h") or 800)
        page_h = float(scroll.get("pageH") or 0)
    except (AttributeError, TypeError, ValueError):
        y, h, page_h = 0.0, 800.0, 0.0
    remaining = max(0, int(page_h - y - h - 80)) if page_h else cap
    amount = max(180, min(int(h * fraction), cap))
    return max(120, min(amount, remaining or amount))


def _summary_needs_more_context(summary: str, obs: dict, forced_scrolls: int) -> bool:
    if forced_scrolls >= 2 or not _page_can_scroll_more(obs):
        return False
    clean = " ".join((summary or "").split())
    lower = clean.lower()
    words = re.findall(r"\w+", clean)
    word_count = len(words)
    detail_score = _summary_detail_score(clean)
    meta_markers = (
        "landed on",
        "fully loaded",
        "no need for further navigation",
        "no further navigation",
        "no need to navigate",
    )
    if any(marker in lower for marker in meta_markers):
        return True
    if word_count >= 75 and detail_score >= 4:
        return False
    if word_count < 90:
        return True
    vague_markers = ("confusion", "issue", "problem", "statement", "apolog")
    if any(marker in lower for marker in vague_markers) and word_count < 140 and detail_score < 4:
        return True
    return False


def _summary_detail_score(summary: str) -> int:
    tokens = re.findall(r"\b[\w.-]+\b", summary or "")
    numbers = {token for token in tokens if any(ch.isdigit() for ch in token)}
    acronyms = {
        token for token in tokens
        if len(token) >= 2 and any(ch.isalpha() for ch in token) and token.upper() == token
    }
    causal_markers = (
        "because", "caused", "due to", "explains", "clarify", "clarifies",
        "introduced", "working with", "so that", "while", "variant",
        "specification", "description",
    )
    score = min(len(numbers), 4) + min(len(acronyms), 3)
    score += sum(1 for marker in causal_markers if marker in summary.lower())
    if re.search(r"\([^)]{3,80}\)", summary or ""):
        score += 1
    return score


def _summary_is_barebones(summary: str, obs: dict) -> bool:
    clean = " ".join((summary or "").split())
    lower = clean.lower()
    words = _word_count(clean)
    detail_score = _summary_detail_score(clean)
    extracted = obs.get("extracted") if isinstance(obs, dict) else {}
    article_words = int(extracted.get("words") or 0) if isinstance(extracted, dict) else 0
    meta_markers = (
        "fully loaded",
        "no need for further navigation",
        "navigation links",
        "social media links",
        "search bar",
        "search box",
        "other sections of the site",
        "extracted main content",
        "main content is extracted",
        "extracted and summarized",
        "article text is visible",
        "contains the complete article content",
    )
    if any(marker in lower for marker in meta_markers):
        return True
    if words < 70:
        return True
    if detail_score >= 4 and words >= 75:
        return False
    if article_words >= 500 and words < 120:
        return True
    vague_markers = ("confusion", "issue", "problem", "statement", "apolog")
    if any(marker in lower for marker in vague_markers) and words < 160 and detail_score < 4:
        return True
    return False


def _summary_reports_unavailable(summary: str) -> str:
    lower = " ".join((summary or "").lower().split())
    if not lower:
        return "empty summary"
    markers = (
        "about:blank",
        "page is empty",
        "page appears to be empty",
        "page appears to be blank",
        "blank page",
        "empty page",
        "has no content",
        "with no content",
        "empty with no visible content",
        "no visible content",
        "no extracted text",
        "no readable content",
        "no readable dom text",
        "content-down",
        "content down",
        "page is unavailable",
        "nothing to summarize",
    )
    for marker in markers:
        if marker in lower:
            return marker
    return ""


def _extraction_summary_prompt(instruction: str, url: str, obs: dict,
                               candidate: str, content: str) -> str:
    extracted = obs.get("extracted") if isinstance(obs, dict) else {}
    source = extracted.get("source", "") if isinstance(extracted, dict) else ""
    title = extracted.get("title", "") if isinstance(extracted, dict) else ""
    viewport_text = obs.get("viewport_text", "") if isinstance(obs, dict) else ""
    return (
        "Summarize the crawled page from extracted page content. Be faithful, "
        "specific, and detailed. Do not discuss browser status, navigation menus, "
        "social links, search bars, sidebars, footers, copyright or legal notices, "
        "sign-in/login/subscription gating (for example 'requires sign-in to view "
        "lyrics'), recommendation modules such as recommended, related, popular, or "
        "'fans also like' items, whether the page is loaded, "
        "or whether more navigation is needed. Never mention extraction internals like "
        "'Extracted main content' or that content was extracted/summarized. "
        "Explain vague references such as 'confusion' by stating the concrete cause. "
        "If the extracted text includes the author's framing, facts, quotes, specs, "
        "dates, or outcome, include them.\n\n"
        f"User instruction: {instruction}\n"
        f"URL: {url}\n"
        f"Title: {title or obs.get('title', '')}\n"
        f"Extractor: {source or 'unknown'}\n"
        f"Rejected candidate summary: {candidate}\n\n"
        f"Visible viewport text:\n{viewport_text[:3000]}\n\n"
        f"Extracted main content:\n{content[:12000]}\n\n"
        "Return only the final summary."
    )


def _summary_verifier_prompt(instruction: str, url: str, obs: dict,
                             candidate: str, content: str) -> str:
    extracted = obs.get("extracted") if isinstance(obs, dict) else {}
    source = extracted.get("source", "") if isinstance(extracted, dict) else ""
    title = extracted.get("title", "") if isinstance(extracted, dict) else ""
    viewport_text = obs.get("viewport_text", "") if isinstance(obs, dict) else ""
    return (
        "You are the verifier for a web crawl summary. Decide whether the candidate "
        "summary is faithful and sufficiently specific for the user's crawl request.\n\n"
        "Accept concise summaries when they cover the central facts, concrete cause, "
        "important numbers/specs/names, and outcome. Do not reject only because the "
        "summary is shorter than the source. Extracted content may be sparse or empty "
        "for sign-in-walled or script-heavy pages; in that case judge faithfulness "
        "against the visible viewport text and the candidate itself, and do not decline "
        "solely because the extracted content is short or empty. Decline if it is mostly browser status, "
        "navigation chrome, vague paraphrase, missing the cause of the article, or "
        "contradicts the extracted content. Decline if it mentions generic page chrome "
        "such as navigation links, social media links, search bars, menus, sidebars, "
        "footers, copyright or legal notices, sign-in/login/subscription gating "
        "(for example 'requires sign-in to view lyrics'), recommendation modules such "
        "as recommended, related, popular, or 'fans also like' items, "
        "or extraction internals such as 'Extracted main content'. Decline "
        "if it merely says the page is empty, blank, unavailable, or has no visible "
        "content; that is a crawl failure or archive-search task, not a successful "
        "page summary.\n\n"
        "Output exactly one line:\n"
        "accept|short reason\n"
        "decline|short reason\n\n"
        f"User instruction: {instruction}\n"
        f"URL: {url}\n"
        f"Title: {title or obs.get('title', '')}\n"
        f"Extractor: {source or 'unknown'}\n\n"
        f"Candidate summary:\n{candidate[:3000]}\n\n"
        f"Visible viewport text:\n{viewport_text[:2000]}\n\n"
        f"Extracted content:\n{content[:9000]}"
    )


def _parse_verifier_response(text: str) -> dict | None:
    line = ""
    for raw in (text or "").splitlines():
        raw = raw.strip().strip("`")
        if raw:
            line = raw
            break
    if not line:
        return None
    parts = [part.strip() for part in line.split("|", 1)]
    verdict = parts[0].lower()
    if verdict not in {"accept", "decline"}:
        return None
    return {
        "accepted": verdict == "accept",
        "reason": parts[1] if len(parts) > 1 else "",
        "raw": text,
    }


def _verify_summary_candidate(brain, instruction: str, url: str, obs: dict,
                              candidate: str, content: str, deadline: float,
                              timeout: float, step_no: int) -> dict | None:
    unavailable_reason = _summary_reports_unavailable(candidate)
    if unavailable_reason:
        return {
            "accepted": False,
            "reason": f"candidate reports unavailable content: {unavailable_reason}",
            "raw": candidate,
        }
    try:
        response = _call_brain_with_retry(
            lambda call_timeout: brain.summarize_text(
                f"agent-summary-verifier-{step_no}",
                _summary_verifier_prompt(instruction, url, obs, candidate, content),
                max_chars=14000,
                max_tokens=128,
                timeout=call_timeout,
            ),
            deadline, timeout, "agent summary verifier",
        )
        parsed = _parse_verifier_response(response)
        if parsed is None:
            STATE.push_trace({
                "ts": _now(),
                "message": "summary verifier returned unparseable response",
                "step": step_no,
                "response": response,
            })
            return None
        STATE.push_trace({
            "ts": _now(),
            "message": "summary verifier accepted" if parsed["accepted"] else "summary verifier declined",
            "step": step_no,
            "reason": parsed.get("reason", ""),
            "response": response,
        })
        return parsed
    except Exception as exc:
        STATE.push_trace({
            "ts": _now(),
            "message": "summary verifier failed",
            "step": step_no,
            "error": f"{type(exc).__name__}: {exc}",
        })
        return None


def _parse_action(text: str) -> dict:
    lines = text.strip().split("\n")
    for line in lines:
        line = line.strip()
        if not line or line.startswith(("#", "//", "```")):
            continue
        parts = [p.strip() for p in line.split("|")]
        if not parts:
            continue
        action = parts[0].lower().strip("`")
        if action == "navigate" and len(parts) >= 2:
            return {"action": "navigate", "url": parts[1]}
        if action == "click" and len(parts) >= 3:
            if parts[1] == "xy" and len(parts) >= 4:
                try:
                    return {
                        "action": "click",
                        "by": "xy",
                        "x": float(parts[2]),
                        "y": float(parts[3]),
                    }
                except ValueError:
                    return {"action": "ask", "raw": text}
            return {"action": "click", "by": parts[1], "value": parts[2]}
        if action == "type" and len(parts) >= 3:
            if parts[1] == "text":
                return {"action": "type", "by": "text", "text": "|".join(parts[2:])}
            if parts[1] == "xy" and len(parts) >= 5:
                try:
                    return {
                        "action": "type",
                        "by": "xy",
                        "x": float(parts[2]),
                        "y": float(parts[3]),
                        "text": "|".join(parts[4:]),
                    }
                except ValueError:
                    return {"action": "ask", "raw": text}
            if len(parts) >= 4:
                return {
                    "action": "type",
                    "by": parts[1],
                    "selector": parts[2],
                    "text": "|".join(parts[3:]),
                }
        if action == "press" and len(parts) >= 2:
            return {"action": "press", "key": parts[1]}
        if action == "submit" and len(parts) >= 3:
            return {"action": "submit", "by": parts[1], "selector": parts[2]}
        if action == "autofill" and len(parts) >= 2 and parts[1] == "bitwarden":
            return {"action": "autofill", "provider": "bitwarden"}
        if action == "login" and len(parts) >= 2 and parts[1] == "bitwarden":
            return {
                "action": "login",
                "provider": "bitwarden",
                "selector": parts[2] if len(parts) >= 3 else "auto",
            }
        if action in {"archive", "wayback"}:
            return {
                "action": "archive",
                "target": parts[1] if len(parts) >= 2 and parts[1] else "current",
            }
        if action == "scroll" and len(parts) >= 2:
            amount = None
            if len(parts) >= 3:
                try:
                    amount = max(100, min(int(float(parts[2])), 4000))
                except ValueError:
                    amount = None
            return {"action": "scroll", "direction": parts[1], "amount": amount}
        if action == "wait" and len(parts) >= 2:
            try:
                seconds = max(0.0, min(float(parts[1]), 30.0))
            except ValueError:
                seconds = 1.0
            return {"action": "wait", "seconds": seconds}
        if action == "screenshot":
            return {"action": "screenshot"}
        if action == "back":
            return {"action": "back"}
        if action == "reload":
            return {"action": "reload"}
        if action == "extract":
            return {"action": "extract"}
        if action == "done":
            return {"action": "done", "answer": parts[1] if len(parts) > 1 else ""}
    if _looks_like_final_summary(text):
        return {"action": "done", "answer": text.strip(), "inferred": True}
    return {"action": "ask", "raw": text}


def _recover_tab(engine, url: str):
    """Open a fresh tab and navigate to url. Returns (tab, bh)."""
    from ..engines.chrome import RealChromeEngine
    tab = engine.new_tab()
    _set_active_tab(engine, tab)
    engine.cdp(tab, "Page.enable")
    bh = engine.activate(tab)
    if url:
        engine.navigate(tab, url)
        bh.wait_for_load(timeout=15)
    return tab, bh


def _observe_page(engine, tab) -> dict:
    script = """
(() => {
  const visible = (el) => {
    const r = el.getBoundingClientRect();
    const s = getComputedStyle(el);
    return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
  };
  const cssPath = (el) => {
    if (el.id) return '#' + CSS.escape(el.id);
    const parts = [];
    let node = el;
    while (node && node.nodeType === 1 && parts.length < 4) {
      let part = node.tagName.toLowerCase();
      if (node.getAttribute('name')) part += `[name="${CSS.escape(node.getAttribute('name'))}"]`;
      else if (node.classList && node.classList.length) part += '.' + [...node.classList].slice(0, 2).map(CSS.escape).join('.');
      const parent = node.parentElement;
      if (parent) {
        const siblings = [...parent.children].filter(x => x.tagName === node.tagName);
        if (siblings.length > 1) part += `:nth-of-type(${siblings.indexOf(node) + 1})`;
      }
      parts.unshift(part);
      node = parent;
    }
    return parts.join(' > ');
  };
  const labelFor = (el) => {
    const aria = el.getAttribute('aria-label') || el.getAttribute('title') || el.getAttribute('alt') || '';
    const val = el.getAttribute('value') || el.getAttribute('placeholder') || '';
    return (aria || el.innerText || val || '').replace(/\\s+/g, ' ').trim().slice(0, 120);
  };
  const controls = [...document.querySelectorAll('a,button,[role=button],input[type=submit],input[type=button]')]
    .filter(visible).slice(0, 60).map(el => {
      const r = el.getBoundingClientRect();
      return {text: labelFor(el), selector: cssPath(el), href: el.href || el.getAttribute('href') || '', x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2)};
    });
  const inputs = [...document.querySelectorAll('input:not([type=hidden]),textarea,select,[contenteditable=true]')]
    .filter(visible).slice(0, 40).map(el => {
      const r = el.getBoundingClientRect();
      return {label: labelFor(el), selector: cssPath(el), tag: el.tagName.toLowerCase(), type: el.getAttribute('type') || '', value: (el.value || '').slice(0, 80), x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2)};
    });
  const forms = [...document.querySelectorAll('form')]
    .filter(visible).slice(0, 20).map(el => ({selector: cssPath(el), text: labelFor(el), action: el.action || el.getAttribute('action') || ''}));
  const seen = new Set();
  const viewportText = [...document.querySelectorAll('h1,h2,h3,h4,p,li,blockquote,figcaption,pre,td,th')]
    .filter(el => {
      if (!visible(el)) return false;
      const r = el.getBoundingClientRect();
      return r.bottom >= 0 && r.top <= innerHeight;
    })
    .map(el => (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim())
    .filter(text => {
      if (!text || text.length < 8 || seen.has(text)) return false;
      seen.add(text);
      return true;
    })
    .join('\\n')
    .slice(0, 5000);
  return {
    title: document.title,
    url: location.href,
    text: (document.body ? document.body.innerText : '').slice(0, 5000),
    viewport_text: viewportText,
    html: (document.documentElement ? document.documentElement.outerHTML : '').slice(0, 400000),
    controls,
    inputs,
    forms,
    scroll: {
      x: scrollX,
      y: scrollY,
      w: innerWidth,
      h: innerHeight,
      pageH: document.documentElement.scrollHeight
    }
  };
})()
"""
    obs = engine.evaluate(tab, script)
    if not isinstance(obs, dict):
        return {}
    obs["extracted"] = _extract_main_content(obs.get("html", ""))
    return obs


def _press_key(engine, tab, key: str) -> None:
    key_map = {
        "Enter": (13, "Enter", "\r"),
        "Return": (13, "Enter", "\r"),
        "Tab": (9, "Tab", "\t"),
        "Escape": (27, "Escape", ""),
        "Backspace": (8, "Backspace", ""),
        "Delete": (46, "Delete", ""),
        " ": (32, "Space", " "),
        "Space": (32, "Space", " "),
        "ArrowLeft": (37, "ArrowLeft", ""),
        "ArrowUp": (38, "ArrowUp", ""),
        "ArrowRight": (39, "ArrowRight", ""),
        "ArrowDown": (40, "ArrowDown", ""),
        "Home": (36, "Home", ""),
        "End": (35, "End", ""),
        "PageDown": (34, "PageDown", ""),
        "PageUp": (33, "PageUp", ""),
    }
    if key in key_map:
        vk, code, text = key_map[key]
    elif len(key) == 1:
        if key.isalpha():
            vk, code = ord(key.upper()), f"Key{key.upper()}"
        elif key.isdigit():
            vk, code = ord(key), f"Digit{key}"
        else:
            vk, code = ord(key), key
        text = key
    else:
        vk, code, text = 0, key, ""
    base = {
        "key": " " if key == "Space" else key,
        "code": code,
        "windowsVirtualKeyCode": vk,
        "nativeVirtualKeyCode": vk,
    }
    engine.cdp(tab, "Input.dispatchKeyEvent", {
        "type": "keyDown",
        **base,
        **({"text": text} if text else {}),
    })
    if text and len(text) == 1:
        engine.cdp(tab, "Input.dispatchKeyEvent", {
            "type": "char",
            "text": text,
            **base,
        })
    engine.cdp(tab, "Input.dispatchKeyEvent", {"type": "keyUp", **base})


def _type_text(engine, tab, text: str) -> None:
    if not text:
        return
    engine.cdp(tab, "Input.insertText", {"text": text})
    engine.evaluate(tab, """
        (() => {
          const el = document.activeElement;
          if (!el) return false;
          el.dispatchEvent(new Event('input', {bubbles: true}));
          el.dispatchEvent(new Event('change', {bubbles: true}));
          return true;
        })()
    """)


def _dispatch_hotkey(engine, tab, key: str, modifiers: list[str]) -> None:
    modifier_bits = {"Alt": 1, "Control": 2, "Meta": 4, "Shift": 8}
    code = f"Key{key.upper()}" if len(key) == 1 and key.isalpha() else key
    vk = ord(key.upper()) if len(key) == 1 else 0
    active = sum(modifier_bits.get(mod, 0) for mod in modifiers)
    for mod in modifiers:
        engine.cdp(tab, "Input.dispatchKeyEvent", {
            "type": "rawKeyDown",
            "key": mod,
            "code": mod,
            "windowsVirtualKeyCode": 0,
            "modifiers": active,
        })
    engine.cdp(tab, "Input.dispatchKeyEvent", {
        "type": "rawKeyDown",
        "key": key,
        "code": code,
        "windowsVirtualKeyCode": vk,
        "modifiers": active,
    })
    engine.cdp(tab, "Input.dispatchKeyEvent", {
        "type": "keyUp",
        "key": key,
        "code": code,
        "windowsVirtualKeyCode": vk,
        "modifiers": active,
    })
    for mod in reversed(modifiers):
        active -= modifier_bits.get(mod, 0)
        engine.cdp(tab, "Input.dispatchKeyEvent", {
            "type": "keyUp",
            "key": mod,
            "code": mod,
            "windowsVirtualKeyCode": 0,
            "modifiers": max(active, 0),
        })


def _bitwarden_autofill(engine, tab) -> None:
    engine.evaluate(tab, """
        (() => {
          const el = document.querySelector('input[type=password], input[name*="user" i], input[name*="email" i], input[type=email], input:not([type])');
          if (el) el.focus();
          return !!el;
        })()
    """)
    modifiers = [
        part.strip()
        for part in os.environ.get("DEEPEST_BITWARDEN_AUTOFILL_MODIFIERS", "Meta,Shift").split(",")
        if part.strip()
    ]
    key = os.environ.get("DEEPEST_BITWARDEN_AUTOFILL_KEY", "l")
    _dispatch_hotkey(engine, tab, key, modifiers)


def _click_xy(engine, tab, x: float, y: float) -> None:
    params = {"x": x, "y": y, "button": "left", "clickCount": 1}
    engine.cdp(tab, "Input.dispatchMouseEvent", {"type": "mousePressed", **params})
    engine.cdp(tab, "Input.dispatchMouseEvent", {"type": "mouseReleased", **params})


def _verification_click_target(engine, tab) -> dict | None:
    try:
        target = engine.evaluate(tab, """
            (() => {
              const visible = (el) => {
                const r = el.getBoundingClientRect();
                const s = getComputedStyle(el);
                return r.width > 0 && r.height > 0 &&
                  s.visibility !== 'hidden' && s.display !== 'none' &&
                  r.bottom >= 0 && r.right >= 0 && r.top <= innerHeight && r.left <= innerWidth;
              };
              const center = (r) => ({
                x: Math.round(Math.min(innerWidth - 8, Math.max(8, r.left + r.width / 2))),
                y: Math.round(Math.min(innerHeight - 8, Math.max(8, r.top + r.height / 2)))
              });
              const labelOf = (el) => [
                el.innerText || el.textContent || '',
                el.getAttribute('aria-label') || '',
                el.getAttribute('title') || '',
                el.getAttribute('alt') || '',
                el.getAttribute('value') || '',
                el.id || '',
                el.className || ''
              ].join(' ').replace(/\\s+/g, ' ').trim();
              const controls = [...document.querySelectorAll(
                'input[type=checkbox],button,[role=checkbox],[role=button],label'
              )].filter(visible);
              for (const el of controls) {
                const label = labelOf(el).toLowerCase();
                if (
                  el.matches('input[type=checkbox],[role=checkbox]') ||
                  /verify|human|captcha|security|continue/.test(label)
                ) {
                  const r = el.getBoundingClientRect();
                  return {
                    ...center(r),
                    method: 'control',
                    label: label.slice(0, 120)
                  };
                }
              }
              const frames = [...document.querySelectorAll('iframe')]
                .filter(visible)
                .map((el) => {
                  const r = el.getBoundingClientRect();
                  const label = [
                    el.src || '',
                    el.title || '',
                    el.name || '',
                    el.id || '',
                    el.className || ''
                  ].join(' ').toLowerCase();
                  return {el, r, label};
                });
              let frame = frames.find((item) =>
                /cloudflare|turnstile|challenge|captcha|cf-chl/.test(item.label)
              );
              if (!frame) {
                frame = frames.find((item) =>
                  item.r.width >= 180 && item.r.height >= 40 && item.r.height <= 180
                );
              }
              if (frame) {
                return {
                  x: Math.round(Math.min(innerWidth - 8, Math.max(8, frame.r.left + Math.min(32, frame.r.width / 2)))),
                  y: Math.round(Math.min(innerHeight - 8, Math.max(8, frame.r.top + frame.r.height / 2))),
                  method: 'iframe-checkbox',
                  label: frame.label.slice(0, 160)
                };
              }
              return null;
            })()
        """)
        return target if isinstance(target, dict) else None
    except Exception as exc:
        STATE.push_trace({
            "ts": _now(),
            "message": "verification target lookup failed",
            "error": f"{type(exc).__name__}: {exc}",
        })
        return None


def _viewport_size(engine, tab) -> dict:
    try:
        size = engine.evaluate(tab, """
            (() => ({w: innerWidth, h: innerHeight, dpr: devicePixelRatio || 1}))()
        """)
        return size if isinstance(size, dict) else {}
    except Exception:
        return {}


def _visual_checkbox_target(png: bytes, viewport: dict | None = None) -> dict | None:
    if not png or Image is None:
        return None
    try:
        img = Image.open(io.BytesIO(png)).convert("RGB")
    except Exception:
        return None

    width, height = img.size
    if width < 80 or height < 80:
        return None
    pixels = img.load()
    y_start = int(height * 0.08)
    y_end = int(height * 0.78)
    x_end = int(width * 0.90)
    min_side = max(10, int(min(width, height) * 0.012))
    max_side = max(34, int(min(width, height) * 0.075))
    visited = bytearray(width * height)
    candidates = []

    def is_dark(x: int, y: int) -> bool:
        r, g, b = pixels[x, y]
        return r < 135 and g < 135 and b < 135 and (max(r, g, b) - min(r, g, b)) < 75

    def brightness(x: int, y: int) -> int:
        r, g, b = pixels[x, y]
        return (r + g + b) // 3

    for y in range(y_start, y_end):
        row = y * width
        for x in range(0, x_end):
            idx = row + x
            if visited[idx] or not is_dark(x, y):
                continue
            stack = [(x, y)]
            visited[idx] = 1
            min_x = max_x = x
            min_y = max_y = y
            count = 0
            while stack:
                cx, cy = stack.pop()
                count += 1
                if cx < min_x:
                    min_x = cx
                elif cx > max_x:
                    max_x = cx
                if cy < min_y:
                    min_y = cy
                elif cy > max_y:
                    max_y = cy
                if count > 3000:
                    continue
                for nx, ny in ((cx + 1, cy), (cx - 1, cy), (cx, cy + 1), (cx, cy - 1)):
                    if nx < 0 or nx >= x_end or ny < y_start or ny >= y_end:
                        continue
                    nidx = ny * width + nx
                    if not visited[nidx] and is_dark(nx, ny):
                        visited[nidx] = 1
                        stack.append((nx, ny))

            box_w = max_x - min_x + 1
            box_h = max_y - min_y + 1
            if not (min_side <= box_w <= max_side and min_side <= box_h <= max_side):
                continue
            aspect = box_w / max(1, box_h)
            if not (0.72 <= aspect <= 1.38):
                continue
            density = count / max(1, box_w * box_h)
            if not (0.08 <= density <= 0.55):
                continue

            inset_x = max(2, box_w // 4)
            inset_y = max(2, box_h // 4)
            inner = []
            for iy in range(min_y + inset_y, max_y - inset_y + 1):
                for ix in range(min_x + inset_x, max_x - inset_x + 1):
                    inner.append(brightness(ix, iy))
            if not inner or (sum(inner) / len(inner)) < 165:
                continue

            cx = min_x + box_w / 2
            cy = min_y + box_h / 2
            left_bias = 1.0 - min(0.55, cx / max(1, width))
            size_score = 1.0 - abs(box_w - box_h) / max(box_w, box_h)
            score = left_bias + size_score + (0.25 if cy < height * 0.60 else 0.0)
            candidates.append((score, cx, cy, box_w, box_h, density))

    if not candidates:
        return None
    _, cx, cy, box_w, box_h, density = max(candidates, key=lambda item: item[0])
    viewport = viewport or {}
    css_w = float(viewport.get("w") or width)
    css_h = float(viewport.get("h") or height)
    return {
        "x": round(cx * css_w / width, 1),
        "y": round(cy * css_h / height, 1),
        "method": "visual-checkbox",
        "label": f"{int(box_w)}x{int(box_h)} density={density:.2f}",
    }


def _try_verification_click(engine, tab, *, step_no: int) -> bool:
    target = _verification_click_target(engine, tab)
    if not target:
        png = _try_screenshot(engine, tab, attempts=1)
        target = _visual_checkbox_target(png or b"", _viewport_size(engine, tab))
        if target:
            STATE.push_trace({
                "ts": _now(),
                "message": "verification control found visually",
                "step": step_no,
                "method": target.get("method", ""),
                "label": target.get("label", ""),
                "x": target.get("x"),
                "y": target.get("y"),
            })
        else:
            STATE.push_trace({
                "ts": _now(),
                "message": "verification control not found",
                "step": step_no,
            })
            return False
    try:
        x = float(target.get("x"))
        y = float(target.get("y"))
        _click_xy(engine, tab, x, y)
        STATE.push_trace({
            "ts": _now(),
            "message": "clicked verification control",
            "step": step_no,
            "method": target.get("method", ""),
            "label": target.get("label", ""),
            "x": x,
            "y": y,
        })
        return True
    except Exception as exc:
        STATE.push_trace({
            "ts": _now(),
            "message": "verification click failed",
            "step": step_no,
            "target": target,
            "error": f"{type(exc).__name__}: {exc}",
        })
        return False


def _wheel_scroll(engine, tab, delta_y: int) -> None:
    engine.cdp(tab, "Input.dispatchMouseEvent", {
        "type": "mouseWheel",
        "x": 400,
        "y": 400,
        "deltaX": 0,
        "deltaY": delta_y,
    })


def _js_scroll(engine, tab, delta_y: int) -> None:
    engine.evaluate(tab, f"""
        (() => {{
          window.scrollBy({{ top: {int(delta_y)}, left: 0, behavior: 'instant' }});
          return true;
        }})()
    """)


def _safe_scroll(engine, tab, delta_y: int, *, method: str, step_no: int) -> bool:
    try:
        if method == "wheel":
            _wheel_scroll(engine, tab, delta_y)
        else:
            _js_scroll(engine, tab, delta_y)
        return True
    except Exception as exc:
        STATE.push_trace({
            "ts": _now(),
            "message": "scroll command failed",
            "step": step_no,
            "method": method,
            "delta_y": delta_y,
            "error": f"{type(exc).__name__}: {exc}",
        })
        return False


def _safe_press_key(engine, tab, key: str, *, step_no: int) -> bool:
    try:
        _press_key(engine, tab, key)
        return True
    except Exception as exc:
        STATE.push_trace({
            "ts": _now(),
            "message": "key command failed",
            "step": step_no,
            "key": key,
            "error": f"{type(exc).__name__}: {exc}",
        })
        return False


def _archive_action_url(target: str, current_url: str, initial_url: str,
                        deadline: float,
                        exclude_urls: set[str] | None = None) -> tuple[str, str]:
    requested = (target or "current").strip()
    lowered = requested.lower()
    if lowered == "current":
        source_url = current_url
    elif lowered == "original":
        source_url = initial_url or current_url
    elif requested.startswith(("http://", "https://")):
        source_url = requested
    else:
        source_url = current_url or initial_url
    source_url = _wayback_original_url(source_url)
    return source_url, _wayback_snapshot_url(source_url, deadline, exclude_urls=exclude_urls)


def _playbook_action_prompt(instruction: str, url: str, knowledge_text: str,
                            down_reason: str, obs: dict, dom: str) -> str:
    return (
        f"User instruction: {instruction}\n"
        f"Current URL: {url}\n"
        f"Content-down reason: {down_reason or 'none'}\n"
        f"Domain playbooks and notes:\n{knowledge_text}\n\n"
        f"Visible controls:\n{json.dumps(obs.get('controls', [])[:20], ensure_ascii=False)}\n"
        f"Visible inputs:\n{json.dumps(obs.get('inputs', [])[:20], ensure_ascii=False)}\n"
        f"Visible forms:\n{json.dumps(obs.get('forms', [])[:10], ensure_ascii=False)}\n"
        f"Visible viewport text:\n{str(obs.get('viewport_text', ''))[:1500]}\n"
        f"Raw page text excerpt:\n{dom[:1200]}\n\n"
        "Choose the next non-archive browser action required by the domain playbook."
    )


def _choose_playbook_action(brain_mod, instruction: str, url: str, knowledge_text: str,
                            down_reason: str, obs: dict, dom: str,
                            deadline: float, timeout: float, step_no: int) -> tuple[dict, str]:
    response = _call_brain_with_retry(
        lambda call_timeout: brain_mod.complete(
            PLAYBOOK_SYSTEM,
            _playbook_action_prompt(instruction, url, knowledge_text, down_reason, obs, dom),
            max_tokens=180,
            timeout=call_timeout,
        ),
        deadline, timeout, "domain playbook planner",
    )
    action = _parse_action(response)
    if action.get("action") in {"archive", "done", "ask"}:
        STATE.push_trace({
            "ts": _now(),
            "message": "domain playbook planner produced unusable action",
            "step": step_no,
            "response": response,
            "action": action,
        })
        return {"action": "ask", "raw": response}, response
    STATE.push_trace({
        "ts": _now(),
        "message": "domain playbook planner chose action",
        "step": step_no,
        "response": response,
        "action": action,
    })
    return action, response


def _do_agentic(instruction: str, initial_url: str = "", timeout_seconds: float | None = None,
                source_link: dict | None = None):
    timeout = _crawl_timeout_seconds(timeout_seconds)
    deadline = _job_deadline(timeout)
    initial_host = _host_of(initial_url) if initial_url else ""
    initial_knowledge = _load_domain_knowledge(initial_host) if initial_host else {"notes": [], "playbooks": []}
    source_id = str(source_link.get("id") or "") if source_link else ""
    STATE.current = CrawlStep(
        id=source_id or (_safe_link_id(initial_url) if initial_url else "agent"),
        link_id=source_id or (_safe_link_id(initial_url) if initial_url else ""),
        url=(source_link.get("url") if source_link else initial_url) or initial_url,
        host=initial_host,
        status="starting",
        prompt=instruction,
        domain_knowledge=initial_knowledge.get("notes", []),
        domain_playbooks=initial_knowledge.get("playbooks", []),
    )
    tab = None
    brain = None
    last_url = ""
    last_host = ""
    last_dom = ""
    archive_tried_urls: set[str] = set()
    verification_waits: dict[str, int] = {}
    verification_clicks: dict[str, int] = {}
    playbook_archive_blocks: dict[str, int] = {}
    playbook_attempted: set[str] = set()
    forced_context_scrolls = 0

    def mark_playbook_attempt(current_url: str, current_knowledge: dict) -> None:
        if current_knowledge.get("playbooks") and current_url and not _is_wayback_url(current_url):
            playbook_attempted.add(_normal_url_key(_wayback_original_url(current_url) or current_url))

    try:
        _check_job_open(deadline)
        engine = _ensure_engine(_remaining_seconds(deadline, timeout))
        engine, tab = _new_tab_with_retry(engine, None, timeout, deadline)
        _set_active_tab(engine, tab)
        engine.cdp(tab, "Page.enable")
        bh = engine.activate(tab)
        if initial_url:
            STATE.update(status="navigating", note=f"opening {initial_url}")
            STATE.push_trace({
                "ts": _now(),
                "message": "initial agent navigation",
                "url": initial_url,
            })
            engine.navigate(tab, initial_url)
            bh.wait_for_load(timeout=_remaining_seconds(deadline, 15))

        MAX_STEPS = 20
        for step_no in range(1, MAX_STEPS + 1):
            _check_job_open(deadline)
            # observe
            try:
                obs = _observe_page(engine, tab)
                url = obs.get("url") or engine.current_url(tab) or ""
                dom = obs.get("text") or engine.dom_text(tab) or ""
                extraction_text = _best_text_for_extraction(obs, dom)
                host = _host_of(url)
                knowledge = _load_domain_knowledge(host) if host else {"notes": [], "playbooks": []}
                last_url = url
                last_host = host
                last_dom = extraction_text or dom
                response_status = _page_response_status(engine, tab)
                verification_reason = _transient_verification_reason(dom, response_status)
                down_reason = _content_down_reason(dom, response_status)
                if (
                    down_reason and not verification_reason
                    and url and not _is_wayback_url(url)
                    and not knowledge.get("playbooks")
                    and url not in archive_tried_urls
                ):
                    archive_tried_urls.add(url)
                    archive_url = (
                        _wayback_snapshot_url(url, deadline, exclude_urls=archive_tried_urls)
                        or _wayback_snapshot_url(initial_url, deadline, exclude_urls=archive_tried_urls)
                    )
                    if archive_url:
                        archive_tried_urls.add(archive_url)
                        STATE.update(
                            status="navigating",
                            url=archive_url,
                            host=_host_of(archive_url),
                            note="trying Internet Archive snapshot",
                        )
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "content down; trying internet archive",
                            "step": step_no,
                            "reason": down_reason,
                            "url": url,
                            "archive_url": archive_url,
                        })
                        engine, tab = _navigate_with_tab_recovery(
                            engine,
                            tab,
                            archive_url,
                            timeout,
                            deadline,
                            reason="agent archive fallback",
                            step_no=step_no,
                        )
                        engine, tab, bh = _wait_for_load_with_tab_recovery(
                            engine,
                            tab,
                            archive_url,
                            timeout,
                            deadline,
                            reason="agent archive fallback",
                            step_no=step_no,
                        )
                        _job_sleep(1, deadline)
                        continue
                if down_reason and not verification_reason and url and _is_wayback_url(url):
                    archive_tried_urls.add(url)
                    source_url = _wayback_original_url(url) or _wayback_original_url(initial_url)
                    next_archive = _wayback_snapshot_url(
                        source_url,
                        deadline,
                        exclude_urls=archive_tried_urls,
                    )
                    if next_archive:
                        archive_tried_urls.add(next_archive)
                        STATE.update(
                            status="navigating",
                            note="archived page unavailable; trying another snapshot",
                            url=next_archive,
                            host=_host_of(next_archive),
                        )
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "archived page unavailable; trying another snapshot",
                            "step": step_no,
                            "reason": down_reason,
                            "url": url,
                            "archive_url": next_archive,
                        })
                        engine, tab = _navigate_with_tab_recovery(
                            engine,
                            tab,
                            next_archive,
                            timeout,
                            deadline,
                            reason="unavailable archive retry",
                            step_no=step_no,
                        )
                        engine, tab, bh = _wait_for_load_with_tab_recovery(
                            engine,
                            tab,
                            next_archive,
                            timeout,
                            deadline,
                            reason="unavailable archive retry",
                            step_no=step_no,
                        )
                        _job_sleep(1, deadline)
                        continue
                    message = _content_down_failure_message(url, down_reason)
                    STATE.push_trace({
                        "ts": _now(),
                        "message": "archived page unavailable",
                        "step": step_no,
                        "reason": down_reason,
                        "url": url,
                    })
                    STATE.update(error=message, status="error")
                    _persist_agent_result(url, "failed", error=message,
                                          source_link=source_link)
                    return
            except Exception:
                STATE.update(note=f"step {step_no} — CDP disconnected, reconnecting...")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "CDP disconnected; reconnecting",
                    "step": step_no,
                })
                engine = _reconnect_engine(_remaining_seconds(deadline, timeout))
                tab, bh = _recover_tab(engine, "")
                url = ""
                dom = ""
                extraction_text = ""
                verification_reason = ""
                host = ""
                obs = {}
                knowledge = {"notes": [], "playbooks": []}

            _publish_screenshot(engine, tab, f"agent step {step_no} screenshot")

            STATE.update(
                status="active", url=url, host=host, dom_text=(extraction_text or dom)[:2000],
                note=f"step {step_no}/{MAX_STEPS}",
                domain_knowledge=knowledge.get("notes", []),
                domain_playbooks=knowledge.get("playbooks", []),
                prompt=instruction if step_no == 1 else "",
            )

            # build prompt for brain
            knowledge_text = _domain_instruction_text(knowledge)
            playbook_key = _normal_url_key(_wayback_original_url(url) or url)
            playbook_block_count = playbook_archive_blocks.get(playbook_key, 0)
            playbook_attempted_here = playbook_key in playbook_attempted
            STATE.push_trace({
                "ts": _now(),
                "message": "agent prompt domain memory",
                "step": step_no,
                "notes": len(knowledge.get("notes", [])),
                "playbooks": len(knowledge.get("playbooks", [])),
                "has_workarounds": bool(knowledge_text),
            })
            extracted = obs.get("extracted") if isinstance(obs, dict) else {}
            article_text = extracted.get("text", "") if isinstance(extracted, dict) else ""
            article_source = extracted.get("source", "") if isinstance(extracted, dict) else ""
            article_words = extracted.get("words", 0) if isinstance(extracted, dict) else 0
            viewport_text = obs.get("viewport_text", "") if isinstance(obs, dict) else ""
            user_prompt = (
                f"--- PAGE STATE ---\n"
                f"User instruction: {instruction}\n"
                f"Current URL: {url}\n"
                f"Known domain workarounds:\n{knowledge_text or '- none'}\n"
                f"Title: {obs.get('title', '')}\n"
                f"Scroll: {json.dumps(obs.get('scroll', {}), ensure_ascii=False)}\n"
                f"Transient verification: {verification_reason or 'none'}\n"
                f"Content-down reason: {down_reason or 'none'}\n"
                f"Domain playbook attempted: {'yes' if playbook_attempted_here else 'no'}\n"
                f"Archive blocked by playbook policy: {playbook_block_count} times\n"
                f"Visible controls:\n{json.dumps(obs.get('controls', [])[:30], ensure_ascii=False)}\n"
                f"Visible inputs:\n{json.dumps(obs.get('inputs', [])[:20], ensure_ascii=False)}\n"
                f"Visible forms:\n{json.dumps(obs.get('forms', [])[:10], ensure_ascii=False)}\n"
                f"Extracted main content ({article_source or 'none'}, {article_words} words):\n"
                f"{article_text[:7000]}\n\n"
                f"Visible viewport text:\n{viewport_text[:3000]}\n\n"
                f"Raw page text excerpt:\n{dom[:1800]}\n\n"
                f"What is your next action?"
            )
            vision_prompt = (
                f"{user_prompt}\n\n"
                "A screenshot of the current browser viewport is attached. "
                "Use it to locate visible verification widgets, checkboxes, or buttons. "
                "When clicking from the screenshot, output click|xy|x|y using CSS viewport "
                "coordinates from the top-left of the screenshot/browser viewport."
            )
            try:
                if brain is None:
                    brain = _ensure_brain(_remaining_seconds(deadline, timeout))
                if knowledge_text and not playbook_attempted_here and not _is_wayback_url(url):
                    action, response = _choose_playbook_action(
                        brain,
                        instruction,
                        url,
                        knowledge_text,
                        down_reason,
                        obs,
                        dom,
                        deadline,
                        timeout,
                        step_no,
                    )
                elif verification_reason and getattr(brain, "has_vision", lambda: False)():
                    png = _try_screenshot(engine, tab, attempts=1)
                    if png:
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "agent brain step using screenshot vision",
                            "step": step_no,
                            "reason": verification_reason,
                        })
                        response = _call_brain_with_retry(
                            lambda call_timeout: brain.complete_with_image(
                                AGENT_SYSTEM,
                                vision_prompt,
                                png,
                                max_tokens=_agent_vision_max_tokens(),
                                temperature=0.2,
                                timeout=call_timeout,
                            ),
                            deadline, timeout, "agent vision brain step",
                        )
                    else:
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "agent vision step skipped; screenshot unavailable",
                            "step": step_no,
                            "reason": verification_reason,
                        })
                        response = _call_brain_with_retry(
                            lambda call_timeout: brain.complete(
                                AGENT_SYSTEM,
                                user_prompt,
                                max_tokens=_agent_brain_max_tokens(),
                                timeout=call_timeout,
                            ),
                            deadline, timeout, "agent brain step",
                        )
                else:
                    response = _call_brain_with_retry(
                        lambda call_timeout: brain.complete(
                            AGENT_SYSTEM,
                            user_prompt,
                            max_tokens=_agent_brain_max_tokens(),
                            timeout=call_timeout,
                        ),
                        deadline, timeout, "agent brain step",
                    )
            except Exception as brain_exc:
                fallback_text = _best_text_for_extraction(obs, dom)
                if _is_brain_failure(brain_exc) and fallback_text.strip():
                    message = f"{type(brain_exc).__name__}: {brain_exc}"
                    detail = _fallback_summary(url or last_url, fallback_text, message)
                    down_reason = _content_down_reason(fallback_text, response_status)
                    if down_reason:
                        failure = _content_down_failure_message(url or last_url, down_reason)
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "captured content is unavailable page",
                            "step": step_no,
                            "reason": down_reason,
                            "error": message,
                        })
                        STATE.update(error=failure, error_detail=message, status="error")
                        _persist_agent_result(url or last_url, "failed", error=failure,
                                              error_detail=message, source_link=source_link)
                        return
                    STATE.push_trace({
                        "ts": _now(),
                        "message": "agent brain failed; failing crawl with captured DOM text",
                        "step": step_no,
                        "error": message,
                    })
                    failure = f"Local brain summarization failed: {message}"
                    STATE.update(error=failure, error_detail=detail, status="error")
                    _persist_agent_result(
                        url or last_url,
                        "failed",
                        error=failure,
                        error_detail=detail,
                        source_link=source_link,
                    )
                    return
                raise

            STATE.update(response=response)
            if not (knowledge_text and not playbook_attempted_here and not _is_wayback_url(url)):
                action = _parse_action(response)
                STATE.push_trace({
                    "ts": _now(),
                    "message": "agent chose action",
                    "step": step_no,
                    "response": response,
                    "action": action,
                })
            elif action.get("action") == "ask":
                message = "Domain playbook planner could not produce a concrete browser action."
                STATE.update(error=message, response=response, status="error")
                _persist_agent_result(last_url or url, "failed", error=message,
                                      error_detail=response, source_link=source_link)
                return

            if action["action"] == "done":
                answer = _clean_final_summary(action.get("answer", response))
                if action.get("inferred"):
                    STATE.push_trace({
                        "ts": _now(),
                        "message": "treated plain summary as done",
                        "step": step_no,
                    })
                extraction_text = _best_text_for_extraction(obs, dom)
                unavailable_reason = _summary_reports_unavailable(answer)
                if not unavailable_reason and (last_url or url) == "about:blank":
                    unavailable_reason = "about:blank"
                if unavailable_reason:
                    if (last_url or url) and _is_wayback_url(last_url or url):
                        archive_tried_urls.add(last_url or url)
                        source_url = _wayback_original_url(last_url or url) or _wayback_original_url(initial_url)
                        next_archive = _wayback_snapshot_url(
                            source_url,
                            deadline,
                            exclude_urls=archive_tried_urls,
                        )
                        if next_archive:
                            archive_tried_urls.add(next_archive)
                            STATE.update(
                                status="navigating",
                                note="archived page empty; trying another snapshot",
                                url=next_archive,
                                host=_host_of(next_archive),
                                response=answer,
                            )
                            STATE.push_trace({
                                "ts": _now(),
                                "message": "summary reports archived page empty; trying another snapshot",
                                "step": step_no,
                                "reason": unavailable_reason,
                                "url": last_url or url,
                                "archive_url": next_archive,
                            })
                            engine, tab = _navigate_with_tab_recovery(
                                engine,
                                tab,
                                next_archive,
                                timeout,
                                deadline,
                                reason="empty archive retry",
                                step_no=step_no,
                            )
                            engine, tab, bh = _wait_for_load_with_tab_recovery(
                                engine,
                                tab,
                                next_archive,
                                timeout,
                                deadline,
                                reason="empty archive retry",
                                step_no=step_no,
                            )
                            _job_sleep(1, deadline)
                            continue
                    failure = _content_down_failure_message(
                        last_url or url,
                        f"summary reports unavailable content: {unavailable_reason}",
                    )
                    STATE.update(error=failure, response=answer, status="error")
                    _persist_agent_result(
                        last_url or url,
                        "failed",
                        error=failure,
                        error_detail=answer,
                        source_link=source_link,
                    )
                    return
                verification = _verify_summary_candidate(
                    brain,
                    instruction,
                    last_url or url,
                    obs,
                    answer,
                    extraction_text,
                    deadline,
                    timeout,
                    step_no,
                )
                verifier_declined = verification is not None and not verification["accepted"]
                if verification is not None and verification["accepted"]:
                    STATE.update(status="done", response=answer)
                    _persist_agent_result(last_url, "ok", summary=answer,
                                          source_link=source_link)
                    _auto_learn_domain(
                        brain, last_host, last_url, "agent-done",
                        STATE.current.trace, summary=answer,
                    )
                    return
                if _summary_needs_more_context(answer, obs, forced_context_scrolls):
                    forced_context_scrolls += 1
                    amount = _context_scroll_amount(obs, fraction=0.32, cap=420)
                    STATE.update(
                        note=(
                            "summary verifier declined; scrolling for more context"
                            if verifier_declined else
                            "summary too thin; scrolling for more context"
                        ),
                        response=answer,
                    )
                    STATE.push_trace({
                        "ts": _now(),
                        "message": (
                            "summary verifier declined; scrolling for context"
                            if verifier_declined else
                            "summary too thin; scrolling for context"
                        ),
                        "step": step_no,
                        "forced_scrolls": forced_context_scrolls,
                        "amount": amount,
                        "summary_words": len(re.findall(r"\w+", answer)),
                    })
                    if not _safe_scroll(engine, tab, amount, method="js", step_no=step_no):
                        STATE.update(note="summary too thin; scroll failed, retrying observation")
                    else:
                        _job_sleep(0.75, deadline)
                    continue
                if verifier_declined or _summary_is_barebones(answer, obs):
                    if _word_count(extraction_text) >= 80:
                        STATE.update(
                            note=(
                                "summary verifier declined; refining from extracted content"
                                if verifier_declined else
                                "refining summary from extracted content"
                            )
                        )
                        STATE.push_trace({
                            "ts": _now(),
                            "message": (
                                "summary verifier declined; using extracted content"
                                if verifier_declined else
                                "summary still barebones; using extracted content"
                            ),
                            "step": step_no,
                            "summary_words": _word_count(answer),
                            "content_words": _word_count(extraction_text),
                            "extractor": (obs.get("extracted") or {}).get("source", ""),
                        })
                        try:
                            refined = _call_brain_with_retry(
                                lambda call_timeout: brain.summarize_text(
                                    f"agent-extracted-summary-{step_no}",
                                    _extraction_summary_prompt(
                                        instruction, last_url or url, obs,
                                        answer, extraction_text,
                                    ),
                                    max_chars=_agent_brain_max_chars(),
                                    max_tokens=_agent_brain_max_tokens(),
                                    timeout=call_timeout,
                                ),
                                deadline, timeout, "agent extracted summary",
                            )
                            refined = _clean_final_summary(refined)
                            refined_verification = _verify_summary_candidate(
                                brain,
                                instruction,
                                last_url or url,
                                obs,
                                refined,
                                extraction_text,
                                deadline,
                                timeout,
                                step_no,
                            )
                            refined_declined = (
                                refined_verification is not None
                                and not refined_verification["accepted"]
                            )
                            if refined_declined or (
                                refined_verification is None
                                and _summary_is_barebones(refined, obs)
                            ):
                                STATE.push_trace({
                                    "ts": _now(),
                                    "message": "extracted-content summary remained barebones",
                                    "step": step_no,
                                    "summary_words": _word_count(refined),
                                    "verifier_reason": (
                                        refined_verification.get("reason", "")
                                        if refined_verification else ""
                                    ),
                                })
                                answer = _clean_final_summary(
                                    _fallback_summary(
                                        last_url or url,
                                        extraction_text,
                                        "verifier declined the extracted-content summary",
                                    )
                                )
                            else:
                                answer = refined
                        except Exception as refine_exc:
                            message = f"{type(refine_exc).__name__}: {refine_exc}"
                            STATE.push_trace({
                                "ts": _now(),
                                "message": "extracted-content summary failed",
                                "step": step_no,
                                "error": message,
                            })
                            if _is_brain_failure(refine_exc):
                                detail = _fallback_summary(last_url or url, extraction_text, message)
                                failure = f"Local brain summarization failed: {message}"
                                STATE.update(error=failure, error_detail=detail, status="error")
                                _persist_agent_result(
                                    last_url or url,
                                    "failed",
                                    error=failure,
                                    error_detail=detail,
                                    source_link=source_link,
                                )
                                return
                            answer = _clean_final_summary(
                                _fallback_summary(last_url or url, extraction_text, message)
                            )
                unavailable_reason = _summary_reports_unavailable(answer)
                if unavailable_reason:
                    failure = _content_down_failure_message(
                        last_url or url,
                        f"summary reports unavailable content: {unavailable_reason}",
                    )
                    STATE.update(error=failure, response=answer, status="error")
                    _persist_agent_result(
                        last_url or url,
                        "failed",
                        error=failure,
                        error_detail=answer,
                        source_link=source_link,
                    )
                    return
                STATE.update(status="done",
                             response=answer)
                _persist_agent_result(last_url, "ok", summary=answer,
                                      source_link=source_link)
                _auto_learn_domain(
                    brain, last_host, last_url, "agent-done",
                    STATE.current.trace, summary=answer,
                )
                return

            if action["action"] == "navigate":
                mark_playbook_attempt(last_url or url, knowledge)
                STATE.update(note=f"navigating to {action['url']}")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "navigating",
                    "step": step_no,
                    "url": action["url"],
                })
                engine.navigate(tab, action["url"])
                bh.wait_for_load(timeout=_remaining_seconds(deadline, 15))
                continue

            if action["action"] == "archive":
                if verification_reason and not _is_wayback_url(last_url or url):
                    verify_key = last_url or url or initial_url
                    wait_count = verification_waits.get(verify_key, 0)
                    click_count = verification_clicks.get(verify_key, 0)
                    if wait_count >= 1 and click_count < 2:
                        verification_clicks[verify_key] = click_count + 1
                        STATE.update(note="verification page detected; clicking before archive fallback")
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "archive deferred for transient verification; clicking",
                            "step": step_no,
                            "reason": verification_reason,
                            "waits": wait_count,
                            "attempt": click_count + 1,
                            "url": verify_key,
                        })
                        if _try_verification_click(engine, tab, step_no=step_no):
                            _job_sleep(8, deadline)
                        else:
                            _job_sleep(5, deadline)
                        continue
                    if wait_count >= 4 and click_count >= 2:
                        message = "Human verification required; automatic verification attempts did not clear the page."
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "archive blocked by persistent verification page",
                            "step": step_no,
                            "reason": verification_reason,
                            "waits": wait_count,
                            "clicks": click_count,
                            "url": verify_key,
                        })
                        STATE.update(error=message, status="error")
                        _persist_agent_result(last_url or url, "failed", error=message,
                                              source_link=source_link)
                        _auto_learn_domain(
                            brain, last_host, last_url, "agent-verification-required",
                            STATE.current.trace, error=message,
                        )
                        return
                    if wait_count < 4:
                        verification_waits[verify_key] = wait_count + 1
                        STATE.update(note="verification page detected; waiting before archive fallback")
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "archive deferred for transient verification; waiting",
                            "step": step_no,
                            "reason": verification_reason,
                            "attempt": wait_count + 1,
                            "url": verify_key,
                        })
                        _job_sleep(5, deadline)
                        continue
                archive_key = _normal_url_key(_wayback_original_url(last_url or url) or (last_url or url))
                if (
                    knowledge.get("playbooks")
                    and archive_key not in playbook_attempted
                    and not _is_wayback_url(last_url or url)
                ):
                    block_count = playbook_archive_blocks.get(archive_key, 0) + 1
                    playbook_archive_blocks[archive_key] = block_count
                    if block_count >= 3:
                        message = (
                            "Domain playbook was available but the agent repeatedly chose archive "
                            "before attempting it."
                        )
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "archive blocked; domain playbook not attempted",
                            "step": step_no,
                            "url": last_url or url,
                            "blocks": block_count,
                        })
                        STATE.update(error=message, status="error")
                        _persist_agent_result(last_url or url, "failed", error=message,
                                              source_link=source_link)
                        return
                    STATE.update(
                        note="archive blocked; domain playbook must be attempted first",
                        response=response,
                    )
                    STATE.push_trace({
                        "ts": _now(),
                        "message": "archive deferred; domain playbook not attempted",
                        "step": step_no,
                        "url": last_url or url,
                        "blocks": block_count,
                        "playbooks": len(knowledge.get("playbooks", [])),
                    })
                    _job_sleep(0.5, deadline)
                    continue
                source_url, archive_url = _archive_action_url(
                    action.get("target", "current"),
                    last_url or url,
                    initial_url,
                    deadline,
                    exclude_urls=archive_tried_urls | {last_url or url},
                )
                if not archive_url:
                    STATE.push_trace({
                        "ts": _now(),
                        "message": "archive tool failed",
                        "step": step_no,
                        "target": action.get("target", "current"),
                        "url": source_url,
                    })
                    STATE.update(note="archive snapshot unavailable")
                    continue
                archive_tried_urls.add(archive_url)
                STATE.update(
                    status="navigating",
                    note="opening Internet Archive snapshot",
                    url=archive_url,
                    host=_host_of(archive_url),
                )
                STATE.push_trace({
                    "ts": _now(),
                    "message": "archive tool navigating",
                    "step": step_no,
                    "target": action.get("target", "current"),
                    "url": source_url,
                    "archive_url": archive_url,
                })
                engine, tab = _navigate_with_tab_recovery(
                    engine,
                    tab,
                    archive_url,
                    timeout,
                    deadline,
                    reason="agent archive tool",
                    step_no=step_no,
                )
                engine, tab, bh = _wait_for_load_with_tab_recovery(
                    engine,
                    tab,
                    archive_url,
                    timeout,
                    deadline,
                    reason="agent archive tool",
                    step_no=step_no,
                )
                _job_sleep(1, deadline)
                continue

            if action["action"] == "click":
                mark_playbook_attempt(last_url or url, knowledge)
                STATE.update(note=f"clicking {action['by']}:{action.get('value', action.get('x', ''))}")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "clicking",
                    "step": step_no,
                    "by": action["by"],
                    "value": action.get("value"),
                    "x": action.get("x"),
                    "y": action.get("y"),
                })
                if action["by"] == "xy":
                    _click_xy(engine, tab, action["x"], action["y"])
                    _job_sleep(2, deadline)
                    continue
                if action["by"] == "text":
                    clicked = engine.evaluate(tab, _CLICK_TEXT_JS(action["value"]))
                    if isinstance(clicked, dict) and clicked.get("href"):
                        STATE.update(note=f"navigating to {clicked['href']}")
                        engine.navigate(tab, clicked["href"])
                        bh.wait_for_load(timeout=_remaining_seconds(deadline, 15))
                    elif isinstance(clicked, dict) and clicked.get("ok"):
                        _job_sleep(3, deadline)
                    else:
                        STATE.update(error=f"element not found: {action['value']}",
                                     status="error")
                        return
                else:
                    engine.evaluate(tab, f"""
                        (()=>{{const el=document.querySelector({json.dumps(action['value'])});
                        if(!el)return false;
                        const h=el.getAttribute('href');
                        if(h){{location.href=h;return'nav:'+h;}}
                        el.click();return true;}})()
                    """)
                    _job_sleep(3, deadline)
                continue

            if action["action"] == "type":
                mark_playbook_attempt(last_url or url, knowledge)
                label = (
                    f"{action.get('x')},{action.get('y')}"
                    if action.get("by") == "xy" else
                    action.get("selector", "focused element")
                )
                STATE.update(note=f"typing into {label}")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "typing",
                    "step": step_no,
                    "by": action.get("by", "css"),
                    "selector": action.get("selector", ""),
                    "x": action.get("x"),
                    "y": action.get("y"),
                })
                if action.get("by") == "xy":
                    _click_xy(engine, tab, action["x"], action["y"])
                    _job_sleep(0.2, deadline)
                    _type_text(engine, tab, action["text"])
                elif action.get("by") == "text":
                    _type_text(engine, tab, action["text"])
                else:
                    ok = engine.evaluate(tab, _TYPE_JS(action["selector"], action["text"]))
                    if not ok:
                        STATE.update(error=f"input not found: {action['selector']}", status="error")
                        return
                _job_sleep(0.5, deadline)
                continue

            if action["action"] == "press":
                mark_playbook_attempt(last_url or url, knowledge)
                STATE.update(note=f"pressing {action['key']}")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "pressing key",
                    "step": step_no,
                    "key": action["key"],
                })
                _press_key(engine, tab, action["key"])
                _job_sleep(1, deadline)
                continue

            if action["action"] == "autofill" and action.get("provider") == "bitwarden":
                mark_playbook_attempt(last_url or url, knowledge)
                STATE.update(note="requesting Bitwarden autofill")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "requesting Bitwarden autofill",
                    "step": step_no,
                    "provider": "bitwarden",
                })
                _bitwarden_autofill(engine, tab)
                _job_sleep(2, deadline)
                continue

            if action["action"] == "login" and action.get("provider") == "bitwarden":
                mark_playbook_attempt(last_url or url, knowledge)
                selector = action.get("selector") or "auto"
                STATE.update(note="requesting Bitwarden autofill and login submit")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "requesting Bitwarden login",
                    "step": step_no,
                    "provider": "bitwarden",
                    "selector": selector,
                })
                _bitwarden_autofill(engine, tab)
                _job_sleep(2, deadline)
                result = engine.evaluate(tab, _LOGIN_SUBMIT_JS(selector))
                if not isinstance(result, dict) or not result.get("ok"):
                    STATE.update(error=f"login form not found: {selector}",
                                 status="error")
                    return
                STATE.push_trace({
                    "ts": _now(),
                    "message": "submitted login form",
                    "step": step_no,
                    "method": result.get("method", ""),
                })
                _job_sleep(3, deadline)
                continue

            if action["action"] == "submit":
                mark_playbook_attempt(last_url or url, knowledge)
                STATE.update(note=f"submitting {action['selector']}")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "submitting",
                    "step": step_no,
                    "selector": action["selector"],
                })
                ok = engine.evaluate(tab, _SUBMIT_JS(action["selector"]))
                if not ok:
                    STATE.update(error=f"form not found: {action['selector']}",
                                 status="error")
                    return
                _job_sleep(2, deadline)
                continue

            if action["action"] == "scroll":
                mark_playbook_attempt(last_url or url, knowledge)
                key = "PageUp" if action["direction"] == "up" else "PageDown"
                amount = action.get("amount")
                STATE.push_trace({
                    "ts": _now(),
                    "message": "scrolling",
                    "step": step_no,
                    "direction": action["direction"],
                    "key": key,
                    "amount": amount or "",
                })
                if amount:
                    delta = amount if action["direction"] == "down" else -amount
                    ok = _safe_scroll(engine, tab, delta, method="js", step_no=step_no)
                else:
                    ok = _safe_press_key(engine, tab, key, step_no=step_no)
                    if not ok:
                        fallback = _context_scroll_amount(obs, fraction=0.55, cap=760)
                        delta = fallback if action["direction"] == "down" else -fallback
                        ok = _safe_scroll(engine, tab, delta, method="js", step_no=step_no)
                if ok:
                    _job_sleep(0.75, deadline)
                continue

            if action["action"] == "wait":
                if verification_reason and not _is_wayback_url(last_url or url):
                    verify_key = last_url or url or initial_url
                    wait_count = verification_waits.get(verify_key, 0)
                    click_count = verification_clicks.get(verify_key, 0)
                    if wait_count >= 1 and click_count < 2:
                        verification_clicks[verify_key] = click_count + 1
                        STATE.update(note="verification page detected; clicking verification control")
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "verification wait converted to click attempt",
                            "step": step_no,
                            "reason": verification_reason,
                            "waits": wait_count,
                            "attempt": click_count + 1,
                            "url": verify_key,
                        })
                        if _try_verification_click(engine, tab, step_no=step_no):
                            _job_sleep(8, deadline)
                        else:
                            _job_sleep(5, deadline)
                        continue
                    if wait_count >= 4 and click_count >= 2:
                        message = "Human verification required; automatic verification attempts did not clear the page."
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "verification page persisted",
                            "step": step_no,
                            "reason": verification_reason,
                            "waits": wait_count,
                            "clicks": click_count,
                            "url": verify_key,
                        })
                        STATE.update(error=message, status="error")
                        _persist_agent_result(last_url or url, "failed", error=message,
                                              source_link=source_link)
                        _auto_learn_domain(
                            brain, last_host, last_url, "agent-verification-required",
                            STATE.current.trace, error=message,
                        )
                        return
                    verification_waits[verify_key] = wait_count + 1
                STATE.push_trace({
                    "ts": _now(),
                    "message": "waiting",
                    "step": step_no,
                    "seconds": action["seconds"],
                })
                _job_sleep(action["seconds"], deadline)
                continue

            if action["action"] == "screenshot":
                continue

            if action["action"] == "back":
                STATE.update(note="going back")
                engine.evaluate(tab, "history.back()")
                _job_sleep(2, deadline)
                continue

            if action["action"] == "reload":
                if verification_reason and not _is_wayback_url(last_url or url):
                    verify_key = last_url or url or initial_url
                    wait_count = verification_waits.get(verify_key, 0)
                    verification_waits[verify_key] = wait_count + 1
                    STATE.update(note="verification page detected; reload blocked, waiting")
                    STATE.push_trace({
                        "ts": _now(),
                        "message": "reload blocked for verification page",
                        "step": step_no,
                        "reason": verification_reason,
                        "attempt": wait_count + 1,
                        "url": verify_key,
                    })
                    if wait_count >= 1:
                        _try_verification_click(engine, tab, step_no=step_no)
                        _job_sleep(8, deadline)
                    else:
                        _job_sleep(5, deadline)
                    continue
                STATE.update(note="reloading")
                engine.cdp(tab, "Page.reload", {"ignoreCache": False})
                _job_sleep(2, deadline)
                continue

            if action["action"] == "extract":
                from ..perception.policy import perceive
                p = perceive(engine, tab, url)
                if p.mode == "vision":
                    summary = _call_brain_with_retry(
                        lambda call_timeout: brain.summarize_image(
                            url, p.image_png, timeout=call_timeout
                        ),
                        deadline, timeout, "agent image extract",
                    )
                else:
                    summary = _call_brain_with_retry(
                        lambda call_timeout: brain.summarize_text(
                            url, p.text or "", timeout=call_timeout
                        ),
                        deadline, timeout, "agent text extract",
                    )
                STATE.update(mode=p.mode, dom_text=p.text or "", response=summary,
                             status="done")
                _persist_agent_result(url or last_url, "ok", summary=summary,
                                      source_link=source_link)
                _auto_learn_domain(brain, last_host, last_url, "agent-extract",
                                   STATE.current.trace, summary=summary)
                return

            # unparseable action — ask again
            STATE.update(note=f"unparseable response, retrying...")

        STATE.update(status="done",
                     response=f"Reached max steps ({MAX_STEPS}) without completing.")
        _persist_agent_result(
            last_url,
            "failed",
            error=f"Reached max steps ({MAX_STEPS}) without completing.",
            source_link=source_link,
        )
        _auto_learn_domain(
            brain, last_host, last_url, "agent-max-steps", STATE.current.trace,
            summary=f"Reached max steps ({MAX_STEPS}) without completing.",
        )

    except Exception as e:
        message = f"{type(e).__name__}: {e}"
        STATE.push_trace({
            "ts": _now(),
            "message": "agent failed",
            "error": message,
        })
        STATE.update(error=message, error_detail=_error_detail(e),
                     status="error")
        _persist_agent_result(last_url, "failed", error=message,
                              error_detail=_error_detail(e),
                              source_link=source_link)
        _auto_learn_domain(brain, last_host, last_url, "agent-error",
                           STATE.current.trace, error=message)
    finally:
        if tab is not None:
            _close_job_tab(engine, tab, reason="agent job finished")
        _set_active_tab()


def _run_agentic_job(instruction: str, initial_url: str = "", timeout_seconds: float | None = None) -> None:
    if not _RUN_LOCK.acquire(blocking=False):
        STATE.current = CrawlStep(status="error", error="Another crawl job is already running.")
        return
    try:
        _CANCEL_EVENT.clear()
        STATE.progress = {"done": 0, "total": 0}
        _do_agentic(instruction, initial_url=initial_url, timeout_seconds=timeout_seconds)
    finally:
        _CANCEL_EVENT.clear()
        _RUN_LOCK.release()


_FIND_URL_JS = lambda text: f"""
(() => {{
    const anchors = document.querySelectorAll('a, button, [role=button], input[type=submit]');
    const target = {json.dumps(text.lower())};
    for (const el of anchors) {{
        const t = (el.textContent || '').toLowerCase().trim();
        const v = (el.getAttribute('value') || '').toLowerCase().trim();
        const alt = (el.getAttribute('alt') || '').toLowerCase().trim();
        if (t.includes(target) || v.includes(target) || alt.includes(target)) {{
            return el.getAttribute('href') || '';
        }}
    }}
    return null;
}})()
"""

_CLICK_TEXT_JS = lambda text: f"""
(() => {{
    const visible = (el) => {{
        const r = el.getBoundingClientRect();
        const s = getComputedStyle(el);
        return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
    }};
    const target = {json.dumps(text.lower())};
    const candidates = document.querySelectorAll(
        'a, button, [role=button], [role=checkbox], input[type=submit], input[type=button], input[type=checkbox], label'
    );
    for (const el of candidates) {{
        if (!visible(el)) continue;
        const haystack = [
            el.textContent || '',
            el.getAttribute('aria-label') || '',
            el.getAttribute('title') || '',
            el.getAttribute('alt') || '',
            el.getAttribute('value') || '',
            el.getAttribute('for') || '',
            el.id || '',
            el.className || ''
        ].join(' ').toLowerCase().replace(/\\s+/g, ' ').trim();
        if (!haystack.includes(target)) continue;
        const href = el.href || el.getAttribute('href') || '';
        if (href) return {{ok: true, href, method: 'href'}};
        el.click();
        return {{ok: true, href: '', method: 'click'}};
    }}
    return {{ok: false}};
}})()
"""

_TYPE_JS = lambda selector, text: f"""
(() => {{
    const el = document.querySelector({json.dumps(selector)});
    if (!el) return false;
    el.focus();
    el.value = {json.dumps(text)};
    el.dispatchEvent(new Event('input', {{bubbles: true}}));
    el.dispatchEvent(new Event('change', {{bubbles: true}}));
    return true;
}})()
"""

_SUBMIT_JS = lambda selector: f"""
(() => {{
    const el = document.querySelector({json.dumps(selector)});
    if (!el) return false;
    const form = el.tagName === 'FORM' ? el : el.closest('form');
    if (!form) return false;
    if (form.requestSubmit) form.requestSubmit();
    else form.submit();
    return true;
}})()
"""

_LOGIN_SUBMIT_JS = lambda selector: f"""
(() => {{
    const selector = {json.dumps(selector)};
    const visible = (el) => {{
        if (!el) return false;
        const r = el.getBoundingClientRect();
        const s = getComputedStyle(el);
        return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
    }};
    let el = null;
    if (selector && selector !== 'auto') {{
        try {{
            el = document.querySelector(selector);
        }} catch (err) {{
            return {{ok: false, method: 'invalid-selector'}};
        }}
    }}
    if (!el) el = document.querySelector('input[type=password]');
    let form = el ? (el.tagName === 'FORM' ? el : el.closest('form')) : null;
    if (!form) {{
        form = [...document.querySelectorAll('form')].find((candidate) =>
            visible(candidate) && candidate.querySelector('input[type=password], input[type=email], input[name*="user" i], input[name*="login" i]')
        );
    }}
    if (form) {{
        if (form.requestSubmit) form.requestSubmit();
        else form.submit();
        return {{ok: true, method: 'form'}};
    }}
    const submit = [...document.querySelectorAll('button, input[type=submit], [role=button]')]
        .find((candidate) => {{
            if (!visible(candidate)) return false;
            const text = (candidate.innerText || candidate.value || candidate.getAttribute('aria-label') || '').toLowerCase();
            return candidate.type === 'submit' || /log\\s*in|sign\\s*in|continue|submit/.test(text);
        }});
    if (!submit) return {{ok: false, method: 'none'}};
    submit.click();
    return {{ok: true, method: 'button'}};
}})()
"""


@app.post("/agent")
async def agent(req: AgentRequest):
    from fastapi.responses import JSONResponse
    if not req.instruction:
        return JSONResponse({"error": "Empty instruction"}, status_code=400)
    try:
        memory_result = _save_operator_memory_command(req.instruction)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    if memory_result:
        return memory_result
    if _RUN_LOCK.locked():
        return JSONResponse({"error": "Another crawl job is already running."}, status_code=409)
    initial_url = _extract_url(req.instruction)
    if _is_direct_crawl_instruction(req.instruction, initial_url):
        link = {
            "id": _safe_link_id(initial_url),
            "url": initial_url,
            "reason": "agent",
            "sub_reason": "prompt",
            "prompt": req.instruction,
        }
        threading.Thread(
            target=_run_crawl_job,
            args=(link, req.timeout_seconds),
            daemon=True,
        ).start()
        return {
            "status": "accepted",
            "instruction": req.instruction,
            "initial_url": initial_url,
            "mode": "direct_crawl",
        }
    threading.Thread(
        target=_run_agentic_job,
        args=(req.instruction, initial_url, req.timeout_seconds),
        daemon=True,
    ).start()
    return {"status": "accepted", "instruction": req.instruction, "initial_url": initial_url}


@app.post("/crawl")
async def crawl(req: CrawlRequest):
    from fastapi.responses import JSONResponse
    if _RUN_LOCK.locked():
        return JSONResponse({"error": "Another crawl job is already running."}, status_code=409)
    if not req.url or not req.url.startswith(("http://", "https://")):
        return JSONResponse({"error": "Invalid URL"}, status_code=400)
    link = {
        "id": req.id or _safe_link_id(req.url),
        "url": req.url,
        "reason": req.reason,
        "sub_reason": req.sub_reason,
    }
    threading.Thread(
        target=_run_crawl_job,
        args=(link, req.timeout_seconds),
        daemon=True,
    ).start()
    return {"status": "accepted", "url": req.url}


@app.post("/prompt")
async def prompt(req: PromptRequest):
    if not req.text:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Empty prompt"}, status_code=400)
    threading.Thread(target=_do_prompt, args=(req.text,), daemon=True).start()
    return {"status": "accepted", "text": req.text}


@app.on_event("shutdown")
def _shutdown():
    services.shutdown_autostarted()


def start(host: str = "0.0.0.0", port: int = 8766):
    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="info")
