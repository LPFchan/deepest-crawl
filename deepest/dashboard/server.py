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
import queue as _queue
import random
import re
import threading
import time
import traceback
import uuid
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable
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
IGNORED_PATH = ROOT / "outputs" / "ignored.json"
DOMAIN_KNOWLEDGE_DIR = ROOT / "outputs" / "domain-knowledge"
_RUN_LOCK = threading.Lock()
_FILE_LOCK = threading.Lock()
_SERVICE_LOCK = threading.Lock()
_SCREENSHOT_LOCK = threading.Lock()
_ACTIVE_JOB_LOCK = threading.Lock()
# Rebound by the job worker to the currently-running job's own cancel event, so a
# Stop only ever cancels the job it was aimed at (never leaks to the next job).
_CANCEL_EVENT = threading.Event()


# --- Job queue ---------------------------------------------------------------
# A single persistent worker thread drains _PENDING one job at a time. The
# worker IS the serializer; _RUN_LOCK (still acquired inside the runners) remains
# the canonical "a browser job is running" signal that /screenshot and
# /services/brain read.


@dataclass
class QueuedJob:
    kind: str  # "crawl" | "fetch-all" | "agentic"
    run: Callable[[], None]
    id: str = ""  # link id, or "crawl-all" for a batch
    url: str = ""
    label: str = ""
    cancel: threading.Event = field(default_factory=threading.Event)
    inbox: "_queue.Queue[str]" = field(default_factory=_queue.Queue)  # live operator instructions
    steerable: threading.Event = field(default_factory=threading.Event)  # set while a drainable loop runs
    member_ids: list = field(default_factory=list)  # per-URL link ids for a batch (fetch-all)
    member_items: list = field(default_factory=list)  # [{id,url}] per batch member, for the queue panel
    done_ids: set = field(default_factory=set)  # batch members already crawled (guarded by _QUEUE_LOCK)
    uid: str = field(default_factory=lambda: uuid.uuid4().hex)  # unique reorder/remove handle


_QUEUE_LOCK = threading.Lock()  # non-reentrant: never call _notify_queue/_refresh_queue_view while held
_QUEUE_COND = threading.Condition(_QUEUE_LOCK)  # wakes the worker when _PENDING/_UNPAUSED change
_PENDING: list[QueuedJob] = []  # the queue (single source of truth), guarded by _QUEUE_LOCK
_CURRENT_JOB: "QueuedJob | None" = None  # guarded by _QUEUE_LOCK
_UNPAUSED = threading.Event()
_UNPAUSED.set()  # set = running; clear() = paused
# Lock-free snapshot read by _serialize on the SSE hot path; rebound atomically.
_QUEUE_VIEW: dict = {"depth": 0, "ids": [], "items": [], "paused": False,
                     "running": "", "running_item": None}


def _refresh_queue_view() -> None:
    """Rebuild the lock-free queue snapshot. Caller must NOT hold _QUEUE_LOCK."""
    global _QUEUE_VIEW
    with _QUEUE_LOCK:
        # Per-URL queued ids: a batch contributes its member link ids (not the
        # synthetic "crawl-all"), a single its own id, so the sidebar can mark each
        # selected row "queued". The running batch contributes its not-yet-crawled
        # members; the in-flight row is excluded client-side via jobLinkId.
        ids: list = []
        for j in _PENDING:
            ids.extend(j.member_ids or [j.id])
        cur = _CURRENT_JOB
        # A canceling batch contributes nothing to the queued count: its remaining
        # members will not be crawled, so /jobs/clear and /jobs/cancel zero the count
        # immediately instead of after the job finishes unwinding.
        running_members: list = []
        if cur is not None and cur.kind == "fetch-all" and not cur.cancel.is_set():
            ids.extend(m for m in cur.member_ids if m not in cur.done_ids)
            # Per-URL rows so the queue panel can list (and scroll) a running batch,
            # not just the one in-flight URL.
            running_members = [m for m in cur.member_items if m.get("id") not in cur.done_ids]
        view = {
            "depth": len(_PENDING),
            "ids": ids,
            "items": [{"uid": j.uid, "id": j.id, "url": j.url, "kind": j.kind, "label": j.label}
                      for j in _PENDING],
            "paused": not _UNPAUSED.is_set(),
            "running": cur.id if cur else "",
            "running_item": ({"uid": cur.uid, "id": cur.id, "label": cur.label, "kind": cur.kind}
                             if cur else None),
            "running_members": running_members,
        }
    _QUEUE_VIEW = view  # atomic rebind; readers need no lock


def _notify_queue() -> None:
    """Refresh the snapshot and push it to SSE clients. Caller must NOT hold _QUEUE_LOCK."""
    _refresh_queue_view()
    STATE.update()  # empty update still notifies listeners -> fresh _QUEUE_VIEW reaches clients


def _enqueue(job: QueuedJob) -> str:
    """Append a job to the queue. Returns status.

    Single crawls dedup only against jobs still PENDING (covers rapid double-clicks
    before a job starts). We deliberately do NOT dedup against the currently-running
    job: re-fetching a URL that is running — or stuck in its async cancel/cleanup —
    means "run it again", so it queues and runs after the current one finishes.
    Otherwise a just-cancelled job stays un-retryable until its cleanup completes.
    """
    with _QUEUE_COND:
        if job.kind == "crawl" and any(j.id == job.id for j in _PENDING):
            return "already_queued"
        _PENDING.append(job)
        _QUEUE_COND.notify()  # wake the worker (notify must be inside the cond)
    _notify_queue()
    return "queued"


def _wait_while_paused() -> bool:
    """Block while the queue is paused; stay responsive to cancel.

    Returns False if the current job was canceled during the wait, else True.
    """
    while not _UNPAUSED.wait(0.2):  # returns True once unpaused, False on timeout (still paused)
        if _CANCEL_EVENT.is_set():
            return False
    return not _CANCEL_EVENT.is_set()


def _job_worker() -> None:
    global _CURRENT_JOB, _CANCEL_EVENT
    while True:
        with _QUEUE_COND:
            # Wait for work AND an un-paused queue. wait(0.5) is a load-bearing
            # backstop against a missed notify; pause holds the next job here.
            while not (_PENDING and _UNPAUSED.is_set()):
                _QUEUE_COND.wait(0.5)
            job = _PENDING.pop(0)
            _CURRENT_JOB = job
            _CANCEL_EVENT = job.cancel  # rebind so Stop targets exactly this job
        # NOTE: _refresh_queue_view acquires _QUEUE_LOCK, so it MUST be outside the
        # cond block above (the lock is non-reentrant).
        _refresh_queue_view()
        # Clear the prior job's terminal frame during the engine/tab-setup gap before
        # the runner emits its own "queued" frame. status must be NON-terminal.
        STATE.current = CrawlStep(
            id=job.id,
            link_id=(job.id if job.kind == "crawl" else ""),
            url=job.url,
            status="starting",
            note="Starting…",
        )
        try:
            job.run()
        except Exception as exc:  # never let the worker thread die or the queue freezes
            STATE.update(error=f"{type(exc).__name__}: {exc}",
                         error_detail=_error_detail(exc), status="error")
        finally:
            with _QUEUE_LOCK:
                _CURRENT_JOB = None
            _notify_queue()
_SERVICE_STATE = {"status": "idle", "note": "", "error": ""}
_LINKS_CACHE = {"mtime": None, "data": []}
_IGNORED_CACHE = {"mtime": None, "ids": set(), "hosts": set()}
_RESULTS_CACHE = {"mtime": None, "data": {}}
_DOMAIN_NOTE_COUNT_CACHE: dict[str, tuple[float | None, int]] = {}
_SCREENSHOT_TAB = None
_LAST_SCREENSHOT_BYTES: bytes | None = None
_ACTIVE_JOB = {"engine": None, "tab": None}
_JOB_TABS: set = set()  # every tab id the current job touched, so reconnect-orphaned tabs get closed
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


def _summary_reserve_seconds() -> float:
    """Dedicated wall-clock budget for the final summary, so a step loop that
    exhausted the job deadline can't starve it. Cancellation still uses the real
    job deadline (via _check_job_open), so this only governs the summary timeout."""
    return _env_float("DEEPEST_SUMMARY_RESERVE_SECONDS", 60.0)


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


_WAYBACK_JS_REDIRECT_RE = re.compile(
    r"""(?:window\.)?(?:top\.|document\.)?location(?:\.href|\.assign|\.replace)?\s*"""
    r"""(?:=|\()\s*["'](https?://[^"']*?/web/\d+[^"']*)["']""",
    re.I,
)
_WAYBACK_META_REFRESH_RE = re.compile(
    r"""<meta[^>]+http-equiv=["']?refresh["']?[^>]*content=["'][^"']*?url=([^"'>]+)""",
    re.I,
)
_WAYBACK_INTERSTITIAL_RE = re.compile(
    r"Got an HTTP\s+3\d\d\s+response at crawl time", re.I,
)
_WAYBACK_IMPATIENT_RE = re.compile(
    r"""class=["']impatient["'][^>]*>\s*<a[^>]+href=["']([^"']+)["']""", re.I,
)


def _wayback_redirect_target(engine, tab, current_url: str) -> str:
    """Return the archived snapshot a redirecting archived page points to.

    Old domains commonly archive as a redirect to a successor domain — a server
    301 captured as Wayback's 'Got an HTTP 3xx response at crawl time'
    interstitial, a meta refresh, or a `window.location` JS redirect that Wayback
    has already rewritten to a `/web/<ts>/<url>` target. We follow only redirects
    that leave the current archived document's host, so same-page asset/self
    references never trigger a hop. Empty string when there is no such redirect.
    """
    if not _is_wayback_url(current_url):
        return ""
    try:
        html = engine.html(tab) or ""
    except Exception:
        return ""
    if not html:
        return ""
    current_host = _host_of(_wayback_original_url(current_url))
    candidates: list[str] = []
    if _WAYBACK_INTERSTITIAL_RE.search(html):
        impatient = _WAYBACK_IMPATIENT_RE.search(html)
        if impatient:
            candidates.append(impatient.group(1))
    candidates.extend(_WAYBACK_JS_REDIRECT_RE.findall(html))
    candidates.extend(_WAYBACK_META_REFRESH_RE.findall(html))
    for raw in candidates:
        target = (raw or "").strip()
        if target.startswith("/web/"):
            target = "https://web.archive.org" + target
        if not _is_wayback_url(target):
            continue
        target_host = _host_of(_wayback_original_url(target))
        if not target_host or target_host == current_host:
            continue
        return target
    return ""


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


def _load_ignored() -> tuple[set, set]:
    """Return (ignored_link_ids, ignored_hosts), cached by file mtime."""
    mtime = _mtime(IGNORED_PATH)
    if _IGNORED_CACHE["mtime"] != mtime:
        data = _load_json(IGNORED_PATH, {}) or {}
        _IGNORED_CACHE.update({
            "mtime": mtime,
            "ids": {str(x) for x in data.get("ids", [])},
            "hosts": {str(x).lower() for x in data.get("hosts", [])},
        })
    return _IGNORED_CACHE["ids"], _IGNORED_CACHE["hosts"]


def _add_ignored(link_id: str | None = None, host: str | None = None) -> None:
    data = _load_json(IGNORED_PATH, {}) or {}
    ids = {str(x) for x in data.get("ids", [])}
    hosts = {str(x).lower() for x in data.get("hosts", [])}
    if link_id:
        ids.add(str(link_id))
    if host:
        hosts.add(str(host).lower())
    _write_json(IGNORED_PATH, {"ids": sorted(ids), "hosts": sorted(hosts)})
    _IGNORED_CACHE["mtime"] = None  # force reload on next read


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


def _operator_memory_command(text: str) -> tuple[str, str]:
    """Parse an operator domain-memory command -> (domain_hint, memory_text).

    Returns ("", "") when the text is not a memory command. The optional target
    domain is given as a slot right after the keyword ("add to domain memory for
    anandtech: ...", bare or full host) or as a trailing host-like "for <host.tld>".
    A trailing BARE word is NOT treated as a domain, so memory text like
    "...wait for the banner" is preserved verbatim.
    """
    normalized = (text or "").strip()
    match = re.match(
        r"(?is)^\s*add\s+(?:to\s+)?domain\s+(?:memory|playbook|note)"
        r"(?:\s+for\s+([A-Za-z0-9.\-]+))?\s*:?\s*(.+)$",
        normalized,
    )
    if not match:
        return "", ""
    domain = (match.group(1) or "").strip()
    memory_text = match.group(2).strip()
    if not domain:
        trail = re.search(r"(?is)\bfor\s+([A-Za-z0-9.\-]+\.[A-Za-z]{2,})\s*$", memory_text)
        if trail:
            domain = trail.group(1).strip()
            memory_text = memory_text[: trail.start()].strip()
    return domain, memory_text


def _normalize_domain_hint(x: str) -> str:
    """Turn a domain hint ("anandtech", "anandtech.com", "https://x/y") into a host.
    Does NOT use _host_of (which returns "" for scheme-less input)."""
    h = (x or "").strip().lower()
    h = re.sub(r"^[a-z]+://", "", h)
    h = h.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    h = h.lstrip("@").strip(".")
    if h.startswith("www."):
        h = h[4:]
    if not h:
        return ""
    if "." not in h:
        h = h + ".com"
    return h


def _save_operator_memory_command(text: str) -> dict:
    domain_hint, memory_text = _operator_memory_command(text)
    if not memory_text:
        return {}
    explicit_url = _extract_url(memory_text) or _extract_url(text)
    if domain_hint:
        host = _normalize_domain_hint(domain_hint)
        url = ""
    elif explicit_url:
        host = _host_of(_wayback_original_url(explicit_url))
        url = explicit_url
    else:
        host, url = _domain_memory_target_from_state()
    if not host:
        raise ValueError(
            "No domain to attach memory to. Add 'for <host>' (e.g. 'add to domain "
            "memory for anandtech.com: always use the internet archive')."
        )
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


def _drain_operator_inbox(operator_updates: list, step_no: int) -> None:
    """Absorb any live operator instructions delivered to the running job into the
    accumulator (kept to the last 8, deduped). Safe: queue.Queue is thread-safe."""
    job = _CURRENT_JOB
    if job is None:
        return
    while True:
        try:
            msg = job.inbox.get_nowait()
        except _queue.Empty:
            break
        msg = (msg or "").strip()
        if msg and msg not in operator_updates:
            operator_updates.append(msg)
            del operator_updates[:-8]
            STATE.push_trace({
                "ts": _now(),
                "message": "operator instruction received",
                "step": step_no,
                "text": msg[:200],
            })


def _operator_override_block(operator_updates: list) -> str:
    if not operator_updates:
        return ""
    lines = "\n".join(f"- {u[:300]}" for u in operator_updates)
    return ("Operator override (supersedes the original instruction — follow now):\n"
            f"{lines}\n\n")


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
        tid = getattr(tab, "id", None)
        if tid is not None:
            _JOB_TABS.add(tid)  # track for end-of-job cleanup (covers reconnect orphans)


def _bring_tab_to_front(engine, tab) -> None:
    """Foreground the job's tab in the host Chrome so a launched job is actually
    visible instead of opening behind other tabs. Best-effort; never fails the job."""
    if engine is None or tab is None:
        return
    try:
        engine.cdp(tab, "Page.bringToFront")
    except Exception as exc:
        STATE.push_trace({"ts": _now(), "message": "bring tab to front failed",
                          "error": f"{type(exc).__name__}: {exc}"})


def _close_job_tabs(engine, *, reason: str = "job cleanup",
                    before_ids: set | None = None) -> None:
    """Close every tab the job touched (current + any orphaned by mid-job reconnects),
    then clear the active-tab/tracking state. Uses the current (healthy) session: a tab
    it still owns closes directly; one orphaned by a dead session is re-adopted then closed.

    When before_ids (the pre-job tab snapshot) is given, also sweep any tab that appeared
    during the job but never reached _JOB_TABS: healthy-opener popups, spawned tabs whose
    adoption failed, and transient double-opens. The job owns the browser for its run, so a
    tab absent from the snapshot is a job-spawned orphan no matter how it was opened."""
    with _ACTIVE_JOB_LOCK:
        ids = set(_JOB_TABS)
        _JOB_TABS.clear()
        _ACTIVE_JOB["engine"] = None
        _ACTIVE_JOB["tab"] = None
    if engine is None:
        return
    if before_ids is not None:
        try:
            ids.update(tid for tid in _snapshot_tab_ids(engine) if tid not in before_ids)
        except Exception:
            pass
    closed = 0
    for tid in ids:
        th = TabHandle(id=tid, backend=getattr(engine, "name", "chrome"))
        try:
            engine.cdp(th, "Page.close", {})
            closed += 1
            continue
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
        if any(g in err for g in _TAB_GONE_ERRORS):
            continue  # already gone
        try:
            engine.claim_tab(int(tid))  # orphaned by a reconnect: adopt into this session
            engine.cdp(th, "Page.close", {})
            closed += 1
        except Exception as exc2:
            STATE.push_trace({"ts": _now(), "message": "residual tab close failed",
                              "reason": reason, "tab": tid,
                              "error": f"{type(exc2).__name__}: {exc2}"})
    if ids:
        STATE.push_trace({"ts": _now(), "message": "closed job tabs",
                          "reason": reason, "count": closed, "tracked": len(ids)})


def _close_active_tab() -> None:
    with _ACTIVE_JOB_LOCK:
        engine = _ACTIVE_JOB.get("engine")
        tab = _ACTIVE_JOB.get("tab")
    if engine is not None and tab is not None:
        # Called from the cancel endpoint while the worker thread may still be
        # using the engine, so do NOT reconnect the transport here; the job's own
        # finally does the authoritative (reconnect-capable) close.
        _close_job_tab(engine, tab, reason="active job cleanup", allow_reconnect=False)


# Errors meaning the tab is genuinely gone (nothing left to close).
_TAB_GONE_ERRORS = ("No tab with id", "No target with given id", "Target closed")
# Errors meaning our debugger session lost the tab, but the tab is likely still
# open in Chrome — reconnect and re-adopt it to actually close it.
_TAB_SESSION_LOST_ERRORS = (
    "not part of browser session",
    "Debugger unattached",
    "unexpected response id",
)


def _close_job_tab(engine, tab, *, reason: str = "job cleanup",
                   allow_reconnect: bool = True) -> None:
    if engine is None or tab is None:
        return
    tab_id = getattr(tab, "id", "")
    try:
        engine.cdp(tab, "Page.close", {})
        STATE.push_trace({"ts": _now(), "message": "closed browser tab",
                          "reason": reason, "tab": tab_id})
        return
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    if any(item in error for item in _TAB_GONE_ERRORS):
        STATE.push_trace({"ts": _now(), "message": "browser tab already closed",
                          "reason": reason, "tab": tab_id, "error": error})
        return
    session_lost = any(item in error for item in _TAB_SESSION_LOST_ERRORS)
    if not (session_lost and allow_reconnect):
        STATE.push_trace({"ts": _now(), "message": "browser tab close failed",
                          "reason": reason, "tab": tab_id, "error": error})
        return
    # The tab is still open but our session can't reach it (the shared debugger
    # detached). Reconnect to a fresh session, re-adopt the tab, and close it.
    try:
        engine = _reconnect_engine()
        engine.claim_tab(tab_id)
        engine.cdp(tab, "Page.close", {})
        STATE.push_trace({"ts": _now(), "message": "closed browser tab after reconnect",
                          "reason": reason, "tab": tab_id})
    except Exception as exc2:
        error2 = f"{type(exc2).__name__}: {exc2}"
        gone = any(item in error2 for item in _TAB_GONE_ERRORS)
        STATE.push_trace({
            "ts": _now(),
            "message": "browser tab already closed" if gone else "browser tab close failed after reconnect",
            "reason": reason,
            "tab": tab_id,
            "error": error2,
            "first_error": error,
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

        if tab is None:
            return None  # nothing live to capture; don't spawn a throwaway about:blank tab
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
        if _is_cdp_timeout(exc):
            # The page never let the navigation settle; a transport reconnect won't
            # unstick it and re-issuing the same navigate just burns another 10s.
            raise
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


def _is_cdp_timeout(exc: Exception) -> bool:
    """The OBU bridge aborted a CDP command at its 10s budget — the page never let
    the command run (navigation never settled / main thread wedged). Distinct from a
    session detach: reconnecting and retrying the SAME command just burns another 10s."""
    message = f"{type(exc).__name__}: {exc}"
    return "Timed out" in message and "waiting for CDP command" in message


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


def _is_real_content_url(url: str) -> bool:
    """True for an http(s) page URL, false for about:blank/chrome:// and friends."""
    return (url or "").strip().lower().startswith(("http://", "https://"))


# http(s) URLs that still can't be driven via the debugger (e.g. Chrome's built-in
# PDF viewer), so a spawned tab pointing at one must not be adopted.
_NON_ATTACHABLE_SUFFIXES = (".pdf",)


def _is_attachable_content_url(url: str) -> bool:
    u = (url or "").strip().lower()
    path = u.split("?", 1)[0].split("#", 1)[0]
    return _is_real_content_url(u) and not path.endswith(_NON_ATTACHABLE_SUFFIXES)


def _snapshot_tab_ids(engine) -> set:
    """Capture the engine's known tab ids so newly spawned tabs can be detected."""
    ids: set = set()
    for getter in (getattr(engine, "session_tabs", None), getattr(engine, "user_tabs", None)):
        if getter is None:
            continue
        try:
            for tab in getter() or []:
                tid = tab.get("id") if isinstance(tab, dict) else None
                if tid is not None:
                    ids.add(tid)
        except Exception:
            continue
    return ids


def _find_spawned_content_tab(engine, opener_tab, before_ids: set, original_url: str):
    """Find a tab opened after `before_ids` that holds real content.

    Generic across providers: any link that opens its destination in a new tab
    (window.open / target=_blank / app deep links) leaves the opener stranded;
    the spawned tab is the one that appeared since the snapshot and carries an
    http(s) URL. Restricting to newly appeared ids avoids hijacking the user's
    own pre-existing tabs.
    """
    opener_id = getattr(opener_tab, "id", None)
    original_host = _host_of(original_url)
    seen: set = set()
    candidates: list[tuple] = []
    for getter in (getattr(engine, "user_tabs", None), getattr(engine, "session_tabs", None)):
        if getter is None:
            continue
        try:
            tabs = getter() or []
        except Exception:
            continue
        for tab in tabs:
            if not isinstance(tab, dict):
                continue
            tid = tab.get("id")
            if tid is None or tid in seen:
                continue
            seen.add(tid)
            if tid in before_ids or tid == opener_id:
                continue
            tab_url = str(tab.get("url", ""))
            if not _is_attachable_content_url(tab_url):
                continue
            candidates.append((tid, tab_url))
    if not candidates:
        return None, ""
    # Prefer a destination on a different host than the original (short) link.
    candidates.sort(key=lambda c: _host_of(c[1]) == original_host)
    return candidates[0]


def _adopt_spawned_content_tab(engine, tab, url: str, before_ids: set,
                               timeout_seconds: float, deadline: float, *,
                               reason: str, step_no: int | None = None):
    """If the opener is stranded at about:blank/detached, switch to the tab the
    link spawned. Returns the (engine, tab) to keep crawling; unchanged when the
    opener is healthy or no spawned content tab is found."""
    if before_ids is None:
        return engine, tab
    opener_url = ""
    stranded = False
    try:
        opener_url = engine.current_url(tab) or ""
        stranded = not _is_real_content_url(opener_url)
    except Exception as exc:
        if not _is_tab_session_error(exc):
            raise
        stranded = True
    if not stranded:
        return engine, tab
    spawned_id, spawned_url = _find_spawned_content_tab(engine, tab, before_ids, url)
    if spawned_id is None:
        return engine, tab
    new_tab = None
    try:
        new_tab = engine.claim_tab(int(spawned_id))
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        # "Cannot attach to this target" can be transient (the spawned tab is still
        # settling). Retry once after a brief, cancel-aware pause before giving up.
        if "Cannot attach" in error:
            _cancelable_sleep(0.5)
            try:
                new_tab = engine.claim_tab(int(spawned_id))
            except Exception as exc2:
                error = f"{type(exc2).__name__}: {exc2}"
    if new_tab is None:
        non_attachable = "Cannot attach" in error
        trace = {
            "ts": _now(),
            "message": ("spawned tab is not attachable; staying on opener"
                        if non_attachable else "failed to adopt spawned content tab"),
            "reason": reason,
            "spawned_tab": spawned_id,
            "spawned_url": spawned_url,
            "error": error,
        }
        if step_no is not None:
            trace["step"] = step_no
        STATE.push_trace(trace)
        return engine, tab
    trace = {
        "ts": _now(),
        "message": "adopted tab spawned by new-tab redirect link",
        "reason": reason,
        "opener_url": opener_url or "about:blank",
        "spawned_tab": spawned_id,
        "spawned_url": spawned_url,
    }
    if step_no is not None:
        trace["step"] = step_no
    STATE.push_trace(trace)
    _close_job_tab(engine, tab, reason="stranded opener after new-tab redirect")
    _set_active_tab(engine, new_tab)
    _bring_tab_to_front(engine, new_tab)
    return engine, new_tab


def _seam_with_recovery(engine, tab, fn, *, replay: bool):
    """Run fn(engine, tab) at the CDP seam, recovering from a tab-session detach.

    On a session-lost error (Debugger unattached / not part of browser session) the
    shared debugger lost the tab while it is still open. Reconnect to a fresh session
    (same as _reconnect_engine elsewhere), re-adopt the tab, and re-run fn ONLY if
    replay=True. Never replay non-idempotent input: a delivered-but-unacked click must
    not double-fire, so callers dispatching Input.* pass replay=False and re-observe.
    Returns (engine, tab, result, recovered). Non-detach / 'tab gone' errors propagate.
    """
    try:
        return engine, tab, fn(engine, tab), False
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if not any(s in error for s in _TAB_SESSION_LOST_ERRORS):
            raise
    STATE.push_trace({
        "ts": _now(),
        "message": "recovering tab session at CDP seam",
        "error": error,
        "replay": replay,
    })
    engine = _reconnect_engine()
    tab = engine.claim_tab(getattr(tab, "id", tab))
    _set_active_tab(engine, tab)
    return engine, tab, (fn(engine, tab) if replay else None), True


def _perceive_with_tab_recovery(engine, tab, url: str, timeout_seconds: float,
                                deadline: float, *, reason: str,
                                before_tab_ids: set | None = None,
                                step_no: int | None = None):
    """Perceive the page, recovering when the tab session detaches.

    Two failure modes are handled:
    - A link opens its destination in a NEW tab and leaves the opener at
      about:blank (e.g. spotify.link and other app/deep links). The opener is
      adopted onto the spawned content tab via `before_tab_ids`.
    - A freshly opened/redirecting tab momentarily reports 'Debugger unattached'
      because the CDP session is still attached to the pre-redirect target; the
      tab is reclaimed and the load re-settled before retrying.
    """
    from ..perception.policy import perceive
    engine, tab = _adopt_spawned_content_tab(
        engine, tab, url, before_tab_ids, timeout_seconds, deadline,
        reason=reason, step_no=step_no,
    )
    try:
        return engine, tab, perceive(engine, tab, url)
    except Exception as exc:
        if not _is_tab_session_error(exc):
            raise
        trace = {
            "ts": _now(),
            "message": "tab detached during perception; reconnecting",
            "reason": reason,
            "tab": getattr(tab, "id", ""),
            "url": url,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if step_no is not None:
            trace["step"] = step_no
        STATE.push_trace(trace)
    # A detach often means the content moved to a spawned tab; try adoption first.
    adopted_engine, adopted_tab = _adopt_spawned_content_tab(
        engine, tab, url, before_tab_ids, timeout_seconds, deadline,
        reason=reason, step_no=step_no,
    )
    if getattr(adopted_tab, "id", None) != getattr(tab, "id", None):
        return adopted_engine, adopted_tab, perceive(adopted_engine, adopted_tab, url)
    engine, tab = _navigate_with_tab_recovery(
        engine, tab, url, timeout_seconds, deadline, reason=reason, step_no=step_no,
    )
    engine, tab, _bh = _wait_for_load_with_tab_recovery(
        engine, tab, url, timeout_seconds, deadline, reason=reason, step_no=step_no,
    )
    _set_active_tab(engine, tab)
    return engine, tab, perceive(engine, tab, url)


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
        "queue": _QUEUE_VIEW,
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
    host: str = ""
    confirm_count: int | None = None
    delay_seconds: float | None = None
    jitter_seconds: float | None = None
    timeout_seconds: float | None = None


class RefreshLinksRequest(BaseModel):
    db: str | None = None
    statuses: list[str] | None = None
    limit: int | None = None


class QueueOrderRequest(BaseModel):
    uids: list[str] = []


class QueueItemRequest(BaseModel):
    uid: str


class IgnoreRequest(BaseModel):
    id: str | None = None
    host: str | None = None


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
    needs_reconnect = False
    pre_open_tab_ids = None
    try:
        _check_job_open(deadline)
        _trace("connecting to real Chrome transport")
        if link.get("prompt"):
            _trace("direct crawl from agent prompt", instruction=link.get("prompt"))
        engine = _ensure_engine(_remaining_seconds(deadline, timeout))
        STATE.update(status="navigating")
        _trace("opening tab", url=url)
        pre_open_tab_ids = _snapshot_tab_ids(engine)
        engine, tab = _new_tab_with_retry(
            engine,
            url,
            timeout,
            deadline,
        )
        _set_active_tab(engine, tab)
        _bring_tab_to_front(engine, tab)
        _job_sleep(2, deadline)
        engine, tab = _adopt_spawned_content_tab(
            engine, tab, url, pre_open_tab_ids, timeout, deadline,
            reason="initial open",
        )
        _publish_screenshot(engine, tab, "initial browser screenshot")

        _check_job_open(deadline)
        _trace("perceiving page")
        engine, tab, p = _perceive_with_tab_recovery(
            engine, tab, url, timeout, deadline, reason="initial perception",
            before_tab_ids=pre_open_tab_ids,
        )
        _set_active_tab(engine, tab)
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
                reuse=(engine, tab, pre_open_tab_ids),
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
                engine, tab, p = _perceive_with_tab_recovery(
                    engine, tab, url, timeout, deadline, reason="direct archive fallback",
                )
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
                reuse=(engine, tab, pre_open_tab_ids),
            )
            return
        _check_job_open(deadline)
        STATE.update(mode=p.mode, dom_text=p.text or "", status="summarizing")
        _trace("summarizing page", mode=p.mode, perception_note=p.note)
        STATE.update(prompt=f"Summarize: {url}", status="thinking")
        _publish_screenshot(engine, tab, "pre-summary browser screenshot")
        try:
            _trace("checking local brain")
            # Reserve a dedicated budget for the final summary (the step loop above
            # may have spent the job deadline). _check_job_open(deadline) above already
            # honored cancellation on the real job deadline.
            summary_deadline = time.monotonic() + _summary_reserve_seconds()
            brain = _ensure_brain(_remaining_seconds(summary_deadline, timeout))
            if p.mode == "vision" and p.image_png:
                summary = _call_brain_with_retry(
                    lambda call_timeout: brain.summarize_image(
                        url, p.image_png, timeout=call_timeout
                    ),
                    summary_deadline, timeout, "image summary",
                )
            else:
                summary = _call_brain_with_retry(
                    lambda call_timeout: brain.summarize_text(
                        url, p.text or "", timeout=call_timeout
                    ),
                    summary_deadline, timeout, "text summary",
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
        cdp_timeout = _is_cdp_timeout(e)
        if cdp_timeout:
            message = ("Page unresponsive: a CDP command timed out after 10s "
                       "(navigation never settled or the page main thread is blocked).")
        _trace("crawl failed", error=message)
        STATE.update(error=message, error_detail=detail, status="error")
        _persist_result(_result_from_state(link, "failed", error=message,
                                           error_detail=detail))
        # A transient page-stall timeout is not a reusable domain fact — keep it out of
        # the per-domain notes AND the brain auto-learn (the failure is still recorded in
        # the crawl result). Otherwise every stuck page pollutes the domain knowledge.
        if not cdp_timeout:
            _append_domain_note(host, "crawl-error", message, url, STATE.current.trace)
            _auto_learn_domain(brain, host, url, "failed", STATE.current.trace, error=message)
        needs_reconnect = _is_tab_session_error(e)
    finally:
        if needs_reconnect:
            # The shared Chrome debugger session detached; reconnect first so the
            # cleanup below runs on a healthy session (and the next job does too).
            try:
                engine = _reconnect_engine()
                STATE.push_trace({
                    "ts": _now(),
                    "message": "reconnected Chrome transport after tab-session error",
                })
            except Exception as reconnect_exc:
                STATE.push_trace({
                    "ts": _now(),
                    "message": "Chrome transport reconnect failed",
                    "error": f"{type(reconnect_exc).__name__}: {reconnect_exc}",
                })
        # Close the current tab AND any orphaned by mid-job reconnects or untracked spawns.
        _close_job_tabs(engine, reason="crawl job finished", before_ids=pre_open_tab_ids)


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
        # cancel lifecycle is per-job (worker rebinds _CANCEL_EVENT); no clear() here
        STATE.progress = {"done": 0, "total": 1}
        _do_crawl(link, timeout_seconds=timeout_seconds)
        STATE.progress = {"done": 1, "total": 1}
    finally:
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
        # cancel lifecycle is per-job (worker rebinds _CANCEL_EVENT); no clear() here
        STATE.progress = {"done": 0, "total": len(links)}
        for i, link in enumerate(links, 1):
            if not _UNPAUSED.is_set():
                STATE.current = CrawlStep(
                    id="crawl-all",
                    status="waiting",
                    note=f"Paused after {i - 1} of {len(links)} URLs.",
                )
                STATE.progress = {"done": i - 1, "total": len(links)}
            # Block here while paused (queue Pause halts the batch between URLs),
            # staying responsive to Stop. False => canceled during the wait.
            if not _wait_while_paused():
                STATE.current = CrawlStep(
                    id="crawl-all",
                    status="canceled",
                    note=f"Canceled after {i - 1} of {len(links)} URLs.",
                )
                STATE.progress = {"done": i - 1, "total": len(links)}
                return
            _do_crawl(dict(link), timeout_seconds=timeout)
            STATE.progress = {"done": i, "total": len(links)}
            # Drop this member from the sidebar "queued" set. Mutate under the lock,
            # then notify in a SEPARATE scope (_QUEUE_LOCK is non-reentrant).
            link_id = str(link.get("id") or "")
            if link_id:
                with _QUEUE_LOCK:
                    if _CURRENT_JOB is not None and _CURRENT_JOB.kind == "fetch-all":
                        _CURRENT_JOB.done_ids.add(link_id)
                _notify_queue()
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
    host: str = "",
    results: dict[str, dict] | None = None,
) -> list[dict]:
    q_norm = q.strip().lower()
    reason_norm = reason.strip().lower()
    status_norm = _normalize_status_filter(status)
    host_norm = host.strip().lower()
    ignored_ids, ignored_hosts = _load_ignored()

    def keep(link: dict) -> bool:
        if str(link.get("id")) in ignored_ids:
            return False
        if ignored_hosts and _host_of(link.get("url", "")).lower() in ignored_hosts:
            return False
        if host_norm and _host_of(link.get("url", "")).lower() != host_norm:
            return False
        if q_norm and q_norm not in str(link.get("url", "")).lower():
            return False
        if reason_norm and reason_norm != str(link.get("reason", "")).lower():
            return False
        if status_norm and status_norm != _link_status(link, results):
            return False
        return True

    return [link for link in links if keep(link)]


@app.get("/links/hosts")
async def link_hosts(limit: int = 200):
    """Distinct hosts (excluding ignored), sorted by link count, for the host filter."""
    ignored_ids, ignored_hosts = _load_ignored()
    counts: dict[str, int] = {}
    for link in _load_links():
        if str(link.get("id")) in ignored_ids:
            continue
        h = _host_of(link.get("url", ""))
        if not h or h.lower() in ignored_hosts:
            continue
        counts[h] = counts.get(h, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[: max(1, limit)]
    return _json_safe({"hosts": [{"host": h, "count": c} for h, c in ranked]})


@app.get("/links")
async def links(limit: int = 100, offset: int = 0, q: str = "", reason: str = "",
                status: str = "", host: str = ""):
    limit = max(1, min(limit, 50000))
    offset = max(0, offset)
    all_links = _load_links()
    results = _load_results_by_id()
    filtered = _filter_links(all_links, q=q, reason=reason, status=status, host=host, results=results)
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
    all_links = _load_links()
    results = _load_results_by_id()
    by_id = {str(l.get("id")): l for l in all_links}
    if req.ids:
        selected = _filter_links([by_id[i] for i in req.ids if i in by_id],
                                 q=req.q, reason=req.reason, status=req.status,
                                 host=req.host, results=results)
    else:
        selected = _filter_links(all_links, q=req.q, reason=req.reason,
                                 status=req.status, host=req.host, results=results)
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
    status = _enqueue(QueuedJob(
        kind="fetch-all",
        run=lambda: _run_fetch_all_job(selected, delay, jitter, timeout),
        id="crawl-all",
        label=f"{len(selected)} URLs",
        member_ids=[str(l.get("id")) for l in selected if l.get("id")],
        member_items=[{"id": str(l.get("id")), "url": l.get("url", "")}
                      for l in selected if l.get("id")],
    ))
    return {
        "status": status,
        "count": len(selected),
        "delay_seconds": delay,
        "jitter_seconds": jitter,
        "timeout_seconds": timeout,
    }


@app.post("/jobs/cancel")
async def cancel_job():
    with _QUEUE_LOCK:
        job = _CURRENT_JOB
    if job is not None:
        job.cancel.set()  # targets exactly this job; never leaks to the next
        _close_active_tab()
        STATE.update(status="canceling", note="Cancel requested; stopping after current URL.")
        return {"status": "canceling"}
    STATE.current = CrawlStep(id="cancel", status="idle", note="No running crawl job.")
    return {"status": "idle"}


@app.post("/jobs/pause")
async def pause_queue():
    _UNPAUSED.clear()
    _notify_queue()
    return {"status": "paused"}


@app.post("/jobs/resume")
async def resume_queue():
    with _QUEUE_COND:
        _UNPAUSED.set()
        _QUEUE_COND.notify()  # wake the worker that's parked on the pause predicate
    _notify_queue()
    return {"status": "running"}


@app.post("/jobs/reorder")
async def reorder_queue(req: QueueOrderRequest):
    order = {uid: i for i, uid in enumerate(req.uids or [])}
    with _QUEUE_LOCK:
        # stable: known uids take the requested order; any not listed keep their
        # relative position at the end. Never touches _CURRENT_JOB (already popped).
        _PENDING.sort(key=lambda j: order.get(j.uid, len(order) + 1))
    _notify_queue()
    return {"status": "reordered"}


@app.post("/jobs/remove")
async def remove_queued(req: QueueItemRequest):
    with _QUEUE_LOCK:
        before = len(_PENDING)
        _PENDING[:] = [j for j in _PENDING if j.uid != req.uid]
        removed = before - len(_PENDING)
    _notify_queue()
    return {"status": "removed", "count": removed}


@app.post("/jobs/clear")
async def clear_queue():
    with _QUEUE_LOCK:
        _PENDING.clear()
        job = _CURRENT_JOB
    if job is not None:
        job.cancel.set()  # mirror /jobs/cancel: per-job token, no _CANCEL_EVENT clear
        _close_active_tab()
        STATE.update(status="canceling", note="Queue cleared; stopping current job.")
    _notify_queue()
    return {"status": "cleared"}


@app.post("/links/ignore")
async def ignore_link(req: IgnoreRequest):
    from fastapi.responses import JSONResponse
    if not req.id and not req.host:
        return JSONResponse({"error": "Provide an id or a host to ignore."}, status_code=400)
    _add_ignored(link_id=req.id, host=req.host)
    return {"status": "ignored", "id": req.id, "host": req.host}


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
    "Do not use the archive tool on a live page that has already loaded readable "
    "content (for example after a verification check cleared): read and summarize "
    "that live content instead. Only fall back to the archive when the live page is "
    "genuinely 404, blocked, empty, or content-down. "
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
    "reload\n"
    "url|setparam|key|value (set or replace one query parameter on the current URL)\n"
    "url|delparam|key (remove one query parameter from the current URL)\n"
    "url|path|/new/path (replace only the path of the current URL)\n\n"
    "Archive actions are forbidden. Done/final summaries are forbidden. "
    "When a playbook only changes the query string or path of the current URL, "
    "prefer the url| actions so the new URL is assembled deterministically instead "
    "of writing it out by hand. Use navigate|URL only for a genuinely different URL."
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


def _is_float(value: str) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


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
        if action == "url" and len(parts) >= 3:
            op = parts[1].lower()
            if op == "path":
                return {"action": "url", "op": "path", "path": "|".join(parts[2:])}
            if op == "delparam":
                return {"action": "url", "op": "delparam", "key": parts[2]}
            if op == "setparam" and len(parts) >= 4:
                return {
                    "action": "url",
                    "op": "setparam",
                    "key": parts[2],
                    "value": "|".join(parts[3:]),
                }
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
            # Tolerate click|x|y when the model drops the literal 'xy' token.
            if parts[1] not in ("text", "css") and _is_float(parts[1]) and _is_float(parts[2]):
                return {"action": "click", "by": "xy", "x": float(parts[1]), "y": float(parts[2])}
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


def _vision_xy_to_css(engine, tab, x: float, y: float) -> tuple[float, float]:
    """Map the vision model's click coordinate to a CSS click coordinate.

    Holo grounds in a 0-1000 normalized space over the screenshot it sees, so the
    raw values are NOT pixels: css = coord / 1000 * viewport_dim. The image->viewport
    scale cancels out of the normalization, so only the live CSS viewport size is
    needed (no screenshot measurement, correct at any device pixel ratio).

    Measured: the model returned (274, 419) for a checkbox truly at ~(376, 331) on a
    1372x790 viewport -> 274/1000*1372=376, 419/1000*790=331. Exact.
    """
    vp = _viewport_size(engine, tab) or {}
    w = float(vp.get("w") or 0.0)
    h = float(vp.get("h") or 0.0)
    cx = (x / 1000.0 * w) if w else x
    cy = (y / 1000.0 * h) if h else y
    if w:
        cx = min(max(0.0, cx), w - 1)
    if h:
        cy = min(max(0.0, cy), h - 1)
    STATE.push_trace({
        "ts": _now(),
        "message": "click coordinate mapping",
        "raw": [round(x, 1), round(y, 1)],
        "viewport": [round(w, 1), round(h, 1)],
        "css": [round(cx, 1), round(cy, 1)],
    })
    return cx, cy


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


def _apply_url_rewrite(current_url: str, action: dict) -> str:
    """Deterministically rewrite the current URL from a structured playbook url action.

    Keeps query-string and path assembly out of the model's hands so playbook URL
    changes are well-formed regardless of how the planner phrases them. Returns an
    empty string when the action cannot produce a valid absolute URL.
    """
    if not current_url:
        return ""
    try:
        parsed = urlparse(current_url)
    except ValueError:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    op = action.get("op")
    if op == "path":
        new_path = (action.get("path") or "").strip()
        if not new_path:
            return ""
        if not new_path.startswith("/"):
            new_path = "/" + new_path
        parsed = parsed._replace(path=new_path)
    elif op in {"setparam", "delparam"}:
        key = (action.get("key") or "").strip()
        if not key:
            return ""
        params = [
            (k, v)
            for k, v in parse_qsl(parsed.query, keep_blank_values=True)
            if k != key
        ]
        if op == "setparam":
            params.append((key, action.get("value") or ""))
        parsed = parsed._replace(query=urlencode(params))
    else:
        return ""
    return urlunparse(parsed)


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
    if action.get("action") == "url":
        rewritten = _apply_url_rewrite(url, action)
        if rewritten and rewritten != url:
            STATE.push_trace({
                "ts": _now(),
                "message": "domain playbook planner rewrote url",
                "step": step_no,
                "from": url,
                "to": rewritten,
                "op": action.get("op"),
            })
            action = {"action": "navigate", "url": rewritten}
        else:
            STATE.push_trace({
                "ts": _now(),
                "message": "domain playbook url rewrite produced no change",
                "step": step_no,
                "response": response,
                "action": action,
            })
            action = {"action": "ask", "raw": response}
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
                source_link: dict | None = None, reuse: tuple | None = None):
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
    needs_reconnect = False
    pre_open_tab_ids = None
    last_url = ""
    last_host = ""
    last_dom = ""
    archive_tried_urls: set[str] = set()
    verification_waits: dict[str, int] = {}
    verification_clicks: dict[str, int] = {}
    playbook_archive_blocks: dict[str, int] = {}
    playbook_attempted: set[str] = set()
    wayback_redirects_followed: set[str] = set()
    wayback_redirect_hops = 0
    consecutive_waits = 0
    consecutive_scrolls = 0
    forced_context_scrolls = 0

    def mark_playbook_attempt(current_url: str, current_knowledge: dict) -> None:
        if current_knowledge.get("playbooks") and current_url and not _is_wayback_url(current_url):
            playbook_attempted.add(_normal_url_key(_wayback_original_url(current_url) or current_url))

    operator_updates: list[str] = []
    try:
        _check_job_open(deadline)
        if reuse is not None:
            # Continue on the tab the caller already opened and navigated (e.g. _do_crawl's
            # agentic fallback) instead of opening a second tab to the same URL.
            engine, tab, pre_open_tab_ids = reuse
            _set_active_tab(engine, tab)
            _bring_tab_to_front(engine, tab)
            engine.cdp(tab, "Page.enable")
            bh = engine.activate(tab)
        else:
            engine = _ensure_engine(_remaining_seconds(deadline, timeout))
            pre_open_tab_ids = _snapshot_tab_ids(engine)
            engine, tab = _new_tab_with_retry(engine, None, timeout, deadline)
            _set_active_tab(engine, tab)
            _bring_tab_to_front(engine, tab)
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
                engine, tab = _adopt_spawned_content_tab(
                    engine, tab, initial_url, pre_open_tab_ids, timeout, deadline,
                    reason="initial agent navigation", step_no=0,
                )
                _set_active_tab(engine, tab)

        MAX_STEPS = 20
        if _CURRENT_JOB is not None:
            _CURRENT_JOB.steerable.set()  # this loop can absorb live operator instructions
        for step_no in range(1, MAX_STEPS + 1):
            _check_job_open(deadline)
            _drain_operator_inbox(operator_updates, step_no)
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
                # Non-DOM signal: when the page yields almost no readable text it is
                # likely a wall/challenge the agent must handle by looking at it.
                page_unreadable = len((extraction_text or dom or "").strip()) < 200
                down_reason = _content_down_reason(dom, response_status)
                if _is_wayback_url(url) and not verification_reason and wayback_redirect_hops < 4:
                    redirect_target = _wayback_redirect_target(engine, tab, url)
                    if redirect_target and redirect_target not in wayback_redirects_followed:
                        wayback_redirects_followed.add(redirect_target)
                        wayback_redirect_hops += 1
                        STATE.update(
                            status="navigating",
                            url=redirect_target,
                            host=_host_of(redirect_target),
                            note="following archived redirect to successor domain",
                        )
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "following archived redirect to successor domain",
                            "step": step_no,
                            "from": url,
                            "to": redirect_target,
                            "hop": wayback_redirect_hops,
                        })
                        engine, tab = _navigate_with_tab_recovery(
                            engine, tab, redirect_target, timeout, deadline,
                            reason="archived redirect follow", step_no=step_no,
                        )
                        engine, tab, bh = _wait_for_load_with_tab_recovery(
                            engine, tab, redirect_target, timeout, deadline,
                            reason="archived redirect follow", step_no=step_no,
                        )
                        _job_sleep(1, deadline)
                        continue
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
            # Only run the playbook planner when there are real playbooks. Notes are
            # hints for the normal agent — they must not route through the planner,
            # which is forbidden from emitting done and would loop forever (and
            # mark_playbook_attempt only records playbook domains, so a notes-only
            # domain would never clear this gate).
            use_playbook_planner = (
                bool(knowledge.get("playbooks"))
                and not playbook_attempted_here
                and not _is_wayback_url(url)
                and not operator_updates  # an operator override takes the normal path
            )
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
                f"Original instruction: {instruction}\n"
                f"{_operator_override_block(operator_updates)}"
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
                "coordinates from the top-left of the screenshot/browser viewport. "
                "For a verification checkbox such as an 'I am not a robot' / Cloudflare / "
                "Turnstile control, you MUST click it by coordinates with click|xy at the "
                "center of the checkbox. Do not use click|text or click|css for verification "
                "controls: they sit inside a cross-origin frame and only a coordinate click "
                "will reach them."
            )
            try:
                if brain is None:
                    brain = _ensure_brain(_remaining_seconds(deadline, timeout))
                if use_playbook_planner:
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
                elif (verification_reason or page_unreadable) and getattr(brain, "has_vision", lambda: False)():
                    png = _try_screenshot(engine, tab, attempts=1)
                    if png:
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "agent brain step using screenshot vision",
                            "step": step_no,
                            "reason": verification_reason or "page unreadable; using vision",
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
            if not use_playbook_planner:
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

            consecutive_waits = consecutive_waits + 1 if action["action"] == "wait" else 0
            consecutive_scrolls = consecutive_scrolls + 1 if action["action"] == "scroll" else 0

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
                            summary_deadline = time.monotonic() + _summary_reserve_seconds()
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
                                summary_deadline, timeout, "agent extracted summary",
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
                    cx, cy = _vision_xy_to_css(engine, tab, action["x"], action["y"])
                    # replay=False: a detached click must not double-fire; on recovery
                    # we re-adopt the tab and re-observe next step instead of re-clicking.
                    engine, tab, _, _ = _seam_with_recovery(
                        engine, tab, lambda e, t: _click_xy(e, t, cx, cy), replay=False)
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
                        # Missed text match is not fatal. The control is often inside a
                        # cross-origin frame (e.g. an "I am not a robot" checkbox) that
                        # text matching can never reach; keep going so the vision step can
                        # retry with a coordinate click|xy instead of killing the crawl.
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "click text target not found; continuing for vision retry",
                            "step": step_no,
                            "value": action["value"],
                        })
                        STATE.update(note=f"element not found: {action['value']}; will retry visually")
                        _job_sleep(1, deadline)
                        continue
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
                    cx, cy = _vision_xy_to_css(engine, tab, action["x"], action["y"])
                    engine, tab, _, recovered = _seam_with_recovery(
                        engine, tab, lambda e, t: _click_xy(e, t, cx, cy), replay=False)
                    if recovered:
                        # the click's focus is uncertain after a re-adopt; re-observe
                        # rather than type blindly into the wrong element.
                        _job_sleep(0.2, deadline)
                        continue
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
                if consecutive_scrolls >= 6 and not _is_wayback_url(last_url or url):
                    # The agent keeps scrolling without finishing — typically an
                    # image/manga gallery with little new text. The full body text is
                    # already extracted, so summarize what we have and finish instead
                    # of scrolling to the job timeout.
                    content = extraction_text or last_dom or dom
                    STATE.push_trace({
                        "ts": _now(),
                        "message": "scroll stalled; summarizing available content",
                        "step": step_no,
                        "scrolls": consecutive_scrolls,
                        "content_words": _word_count(content),
                    })
                    try:
                        summary_deadline = time.monotonic() + _summary_reserve_seconds()
                        summary = _call_brain_with_retry(
                            lambda call_timeout: brain.summarize_text(
                                f"agent-scroll-stall-{step_no}",
                                _extraction_summary_prompt(
                                    instruction, last_url or url, obs, "", content,
                                ),
                                max_chars=_agent_brain_max_chars(),
                                max_tokens=_agent_brain_max_tokens(),
                                timeout=call_timeout,
                            ),
                            summary_deadline, timeout, "agent scroll-stall summary",
                        )
                        summary = _clean_final_summary(summary)
                    except Exception as exc:
                        STATE.push_trace({
                            "ts": _now(),
                            "message": "scroll-stall summary failed; using fallback",
                            "step": step_no,
                            "error": f"{type(exc).__name__}: {exc}",
                        })
                        summary = _clean_final_summary(
                            _fallback_summary(last_url or url, content, "scroll stalled")
                        )
                    if _summary_reports_unavailable(summary) or _word_count(summary) < 5:
                        message = "Page content could not be summarized after repeated scrolling."
                        STATE.update(error=message, status="error")
                        _persist_agent_result(last_url or url, "failed", error=message,
                                              source_link=source_link)
                        return
                    STATE.update(status="done", response=summary)
                    _persist_agent_result(last_url or url, "ok", summary=summary,
                                          source_link=source_link)
                    _auto_learn_domain(
                        brain, last_host, last_url, "agent-scroll-stall",
                        STATE.current.trace, summary=summary,
                    )
                    return
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
                elif consecutive_waits >= 5 and not _is_wayback_url(last_url or url):
                    # Anti-stall backstop: never wait forever. The vision + click|xy
                    # path is what solves verification walls; if the agent only ever
                    # waits and the page never advances, fail with a clear reason
                    # instead of looping to the job timeout.
                    message = (
                        "Page did not progress after repeated waits; likely an unsolved "
                        "verification or load wall."
                    )
                    STATE.push_trace({
                        "ts": _now(),
                        "message": "giving up after stalled waits",
                        "step": step_no,
                        "waits": consecutive_waits,
                        "url": last_url or url,
                    })
                    STATE.update(error=message, status="error")
                    _persist_agent_result(last_url or url, "failed", error=message,
                                          source_link=source_link)
                    _auto_learn_domain(
                        brain, last_host, last_url, "agent-stalled-waits",
                        STATE.current.trace, error=message,
                    )
                    return
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
                summary_deadline = time.monotonic() + _summary_reserve_seconds()
                if p.mode == "vision":
                    summary = _call_brain_with_retry(
                        lambda call_timeout: brain.summarize_image(
                            url, p.image_png, timeout=call_timeout
                        ),
                        summary_deadline, timeout, "agent image extract",
                    )
                else:
                    summary = _call_brain_with_retry(
                        lambda call_timeout: brain.summarize_text(
                            url, p.text or "", timeout=call_timeout
                        ),
                        summary_deadline, timeout, "agent text extract",
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
        cdp_timeout = _is_cdp_timeout(e)
        if cdp_timeout:
            message = ("Page unresponsive: a CDP command timed out after 10s "
                       "(navigation never settled or the page main thread is blocked).")
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
        # Don't let a transient page-stall timeout become a learned per-domain note.
        if not cdp_timeout:
            _auto_learn_domain(brain, last_host, last_url, "agent-error",
                               STATE.current.trace, error=message)
        needs_reconnect = _is_tab_session_error(e)
    finally:
        if _CURRENT_JOB is not None:
            _drain_operator_inbox(operator_updates, -1)  # catch any last-moment delivery
            with _QUEUE_LOCK:
                _CURRENT_JOB.steerable.clear()
        if needs_reconnect:
            # The shared Chrome debugger session detached; reconnect first so the
            # cleanup below runs on a healthy session (and the next job does too).
            try:
                engine = _reconnect_engine()
                STATE.push_trace({
                    "ts": _now(),
                    "message": "reconnected Chrome transport after tab-session error",
                })
            except Exception as reconnect_exc:
                STATE.push_trace({
                    "ts": _now(),
                    "message": "Chrome transport reconnect failed",
                    "error": f"{type(reconnect_exc).__name__}: {reconnect_exc}",
                })
        # Close the current tab AND any orphaned by mid-job reconnects or untracked spawns.
        _close_job_tabs(engine, reason="agent job finished", before_ids=pre_open_tab_ids)


def _run_agentic_job(instruction: str, initial_url: str = "", timeout_seconds: float | None = None) -> None:
    if not _RUN_LOCK.acquire(blocking=False):
        STATE.current = CrawlStep(status="error", error="Another crawl job is already running.")
        return
    try:
        # cancel lifecycle is per-job (worker rebinds _CANCEL_EVENT); no clear() here
        STATE.progress = {"done": 0, "total": 0}
        _do_agentic(instruction, initial_url=initial_url, timeout_seconds=timeout_seconds)
    finally:
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
    initial_url = _extract_url(req.instruction)
    # Live steering: a URL-less message while a drainable agentic loop is running is
    # delivered to that job instead of queuing a new one. (Pasted URL => new job.)
    if not initial_url:
        with _QUEUE_LOCK:
            job = _CURRENT_JOB
            deliver = (job is not None and job.steerable.is_set()
                       and not job.cancel.is_set())
            if deliver:
                job.inbox.put(req.instruction)
        if deliver:
            STATE.push_trace({
                "ts": _now(),
                "message": "operator instruction delivered to running job",
                "text": req.instruction[:200],
            })
            return {"status": "delivered", "instruction": req.instruction}
    if _is_direct_crawl_instruction(req.instruction, initial_url):
        link = {
            "id": _safe_link_id(initial_url),
            "url": initial_url,
            "reason": "agent",
            "sub_reason": "prompt",
            "prompt": req.instruction,
        }
        status = _enqueue(QueuedJob(
            kind="crawl",
            run=lambda: _run_crawl_job(link, req.timeout_seconds),
            id=link["id"],
            url=initial_url,
        ))
        return {
            "status": status,
            "instruction": req.instruction,
            "initial_url": initial_url,
            "mode": "direct_crawl",
        }
    status = _enqueue(QueuedJob(
        kind="agentic",
        run=lambda: _run_agentic_job(req.instruction, initial_url, req.timeout_seconds),
        id=_safe_link_id(initial_url) if initial_url else "agent",
        url=initial_url,
        label="agent",
    ))
    return {"status": status, "instruction": req.instruction, "initial_url": initial_url}


@app.post("/crawl")
async def crawl(req: CrawlRequest):
    from fastapi.responses import JSONResponse
    if not req.url or not req.url.startswith(("http://", "https://")):
        return JSONResponse({"error": "Invalid URL"}, status_code=400)
    link = {
        "id": req.id or _safe_link_id(req.url),
        "url": req.url,
        "reason": req.reason,
        "sub_reason": req.sub_reason,
    }
    status = _enqueue(QueuedJob(
        kind="crawl",
        run=lambda: _run_crawl_job(link, req.timeout_seconds),
        id=link["id"],
        url=req.url,
    ))
    return {"status": status, "url": req.url}


@app.post("/prompt")
async def prompt(req: PromptRequest):
    if not req.text:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Empty prompt"}, status_code=400)
    threading.Thread(target=_do_prompt, args=(req.text,), daemon=True).start()
    return {"status": "accepted", "text": req.text}


@app.on_event("startup")
def _start_job_worker():
    threading.Thread(target=_job_worker, daemon=True, name="job-worker").start()


@app.on_event("shutdown")
def _shutdown():
    services.shutdown_autostarted()


def start(host: str = "0.0.0.0", port: int = 8766):
    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="info")
