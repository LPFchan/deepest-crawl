const SCREENSHOT_URL = "/screenshot";
const EVENTS_URL = "/events";
const LINKS_URL = "/links";
const SERVICES_URL = "/services";

const state = {
  visibleLinks: [],
  currentUrl: "",
  currentHost: "",
  currentId: "",
  lastPrompt: "",
  lastError: "",
  loadingLinks: false,
  jobActive: false,
  jobLinkId: "",
  queue: { paused: false, depth: 0, ids: [], items: [], running_item: null },
  progress: { done: 0, total: 0 },
  dragging: false,
  queueTotal: 0,
  queueFiltered: 0,
  queuePageSize: 50,
  queueMaxVisible: 250,
  queueOffset: 0,
  queuePaging: false,
  chromeReady: false,
  screenshotBusy: false,
  screenshotObjectUrl: "",
  chatEntries: [],
  traceEntries: [],
  timelineSeq: 0,
  lastResponse: "",
  brainModels: [],
  activeBrainModelId: "",
  selectedIds: new Set(),
  selectionAnchorId: "",
  dragSelection: null,
  suppressNextLinkClick: false,
};

const $ = (id) => document.getElementById(id);
const PANE_WIDTHS_KEY = "deepest:pane-widths:v1";

function esc(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function setBusy(button, busy, text) {
  if (!button) return;
  button.disabled = busy;
  if (text) button.textContent = text;
}

function setJobActive(active) {
  const changed = state.jobActive !== active;
  state.jobActive = active;
  if (!active) state.jobLinkId = "";
  renderNowBar();
  if (changed) renderLinks();
}

// --- Now-playing bottom bar + slide-up queue panel ---
function renderNowBar() {
  const bar = $("now-bar");
  if (!bar) return;
  const q = state.queue || {};
  const queuedN = (q.ids || []).length;
  const show = state.jobActive || queuedN > 0;
  bar.hidden = !show;
  if (!show) { hideQueuePanel(); renderQueuePanel(); return; }
  const pp = $("now-playpause");
  pp.textContent = q.paused ? "▶" : "⏸";
  pp.setAttribute("aria-label", q.paused ? "Resume queue" : "Pause queue");
  const title = state.jobActive
    ? (state.currentUrl || (q.running_item && q.running_item.label) || "crawling…")
    : `${queuedN} queued`;
  $("now-title").textContent = title;
  const p = state.progress || { done: 0, total: 0 };
  let prog = "";
  if (state.jobActive) prog = p.total > 0 ? `${p.done}/${p.total}` : "working…";
  if (queuedN > 0) prog = prog ? `${prog} · ${queuedN} queued` : `${queuedN} queued`;
  $("now-progress").textContent = prog;
  renderQueuePanel();
}

function renderQueuePanel() {
  const list = $("queue-panel-list");
  const panel = $("queue-panel");
  if (!list || !panel || panel.hidden) return;
  if (state.dragging) return;  // don't yank the DOM out from under an active drag
  const q = state.queue || {};
  const rows = [];
  if (state.jobActive && q.running_item) {
    const now = state.currentUrl || q.running_item.label || "crawling…";
    rows.push(`<div class="queue-row now"><span></span>` +
      `<span class="qr-title" title="${esc(now)}">▶ ${esc(now)}</span><span></span></div>`);
  }
  (q.items || []).forEach((it) => {
    const text = it.label || it.url || it.id || "";
    rows.push(`<div class="queue-row" data-uid="${esc(it.uid)}">` +
      `<span class="drag-handle" data-drag="1" title="Drag to reorder">⋮⋮</span>` +
      `<span class="qr-title" title="${esc(it.url || text)}">${esc(text)}</span>` +
      `<button class="qr-remove" type="button" data-remove="${esc(it.uid)}" aria-label="Remove">✕</button>` +
      `</div>`);
  });
  list.innerHTML = rows.join("") ||
    `<div class="queue-row" style="border:none;justify-content:center;color:var(--muted)">Queue is empty</div>`;
}

function toggleQueuePanel() {
  const panel = $("queue-panel");
  if (!panel) return;
  if (panel.hidden) { panel.hidden = false; renderQueuePanel(); }
  else panel.hidden = true;
}

function hideQueuePanel() {
  const panel = $("queue-panel");
  if (panel) panel.hidden = true;
}

async function postJob(path, body) {
  try {
    const res = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
  } catch (err) {
    addMessage("tool", String(err));
  }
}

async function clearQueue() { hideQueuePanel(); await postJob("/jobs/clear"); }
async function removeQueued(uid) { await postJob("/jobs/remove", { uid }); }
async function reorderQueue(uids) { await postJob("/jobs/reorder", { uids }); }

// --- per-row context menu (right-click / three-dot) ---
function hostOf(url) {
  try { return new URL(url).hostname.replace(/^www\./, ""); } catch (e) { return ""; }
}

async function ignoreLink(id) {
  await postJob("/links/ignore", { id });
  state.visibleLinks = state.visibleLinks.filter((l) => l.id !== id);
  renderLinks();
  loadLinks(state.queueOffset, { preserveScroll: true });
}

async function ignoreDomain(host) {
  if (!host) return;
  await postJob("/links/ignore", { host });
  loadLinks(0);
}

function closeContextMenu() {
  const menu = $("ctx-menu");
  if (menu) menu.hidden = true;
}

function openContextMenu(linkId, x, y) {
  const link = state.visibleLinks.find((l) => l.id === linkId);
  if (!link) return;
  const host = link.host || hostOf(link.url) || "";
  const menu = $("ctx-menu");
  const items = [
    `<button type="button" class="ctx-item" data-action="fetch">Fetch</button>`,
    `<button type="button" class="ctx-item" data-action="queue">Add to queue</button>`,
    `<div class="ctx-sep"></div>`,
    `<button type="button" class="ctx-item danger" data-action="ignore">Ignore this link</button>`,
  ];
  if (host) {
    items.push(`<button type="button" class="ctx-item danger" data-action="ignore-host">Ignore ${esc(host)}</button>`);
    items.push(`<button type="button" class="ctx-item" data-action="search-host">Search ${esc(host)}</button>`);
  }
  menu.innerHTML = items.join("");
  menu.dataset.linkId = linkId;
  menu.dataset.host = host;
  menu.style.left = `${x}px`;
  menu.style.top = `${y}px`;
  menu.hidden = false;
  const r = menu.getBoundingClientRect();
  if (r.right > window.innerWidth - 8) menu.style.left = `${Math.max(8, window.innerWidth - r.width - 8)}px`;
  if (r.bottom > window.innerHeight - 8) menu.style.top = `${Math.max(8, window.innerHeight - r.height - 8)}px`;
}

function onContextMenuAction(event) {
  const item = event.target.closest("[data-action]");
  if (!item) return;
  const menu = $("ctx-menu");
  const id = menu.dataset.linkId;
  const host = menu.dataset.host;
  closeContextMenu();
  switch (item.dataset.action) {
    case "fetch":
    case "queue": fetchLink(id); break;
    case "ignore": ignoreLink(id); break;
    case "ignore-host": ignoreDomain(host); break;
    case "search-host":
      $("link-search").value = host;
      clearSelection();
      loadLinks(0);
      break;
  }
}

// pointer-based drag reorder within the queue panel
let _drag = null;
function onPanelPointerDown(event) {
  const handle = event.target.closest("[data-drag]");
  if (!handle) return;
  const row = handle.closest(".queue-row[data-uid]");
  if (!row) return;
  event.preventDefault();
  event.stopPropagation();  // keep the global link-drag-selection finishers out of it
  state.dragging = true;
  _drag = { row, pointerId: event.pointerId };
  row.classList.add("dragging");
  try { handle.setPointerCapture(event.pointerId); } catch (e) {}
}
function onPanelPointerMove(event) {
  if (!_drag) return;
  event.preventDefault();
  const list = $("queue-panel-list");
  const rows = [...list.querySelectorAll(".queue-row[data-uid]")].filter((r) => r !== _drag.row);
  const after = rows.find((r) => {
    const box = r.getBoundingClientRect();
    return event.clientY < box.top + box.height / 2;
  });
  if (after) list.insertBefore(_drag.row, after);
  else list.appendChild(_drag.row);
}
function onPanelPointerUp(event) {
  if (!_drag) return;
  event.preventDefault();
  event.stopPropagation();
  const list = $("queue-panel-list");
  _drag.row.classList.remove("dragging");
  const uids = [...list.querySelectorAll(".queue-row[data-uid]")].map((r) => r.dataset.uid);
  _drag = null;
  state.dragging = false;
  // optimistic: reorder state.queue.items so the reconciling SSE frame matches
  const byUid = new Map((state.queue.items || []).map((it) => [it.uid, it]));
  state.queue.items = uids.map((u) => byUid.get(u)).filter(Boolean);
  reorderQueue(uids);
}

function applyTheme(theme) {
  const t = theme === "dark" ? "dark" : "light";
  document.documentElement.dataset.theme = t;
  const btn = $("theme-toggle");
  if (btn) btn.textContent = t === "dark" ? "☀ Light" : "☾ Dark";
}

function toggleTheme() {
  const cur = document.documentElement.dataset.theme === "dark" ? "dark" : "light";
  const next = cur === "dark" ? "light" : "dark";
  try { localStorage.setItem("deepest:theme", next); } catch (e) {}
  applyTheme(next);
}

async function togglePause() {
  const paused = !!(state.queue && state.queue.paused);
  try {
    const res = await fetch(paused ? "/jobs/resume" : "/jobs/pause", { method: "POST" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
  } catch (err) {
    addMessage("tool", String(err));
  }
}

function applyPaneWidths(widths) {
  const root = document.documentElement;
  root.style.setProperty("--queue-pane-width", `${widths.queue}fr`);
  root.style.setProperty("--browser-pane-width", `${widths.browser}fr`);
  root.style.setProperty("--agent-pane-width", `${widths.agent}fr`);
}

function loadPaneWidths() {
  try {
    const saved = JSON.parse(localStorage.getItem(PANE_WIDTHS_KEY) || "{}");
    let widths = {
      queue: Number(saved.queue) || 28,
      browser: Number(saved.browser) || 42,
      agent: Number(saved.agent) || 30,
    };
    const sum = widths.queue + widths.browser + widths.agent;
    if (sum < 10 || sum > 1000) widths = { queue: 28, browser: 42, agent: 30 };
    applyPaneWidths(widths);
  } catch {
    applyPaneWidths({ queue: 28, browser: 42, agent: 30 });
  }
}

function savePaneWidths(widths) {
  localStorage.setItem(PANE_WIDTHS_KEY, JSON.stringify(widths));
}

function currentPaneWidths() {
  const style = getComputedStyle(document.documentElement);
  const parse = (name, fallback) => parseFloat(style.getPropertyValue(name)) || fallback;
  return {
    queue: parse("--queue-pane-width", 28),
    browser: parse("--browser-pane-width", 42),
    agent: parse("--agent-pane-width", 30),
  };
}

function bindPaneResizers() {
  const workspace = document.querySelector(".workspace");
  if (!workspace) return;
  document.querySelectorAll(".pane-resizer").forEach((resizer) => {
    resizer.addEventListener("pointerdown", (event) => {
      if (window.matchMedia("(max-width: 1220px)").matches) return;
      event.preventDefault();
      resizer.setPointerCapture(event.pointerId);
      resizer.classList.add("dragging");
      const startX = event.clientX;
      const start = currentPaneWidths();
      const totalWidth = workspace.getBoundingClientRect().width;
      const totalUnits = start.queue + start.browser + start.agent;
      const min = { queue: 18, browser: 28, agent: 18 };

      const onMove = (moveEvent) => {
        const delta = ((moveEvent.clientX - startX) / totalWidth) * totalUnits;
        let next = { ...start };
        if (resizer.dataset.resizer === "queue-browser") {
          next.queue = Math.max(min.queue, start.queue + delta);
          next.browser = Math.max(min.browser, start.browser - (next.queue - start.queue));
        } else {
          next.browser = Math.max(min.browser, start.browser + delta);
          next.agent = Math.max(min.agent, start.agent - (next.browser - start.browser));
        }
        applyPaneWidths(next);
        savePaneWidths(next);
      };

      const onUp = () => {
        resizer.classList.remove("dragging");
        resizer.removeEventListener("pointermove", onMove);
        resizer.removeEventListener("pointerup", onUp);
        resizer.removeEventListener("pointercancel", onUp);
      };

      resizer.addEventListener("pointermove", onMove);
      resizer.addEventListener("pointerup", onUp);
      resizer.addEventListener("pointercancel", onUp);
    });
  });
}

function setStatus(status, detail) {
  const dot = $("status-dot");
  const activeStates = ["starting_brain", "brain_ready", "starting_chrome", "active", "waiting", "canceling", "services_starting", "services_ready", "services_busy"];
  dot.className = "dot " + (activeStates.includes(status) ? "active" : status);
  $("status-text").textContent = detail || status || "idle";
}

function setProgress(progress) {
  state.progress = { done: progress?.done || 0, total: progress?.total || 0 };
  const total = state.progress.total;
  // Only show progress while a job is active; "0 / 0" at idle is noise.
  $("progress-text").textContent = total ? `${state.progress.done} / ${total}` : "";
}

function setMode(mode) {
  $("mode-display").textContent = mode || "none";
}

function setUrl(url, note) {
  $("url-display").textContent = note || url || "waiting";
}

async function showScreenshot() {
  if (state.screenshotBusy) return;
  state.screenshotBusy = true;
  const frame = $("browser-frame");
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), 2500);
  try {
    const res = await fetch(`${SCREENSHOT_URL}?${Date.now()}`, {
      cache: "no-store",
      signal: controller.signal,
    });
    if (res.status === 204) {
      if (!frame.querySelector("img")) {
        frame.innerHTML = `<div class="placeholder">Chrome ready, waiting for screenshot</div>`;
      }
      return;
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const blob = await res.blob();
    const objectUrl = URL.createObjectURL(blob);
    const img = frame.querySelector("img");
    if (img) {
      img.src = objectUrl;
    } else {
      frame.innerHTML = `<img src="${objectUrl}" alt="browser screenshot">`;
    }
    if (state.screenshotObjectUrl) URL.revokeObjectURL(state.screenshotObjectUrl);
    state.screenshotObjectUrl = objectUrl;
  } catch (err) {
    if (!frame.querySelector("img")) {
      frame.innerHTML = `<div class="placeholder">${esc(String(err))}</div>`;
    }
  } finally {
    window.clearTimeout(timeoutId);
    state.screenshotBusy = false;
  }
}

function addMessage(role, content) {
  state.chatEntries.push({
    kind: "message",
    role,
    content: String(content || ""),
    ts: Date.now(),
    seq: ++state.timelineSeq,
  });
  renderTimeline();
}

function clearChat() {
  state.chatEntries = [];
  state.lastPrompt = "";
  state.lastError = "";
  state.lastResponse = "";
  renderTimeline();
}

async function clearActivity() {
  state.chatEntries = [];
  state.traceEntries = [];
  renderTimeline();
  try {
    const res = await fetch("/activity/clear", { method: "POST" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
  } catch (err) {
    addMessage("tool", `Clear failed: ${String(err)}`);
  }
}

async function writeClipboardText(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  try {
    if (!document.execCommand("copy")) {
      throw new Error("copy command failed");
    }
  } finally {
    textarea.remove();
  }
}

async function copyActivityBlocks() {
  const button = $("copy-chat-btn");
  const blocks = Array.from(document.querySelectorAll("#chat-log .msg"))
    .map((block) => block.innerText.trim())
    .filter(Boolean);
  if (!blocks.length) {
    setBusy(button, false, "Nothing");
    window.setTimeout(() => setBusy(button, false, "Copy"), 900);
    return;
  }
  try {
    await writeClipboardText(blocks.join("\n\n"));
    setBusy(button, false, "Copied");
  } catch (err) {
    addMessage("tool", `Copy failed: ${String(err)}`);
    setBusy(button, false, "Copy");
    return;
  }
  window.setTimeout(() => setBusy(button, false, "Copy"), 1200);
}

function traceTimestamp(event, index) {
  const parsed = Date.parse(event?.ts || "");
  if (Number.isFinite(parsed)) return parsed;
  return index;
}

function renderTimeline() {
  const list = $("timeline-list");
  if (!list) return;
  const entries = [...state.traceEntries, ...state.chatEntries]
    .sort((a, b) => (a.ts - b.ts) || (a.seq - b.seq));
  if (!entries.length) {
    renderEmpty(list, "No agent activity");
    return;
  }
  list.innerHTML = entries.map((entry) => {
    if (entry.kind === "trace") {
      return `
        <div class="msg trace">
          <div class="msg-label">trace</div>
          <div>${esc(entry.content)}</div>
          ${entry.meta ? `<div class="trace-meta">${esc(entry.meta)}</div>` : ""}
        </div>`;
    }
    return `
      <div class="msg ${esc(entry.role)} chat-message">
        <div class="msg-label">${esc(entry.role)}</div>
        <div>${esc(entry.content)}</div>
      </div>`;
  }).join("");
  $("chat-log").scrollTop = $("chat-log").scrollHeight;
}

function renderEmpty(target, text) {
  target.innerHTML = `<div class="empty">${esc(text)}</div>`;
}

function selectedCount() {
  return state.selectedIds.size;
}

function updateCrawlActionLabel() {
  const btn = $("crawl-all-btn");
  if (!btn) return;
  if (btn.disabled) return;
  if (selectedCount()) {
    btn.textContent = "Crawl Selected";
    btn.title = `${selectedCount()} selected`;
  } else {
    btn.textContent = "Crawl All";
    btn.title = "";
  }
}

function clearSelection() {
  state.selectedIds.clear();
  state.selectionAnchorId = "";
  updateCrawlActionLabel();
}

function visibleLinkIndex(id) {
  return state.visibleLinks.findIndex((link) => link.id === id);
}

function selectVisibleRange(fromId, toId, additive = false) {
  const from = visibleLinkIndex(fromId);
  const to = visibleLinkIndex(toId);
  if (from < 0 || to < 0) return;
  if (!additive) state.selectedIds.clear();
  const start = Math.min(from, to);
  const end = Math.max(from, to);
  for (let i = start; i <= end; i += 1) {
    state.selectedIds.add(state.visibleLinks[i].id);
  }
  updateCrawlActionLabel();
}

function toggleSelected(id) {
  if (state.selectedIds.has(id)) state.selectedIds.delete(id);
  else state.selectedIds.add(id);
  state.selectionAnchorId = id;
  updateCrawlActionLabel();
}

function renderLinks() {
  const list = $("link-list");
  if (state.loadingLinks) {
    renderEmpty(list, "Loading");
    updateCrawlActionLabel();
    return;
  }
  if (!state.visibleLinks.length) {
    renderEmpty(list, "No URLs");
    updateCrawlActionLabel();
    return;
  }

  const queuedIds = new Set(state.queue.ids || []);
  const rows = state.visibleLinks.map((row) => {
    const status = row.status || "pending";
    const cls = status === "success" || status === "ok" ? "ok" : status === "failed" ? "failed" : "";
    const label = status === "ok" ? "success" : status;
    const isCurrent = row.id === state.currentId;
    const active = isCurrent ? " active" : "";
    const selected = state.selectedIds.has(row.id) ? " selected" : "";
    const isJobRow = state.jobActive && row.id === state.jobLinkId;
    const queued = !isJobRow && queuedIds.has(row.id) ? " queued" : "";
    const control = isJobRow
      ? `<button type="button" class="fetch-spin" data-stop-job="1" title="Stop crawl" aria-label="Stop crawl">
            <span class="spin-ring"></span>
            <span class="spin-stop"></span>
          </button>`
      : `<div class="row-controls">
            <button type="button" class="row-play" data-fetch-id="${esc(row.id)}" title="Fetch" aria-label="Fetch">▶</button>
            <button type="button" class="row-menu" data-menu-id="${esc(row.id)}" title="More actions" aria-label="More actions">⋮</button>
          </div>`;
    return `
      <div class="link-row${active}${selected}${queued}" data-link-id="${esc(row.id)}">
        <div>
          <div class="link-url" title="${esc(row.url)}">${esc(row.url)}</div>
          <div class="link-meta">
            <span class="pill ${cls}">${esc(label)}</span>
            ${queued ? `<span class="pill queued">queued</span>` : ""}
          </div>
          ${row.error ? `<div class="link-error" title="${esc(row.error)}">${esc(row.error)}</div>` : ""}
        </div>
        ${control}
      </div>`;
  }).join("");
  const footerText = state.queuePaging ? "Loading…" : "";
  list.innerHTML = rows + (footerText ? `<div class="list-footer">${esc(footerText)}</div>` : "");
  updateCrawlActionLabel();
}

function renderDetail(detail) {
  const link = detail.link || {};
  const result = detail.result || {};
  const url = result.url || link.url || "";
  const host = detail.host || "";

  state.currentUrl = url || state.currentUrl;
  state.currentHost = host || state.currentHost;
  state.currentId = result.id || link.id || state.currentId;

  setUrl(url, result.note || "");
  setMode(result.mode || "saved");
  $("dom-text").textContent = result.summary || link.url || "-";
  $("error-text").textContent = result.error_detail || result.error || "-";
  renderTrace(result.trace || []);
  renderKnowledge((detail.domain_knowledge || {}).notes || result.domain_knowledge || []);
  renderPlaybooks((detail.domain_knowledge || {}).playbooks || result.domain_playbooks || []);

  clearChat();
  addMessage("tool", `Selected ${url || state.currentId}`);
  if (result.summary) addMessage("assistant", result.summary);
  if (result.error) addMessage("tool", result.error);
  renderLinks();
}

async function selectLink(id) {
  if (!id) return;
  state.currentId = id;
  state.selectionAnchorId = id;
  renderLinks();
  try {
    const res = await fetch(`/results/${encodeURIComponent(id)}`);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    renderDetail(data);
  } catch (err) {
    addMessage("tool", String(err));
  }
}

function currentQueueParams(limit, offset) {
  const q = $("link-search").value.trim();
  const status = $("status-filter").value;
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  if (q) params.set("q", q);
  if (status) params.set("status", status);
  return params;
}

function updateQueueSummary() {
  const el = $("link-count");
  if (!el) return;
  const total = state.queueFiltered || state.queueTotal || 0;
  el.textContent = total ? `${total} URLs` : "";
}

function captureLinkScrollAnchor(list) {
  if (!list) return null;
  const rows = Array.from(list.querySelectorAll("[data-link-id]"));
  const listTop = list.getBoundingClientRect().top;
  for (const row of rows) {
    const rect = row.getBoundingClientRect();
    if (rect.bottom >= listTop) {
      return {
        id: row.dataset.linkId,
        delta: rect.top - listTop,
        scrollTop: list.scrollTop,
        scrollHeight: list.scrollHeight,
      };
    }
  }
  return {
    id: "",
    delta: 0,
    scrollTop: list.scrollTop,
    scrollHeight: list.scrollHeight,
  };
}

function restoreLinkScrollAnchor(list, anchor, fallbackTop = 0) {
  if (!list || !anchor) return;
  if (anchor.id) {
    const row = list.querySelector(`[data-link-id="${CSS.escape(anchor.id)}"]`);
    if (row) {
      const listTop = list.getBoundingClientRect().top;
      const rect = row.getBoundingClientRect();
      list.scrollTop += rect.top - listTop - anchor.delta;
      return;
    }
  }
  list.scrollTop = Math.min(fallbackTop, Math.max(0, list.scrollHeight - list.clientHeight));
}

async function loadLinks(offset = 0, options = {}) {
  const nextOffset = Math.max(0, offset);
  const preserveScroll = Boolean(options.preserveScroll);
  const mode = options.mode || "replace";
  const list = $("link-list");
  const shouldAnchorScroll = preserveScroll || mode === "append" || mode === "prepend";
  const scrollAnchor = shouldAnchorScroll ? captureLinkScrollAnchor(list) : null;
  const previousScrollTop = shouldAnchorScroll ? list.scrollTop : 0;
  const previousScrollHeight = list.scrollHeight;
  const isInitial = !state.visibleLinks.length || nextOffset === 0;
  const replacing = mode === "replace";
  if (!preserveScroll && replacing) {
    if (isInitial) state.loadingLinks = true;
    else state.queuePaging = true;
    renderLinks();
  } else {
    state.queuePaging = true;
  }
  const fetchLimit = options.limit || (
    preserveScroll && replacing && state.visibleLinks.length
      ? state.visibleLinks.length
      : state.queuePageSize
  );
  const params = currentQueueParams(fetchLimit, nextOffset);

  try {
    const res = await fetch(`${LINKS_URL}?${params.toString()}`);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    const rows = data.rows || [];
    if (mode === "append") {
      const existing = new Set(state.visibleLinks.map((row) => row.id));
      state.visibleLinks = state.visibleLinks.concat(rows.filter((row) => !existing.has(row.id)));
      if (state.visibleLinks.length > state.queueMaxVisible) {
        const removeCount = state.visibleLinks.length - state.queueMaxVisible;
        state.visibleLinks = state.visibleLinks.slice(removeCount);
        state.queueOffset += removeCount;
      }
    } else if (mode === "prepend") {
      const existing = new Set(state.visibleLinks.map((row) => row.id));
      const freshRows = rows.filter((row) => !existing.has(row.id));
      state.visibleLinks = freshRows.concat(state.visibleLinks);
      state.queueOffset = data.offset || nextOffset;
      if (state.visibleLinks.length > state.queueMaxVisible) {
        state.visibleLinks = state.visibleLinks.slice(0, state.queueMaxVisible);
      }
    } else {
      state.visibleLinks = rows;
      state.queueOffset = data.offset || nextOffset;
    }
    state.queueTotal = data.total || 0;
    state.queueFiltered = data.filtered || 0;
    updateQueueSummary();
  } catch (err) {
    if (replacing) state.visibleLinks = [];
    addMessage("tool", String(err));
  } finally {
    state.loadingLinks = false;
    state.queuePaging = false;
    renderLinks();
    if (mode === "append") {
      restoreLinkScrollAnchor(list, scrollAnchor, previousScrollTop);
    } else if (mode === "prepend") {
      restoreLinkScrollAnchor(
        list,
        scrollAnchor,
        previousScrollTop + Math.max(0, list.scrollHeight - previousScrollHeight),
      );
    } else if (preserveScroll) {
      restoreLinkScrollAnchor(list, scrollAnchor, previousScrollTop);
    } else {
      list.scrollTop = 0;
    }
  }
}

function handleLinkRowClick(event, id) {
  if (!id) return;
  if (state.suppressNextLinkClick) {
    state.suppressNextLinkClick = false;
    return;
  }
  if (event.shiftKey) {
    const anchor = state.selectionAnchorId || state.currentId || id;
    selectVisibleRange(anchor, id, event.metaKey || event.ctrlKey);
    state.selectionAnchorId = id;
    renderLinks();
    return;
  }
  if (event.metaKey || event.ctrlKey) {
    toggleSelected(id);
    renderLinks();
    return;
  }
  if (selectedCount()) {
    clearSelection();
  }
  state.selectionAnchorId = id;
  selectLink(id);
}

function beginLinkDragSelection(event) {
  if (event.button !== 0) return;
  if (event.target.closest("button")) return;
  const row = event.target.closest("[data-link-id]");
  if (!row) return;
  state.dragSelection = {
    anchorId: row.dataset.linkId,
    additive: event.metaKey || event.ctrlKey,
    started: false,
  };
}

function extendLinkDragSelection(event) {
  if (!state.dragSelection) return;
  const row = event.target.closest("[data-link-id]");
  if (!row) return;
  const id = row.dataset.linkId;
  if (!id) return;
  if (id === state.dragSelection.anchorId && !state.dragSelection.started) return;
  state.dragSelection.started = true;
  state.suppressNextLinkClick = true;
  state.selectionAnchorId = state.dragSelection.anchorId;
  selectVisibleRange(state.dragSelection.anchorId, id, state.dragSelection.additive);
  renderLinks();
}

function endLinkDragSelection() {
  state.dragSelection = null;
}

async function loadMoreIfNeeded() {
  const list = $("link-list");
  if (state.loadingLinks || state.queuePaging) return;
  if (!state.visibleLinks.length) return;
  if (list.scrollTop < 24 && state.queueOffset > 0) {
    await loadLinks(Math.max(0, state.queueOffset - state.queuePageSize), { mode: "prepend" });
    return;
  }
  if (state.queueOffset + state.visibleLinks.length >= state.queueFiltered) return;
  const remaining = list.scrollHeight - list.scrollTop - list.clientHeight;
  if (remaining < 120) await loadLinks(state.queueOffset + state.visibleLinks.length, { mode: "append" });
}

function setServiceChip(id, label, ready, detail) {
  const chip = $(id);
  chip.className = `service-chip ${ready ? "ready" : "error"}`;
  chip.textContent = `${label}: ${ready ? "ready" : "offline"}`;
  chip.title = detail || "";
}

function lastLogLine(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(-1)[0] || "";
}

function renderServiceDetail(data) {
  const detail = $("service-detail");
  if (!detail) return;
  const brain = data.brain || {};
  const startup = data.startup || {};
  const parts = [];
  if (!brain.ready) {
    parts.push("Brain offline");
    if (brain.managed_exit_code !== null && brain.managed_exit_code !== undefined) {
      parts.push(`exit ${brain.managed_exit_code}`);
    }
    if (brain.log_path) parts.push(brain.log_path);
    const tail = lastLogLine(brain.log_tail);
    if (tail) parts.push(tail);
  }
  if (startup.error) parts.push(startup.error);
  detail.textContent = parts.join(" · ");
  detail.title = [
    brain.log_tail || "",
    startup.error || "",
  ].filter(Boolean).join("\n\n");
}

function renderBrainModelSelect(brain) {
  const select = $("brain-model-select");
  if (!select) return;
  const models = brain?.available_models || [];
  const active = models.find((model) => model.active);
  state.brainModels = models;
  state.activeBrainModelId = active?.id || "";
  const current = select.value;
  const options = models
    .filter((model) => model.exists && (model.source === "local" || model.active || model.id.startsWith("qwen")))
    .map((model) => {
      const vision = model.vision ? "vision" : model.vision === false ? "text" : "?";
      const selected = model.active ? " selected" : "";
      return `<option value="${esc(model.id)}"${selected}>${esc(model.label)} · ${vision}</option>`;
    })
    .join("");
  select.innerHTML = `<option value="">Brain model</option>${options}`;
  if (current && models.some((model) => model.id === current)) {
    select.value = current;
  } else if (active) {
    select.value = active.id;
  }
}

async function loadServices() {
  try {
    const res = await fetch(SERVICES_URL);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    setServiceChip("brain-chip", "Brain", data.brain?.ready, data.brain?.model);
    setServiceChip("chrome-chip", "Chrome", data.chrome?.ready, data.chrome?.socket_path || data.chrome?.registry);
    renderBrainModelSelect(data.brain);
    renderServiceDetail(data);
    state.chromeReady = Boolean(data.chrome?.ready);
    if (state.chromeReady) showScreenshot();
    const startup = data.startup || {};
    const startBtn = $("start-services-btn");
    if (startup.status === "starting" || startup.status === "busy") {
      setBusy(startBtn, true, "Start");
      startBtn.hidden = false;
    } else {
      setBusy(startBtn, false, "Start");
      // Contextual: hide Start when both services are already up (no dead chrome).
      startBtn.hidden = Boolean(data.brain?.ready && data.chrome?.ready);
    }
    if (startup.error && startup.error !== state.lastError) {
      state.lastError = startup.error;
      addMessage("tool", startup.error);
    }
  } catch (err) {
    state.chromeReady = false;
    setServiceChip("brain-chip", "Brain", false, String(err));
    setServiceChip("chrome-chip", "Chrome", false, String(err));
    $("start-services-btn").hidden = false;
    const detail = $("service-detail");
    if (detail) {
      detail.textContent = String(err);
      detail.title = String(err);
    }
  }
}

async function startServices() {
  const btn = $("start-services-btn");
  const modelId = $("brain-model-select")?.value || "";
  setBusy(btn, true, "Starting");
  try {
    const res = await fetch("/services/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        brain: true,
        chrome: true,
        model_id: modelId || null,
      }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    addMessage("tool", "Starting local MLX brain and Chrome transport");
  } catch (err) {
    addMessage("tool", String(err));
    setBusy(btn, false, "Start");
  } finally {
    setTimeout(loadServices, 1000);
  }
}

async function configureBrainModel() {
  const select = $("brain-model-select");
  const modelId = select?.value || "";
  if (!modelId || modelId === state.activeBrainModelId) return;
  setBusy($("start-services-btn"), true, "Switching");
  try {
    const res = await fetch("/services/brain", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model_id: modelId, restart_brain: true }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    addMessage("tool", `Brain model selected: ${data.selected?.label || modelId}`);
    await loadServices();
  } catch (err) {
    addMessage("tool", String(err));
    await loadServices();
  } finally {
    setBusy($("start-services-btn"), false, "Start");
  }
}

async function fetchLink(id) {
  const row = state.visibleLinks.find((link) => link.id === id);
  if (!row) return;
  $("url-input").value = row.url;
  addMessage("user", `fetch: ${row.url}`);
  // No optimistic spinner/jobActive — SSE drives run-state, so a queued fetch
  // shows a "queued" pill rather than moving the spinner onto this row.
  try {
    const res = await fetch("/crawl", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        id: row.id,
        url: row.url,
        reason: row.reason,
        sub_reason: row.sub_reason,
      }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    if (data.status === "already_queued") addMessage("tool", `Already queued: ${row.url}`);
  } catch (err) {
    addMessage("tool", String(err));
  }
}

async function crawlAll() {
  const q = $("link-search").value.trim();
  const reason = "";
  const status = $("status-filter").value;
  const ids = Array.from(state.selectedIds);
  const usingSelection = ids.length > 0;
  const count = usingSelection ? ids.length : (state.queueFiltered || state.visibleLinks.length);
  if (!count) {
    addMessage("tool", usingSelection ? "No selected URLs to crawl" : "No filtered URLs to crawl");
    return;
  }
  const ok = window.confirm(
    `${usingSelection ? "Crawl selected" : "Crawl all"} ${count} ${usingSelection ? "selected" : "filtered"} URLs?\n\nThis uses your real Chrome profile and waits between URLs.`
  );
  if (!ok) {
    addMessage("tool", `${usingSelection ? "Crawl selected" : "Crawl all"} canceled before starting`);
    return;
  }
  clearChat();
  // No optimistic run-state: the batch may queue behind a running job. SSE drives
  // jobActive/spinner/queued; the user's selection (.active) is preserved throughout.
  addMessage("user", `${usingSelection ? "crawl selected" : "crawl all"}: ${count}`);
  try {
    const res = await fetch("/fetch-all", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        q,
        reason,
        status,
        ids: usingSelection ? ids : null,
        confirm_count: count,
      }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    if (data.status === "already_queued") {
      addMessage("tool", `Already queued: ${data.count} URLs`);
    } else {
      addMessage("tool", `Queued ${data.count} URLs; ${data.delay_seconds}s + ${data.jitter_seconds}s jitter between URLs; ${data.timeout_seconds}s timeout`);
    }
    if (usingSelection) clearSelection();
  } catch (err) {
    addMessage("tool", String(err));
  }
}

async function cancelJob() {
  try {
    const res = await fetch("/jobs/cancel", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    addMessage("tool", data.status === "canceling" ? "Cancel requested" : "No running job");
    if (data.status !== "canceling") setJobActive(false);
  } catch (err) {
    addMessage("tool", String(err));
  }
}

function renderTrace(trace) {
  if (!trace || !trace.length) {
    state.traceEntries = [];
    renderTimeline();
    return;
  }
  state.traceEntries = trace.slice(-60).map((event, index) => {
    const meta = Object.entries(event)
      .filter(([key]) => key !== "message")
      .map(([key, value]) => `${key}=${String(value)}`)
      .join(" ");
    return {
      kind: "trace",
      role: "trace",
      content: event.message || "",
      meta,
      ts: traceTimestamp(event, index),
      seq: index,
    };
  });
  renderTimeline();
}

function renderKnowledge(notes) {
  const list = $("knowledge-list");
  if (!notes || !notes.length) {
    renderEmpty(list, "No domain notes");
    return;
  }
  list.innerHTML = notes.slice(-12).reverse().map((note) => `
    <div class="knowledge-item">
      <div>${esc(note.text)}</div>
      <div class="knowledge-meta">${esc(note.source || "")} ${esc(note.ts || "")}</div>
    </div>`).join("");
}

function renderPlaybooks(playbooks) {
  const list = $("playbook-list");
  if (!playbooks || !playbooks.length) {
    renderEmpty(list, "No playbooks");
    return;
  }
  list.innerHTML = playbooks.slice(-8).reverse().map((playbook) => {
    const steps = (playbook.steps || []).slice(0, 6)
      .map((step, idx) => `<div>${idx + 1}. ${esc(step)}</div>`)
      .join("");
    return `
      <div class="playbook-item">
        <div><strong>${esc(playbook.title || "Playbook")}</strong></div>
        <div>${steps}</div>
        <div class="knowledge-meta">${esc(playbook.source || "")} ${esc(playbook.ts || "")}</div>
      </div>`;
  }).join("");
}

async function saveDomainNote(event) {
  event.preventDefault();
  const input = $("domain-note-input");
  const text = input.value.trim();
  if (!text) return;
  try {
    const res = await fetch("/domain-note", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: state.currentUrl, host: state.currentHost, text }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    input.value = "";
    renderKnowledge(data.notes);
    renderPlaybooks(data.playbooks || []);
  } catch (err) {
    addMessage("tool", String(err));
  }
}

async function saveDomainPlaybook(event) {
  event.preventDefault();
  const titleInput = $("domain-playbook-title");
  const stepsInput = $("domain-playbook-steps");
  const title = titleInput.value.trim();
  const steps = stepsInput.value.trim();
  if (!steps) return;
  try {
    const res = await fetch("/domain-playbook", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: state.currentUrl, host: state.currentHost, title, steps }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    titleInput.value = "";
    stepsInput.value = "";
    renderPlaybooks(data.playbooks || []);
  } catch (err) {
    addMessage("tool", String(err));
  }
}

function updateState(data) {
  if (data.ping) return;

  const terminal = ["done", "error", "idle", "canceled", "pending"].includes(data.status);
  const status = data.error || data.status === "error" ? "error" : terminal ? "idle" : "active";
  const isCrawlJob = data.id === "crawl-all" || Boolean(data.link_id) || Boolean(data.url);
  setJobActive(!terminal && isCrawlJob);
  setStatus(status, data.status || "idle");
  setProgress(data.progress);
  setMode(data.mode);
  setUrl(data.url, data.note);

  state.currentUrl = data.url || state.currentUrl;
  state.currentHost = data.host || state.currentHost;
  // The crawling row is tracked separately from the user's selection (currentId).
  // During a batch, each per-URL frame carries the real link_id, so jobLinkId
  // advances to the member being crawled (it gets the spinner, not a pill). The
  // synthetic "crawl-all" inter-URL frames have an empty link_id, so the spinner
  // holds on the last-crawled row through waits instead of jumping to a phantom row.
  const prevJobLinkId = state.jobLinkId;
  if (!terminal && data.link_id) state.jobLinkId = data.link_id;
  let needsLinkRender = state.jobLinkId !== prevJobLinkId;

  // Queue state travels on every SSE frame; re-render rows/controls only on change.
  if (data.queue) {
    const dq = data.queue;
    const q = {
      paused: !!dq.paused,
      depth: dq.depth || 0,
      ids: dq.ids || [],
      items: dq.items || [],
      running_item: dq.running_item || null,
    };
    const changed = q.paused !== state.queue.paused
      || q.depth !== state.queue.depth
      || q.ids.join(",") !== (state.queue.ids || []).join(",")
      || q.items.map((i) => i.uid).join(",") !== (state.queue.items || []).map((i) => i.uid).join(",");
    if (changed && !state.dragging) {
      state.queue = q;
      needsLinkRender = true;
    } else if (changed) {
      // mid-drag: absorb non-order fields but don't disturb the panel DOM
      state.queue.paused = q.paused;
      state.queue.depth = q.depth;
      state.queue.ids = q.ids;
      state.queue.running_item = q.running_item;
    }
  }
  renderNowBar();  // bar reflects progress/title every frame; cheap
  if (needsLinkRender) renderLinks();

  $("dom-text").textContent = data.dom_text || (data.mode === "vision" ? "[vision fallback]" : "-");
  $("error-text").textContent = data.error_detail || data.error || "-";
  if (data.has_screenshot || state.chromeReady) showScreenshot();
  renderTrace(data.trace);
  renderKnowledge(data.domain_knowledge);
  renderPlaybooks(data.domain_playbooks);

  if (data.prompt && data.prompt !== state.lastPrompt) {
    state.lastPrompt = data.prompt;
    addMessage("user", data.prompt);
  }
  if (data.response) {
    if (data.response !== state.lastResponse) {
      state.lastResponse = data.response;
      addMessage("assistant", data.response);
    }
  }
  if (data.error && data.error !== state.lastError) {
    state.lastError = data.error;
    addMessage("tool", data.error);
  }

  if (["done", "error", "idle", "canceled"].includes(data.status)) {
    setBusy($("crawl-btn"), false, "Crawl");
    setBusy($("prompt-btn"), false, "Send");
    setJobActive(false);
    if (data.id === "crawl-all" || data.status === "canceled") {
      setBusy($("crawl-all-btn"), false, "Crawl All");
      updateCrawlActionLabel();
    }
    loadLinks(state.queueOffset, { preserveScroll: true });
  }
}

function connectEvents() {
  const es = new EventSource(EVENTS_URL);
  // Empty when healthy (hidden via :empty); only surfaces on trouble.
  $("conn-status").textContent = "";

  es.onmessage = (event) => {
    try {
      updateState(JSON.parse(event.data));
    } catch (err) {
      console.error(err);
    }
  };

  es.onerror = () => {
    $("conn-status").textContent = "reconnecting";
    es.close();
    setTimeout(connectEvents, 2000);
  };
}

function bindEvents() {
  bindPaneResizers();

  $("crawl-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const url = $("url-input").value.trim();
    if (!url) return;
    // No optimistic run-state — the job may be queued. SSE drives spinner/status.
    clearChat();
    addMessage("user", `crawl: ${url}`);
    try {
      const res = await fetch("/crawl", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      if (data.status === "already_queued") addMessage("tool", `Already queued: ${url}`);
    } catch (err) {
      addMessage("tool", String(err));
    }
  });

  $("prompt-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const input = $("prompt-input");
    const instruction = input.value.trim();
    if (!instruction) return;
    input.value = "";
    state.lastPrompt = instruction;
    addMessage("user", instruction);
    // No optimistic "Thinking" — an agent job may queue behind a running job.
    try {
      const res = await fetch("/agent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ instruction }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      if (data.status === "already_queued") addMessage("tool", `Already queued: ${instruction}`);
    } catch (err) {
      addMessage("tool", String(err));
    }
  });

  $("domain-note-form").addEventListener("submit", saveDomainNote);
  $("domain-playbook-form").addEventListener("submit", saveDomainPlaybook);
  $("copy-chat-btn").addEventListener("click", copyActivityBlocks);
  $("clear-chat-btn").addEventListener("click", clearActivity);
  $("crawl-all-btn").addEventListener("click", crawlAll);
  $("theme-toggle").addEventListener("click", toggleTheme);
  applyTheme(document.documentElement.dataset.theme);
  $("start-services-btn").addEventListener("click", startServices);
  $("brain-model-select").addEventListener("change", configureBrainModel);
  // now-playing bar + queue panel
  $("now-playpause").addEventListener("click", togglePause);
  $("now-clear").addEventListener("click", clearQueue);
  $("now-info").addEventListener("click", toggleQueuePanel);
  $("queue-panel-close").addEventListener("click", hideQueuePanel);
  const qlist = $("queue-panel-list");
  qlist.addEventListener("click", (event) => {
    const rm = event.target.closest("[data-remove]");
    if (rm) { event.stopPropagation(); removeQueued(rm.dataset.remove); }
  });
  qlist.addEventListener("pointerdown", onPanelPointerDown);
  qlist.addEventListener("pointermove", onPanelPointerMove);
  qlist.addEventListener("pointerup", onPanelPointerUp);
  qlist.addEventListener("pointercancel", onPanelPointerUp);
  $("status-filter").addEventListener("change", () => {
    clearSelection();
    loadLinks(0);
  });
  $("link-search").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      clearSelection();
      loadLinks(0);
    }
  });
  $("link-list").addEventListener("pointerdown", beginLinkDragSelection);
  $("link-list").addEventListener("pointerover", extendLinkDragSelection);
  window.addEventListener("pointerup", endLinkDragSelection);
  window.addEventListener("pointercancel", endLinkDragSelection);
  $("link-list").addEventListener("click", (event) => {
    const stop = event.target.closest("[data-stop-job]");
    if (stop) {
      event.stopPropagation();
      cancelJob();
      return;
    }
    const menuBtn = event.target.closest("[data-menu-id]");
    if (menuBtn) {
      event.stopPropagation();
      const r = menuBtn.getBoundingClientRect();
      openContextMenu(menuBtn.dataset.menuId, r.left, r.bottom + 2);
      return;
    }
    const button = event.target.closest("[data-fetch-id]");
    if (button) {
      event.stopPropagation();
      fetchLink(button.dataset.fetchId);
      return;
    }
    const row = event.target.closest("[data-link-id]");
    if (row) handleLinkRowClick(event, row.dataset.linkId);
  });
  $("link-list").addEventListener("contextmenu", (event) => {
    const row = event.target.closest("[data-link-id]");
    if (!row) return;
    event.preventDefault();
    openContextMenu(row.dataset.linkId, event.clientX, event.clientY);
  });
  $("ctx-menu").addEventListener("click", onContextMenuAction);
  document.addEventListener("click", (event) => {
    if (!event.target.closest("#ctx-menu")) closeContextMenu();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeContextMenu();
  });
  $("link-list").addEventListener("scroll", loadMoreIfNeeded);
}

loadPaneWidths();
bindEvents();
connectEvents();
loadLinks(0);
loadServices();
renderTrace([]);
renderKnowledge([]);
renderPlaybooks([]);
setJobActive(false);

setInterval(() => {
  if (state.chromeReady || $("status-dot").classList.contains("active") || $("status-dot").classList.contains("error")) {
    showScreenshot();
  }
}, 3000);

setInterval(loadServices, 5000);
