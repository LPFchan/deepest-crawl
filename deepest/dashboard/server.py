"""FastAPI dashboard server — live view of what deepest-crawl is doing.

Endpoints:
  GET  /                  Dashboard HTML
  GET  /events            SSE stream of state updates
  GET  /screenshot        Current screenshot PNG
  GET  /state             JSON snapshot of current state
"""

from __future__ import annotations

import asyncio
import json
import base64
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from sse_starlette.sse import EventSourceResponse

from .state import STATE, CrawlStep

HERE = Path(__file__).resolve().parent
app = FastAPI(title="deepest-crawl dashboard")


def _serialize(step: CrawlStep) -> dict:
    d = {
        "id": step.id,
        "url": step.url,
        "mode": step.mode,
        "status": step.status,
        "error": step.error,
        "note": step.note,
        "dom_text": step.dom_text[:2000] if step.dom_text else "",
        "prompt": step.prompt[:2000] if step.prompt else "",
        "response": step.response,
        "actions": step.actions[-20:],
        "has_screenshot": step.png_bytes is not None,
        "progress": STATE.progress,
    }
    return d


async def _event_generator(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    remove = STATE.listen(lambda step: queue.put_nowait(_serialize(step)))
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
    return Response(status_code=204)


@app.get("/state")
async def state():
    return _serialize(STATE.current)


@app.get("/")
async def index():
    html = (HERE / "index.html").read_text()
    return HTMLResponse(html)


def start(host: str = "127.0.0.1", port: int = 8766):
    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="info")
