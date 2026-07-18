"""
server.py — FastAPI backend for the Agent Village dashboard (read-only viewer).

Run:
    pip install -r requirements.txt
    python server.py            # serves on http://0.0.0.0:8765

Endpoints:
    GET /                       -> index.html
    GET /agents.json           -> shared agent registry
    GET /api/agents            -> {"agents": [...], "diagnostics": {...}}
    GET /api/delegations       -> {"delegations": [...]}   (?since=<epoch>)
    GET /api/agents/stream     -> SSE: pushes agents + new delegations on change
    GET /api/diagnostics       -> what Hermes sources were detected

All data is read live from ~/.hermes via agent_status.py. Nothing is mocked
and nothing is written back to Hermes/Todoist — this is a pure window.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse

import agent_status

BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML = BASE_DIR / "index.html"
AGENTS_JSON = BASE_DIR / "agents.json"

FAST_POLL = 1.0       # cheap watermark check cadence (seconds)
HEARTBEAT = 15.0      # force a refresh at least this often (keeps status ages fresh)

app = FastAPI(title="Agent Village Dashboard")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    if INDEX_HTML.exists():
        return HTMLResponse(INDEX_HTML.read_text())
    return HTMLResponse("<h1>index.html missing</h1>", status_code=500)


@app.get("/agents.json")
async def agents_json() -> FileResponse:
    return FileResponse(AGENTS_JSON, media_type="application/json")


@app.get("/api/agents")
async def api_agents() -> JSONResponse:
    return JSONResponse({
        "agents": agent_status.get_agents(),
        "diagnostics": agent_status.diagnostics(),
    })


@app.get("/api/delegations")
async def api_delegations(since: float | None = None) -> JSONResponse:
    return JSONResponse({"delegations": agent_status.get_delegations(since=since)})


@app.get("/api/diagnostics")
async def api_diagnostics() -> JSONResponse:
    return JSONResponse(agent_status.diagnostics())


@app.get("/api/feed")
async def api_feed(limit: int = 15) -> JSONResponse:
    """Village feed: delegations + the target agent's first reply after each."""
    return JSONResponse({"feed": agent_status.get_feed(limit=min(limit, 40))})


@app.get("/api/messages/{thread}")
async def api_messages(thread: int, limit: int = 30) -> JSONResponse:
    """One agent's internal chat (recent user/assistant messages, oldest first)."""
    return JSONResponse({
        "thread": thread,
        "messages": agent_status.get_thread_messages(thread, limit=min(limit, 100)),
    })


@app.get("/api/agents/stream")
async def api_stream(request: Request) -> StreamingResponse:
    async def event_gen():
        last_watermark = None
        # Seed to the newest existing delegation so a fresh connection only
        # ANIMATES delegations that happen from now on — history is not replayed
        # as a flood of walks. (The ticker seeds its history from /api/delegations.)
        existing = await asyncio.to_thread(agent_status.get_delegations, 1)
        last_delegation_ts = existing[-1]["ts"] if existing else 0.0
        last_push = 0.0

        while True:
            if await request.is_disconnected():
                break

            watermark = await asyncio.to_thread(agent_status.get_watermark)
            now = asyncio.get_event_loop().time()
            changed = watermark != last_watermark
            heartbeat_due = (now - last_push) >= HEARTBEAT

            if changed or heartbeat_due:
                agents = await asyncio.to_thread(agent_status.get_agents)
                yield _sse("agents", {"agents": agents})
                last_push = now

            if changed:
                new_delegs = await asyncio.to_thread(
                    agent_status.get_delegations, 20, last_delegation_ts)
                if new_delegs:
                    last_delegation_ts = max(d["ts"] for d in new_delegs)
                    yield _sse("delegations", {"delegations": new_delegs})

            last_watermark = watermark
            await asyncio.sleep(FAST_POLL)

    return StreamingResponse(event_gen(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    })


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765)
