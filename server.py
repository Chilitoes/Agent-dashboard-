"""
server.py — FastAPI backend for the Agent Village dashboard.

Run:
    pip install -r requirements.txt
    python server.py            # serves on http://0.0.0.0:8765

Endpoints:
    GET /                       -> index.html
    GET /api/agents            -> {"agents": [...], "diagnostics": {...}}
    GET /api/delegations       -> {"delegations": [...]}   (?since=<epoch>)
    GET /api/agents/stream     -> SSE: agent status + new delegations every ~5s
    GET /api/diagnostics       -> what Hermes sources were detected

All data is read live from ~/.hermes via agent_status.py.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

import agent_status

BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML = BASE_DIR / "index.html"
POLL_SECONDS = 5

app = FastAPI(title="Agent Village Dashboard")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    if INDEX_HTML.exists():
        return HTMLResponse(INDEX_HTML.read_text())
    return HTMLResponse("<h1>index.html missing</h1>", status_code=500)


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


@app.get("/api/agents/stream")
async def api_stream(request: Request) -> StreamingResponse:
    async def event_gen():
        last_delegation_ts = 0.0
        # Prime the client with current state immediately.
        agents = await asyncio.to_thread(agent_status.get_agents)
        yield _sse("agents", {"agents": agents})
        while True:
            if await request.is_disconnected():
                break
            agents = await asyncio.to_thread(agent_status.get_agents)
            yield _sse("agents", {"agents": agents})

            new_delegs = await asyncio.to_thread(
                agent_status.get_delegations, 20, last_delegation_ts)
            if new_delegs:
                last_delegation_ts = max(d["ts"] for d in new_delegs)
                yield _sse("delegations", {"delegations": new_delegs})

            await asyncio.sleep(POLL_SECONDS)

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
