# Agent Village Dashboard

A real-time, read-only **window** into your Hermes agents. Nine agents are shown
as a 3×3 village of flat tiles; when the General agent delegates work, a walker
sprite walks from General's room to the target agent's room.

The dashboard never thinks or acts on its own — **General does all the
delegating using his own intelligence**. The dashboard only observes the traces
that leaves in `~/.hermes/state.db` and animates them.

![board](docs/board.png)

## How it works

| Concern | Signal used |
|---|---|
| **Status** (working/active/idle/afk/offline) | session lifecycle (open sessions) + most-recent message timestamp per `thread_id` |
| **Delegation** (the walk) | a child session spawned under General's thread (`parent.thread_id = 1`) whose own `thread_id` belongs to another agent — i.e. the real hand-off General created |
| **"Working" hint** | running cron jobs in `~/.hermes/cron/jobs.json` |

There is **no chat-text parsing** and **no Todoist writes** — delegation is
detected structurally, and side effects belong to Hermes, not the viewer.

## Files

| File | Role |
|---|---|
| `agents.json` | Single source of truth: agent ids, threads, emojis, grid positions, colors |
| `agent_status.py` | Reads `state.db` / `jobs.json`; computes status + delegations |
| `server.py` | FastAPI: serves the UI and the `/api/*` endpoints + SSE stream |
| `index.html` | DOM + CSS frontend. Walk = CSS `transform` transition (immune to status refresh) |

## Run

```bash
pip install -r requirements.txt
python server.py            # http://0.0.0.0:8765
```

Then open the dashboard (e.g. behind your Tailscale funnel at
`https://…/agents/`).

### Configuration (env vars)

| Var | Default | Purpose |
|---|---|---|
| `HERMES_DIR` | `~/.hermes` | Base Hermes directory |
| `HERMES_STATE_DB` | `$HERMES_DIR/state.db` | SQLite state DB |

Check what the server detected:

```bash
curl localhost:8765/api/diagnostics
```

## Endpoints

- `GET /` — dashboard
- `GET /agents.json` — shared agent registry
- `GET /api/agents` — agents with live status + diagnostics
- `GET /api/delegations?since=<epoch>` — recent delegation events
- `GET /api/agents/stream` — SSE; pushes updates only when the DB changes
  (cheap `MAX(messages.id)` watermark), with a 15s heartbeat

## Schema assumptions

Confirmed against the live Hermes DB:

```
messages(id, session_id, role, content, timestamp, tool_calls, tool_name, active, …)
sessions(id, thread_id, parent_session_id, started_at, ended_at, end_reason, …)
```

A message's "topic" is the `thread_id` of its session (join
`messages.session_id → sessions.id`). Thread ids: General 1, Reminders 2,
Finance 5, Memory 6, Predictions 7, Books 81, Health 119, Japanese 362.
Coding shares thread 1 with General.

> If the walk never fires, it means General isn't spawning child sessions on
> delegation. The dashboard shows what General actually does — so the fix is on
> the Hermes side (have General spawn/hand-off a session in the target thread),
> not here.
