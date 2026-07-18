"""
agent_status.py — reads real Hermes state to drive the Agent Village dashboard.

The dashboard is a read-only *window* into Hermes. The General agent does its
own thinking and delegation; we only observe the traces it leaves in the DB.

Data sources (on the Hermes machine):
  - ~/.hermes/state.db         SQLite sessions/messages  (primary)
  - ~/.hermes/cron/jobs.json   cron job states           (weak "working" hint)

Real schema (confirmed from the Hermes box):
  messages(id, session_id, role, content, timestamp, tool_calls, tool_name, active, ...)
  sessions(id, thread_id, parent_session_id, started_at, ended_at, end_reason, ...)

Concepts:
  * "topic" of a message = thread_id of its session.
  * status  = derived from session lifecycle (open session / recent messages).
  * delegation = a child session spawned under General's thread (1) whose own
    thread_id belongs to another agent. That IS the delegation edge General
    created when it decided to hand work off — no chat-text parsing.

Agent registry lives in agents.json (single source of truth shared with the
frontend). If state.db is missing, agents degrade to "offline" without crashing.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
AGENTS_JSON = BASE_DIR / "agents.json"

HERMES_DIR = Path(os.environ.get("HERMES_DIR", Path.home() / ".hermes"))
CRON_JOBS_PATH = HERMES_DIR / "cron" / "jobs.json"
STATE_DB_PATH = Path(os.environ.get("HERMES_STATE_DB", HERMES_DIR / "state.db"))

GENERAL_THREAD = 1

# Thresholds (seconds) for translating "last activity" into a status label.
ACTIVE_WINDOW = 5 * 60          # < 5m  -> active
IDLE_WINDOW = 30 * 60           # < 30m -> idle
AFK_WINDOW = 6 * 60 * 60        # < 6h  -> afk ; older -> offline


# --------------------------------------------------------------------------
# Agent registry (loaded from agents.json)
# --------------------------------------------------------------------------
def load_agents() -> list[dict]:
    return json.loads(AGENTS_JSON.read_text())["agents"]


AGENTS = load_agents()
# thread -> target agent id, excluding General/Coding which live on thread 1.
THREAD_TO_AGENT: dict[int, str] = {
    a["thread"]: a["id"]
    for a in AGENTS
    if a["thread"] != GENERAL_THREAD
}


# --------------------------------------------------------------------------
# DB access
# --------------------------------------------------------------------------
def _connect() -> sqlite3.Connection | None:
    if not STATE_DB_PATH.exists():
        return None
    try:
        con = sqlite3.connect(f"file:{STATE_DB_PATH}?mode=ro", uri=True, timeout=2)
        con.row_factory = sqlite3.Row
        return con
    except sqlite3.Error:
        return None


def _to_epoch(value: Any) -> float:
    """Normalize a timestamp (epoch int/float, epoch-ms, or ISO text)."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value) / 1000.0 if value > 1e12 else float(value)
    s = str(value).strip()
    if not s:
        return 0.0
    if s.replace(".", "", 1).isdigit():
        v = float(s)
        return v / 1000.0 if v > 1e12 else v
    from datetime import datetime
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:26], fmt).timestamp()
        except ValueError:
            continue
    return 0.0


# --------------------------------------------------------------------------
# Cron jobs -> weak "working" hint
# --------------------------------------------------------------------------
def read_cron_jobs() -> list[dict]:
    if not CRON_JOBS_PATH.exists():
        return []
    try:
        data = json.loads(CRON_JOBS_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return []
    if isinstance(data, dict):
        if isinstance(data.get("jobs"), list):
            return data["jobs"]
        return [v for v in data.values() if isinstance(v, dict)]
    return data if isinstance(data, list) else []


def _running_cron_threads(jobs: list[dict]) -> set[int]:
    """Return threads that currently have a *running* cron job."""
    running: set[int] = set()
    for job in jobs:
        state = str(job.get("state", job.get("status", ""))).lower()
        if state not in ("running", "active", "working") and job.get("running") is not True:
            continue
        for key in ("thread_id", "thread", "topic"):
            if key in job:
                try:
                    running.add(int(str(job[key]).lstrip("t")))
                except (ValueError, TypeError):
                    pass
    return running


# --------------------------------------------------------------------------
# Per-thread activity from the DB
# --------------------------------------------------------------------------
def _last_activity_by_thread(con: sqlite3.Connection, threads: set[int]) -> dict[int, float]:
    out: dict[int, float] = {}
    if not threads:
        return out
    ph = ",".join("?" for _ in threads)
    try:
        q = (
            "SELECT s.thread_id AS thread, MAX(m.timestamp) AS last_t "
            "FROM messages m JOIN sessions s ON m.session_id = s.id "
            f"WHERE s.thread_id IN ({ph}) GROUP BY s.thread_id"
        )
        for row in con.execute(q, tuple(threads)).fetchall():
            if row["thread"] is not None:
                out[int(row["thread"])] = _to_epoch(row["last_t"])
    except sqlite3.Error:
        pass
    return out


def _open_sessions_by_thread(con: sqlite3.Connection, threads: set[int]) -> set[int]:
    """Threads that currently have an unfinished (ended_at IS NULL) session."""
    out: set[int] = set()
    if not threads:
        return out
    ph = ",".join("?" for _ in threads)
    try:
        q = (
            "SELECT DISTINCT thread_id FROM sessions "
            f"WHERE ended_at IS NULL AND thread_id IN ({ph})"
        )
        for row in con.execute(q, tuple(threads)).fetchall():
            if row["thread_id"] is not None:
                out.add(int(row["thread_id"]))
    except sqlite3.Error:
        pass
    return out


def _status(age: float | None, working: bool, has_open: bool, always_online: bool):
    if working:
        return "working", "Active now"
    if age is not None and age < ACTIVE_WINDOW:
        return "active", "Active now"
    if has_open and (age is None or age < IDLE_WINDOW):
        return "working", "In session"
    if age is None:
        return ("active", "Online") if always_online else ("offline", "Offline")
    if age < IDLE_WINDOW:
        return "idle", f"Last active {int(age // 60)}m ago"
    if age < AFK_WINDOW:
        return "afk", f"AFK {int(age // 3600)}h ago"
    if always_online:
        return "active", "Online"
    return "offline", "Offline"


def get_agents() -> list[dict]:
    """The 9 agents with live status merged onto their static config."""
    threads = {a["thread"] for a in AGENTS}
    now = time.time()

    last_act: dict[int, float] = {}
    open_threads: set[int] = set()
    con = _connect()
    if con is not None:
        try:
            last_act = _last_activity_by_thread(con, threads)
            open_threads = _open_sessions_by_thread(con, threads)
        finally:
            con.close()

    running_cron = _running_cron_threads(read_cron_jobs())

    result = []
    for a in AGENTS:
        th = a["thread"]
        last_t = last_act.get(th)
        age = (now - last_t) if last_t else None
        working = th in running_cron
        has_open = th in open_threads
        status, label = _status(age, working, has_open, a.get("alwaysOnline", False))
        result.append({
            **a,
            "status": status,
            "statusLabel": label,
            "lastActive": last_t,
        })
    return result


# --------------------------------------------------------------------------
# Delegation detection — structural (child sessions under General)
# --------------------------------------------------------------------------
def get_delegations(limit: int = 20, since: float | None = None) -> list[dict]:
    """A delegation = a session whose parent lives on General's thread and whose
    own thread belongs to another agent. Returns:
      {id, from:'general', to:<agent id>, thread, ts}
    """
    con = _connect()
    if con is None:
        return []
    events: list[dict] = []
    try:
        target_threads = list(THREAD_TO_AGENT)
        ph = ",".join("?" for _ in target_threads)
        q = (
            "SELECT child.id AS child_id, child.thread_id AS thread, "
            "       child.started_at AS started "
            "FROM sessions child "
            "JOIN sessions parent ON child.parent_session_id = parent.id "
            f"WHERE parent.thread_id = ? AND child.thread_id IN ({ph}) "
            "ORDER BY child.started_at DESC LIMIT ?"
        )
        rows = con.execute(q, (GENERAL_THREAD, *target_threads, max(limit * 2, 40))).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()

    for row in rows:
        target = THREAD_TO_AGENT.get(int(row["thread"]))
        if not target:
            continue
        ts = _to_epoch(row["started"])
        if since is not None and ts <= since:
            continue
        events.append({
            "id": f"deleg-{row['child_id']}",
            "from": "general",
            "to": target,
            "thread": int(row["thread"]),
            "ts": ts,
        })
        if len(events) >= limit:
            break
    events.sort(key=lambda e: e["ts"])
    return events


def get_watermark() -> str:
    """Cheap change token: bumps whenever a message or session is added.
    Lets the SSE loop skip expensive work when nothing changed."""
    con = _connect()
    if con is None:
        return "0:0"
    try:
        msg_max = con.execute("SELECT MAX(id) FROM messages").fetchone()[0] or 0
        sess_cnt = con.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] or 0
        return f"{msg_max}:{sess_cnt}"
    except sqlite3.Error:
        return "0:0"
    finally:
        con.close()


def diagnostics() -> dict:
    con = _connect()
    tables: list[str] = []
    msg_count = None
    if con is not None:
        try:
            tables = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            msg_count = con.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        except sqlite3.Error:
            pass
        finally:
            con.close()
    return {
        "hermes_dir": str(HERMES_DIR),
        "state_db_found": STATE_DB_PATH.exists(),
        "cron_jobs_found": CRON_JOBS_PATH.exists(),
        "tables": tables,
        "message_count": msg_count,
        "watermark": get_watermark(),
    }


if __name__ == "__main__":
    import pprint
    print("=== diagnostics ==="); pprint.pp(diagnostics())
    print("\n=== agents ==="); pprint.pp(get_agents())
    print("\n=== delegations ==="); pprint.pp(get_delegations())
