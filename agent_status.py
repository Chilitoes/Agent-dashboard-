"""
agent_status.py — reads real Hermes state to drive the Agent Village dashboard.

Data sources (all on the Hermes machine):
  - ~/.hermes/cron/jobs.json   cron job states  -> "working" status
  - ~/.hermes/state.db         SQLite sessions/messages -> topic activity + delegations

Real schema (confirmed from the Hermes box):
  messages(id, session_id, role, content, timestamp, ...)
  sessions(id, thread_id, ...)            # thread_id is the "topic" (an integer)

A message's topic = the thread_id of its session, so everything joins
messages.session_id -> sessions.id and filters on sessions.thread_id.

Nothing here mocks data. If state.db is missing an agent degrades to
"offline" instead of crashing, so the dashboard still renders.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

HERMES_DIR = Path(os.environ.get("HERMES_DIR", Path.home() / ".hermes"))
CRON_JOBS_PATH = HERMES_DIR / "cron" / "jobs.json"
STATE_DB_PATH = Path(os.environ.get("HERMES_STATE_DB", HERMES_DIR / "state.db"))

# Thresholds (seconds) for translating "last activity" into a status label.
ACTIVE_WINDOW = 5 * 60          # < 5m  -> active
IDLE_WINDOW = 30 * 60           # < 30m -> idle
AFK_WINDOW = 6 * 60 * 60        # < 6h  -> afk ; older -> offline


# --------------------------------------------------------------------------
# Agent registry. `thread` is the integer Hermes thread_id (sessions.thread_id).
# Coding shares thread 1 with General (no dedicated thread exists).
# --------------------------------------------------------------------------
@dataclass
class Agent:
    id: str
    name: str
    thread: int
    emoji: str
    grid: tuple[int, int]          # (col, row) in the 3x3 grid, 0-indexed
    cron_keywords: tuple[str, ...] = ()
    always_online: bool = False
    description: str = ""


AGENTS: list[Agent] = [
    Agent("general",     "General",     1,   "⚙️", (1, 1), always_online=True,
          cron_keywords=("brief", "general"),
          description="Coordinates the village and delegates to the other agents."),
    Agent("finance",     "Finance",     5,   "💰", (1, 0),
          cron_keywords=("budget", "finance", "spend"),
          description="Budgets, spending, and money tracking."),
    Agent("reminders",   "Reminders",   2,   "📋", (1, 2),
          cron_keywords=("reminder", "vitamin", "breakfast", "lunch", "dinner"),
          description="Reminders and Todoist tasks."),
    Agent("memory",      "Memory",      6,   "🧠", (0, 0),
          cron_keywords=("memory", "notion", "log"),
          description="Long-term memory / Notion logging."),
    Agent("predictions", "Predictions", 7,   "🔮", (0, 1),
          cron_keywords=("predict", "forecast", "free models"),
          description="Forecasts and predictions."),
    Agent("books",       "Books",       81,  "📚", (2, 1),
          cron_keywords=("book", "download"),
          description="Book downloads and reading."),
    Agent("health",      "Health",      119, "💪", (2, 0),
          cron_keywords=("health", "run", "fitness"),
          description="Health and fitness tracking."),
    Agent("japanese",    "Japanese",    362, "🗾", (0, 2),
          cron_keywords=("japanese", "quiz"),
          description="Japanese study and quizzes."),
    Agent("coding",      "Coding",      1,   "💻", (2, 2),
          cron_keywords=("coding", "code", "dev"),
          description="Coding tasks and dev work."),
]

AGENTS_BY_ID = {a.id: a for a in AGENTS}

# Delegation keyword patterns matched in General (thread 1) user messages.
# Keyed by target agent id -> list of lowercase substrings.
DELEGATION_KEYWORDS: dict[str, list[str]] = {
    "reminders":   ["reminder"],
    "finance":     ["budget"],
    "memory":      ["log to", "save to", "log to notion"],
    "predictions": ["predict"],
    "books":       ["book", "download"],
    "health":      ["health", "track health"],
    "japanese":    ["japanese", "quiz"],
    "coding":      ["coding task", "code this"],
}

GENERAL_THREAD = 1


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
    """Normalize a `timestamp` value (epoch int/float, epoch-ms, or ISO text)."""
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
    iso = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso).timestamp()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:26], fmt).timestamp()
        except ValueError:
            continue
    return 0.0


# --------------------------------------------------------------------------
# Cron jobs -> which agents are "working"
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
    if isinstance(data, list):
        return data
    return []


def _agent_working_from_cron(agent: Agent, jobs: list[dict]) -> bool:
    """A cron job makes an agent 'working' if it references one of the agent's
    keywords AND currently looks active/running."""
    if not agent.cron_keywords:
        return False
    for job in jobs:
        blob = json.dumps(job).lower()
        if not any(kw in blob for kw in agent.cron_keywords):
            continue
        state = str(job.get("state", job.get("status", ""))).lower()
        if state in ("running", "active", "working"):
            return True
        if job.get("running") is True:
            return True
    return False


# --------------------------------------------------------------------------
# Last-activity lookup per thread (topic)
# --------------------------------------------------------------------------
def _last_activity_by_thread() -> dict[int, float]:
    out: dict[int, float] = {}
    con = _connect()
    if con is None:
        return out
    try:
        threads = sorted({a.thread for a in AGENTS})
        placeholders = ",".join("?" for _ in threads)
        q = (
            "SELECT s.thread_id AS thread, MAX(m.timestamp) AS last_t "
            "FROM messages m JOIN sessions s ON m.session_id = s.id "
            f"WHERE s.thread_id IN ({placeholders}) "
            "GROUP BY s.thread_id"
        )
        for row in con.execute(q, tuple(threads)).fetchall():
            if row["thread"] is not None:
                out[int(row["thread"])] = _to_epoch(row["last_t"])
    except sqlite3.Error:
        pass
    finally:
        con.close()
    return out


def _status_from_age(age: float | None, working: bool, always_online: bool) -> tuple[str, str]:
    if working:
        return "working", "Active now"
    if age is None:
        return ("active", "Online") if always_online else ("offline", "Offline")
    if age < ACTIVE_WINDOW:
        return "active", "Active now"
    if age < IDLE_WINDOW:
        return "idle", f"Last active {int(age // 60)}m ago"
    if age < AFK_WINDOW:
        return "afk", f"AFK {int(age // 3600)}h ago"
    if always_online:
        return "active", "Online"
    return "offline", "Offline"


def get_agents() -> list[dict]:
    """Return the 9 agents with live status for the API/frontend."""
    jobs = read_cron_jobs()
    last_act = _last_activity_by_thread()
    now = time.time()
    result = []
    for a in AGENTS:
        last_t = last_act.get(a.thread)
        age = (now - last_t) if last_t else None
        working = _agent_working_from_cron(a, jobs)
        status, label = _status_from_age(age, working, a.always_online)
        result.append({
            "id": a.id,
            "name": a.name,
            "thread": a.thread,
            "emoji": a.emoji,
            "col": a.grid[0],
            "row": a.grid[1],
            "status": status,
            "statusLabel": label,
            "lastActive": last_t,
            "description": a.description,
        })
    return result


# --------------------------------------------------------------------------
# Delegation detection from General (thread 1) user messages
# --------------------------------------------------------------------------
def get_delegations(limit: int = 20, since: float | None = None) -> list[dict]:
    """Scan recent General-thread user messages and return delegation events:
    {id, from:'general', to:<agent id>, text, ts}."""
    con = _connect()
    if con is None:
        return []
    events: list[dict] = []
    try:
        q = (
            "SELECT m.id AS mid, m.content AS content, m.timestamp AS ts "
            "FROM messages m JOIN sessions s ON m.session_id = s.id "
            "WHERE s.thread_id = ? AND m.role = 'user' "
            "ORDER BY m.timestamp DESC LIMIT ?"
        )
        rows = con.execute(q, (GENERAL_THREAD, max(limit * 4, 40))).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()

    for row in rows:
        content = row["content"]
        if not content:
            continue
        ts = _to_epoch(row["ts"])
        if since is not None and ts <= since:
            continue
        low = str(content).lower()
        for target, keywords in DELEGATION_KEYWORDS.items():
            if any(kw in low for kw in keywords):
                events.append({
                    "id": f"{row['mid']}-{target}",
                    "from": "general",
                    "to": target,
                    "text": str(content)[:200],
                    "ts": ts,
                })
                break
        if len(events) >= limit:
            break
    events.sort(key=lambda e: e["ts"])
    return events


def diagnostics() -> dict:
    """Self-report so the API/README can show what was detected."""
    con = _connect()
    tables: list[str] = []
    row_count = None
    if con is not None:
        try:
            tables = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            row_count = con.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        except sqlite3.Error:
            pass
        finally:
            con.close()
    return {
        "hermes_dir": str(HERMES_DIR),
        "cron_jobs_found": CRON_JOBS_PATH.exists(),
        "state_db_found": STATE_DB_PATH.exists(),
        "tables": tables,
        "message_count": row_count,
    }


if __name__ == "__main__":
    import pprint
    print("=== diagnostics ==="); pprint.pp(diagnostics())
    print("\n=== agents ==="); pprint.pp(get_agents())
    print("\n=== delegations ==="); pprint.pp(get_delegations())
