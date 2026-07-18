"""
agent_status.py — reads real Hermes state to drive the Agent Village dashboard.

Data sources (all on the Hermes machine):
  - ~/.hermes/cron/jobs.json   cron job states  -> "working"/"active"
  - ~/.hermes/state.db         SQLite messages/sessions -> topic activity + delegations

Nothing here mocks data. If a source file is missing the agent simply degrades
to "offline"/"idle" instead of crashing, so the dashboard still renders.

The state.db schema is auto-detected at import time (see _detect_message_table)
because the exact table/column names differ between Hermes builds. If detection
picks the wrong table, override it explicitly with the HERMES_MSG_* env vars:

  HERMES_MSG_TABLE=messages
  HERMES_MSG_TOPIC_COL=topic
  HERMES_MSG_TEXT_COL=content
  HERMES_MSG_TIME_COL=created_at
  HERMES_MSG_ROLE_COL=role       (optional; used to isolate user messages)
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HERMES_DIR = Path(os.environ.get("HERMES_DIR", Path.home() / ".hermes"))
CRON_JOBS_PATH = HERMES_DIR / "cron" / "jobs.json"
STATE_DB_PATH = Path(os.environ.get("HERMES_STATE_DB", HERMES_DIR / "state.db"))

# Thresholds (seconds) for translating "last activity" into a status label.
ACTIVE_WINDOW = 5 * 60          # < 5m  -> active/working
IDLE_WINDOW = 30 * 60           # < 30m -> idle
AFK_WINDOW = 6 * 60 * 60        # < 6h  -> afk ; older -> offline


# --------------------------------------------------------------------------
# Agent registry (mirrors the spec). `topic` is the Hermes thread id.
# --------------------------------------------------------------------------
@dataclass
class Agent:
    id: str
    name: str
    topic: str
    emoji: str
    grid: tuple[int, int]          # (col, row) in the 3x3 grid, 0-indexed
    always_online: bool = False
    description: str = ""


AGENTS: list[Agent] = [
    Agent("general",     "General",     "t1",   "⚙️", (1, 1), always_online=True,
          description="Coordinates the village and delegates to the other agents."),
    Agent("finance",     "Finance",     "t5",   "💰", (1, 0),
          description="Budgets, spending, and money tracking."),
    Agent("reminders",   "Reminders",   "t2",   "📋", (1, 2),
          description="Reminders and Todoist tasks."),
    Agent("memory",      "Memory",      "t6",   "🧠", (0, 0),
          description="Long-term memory / Notion logging."),
    Agent("predictions", "Predictions", "t7",   "🔮", (0, 1),
          description="Forecasts and predictions."),
    Agent("books",       "Books",       "t81",  "📚", (2, 1),
          description="Book downloads and reading."),
    Agent("health",      "Health",      "t119", "💪", (2, 0),
          description="Health and fitness tracking."),
    Agent("japanese",    "Japanese",    "t362", "🗾", (0, 2),
          description="Japanese study and quizzes."),
    Agent("coding",      "Coding",      "t1",   "💻", (2, 2),
          description="Coding tasks and dev work."),
]

AGENTS_BY_ID = {a.id: a for a in AGENTS}
AGENTS_BY_TOPIC: dict[str, list[Agent]] = {}
for _a in AGENTS:
    AGENTS_BY_TOPIC.setdefault(_a.topic, []).append(_a)


# --------------------------------------------------------------------------
# Delegation patterns: (regex on General/t1 user text) -> target agent id
# --------------------------------------------------------------------------
DELEGATION_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bset (?:a )?reminder\b", re.I),        "reminders"),
    (re.compile(r"\bset (?:a )?budget\b", re.I),          "finance"),
    (re.compile(r"\blog to notion\b", re.I),              "memory"),
    (re.compile(r"\bpredict\b", re.I),                    "predictions"),
    (re.compile(r"\bdownload (?:a )?book\b", re.I),       "books"),
    (re.compile(r"\btrack (?:my )?health\b", re.I),       "health"),
    (re.compile(r"\bjapanese quiz\b", re.I),              "japanese"),
    (re.compile(r"\bcoding task\b", re.I),                "coding"),
]


# --------------------------------------------------------------------------
# state.db schema detection
# --------------------------------------------------------------------------
@dataclass
class MsgSchema:
    table: str
    topic_col: str
    text_col: str
    time_col: str
    role_col: str | None = None
    time_is_epoch: bool = True     # True if numeric epoch, False if ISO text
    ok: bool = True
    note: str = ""


_TEXT_CANDIDATES = ["content", "text", "body", "message", "msg"]
_TOPIC_CANDIDATES = ["topic", "thread", "thread_id", "topic_id", "session", "session_id", "channel"]
_TIME_CANDIDATES = ["created_at", "timestamp", "ts", "time", "created", "date"]
_ROLE_CANDIDATES = ["role", "sender", "author", "direction", "from"]


def _connect() -> sqlite3.Connection | None:
    if not STATE_DB_PATH.exists():
        return None
    try:
        con = sqlite3.connect(f"file:{STATE_DB_PATH}?mode=ro", uri=True, timeout=2)
        con.row_factory = sqlite3.Row
        return con
    except sqlite3.Error:
        return None


def _detect_message_table() -> MsgSchema:
    # Explicit override wins.
    env_table = os.environ.get("HERMES_MSG_TABLE")
    if env_table:
        return MsgSchema(
            table=env_table,
            topic_col=os.environ.get("HERMES_MSG_TOPIC_COL", "topic"),
            text_col=os.environ.get("HERMES_MSG_TEXT_COL", "content"),
            time_col=os.environ.get("HERMES_MSG_TIME_COL", "created_at"),
            role_col=os.environ.get("HERMES_MSG_ROLE_COL") or None,
            note="from env override",
        )

    con = _connect()
    if con is None:
        return MsgSchema("", "", "", "", ok=False, note="state.db not found/unreadable")

    try:
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

        def cols(t: str) -> list[str]:
            return [r[1] for r in con.execute(f"PRAGMA table_info('{t}')").fetchall()]

        def pick(available: list[str], candidates: list[str]) -> str | None:
            low = {c.lower(): c for c in available}
            for cand in candidates:
                if cand in low:
                    return low[cand]
            return None

        # Prefer a table literally named "messages", else any table that has a
        # text + topic + time column.
        ordered = sorted(tables, key=lambda t: (t.lower() != "messages", t.lower()))
        for t in ordered:
            c = cols(t)
            text = pick(c, _TEXT_CANDIDATES)
            topic = pick(c, _TOPIC_CANDIDATES)
            tcol = pick(c, _TIME_CANDIDATES)
            if text and topic and tcol:
                role = pick(c, _ROLE_CANDIDATES)
                # Sample a time value to guess epoch vs ISO string.
                epoch = True
                try:
                    sample = con.execute(
                        f"SELECT \"{tcol}\" FROM \"{t}\" "
                        f"WHERE \"{tcol}\" IS NOT NULL LIMIT 1").fetchone()
                    if sample and isinstance(sample[0], str) and not sample[0].isdigit():
                        epoch = False
                except sqlite3.Error:
                    pass
                return MsgSchema(t, topic, text, tcol, role, epoch,
                                 note=f"auto-detected in table '{t}'")

        return MsgSchema("", "", "", "", ok=False,
                         note=f"no messages-like table among {tables}")
    finally:
        con.close()


MSG_SCHEMA = _detect_message_table()


def _to_epoch(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        # Some stores use ms.
        return float(value) / 1000.0 if value > 1e12 else float(value)
    s = str(value).strip()
    if s.isdigit():
        v = float(s)
        return v / 1000.0 if v > 1e12 else v
    # ISO-ish string
    from datetime import datetime
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
        # Accept {"jobs": [...]} or {id: job, ...}
        if "jobs" in data and isinstance(data["jobs"], list):
            return data["jobs"]
        return list(data.values())
    if isinstance(data, list):
        return data
    return []


def _agent_has_active_cron(agent: Agent, jobs: list[dict]) -> bool:
    """A cron job counts as making an agent 'working' if it references the
    agent's topic or id and looks currently active/running."""
    needles = {agent.id.lower(), agent.topic.lower(), agent.name.lower()}
    for job in jobs:
        blob = json.dumps(job).lower()
        if not any(n in blob for n in needles):
            continue
        state = str(job.get("state", job.get("status", ""))).lower()
        if state in ("running", "active", "working"):
            return True
        if job.get("enabled") and job.get("running"):
            return True
    return False


# --------------------------------------------------------------------------
# Last-activity lookup per topic
# --------------------------------------------------------------------------
def _last_activity_by_topic() -> dict[str, float]:
    out: dict[str, float] = {}
    if not MSG_SCHEMA.ok:
        return out
    con = _connect()
    if con is None:
        return out
    try:
        topics = {a.topic for a in AGENTS}
        placeholders = ",".join("?" for _ in topics)
        q = (f'SELECT "{MSG_SCHEMA.topic_col}" AS topic, '
             f'MAX("{MSG_SCHEMA.time_col}") AS last_t '
             f'FROM "{MSG_SCHEMA.table}" '
             f'WHERE "{MSG_SCHEMA.topic_col}" IN ({placeholders}) '
             f'GROUP BY "{MSG_SCHEMA.topic_col}"')
        for row in con.execute(q, tuple(topics)).fetchall():
            out[str(row["topic"])] = _to_epoch(row["last_t"])
    except sqlite3.Error:
        pass
    finally:
        con.close()
    return out


def _status_from_age(age: float | None, working: bool, always_online: bool) -> tuple[str, str]:
    now_hint = ""
    if working:
        return "working", "Active now"
    if always_online and (age is None or age > AFK_WINDOW):
        return "active", "Online"
    if age is None:
        return "offline", "Offline"
    if age < ACTIVE_WINDOW:
        return "active", "Active now"
    if age < IDLE_WINDOW:
        return "idle", f"Last active {int(age // 60)}m ago"
    if age < AFK_WINDOW:
        return "afk", f"AFK {int(age // 3600)}h ago"
    return "offline", "Offline"


def get_agents() -> list[dict]:
    """Return the 9 agents with live status for the API/frontend."""
    jobs = read_cron_jobs()
    last_act = _last_activity_by_topic()
    now = time.time()
    result = []
    for a in AGENTS:
        last_t = last_act.get(a.topic)
        age = (now - last_t) if last_t else None
        working = _agent_has_active_cron(a, jobs)
        status, label = _status_from_age(age, working, a.always_online)
        result.append({
            "id": a.id,
            "name": a.name,
            "topic": a.topic,
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
# Delegation detection from General (t1) user messages
# --------------------------------------------------------------------------
def get_delegations(limit: int = 20, since: float | None = None) -> list[dict]:
    """Scan recent General-topic user messages and return delegation events:
    {from: 'general', to: <agent id>, text, ts, id}."""
    if not MSG_SCHEMA.ok:
        return []
    con = _connect()
    if con is None:
        return []
    events: list[dict] = []
    try:
        sel_role = f', "{MSG_SCHEMA.role_col}" AS role' if MSG_SCHEMA.role_col else ""
        q = (f'SELECT rowid AS rid, "{MSG_SCHEMA.text_col}" AS text, '
             f'"{MSG_SCHEMA.time_col}" AS t{sel_role} '
             f'FROM "{MSG_SCHEMA.table}" '
             f'WHERE "{MSG_SCHEMA.topic_col}" = ? '
             f'ORDER BY "{MSG_SCHEMA.time_col}" DESC LIMIT ?')
        rows = con.execute(q, ("t1", max(limit * 4, 40))).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()

    for row in rows:
        text = row["text"]
        if not text:
            continue
        # Only user-authored messages should trigger a delegation (skip the
        # agent's own replies) when we can tell them apart.
        if MSG_SCHEMA.role_col:
            role = str(row["role"]).lower()
            if role not in ("user", "human", "me", "in", "inbound"):
                continue
        ts = _to_epoch(row["t"])
        if since is not None and ts <= since:
            continue
        for pattern, target in DELEGATION_PATTERNS:
            if pattern.search(str(text)):
                events.append({
                    "id": f"{row['rid']}-{target}",
                    "from": "general",
                    "to": target,
                    "text": str(text)[:200],
                    "ts": ts,
                })
                break
        if len(events) >= limit:
            break
    events.sort(key=lambda e: e["ts"])
    return events


def diagnostics() -> dict:
    """Small self-report so the /api and README can show what was detected."""
    return {
        "hermes_dir": str(HERMES_DIR),
        "cron_jobs_path": str(CRON_JOBS_PATH),
        "cron_jobs_found": CRON_JOBS_PATH.exists(),
        "state_db_path": str(STATE_DB_PATH),
        "state_db_found": STATE_DB_PATH.exists(),
        "schema_ok": MSG_SCHEMA.ok,
        "schema_note": MSG_SCHEMA.note,
        "schema_table": MSG_SCHEMA.table,
        "schema_cols": {
            "topic": MSG_SCHEMA.topic_col,
            "text": MSG_SCHEMA.text_col,
            "time": MSG_SCHEMA.time_col,
            "role": MSG_SCHEMA.role_col,
        },
    }


if __name__ == "__main__":
    import pprint
    print("=== diagnostics ===")
    pprint.pp(diagnostics())
    print("\n=== agents ===")
    pprint.pp(get_agents())
    print("\n=== delegations ===")
    pprint.pp(get_delegations())
