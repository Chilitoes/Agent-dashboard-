# Teaching Hermes to drive the village

The dashboard is a **read-only window**. It never guesses — it animates the
traces General leaves in `~/.hermes/state.db`. The richer those traces, the
smoother the village looks. General does this by dropping small **markers** in
his own replies. They're invisible to you (strip them in your UI if you like),
but the viewer reads them.

Add the block below to General's system prompt.

---

## System-prompt addition (paste into General)

> **Village markers.** You coordinate a village of specialist agents. Whenever
> you hand off, finish, or gather work, emit the matching marker **inline in
> your reply** (they are machine-read by the dashboard and safe to leave in):
>
> - **Delegating a task** — the moment you decide who does the work:
>   `[[delegate: <agent> | <short task description>]]`
>   e.g. `[[delegate: memory | log the Q3 recap to Notion]]`
>
> - **A task is finished** — emit this as soon as the work is actually done,
>   with a one-line result (this is the TLDR the village shows):
>   `[[done: <agent> | <short result / outcome>]]`
>   e.g. `[[done: memory | logged 3 notes, tagged Q3]]`
>
> - **A collaboration** — when a task needs several agents working together,
>   name them all; they gather in the shared Office:
>   `[[collab: <agent>, <agent> | <short task>]]`
>   e.g. `[[collab: memory, finance | reconcile the Q3 budget]]`
>
> Valid `<agent>` ids: `finance, memory, reminders, predictions, books,
> health, japanese, coding`. Use one marker per line. Emit `delegate` **before**
> the agent starts and `done` **immediately** when it finishes — don't wait.

---

## Why each marker matters

| Marker | What the village does |
|---|---|
| `[[delegate: …]]` | General walks to the agent's room; the task chip + feed line appear. Authoritative — it turns off the fragile keyword guessing. |
| `[[done: …]]` | The agent flips to **idle/Done instantly** (no 3-minute wait) and pops the real result as its speech-bubble TLDR. Fixes "memory takes a while to update when it finishes". |
| `[[collab: …]]` | The named agents converge on the shared Office room and work together. |

Sub-agents (Hermes MOA / parallel workers) need **no marker** — the dashboard
detects them structurally from `sessions.parent_session_id` and shows them in
the Sub-agent Bay while they're live.

## Timing tips

- Emit `[[done: …]]` in the **same turn** the work completes. That's what makes
  completion feel instant instead of inferred from idle time.
- Keep the result text short (a phone-width TLDR). One clause is ideal.
- It's fine to emit `delegate` and, later, `done` for the same agent — the
  village pairs them up in the room's history.
