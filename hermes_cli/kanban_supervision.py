"""Transfer a routing run to durable external supervision without completing work."""

from __future__ import annotations

import hashlib
import json
import re
import time

from hermes_cli import kanban_db as kb
from hermes_cli.kanban_db_connect import write_txn


def handoff_to_supervision(conn, task_id: str, reference: str, *,
                           expected_run_id: int | None = None,
                           expected_body_sha256: str | None = None,
                           expected_tenant: str | None = None) -> bool:
    """A worker owns its run; a reconciler may recover only an idle transient block.

    The caller verifies external execution. This kernel operation atomically
    releases worker ownership and records supervision in the existing run ledger.
    No claim, expiry or worker PID is borrowed from the external executor.
    """
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}", reference):
        raise ValueError("exact supervision reference required")
    if expected_run_id is None and not re.fullmatch(r"[a-f0-9]{64}", expected_body_sha256 or ""):
        raise ValueError("recovery requires the inspected body digest")
    metadata = {"supervision": {"reference": reference}}
    with write_txn(conn):
        task = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if task is None:
            return False
        if expected_tenant is not None and task["tenant"] != expected_tenant:
            return False
        if expected_body_sha256 is not None and hashlib.sha256((task["body"] or "").encode()).hexdigest() != expected_body_sha256:
            return False
        current = task["current_run_id"]
        prior = conn.execute("SELECT metadata FROM task_runs WHERE id = ? AND task_id = ? AND status = 'running'",
                             (current, task_id)).fetchone()
        if prior and json.loads(prior["metadata"] or "{}").get("supervision") == metadata["supervision"]:
            return (task["status"] == "running" and task["claim_lock"] is None and task["worker_pid"] is None
                    and (expected_run_id is None or run_handed_to_supervision(conn, task_id, expected_run_id)))
        if expected_run_id is not None:
            if task["status"] != "running" or current != expected_run_id:
                return False
        elif (task["status"] != "blocked" or task["block_kind"] != "transient"
              or current is not None or task["claim_lock"] is not None or task["worker_pid"] is not None):
            return False
        if not kb._parents_satisfied(conn, task_id):
            return False
        released = kb._end_run(conn, task_id, outcome="released", metadata=metadata)
        now = int(time.time())
        run_id = conn.execute("""
            INSERT INTO task_runs (task_id, profile, step_key, status, started_at, metadata)
            VALUES (?, ?, ?, 'running', ?, ?)
            """, (task_id, task["assignee"], task["current_step_key"], now, json.dumps(metadata))).lastrowid
        conn.execute("""
            UPDATE tasks SET status = 'running', current_run_id = ?, claim_lock = NULL,
                claim_expires = NULL, worker_pid = NULL, worker_started_at = NULL,
                last_heartbeat_at = NULL, block_kind = NULL
            WHERE id = ?
            """, (run_id, task_id))
        kb._append_event(conn, task_id, "supervised", {"reference": reference, "released_run_id": released}, run_id=run_id)
    kb.notify_task_updated(conn, task_id, ["status", "current_run_id"])
    return True


def run_handed_to_supervision(conn, task_id: str, run_id: int) -> bool:
    """Only a released owning run with a durable supervision receipt ends its guard."""
    run = conn.execute("SELECT outcome, metadata FROM task_runs WHERE id = ? AND task_id = ?",
                       (run_id, task_id)).fetchone()
    if not run or run["outcome"] != "released":
        return False
    return bool(json.loads(run["metadata"] or "{}").get("supervision", {}).get("reference"))


def task_under_supervision(conn, task_id: str) -> bool:
    """A recorded external run deliberately has no local worker claim/heartbeat."""
    row = conn.execute(_RUNNING_TASKS_SQL + " AND t.id = ?", (task_id,)).fetchone()
    return bool(row and _supervised_row(row))


_RUNNING_TASKS_SQL = """
    SELECT t.id, t.assignee, t.claim_lock, t.worker_pid,
           r.status AS run_status, r.metadata AS run_metadata
    FROM tasks t LEFT JOIN task_runs r ON r.id = t.current_run_id AND r.task_id = t.id
    WHERE t.status = 'running'
"""


def _supervised_row(row) -> bool:
    supervision = kb._json_dict(row["run_metadata"]).get("supervision")
    return (row["run_status"] == "running" and row["claim_lock"] is None and row["worker_pid"] is None
            and isinstance(supervision, dict) and bool(supervision.get("reference")))


def running_local_tasks(conn):
    """Local concurrency counts preserve legacy rows and exclude durable external runs."""
    return [row for row in conn.execute(_RUNNING_TASKS_SQL) if not _supervised_row(row)]
