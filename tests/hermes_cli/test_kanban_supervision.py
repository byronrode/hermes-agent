import hashlib

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli.kanban_db_connect import connect_closing, init_db
from hermes_cli.kanban_supervision import handoff_to_supervision, run_handed_to_supervision
from hermes_cli.kanban_db_dispatch import detect_crashed_workers, detect_stale_running, reconcile_orphaned_running
from hermes_cli import kanban_db_dispatch as dispatch


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    init_db()
    with connect_closing() as connection:
        yield connection


def test_routing_worker_handoff_remains_running_without_worker_or_retry(conn):
    task_id = kb.create_task(conn, title="external work", body="source")
    task = kb.claim_task(conn, task_id)
    run_id = task.current_run_id
    assert handoff_to_supervision(conn, task_id, "external:exact", expected_run_id=run_id)
    assert run_handed_to_supervision(conn, task_id, run_id)
    task = kb.get_task(conn, task_id)
    assert task.status == "running"
    assert task.current_run_id != run_id
    assert task.worker_pid is None and task.claim_lock is None and task.claim_expires is None
    assert detect_crashed_workers(conn) == []
    assert kb.release_stale_claims(conn) == 0
    conn.execute("UPDATE task_runs SET started_at=1 WHERE id=?", (task.current_run_id,))
    conn.commit()
    assert detect_stale_running(conn, stale_timeout_seconds=1) == []
    assert reconcile_orphaned_running(conn) == []
    assert kb.get_task(conn, task_id).status == "running"
    assert not kb.complete_task(conn, task_id, summary="routing finished", expected_run_id=run_id)
    runs = kb.list_runs(conn, task_id)
    assert all(run.outcome != "completed" for run in runs)
    assert handoff_to_supervision(conn, task_id, "external:exact", expected_run_id=run_id)
    assert len(kb.list_runs(conn, task_id)) == len(runs)
    assert not handoff_to_supervision(conn, task_id, "external:exact", expected_run_id=run_id + 999)


@pytest.mark.parametrize("kind", ["transient", "needs_input", None])
def test_recovery_only_restores_inspected_transient_block(conn, kind):
    task_id = kb.create_task(conn, title="external work", body="source")
    task = kb.claim_task(conn, task_id)
    assert kb.block_task(conn, task_id, kind=kind, reason="routing ended", expected_run_id=task.current_run_id)
    digest = hashlib.sha256(b"source").hexdigest()
    assert not handoff_to_supervision(conn, task_id, "external:exact", expected_body_sha256="0" * 64)
    restored = handoff_to_supervision(conn, task_id, "external:exact", expected_body_sha256=digest)
    assert restored is (kind == "transient")
    assert kb.get_task(conn, task_id).status == ("running" if restored else "blocked")


def test_stale_worker_cannot_handoff_another_run(conn):
    task_id = kb.create_task(conn, title="external work")
    task = kb.claim_task(conn, task_id)
    assert not handoff_to_supervision(conn, task_id, "external:exact", expected_run_id=task.current_run_id + 1)
    assert not run_handed_to_supervision(conn, task_id, task.current_run_id)
    assert not handoff_to_supervision(conn, task_id, "external:exact",
        expected_run_id=task.current_run_id, expected_tenant="other")


def test_plugin_handoff_ends_worker_stop_guard_from_kernel_receipt(conn, monkeypatch):
    from agent.kanban_stop import build_kanban_stop_nudge
    task_id = kb.create_task(conn, title="external work")
    task = kb.claim_task(conn, task_id)
    monkeypatch.setenv("HERMES_KANBAN_TASK", task_id)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(task.current_run_id))
    assert build_kanban_stop_nudge(messages=[]) is not None
    assert handoff_to_supervision(conn, task_id, "external:exact", expected_run_id=task.current_run_id)
    assert build_kanban_stop_nudge(messages=[]) is None


@pytest.mark.parametrize("cap", ["max_spawn", "max_in_progress", "max_in_progress_per_profile"])
def test_supervision_does_not_consume_local_worker_capacity(conn, all_assignees_spawnable, cap):
    supervised = kb.create_task(conn, title="external work", assignee="vera")
    task = kb.claim_task(conn, supervised)
    assert handoff_to_supervision(conn, supervised, "external:exact", expected_run_id=task.current_run_id)
    ready = kb.create_task(conn, title="unrelated ready work", assignee="vera")
    assert dispatch.count_running_tasks(conn) == 0
    result = dispatch.dispatch_once(conn, spawn_fn=lambda *_args, **_kwargs: 424242, **{cap: 1})
    assert [spawn[0] for spawn in result.spawned] == [ready]
    assert kb.get_task(conn, supervised).status == "running"
    assert dispatch.count_running_tasks(conn) == 1
