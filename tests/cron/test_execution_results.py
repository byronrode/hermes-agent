"""A scheduled result must belong to its exact, owner-completed execution."""

import hashlib

import pytest

from cron import executions, jobs, scheduler


@pytest.mark.parametrize("text", ["Assessed risk: café", "x" * (64 * 1024 + 1), None])
def test_result_binding_is_bounded_profile_scoped_and_terminal(tmp_path, monkeypatch, text):
    first_home, other_home = tmp_path / "first", tmp_path / "other"
    monkeypatch.setenv("HERMES_HOME", str(first_home))
    attempt = executions.create_execution("same-job", source="builtin")
    owner = executions._PROCESS_ID
    monkeypatch.setattr(executions, "_PROCESS_ID", "different-owner")
    assert executions.finish_execution(attempt["id"], success=True, result_text="forged") is None
    monkeypatch.setattr(executions, "_PROCESS_ID", owner)
    saved = executions.finish_execution(attempt["id"], success=True, result_text=text)
    expected = text if text and len(text.encode()) <= executions.MAX_RESULT_BYTES else None
    assert saved["result_text"] == expected
    assert saved["result_sha256"] == (hashlib.sha256(expected.encode()).hexdigest() if expected else None)
    assert executions.finish_execution(attempt["id"], success=True, result_text="replacement") is None
    monkeypatch.setenv("HERMES_HOME", str(other_home))
    assert executions.get_execution(attempt["id"]) is None
    monkeypatch.setenv("HERMES_HOME", str(first_home))
    assert executions.get_execution(attempt["id"]) == saved


def test_scheduler_binds_display_result_without_raw_transcript_or_delivery_claim(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(scheduler, "run_job", lambda *a, **kw: (
        True, "private tool transcript must stay out of the result", "The assessed report", None))
    job = jobs.create_job(prompt="fixture only", schedule="every 1h", deliver="none")
    assert scheduler.run_one_job(job) is True
    row = executions.latest_execution(job["id"])
    assert row["status"] == "completed"
    assert row["result_text"] == "The assessed report"
    assert row["result_sha256"] == hashlib.sha256(b"The assessed report").hexdigest()
    assert row["delivery_outcome"] != "delivered"
    # A completed result is content provenance, never proof somebody received it.
    assert "private tool" not in row["result_text"]
