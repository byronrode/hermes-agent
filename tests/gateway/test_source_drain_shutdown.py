"""Retained source transactions must not outlive the SessionDB close gate."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock

import pytest

from gateway.run import GatewayRunner
from gateway.run_shutdown import GatewayShutdownMixin


@pytest.mark.asyncio
async def test_pending_source_binding_keeps_shutdown_db_open(monkeypatch):
    release = asyncio.Event()
    task = asyncio.create_task(release.wait())
    db = SimpleNamespace(close=Mock())
    runner = SimpleNamespace(
        _source_drain_task=task, _session_db=db,
        _running_agent_count=lambda: 0, _active_cron_job_count=lambda: 0,
        _active_api_run_count=lambda: 0, _active_api_worker_count=lambda: 0,
        _active_deferred_agent_worker_count=lambda: 0,
    )
    monkeypatch.setattr(GatewayRunner, '_shutdown_executor', lambda *a, **kw: 0)
    monkeypatch.setattr(GatewayRunner, 'close_all_session_db_handles', lambda *a: None)
    ctx = GatewayShutdownMixin._StopContext(deferred_count=lambda: 0, started_at=time.monotonic())
    try:
        assert GatewayShutdownMixin._active_work_count(runner) == 1
        assert any(GatewayShutdownMixin._drain_work_counts(runner))
        GatewayShutdownMixin._stop_quiesce_and_close_session_dbs(runner, 1, ctx)
        db.close.assert_not_called()
        release.set()
        await task
        assert GatewayShutdownMixin._active_work_count(runner) == 0
        GatewayShutdownMixin._stop_quiesce_and_close_session_dbs(runner, 1, ctx)
        db.close.assert_called_once()
    finally:
        release.set()
        await task


@pytest.mark.asyncio
async def test_shutdown_during_binding_does_not_consume_source(monkeypatch):
    import gateway.source_delivery as owner
    import hermes_cli.plugins
    import gateway.platforms.api_server_source_receipts as receipts
    entered = asyncio.Event()
    release = asyncio.Event()
    source = {'provider': 'synthetic', 'session_id': 'session', 'message_id': 'message'}
    db = SimpleNamespace(
        pending_gateway_sources=lambda: [source], mark_gateway_source_polled=Mock(),
        resolve_resume_session_id=lambda session: session,
        append_source_result_delivery=Mock(return_value=1), consume_gateway_source=Mock(),
    )
    async def bind(*args):
        entered.set()
        await release.wait()
        return True
    provider = SimpleNamespace(read_result=AsyncMock(return_value={}), bind_gateway_receipt=bind)
    manager = SimpleNamespace(_api_source_providers={'synthetic': provider})
    runner = SimpleNamespace(_primary_profile_name='default', _session_db=db, _running=True)
    monkeypatch.setattr(hermes_cli.plugins, 'get_plugin_manager', lambda: manager)
    monkeypatch.setattr(owner, '_source_route', lambda *a: (None, None))
    monkeypatch.setattr(receipts, '_delivery', lambda *a: dict(identity='result', content='Synthetic result',
        metadata={}, display_kind='source_result'))
    monkeypatch.setattr(owner.ledger, 'source_obligation', lambda *a: {'state': 'delivered', 'obligation_id': 'ack'})
    monkeypatch.setattr(owner.ledger, 'source_delivery_receipt', lambda *a: {'state': 'delivered'})
    monkeypatch.setattr(owner, '_DRAIN_WAIT_SECONDS', 0.02)
    try:
        await owner.drain_gateway_sources(runner)
        await asyncio.wait_for(entered.wait(), 5)
        runner._running = False
        release.set()
        await asyncio.wait_for(asyncio.shield(runner._source_drain_task), 5)
        db.consume_gateway_source.assert_not_called()
        old_task = runner._source_drain_task
        await owner.drain_gateway_sources(runner)
        assert runner._source_drain_task is old_task
        assert provider.read_result.await_count == 1
    finally:
        release.set()
        await runner._source_drain_task
