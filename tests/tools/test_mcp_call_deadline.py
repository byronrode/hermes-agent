"""Registered MCP calls share a caller deadline without changing other requests."""
import asyncio
import json
import threading
import time
from types import SimpleNamespace

import pytest


def test_registered_deadline_cancels_only_its_rpc_and_preserves_profiles(tmp_path, monkeypatch):
    from mcp import types
    from hermes_constants import get_hermes_home, set_hermes_home_override, reset_hermes_home_override
    from tools import mcp_tool_handlers as handlers, mcp_tool_loop as loop
    from tools.mcp_tool_deadline import current_deadline
    from tools.registry import ToolRegistry

    cancelled = threading.Event()
    independent_completed = threading.Event()
    independent_tasks = []
    calls = []
    class Session:
        async def call_tool(self, name, arguments, **kwargs):
            calls.append((str(get_hermes_home()), arguments.copy(), kwargs.copy()))
            if arguments.get('sdk_timeout'):
                from mcp.shared.exceptions import MCPError
                from mcp_types import REQUEST_TIMEOUT
                error = MCPError(code=REQUEST_TIMEOUT, message='fixture SDK timeout')
                error.__context__ = TimeoutError()
                raise error
            if arguments.get('hang'):
                async def independent_request():
                    await asyncio.sleep(0.1)
                    independent_completed.set()
                independent_tasks.append(asyncio.create_task(independent_request()))
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
            return types.CallToolResult(content=[types.TextContent(type='text', text='ok')])

    server = SimpleNamespace(session=Session(), _rpc_lock=asyncio.Lock())
    monkeypatch.setattr(handlers, '_trust_gate_check', lambda *_: None)
    monkeypatch.setattr(handlers, '_check_circuit_breaker', lambda *_: None)
    monkeypatch.setattr(handlers, '_acquire_call_server', lambda *_: (server, None))
    monkeypatch.setattr(handlers, '_tool_is_read_only', lambda *_: True)
    registry = ToolRegistry()
    registry.register(name='bounded', toolset='fixture', schema={'name':'bounded', 'parameters':{'type':'object'}},
                      handler=handlers._make_tool_handler('fixture', 'bounded', 5))
    loop._ensure_mcp_loop()
    try:
        for index, name in enumerate(('a', 'b', 'a')):
            home = tmp_path / name
            home.mkdir(exist_ok=True)
            token = set_hermes_home_override(str(home))
            try:
                start = time.monotonic()
                if index == 0:
                    result = json.loads(registry.dispatch('bounded', {'hang':True}, deadline_monotonic=start + 0.05))
                    assert result['deadline_exceeded'] is True
                    assert result['outcome_uncertain'] is False
                    assert time.monotonic() - start < 2
                    assert cancelled.wait(2)
                    assert independent_completed.wait(2)
                else:
                    result = json.loads(registry.dispatch('bounded', {}))
                    assert 'error' not in result
                assert current_deadline() is None
            finally:
                reset_hermes_home_override(token)
        assert [home for home, _, _ in calls] == [str(tmp_path / name) for name in ('a','b','a')]
        assert calls[0][1] == {'hang':True}
        assert 0 < calls[0][2]['read_timeout_seconds'] <= 0.05
        assert calls[1][2] == calls[2][2] == {}
        health_failures = []
        from tools import mcp_tool as mcp
        monkeypatch.setattr(mcp, '_bump_server_error', lambda *_: health_failures.append(True))
        result = json.loads(registry.dispatch('bounded', {'sdk_timeout':True}, deadline_monotonic=time.monotonic() + 1))
        assert result['deadline_exceeded'] is True
        assert health_failures == []
    finally:
        loop._stop_mcp_loop()


def test_deadline_validation_nested_scope_and_recovery_do_not_extend_budget(monkeypatch):
    from tools import mcp_tool_deadline as deadline, mcp_tool_handlers as handlers
    from tools.registry import ToolRegistry

    clock = [100.0]
    monkeypatch.setattr(deadline.time, 'monotonic', lambda: clock[0])
    with deadline.call_deadline(120):
        with deadline.call_deadline(150):
            assert deadline.remaining_timeout(300) == 20
            assert deadline.remaining_timeout(5) == 5
        with deadline.call_deadline(110):
            assert deadline.remaining_timeout() == 10
        assert deadline.remaining_timeout() == 20
    assert deadline.current_deadline() is None

    calls = []
    monkeypatch.setattr(handlers, '_trust_gate_check', lambda *_: None)
    monkeypatch.setattr(handlers, '_check_circuit_breaker', lambda *_: None)
    monkeypatch.setattr(handlers, '_acquire_call_server', lambda *_: (SimpleNamespace(), None))
    monkeypatch.setattr(handlers, '_tool_is_read_only', lambda *_: True)
    def expired_call(*args, **kwargs):
        clock[0] = 101
        raise RuntimeError('HTTP 401')
    monkeypatch.setattr(handlers._loop, '_run_on_mcp_loop', expired_call)
    monkeypatch.setattr(handlers, '_handle_auth_error_and_retry', lambda *args: calls.append('recovery'))
    registry = ToolRegistry()
    registry.register(name='bounded', toolset='fixture', schema={'name':'bounded', 'parameters':{'type':'object'}},
                      handler=handlers._make_tool_handler('fixture', 'bounded', 300))
    for invalid in (True, float('nan'), float('inf'), '120'):
        result = json.loads(registry.dispatch('bounded', {}, deadline_monotonic=invalid))
        assert 'finite number' in result['error']
    result = json.loads(registry.dispatch('bounded', {}, deadline_monotonic=100.5))
    assert result['deadline_exceeded'] is True
    assert calls == []
    assert deadline.current_deadline() is None

    from tools import mcp_tool as mcp, mcp_tool_discovery as discovery
    key = discovery._server_key('deadline-lazy')
    monkeypatch.setattr(mcp, '_servers', {})
    monkeypatch.setattr(mcp, '_lazy_server_configs', {key:{'command':'fixture'}})
    monkeypatch.setattr(mcp, '_server_connecting', set())
    monkeypatch.setattr(mcp, '_server_connect_errors', {})
    monkeypatch.setattr(discovery, '_connect_cooldown_active', lambda *_: False)
    monkeypatch.setattr(discovery, '_note_connect_failure', lambda *_: pytest.fail('caller deadline poisoned server health'))
    monkeypatch.setattr(discovery._loop, '_ensure_mcp_loop', lambda: None)
    def caller_expired(*args, **kwargs):
        raise deadline.MCPCallDeadlineExpired('caller stopped waiting')
    monkeypatch.setattr(discovery._loop, '_run_on_mcp_loop', caller_expired)
    with deadline.call_deadline(102), pytest.raises(deadline.MCPCallDeadlineExpired):
        discovery._ensure_lazy_server_connected('deadline-lazy')
    assert key not in mcp._server_connecting
    assert mcp._server_connect_errors == {}
