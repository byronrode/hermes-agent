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


def test_eager_public_discovery_expiry_does_not_poison_cooldown(tmp_path, monkeypatch):
    from unittest.mock import Mock
    from hermes_constants import get_hermes_home
    from tools import mcp_tool as mcp, mcp_tool_discovery as discovery, mcp_tool_loop as loop
    from tools.mcp_tool_deadline import call_deadline, MCPCallDeadlineExpired

    cancelled = threading.Event()
    async def connect(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
    monkeypatch.setattr(discovery, '_connect_server', connect)
    monkeypatch.setattr(discovery._config, '_load_mcp_config', lambda:{'deadline-eager':{'command':'fixture','lazy':False}})
    for name, value in (('_servers',{}),('_server_connecting',set()),('_server_connect_errors',{}),
                        ('_lazy_server_configs',{}),('_lazy_server_fingerprints',{}),('_lazy_server_tool_names',{})):
        monkeypatch.setattr(mcp, name, value)
    failure = Mock()
    monkeypatch.setattr(discovery, '_record_connect_failure', failure)
    monkeypatch.setattr(mcp, '_MCP_DISCOVERY_LOCK_PATH', str(get_hermes_home() / '.mcp-discovery.lock'))
    try:
        with pytest.raises(MCPCallDeadlineExpired), call_deadline(time.monotonic() + 0.05):
            discovery.discover_mcp_tools(['deadline-eager'])
        assert cancelled.wait(2)
        assert mcp._server_connecting == set()
        assert mcp._server_connect_errors == {}
        failure.assert_not_called()
    finally:
        loop._stop_mcp_loop()


@pytest.mark.platforms('posix')
def test_real_cross_process_discovery_lock_honors_deadline(tmp_path, monkeypatch):
    import subprocess
    import sys
    from hermes_constants import get_hermes_home
    from tools import mcp_tool as mcp, mcp_tool_discovery as discovery, mcp_tool_loop as loop
    from tools.mcp_tool_deadline import call_deadline, MCPCallDeadlineExpired
    path = get_hermes_home() / '.mcp-discovery.lock'
    path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(mcp, '_MCP_DISCOVERY_LOCK_PATH', str(path))
    child = subprocess.Popen([sys.executable, '-c',
        'import fcntl,sys; f=open(sys.argv[1],"w"); fcntl.flock(f,fcntl.LOCK_EX); print("locked",flush=True); sys.stdin.read()',
        str(path)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == 'locked'
        start = time.monotonic()
        with pytest.raises(MCPCallDeadlineExpired), call_deadline(start + 0.05):
            discovery._acquire_discovery_lock_with_retry()
        assert time.monotonic() - start < 2
    finally:
        child.stdin.close()
        child.wait(timeout=2)
        child.stdout.close()
    cookie = loop._try_acquire_mcp_discovery_lock()
    assert cookie is not None
    cookie.release()


def test_subinterval_readiness_retains_available_budget():
    from tools import mcp_tool_loop as loop
    from tools.mcp_tool_deadline import call_deadline
    server = SimpleNamespace(session=None, _ready=threading.Event())
    def ready():
        server.session = object()
        server._ready.set()
    timer = threading.Timer(0.1, ready)
    timer.start()
    try:
        with call_deadline(time.monotonic() + 0.24):
            assert loop._wait_for_server_session_ready(server, timeout=15) is True
    finally:
        timer.join(timeout=2)
