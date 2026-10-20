"""Observed long-running status must be useful without exposing raw activity text."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.status_phrases import format_activity_status


@pytest.mark.parametrize("tool,description,provenance,expected", [
    ("web_search", "private query", "unknown", "Searching the web"),
    ("private_plugin_name", "private query", "unknown", "Waiting for a tool result"),
    (None, "waiting for provider response (private details)", "unknown", "Waiting for the AI service"),
    (None, "private conversation", "agent.compression", "Condensing the conversation"),
    (None, "private unknown state", "unknown", "Current activity is unavailable"),
])
def test_status_uses_observations_without_relaying_private_text(tool, description, provenance, expected):
    text = format_activity_status({
        "current_tool": tool, "last_activity_desc": description,
        "last_activity_provenance": provenance, "api_call_count": 4,
        "seconds_since_activity": 185, "max_iterations": 90,
        "args": {"secret": "private credentials"},
    }, elapsed_minutes=12)
    assert expected in text
    assert "12 min elapsed" in text and "4 model requests" in text
    assert "last activity report 3 min ago" in text
    assert "No reliable completion estimate" in text
    assert "private" not in text and "/90" not in text


@pytest.mark.asyncio
async def test_platform_config_drives_contextual_delivery_and_stops_with_executor(monkeypatch):
    from gateway import run
    from gateway.config import Platform
    from gateway.session import SessionSource
    from gateway.turn_context import TurnContext

    config = {"display": {"long_running_notifications": "generic", "platforms": {
        "whatsapp_cloud": {"long_running_notifications": "contextual", "interim_assistant_messages": True},
    }}}
    monkeypatch.setattr(run, "_load_gateway_config", lambda: config)
    monkeypatch.setenv("HERMES_AGENT_NOTIFY_INTERVAL", "0.01")
    runner = object.__new__(run.GatewayRunner)
    runner._resolve_turn_toolsets = lambda *a: ([], [])
    runner._draining = runner._restart_requested = False
    source = SessionSource(platform=Platform.WHATSAPP_CLOUD, chat_id="test-chat", chat_type="dm")
    agent = MagicMock()
    agent.get_activity_summary.return_value = {
        "current_tool": "web_search", "api_call_count": 7,
        "last_activity_desc": "private search query", "seconds_since_activity": 1,
    }
    runner._running_agents = {"sess": agent}
    executor = asyncio.get_running_loop().create_future()
    adapter = MagicMock()
    adapter.supports_status_text = False

    async def delivered(*args, **kwargs):
        executor.set_result(None)
        return SimpleNamespace(success=True, message_id="status-1")

    adapter.send = AsyncMock(side_effect=delivered)
    runner._delivery_adapter_for = lambda source: adapter
    display = runner._run_agent_display_settings(source)
    assert display.interim_assistant_messages_enabled is True
    ctx = TurnContext(source=source, session_key="sess")
    ctx.agent_holder[0] = agent
    await asyncio.wait_for(runner._run_agent_notify_long_running(display, ctx, [executor]), 5)
    adapter.send.assert_awaited_once()
    text = adapter.send.call_args.args[1]
    assert "Searching the web" in text and "7 model requests" in text
    assert "private" not in text
    assert adapter.send.call_args.kwargs["metadata"]["_interim_send"] is True
