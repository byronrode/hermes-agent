"""CLI and scheduled sends reuse the Cloud adapter without a webhook server."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from gateway.config import Platform, PlatformConfig
from tools import send_message_tool as sends


@pytest.mark.parametrize("scheduled", [False, True])
def test_cloud_send_without_gateway_preserves_transport_and_template(monkeypatch, tmp_path, scheduled):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(sends, "_live_adapter", lambda _: (None, None))
    config = PlatformConfig(extra={"phone_number_id": "123", "access_token": "test-token",
                                   "scheduled_template": {"name": "reminder", "language": "en"}})
    client = MagicMock()
    client.post = AsyncMock(return_value=httpx.Response(200, json={"messages": [{"id": "receipt"}]}))
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=client)
    context.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: context)
    if scheduled:
        from cron.scheduler_delivery import _standalone_send
        target = SimpleNamespace(job={"id": "test-job"}, platform=Platform.WHATSAPP_CLOUD,
                                 pconfig=config, chat_id="456", thread_id=None, where="test")
        result, error = _standalone_send(target, "Requested result", [])
        assert error is None
    else:
        result = asyncio.run(sends._send_to_platform(Platform.WHATSAPP_CLOUD, config, "456", "Requested result"))
    assert result == {"success": True, "message_id": "receipt"}
    request = client.post.call_args
    assert request.args[0].endswith("/123/messages")
    assert request.kwargs["headers"]["Authorization"] == "Bearer test-token"
    payload = request.kwargs["json"]
    assert payload["to"] == "456"
    assert payload["type"] == ("template" if scheduled else "text")
    if scheduled:
        assert payload["template"]["name"] == "reminder"
    else:
        assert payload["text"]["body"] == "Requested result"
    context.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("failure", ["credential", "graph"])
def test_cloud_standalone_failure_is_not_success(monkeypatch, tmp_path, failure):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(sends, "_live_adapter", lambda _: (None, None))
    config = PlatformConfig(extra={"phone_number_id": "123", "access_token": "" if failure == "credential" else "test-token"})
    client = MagicMock()
    client.post = AsyncMock(return_value=httpx.Response(400, json={"error": {"code": 132001, "message": "Template unavailable"}}))
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=client)
    context.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_: context)
    result = asyncio.run(sends._send_to_platform(Platform.WHATSAPP_CLOUD, config, "456", "Requested result"))
    assert result.get("error")
    assert not result.get("success")
    if failure == "credential":
        client.post.assert_not_called()
    else:
        assert "132001" in result["error"]
