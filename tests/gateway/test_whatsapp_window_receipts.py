"""An accepted Graph send may report an expired window in a later signed webhook."""

import hashlib
import hmac
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway import whatsapp_chat_window as windows
from tests.gateway.test_whatsapp_cloud import _make_adapter, _mock_httpx_response, _post_request


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["after", "before", "duplicate", "wrong_chat", "wrong_phone",
                                  "unsigned", "other_error", "expired", "delivered"])
async def test_signed_window_failure_retries_only_its_undelivered_scheduled_message(tmp_path, monkeypatch, case):
    now = 1700000000
    monkeypatch.setattr(windows.time, "time", lambda: now)
    adapter = _make_adapter(app_secret="test-key")
    adapter._window_home = tmp_path
    adapter.config.extra = {"scheduled_template": {
        "name": "reminder", "language": "en", "header_source": "job_name",
    }}
    adapter._http_client = MagicMock()
    sends = []

    async def receipt(phase="failed"):
        status = {"id": "plain-id", "recipient_id": "wrong" if case == "wrong_chat" else "chat",
                  "status": phase, "errors": [{"code": 190 if case == "other_error" else 131047}]}
        payload = {"object": "whatsapp_business_account", "entry": [{"changes": [{
            "field": "messages", "value": {"metadata": {
                "phone_number_id": "wrong" if case == "wrong_phone" else adapter._phone_number_id,
            }, "statuses": [status]},
        }]}]}
        body = json.dumps(payload).encode()
        signature = hmac.new(adapter._app_secret.encode(), body, hashlib.sha256).hexdigest()
        response = await adapter._handle_webhook(_post_request(body, {
            "X-Hub-Signature-256": "sha256=" + ("invalid" if case == "unsigned" else signature),
        }))
        assert response.status == (401 if case == "unsigned" else 200)

    async def post(url, **kwargs):
        payload = kwargs["json"]
        sends.append(payload)
        if payload["type"] == "text" and case == "before":
            await receipt()
        return _mock_httpx_response(200, {"messages": [{"id": "plain-id" if payload["type"] == "text" else "fallback-id"}]})

    adapter._http_client.post = AsyncMock(side_effect=post)
    result = await adapter.send("chat", "Have a look at your notes.", metadata={"job_id": "job", "job_name": "Project notes"})
    assert result.success
    if case == "expired":
        monkeypatch.setattr(windows.time, "time", lambda: now + windows.FALLBACK_RECEIPT_SECONDS + 1)
    if case == "delivered":
        await receipt("delivered")
    await receipt()
    await receipt()
    expected_retry = case in {"after", "before", "duplicate"}
    assert [send["type"] for send in sends] == (["text", "template"] if expected_retry else ["text"])
    if expected_retry:
        assert sends[1]["to"] == "chat"
        assert sends[1]["template"]["components"][0]["parameters"][0]["text"] == "Project notes"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["success", "uncertain", "expired"])
async def test_confirmed_receipt_survives_restart_and_claims_one_fallback(tmp_path, monkeypatch, case):
    import asyncio
    now = 1700000000
    monkeypatch.setattr(windows.time, "time", lambda: now)
    template = {"name": "reminder", "language": "en"}
    windows.remember_send(tmp_path, "phone", "chat", "id", "A nudge.", template, "Notes")
    windows.remember_send(tmp_path, "phone", "chat", "id", rejected=True)
    assert windows.retry_keys(tmp_path / "other-profile", "phone") == []
    assert windows.retry_keys(tmp_path, "other-phone") == []
    if case == "expired":
        monkeypatch.setattr(windows.time, "time", lambda: now + windows.FALLBACK_RECEIPT_SECONDS + 1)
    adapter = _make_adapter(phone_number_id="phone")
    adapter._window_home = tmp_path
    adapter._http_client = MagicMock()
    adapter._http_client.post = AsyncMock(return_value=_mock_httpx_response(200, {"messages": [{"id": "fallback-id"}]}))
    if case == "uncertain":
        adapter._http_client.post.side_effect = TimeoutError("Uncertain fallback")
    await asyncio.gather(*(adapter._retry_window_rejection(chat, message)
                           for chat, message in windows.retry_keys(tmp_path, "phone") * 2))
    await adapter._retry_window_rejection("chat", "id")
    assert adapter._http_client.post.call_count == (0 if case == "expired" else 1)
