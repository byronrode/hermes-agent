"""A final reply the outbound policy refuses must never end in silence (15 Nov WhatsApp incident).

Mara built the image, the policy held her answer, and nothing told Byron. The turn now
derives custody for the adapter's text/attachment split, and when a part is still refused
it says so in the same chat.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.outbound_policy import authorized_reply_scope, derive_reply_grants, retain_final_reply, reply_handoff
from gateway.platforms.base import BasePlatformAdapter
from gateway.platforms.event import MessageEvent, MessageType
from gateway.session import SessionSource, build_session_key


class _Adapter(BasePlatformAdapter):
    def __init__(self, refuse_first=True) -> None:
        super().__init__(PlatformConfig(enabled=True, token="x"), Platform.WHATSAPP_CLOUD)
        self.sent, self.refuse_first = [], refuse_first

    async def connect(self): ...
    async def disconnect(self): ...
    async def get_chat_info(self, chat_id): return {}
    async def send_typing(self, chat_id, metadata=None): ...

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        from gateway.platforms.base import SendResult
        self.sent.append(content)
        if self.refuse_first and len(self.sent) <= 2:  # the send and its plain-text fallback
            return SendResult(success=False, error="outbound held: policy refused delivery")
        return SendResult(success=True, message_id="receipt")


def _event():
    source = SessionSource(platform=Platform.WHATSAPP_CLOUD, user_id="27840000000", chat_id="27840000000",
                           user_name="Byron", chat_type="dm")
    return MessageEvent(text="When do I get it?", message_type=MessageType.TEXT, source=source,
                        message_id="wamid.native", raw_message={"id": "wamid.native", "from": "27840000000"})


def _handler(response):
    runner = SimpleNamespace(_is_session_run_current=lambda *a: True,
                             _is_user_authorized_for_source=lambda *a, **k: True)

    async def handler(event):
        with authorized_reply_scope(runner, event, event.source, "session", 1):
            retain_final_reply(response)
        return response
    return handler


@pytest.mark.asyncio
async def test_refused_reply_is_followed_by_a_plain_notice_in_the_same_chat(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    adapter = _Adapter()
    adapter.set_message_handler(_handler("Your list is ready.\n\nhttps://example.org/list"))
    event = _event()
    await adapter._process_message_background(event, build_session_key(event.source))
    assert adapter.sent[0].startswith("Your list is ready.")
    assert adapter.sent[-1] == _Adapter._REPLY_NOT_SENT_NOTICE
    assert "did not send" in adapter.sent[-1] and "/" not in adapter.sent[-1]


@pytest.mark.asyncio
async def test_timeout_is_not_reported_as_unsent(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    adapter = _Adapter(refuse_first=False)
    async def send(chat_id, content, reply_to=None, metadata=None):
        from gateway.platforms.base import SendResult
        adapter.sent.append(content)
        return SendResult(success=False, error="request timed out")
    adapter.send = send
    adapter.set_message_handler(_handler("Your list is ready."))
    event = _event()
    await adapter._process_message_background(event, build_session_key(event.source))
    assert _Adapter._REPLY_NOT_SENT_NOTICE not in adapter.sent


@pytest.mark.asyncio
async def test_delivered_reply_sends_no_notice(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    adapter = _Adapter(refuse_first=False)
    adapter.set_message_handler(_handler("Your list is ready."))
    event = _event()
    await adapter._process_message_background(event, build_session_key(event.source))
    assert adapter.sent == ["Your list is ready."]


def test_media_tag_reply_carries_custody_to_text_and_attachment(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    runner = SimpleNamespace(_is_session_run_current=lambda *a: True,
                             _is_user_authorized_for_source=lambda *a, **k: True)
    event = _event()
    response = "Here it is.\n\nMEDIA:/tmp/picture.jpg"
    with authorized_reply_scope(runner, event, event.source, "session", 1):
        retain_final_reply(response)
    adapter = _Adapter()
    extracted = SimpleNamespace(text_content="Here it is.", images=[], media_files=[("/tmp/picture.jpg", False)],
                                local_files=[])
    adapter._derive_reply_custody(event, response, extracted)
    assert reply_handoff("whatsapp_cloud", "27840000000", "Here it is.") is not None
    assert reply_handoff("whatsapp_cloud", "27840000000", "") is not None
    assert reply_handoff("whatsapp_cloud", "27840000000", "Different text") is None
