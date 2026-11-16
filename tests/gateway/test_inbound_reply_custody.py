"""Every answer to Byron's own WhatsApp message gets through the outbound gate (16 Nov incident).

He typed /restart and /reset; both ran, both answers were refused by the gate because only the
agent-turn handler held reply custody. Custody is now taken once, when the adapter accepts the
message, for the whole handling of it. The real Mara policy decides here; nothing is sent and
every id is synthetic.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway import feedback, outbound_policy
from gateway.config import Platform, PlatformConfig
from gateway.outbound_policy import evaluate as _evaluate
from gateway.platforms.base import BasePlatformAdapter, EphemeralReply, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.session import SessionSource, build_session_key
from tests.gateway.restart_test_helpers import make_restart_runner

POLICY = Path.home() / ".local/share/mara-orchestration/plugins/mara-outbound/policy.py"
BYRON = "27840000000"
OTHER = "27849999999"


def evaluate(*args, **kwargs):
    """The real decision. The gate treats anything run under pytest as a test and holds it from a real
    recipient, which would hide the custody rules these tests are about."""
    saved = os.environ.pop("PYTEST_CURRENT_TEST", None)
    try:
        return _evaluate(*args, **kwargs)
    finally:
        if saved is not None:
            os.environ["PYTEST_CURRENT_TEST"] = saved


@pytest.fixture(autouse=True)
def gate(tmp_path, monkeypatch):
    if not POLICY.exists():
        pytest.skip("the Mara outbound policy is not installed on this host")
    home = tmp_path / "hermes-home"
    home.mkdir()
    (home / "config.yaml").write_text(json.dumps({"outbound_policy": {
        "required": True, "callback_file": str(POLICY), "settings": {"real_recipients": [BYRON]}}}))
    monkeypatch.setenv("HERMES_HOME", str(home))
    state = tmp_path / "feedback-home" / "state" / "feedback"
    state.mkdir(parents=True)
    (state / "config.json").write_text(json.dumps({
        "owners": {"whatsapp_cloud": [BYRON]}, "realms": [{"platform": "whatsapp_cloud", "realm": "personal"}]}))
    monkeypatch.setenv("HERMES_FEEDBACK_HOME", str(tmp_path / "feedback-home"))
    outbound_policy._HELD_REPLY_SEEN.clear()
    return home


class Gated(BasePlatformAdapter):
    """Sends through the real gate, like WhatsApp Cloud's transport does."""

    def __init__(self, home, authorized=True, run_current=False):
        super().__init__(PlatformConfig(enabled=True, token="x"), Platform.WHATSAPP_CLOUD)
        self.home, self.delivered, self.held = home, [], []
        # The session generation is INVALIDATED here on purpose: /reset and /new end the run they answer.
        self.gateway_runner = SimpleNamespace(
            _is_session_run_current=lambda *a: run_current,
            _is_user_authorized_for_source=lambda *a, **k: authorized)

    async def connect(self): ...
    async def disconnect(self): ...
    async def get_chat_info(self, chat_id): return {}
    async def send_typing(self, chat_id, metadata=None): ...
    def _is_dm_allowed(self, chat_id): return str(chat_id) == BYRON

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        receipt = evaluate("whatsapp_cloud", chat_id, content, profile_home=self.home, operation="emit")
        if receipt.get("allow") is not True:
            self.held.append(content)
            return SendResult(success=False, error=receipt["error"])
        self.delivered.append(content)
        return SendResult(success=True, message_id="receipt")


def event_from(sender=BYRON, text="hello", *, forged=False, internal=False):
    source = SessionSource(platform=Platform.WHATSAPP_CLOUD, user_id=sender, chat_id=sender,
                           user_name="Byron", chat_type="dm")
    event = MessageEvent(text=text, message_type=MessageType.TEXT, source=source, message_id="wamid.cmd",
                         raw_message={"id": "wamid.cmd", "from": OTHER if forged else sender})
    event.internal = internal
    return event


async def process(adapter, event, handler):
    adapter.set_message_handler(handler)
    await adapter._process_message_background(event, build_session_key(event.source))


# ── what now gets through ───────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("command,answer", [
    ("/restart", EphemeralReply("♻ Restarting the gateway.")),
    ("/reset", "✨ Session reset! Starting fresh."),
    ("/help", "Commands: /new /reset /stop /status /model — see ~/.hermes/skills for more (v1.2.3)"),
    ("/status", "Session abc running in /Users/someone/project; 3 turns."),
])
async def test_slash_command_answers_are_delivered(gate, command, answer):
    adapter = Gated(gate)
    async def handler(event):
        return answer
    await process(adapter, event_from(text=command), handler)
    assert adapter.delivered == [str(answer)] and adapter.held == []


@pytest.mark.asyncio
async def test_error_reply_is_delivered(gate):
    adapter = Gated(gate)
    async def handler(event):
        raise RuntimeError("model unreachable")
    await process(adapter, event_from(text="what is the time"), handler)
    assert len(adapter.delivered) == 1 and "error" in adapter.delivered[0].lower() and adapter.held == []


@pytest.mark.asyncio
async def test_mid_turn_interim_message_is_delivered(gate):
    adapter = Gated(gate)
    async def handler(event):
        await adapter.send(event.source.chat_id, "Still working on it.")
        return "Done."
    await process(adapter, event_from(text="build it"), handler)
    assert adapter.delivered == ["Still working on it.", "Done."] and adapter.held == []


@pytest.mark.asyncio
async def test_busy_acknowledgement_to_a_message_that_arrives_mid_turn_is_delivered(gate):
    adapter = Gated(gate)
    async def busy(event, session_key):
        await adapter.send(event.source.chat_id, "⏳ Queued for the next turn.")
        return True
    adapter.set_busy_session_handler(busy)
    event = event_from(text="and also this")
    await adapter._handle_message_while_active(event, build_session_key(event.source))
    assert adapter.delivered == ["⏳ Queued for the next turn."] and adapter.held == []


@pytest.mark.asyncio
async def test_bypass_command_while_busy_is_delivered(gate):
    adapter = Gated(gate)
    async def handler(event):
        return "⚡ Stopped."
    adapter.set_message_handler(handler)
    event = event_from(text="/stop")
    await adapter._handle_message_while_active(event, build_session_key(event.source))
    assert adapter.delivered == ["⚡ Stopped."] and adapter.held == []


# ── what is still held ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cron_and_system_output_is_still_held(gate):
    adapter = Gated(gate)
    await adapter.send(BYRON, "Nightly job finished: 4 files changed")          # cron/system, no inbound message
    await adapter.send(BYRON, "♻️ Gateway online — Hermes is back and ready.")  # home-channel startup notice
    assert adapter.delivered == [] and len(adapter.held) == 2


@pytest.mark.asyncio
async def test_custody_ends_with_the_message(gate):
    adapter = Gated(gate)
    async def handler(event):
        return "ok"
    await process(adapter, event_from(), handler)
    await adapter.send(BYRON, "A later system notice")
    assert adapter.delivered == ["ok"] and adapter.held == ["A later system notice"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", [
    {"internal": True},      # a system wake, not his message
    {"forged": True},        # the native envelope does not match the sender
], ids=["internal-event", "unverified-envelope"])
async def test_events_that_are_not_his_authenticated_message_get_no_custody(gate, kwargs):
    adapter = Gated(gate)
    async def handler(event):
        return "A notification"
    await process(adapter, event_from(**kwargs), handler)
    assert adapter.delivered == [] and adapter.held[0] == "A notification"


@pytest.mark.asyncio
async def test_unauthorized_sender_gets_no_custody(gate):
    adapter = Gated(gate, authorized=False)
    async def handler(event):
        return "hello stranger"
    await process(adapter, event_from(), handler)
    assert adapter.delivered == []


# ── a held reply is not silent ──────────────────────────────────────────────

def spooled(tmp_path):
    files = sorted((tmp_path / "feedback-home" / "state" / "feedback" / "spool").glob("promise-*.jsonl"))
    return [json.loads(line) for f in files for line in f.read_text().splitlines()]


@pytest.mark.asyncio
async def test_held_reply_reaches_the_waiting_list(gate, tmp_path):
    adapter = Gated(gate)
    async def handler(event):
        return "Your answer"
    await process(adapter, event_from(text="/restart", forged=True), handler)
    items = [r for r in spooled(tmp_path) if r.get("type") == "open_item"]
    assert len(items) == 1
    item = items[0]
    assert item["kind"] == "reply_held" and item["channel"] == "whatsapp_cloud" and item["chat_id"] == BYRON
    assert item["realm"] == "personal" and "/restart" in item["what"] and item["reply_text"] == "Your answer"
    assert item["key"] == f"replyheld:whatsapp_cloud:{BYRON}:wamid.cmd"


@pytest.mark.asyncio
async def test_held_cron_output_and_strangers_do_not_reach_the_waiting_list(gate, tmp_path):
    adapter = Gated(gate)
    await adapter.send(BYRON, "Nightly job finished")
    async def handler(event):
        return "hi"
    stranger = event_from(sender=OTHER)
    await process(adapter, stranger, handler)           # held, but not Byron's identity
    assert spooled(tmp_path) == []


# ── /restart: the answer after the restart ──────────────────────────────────

NOTICE = "♻ Gateway restarted successfully. Your session continues."


def record_from_command(gate):
    """What the /restart handler writes into .restart_notify.json, built under a real inbound scope."""
    from gateway.slash_commands import _restart_notify_payload
    adapter = Gated(gate)
    event = event_from(text="/restart")
    with adapter._inbound_reply_scope(event):
        return _restart_notify_payload(event)


def test_restart_record_carries_expiring_custody_only_for_his_own_command(gate):
    record = record_from_command(gate)
    custody = record["reply_custody"]
    assert custody["user_id"] == BYRON and custody["recipient"] == BYRON and custody["message_id"] == "wamid.cmd"
    assert 0 < custody["expires_at"] - custody["issued_at"] <= 900
    from gateway.slash_commands import _restart_notify_payload
    assert "reply_custody" not in _restart_notify_payload(event_from(text="/restart"))           # no scope
    with Gated(gate)._inbound_reply_scope(event_from(internal=True)):
        assert "reply_custody" not in _restart_notify_payload(event_from(internal=True))         # not his message


def test_restart_notice_is_delivered_once_for_exactly_that_text(gate):
    record = record_from_command(gate)
    with outbound_policy.restart_reply_scope(record, NOTICE):
        assert evaluate("whatsapp_cloud", BYRON, NOTICE, profile_home=gate, operation="emit")["allow"] is True
        assert evaluate("whatsapp_cloud", BYRON, "Another message", profile_home=gate)["allow"] is not True
    assert evaluate("whatsapp_cloud", BYRON, NOTICE, profile_home=gate)["allow"] is not True      # scope over


def test_restart_custody_expires_and_must_match_the_profile_and_chat(gate, tmp_path):
    record = record_from_command(gate)
    def allowed(rec, **kw):
        with outbound_policy.restart_reply_scope(rec, NOTICE, **kw):
            return evaluate("whatsapp_cloud", BYRON, NOTICE, profile_home=gate)["allow"] is True
    assert allowed(record)
    assert not allowed(json.loads(json.dumps({**record, "reply_custody": {**record["reply_custody"], "expires_at": 1}})))
    assert not allowed({**record, "reply_custody": {**record["reply_custody"], "home_scope": str(tmp_path / "other")}})
    assert not allowed({**record, "chat_id": OTHER})
    assert not allowed({k: v for k, v in record.items() if k != "reply_custody"})
    assert not allowed(record, still_authorized=lambda: False)


@pytest.mark.asyncio
async def test_send_restart_notification_delivers_through_the_gate_and_consumes_the_record(gate, monkeypatch):
    monkeypatch.setattr(gateway_run, "_hermes_home", gate)
    adapter = Gated(gate)
    runner, _native = make_restart_runner()
    runner.adapters = {Platform.WHATSAPP_CLOUD: adapter}
    runner.config.platforms = {Platform.WHATSAPP_CLOUD: PlatformConfig(enabled=True, token="x")}
    notify = gate / ".restart_notify.json"
    notify.write_text(json.dumps(record_from_command(gate)))
    assert await runner._send_restart_notification() is not None
    assert adapter.delivered == [NOTICE] and not notify.exists()
    assert await runner._send_restart_notification() is None                 # single use: the record is gone
    # The same notice without his command's record (e.g. a plain startup) is still held.
    notify.write_text(json.dumps({"platform": "whatsapp_cloud", "chat_id": BYRON, "chat_type": "dm"}))
    await runner._send_restart_notification()
    assert adapter.delivered == [NOTICE] and adapter.held == [NOTICE]
