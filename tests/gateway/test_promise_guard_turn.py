"""Promise guard wired into a gateway turn: continuation, cap, stop line, open item, kill switch,
and the outbound gate's authenticated-reply custody across the continuation.

Everything is synthetic: fake agent, capture adapter, a throwaway Hermes home. Nothing is sent.
"""

import json
from types import SimpleNamespace

import pytest

from gateway import feedback, outbound_policy, promise_guard as pg
from gateway.config import Platform
from gateway.platforms.event import MessageEvent, MessageType, SessionSource
from gateway.run import GatewayRunner  # noqa: F401  (imported at collection time, like the sibling turn tests)
from tests.gateway.test_run_progress_topics import ProgressCaptureAdapter, _make_runner, _run_with_agent

OWNER = "owner-test-1"
PROMISE = "I'm making your version now."
RESULT = "Here is your version: 20 albums, one per artist."
INSTRUCTION = pg.continuation_instruction("make your version")


@pytest.fixture(autouse=True)
def guard_home(tmp_path, monkeypatch):
    home = tmp_path / "feedback-home"
    state = home / "state" / "feedback"
    state.mkdir(parents=True)
    (state / "config.json").write_text(json.dumps({
        "owners": {"whatsapp_cloud": [OWNER]}, "realms": [{"platform": "whatsapp_cloud", "realm": "personal"}]}))
    monkeypatch.setenv("HERMES_FEEDBACK_HOME", str(home))
    return home


def scripted_agent(replies, seen):
    class Agent:
        def __init__(self, **kwargs):
            self.tools = []

        def run_conversation(self, message, conversation_history=None, task_id=None, **kwargs):
            seen.append(message)
            text = replies[min(len(seen) - 1, len(replies) - 1)]
            return {"final_response": text, "messages": [], "api_calls": 1}
    return Agent


def scripted_model(monkeypatch, verdicts):
    """Replace only the auxiliary model call; the guard's prompt, parsing and rules run for real."""
    asked = []

    def call(messages, timeout):
        asked.append(messages[1]["content"])
        verdict = verdicts[min(len(asked) - 1, len(verdicts) - 1)]
        if isinstance(verdict, Exception):
            raise verdict
        return json.dumps(verdict)
    monkeypatch.setattr(pg, "_call_auxiliary", call)
    return asked


YES = {"promised_undelivered": True, "what": "make your version"}
NO = {"promised_undelivered": False, "what": ""}


def owner_proof(monkeypatch, user_id=OWNER, kind="authenticated_reply"):
    proof = {"kind": kind, "platform": "whatsapp_cloud", "user_id": user_id, "recipient": "27800000000",
             "message_id": "wamid.TEST", "source_text": "make me the same but with my albums", "home_scope": "x"}
    monkeypatch.setattr(outbound_policy, "current_provenance", lambda content: proof)


async def run_turn(monkeypatch, tmp_path, replies, seen):
    return await _run_with_agent(
        monkeypatch, tmp_path, scripted_agent(replies, seen), session_id="sess-guard",
        platform=Platform.WHATSAPP_CLOUD, chat_id="27800000000", chat_type="dm", thread_id=None, user_id=OWNER)


def spooled(home):
    records = []
    for path in sorted((home / "state" / "feedback" / "spool").glob("*.jsonl")) if (home / "state" / "feedback" / "spool").exists() else []:
        records += [json.loads(line) for line in path.read_text().splitlines()]
    return records


@pytest.mark.asyncio
async def test_a_promise_is_continued_in_the_same_turn_with_the_fixed_instruction(monkeypatch, tmp_path, guard_home):
    owner_proof(monkeypatch)
    asked = scripted_model(monkeypatch, [YES, NO])
    seen = []
    adapter, result = await run_turn(monkeypatch, tmp_path, [PROMISE, RESULT], seen)

    assert seen == ["hello", INSTRUCTION]               # the second run got exactly the fixed instruction
    assert result["final_response"] == RESULT           # the turn's final reply is the delivered result
    assert [c["content"] for c in adapter.sent] == [PROMISE]  # the promise itself was delivered first
    assert len(asked) == 2 and "make me the same but with my albums" in asked[0]
    assert spooled(guard_home) == []                    # delivered: nothing to wait on


@pytest.mark.asyncio
async def test_two_continuations_then_the_stop_line_and_an_open_item(monkeypatch, tmp_path, guard_home):
    owner_proof(monkeypatch)
    scripted_model(monkeypatch, [YES])
    seen = []
    adapter, result = await run_turn(monkeypatch, tmp_path, [PROMISE, "Still checking.", "Still working on it."], seen)

    assert seen == ["hello", INSTRUCTION, INSTRUCTION]  # exactly two automatic continuations
    assert result["final_response"] == (
        "Still working on it.\n\nNot done yet: make your version. I have stopped; reply here to make me continue.")
    assert [c["content"] for c in adapter.sent] == [PROMISE, "Still checking."]
    (item,) = spooled(guard_home)
    assert item["type"] == "open_item" and item["kind"] == "promise_not_delivered"
    assert item["what"] == "make your version" and item["continuations"] == 2
    assert item["channel"] == "whatsapp_cloud" and item["realm"] == "personal"
    assert item["key"] == "promise:whatsapp_cloud:27800000000:wamid.TEST"
    assert item["request_text"] == "make me the same but with my albums" and "Still working" in item["reply_text"]


@pytest.mark.asyncio
async def test_a_blocker_statement_ends_the_chain_without_a_stop_line(monkeypatch, tmp_path, guard_home):
    owner_proof(monkeypatch)
    scripted_model(monkeypatch, [YES, NO])
    seen = []
    _adapter, result = await run_turn(monkeypatch, tmp_path, [PROMISE, "I can't: the image key expired."], seen)
    assert seen == ["hello", INSTRUCTION] and result["final_response"] == "I can't: the image key expired."
    assert spooled(guard_home) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("why", ["kill_switch", "not_owner", "not_authenticated", "internal_event", "classifier_down"])
async def test_nothing_changes_when_the_guard_must_stay_out(monkeypatch, tmp_path, guard_home, why):
    asked = scripted_model(monkeypatch, [RuntimeError("provider down") if why == "classifier_down" else YES])
    if why == "not_owner":
        owner_proof(monkeypatch, user_id="someone-else")
    elif why == "not_authenticated":
        monkeypatch.setattr(outbound_policy, "current_provenance", lambda content: None)
    elif why == "internal_event":
        owner_proof(monkeypatch, kind="personal_reminder")
    else:
        owner_proof(monkeypatch)
    if why == "kill_switch":
        (guard_home / "state" / "promise_guard.json").write_text(json.dumps({"enabled": False}))
    seen = []
    adapter, result = await run_turn(monkeypatch, tmp_path, [PROMISE], seen)

    assert seen == ["hello"] and result["final_response"] == PROMISE and adapter.sent == []
    assert spooled(guard_home) == []
    if why == "classifier_down":
        assert len(asked) == 1                          # asked once, failed, reply left alone
    else:
        assert asked == []                              # never even asked


@pytest.mark.asyncio
async def test_a_reply_with_an_attachment_is_never_called_a_promise(monkeypatch, tmp_path, guard_home):
    owner_proof(monkeypatch)
    asked = scripted_model(monkeypatch, [YES])
    seen = []
    _a, result = await run_turn(monkeypatch, tmp_path, ["Here it is.\n\nMEDIA:/tmp/synthetic/x.jpg"], seen)
    assert seen == ["hello"] and asked == [] and "MEDIA:" in result["final_response"]


@pytest.mark.asyncio
async def test_max_continuations_is_configurable(monkeypatch, tmp_path, guard_home):
    owner_proof(monkeypatch)
    scripted_model(monkeypatch, [YES])
    (guard_home / "state" / "promise_guard.json").write_text(json.dumps({"max_continuations": 1}))
    seen = []
    _a, result = await run_turn(monkeypatch, tmp_path, [PROMISE, "Still on it."], seen)
    assert seen == ["hello", INSTRUCTION] and result["final_response"].endswith("reply here to make me continue.")


# ── the outbound gate: the continuation is still an authenticated reply ─────

class _Runner(SimpleNamespace):
    def _is_session_run_current(self, key, generation):
        return True

    def _is_user_authorized_for_source(self, source, allow_adapter_delegation=True):
        return True


@pytest.mark.asyncio
async def test_the_continuation_keeps_the_authenticated_reply_custody(monkeypatch, tmp_path, guard_home):
    """Run the whole chain inside the real ``authorized_reply_scope`` of a native-valid WhatsApp
    message. Every send, including the one before the continuation and the continuation's own, must
    see the authenticated-reply proof; an internal event gets none (that is what held the 22:16 ack)."""
    seen_proofs = []

    class SpyAdapter(ProgressCaptureAdapter):
        async def send(self, chat_id, content, reply_to=None, metadata=None):
            seen_proofs.append((content, (outbound_policy.current_provenance(content) or {}).get("kind")))
            return await super().send(chat_id, content, reply_to=reply_to, metadata=metadata)

    scripted_model(monkeypatch, [YES, NO])
    runner_stub = _Runner()
    source = SessionSource(platform=Platform.WHATSAPP_CLOUD, chat_id="27800000000", chat_type="dm", user_id=OWNER)
    event = MessageEvent(text="make me the same but with my albums", message_type=MessageType.TEXT, source=source,
                         message_id="wamid.TEST", raw_message={"id": "wamid.TEST", "from": OWNER})
    internal = MessageEvent(text="tick", message_type=MessageType.TEXT, source=source, message_id="wamid.TEST",
                            raw_message={"id": "wamid.TEST", "from": OWNER}, internal=True)

    with outbound_policy.authorized_reply_scope(runner_stub, internal, source, "k", 1):
        assert outbound_policy.current_provenance("x") is None          # internal: no custody

    seen = []
    with outbound_policy.authorized_reply_scope(runner_stub, event, source, "k", 1):
        adapter, result = await _run_with_agent(
            monkeypatch, tmp_path, scripted_agent([PROMISE, RESULT], seen), session_id="sess-custody",
            platform=Platform.WHATSAPP_CLOUD, chat_id="27800000000", chat_type="dm", thread_id=None,
            user_id=OWNER, adapter_cls=SpyAdapter)

    assert seen == ["hello", INSTRUCTION] and result["final_response"] == RESULT
    assert seen_proofs == [(PROMISE, "authenticated_reply")]            # the pre-continuation send is custodial
    assert outbound_policy.current_provenance("x") is None              # scope closed afterwards


# ── the open item leaves the list when Byron writes again ───────────────────

def _state(chat="27800000000"):
    return pg.new_state({"platform": "whatsapp_cloud", "recipient": chat, "user_id": OWNER, "message_id": "wamid.TEST"},
                        "ask", SimpleNamespace(chat_type="dm", scope_id=None, profile=None))


def test_byrons_next_message_in_that_chat_clears_the_open_item(guard_home):
    pg.record_open_item(_state(), "send the images", reply="I'm checking.")
    assert [r["type"] for r in spooled(guard_home)] == ["open_item"]
    assert pg.note_owner_message("whatsapp_cloud", "27899999999", OWNER) is False       # another chat
    assert pg.note_owner_message("whatsapp_cloud", "27800000000", "someone-else") is False  # not him
    assert pg.note_owner_message("whatsapp_cloud", "27800000000", OWNER) is True
    cleared = spooled(guard_home)[-1]
    assert cleared["type"] == "open_item_clear" and cleared["chat_id"] == "27800000000"
    assert cleared["key"] == "promise:whatsapp_cloud:27800000000:wamid.TEST"
    assert pg.note_owner_message("whatsapp_cloud", "27800000000", OWNER) is False          # once only
    assert [r["type"] for r in spooled(guard_home)] == ["open_item", "open_item_clear"]


def test_begin_turn_clears_the_open_item_for_his_real_message(guard_home):
    pg.record_open_item(_state(), "send the images")
    source = SimpleNamespace(platform=Platform.WHATSAPP_CLOUD, chat_id="27800000000", user_id=OWNER, chat_type="dm",
                             thread_id=None, scope_id=None, profile=None)
    event = SimpleNamespace(channel_prompt=None, auto_skill=None, reply_to_message_id=None, message_id="m2", text="continue")
    feedback.begin_turn(event, source, "sess", "turn")
    assert spooled(guard_home)[-1]["type"] == "open_item_clear"


def test_nothing_is_spooled_for_an_ordinary_message(guard_home):
    source = SimpleNamespace(platform=Platform.WHATSAPP_CLOUD, chat_id="27800000000", user_id=OWNER, chat_type="dm",
                             thread_id=None, scope_id=None, profile=None)
    event = SimpleNamespace(channel_prompt=None, auto_skill=None, reply_to_message_id=None, message_id="m2", text="hi")
    feedback.begin_turn(event, source, "sess", "turn")
    assert spooled(guard_home) == []
