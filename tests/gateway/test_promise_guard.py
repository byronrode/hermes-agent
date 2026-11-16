"""Promise guard: classifier, fixtures, kill switch, fixed texts.

Every identity and chat here is synthetic; nothing touches a real number or channel. The live
model check at the bottom runs only with PROMISE_GUARD_LIVE=1 and calls the configured auxiliary
model with synthetic text (never a messaging provider).
"""

import asyncio
import json
import os
import time

import pytest

from gateway import feedback, promise_guard as pg

OWNER = "owner-test-1"

# Real replies from 15 Nov 2026 (session 20261022_195303_444f236e). The 21:30 "I'm making your
# version" text was not retained verbatim; that fixture is reconstructed from Byron's description.
PROMISES = {
    "restated_brief_22_18": (
        "Individual images. Like this,",
        "Yes. **20 separate transparent PNGs**, each tightly cropped to **only the clear jewel-case spine "
        "and original printed insert**. No black background, no front cover, no screenshot UI, no added labels."),
    "making_your_version_21_30": (
        "make me the same but with my albums",
        "I'm making your version now: your 20 records in the same CD-spine style."),
    "checking_for_scans_22_17": (
        "Individual images. Like this,",
        "I'm checking for actual spine scans for each of the 20 albums, then I'll send them individually."),
}
NEGATIVES = {
    "direct_answer": ("what time is the meeting?", "The meeting is at 15:00 in the Ignis channel."),
    "question_to_him": ("draft the email", "Which tone do you want: formal or friendly, and should Koen be on copy?"),
    "offer_as_question": ("is the build green?", "Yes, the build is green. Want me to open the release PR?"),
    "cannot_because": ("send me the X post", "I can't read that post because X blocks the fetch, so I have not ranked anything against it."),
    "reminder_set": ("remind me at 5", "Done. I set a reminder for 17:00 today: call the bank."),
    "past_tense_report": ("did you merge it", "I merged the PR at 14:02 and verified the staging build. Nothing else is pending."),
    "greeting": ("thanks", "You're welcome, Byron."),
    "explanation": ("how does the gate work?", "The gate only lets a WhatsApp message through when it answers a message you sent, so it holds anything proactive."),
    "result_in_text": ("top 3 albums", "Your top 3: 1. Phobia (326) 2. Bleed American (326) 3. Every Kingdom (306)."),
    "stopping_statement": ("do it", "I'm stopping here: the Sync key expired and I cannot reach Last.fm until you renew it."),
}
WITH_ATTACHMENT = ("make the image", "Here is your version.\n\nMEDIA:/tmp/synthetic/byron-20-records.jpg")


@pytest.fixture(autouse=True)
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_FEEDBACK_HOME", str(tmp_path))
    state = tmp_path / "state" / "feedback"
    state.mkdir(parents=True)
    (state / "config.json").write_text(json.dumps({
        "owners": {"whatsapp_cloud": [OWNER], "discord": [OWNER]},
        "realms": [{"platform": "whatsapp_cloud", "realm": "personal"}]}))
    return tmp_path


def fake(verdict):
    calls = []

    def call(messages, timeout):
        calls.append((messages, timeout))
        return verdict if isinstance(verdict, str) else json.dumps(verdict)
    call.calls = calls
    return call


# ── parsing ─────────────────────────────────────────────────────────────────

def test_parse_strict_json_and_fenced_json():
    assert pg.parse_verdict('{"promised_undelivered": true, "what": "make your version."}') == {
        "promised_undelivered": True, "what": "make your version"}
    assert pg.parse_verdict('```json\n{"promised_undelivered": false, "what": "x"}\n```') == {
        "promised_undelivered": False, "what": ""}


@pytest.mark.parametrize("raw", ["", "yes", "{}", '{"promised_undelivered": "true"}', "[true]", None, '{"what": "x"}'])
def test_parse_rejects_anything_not_strict(raw):
    with pytest.raises(Exception):
        pg.parse_verdict(raw)


def test_promised_without_a_phrase_gets_a_default_and_long_what_is_capped():
    assert pg.parse_verdict('{"promised_undelivered": true, "what": ""}')["what"]
    long = pg.parse_verdict(json.dumps({"promised_undelivered": True, "what": "x" * 500}))["what"]
    assert len(long) <= pg.WHAT_CAP


# ── prefilter ───────────────────────────────────────────────────────────────

def test_attachment_empty_and_very_long_replies_never_reach_the_model():
    call = fake({"promised_undelivered": True, "what": "x"})
    assert pg.classify(WITH_ATTACHMENT[1], WITH_ATTACHMENT[0], call=call)["skipped"] == "has_attachment"
    assert pg.classify("hello", has_media=True, call=call)["skipped"] == "has_attachment"
    assert pg.classify("   ", call=call)["skipped"] == "empty"
    assert pg.classify("I'll do it. " * 400, call=call)["skipped"] == "long_reply_is_a_result"
    assert pg.classify("I'll make it now. Sound good?", call=call)["skipped"] == "ends_with_a_question"
    assert pg.classify("Which tone do you want? **", call=call)["skipped"] == "ends_with_a_question"
    assert call.calls == []


# ── fixtures: both real promises are detected, the negatives are not ────────

@pytest.mark.parametrize("name", sorted(PROMISES))
def test_real_promise_reaches_the_classifier_with_its_context(name):
    request, reply = PROMISES[name]
    call = fake({"promised_undelivered": True, "what": "send the 20 spine images"})
    verdict = pg.classify(reply, request, call=call)
    assert verdict["promised_undelivered"] is True and verdict["what"] == "send the 20 spine images"
    messages, timeout = call.calls[0]
    assert messages[0]["content"] == pg.SYSTEM_PROMPT and timeout == pg.DEFAULT_TIMEOUT
    body = messages[1]["content"]
    assert reply[:40] in body and request in body and "attachment in this reply: no" in body


@pytest.mark.parametrize("name", sorted(NEGATIVES))
def test_negative_is_not_a_promise_when_the_model_says_so(name):
    request, reply = NEGATIVES[name]
    verdict = pg.classify(reply, request, call=fake({"promised_undelivered": False, "what": ""}))
    assert verdict["promised_undelivered"] is False and verdict["what"] == ""


# ── never blocks, never raises ──────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["not json", "", '{"promised_undelivered": "maybe"}'])
def test_garbage_answer_counts_as_not_a_promise(bad):
    assert pg.classify("I'm doing it.", call=fake(bad))["promised_undelivered"] is False


def test_provider_exception_counts_as_not_a_promise():
    def boom(messages, timeout):
        raise RuntimeError("provider down")
    verdict = pg.classify("I'm doing it.", call=boom)
    assert verdict["promised_undelivered"] is False and verdict["error"] == "RuntimeError"


def test_async_check_is_capped_by_the_timeout(home):
    (home / "state" / "promise_guard.json").write_text(json.dumps({"timeout": 0.2}))

    def slow(messages, timeout):
        time.sleep(2)
        return '{"promised_undelivered": true, "what": "x"}'

    async def timed():
        started = time.monotonic()
        verdict = await pg.classify_async("I'm doing it.", call=slow)
        return verdict, time.monotonic() - started

    verdict, elapsed = asyncio.run(timed())  # asyncio.run itself joins the sleeping worker at exit
    assert verdict["promised_undelivered"] is False and verdict["error"] == "timeout"
    assert elapsed < 1.0


def test_async_check_returns_the_verdict_when_fast():
    verdict = asyncio.run(pg.classify_async("I'm doing it.", call=fake({"promised_undelivered": True, "what": "do it"})))
    assert verdict["promised_undelivered"] is True


# ── kill switch and limits ──────────────────────────────────────────────────

def test_kill_switch_defaults_on_and_turns_off_only_when_explicit(home):
    path = home / "state" / "promise_guard.json"
    assert pg.enabled() is True                       # no file: on
    path.write_text("{ not json"); assert pg.enabled() is True
    path.write_text(json.dumps({"enabled": True})); assert pg.enabled() is True
    path.write_text(json.dumps({"enabled": False})); assert pg.enabled() is False
    path.write_text(json.dumps({"enabled": False, "max_continuations": 1}))
    assert pg.max_continuations() == 1
    path.write_text(json.dumps({})); assert pg.max_continuations() == 2 and pg.timeout_seconds() == 5.0


# ── identity: only Byron's authenticated reply is checked ───────────────────

def proof(**over):
    base = {"kind": "authenticated_reply", "platform": "whatsapp_cloud", "user_id": OWNER,
            "recipient": "chat-1", "message_id": "m1", "source_text": "hi"}
    return {**base, **over}


def test_only_the_verified_owner_on_whatsapp_or_discord_is_eligible():
    assert pg.eligible_proof(proof()) and pg.eligible_proof(proof(platform="discord"))
    assert not pg.eligible_proof(proof(user_id="someone-else"))
    assert not pg.eligible_proof(proof(kind="personal_reminder"))
    assert not pg.eligible_proof(proof(platform="telegram"))
    assert not pg.eligible_proof(None)


# ── fixed texts ─────────────────────────────────────────────────────────────

def test_fixed_instruction_and_stop_line_are_exact():
    assert pg.continuation_instruction("make your version\n") == (
        "You told Byron you would: make your version. Do it now in this turn and send the result. "
        "If you cannot, say exactly what is blocking it.")
    assert pg.stop_line("make your version") == "Not done yet: make your version. I have stopped; reply here to make me continue."
    assert pg.with_stop_line("I'm checking.\n", "check") == (
        "I'm checking.\n\nNot done yet: check. I have stopped; reply here to make me continue.")


# ── live model check (opt in) ───────────────────────────────────────────────

@pytest.mark.skipif(os.environ.get("PROMISE_GUARD_LIVE") != "1", reason="live auxiliary model; opt in")
def test_live_model_catches_both_real_promises_and_clears_the_negatives():
    rows = [(n, r, t, True) for n, (r, t) in PROMISES.items()]
    rows += [(n, r, t, False) for n, (r, t) in NEGATIVES.items()]
    wrong = []
    for name, request, reply, expected in rows:
        verdict = pg.classify(reply, request)
        print(f"{name}: {verdict}")
        if verdict["promised_undelivered"] is not expected:
            wrong.append((name, verdict))
    assert not wrong, wrong
