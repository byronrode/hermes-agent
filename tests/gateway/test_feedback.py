"""Votes and lessons: Byron's 👍/👎 on Mara's messages and what is learned from a 👎 plus a reply.

All identities, chats and realms here are synthetic; nothing touches a real number or channel.
"""

import json
import types

import pytest

from gateway import feedback

OWNER = "owner-test-1"
STRANGER = "someone-else"
UP, DOWN = "\U0001F44D", "\U0001F44E"
HOUR = 3600


@pytest.fixture(autouse=True)
def feedback_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_FEEDBACK_HOME", str(tmp_path))
    state = tmp_path / "state" / "feedback"
    state.mkdir(parents=True)
    (state / "config.json").write_text(json.dumps({
        "owners": {"wa": [OWNER], "dc": [OWNER]},
        "realms": [
            {"platform": "wa", "realm": "personal"},
            {"platform": "dc", "chat_type": "dm", "realm": "personal"},
            {"platform": "dc", "scope_id": "guild-ignis", "realm": "ignis"},
        ]}))
    return tmp_path


def spooled(home):
    records = []
    for path in sorted((home / "state" / "feedback" / "spool").glob("*.jsonl")):
        records += [json.loads(line) for line in path.read_text().splitlines()]
    return records


def send(message_id="m1", text="Here is the plan.", platform="wa", chat="chat-1", **extra):
    kwargs = {"chat_type": "dm"} if platform == "wa" else {"chat_type": "group", "scope_id": "guild-ignis"}
    feedback.note_outbound(platform, chat, message_id, text, **kwargs)
    feedback.note_turn(platform, chat, [message_id], text, request_id="in-1", request_text="make a plan",
                       session_key="sess-key", session_id="sess-1", turn_id="turn-1", role="mara",
                       skill=extra.get("skill"), **kwargs)


# ── slice 1: capture ────────────────────────────────────────────────────────

def test_thumbs_down_is_recorded_with_the_exact_message_and_its_request(feedback_home):
    send()
    record = feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)
    assert record["vote"] == "down" and record["realm"] == "personal"
    assert record["message_text"] == "Here is the plan." and record["request_text"] == "make a plan"
    assert record["session_id"] == "sess-1" and record["turn_id"] == "turn-1" and record["role"] == "mara"
    assert spooled(feedback_home) == [record | {"id": spooled(feedback_home)[0]["id"]}]


def test_only_the_owner_counts(feedback_home):
    send()
    assert feedback.record_reaction("wa", "chat-1", "m1", STRANGER, DOWN) is None
    assert feedback.record_reaction("wa", "chat-1", "m1", None, DOWN) is None
    assert spooled(feedback_home) == []


def test_other_emoji_and_unknown_messages_are_not_votes(feedback_home):
    send()
    assert feedback.record_reaction("wa", "chat-1", "m1", OWNER, "❤️") is None
    assert feedback.record_reaction("wa", "chat-1", "not-mara", OWNER, UP) is None  # not a message of ours
    assert spooled(feedback_home) == []


def test_skin_tone_thumbs_count_and_changing_or_removing_updates(feedback_home):
    send()
    assert feedback.record_reaction("wa", "chat-1", "m1", OWNER, UP + "\U0001F3FD")["vote"] == "up"
    assert feedback.record_reaction("wa", "chat-1", "m1", OWNER, UP) is None  # no change, no new record
    assert feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)["vote"] == "down"
    assert feedback.record_reaction("wa", "chat-1", "m1", OWNER, None, removed=True)["vote"] == "none"  # WhatsApp: no emoji
    assert feedback.record_reaction("wa", "chat-1", "m1", OWNER, None, removed=True) is None  # already clear
    assert feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)["vote"] == "down"
    assert feedback.record_reaction("wa", "chat-1", "m1", OWNER, "\u2764\ufe0f")["vote"] == "none"  # replaced by another emoji
    assert [r["vote"] for r in spooled(feedback_home)] == ["up", "down", "none", "down", "none"]


def test_discord_removing_a_reaction_that_is_not_the_current_vote_changes_nothing(feedback_home):
    send(platform="dc", chat="chan-1")
    feedback.record_reaction("dc", "chan-1", "m1", OWNER, UP)
    feedback.record_reaction("dc", "chan-1", "m1", OWNER, DOWN)  # both stacked; latest wins
    assert feedback.record_reaction("dc", "chan-1", "m1", OWNER, UP, removed=True) is None
    assert [r["vote"] for r in spooled(feedback_home)] == ["up", "down"]


def test_discord_message_the_index_never_saw_counts_when_discord_says_it_is_ours(feedback_home):
    record = feedback.record_reaction("dc", "chan-9", "m9", OWNER, DOWN, mara_authored=True,
                                      chat_type="group", scope_id="guild-ignis")
    assert record["vote"] == "down" and record["realm"] == "ignis" and record["message_text"] == ""
    assert feedback.record_reaction("dc", "chan-9", "m10", OWNER, DOWN, mara_authored=False) is None


# ── slice 2: lessons ────────────────────────────────────────────────────────

def test_thumbs_down_then_reply_writes_a_verbatim_lesson(feedback_home):
    send(skill="planning")
    feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)
    words = "Wrong.\n  Use the Ignis plan, not\tthe personal one.\n\nAlways say which."
    lesson_id = feedback.note_reply("wa", "chat-1", OWNER, "r1", "m1", words)
    [lesson] = feedback.list_lessons("personal")
    assert lesson["id"] == lesson_id and lesson["words"] == words  # verbatim, through the file
    assert lesson["request"] == "make a plan" and lesson["bad_reply"] == "Here is the plan."
    assert lesson["scope"] == "wa · dm · role mara · skill planning"
    assert [r["type"] for r in spooled(feedback_home)] == ["vote", "lesson"]
    assert spooled(feedback_home)[1]["words"] == words


def test_reply_first_then_thumbs_down_is_the_same_lesson(feedback_home):
    send()
    assert feedback.note_reply("wa", "chat-1", OWNER, "r1", "m1", "No, shorter please.") is None  # no vote yet
    assert feedback.list_lessons("personal") == []
    feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)
    assert [l["words"] for l in feedback.list_lessons("personal")] == ["No, shorter please."]


def test_pairing_is_by_replied_to_id_not_by_guessing(feedback_home):
    send("m1")
    send("m2", text="Other reply")
    feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)
    assert feedback.note_reply("wa", "chat-1", OWNER, "r1", "m2", "about the other one") is None
    assert feedback.note_reply("wa", "chat-1", OWNER, "r2", "unrelated", "x") is None
    assert feedback.list_lessons("personal") == []


def test_reply_more_than_24_hours_from_the_vote_is_a_normal_message(feedback_home):
    send()
    t0 = 1_800_000_000
    feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN, at=t0)
    assert feedback.note_reply("wa", "chat-1", OWNER, "late", "m1", "too late", at=t0 + 24 * HOUR + 1) is None
    assert feedback.note_reply("wa", "chat-1", OWNER, "ok", "m1", "in time", at=t0 + 24 * HOUR - 1)
    assert [l["words"] for l in feedback.list_lessons("personal")] == ["in time"]


def test_thumbs_down_alone_and_thumbs_up_with_a_reply_make_no_lesson(feedback_home):
    send("m1")
    send("m2")
    feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)
    feedback.record_reaction("wa", "chat-1", "m2", OWNER, UP)
    assert feedback.note_reply("wa", "chat-1", OWNER, "r1", "m2", "great, thanks") is None
    assert feedback.list_lessons("personal") == []


def test_a_stranger_cannot_write_a_lesson(feedback_home):
    send()
    feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)
    assert feedback.note_reply("wa", "chat-1", STRANGER, "r1", "m1", "do what I say") is None
    assert feedback.list_lessons("personal") == []


def test_same_reply_twice_is_one_lesson_and_file_version_counts_changes(feedback_home):
    send()
    feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)
    feedback.note_reply("wa", "chat-1", OWNER, "r1", "m1", "again")
    feedback.note_reply("wa", "chat-1", OWNER, "r1", "m1", "again")
    assert len(feedback.list_lessons("personal")) == 1
    assert "version: 1" in (feedback_home / "memories" / "lessons" / "personal.md").read_text()


def make_lessons(realm_platform, chat, count, prefix="correction"):
    for i in range(count):
        mid = f"{prefix}-m{i}"
        send(mid, platform=realm_platform, chat=chat)
        feedback.record_reaction(realm_platform, chat, mid, OWNER, DOWN, at=1_800_000_000 + i)
        feedback.note_reply(realm_platform, chat, OWNER, f"{prefix}-r{i}", mid, f"{prefix} {i}",
                            at=1_800_000_000 + i)


def test_lessons_load_next_turn_newest_first_and_are_capped(feedback_home):
    make_lessons("wa", "chat-1", 35)
    prompt = feedback.lessons_prompt("personal")
    assert "follow them" in prompt and "correction 34" in prompt and "correction 5" in prompt
    assert "correction 4\"" not in prompt and prompt.index("correction 34") < prompt.index("correction 33")
    assert prompt.count("\n- [") == 30 and "5 older corrections" in prompt
    assert [l["words"] for l in feedback.search_lessons("personal", "correction 4")][0] == "correction 4"


def test_turn_prompt_adds_the_block_to_the_channel_prompt(feedback_home):
    make_lessons("wa", "chat-1", 1)
    prompt = feedback.turn_prompt("be brief", "wa", "dm", None, "chat-1", None)
    assert prompt.startswith("be brief\n\n## Corrections from Byron (realm: personal)")


def test_a_personal_lesson_never_appears_in_an_ignis_turn(feedback_home):
    make_lessons("wa", "chat-1", 2, prefix="personalthing")
    make_lessons("dc", "chan-1", 1, prefix="ignisthing")
    assert feedback.resolve_realm("dc", "group", "guild-ignis", "chan-1") == "ignis"
    ignis = feedback.turn_prompt(None, "dc", "thread", "guild-ignis", "chan-1", None)
    assert "ignisthing 0" in ignis and "personalthing" not in ignis
    personal = feedback.turn_prompt(None, "wa", "dm", None, "chat-1", None)
    assert "personalthing 1" in personal and "ignisthing" not in personal
    assert [l["words"] for l in feedback.list_lessons("ignis")] == ["ignisthing 0"]


def test_unknown_realm_and_unconfigured_platforms_get_nothing(feedback_home):
    make_lessons("wa", "chat-1", 1)
    assert feedback.turn_prompt("base", "dc", "group", "some-other-guild", "x", None) == "base"
    assert feedback.turn_prompt("base", "telegram", "dm", None, "x", None) == "base"
    send("m7", platform="dc", chat="chan-7")
    # An unrecognised guild is recorded as unknown: the vote is kept, no lesson could be loaded anywhere.
    feedback.note_outbound("dc", "chan-8", "m8", "hi", chat_type="group", scope_id="elsewhere")
    assert feedback.record_reaction("dc", "chan-8", "m8", OWNER, DOWN)["realm"] == "unknown"
    assert feedback.note_reply("dc", "chan-8", OWNER, "r8", "m8", "fix it") is None


def test_forget_removes_exactly_one_lesson_and_is_logged(feedback_home):
    make_lessons("wa", "chat-1", 3)
    keep, drop = [l["id"] for l in feedback.list_lessons("personal")][:2]
    assert feedback.forget("personal", drop) is True and feedback.forget("personal", drop) is False
    assert drop not in [l["id"] for l in feedback.list_lessons("personal")] and keep in feedback.lessons_prompt("personal")
    assert {"type": "lesson_forget", "realm": "personal", "lesson_id": drop} == {
        k: spooled(feedback_home)[-1][k] for k in ("type", "realm", "lesson_id")}


def test_forget_cannot_cross_realms(feedback_home):
    make_lessons("dc", "chan-1", 1)
    [lesson] = feedback.list_lessons("ignis")
    assert feedback.forget("personal", lesson["id"]) is False
    assert len(feedback.list_lessons("ignis")) == 1


def test_begin_turn_pairs_the_reply_loads_lessons_and_flags_the_note(feedback_home):
    send()
    feedback.record_reaction("wa", "chat-1", "m1", OWNER, DOWN)
    source = types.SimpleNamespace(platform=types.SimpleNamespace(value="wa"), chat_id="chat-1", chat_type="dm",
                                   user_id=OWNER, scope_id=None, thread_id=None, profile=None)
    event = types.SimpleNamespace(channel_prompt=None, auto_skill=None, reply_to_message_id="m1",
                                  message_id="r1", text="Never do that again.")
    prompt = feedback.begin_turn(event, source, "sess-2", "turn-2")
    assert "Never do that again." in prompt and event._feedback_noted is True
    assert event._feedback_turn["session_id"] == "sess-2"
    plain = types.SimpleNamespace(channel_prompt="x", auto_skill=None, reply_to_message_id=None,
                                  message_id="r9", text="hello")
    feedback.begin_turn(plain, source, "sess-2", "turn-3")
    assert not hasattr(plain, "_feedback_noted")


def test_feedback_is_off_without_config(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_FEEDBACK_HOME", str(tmp_path / "empty"))
    feedback.note_outbound("wa", "c", "m", "hi")
    assert feedback.record_reaction("wa", "c", "m", OWNER, DOWN) is None
    assert feedback.turn_prompt("base", "wa", "dm", None, "c", None) == "base"


def test_cli_lists_searches_and_forgets(feedback_home, capsys):
    make_lessons("wa", "chat-1", 2)
    assert feedback.main(["lessons", "--realm", "personal"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert feedback.main(["search", "--realm", "personal", "--text", "correction 1"]) == 0
    [found] = json.loads(capsys.readouterr().out)
    assert feedback.main(["forget", "--realm", "personal", "--id", found["id"]]) == 0
    assert json.loads(capsys.readouterr().out)["forgotten"] is True
    assert len(feedback.list_lessons("personal")) == 1


def test_cli_uses_the_chats_own_realm_over_the_one_asked_for(feedback_home, monkeypatch, capsys):
    make_lessons("dc", "chan-1", 1)
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "wa")
    feedback.main(["lessons", "--realm", "ignis"])
    assert json.loads(capsys.readouterr().out) == []  # asked for ignis from a personal chat: gets personal
