"""The WhatsApp Cloud and Discord adapters hand 👍/👎 reactions to the feedback loop, silently.

Synthetic identities only; no network and no real chat is touched.
"""

import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway import feedback
from gateway.config import Platform, PlatformConfig

OWNER = "owner-test-1"
UP, DOWN = "\U0001F44D", "\U0001F44E"


@pytest.fixture(autouse=True)
def feedback_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_FEEDBACK_HOME", str(tmp_path))
    state = tmp_path / "state" / "feedback"
    state.mkdir(parents=True)
    (state / "config.json").write_text(json.dumps({
        "owners": {"whatsapp_cloud": [OWNER], "discord": [OWNER]},
        "realms": [{"platform": "whatsapp_cloud", "realm": "personal"},
                   {"platform": "discord", "scope_id": "guild-1", "realm": "ignis"}]}))
    return tmp_path


def votes(home):
    out = []
    for path in sorted((home / "state" / "feedback" / "spool").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return [r for r in out if r["type"] == "vote"]


def whatsapp_adapter(tmp_path, allowed=(OWNER,)):
    from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter
    adapter = WhatsAppCloudAdapter.__new__(WhatsAppCloudAdapter)
    adapter.platform = Platform.WHATSAPP_CLOUD
    adapter._phone_number_id = "pn-1"
    adapter._window_home = tmp_path
    adapter._is_dm_allowed = lambda sender: sender in allowed
    adapter.send = AsyncMock(side_effect=AssertionError("a reaction must never be answered"))
    return adapter


def reaction(sender, wamid, emoji=None):
    body = {"message_id": wamid}
    if emoji:
        body["emoji"] = emoji
    return {"from": sender, "id": "wamid.rx", "type": "reaction", "timestamp": "1790000000", "reaction": body}


@pytest.mark.asyncio
async def test_whatsapp_reaction_is_recorded_changed_removed_and_never_answered(feedback_home):
    adapter = whatsapp_adapter(feedback_home)
    feedback.note_outbound("whatsapp_cloud", OWNER, "wamid.mara1", "the reply", chat_type="dm")
    meta = {"phone_number_id": "pn-1"}
    for emoji in (DOWN, UP, None):
        assert await adapter._build_message_event_from_cloud(reaction(OWNER, "wamid.mara1", emoji), {}, meta) is None
    assert [v["vote"] for v in votes(feedback_home)] == ["down", "up", "none"]
    adapter.send.assert_not_called()


@pytest.mark.asyncio
async def test_whatsapp_reaction_from_anyone_else_or_on_other_messages_is_ignored(feedback_home):
    adapter = whatsapp_adapter(feedback_home, allowed=(OWNER, "stranger"))
    feedback.note_outbound("whatsapp_cloud", OWNER, "wamid.mara1", "the reply", chat_type="dm")
    meta = {"phone_number_id": "pn-1"}
    await adapter._build_message_event_from_cloud(reaction("stranger", "wamid.mara1", DOWN), {}, meta)
    await adapter._build_message_event_from_cloud(reaction(OWNER, "wamid.not-ours", DOWN), {}, meta)
    await adapter._build_message_event_from_cloud(reaction(OWNER, "wamid.mara1", DOWN), {}, {"phone_number_id": "other"})
    assert votes(feedback_home) == []


def _ensure_discord_mock():
    if "discord" in sys.modules and hasattr(sys.modules["discord"], "__file__"):
        return
    discord_mod = MagicMock()
    sys.modules.setdefault("discord", discord_mod)
    sys.modules.setdefault("discord.ext", MagicMock())
    sys.modules.setdefault("discord.ext.commands", MagicMock())


@pytest.mark.asyncio
async def test_discord_reaction_events_are_recorded_for_the_owner_on_our_messages(feedback_home):
    _ensure_discord_mock()
    from plugins.platforms.discord.adapter import DiscordAdapter
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="***"))
    adapter._client = SimpleNamespace(user=SimpleNamespace(id=99999, name="HermesBot"))
    adapter.send = AsyncMock(side_effect=AssertionError("a reaction must never be answered"))
    payload = lambda user, emoji, author=99999, guild=777, mid=900: SimpleNamespace(  # noqa: E731
        emoji=emoji, user_id=user, channel_id=555, message_id=mid, guild_id=guild, message_author_id=author)
    owner = int("1" + "0" * 5)
    feedback.load_config()  # ensure state dir exists
    cfg = feedback_home / "state" / "feedback" / "config.json"
    data = json.loads(cfg.read_text())
    data["owners"]["discord"] = [str(owner)]
    data["realms"].append({"platform": "discord", "chat_type": "dm", "realm": "personal"})
    data["realms"][1]["scope_id"] = "777"
    cfg.write_text(json.dumps(data))

    await adapter._on_feedback_reaction(payload(owner, DOWN), removed=False)
    await adapter._on_feedback_reaction(payload(owner, DOWN), removed=True)
    await adapter._on_feedback_reaction(payload(owner, DOWN, guild=None, mid=901), removed=False)  # a DM
    await adapter._on_feedback_reaction(payload(12345, DOWN), removed=False)  # someone else
    await adapter._on_feedback_reaction(payload(owner, UP, author=42), removed=False)  # not our message
    await adapter._on_feedback_reaction(payload(99999, "✅"), removed=False)  # our own lifecycle emoji
    got = votes(feedback_home)
    assert [(v["vote"], v["realm"]) for v in got] == [("down", "ignis"), ("none", "ignis"), ("down", "personal")]
    adapter.send.assert_not_called()
