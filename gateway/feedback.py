"""Byron's thumbs-up / thumbs-down votes and the lessons learned from his corrections.

Everything here is plain rules: no model decides what counts as a vote, a pair or a lesson.

* A 👍 or 👎 reaction by a configured owner on a message Mara sent is recorded, silently
  (no reply to a reaction, ever). Removing or changing the reaction updates the record.
* Votes and lessons are written to an append-only JSONL spool that Trail ingests into its
  store (``trail feedback``). The gateway never writes the Trail database directly, so a
  paused Trail cannot lose a vote.
* A 👎 on a Mara message plus a reply to that same message (matched by the replied-to message
  id) within 24 hours, in either order, becomes a *lesson*: his words verbatim. Lessons live in
  one plain file per realm under ``memories/lessons/``; at the start of a Mara turn the newest
  ones for that realm are added to the turn's ephemeral prompt. A model may read lessons; only
  this module writes, and only deletion by id (``forget``) removes one.

Configuration is ``<hermes root>/state/feedback/config.json``; without it the feature is off
(fail closed). Shape::

    {"owners": {"whatsapp_cloud": ["<id>"], "discord": ["<id>"]},
     "realms": [{"platform": "whatsapp_cloud", "realm": "personal"},
                {"platform": "discord", "chat_type": "dm", "realm": "personal"},
                {"platform": "discord", "scope_id": "<guild id>", "realm": "ignis"}]}

First matching realm rule wins; no match means realm ``unknown``: the vote is kept (owner
reads only) but no lesson is written or loaded for it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

REALMS = ("personal", "ignis", "rodehouse", "velocity")
PAIR_WINDOW_SECONDS = 24 * 3600
PROMPT_LESSON_CAP = 30
PROMPT_WORDS_CAP = 600
PROMPT_REQUEST_CAP = 200
BAD_REPLY_CAP = 600
SENT_TEXT_CAP = 2000
SENT_RETENTION_SECONDS = 90 * 24 * 3600
NOTED_LINE = "Noted for next time."

_LOCK = threading.RLock()
_SKIN_TONES = {chr(c) for c in range(0x1F3FB, 0x1F400)} | {"️"}
_UP, _DOWN = "\U0001F44D", "\U0001F44E"


# ── paths and configuration ────────────────────────────────────────────────

def _root() -> Path:
    override = os.environ.get("HERMES_FEEDBACK_HOME")
    if override:
        return Path(override)
    from hermes_constants import get_default_hermes_root
    return Path(get_default_hermes_root())


def _state_dir() -> Path:
    path = _root() / "state" / "feedback"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _lessons_dir() -> Path:
    path = _root() / "memories" / "lessons"
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_config() -> Dict[str, Any]:
    try:
        data = json.loads((_state_dir() / "config.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def is_owner(platform: str, user_id: Optional[str]) -> bool:
    """Only the configured identities count; an empty config means nobody does."""
    owners = (load_config().get("owners") or {}).get(str(platform)) or []
    return bool(user_id) and str(user_id) in {str(o) for o in owners}


def enabled(platform: str) -> bool:
    return bool((load_config().get("owners") or {}).get(str(platform)))


def resolve_realm(platform: str, chat_type: Optional[str] = None, scope_id: Optional[str] = None,
                  chat_id: Optional[str] = None, profile: Optional[str] = None) -> str:
    """Deterministic realm from where the message happened; ``unknown`` when no rule matches."""
    facts = {"platform": str(platform), "chat_type": chat_type or "", "scope_id": scope_id or "",
             "chat_id": chat_id or "", "profile": profile or ""}
    for rule in load_config().get("realms") or []:
        if not isinstance(rule, dict) or rule.get("realm") not in REALMS:
            continue
        keys = [k for k in facts if k in rule]
        if keys and all(str(rule[k]) == facts[k] for k in keys):
            return str(rule["realm"])
    return "unknown"


def normalize_vote(emoji: Optional[str]) -> Optional[str]:
    """``up`` for 👍, ``down`` for 👎 (any skin tone); anything else is not a vote."""
    base = "".join(ch for ch in (emoji or "") if ch not in _SKIN_TONES)
    return {_UP: "up", _DOWN: "down"}.get(base)


# ── local state (sent-message index, vote state, replies) ──────────────────

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_state_dir() / "feedback.db"), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sent(
          key TEXT PRIMARY KEY, platform TEXT NOT NULL, chat_id TEXT NOT NULL, message_id TEXT NOT NULL,
          chat_type TEXT, scope_id TEXT, realm TEXT, text TEXT, request_id TEXT, request_text TEXT,
          session_key TEXT, session_id TEXT, turn_id TEXT, role TEXT, skill TEXT, sent_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS votes(
          key TEXT PRIMARY KEY, vote TEXT NOT NULL, voted_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS replies(
          key TEXT PRIMARY KEY, replied_to TEXT NOT NULL, reply_id TEXT NOT NULL, text TEXT NOT NULL,
          at REAL NOT NULL, lesson_id TEXT);
        CREATE INDEX IF NOT EXISTS replies_to ON replies(replied_to);
        """)
    return conn


def _key(platform: str, chat_id: str, message_id: str) -> str:
    return f"{platform}:{chat_id}:{message_id}"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _trim(text: Optional[str], cap: int) -> str:
    text = (text or "").replace("\r\n", "\n")
    return text if len(text) <= cap else text[: cap - 1] + "…"


def _spool(record: Dict[str, Any]) -> None:
    """Append one record for Trail to ingest. Never raises into the caller."""
    record = {"id": uuid.uuid4().hex, **record}
    try:
        directory = _state_dir() / "spool"
        directory.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        stamp = datetime.fromtimestamp(time.time(), timezone.utc).strftime("%Y%m%d")
        with _LOCK, open(directory / f"feedback-{stamp}.jsonl", "a", encoding="utf-8") as fh:
            fh.write(line)
    except OSError:
        logger.warning("[feedback] could not write spool record", exc_info=True)


def note_outbound(platform: str, chat_id: Any, message_id: Any, text: Optional[str], *,
                  chat_type: Optional[str] = None, scope_id: Optional[str] = None,
                  profile: Optional[str] = None) -> None:
    """Remember a message Mara sent so a later reaction or reply can be matched to it.

    Called from the adapters with the exact text of that message; the turn context (the request
    it answered, the session and turn, the role) is merged in later by :func:`note_turn`.
    """
    if not (enabled(platform) and chat_id and message_id):
        return
    try:
        realm = resolve_realm(platform, chat_type, scope_id, str(chat_id), profile)
        now = time.time()
        with _LOCK, _connect() as db:
            db.execute(
                "INSERT INTO sent(key,platform,chat_id,message_id,chat_type,scope_id,realm,text,sent_at) "
                "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET "
                "text=COALESCE(NULLIF(sent.text,''),excluded.text)",
                (_key(platform, str(chat_id), str(message_id)), platform, str(chat_id), str(message_id),
                 chat_type, scope_id, realm, _trim(text, SENT_TEXT_CAP), now))
            db.execute("DELETE FROM sent WHERE sent_at < ?", (now - SENT_RETENTION_SECONDS,))
    except Exception:
        logger.warning("[feedback] note_outbound failed", exc_info=True)


def note_turn(platform: str, chat_id: Any, message_ids: List[Any], text: Optional[str], *,
              request_id: Optional[str], request_text: Optional[str], session_key: Optional[str],
              session_id: Optional[str], turn_id: Optional[str], role: Optional[str],
              skill: Optional[str], chat_type: Optional[str] = None, scope_id: Optional[str] = None,
              profile: Optional[str] = None) -> None:
    """Attach the turn that produced these message ids (the final reply and any chunks)."""
    if not (enabled(platform) and chat_id):
        return
    try:
        realm = resolve_realm(platform, chat_type, scope_id, str(chat_id), profile)
        now = time.time()
        with _LOCK, _connect() as db:
            for mid in message_ids:
                if not mid:
                    continue
                db.execute(
                    "INSERT INTO sent(key,platform,chat_id,message_id,chat_type,scope_id,realm,text,sent_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(key) DO NOTHING",
                    (_key(platform, str(chat_id), str(mid)), platform, str(chat_id), str(mid), chat_type,
                     scope_id, realm, _trim(text, SENT_TEXT_CAP), now))
                db.execute(
                    "UPDATE sent SET request_id=?, request_text=?, session_key=?, session_id=?, turn_id=?, "
                    "role=?, skill=?, realm=COALESCE(realm,?) WHERE key=?",
                    (request_id, _trim(request_text, 1000), session_key, session_id, turn_id, role, skill,
                     realm, _key(platform, str(chat_id), str(mid))))
    except Exception:
        logger.warning("[feedback] note_turn failed", exc_info=True)


# ── votes ───────────────────────────────────────────────────────────────────

def record_reaction(platform: str, chat_id: Any, message_id: Any, user_id: Optional[str],
                    emoji: Optional[str], removed: bool = False, *, at: Optional[float] = None,
                    mara_authored: Optional[bool] = None, chat_type: Optional[str] = None,
                    scope_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Apply one reaction event. Returns the spooled vote record, or None when it is not a vote.

    ``mara_authored`` is the adapter's own knowledge that the message is Mara's (Discord gives the
    author); otherwise the message must be in the sent index. Never replies to the reaction.
    """
    if not is_owner(platform, user_id) or not (chat_id and message_id):
        return None
    try:
        now = at if at is not None else time.time()
        key = _key(platform, str(chat_id), str(message_id))
        new = None if removed else normalize_vote(emoji)
        with _LOCK, _connect() as db:
            sent = db.execute("SELECT * FROM sent WHERE key=?", (key,)).fetchone()
            if sent is None and not mara_authored:
                return None  # not provably a Mara message (e.g. Byron reacting to his own)
            current = db.execute("SELECT vote FROM votes WHERE key=?", (key,)).fetchone()
            current_vote = current["vote"] if current else None
            if removed:
                # Discord stacks reactions: removing the one that is not the current vote changes nothing.
                if normalize_vote(emoji) != current_vote:
                    return None
                new = None
            vote = new or "none"
            if vote == (current_vote or "none") and current is not None:
                return None
            if vote == "none" and current is None:
                return None
            db.execute("INSERT INTO votes(key,vote,voted_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET "
                       "vote=excluded.vote, voted_at=excluded.voted_at", (key, vote, now))
            row = dict(sent) if sent is not None else {}
            if sent is None:  # Mara's own message that the sent index never saw (media, notices)
                row = {"platform": platform, "chat_type": chat_type, "scope_id": scope_id,
                       "realm": resolve_realm(platform, chat_type, scope_id, str(chat_id))}
            record = {
                "type": "vote", "key": key, "vote": vote, "time": _iso(now),
                "channel": platform, "chat_id": str(chat_id), "message_id": str(message_id),
                "realm": row.get("realm") or resolve_realm(platform, chat_type, scope_id, str(chat_id)),
                "message_text": row.get("text") or "", "request_id": row.get("request_id") or "",
                "request_text": row.get("request_text") or "", "session_key": row.get("session_key") or "",
                "session_id": row.get("session_id") or "", "turn_id": row.get("turn_id") or "",
                "role": row.get("role") or "", "skill": row.get("skill") or ""}
            _spool(record)
            if vote == "down":
                _make_lessons_for(db, key, row, now)
        return record
    except Exception:
        logger.warning("[feedback] record_reaction failed", exc_info=True)
        return None


# ── replies and lessons ─────────────────────────────────────────────────────

def note_reply(platform: str, chat_id: Any, user_id: Optional[str], reply_id: Any,
               replied_to_id: Any, text: Optional[str], *, at: Optional[float] = None,
               chat_type: Optional[str] = None, scope_id: Optional[str] = None,
               profile: Optional[str] = None) -> Optional[str]:
    """Byron replied to a Mara message. If that message already has a 👎 within 24 hours, the
    reply is a correction and becomes a lesson. Returns the lesson id, if one was written."""
    if not (is_owner(platform, user_id) and chat_id and replied_to_id and reply_id and (text or "").strip()):
        return None
    try:
        now = at if at is not None else time.time()
        target = _key(platform, str(chat_id), str(replied_to_id))
        with _LOCK, _connect() as db:
            sent = db.execute("SELECT * FROM sent WHERE key=?", (target,)).fetchone()
            if sent is None:
                return None
            db.execute("INSERT OR IGNORE INTO replies(key,replied_to,reply_id,text,at) VALUES(?,?,?,?,?)",
                       (_key(platform, str(chat_id), str(reply_id)), target, str(reply_id),
                        text.replace("\r\n", "\n"), now))
            vote = db.execute("SELECT * FROM votes WHERE key=?", (target,)).fetchone()
            if vote is None or vote["vote"] != "down" or abs(now - vote["voted_at"]) > PAIR_WINDOW_SECONDS:
                return None
            return _make_lessons_for(db, target, dict(sent), vote["voted_at"])
    except Exception:
        logger.warning("[feedback] note_reply failed", exc_info=True)
        return None


def _make_lessons_for(db: sqlite3.Connection, target: str, sent: Dict[str, Any], voted_at: float) -> Optional[str]:
    """Turn every unpaired reply to ``target`` within 24 hours of the 👎 into a lesson."""
    last = None
    rows = db.execute("SELECT * FROM replies WHERE replied_to=? AND lesson_id IS NULL ORDER BY at", (target,)).fetchall()
    for reply in rows:
        if abs(reply["at"] - voted_at) > PAIR_WINDOW_SECONDS:
            continue
        realm = sent.get("realm") or "unknown"
        if realm not in REALMS:
            continue  # no realm, no lesson: it could not be loaded safely anywhere
        lesson = {
            "id": "L-" + hashlib.sha1(reply["key"].encode()).hexdigest()[:8],
            "realm": realm, "date": _iso(reply["at"]), "channel": sent.get("platform", ""),
            "scope": scope_tag(sent), "words": reply["text"],
            "request": sent.get("request_text") or "", "bad_reply": _trim(sent.get("text"), BAD_REPLY_CAP),
            "vote_key": target, "reply_key": reply["key"], "session_id": sent.get("session_id") or "",
            "turn_id": sent.get("turn_id") or ""}
        if add_lesson(lesson):
            db.execute("UPDATE replies SET lesson_id=? WHERE key=?", (lesson["id"], reply["key"]))
            last = lesson["id"]
    return last


def scope_tag(sent: Dict[str, Any]) -> str:
    """Where it happened, from recorded facts only: channel, role, skill, task type if recorded."""
    parts = [str(sent.get("platform") or "")]
    if sent.get("chat_type"):
        parts.append(str(sent["chat_type"]))
    parts.append("role " + str(sent.get("role") or "mara"))
    if sent.get("skill"):
        parts.append("skill " + str(sent["skill"]))
    return " · ".join(p for p in parts if p)


# ── lesson file (one per realm) ─────────────────────────────────────────────

_FIELDS = ("date", "channel", "scope", "vote_key", "reply_key", "session_id", "turn_id")
_BLOCKS = (("words", "his words"), ("request", "original request"), ("bad_reply", "bad reply (trimmed)"))


def _path(realm: str) -> Path:
    if realm not in REALMS:
        raise ValueError(f"unknown realm {realm!r}")
    return _lessons_dir() / f"{realm}.md"


def _quote(text: str) -> str:
    return "\n".join(("> " + line) if line else ">" for line in (text or "").replace("\r\n", "\n").split("\n"))


def _render(realm: str, version: int, lessons: List[Dict[str, Any]]) -> str:
    out = [f"# Corrections from Byron — realm: {realm}", "",
           "Written only by gateway/feedback.py from Byron's thumbs-down + reply. Do not edit by hand.",
           f"version: {version}", f"count: {len(lessons)}", ""]
    for lesson in lessons:
        out.append(f"## {lesson['id']}")
        out.extend(f"{k}: {str(lesson.get(k, '')).replace(chr(10), ' ')}" for k in _FIELDS)
        for key, label in _BLOCKS:
            out.append(f"{label}:")
            out.append(_quote(str(lesson.get(key, ""))))
        out.append("")
    return "\n".join(out)


def _parse(text: str) -> tuple[int, List[Dict[str, Any]]]:
    version, lessons, current, block = 0, [], None, None
    labels = {label: key for key, label in _BLOCKS}
    for line in text.split("\n"):
        if line.startswith("## L-"):
            current, block = {"id": line[3:].strip(), **{key: [] for key, _ in _BLOCKS}}, None
            lessons.append(current)
        elif current is None:
            if line.startswith("version: "):
                version = int(line[9:] or 0)
        elif line.startswith(">") and block:
            current[block].append(line[2:] if line.startswith("> ") else "")
        else:
            label = line[:-1] if line.endswith(":") else None
            if label in labels:
                block = labels[label]
            elif ": " in line and line.split(": ", 1)[0] in _FIELDS:
                key, value = line.split(": ", 1)
                current[key] = value
                block = None
    for lesson in lessons:
        for key, _ in _BLOCKS:
            lesson[key] = "\n".join(lesson[key])
    return version, lessons


def _read(realm: str) -> tuple[int, List[Dict[str, Any]]]:
    try:
        return _parse(_path(realm).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return 0, []


def _write(realm: str, version: int, lessons: List[Dict[str, Any]]) -> None:
    path = _path(realm)
    tmp = path.with_suffix(f".tmp{os.getpid()}")
    tmp.write_text(_render(realm, version, lessons), encoding="utf-8")
    os.replace(tmp, path)


def add_lesson(lesson: Dict[str, Any]) -> bool:
    """Prepend a lesson (newest first) and bump the file version. Idempotent by id."""
    with _LOCK:
        version, lessons = _read(lesson["realm"])
        if any(item["id"] == lesson["id"] for item in lessons):
            return False
        _write(lesson["realm"], version + 1, [lesson] + lessons)
        _spool({"type": "lesson", "time": _iso(time.time()), "version": version + 1, **lesson})
        return True


def list_lessons(realm: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    lessons = _read(realm)[1]
    return lessons[:limit] if limit else lessons


def search_lessons(realm: str, text: str) -> List[Dict[str, Any]]:
    needle = text.lower()
    return [lesson for lesson in _read(realm)[1]
            if needle in " ".join(str(lesson.get(k, "")) for k in ("words", "request", "bad_reply", "scope")).lower()]


def forget(realm: str, lesson_id: str) -> bool:
    """Deterministic delete by id; logged to the spool (Trail) and the local forget log."""
    with _LOCK:
        version, lessons = _read(realm)
        kept = [lesson for lesson in lessons if lesson["id"] != lesson_id]
        if len(kept) == len(lessons):
            return False
        _write(realm, version + 1, kept)
        _spool({"type": "lesson_forget", "time": _iso(time.time()), "realm": realm, "lesson_id": lesson_id,
                "version": version + 1})
        return True


def _cli(realm: str, command: str) -> str:
    root = Path(__file__).resolve().parents[1]
    return f"cd {root} && venv/bin/python3 -m gateway.feedback {command} --realm {realm}"


def lessons_prompt(realm: Optional[str]) -> str:
    """The block added to a turn's ephemeral prompt: the realm's newest lessons, most recent first."""
    if realm not in REALMS:
        return ""
    lessons = list_lessons(realm, PROMPT_LESSON_CAP)
    if not lessons:
        return ""
    total = len(_read(realm)[1])
    lines = [f"## Corrections from Byron (realm: {realm}) — follow them",
             "Byron saved these by marking a reply of yours 👎 and answering it. They are his own words. "
             "Follow them, most recent first. Do not edit, merge, rewrite or delete them yourself; if he asks "
             "what he has corrected you on, run `" + _cli(realm, "lessons") + "`; to forget one he names, run `"
             + _cli(realm, "forget") + " --id <id>` and tell him it is forgotten."]
    for lesson in lessons:
        words = _trim(lesson["words"], PROMPT_WORDS_CAP).replace("\n", " ⏎ ")
        about = _trim(lesson.get("request"), PROMPT_REQUEST_CAP).replace("\n", " ")
        lines.append(f"- [{lesson['id']} · {lesson.get('date', '')[:10]}] \"{words}\""
                     + (f" (about his request: \"{about}\")" if about else ""))
    if total > len(lessons):
        lines.append(f"({total - len(lessons)} older corrections are saved; search them with "
                     f"`{_cli(realm, 'search')} --text <words>`.)")
    return "\n".join(lines)


def turn_prompt(base: Optional[str], platform: str, chat_type: Optional[str], scope_id: Optional[str],
                chat_id: Optional[str], profile: Optional[str]) -> Optional[str]:
    """``base`` plus the realm's lessons block; unchanged when feedback is off or the realm unknown."""
    try:
        if not enabled(platform):
            return base
        block = lessons_prompt(resolve_realm(platform, chat_type, scope_id, chat_id, profile))
        return (f"{base}\n\n{block}" if base else block) if block else base
    except Exception:
        logger.warning("[feedback] lessons prompt failed", exc_info=True)
        return base


def begin_turn(event: Any, source: Any, session_id: Optional[str], turn_id: Optional[str]) -> Optional[str]:
    """Called once per inbound turn, before the agent runs. Records the turn's identity for the
    reply it will produce, pairs a reply to a 👎'd message into a lesson, and returns the turn's
    ephemeral prompt (the channel prompt plus the realm's corrections). Never raises."""
    base = getattr(event, "channel_prompt", None)
    try:
        platform = getattr(source.platform, "value", str(source.platform))
        if not enabled(platform):
            return base
        skill = getattr(event, "auto_skill", None)
        event._feedback_turn = {"session_id": session_id, "turn_id": turn_id,
                                "skill": ",".join(skill) if isinstance(skill, (list, tuple)) else skill}
        reply_to = getattr(event, "reply_to_message_id", None)
        if reply_to:
            lesson_id = note_reply(
                platform, getattr(source, "thread_id", None) or source.chat_id, getattr(source, "user_id", None),
                getattr(event, "message_id", None), reply_to, getattr(event, "text", None),
                chat_type=source.chat_type, scope_id=getattr(source, "scope_id", None),
                profile=getattr(source, "profile", None))
            if lesson_id:
                event._feedback_noted = True
        return turn_prompt(base, platform, source.chat_type, getattr(source, "scope_id", None), source.chat_id,
                           getattr(source, "profile", None))
    except Exception:
        logger.warning("[feedback] begin_turn failed", exc_info=True)
        return base


# ── command line (Mara runs this to list, search and forget) ────────────────

def _session_realm() -> Optional[str]:
    platform = os.environ.get("HERMES_SESSION_PLATFORM")
    if not platform:
        return None
    realm = resolve_realm(platform, os.environ.get("HERMES_SESSION_CHAT_TYPE"),
                          os.environ.get("HERMES_SESSION_SCOPE_ID"), os.environ.get("HERMES_SESSION_CHAT_ID"),
                          os.environ.get("HERMES_SESSION_PROFILE"))
    return realm if realm in REALMS else None


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m gateway.feedback")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("lessons", "search", "forget"):
        p = sub.add_parser(name)
        p.add_argument("--realm", choices=REALMS)
        if name == "search":
            p.add_argument("--text", required=True)
        if name == "forget":
            p.add_argument("--id", required=True)
    args = parser.parse_args(argv)
    realm = _session_realm() or args.realm  # the chat's own realm wins over what a caller asks for
    if realm is None:
        parser.error("--realm is required outside a chat session")
    if args.command == "lessons":
        result: Any = list_lessons(realm)
    elif args.command == "search":
        result = search_lessons(realm, args.text)
    else:
        result = {"forgotten": forget(realm, args.id), "id": args.id, "realm": realm}
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
