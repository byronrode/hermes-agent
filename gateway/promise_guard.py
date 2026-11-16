"""Promise guard: a reply that says "I'm doing X" must not be the end of the turn.

Twice on 15 November 2026 Mara told Byron she was making or checking something and the turn
ended with nothing done and nothing waiting to pick it up. This module is the gateway-side rule
that closes that hole. It is a tool, not Mara's judgement:

1. After the final text reply of a turn that came from Byron's verified identity on WhatsApp or
   Discord, one small classification call asks whether the reply *promises work it did not
   deliver* (strict JSON, fixed prompt, cheap auxiliary model, 5 second cap).
2. If so, the gateway re-runs the same turn with a fixed internal instruction, at most twice per
   original message. The continuation runs *inside* the original authenticated turn (the same
   mechanism as a queued follow-up), so the outbound gate still sees an authenticated reply.
3. If the last reply is still a promise, the gateway appends one fixed line saying it stopped and
   records an open item in Trail so it appears in the attention list.

Failure of the check never blocks or delays delivery beyond the cap: any error, timeout or
unparseable answer counts as "not a promise" and is logged.

Kill switch: ``<hermes root>/state/promise_guard.json`` containing ``{"enabled": false}``.
No file, or any other content, means on. Optional keys: ``timeout`` (seconds, default 5) and
``max_continuations`` (default 2).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

PLATFORMS = ("whatsapp_cloud", "discord")
DEFAULT_TIMEOUT = 5.0
DEFAULT_MAX_CONTINUATIONS = 2
REPLY_CAP = 3000            # characters of the reply shown to the classifier
REQUEST_CAP = 800           # characters of Byron's message shown to the classifier
WHAT_CAP = 160
LONG_REPLY_SKIP = 2500      # a reply this long is a result, not a promise: skip the model call
STATE_KIND = "promise_guard"

CONTINUATION_TEMPLATE = (
    "You told Byron you would: {what}. Do it now in this turn and send the result. "
    "If you cannot, say exactly what is blocking it."
)
STOP_LINE_TEMPLATE = "Not done yet: {what}. I have stopped; reply here to make me continue."

SYSTEM_PROMPT = """You audit ONE reply from an assistant to its user. Decide whether the reply PROMISES WORK IT DID NOT DELIVER.

promised_undelivered is true only when ALL of these hold:
- The reply says the assistant is doing, will do, is checking, is making, is sourcing, is sending or is about to produce something, OR it restates a deliverable it has agreed to produce ("Yes. 20 separate PNGs, each cropped to...") as a plan, and
- the reply itself does NOT contain the result, and
- the reply has NO attachment (the facts below say whether it has one), and
- the reply does NOT ask the user a question or wait for the user's answer, and
- the reply does NOT say the assistant is stopping, cannot do it, or is blocked.

promised_undelivered is false for: a direct answer or result; a reply that contains the finished output or an attachment; a question or an offer phrased as a question ("Want me to ...?"); an admission that it cannot do something and why; a report of work already finished (past tense); a confirmation that a reminder or task was already created; a short acknowledgement of thanks or a greeting; an explanation of how something works.

Answer with ONE line of strict JSON and nothing else:
{"promised_undelivered": true|false, "what": "<short phrase naming the undelivered work, in the first person, empty if false>"}"""


# ── configuration (kill switch) ─────────────────────────────────────────────

def _root() -> Path:
    from gateway import feedback
    return feedback._root()


def config_path() -> Path:
    return _root() / "state" / "promise_guard.json"


def load_config() -> Dict[str, Any]:
    try:
        data = json.loads(config_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def enabled() -> bool:
    """On unless the config file explicitly says ``"enabled": false``."""
    return load_config().get("enabled") is not False


def timeout_seconds() -> float:
    value = load_config().get("timeout")
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and 0 < value <= 30 \
        else DEFAULT_TIMEOUT


def max_continuations() -> int:
    value = load_config().get("max_continuations")
    return int(value) if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 5 \
        else DEFAULT_MAX_CONTINUATIONS


# ── who and what is checked ─────────────────────────────────────────────────

def eligible_proof(proof: Optional[Dict[str, Any]]) -> bool:
    """True only for the authenticated-reply proof the outbound gate already builds: Byron's verified
    identity (the feedback owner list) writing to us on WhatsApp or Discord, a real inbound message
    (never an internal or bot event, which ``authorized_reply_scope`` refuses to prove)."""
    from gateway import feedback
    if not isinstance(proof, dict) or proof.get("kind") != "authenticated_reply":
        return False
    platform = str(proof.get("platform") or "")
    return platform in PLATFORMS and feedback.enabled(platform) and feedback.is_owner(platform, proof.get("user_id"))


def prefilter(reply: str, *, has_media: bool) -> Optional[str]:
    """A rule that says "not a promise" without a model call; ``None`` means ask the classifier."""
    text = (reply or "").strip()
    if not text:
        return "empty"
    if has_media or re.search(r"(?m)^\s*MEDIA:", text):
        return "has_attachment"
    if len(text) > LONG_REPLY_SKIP:
        return "long_reply_is_a_result"
    if text.rstrip("*_ \n\"')”").endswith("?"):
        return "ends_with_a_question"
    return None


def clean_what(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip().strip(".").strip()
    return text[: WHAT_CAP - 1] + "…" if len(text) > WHAT_CAP else text


def parse_verdict(raw: Any) -> Dict[str, Any]:
    """Strict JSON in, ``{"promised_undelivered": bool, "what": str}`` out; ValueError otherwise."""
    text = (raw or "").strip() if isinstance(raw, str) else ""
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.S)
    if fenced:
        text = fenced.group(1)
    data = json.loads(text)
    if not isinstance(data, dict) or not isinstance(data.get("promised_undelivered"), bool):
        raise ValueError("verdict must be an object with a boolean promised_undelivered")
    what = clean_what(data.get("what"))
    promised = data["promised_undelivered"]
    if promised and not what:
        what = "finish the work I just described"
    return {"promised_undelivered": promised, "what": what if promised else ""}


def build_messages(reply: str, request: str, *, has_media: bool) -> list:
    user = (
        f"Facts: attachment in this reply: {'yes' if has_media else 'no'}.\n"
        f"User's message (context only, may be empty):\n<<<\n{(request or '').strip()[:REQUEST_CAP]}\n>>>\n"
        f"Assistant reply to audit:\n<<<\n{reply.strip()[:REPLY_CAP]}\n>>>")
    return [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]


def _call_auxiliary(messages: list, timeout: float) -> str:
    """The configured cheap auxiliary model (``auxiliary.promise_check``). No new provider."""
    from agent.auxiliary_client import call_llm
    response = call_llm(task="promise_check", messages=messages, max_tokens=120, temperature=None,
                        timeout=timeout, reasoning_config={"enabled": False})
    return response.choices[0].message.content or ""


def classify(reply: str, request: str = "", *, has_media: bool = False,
             call: Optional[Callable[[list, float], str]] = None,
             timeout: Optional[float] = None) -> Dict[str, Any]:
    """Synchronous verdict. Never raises: any problem is ``promised_undelivered: False`` plus a
    ``skipped``/``error`` note for the log."""
    skip = prefilter(reply, has_media=has_media)
    if skip:
        return {"promised_undelivered": False, "what": "", "skipped": skip}
    started = time.monotonic()
    try:
        raw = (call or _call_auxiliary)(build_messages(reply, request, has_media=has_media),
                                        timeout or timeout_seconds())
        verdict = parse_verdict(raw)
    except Exception as exc:  # noqa: BLE001 - a failed check must never reach the user
        logger.warning("[promise_guard] check failed (%s: %s); treating as not a promise",
                       type(exc).__name__, str(exc)[:200])
        return {"promised_undelivered": False, "what": "", "error": type(exc).__name__}
    verdict["seconds"] = round(time.monotonic() - started, 2)
    return verdict


async def classify_async(reply: str, request: str = "", *, has_media: bool = False,
                         call: Optional[Callable[[list, float], str]] = None,
                         run_in_executor: Optional[Callable[..., Any]] = None) -> Dict[str, Any]:
    """:func:`classify` in a worker thread with a hard wall-clock cap, so a stuck provider cannot hold the
    reply. ``run_in_executor`` is the gateway's context-carrying executor (profile secret scope)."""
    limit = timeout_seconds()
    try:
        job = (lambda: classify(reply, request, has_media=has_media, call=call, timeout=limit))
        if run_in_executor is not None:
            future = run_in_executor(job)
        else:
            future = asyncio.get_running_loop().run_in_executor(None, job)
        return await asyncio.wait_for(future, timeout=limit + 0.5)
    except asyncio.TimeoutError:
        logger.warning("[promise_guard] check timed out after %.1fs; treating as not a promise", limit)
        return {"promised_undelivered": False, "what": "", "error": "timeout"}
    except Exception as exc:  # noqa: BLE001
        logger.warning("[promise_guard] check errored (%s); treating as not a promise", type(exc).__name__)
        return {"promised_undelivered": False, "what": "", "error": type(exc).__name__}


# ── the fixed texts ─────────────────────────────────────────────────────────

def continuation_instruction(what: str) -> str:
    return CONTINUATION_TEMPLATE.format(what=clean_what(what) or "finish the work I just described")


def stop_line(what: str) -> str:
    return STOP_LINE_TEMPLATE.format(what=clean_what(what) or "the work I described")


def with_stop_line(reply: str, what: str) -> str:
    return f"{(reply or '').rstrip()}\n\n{stop_line(what)}"


# ── chain state carried through the recursive turn ──────────────────────────

def new_state(proof: Dict[str, Any], request: str, source: Any = None) -> Dict[str, Any]:
    """State for one original user message: how many continuations ran and where it came from."""
    from gateway import feedback
    platform = str(proof.get("platform") or "")
    realm = feedback.resolve_realm(
        platform, getattr(source, "chat_type", None), getattr(source, "scope_id", None),
        str(proof.get("recipient") or ""), getattr(source, "profile", None))
    return {"kind": STATE_KIND, "count": 0, "request": (request or "")[:REQUEST_CAP], "realm": realm,
            "platform": platform, "chat_id": proof.get("recipient"), "user_id": proof.get("user_id"),
            "message_id": proof.get("message_id"), "whats": []}


# ── the open item in Trail ──────────────────────────────────────────────────

def record_open_item(state: Dict[str, Any], what: str, *, reply: str = "", session_key: Optional[str] = None,
                     session_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Spool one ``open_item`` record for Trail's attention list (the same append-only spool the
    votes use; the gateway never writes the Trail database). Never raises."""
    try:
        from gateway import feedback
        platform = str(state.get("platform") or "")
        chat_id = str(state.get("chat_id") or "")
        record = {
            "type": "open_item", "kind": "promise_not_delivered",
            "key": f"promise:{platform}:{chat_id}:{state.get('message_id') or ''}",
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "channel": platform, "chat_id": chat_id, "message_id": str(state.get("message_id") or ""),
            "realm": state.get("realm") or "unknown",
            "what": clean_what(what), "continuations": int(state.get("count") or 0),
            "request_text": feedback._trim(state.get("request"), 1000),
            "reply_text": feedback._trim(reply, 1000),
            "session_key": session_key or "", "session_id": session_id or ""}
        feedback._spool(record)
        return record
    except Exception:  # noqa: BLE001
        logger.warning("[promise_guard] could not record the open item", exc_info=True)
        return None


# ── command line: show the switch, try the classifier on a text ─────────────

def main(argv: Optional[list] = None) -> int:
    import argparse
    import sys
    parser = argparse.ArgumentParser(prog="python -m gateway.promise_guard")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="kill-switch state and limits")
    check = sub.add_parser("check", help="classify one reply (synthetic text only; sends nothing)")
    check.add_argument("--reply", required=True)
    check.add_argument("--request", default="")
    check.add_argument("--attachment", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "status":
        result: Any = {"enabled": enabled(), "config": str(config_path()), "timeout": timeout_seconds(),
                       "max_continuations": max_continuations()}
    else:
        result = classify(args.reply, args.request, has_media=args.attachment)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
