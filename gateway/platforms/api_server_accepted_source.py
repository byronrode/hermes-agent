"""Current authenticated API admission, resolved from its owning run registry.

This is an in-process source-owner interface, not a bearer capability. Shell
hooks may project its metadata; only a trusted in-process consumer can resolve
the current record. Company permission and attestation remain external.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import sqlite3


_current = ContextVar("accepted_api_run_resolver", default=None)


def admission_resolver(owner, *, scope, key, fingerprint, run_id, session_id,
                       profile, message, authenticated):
    def resolve():
        if not authenticated or not key or not isinstance(message, str):
            return None
        if not owner._run_idempotency_store.durable or owner._run_owners.get(run_id) != scope:
            return None
        outcome, record = owner._run_idempotency_store.lookup(scope, key, fingerprint)
        if outcome != "reused" or record is None or record["run_id"] != run_id:
            return None
        status = record["status"]
        if status.get("run_id") != run_id or status.get("session_id") != session_id:
            return None
        return {"run_id": run_id, "session_id": session_id, "profile": profile,
                "auth_scope": scope, "message_sha256": hashlib.sha256(message.encode()).hexdigest(),
                "accepted_at": datetime.fromtimestamp(status["created_at"], timezone.utc).isoformat()}
    def qualified_source():
        if resolve() is None:
            return None
        from gateway.platforms.api_server_source_receipts import read_source
        return read_source(owner, scope, run_id)
    resolve.qualified_source = qualified_source
    return resolve


@contextmanager
def accepted_run_scope(resolve):
    # Context copies retain object references after ContextVar.reset(). Revoke
    # this scope's resolver for every copy when its owning execution exits.
    active = True
    def scoped_resolve():
        return resolve() if active and resolve is not None else None
    def qualified_source():
        reader = getattr(resolve, 'qualified_source', None)
        return reader() if active and callable(reader) else None
    scoped_resolve.qualified_source = qualified_source
    token = _current.set(scoped_resolve)
    try:
        yield
    finally:
        active = False
        _current.reset(token)


def current_accepted_run(*, session_id=None):
    from agent.delegation_context import is_delegated_child_process_context
    if is_delegated_child_process_context():
        return None
    resolve = _current.get()
    try:
        accepted = resolve() if resolve is not None else None
        if accepted is not None and session_id is not None and accepted['session_id'] != session_id:
            return None
        return accepted
    except (sqlite3.Error, OSError, ValueError, KeyError, OverflowError):
        return None


def current_accepted_source(*, session_id=None):
    """Resolve only a currently qualified provider source, never caller IDs.

    Reuses the exact authenticated read_source owner (profile, PID/epoch,
    reservation, provider, lifetime and status checks). No keys or new store.
    """
    if current_accepted_run(session_id=session_id) is None:
        return None
    resolve = _current.get()
    try:
        source = resolve.qualified_source() if resolve is not None else None
        if source is not None and session_id is not None and source['session_id'] != session_id:
            return None
        return source
    except (sqlite3.Error, OSError, ValueError, KeyError, OverflowError):
        return None
