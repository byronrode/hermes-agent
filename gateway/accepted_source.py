"""Post-authorization messaging source custody, scoped to an actual gateway turn.

This is an in-process resolver, never a public metadata bearer capability.
Durability belongs to the existing inbound user row, not another source store.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
import json
import threading
import sqlite3
from pathlib import Path

_current = ContextVar('authorized_gateway_source', default=None)
_execution = ContextVar('authorized_gateway_execution', default=None)
_inspection = ContextVar('authorized_gateway_inspection', default=None)


@contextmanager
def gateway_source_execution(agent):
    """Only the actual native agent worker can exercise its turn's resolver."""
    owner = [threading.get_ident(), True, agent]
    token = _execution.set(owner)
    try:
        yield
    finally:
        owner[1] = False
        _execution.reset(token)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _owns_execution(execution, *, inspect_only=False):
    from tools.interrupt import is_interrupted
    if not execution or not execution[1] or is_interrupted():
        return False
    tid = threading.get_ident()
    inspection = _inspection.get()
    if (inspect_only and inspection and inspection[0] is execution
            and inspection[1] == tid and inspection[2]):
        return True
    if execution[0] == tid:
        return True
    # Sequential and parallel native tools use registered daemon workers.
    # A copied ContextVar in an arbitrary background thread is insufficient.
    agent = execution[2]
    lock = getattr(agent,'_tool_worker_threads_lock',None)
    if lock is None:
        return False
    with lock:
        return tid in agent._tool_worker_threads


def prepare_gateway_source_inspection():
    """Native hook dispatcher may lend inspection, never admission, to one worker.

    Capture on the actual agent/tool owner before launching a bounded callback.
    A copied context in an arbitrary thread cannot create or exercise this scope.
    The dispatcher revokes on timeout without waiting for an abandoned callback.
    """
    execution = _execution.get()
    inspection = [execution,None,_owns_execution(execution)]
    def run(callback, *args):
        inspection[1] = threading.get_ident()
        token = _inspection.set(inspection)
        try:
            return callback(*args)
        finally:
            inspection[2] = False
            _inspection.reset(token)
    def revoke():
        inspection[2] = False
    return run,revoke


@contextmanager
def authorized_event_scope(runner, event, source, session_id, session_key, generation, display_metadata=None):
    from gateway.config import Platform
    from hermes_constants import get_hermes_home
    identity = getattr(source, '_identity', None)
    # Only the native receiving adapter may establish external-event provenance.
    external = (event is not None and not getattr(event,'internal',False) and source.platform == Platform.DISCORD)
    intake = runner._intake_adapter_for(source) if external else None
    facts_reader = getattr(intake,'source_event_identity',None)
    facts = facts_reader(event) if callable(facts_reader) else None
    home = Path(get_hermes_home()).resolve()
    profile = getattr(identity, 'runtime_profile', None) or source.profile or 'default'
    eligible = (event is not None and not getattr(event, 'internal', False)
        and source.platform == Platform.DISCORD and not source.is_bot
        and profile == 'default' and intake is not None and intake.is_connected is True and facts and event.message_id
        and session_key and type(generation) is int and generation > 0
        and source.user_id and source.chat_id and isinstance(event.text, str)
        and runner._is_user_authorized_for_source(source, allow_adapter_delegation=False))
    base = None
    if eligible:
        route = dict(platform='discord', profile=profile, session_id=session_id,
            home_scope=_digest(str(home)),
            run_generation=generation,
            session_key=session_key, message_id=str(event.message_id), user_id=str(source.user_id),
            guild_id=str(source.scope_id or ''), channel_id=str(source.chat_id),
            parent_channel_id=str(source.parent_chat_id or ''),chat_type=source.chat_type,
            thread_id=str(source.thread_id or ''), transport_profile=str(getattr(intake, '_owner_profile', None) or 'default'),
            message_sha256=hashlib.sha256(event.text.encode()).hexdigest())
        route.update(facts)
        event_key = {k:route[k] for k in ('platform','profile','home_scope','message_id','user_id',
            'guild_id','original_channel_id','native_message_sha256')}
        base = dict(route, run_id='run_' + _digest(event_key)[:32], auth_scope=_digest(route))
    active = True
    selected_owner = None
    def resolve(admit, requested_session):
        nonlocal active, selected_owner
        from agent.delegation_context import is_delegated_child_process_context
        from tools.approval_context import _approval_turn_id, _approval_session_id
        execution = _execution.get()
        db = getattr(runner._session_db, '_db', runner._session_db) if base else None
        tip = (db.resolve_resume_session_id(session_id) or session_id) if db else session_id
        sessions = {session_id,tip}
        if (not active or base is None or is_delegated_child_process_context()
                or not _owns_execution(execution,inspect_only=not admit)
                or Path(get_hermes_home()).resolve() != home
                or runner._intake_adapter_for(source) is not intake
                or intake.is_connected is not True
                or facts_reader(event) != facts
                or not runner._is_session_run_current(session_key, generation)
                or not runner._is_user_authorized_for_source(source, allow_adapter_delegation=False)
                or getattr(execution[2],'session_id',None) not in sessions
                or (requested_session is not None and requested_session not in sessions)
                or not getattr(execution[2],'_current_turn_id',None)
                or (_approval_session_id.get() and _approval_session_id.get() != execution[2].session_id)
                or (_approval_turn_id.get() and _approval_turn_id.get() != execution[2]._current_turn_id)):
            return None
        current_route = dict(base, message_id=str(event.message_id), user_id=str(source.user_id),
            guild_id=str(source.scope_id or ''), channel_id=str(source.chat_id),thread_id=str(source.thread_id or ''),
            parent_channel_id=str(source.parent_chat_id or ''),chat_type=source.chat_type,
            message_sha256=hashlib.sha256(event.text.encode()).hexdigest())
        if current_route != base:
            return None
        from hermes_cli.plugins import get_plugin_manager
        manager = get_plugin_manager()
        selected = []
        for name, provider in manager._api_source_providers.items():
            qualify = getattr(provider, 'qualify_gateway_source', None)
            choice = qualify(dict(base)) if callable(qualify) else None
            if choice:
                selected.append((name,choice,provider))
        if len(selected) != 1:
            if selected_owner is not None:active = False
            return None
        name, choice, provider = selected[0]
        if selected_owner is not None and (selected_owner[0] is not manager or selected_owner[1] is not provider):
            active = False
            return None
        selected_owner = (manager,provider)
        if not isinstance(choice,dict) or set(choice) != {'intent'}:
            return None
        if not admit:
            # Hooks precede the inbound-row flush. This revocable inspection is
            # guidance only: admitting tools must still bind the durable row.
            return dict(base,turn_id=execution[2]._current_turn_id,provider=name,intent=choice['intent'])
        bound = db.bind_gateway_source_turn(session_id,base,execution[2]._current_turn_id,
            {'provider':name,'intent':choice['intent']},admit=admit)
        if admit and bound and display_metadata is not None:
            # The live user dict carries this metadata through the turn-end
            # flush, so persistence cannot erase the DB-owned turn binding.
            display_metadata['gateway_accepted_source'] = bound
        return bound
    token = _current.set(resolve)
    try:
        yield base
    finally:
        active = False
        _current.reset(token)


def current_gateway_source(*, session_id=None, admit=True):
    resolve = _current.get()
    try:
        source = resolve(admit is True,session_id) if resolve else None
    except (sqlite3.Error,OSError,ValueError,KeyError,OverflowError):
        return None
    return source
