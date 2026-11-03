"""Owner-only retry of a persisted, qualified failed intake, without a model turn."""
import asyncio
from contextvars import ContextVar, copy_context
import hashlib
import json
import os
import threading
import time
import uuid

from aiohttp import web

_current = ContextVar('gateway_failed_intake_recovery', default=None)
_REASONS = frozenset({'qualified_source_required','source_custody_changed','unbound_intake_result',
    'intake_not_failed_before_capture','unique_failed_intake_required','recovery_custody_changed',
    'recovery_payload_changed','gateway_owner_unavailable','intake_recovery_unavailable','source_owner_live',
    'source_owner_or_policy_changed','recovery_evidence_changed','recovery_owner_live',
    'recovery_attempts_exhausted','recovery_owner_unqualified','no_caller_payload_permitted'})


def current_intake_recovery():
    scope = _current.get()
    if scope is None or not scope['active'] or scope['thread'] != threading.get_ident():
        return None
    scope['validate']()
    return json.loads(json.dumps(scope['evidence']))


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def failed_intake(db, message_id, tool_name):
    rows = db._read_all("SELECT m.id,m.session_id,m.content,m.display_metadata FROM messages m JOIN sessions s ON s.id=m.session_id "
        "WHERE s.source='discord' AND m.role='user' AND (m.active=1 OR m.compacted=1) AND m.platform_message_id=? "
        "AND json_extract(m.display_metadata,'$.gateway_accepted_source') IS NOT NULL", (message_id,))
    if len(rows) != 1:
        raise ValueError('qualified_source_required')
    row = rows[0]
    meta = json.loads(row['display_metadata'])
    source = meta['gateway_accepted_source']
    candidate = meta.get('gateway_source_candidate')
    if (not isinstance(candidate,dict)
            or source != dict(candidate,turn_id=source['turn_id'],provider=source['provider'],intent=source['intent'])
            or source['session_id'] != row['session_id'] or meta.get('gateway_source_consumed')):
        raise ValueError('source_custody_changed')
    db.gateway_source_row(source)
    # Never search unrelated sessions or cross the next user turn.
    messages = db._read_all("SELECT id,role,content,tool_calls,tool_name,tool_call_id FROM messages "
        "WHERE session_id=? AND id>? AND id<coalesce((SELECT min(id) FROM messages WHERE session_id=? "
        "AND role='user' AND id>?),9223372036854775807) AND (active=1 OR compacted=1) ORDER BY id LIMIT 200",
        (row['session_id'],row['id'],row['session_id'],row['id']))
    calls = {}
    failures = []
    for message in messages:
        if message['role'] == 'assistant' and message['tool_calls']:
            for call in json.loads(message['tool_calls']):
                function = call.get('function', {})
                raw = function.get('arguments')
                args = json.loads(raw) if isinstance(raw,str) else raw
                if function.get('name') == 'tool_call':
                    from tools.tool_search_validation import normalize_tool_call_entries
                    nested, error = normalize_tool_call_entries(args) if isinstance(args,dict) else ([], 'invalid')
                    if error or len(nested) != 1 or nested[0].get('name') != tool_name:
                        continue
                    args = nested[0].get('arguments')
                elif function.get('name') != tool_name:
                    continue
                if isinstance(args,dict):
                    calls[call.get('id')] = (message['id'],raw,args,message['tool_calls'])
        elif message['role'] == 'tool' and message['tool_name'] == tool_name:
            prior = calls.get(message['tool_call_id'])
            if prior is None:
                raise ValueError('unbound_intake_result')
            result = json.loads(message['content'])
            if (result.get('blocked_phase') != 'canonical_capture'
                    or result.get('execution_verified') is not False or result.get('delivery_verified') is not False
                    or result.get('items')):
                raise ValueError('intake_not_failed_before_capture')
            failures.append((prior,message['id'],message['content']))
    if len(failures) != 1:
        raise ValueError('unique_failed_intake_required')
    prior, result_row, result_body = failures[0]
    evidence = dict(source=source, arguments=prior[2], user_row=row['id'],
        call_row=prior[0], result_row=result_row,
        call_hash=_hash(prior[3]),result_hash=_hash(result_body),user_hash=_hash(row['content']),
        intent_hash=_hash([source,row['content'],prior[0],prior[1],result_row,result_body]))
    return evidence, meta.get('gateway_intake_recovery')


def _cas(db, evidence, previous, value):
    def write(conn):
        row = conn.execute('SELECT display_metadata,content FROM messages WHERE id=?',(evidence['user_row'],)).fetchone()
        meta = json.loads(row[0]) if row else {}
        if (meta.get('gateway_accepted_source') != evidence['source']
                or meta.get('gateway_source_consumed') or meta.get('gateway_intake_recovery') != previous
                or _hash(row[1]) != evidence['user_hash']):
            raise ValueError('recovery_custody_changed')
        call = conn.execute('SELECT tool_calls FROM messages WHERE id=?',(evidence['call_row'],)).fetchone()
        result = conn.execute('SELECT content FROM messages WHERE id=?',(evidence['result_row'],)).fetchone()
        if (not call or not result or _hash(call[0]) != evidence['call_hash']
                or _hash(result[0]) != evidence['result_hash']):
            raise ValueError('recovery_payload_changed')
        meta['gateway_intake_recovery'] = value
        conn.execute('UPDATE messages SET display_metadata=? WHERE id=?',
            (json.dumps(meta,sort_keys=True),evidence['user_row']))
    db._execute_write(write)


def retry_failed_intake(runner, message_id):
    from gateway.source_delivery import _source_route
    from gateway.platforms.api_server_runs import _owner_alive
    from gateway.status import get_process_start_time
    from hermes_cli.plugins import get_plugin_manager
    if getattr(runner,'_primary_profile_name',None) != 'default' or not getattr(runner,'_running',False):
        raise ValueError('gateway_owner_unavailable')
    db = getattr(runner._session_db,'_db',runner._session_db)
    # Only the durable native source can select a provider. A public message ID
    # or API operator credential cannot manufacture a channel gesture.
    rows = db._read_all("SELECT display_metadata FROM messages WHERE role='user' AND platform_message_id=? "
        "AND json_extract(display_metadata,'$.gateway_accepted_source') IS NOT NULL",(message_id,))
    if len(rows) != 1:
        raise ValueError('qualified_source_required')
    source = json.loads(rows[0][0])['gateway_accepted_source']
    manager = get_plugin_manager()
    provider = manager._api_source_providers.get(source.get('provider'))
    callback = getattr(provider,'retry_gateway_intake',None)
    tool_name = getattr(provider,'gateway_intake_tool',None)
    if (not callable(callback) or not isinstance(tool_name,str)
            or not callable(getattr(provider,'validate_gateway_intake_recovery',None))):
        raise ValueError('intake_recovery_unavailable')
    evidence, prior = failed_intake(db,message_id,tool_name)
    holder = f'pid={os.getpid()}:intake-recovery={uuid.uuid4()}'
    if not db.try_acquire_session_turn_lease(source['session_id'],holder,ttl_seconds=300):
        raise ValueError('source_owner_live')
    try:
        deadline = time.monotonic() + 120
        def validate():
            _, adapter = _source_route(runner,db,source)
            if (time.monotonic() >= deadline or adapter is None or not runner._running
                    or runner._is_session_running(source['session_key'])
                    or get_plugin_manager() is not manager
                    or manager._api_source_providers.get(source['provider']) is not provider
                    or provider.qualify_gateway_source(dict(source)) != {'intent':source['intent']}):
                raise ValueError('source_owner_or_policy_changed')
            fresh, _ = failed_intake(db,message_id,tool_name)
            if fresh != evidence or not db.refresh_session_turn_lease(source['session_id'],holder,ttl_seconds=300):
                raise ValueError('recovery_evidence_changed')
        validate()
        if prior and prior.get('intent_hash') != evidence['intent_hash']:
            raise ValueError('recovery_payload_changed')
        if prior and prior.get('state') == 'running' and _owner_alive(prior.get('pid',0),prior.get('started',0)):
            raise ValueError('recovery_owner_live')
        if prior and prior.get('state') != 'complete' and prior.get('attempt',0) >= 3:
            raise ValueError('recovery_attempts_exhausted')
        execution = dict(intent_hash=evidence['intent_hash'],execution_id=str(uuid.uuid4()),
            pid=os.getpid(),started=get_process_start_time(os.getpid()),state='running',
            attempt=(prior or {}).get('attempt',0)+1)
        if not execution['started']:
            raise ValueError('recovery_owner_unqualified')
        scope = dict(active=True,thread=threading.get_ident(),
            evidence=dict(evidence,recovery_receipt=(prior or {}).get('receipt')),validate=validate)
        token = _current.set(scope)
        try:
            # Provider validates current company/owner permission even for a
            # lost-response receipt. A completed recovery never dispatches again.
            provider.validate_gateway_intake_recovery()
            if prior and prior.get('state') == 'complete':
                return prior['receipt']
            _cas(db,evidence,prior,execution)
            try:
                result = callback()
                validate()
            except Exception:
                _cas(db,evidence,execution,dict(execution,state='retryable'))
                raise
            receipt = dict(state='captured',source_run_id=source['run_id'],session_id=source['session_id'],
                turn_id=source['turn_id'],message_id=source['message_id'],intent_hash=evidence['intent_hash'],
                execution_id=execution['execution_id'],items=result['items'])
            complete = dict(execution,state='complete',receipt=receipt)
            _cas(db,evidence,execution,complete)
            return receipt
        finally:
            scope['active'] = False
            _current.reset(token)
    finally:
        db.release_session_turn_lease(source['session_id'],holder)


async def handle_retry(owner, request):
    from gateway.platforms.api_server_source_receipts import _authorized
    error = _authorized(owner,request)
    if error is not None:
        return error
    try:
        if await request.json() != {}:
            raise ValueError('no_caller_payload_permitted')
        runner = owner.gateway_runner
        # The worker and durable PID claim survive a disconnected HTTP caller.
        # Existing API shutdown accounting keeps SessionDB open until it exits.
        from gateway.platforms.api_server_runs import _submit_api_worker
        context = copy_context()
        work = _submit_api_worker(asyncio.get_running_loop(),
            lambda: context.run(retry_failed_intake,runner,request.match_info['message_id']))
        return web.json_response(await asyncio.shield(work))
    except (ValueError,RuntimeError,OSError,KeyError,TypeError) as error:
        reason = str(error)
        return web.json_response({'error':'failed_intake_recovery_unavailable',
            'reason':reason if reason in _REASONS else 'current_policy_or_intake_unavailable'},status=409)
