"""Bounded admission readback and detached result lifecycle, never model provenance.

OS protection of this owner, its code and registry is a deployment prerequisite.
The endpoint does not certify that protection and holds no company attester key.
"""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import uuid

from aiohttp import web


def status_role(message):
    if not isinstance(message, str):
        return None
    message = re.sub(r'^\[Mara (?:message|voice note): [0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\]\n', '', message)
    if not 0 < len(message) <= 300:
        return None
    role = r'(cto|cpo|ciso|support|head of customer success)'
    patterns = (r'(?:ignis\s+)?' + role + r'\s+status',
                r'ask (?:the )?' + role + r' for (?:a |the |current )?status',
                r'(?:mara,? )?get (?:a |the |current )?status from (?:the )?' + role,
                r'what(?:\'s| is) (?:the )?(?:current )?status (?:of|from) (?:the )?' + role)
    for pattern in patterns:
        match = re.fullmatch(pattern + r'[?.!]?', message.strip(), re.I)
        if match:
            return 'support' if match[1].lower() == 'head of customer success' else match[1].lower()
    return None


def admission_source(*, message, authenticated, profile, run_id, session_id, scope,
                     fingerprint, created_at, history_delivery):
    role = status_role(message)
    if not authenticated or not role or profile != 'default' or not history_delivery or not fingerprint:
        return None
    return {'version': 1, 'intent': 'role_status', 'role': role, 'run_id': run_id,
            'session_id': session_id, 'profile': profile, 'auth_scope': scope,
            'message_sha256': hashlib.sha256(message.encode()).hexdigest(),
            'body_fingerprint': fingerprint,
            'accepted_at': datetime.fromtimestamp(created_at, timezone.utc).isoformat()}


def read_source(owner, scope, run_id):
    store = owner._run_idempotency_store
    if not store.durable or owner._run_owners.get(run_id) != scope:
        raise ValueError('source_owner_unavailable')
    record = store.accepted_source_for_run(scope, run_id)
    if not record:
        raise ValueError('source_not_qualified')
    source, status = record['source'], record['status']
    current = owner._run_statuses.get(run_id) or {}
    accepted = datetime.fromisoformat(source['accepted_at'])
    if (source['run_id'] != run_id or source['auth_scope'] != scope
            or source['body_fingerprint'] != record['fingerprint']
            or status.get('run_id') != run_id or status.get('session_id') != source['session_id']
            or status.get('status') not in {'queued', 'running', 'completed', 'waiting_for_approval'}
            or source['accepted_at'] != datetime.fromtimestamp(status['created_at'], timezone.utc).isoformat()
            or status.get('delivery_id')
            or current.get('run_id') != run_id or current.get('session_id') != source['session_id']
            or current.get('status') not in {'queued', 'running', 'completed', 'waiting_for_approval'}
            or current.get('delivery_id')
            or (record['owner_pid'], record['owner_started']) != (owner._run_owner_pid, owner._run_owner_started)
            or not 0 <= (datetime.now(timezone.utc) - accepted).total_seconds() <= 86400):
        raise ValueError('source_owner_changed')
    return {**source, 'owner_pid': record['owner_pid'], 'owner_started': record['owner_started'],
            'owner_revision': owner._run_owner_revision}


def _authorized(owner, request):
    # Room grants and an unkeyed test listener never acquire source authority.
    error = owner._check_auth(request)
    if error is not None:
        return error
    from gateway.platforms.api_server import _api_request_profile
    if (_api_request_profile.get() not in (None, 'default')
            or not owner._expected_api_key() or owner._room_grant_token(request)):
        return web.json_response({'error': 'source_auth_required'}, status=403)
    return None


async def handle_source(owner, request):
    error = _authorized(owner, request)
    if error is not None:
        return error
    try:
        source = read_source(owner, owner._run_idempotency_scope(request), request.match_info['run_id'])
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error, OverflowError):
        return web.json_response({'error': 'source_unavailable'}, status=404)
    return web.json_response(source)


async def handle_result(owner, request):
    error = _authorized(owner, request)
    if error is not None:
        return error
    try:
        if await request.json() != {}:
            raise ValueError('no_result_or_destination_arguments')
        source = read_source(owner, owner._run_idempotency_scope(request), request.match_info['run_id'])
        # Fixed installation-owned command; no shell, payload-selected executable,
        # environment credential or raw prompt. The protected host independently
        # reads this admission and native proof, rather than trusting this JSON.
        transport = owner.config.extra.get('source_result_transport')
        if transport != {'host': 'ignis-one'}:
            return web.json_response({'state': 'unavailable'}, status=503)
        home = Path.home()
        command = [str(home / '.local/share/mara-host-agent/current'),
                   '-config', str(home / '.config/mara-host-agent/config.json'),
                   '-send-action', 'role-status-source', '-target', 'ignis-one']
        process = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            env={'PATH': '/usr/bin:/bin', 'HOME': str(home)})
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(json.dumps(
                {'run_id': source['run_id'], 'phase': 'read'}).encode()), timeout=3.5)
        except (TimeoutError, asyncio.CancelledError):
            process.kill()
            await process.wait()
            raise
        if process.returncode or len(stdout) > 65536:
            raise ValueError('protected_result_unavailable')
        result = json.loads(stdout)
        # Re-read after I/O: cancellation/revocation/session substitution cannot
        # publish into a now-changed run. Caller-supplied JSON is never accepted.
        if source != read_source(owner, owner._run_idempotency_scope(request), source['run_id']):
            raise ValueError('source_changed')
        if result.get('accepted_source') != source:
            raise ValueError('source_binding_mismatch')
        if result.get('state') != 'answered':
            return web.json_response({'state': result.get('state') if result.get('state') in {'queued', 'unavailable'} else 'unavailable'})
        answer = result['answer']
        if (not isinstance(answer, dict) or not isinstance(answer.get('text'), str)
                or not 0 < len(answer['text'].strip()) <= 6000
                or not re.fullmatch(r'document:[0-9a-f-]{36}:[0-9a-f-]{36}', result.get('source_reference', ''))
                or not re.fullmatch(r'[0-9a-f-]{36}', result.get('request_id', ''))
                or not re.fullmatch(r'run:[0-9a-f-]{36}', answer.get('source_reference', ''))):
            raise ValueError('malformed_result')
        for identity in (result['request_id'], answer['agent_id'], answer['source_reference'][4:]):
            if str(uuid.UUID(identity)) != identity:
                raise ValueError('malformed_result_identity')
        authored = datetime.fromisoformat(answer['updated_at'].replace('Z', '+00:00'))
        if (not 0 <= (datetime.now(timezone.utc) - authored).total_seconds() <= 300
                or authored < datetime.fromisoformat(source['accepted_at'])):
            raise ValueError('stale_result')
        db = await owner._ensure_session_db_async()
        # Existing detached delivery writer owns locking, lineage and idempotency.
        metadata = {'delegation_id': 'role-status:' + result['request_id'],
                    'source_run_id': source['run_id'], 'source_reference': result['source_reference'],
                    'role': source['role'], 'agent_id': answer['agent_id'],
                    'answer_source_reference': answer['source_reference'], 'authored_at': answer['updated_at']}
        session = await asyncio.to_thread(db.resolve_resume_session_id, source['session_id'])
        session = session or source['session_id']
        row = await asyncio.to_thread(db.append_role_result_delivery, session,
                                     source['role'].upper() + ': ' + answer['text'], metadata)
        lineage = await asyncio.to_thread(db._resume_lineage_ids, session)
        messages = []
        for identity in lineage:
            messages.extend(await asyncio.to_thread(db.get_messages, identity, include_compacted=True))
        if not any(m['id'] == row and m['role'] == 'assistant' and m.get('display_kind') == 'role_status_result'
                   and m.get('display_metadata') == metadata for m in messages):
            raise ValueError('delivery_readback_failed')
        return web.json_response({'state': 'persisted', 'run_id': source['run_id'],
                                  'session_id': source['session_id'], 'message_id': row})
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, TimeoutError, sqlite3.Error, OverflowError):
        return web.json_response({'state': 'unavailable'}, status=503)
