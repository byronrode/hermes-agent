"""Generic accepted-source readback and detached delivery lifecycle.

Classification and protected transport belong to an external, profile-scoped
plugin. This owner supplies only admission integrity and delivery primitives.
OS protection of the owner, source and registry remains a deployment prerequisite.
"""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import re
import sqlite3

from aiohttp import web


_FAILURES = (ValueError, KeyError, TypeError, OSError, RuntimeError, TimeoutError, sqlite3.Error, OverflowError)


def source_provider(owner):
    name = owner.config.extra.get('accepted_source_provider')
    if not isinstance(name, str) or not name:
        return None
    from hermes_cli.plugins import get_plugin_manager
    manager = get_plugin_manager()
    if not manager._discovered:
        manager.discover_and_load()
    return manager._api_source_providers.get(name)


def admission_source(*, message, authenticated, profile, run_id, session_id, scope,
                     fingerprint, created_at, history_delivery, provider=None):
    if not authenticated or not isinstance(message, str) or not history_delivery or not fingerprint or provider is None:
        return None
    try:
        attributes = provider.qualify(message=message, profile=profile)
    except Exception:
        # A broken optional classifier removes source authority, not unrelated
        # ordinary intake. No private input or plugin exception is logged here.
        return None
    if not isinstance(attributes, dict) or not attributes:
        return None
    source = {'version': 1, 'provider': provider.name, 'run_id': run_id,
              'session_id': session_id, 'profile': profile, 'auth_scope': scope,
              'message_sha256': hashlib.sha256(message.encode()).hexdigest(),
              'body_fingerprint': fingerprint,
              'accepted_at': datetime.fromtimestamp(created_at, timezone.utc).isoformat()}
    if ((set(source) | {'owner_pid','owner_started','owner_revision'}) & set(attributes)
            or not all(isinstance(k, str) and isinstance(v, str) and len(v) <= 128 for k,v in attributes.items())):
        return None
    if len(json.dumps(attributes)) > 2048:
        return None
    return {**source, **attributes}


def read_source(owner, scope, run_id):
    store = owner._run_idempotency_store
    if not store.durable or owner._run_owners.get(run_id) != scope:
        raise ValueError('source_owner_unavailable')
    record = store.accepted_source_for_run(scope, run_id)
    if not record:
        raise ValueError('source_not_qualified')
    source, status = record['source'], record['status']
    provider = source_provider(owner)
    from hermes_cli.profiles import get_active_profile_name
    current = owner._run_statuses.get(run_id) or {}
    accepted = datetime.fromisoformat(source['accepted_at'])
    if (provider is None or provider.name != source['provider'] or source['profile'] != get_active_profile_name()
            or source['run_id'] != run_id or source['auth_scope'] != scope
            or source['body_fingerprint'] != record['fingerprint']
            or status.get('run_id') != run_id or status.get('session_id') != source['session_id']
            or status.get('status') not in {'queued', 'running', 'completed', 'waiting_for_approval'}
            or source['accepted_at'] != datetime.fromtimestamp(status['created_at'], timezone.utc).isoformat()
            or status.get('delivery_id')
            or current.get('run_id') != run_id or current.get('session_id') != source['session_id']
            or current.get('status') not in {'queued', 'running', 'completed', 'waiting_for_approval'}
            or current.get('delivery_id')
            or (record['owner_pid'], record['owner_started']) != (owner._run_owner_pid, owner._run_owner_started)
            or not 0 <= (datetime.now(timezone.utc) - accepted).total_seconds() <= store.RETENTION_SECONDS):
        raise ValueError('source_owner_changed')
    return {**source, 'owner_pid': record['owner_pid'], 'owner_started': record['owner_started'],
            'owner_revision': owner._run_owner_revision}


def _authorized(owner, request):
    error = owner._check_auth(request)
    if error is not None:
        return error
    if not owner._expected_api_key() or owner._room_grant_token(request):
        return web.json_response({'error': 'source_auth_required'}, status=403)
    return None


async def handle_source(owner, request):
    error = _authorized(owner, request)
    if error is not None:
        return error
    try:
        source = read_source(owner, owner._run_idempotency_scope(request), request.match_info['run_id'])
    except _FAILURES:
        return web.json_response({'error': 'source_unavailable'}, status=404)
    return web.json_response(source)


def _delivery(result, source):
    if not isinstance(result, dict) or result.get('accepted_source') != source:
        raise ValueError('source_binding_mismatch')
    if result.get('state') != 'answered':
        return None
    metadata = result.get('metadata')
    if (not isinstance(result.get('content'), str) or not 0 < len(result['content'].strip()) <= 7000
            or not re.fullmatch(r'[A-Za-z0-9_.:@+/-]{1,239}', str(result.get('identity', '')))
            or not re.fullmatch(r'[a-z][a-z0-9_]{0,79}', str(result.get('display_kind', '')))
            or not isinstance(metadata, dict) or metadata.get('delegation_id') != result['identity']
            or metadata.get('source_run_id') != source['run_id'] or len(json.dumps(result)) > 65536):
        raise ValueError('malformed_source_result')
    return result


async def handle_result(owner, request):
    error = _authorized(owner, request)
    if error is not None:
        return error
    try:
        if await request.json() != {}:
            raise ValueError('no_result_or_destination_arguments')
        scope, run_id = owner._run_idempotency_scope(request), request.match_info['run_id']
        source = read_source(owner, scope, run_id)
        provider = source_provider(owner)
        # The registered external owner must revalidate current authority and
        # immutable proof even for a previously validated/persisted result.
        result = await asyncio.wait_for(provider.read_result(source), timeout=3.5)
        if source_provider(owner) is not provider or source != read_source(owner, scope, run_id):
            raise ValueError('source_changed')
        delivery = _delivery(result, source)
        if delivery is None:
            return web.json_response({'state': result.get('state') if result.get('state') in {'queued','unavailable'} else 'unavailable'})
        db = await owner._ensure_session_db_async()
        session = await asyncio.to_thread(db.resolve_resume_session_id, source['session_id']) or source['session_id']
        # Check durable identity BEFORE applying a new-result age limit. Both the
        # reservation's pending slot and the existing transcript survive retries.
        pending = owner._run_idempotency_store.source_result_for_run(scope, run_id)
        existing = await asyncio.to_thread(db.get_source_result_delivery, session, delivery['identity'], delivery['display_kind'])
        if pending is not None and pending != delivery:
            raise ValueError('source_result_identity_changed')
        if existing is not None and (existing['content'] != delivery['content'] or existing['metadata'] != delivery['metadata']):
            raise ValueError('persisted_source_result_changed')
        if pending is None and existing is None:
            authored = datetime.fromisoformat(delivery['authored_at'].replace('Z','+00:00'))
            validated = datetime.fromisoformat(delivery['validated_at'].replace('Z','+00:00'))
            # A protected producer may have first validated before this owner
            # received it. This timestamp is not accepted from HTTP/model JSON.
            if (not 0 <= (validated-authored).total_seconds() <= 300
                    or validated > datetime.now(timezone.utc)
                    or authored < datetime.fromisoformat(source['accepted_at'])):
                raise ValueError('stale_first_source_result')
        # Persist first validation before the transcript lease can defer writing.
        owner._run_idempotency_store.remember_source_result(scope, run_id, source, delivery)
        if source != read_source(owner, scope, run_id):
            raise ValueError('source_changed_before_delivery')
        row = await asyncio.to_thread(db.append_source_result_delivery, session,
            delivery['content'], delivery['metadata'], display_kind=delivery['display_kind'])
        persisted = await asyncio.to_thread(db.get_source_result_delivery, session, delivery['identity'], delivery['display_kind'])
        if persisted is None or persisted['id'] != row or persisted['content'] != delivery['content'] or persisted['metadata'] != delivery['metadata']:
            raise ValueError('delivery_readback_failed')
        return web.json_response({'state':'persisted','run_id':run_id,'session_id':source['session_id'],'message_id':row})
    except _FAILURES:
        return web.json_response({'state':'unavailable'}, status=503)
