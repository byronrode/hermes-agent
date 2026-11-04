"""Generic accepted-source readback and detached delivery lifecycle.

Classification and protected transport belong to an external, profile-scoped
plugin. This owner supplies only admission integrity and delivery primitives.
OS protection of the owner, source and registry remains a deployment prerequisite.
"""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import math
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


class SourceSnapshotUnavailable(ValueError):
    pass


_DIAGNOSTIC_REASONS = frozenset({
    'source_owner_unavailable','source_not_qualified','source_owner_changed',
    'settled_source_snapshot_required','settled_source_snapshot_changed',
    'source_provider_changed','snapshot_recovery_unavailable','invalid_source_read_budget',
    'source_changed','source_changed_before_delivery','no_result_or_destination_arguments',
    'source_binding_mismatch','malformed_source_result','source_result_identity_changed',
    'persisted_source_result_changed','stale_first_source_result','delivery_readback_failed',
})


def source_diagnostic(error, phase):
    if isinstance(error, SourceResolutionFailure):
        return error.diagnostic
    reason = str(error)
    kind = type(error).__name__
    kinds = {'ValueError','KeyError','TypeError','OSError','RuntimeError','TimeoutError',
             'JSONDecodeError','OperationalError','RequestLifecycleError','PaperclipError','PermissionError'}
    # Provider, JSON and OS messages can contain private input or credentials.
    return {'phase':phase, 'error_type':kind if kind in kinds else 'SourceError',
            'reason':reason if reason in _DIAGNOSTIC_REASONS else
            ('source_read_timeout' if isinstance(error, TimeoutError) else 'source_read_failed')}


class SourceResolutionFailure(ValueError):
    def __init__(self, error, phase):
        self.diagnostic = source_diagnostic(error, phase)
        super().__init__('source_resolution_failed')


def read_source(owner, scope, run_id, *, snapshot=None, allow_unsnapshotted=False):
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
    same_owner = (record['owner_pid'], record['owner_started']) == (owner._run_owner_pid, owner._run_owner_started)
    if not same_owner:
        from gateway.platforms.api_server_runs import _owner_alive
        if (status.get('status') != 'completed' or current.get('status') != 'completed'
                or record['owner_pid'] <= 0 or record['owner_started'] <= 0
                or _owner_alive(record['owner_pid'], record['owner_started'])):
            raise ValueError('source_owner_changed')
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
            or not 0 <= (datetime.now(timezone.utc) - accepted).total_seconds() <= store.RETENTION_SECONDS):
        raise ValueError('source_owner_changed')
    base = {**source, 'owner_pid': record['owner_pid'], 'owner_started': record['owner_started'],
            'owner_revision': owner._run_owner_revision if same_owner else None}
    if same_owner:
        return base
    pending = store.source_result_for_run(scope, run_id)
    if pending is not None and (not isinstance(pending, dict) or not isinstance(pending.get('accepted_source'), dict)):
        raise ValueError('settled_source_snapshot_changed')
    original = pending['accepted_source'] if pending is not None else snapshot
    if original is None:
        if allow_unsnapshotted:
            return base
        raise SourceSnapshotUnavailable('settled_source_snapshot_required')
    if (not isinstance(original, dict) or set(original) != set(base)
            or any(original[k] != v for k,v in base.items() if k != 'owner_revision')
            or not re.fullmatch(r'[0-9a-f]{40}', str(original['owner_revision']))):
        raise ValueError('settled_source_snapshot_changed')
    return original


def revalidate_source(owner, request, scope, run_id, *, snapshot):
    """A remote await may cross normal terminal cache retirement.

    Rehydrate through the existing authenticated/scoped durable run lifecycle,
    never by copying a producer snapshot into ownership state. Explicit foreign
    ownership is a denial, not a cache miss. All source/epoch/liveness guards
    still run against the current durable reservation and original snapshot.
    """
    if (_authorized(owner, request) is not None
            or owner._run_idempotency_scope(request) != scope
            or owner._run_owners.get(run_id) not in (None,scope)):
        raise ValueError('source_owner_unavailable')
    owner._durable_run_status(request,run_id)
    return read_source(owner,scope,run_id,snapshot=snapshot)


def source_read_budget(provider):
    timeout = getattr(provider, 'read_timeout_seconds', 3.5)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 30:
        raise ValueError('invalid_source_read_budget')
    return timeout


async def resolve_source(owner, request, scope, run_id):
    # Reuse the authenticated durable-run lifecycle; it marks abandoned active
    # runs interrupted and NEVER launches/reassigns a model or admission.
    phase = 'durable_status'
    try:
        owner._durable_run_status(request, run_id)
        phase = 'source_validation'
        try:
            return read_source(owner, scope, run_id)
        except SourceSnapshotUnavailable:
            phase = 'snapshot_provider'
            provider = source_provider(owner)
            recover = getattr(provider, 'recover_source', None)
            if not callable(recover):
                raise ValueError('snapshot_recovery_unavailable')
            base = read_source(owner, scope, run_id, allow_unsnapshotted=True)
            timeout = source_read_budget(provider)
            phase = 'snapshot_recovery'
            snapshot = await asyncio.wait_for(recover(base), timeout)
            phase = 'snapshot_provider_validation'
            if source_provider(owner) is not provider:
                raise ValueError('source_provider_changed')
            phase = 'snapshot_validation'
            return revalidate_source(owner, request, scope, run_id, snapshot=snapshot)
    except _FAILURES as error:
        raise SourceResolutionFailure(error, phase) from None


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
        source = await resolve_source(owner, request, owner._run_idempotency_scope(request), request.match_info['run_id'])
    except _FAILURES as error:
        return web.json_response({'error': 'source_unavailable',
                                  'diagnostic':source_diagnostic(error,'source_validation')}, status=404)
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
    phase = 'request_validation'
    try:
        if await request.json() != {}:
            raise ValueError('no_result_or_destination_arguments')
        scope, run_id = owner._run_idempotency_scope(request), request.match_info['run_id']
        source = await resolve_source(owner, request, scope, run_id)
        phase = 'producer_lookup'
        provider = source_provider(owner)
        # The registered external owner must revalidate current authority and
        # immutable proof even for a previously validated/persisted result.
        phase = 'producer_budget'
        timeout = source_read_budget(provider)
        phase = 'producer_read'
        result = await asyncio.wait_for(provider.read_result(source), timeout=timeout)
        phase = 'producer_validation'
        if source_provider(owner) is not provider or source != revalidate_source(owner, request, scope, run_id, snapshot=source):
            raise ValueError('source_changed')
        multiple=getattr(provider,'source_result_protocol',None)=='per_intent_v1' and source.get('intent')=='mara_request'
        more=False
        if multiple and isinstance(result,dict):
            more=result.get('more_results') is True
            result={k:v for k,v in result.items() if k!='more_results'}
        delivery = _delivery(result, source)
        if delivery is None:
            return web.json_response({'state': result.get('state') if result.get('state') in {'queued','unavailable'} else 'unavailable'})
        phase = 'delivery_commit'
        db = await owner._ensure_session_db_async()
        session = await asyncio.to_thread(db.resolve_resume_session_id, source['session_id']) or source['session_id']
        # Check durable identity BEFORE applying a new-result age limit. Both the
        # reservation's pending slot and the existing transcript survive retries.
        pending = owner._run_idempotency_store.source_result_for_run(scope, run_id, **({'identity':delivery['identity']} if multiple else {}))
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
        owner._run_idempotency_store.remember_source_result(scope, run_id, source, delivery, **({'allow_multiple':True} if multiple else {}))
        if source != revalidate_source(owner, request, scope, run_id, snapshot=source):
            raise ValueError('source_changed_before_delivery')
        row = await asyncio.to_thread(db.append_source_result_delivery, session,
            delivery['content'], delivery['metadata'], display_kind=delivery['display_kind'])
        persisted = await asyncio.to_thread(db.get_source_result_delivery, session, delivery['identity'], delivery['display_kind'])
        if persisted is None or persisted['id'] != row or persisted['content'] != delivery['content'] or persisted['metadata'] != delivery['metadata']:
            raise ValueError('delivery_readback_failed')
        return web.json_response({'state':'persisted','run_id':run_id,'session_id':source['session_id'],'message_id':row,**({'more_results':more} if multiple else {})})
    except _FAILURES as error:
        return web.json_response({'state':'unavailable',
                                  'diagnostic':source_diagnostic(error,phase)}, status=503)
