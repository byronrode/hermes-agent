"""Profile-owned outbound policy at provider transport admission.

A configured policy is required and fail-closed. Other profiles retain ordinary
transport behavior. Provider calls consume the same receipt regardless of caller.
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from typing import Any


class OutboundHeld(RuntimeError):
    def __init__(self, receipt):
        self.receipt = receipt
        super().__init__('outbound held: ' + receipt.get('reason', 'policy refused delivery'))


def evaluate(platform: str, recipient: str, content: str, *, controls=None,
             profile_home=None, dry_run=False, operation='send', test=False, probe=False) -> dict[str, Any]:
    from hermes_constants import get_hermes_home
    from hermes_yaml import safe_load
    home = Path(profile_home or get_hermes_home())
    held = lambda reason: {'allow': False, 'success': False, 'state': 'held', 'delivered': False, 'reason': reason, 'error': 'outbound held: ' + reason}
    if dry_run:
        return held('dry run: no provider delivery')
    config_path = home / 'config.yaml'
    try:
        config = safe_load(config_path.read_text()) or {} if config_path.exists() else {}
        policy = config.get('outbound_policy') or {}
    except Exception:
        return held('profile configuration could not be read')
    if not policy.get('required'):
        return {'allow': True}
    try:
        source = Path(policy['callback_file'])
        if not source.is_absolute():
            return held('outbound callback must be a protected absolute path')
        spec = importlib.util.spec_from_file_location('hermes_profile_outbound_policy', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        provenance = current_provenance(content) or reply_handoff(platform, recipient, content, profile_home=home)
        if provenance and provenance.get('home_scope') != str(home):
            provenance = None
        receipt = module.decide({'platform': platform, 'recipient': str(recipient), 'content': content,
            'controls': controls, 'dry_run': dry_run, 'operation': operation, 'test': test or bool(os.environ.get('PYTEST_CURRENT_TEST')), 'probe': probe,
            'profile_home': str(home), 'provenance': provenance}, settings=policy.get('settings') or {})
        if isinstance(receipt, dict) and receipt.get('allow') is True:
            if operation == 'emit':
                reply_handoff(platform, recipient, content, consume=True, profile_home=home)
            return receipt
        return {**(receipt if isinstance(receipt, dict) else {}),
                **held(receipt.get('reason', 'outbound policy held the message') if isinstance(receipt, dict) else 'outbound policy held the message')}
    except Exception:
        return held('required outbound policy unavailable')


def _discord_body(payload):
    if not isinstance(payload, dict) or payload.get('type') in {5, 6, 8, 9}:
        return None  # Interaction acknowledgement/modal, not a delivered message body.
    body = payload.get('data') if isinstance(payload.get('data'), dict) else payload
    if isinstance(body.get('message'), dict):
        body = body['message']
    if not any(key in body for key in ('content', 'embeds', 'components', 'attachments')):
        return None
    visible_keys = {'content', 'title', 'description', 'text', 'name', 'value', 'label',
                    'filename', 'caption', 'placeholder', 'url'}
    def visible(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in visible_keys and isinstance(child, str):
                    yield child
                elif isinstance(child, (dict, list)):
                    yield from visible(child)
        elif isinstance(value, list):
            for child in value:
                yield from visible(child)
    return '\n'.join(visible(body)), body.get('components')


def install_discord_transport(client, *, profile_home):
    """Guard native HTTP and its shared session, including SDK interaction followups."""
    from contextvars import ContextVar
    admitted = ContextVar('hermes_discord_admitted', default=False)
    original = client.http.request
    if getattr(original, '_hermes_outbound_guard', False):
        return

    def guard_session():
        session = getattr(client.http, '_HTTPClient__session', None)
        if session is None or getattr(session, 'closed', True):
            return
        previous = session._request
        if getattr(previous, '_hermes_outbound_guard', False):
            return
        async def session_request(method, url, *args, **kwargs):
            if not admitted.get() and str(method).upper() in {'POST', 'PATCH'}:
                body = _discord_body(kwargs.get('json'))
                if body is None and kwargs.get('data') is not None:
                    # Direct webhook multipart must expose its plain payload before emission.
                    data = kwargs['data']
                    if isinstance(data, str):
                        body = _discord_body(json.loads(data))
                    else:
                        for options, _headers, value in getattr(data, '_fields', []):
                            if options.get('name') == 'payload_json':
                                body = _discord_body(json.loads(value))
                                break
                    if body is None:
                        receipt = evaluate('discord', '', '', profile_home=profile_home)
                        if receipt.get('allow') is not True:
                            raise OutboundHeld(receipt)
                if body is not None:
                    receipt = evaluate('discord', '', body[0], controls=body[1], profile_home=profile_home,
                                       operation='edit' if str(method).upper() == 'PATCH' else 'send')
                    if receipt.get('allow') is not True:
                        raise OutboundHeld(receipt)
            return await previous(method, url, *args, **kwargs)
        session_request._hermes_outbound_guard = True
        session._request = session_request

    async def request(route, *args, **kwargs):
        guard_session()  # static_login has now materialized the SDK's shared session.
        payload = kwargs.get('json')
        if payload is None:
            for part in kwargs.get('form') or []:
                if part.get('name') == 'payload_json':
                    payload = json.loads(part['value'])
                    break
        body = _discord_body(payload)
        if getattr(route, 'method', '') in {'POST', 'PATCH'} and body is not None:
            receipt = evaluate('discord', str(getattr(route, 'channel_id', '') or ''), body[0],
                controls=body[1], profile_home=profile_home,
                operation='edit' if route.method == 'PATCH' else 'send')
            if receipt.get('allow') is not True:
                raise OutboundHeld(receipt)
        token = admitted.set(body is not None)
        try:
            return await original(route, *args, **kwargs)
        finally:
            admitted.reset(token)
    request._hermes_outbound_guard = True
    client.http.request = request


# These custody scopes are entered by the native gateway/cron owners, never by
# caller metadata. Their resolver revalidates the live source before every send.
from contextlib import contextmanager
from contextvars import ContextVar
import hashlib

_provenance = ContextVar('outbound_delivery_provenance', default=None)


def _text_digest(text):
    return hashlib.sha256(' '.join(str(text).split()).encode()).hexdigest()


def cron_fingerprint(job):
    keys = ('id', 'name', 'prompt', 'schedule', 'deliver', 'deliver_extra', 'profile', 'script', 'no_agent')
    return hashlib.sha256(json.dumps({key: job.get(key) for key in keys}, sort_keys=True,
                                    separators=(',', ':'), default=str).encode()).hexdigest()


@contextmanager
def authorized_cron_scope(job, content, *, for_failure=False):
    active = True
    from hermes_constants import get_hermes_home
    identity = {'kind': 'personal_reminder', 'home_scope': str(get_hermes_home()), 'job_id': str(job.get('id') or ''),
                'job_sha256': cron_fingerprint(job), 'output_sha256': _text_digest(content)}
    token = _provenance.set(lambda: dict(identity) if active and not for_failure else None)
    try:
        yield
    finally:
        active = False
        _provenance.reset(token)


@contextmanager
def authorized_reply_scope(runner, event, source, session_key, generation):
    active = True
    platform = getattr(getattr(source, 'platform', None), 'value', str(getattr(source, 'platform', '')))
    actor, recipient = str(source.user_id or ''), str(source.chat_id or '')
    message_id = str(getattr(event, 'message_id', '') or '')
    original = str(getattr(event, 'text', '') or '')
    raw = getattr(event, 'raw_message', None)
    home = None
    from hermes_constants import get_hermes_home
    home = str(get_hermes_home())
    def resolve():
        if (not active or event is None or getattr(event, 'internal', False)
                or getattr(source, 'is_bot', False) or not message_id
                or str(get_hermes_home()) != home
                or not runner._is_session_run_current(session_key, generation)
                or not runner._is_user_authorized_for_source(source, allow_adapter_delegation=False)
                or str(source.user_id or '') != actor or str(source.chat_id or '') != recipient
                or str(getattr(event, 'message_id', '') or '') != message_id
                or str(getattr(event, 'text', '') or '') != original):
            return None
        if platform == 'whatsapp_cloud':
            if not isinstance(raw, dict) or str(raw.get('id', '')) != message_id or str(raw.get('from', '')) != actor:
                return None
        elif platform == 'discord':
            adapter = runner._intake_adapter_for(source)
            reader = getattr(adapter, 'source_event_identity', None)
            if not callable(reader) or not reader(event):
                return None
        else:
            return None
        return {'kind': 'authenticated_reply', 'home_scope': home, 'platform': platform, 'user_id': actor,
                'recipient': recipient, 'message_id': message_id, 'source_text': original}
    token = _provenance.set(resolve)
    try:
        yield
    finally:
        active = False
        _provenance.reset(token)


def current_provenance(content):
    reader = _provenance.get()
    proof = reader() if callable(reader) else None
    if proof and proof.get('kind') == 'personal_reminder' and proof.get('output_sha256') != _text_digest(content):
        return None
    return proof


# A final response returns from the authenticated handler before the adapter
# emits it. Retain only this exact output/recipient for that short handoff.
import time
import threading
_reply_grants = {}
_reply_grants_lock = threading.Lock()


def retain_final_reply(content):
    proof = current_provenance(content)
    if not proof or proof.get('kind') != 'authenticated_reply':
        return
    key = (proof['home_scope'], proof['platform'], proof['recipient'].lstrip('+'), _text_digest(content))
    with _reply_grants_lock:
        now = time.monotonic()
        for old, (_value, deadline) in list(_reply_grants.items()):
            if deadline < now:
                _reply_grants.pop(old, None)
        _reply_grants[key] = (proof, now + 60)


def reply_handoff(platform, recipient, content, *, consume=False, profile_home=None):
    from hermes_constants import get_hermes_home
    key = (str(profile_home or get_hermes_home()), platform, str(recipient).lstrip('+'), _text_digest(content))
    with _reply_grants_lock:
        value = _reply_grants.get(key)
        if not value or value[1] < time.monotonic():
            _reply_grants.pop(key, None)
            return None
        if consume:
            _reply_grants.pop(key, None)
        return dict(value[0])
