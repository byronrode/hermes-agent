"""Profile-owned outbound policy at provider transport admission.

A configured policy is required and fail-closed. Other profiles retain ordinary
transport behavior. Provider calls consume the same receipt regardless of caller.
"""
from __future__ import annotations

import importlib.util
import json
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
        receipt = module.decide({'platform': platform, 'recipient': str(recipient), 'content': content,
            'controls': controls, 'dry_run': dry_run, 'operation': operation, 'test': test, 'probe': probe,
            'profile_home': str(home)}, settings=policy.get('settings') or {})
        if isinstance(receipt, dict) and receipt.get('allow') is True:
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
