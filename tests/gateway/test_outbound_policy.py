import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.outbound_policy import evaluate, install_discord_transport, OutboundHeld


def test_required_policy_is_profile_scoped_and_fails_closed(tmp_path):
    callback = tmp_path / 'policy.py'
    callback.write_text("def decide(event, settings=None):\n return {'allow': event['content'].startswith('Useful')}\n")
    first, second = tmp_path / 'first', tmp_path / 'second'
    first.mkdir(); second.mkdir()
    (first / 'config.yaml').write_text(json.dumps({'outbound_policy': {'required': True, 'callback_file': str(callback)}}))
    (second / 'config.yaml').write_text('{}')
    assert evaluate('discord', 'person', 'Builder output', profile_home=first)['allow'] is not True
    assert evaluate('discord', 'person', 'Builder output', profile_home=second)['allow'] is True
    assert evaluate('discord', 'person', 'Builder output', profile_home=first)['allow'] is not True
    assert evaluate('discord', 'person', 'Useful decision', profile_home=first)['allow'] is True
    from gateway.outbound_policy import (authorized_reply_scope, current_provenance,
        retain_final_reply, reply_handoff, authorized_cron_scope, cron_fingerprint)
    source = SimpleNamespace(platform=SimpleNamespace(value='whatsapp_cloud'), user_id='person',
                             chat_id='person', is_bot=False)
    event = SimpleNamespace(internal=False, message_id='native-message', text='When is it?',
                            raw_message={'id': 'native-message', 'from': 'person'})
    runner = SimpleNamespace(_is_session_run_current=lambda *args: True,
                             _is_user_authorized_for_source=lambda *args, **kwargs: True)
    with authorized_reply_scope(runner, event, source, 'session', 1):
        assert current_provenance('Answer')['kind'] == 'authenticated_reply'
        retain_final_reply('Exact answer')
    assert current_provenance('Answer') is None
    assert reply_handoff('whatsapp_cloud', 'person', 'Changed answer') is None
    assert reply_handoff('whatsapp_cloud', 'person', 'Exact answer', profile_home=second) is None
    assert reply_handoff('whatsapp_cloud', 'person', 'Exact answer', consume=True)['message_id'] == 'native-message'
    assert reply_handoff('whatsapp_cloud', 'person', 'Exact answer') is None
    event.raw_message['from'] = 'forged'
    with authorized_reply_scope(runner, event, source, 'session', 1):
        assert current_provenance('Answer') is None
    job = {'id': 'standing-job', 'prompt': 'Take the medicine', 'deliver': 'whatsapp_cloud'}
    with authorized_cron_scope(job, 'Take the medicine'):
        assert current_provenance('Take the medicine')['job_sha256'] == cron_fingerprint(job)
        assert current_provenance('Unrelated announcement') is None
    assert current_provenance('Take the medicine') is None
    with authorized_cron_scope(job, 'Could not run the reminder', for_failure=True):
        assert current_provenance('Could not run the reminder') is None
    callback.write_text("def decide(event, settings=None):\n return {'allow': not event.get('test')}\n")
    assert evaluate('discord', 'Byron', 'Useful plausible decision', profile_home=first)['allow'] is not True
    callback.unlink()
    held = evaluate('discord', 'person', 'Useful decision', profile_home=first)
    assert held['state'] == 'held' and held['delivered'] is False


def test_native_sdk_and_followup_session_cannot_bypass_policy(tmp_path):
    callback = tmp_path / 'policy.py'
    callback.write_text("def decide(event, settings=None):\n return {'allow': event['content'].startswith('Useful')}\n")
    (tmp_path / 'config.yaml').write_text(json.dumps({'outbound_policy': {'required': True, 'callback_file': str(callback)}}))
    emitted = []
    async def network(method, url, *args, **kwargs):
        emitted.append(kwargs.get('json'))
        return {'id': 'real-fake-receipt'}
    session = SimpleNamespace(_request=network, closed=False)
    async def http_request(route, **kwargs):
        return await session._request(route.method, 'https://discord.com/api/v10/channels/person/messages', **kwargs)
    http = SimpleNamespace(request=http_request, _HTTPClient__session=session)
    client = SimpleNamespace(http=http)
    install_discord_transport(client, profile_home=tmp_path)
    async def scenario():
        for method in ('POST', 'PATCH'):
            with pytest.raises(OutboundHeld):
                await http.request(SimpleNamespace(method=method, channel_id='person'), json={'content': 'Builder output'})
        for hidden in ({'embeds': [{'fields': [{'name': 'Builder field', 'value': 'Builder report'}]}]},
                       {'embeds': [{'footer': {'text': 'Builder footer'}}]},
                       {'attachments': [{'filename': 'Builder attachment'}]}):
            with pytest.raises(OutboundHeld):
                await http.request(SimpleNamespace(method='POST', channel_id='person'), json=hidden)
        await http.request(SimpleNamespace(method='POST', channel_id='person'), json={'content': 'Useful decision'})
        with pytest.raises(OutboundHeld):
            await session._request('POST', 'https://discord.com/api/v10/webhooks/id/token', json={'content': 'Builder followup'})
        with pytest.raises(OutboundHeld):
            await session._request('POST', 'https://discord.com/api/v10/interactions/id/token/callback', json={'type': 4, 'data': {'content': 'Builder callback'}})
        await session._request('POST', 'https://discord.com/api/v10/interactions/id/token/callback', json={'type': 9, 'data': {'title': 'Ask', 'components': []}})
        from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter
        cloud = object.__new__(WhatsAppCloudAdapter)
        cloud._outbound_policy_home = tmp_path
        cloud._http_client = SimpleNamespace(post=network)
        messages, error = await cloud._post_messages({'to': 'person', 'type': 'text',
            'text': {'body': 'Builder WhatsApp'}}, fail_log='synthetic', reject_log='synthetic')
        assert messages == [] and error.startswith('outbound held:')
    asyncio.run(scenario())
    assert all('Builder' not in str(payload) for payload in emitted)


def test_whatsapp_reply_survives_native_handler_and_formatter_handoff(tmp_path, monkeypatch):
    """Real Base -> gateway wrapper -> Cloud.send with a sanitized long-turn envelope."""
    import httpx
    from unittest.mock import AsyncMock
    from gateway.config import PlatformConfig
    from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter
    from gateway.run_turn import GatewayTurnMixin
    from gateway.session import build_session_key
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    import gateway.outbound_policy as policy

    callback = tmp_path / 'reply_policy.py'
    callback.write_text("def decide(event, settings=None):\n p=event.get('provenance') or {}\n return {'allow': p.get('kind')=='authenticated_reply' and p.get('message_id')=='wamid.sanitized-native'}\n")
    (tmp_path / 'config.yaml').write_text(json.dumps({'outbound_policy': {
        'required': True, 'callback_file': str(callback)}}))
    clock = [100.0]
    monkeypatch.setattr(policy.time, 'monotonic', lambda: clock[0])
    response = '**Subject:** Draft for your review\n\nThis is the requested draft, with **confirmed facts**.'
    token = set_hermes_home_override(tmp_path)
    try:
        config = PlatformConfig(enabled=True, extra={'phone_number_id': '123', 'access_token': 'fake-token'})
        adapter = WhatsAppCloudAdapter(config)
        adapter._allow_from = {'27840000000'}
        adapter._dm_policy = 'allowlist'
        adapter._http_client = SimpleNamespace(post=AsyncMock(return_value=httpx.Response(
            200, json={'messages': [{'id': 'fake-provider-receipt'}]})))
        class Runner:
            def _is_session_run_current(self, *args):
                return True
            def _is_user_authorized_for_source(self, *args, **kwargs):
                return True
            async def _handle_message_with_agent_owned(self, event, source, key, generation):
                # Native skill/context rendering changes presentation, not the user envelope.
                event.text = '[trusted skill context]\n' + event.text
                clock[0] += 86.5
                await asyncio.sleep(0)
                return response
        runner = Runner()
        async def handler(event):
            return await GatewayTurnMixin._handle_message_with_agent(
                runner, event, event.source, build_session_key(event.source), 1)
        adapter.set_message_handler(handler)
        async def scenario():
            raw = {'id': 'wamid.sanitized-native', 'from': '27840000000', 'type': 'text',
                   'text': {'body': 'Please prepare a draft for my review.'}}
            event = await adapter._build_message_event_from_cloud(raw, {}, {'phone_number_id': '123'})
            assert event is not None
            await adapter.handle_message(event)
            tasks = list(adapter._session_tasks.values())
            if tasks:
                await asyncio.gather(*tasks)
            text_calls = [call.kwargs['json'] for call in adapter._http_client.post.call_args_list
                          if call.kwargs.get('json', {}).get('type') == 'text']
            assert len(text_calls) == 1
            assert text_calls[0]['text']['body'] == adapter.format_message(response)
            assert text_calls[0]['text']['body'] != response
            assert text_calls[0]['context']['message_id'] == 'wamid.sanitized-native'
            denied = await adapter.send('27840000000', response)
            assert not denied.success
            assert len([call for call in adapter._http_client.post.call_args_list
                        if call.kwargs.get('json', {}).get('type') == 'text']) == 1
        asyncio.run(scenario())
    finally:
        reset_hermes_home_override(token)
