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
