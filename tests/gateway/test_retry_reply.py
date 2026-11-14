"""Native persisted exact-reply recovery must never become a caller-labelled grant."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest


@pytest.mark.parametrize("platform",["whatsapp_cloud","discord"])
def test_native_held_reply_recovery_exact_once(tmp_path, monkeypatch, platform):
    from gateway import delivery_ledger as ledger
    from gateway.config import PlatformConfig
    from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter
    from gateway.retry_reply import retry_reply
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    monkeypatch.setattr(ledger,'_db_path',lambda:tmp_path/'state.db')
    callback=tmp_path/'policy.py'
    callback.write_text("def decide(event, settings=None):\n p=event.get('provenance') or {}\n return {'allow':p.get('kind')=='authenticated_reply' and p.get('message_id')=='wamid.native'}\n")
    (tmp_path/'config.yaml').write_text(json.dumps({'outbound_policy':{'required':True,'callback_file':str(callback)}}))
    home=set_hermes_home_override(tmp_path)
    content='**Subject:** Here is the draft you requested.'
    actor='27840000000' if platform=='whatsapp_cloud' else '446319827712475137'
    chat='27840000000' if platform=='whatsapp_cloud' else '14800000000'
    chat_type='dm' if platform=='whatsapp_cloud' else 'group'
    key=f'agent:main:{platform}:{chat_type}:{chat}'
    oid=ledger.compute_obligation_id(key,'wamid.native',content)
    try:
        with ledger._transaction() as conn:
            conn.execute('CREATE TABLE sessions(id TEXT,source TEXT,user_id TEXT,chat_id TEXT,chat_type TEXT,session_key TEXT,origin_json TEXT)')
            conn.execute('CREATE TABLE messages(id INTEGER,session_id TEXT,role TEXT,content TEXT,timestamp REAL,active INTEGER,platform_message_id TEXT)')
            origin=json.dumps({'platform':platform,'user_id':actor,'chat_id':chat})
            conn.execute('INSERT INTO sessions VALUES(?,?,?,?,?,?,?)',('s',platform,actor,chat,chat_type,key,origin))
            conn.execute('INSERT INTO messages VALUES(?,?,?,?,?,?,?)',(1,'s','user','Please prepare a draft.',90,1,'wamid.native'))
            conn.execute('INSERT INTO messages VALUES(?,?,?,?,?,?,?)',(2,'s','assistant',content,100,1,None))
        ledger.record_obligation(obligation_id=oid,session_key=key,platform=platform,chat_id=chat,thread_id=None,content=content)
        ledger.mark_failed(oid,'outbound held: WhatsApp only authenticated reply')
        with ledger._transaction() as conn:
            conn.execute('UPDATE delivery_obligations SET created_at=108,attempts=3 WHERE obligation_id=?',(oid,))
        adapter=WhatsAppCloudAdapter(PlatformConfig(enabled=True,extra={'phone_number_id':'123','access_token':'fake'}))
        adapter._allow_from={'27840000000'}
        adapter._dm_policy='allowlist'
        adapter._http_client=SimpleNamespace(post=AsyncMock(return_value=httpx.Response(200,json={'messages':[{'id':'provider-receipt'}]})))
        if platform=='discord':
            from gateway.retry_reply import DiscordRecoveryAdapter
            from plugins.platforms.discord import adapter as discord_module
            from gateway.outbound_policy import evaluate
            from gateway.config import PlatformConfig
            async def native_send(config,chat_id,content,**kwargs):
                # Stub only provider IO; the existing standalone policy admission still runs.
                assert evaluate('discord',kwargs['thread_id'],content)['allow']
                return {'success':True,'message_id':'provider-receipt'}
            fake=AsyncMock(side_effect=native_send)
            monkeypatch.setattr(discord_module,'_standalone_send',fake)
            adapter=DiscordRecoveryAdapter(PlatformConfig(enabled=True,token='fake'))
            adapter._http_client=SimpleNamespace(post=fake)
        async def scenario():
            check=await retry_reply(oid,adapter)
            assert check['state']=='validated' and not check['delivered']
            assert adapter._http_client.post.call_count==0
            with ledger._transaction() as conn:
                conn.execute("UPDATE messages SET platform_message_id='wamid.wrong' WHERE id=1")
            with pytest.raises(ValueError,match='digest'):
                await retry_reply(oid,adapter,send=True)
            assert adapter._http_client.post.call_count==0
            with ledger._transaction() as conn:
                conn.execute("UPDATE messages SET platform_message_id='wamid.native' WHERE id=1")
            delivered=await retry_reply(oid,adapter,send=True)
            assert delivered['state']=='delivered' and delivered['message_id']=='provider-receipt'
            assert adapter._http_client.post.call_count==1
            with pytest.raises(ValueError,match='unclaimed'):
                await retry_reply(oid,adapter,send=True)
            assert adapter._http_client.post.call_count==1
        asyncio.run(scenario())
        with ledger._transaction() as conn:
            row=conn.execute('SELECT state,native_message_id,attempts FROM delivery_obligations WHERE obligation_id=?',(oid,)).fetchone()
            assert row==('delivered','provider-receipt',4)
    finally:
        reset_hermes_home_override(home)


@pytest.mark.parametrize('platform',['whatsapp_cloud','discord'])
def test_operator_followup_native_custody_and_uncertain_dedup(tmp_path,monkeypatch,platform):
    from gateway import delivery_ledger as ledger
    from gateway.outbound_policy import current_provenance
    from gateway.platforms.base import SendResult
    from gateway.retry_reply import follow_up
    from hermes_constants import set_hermes_home_override,reset_hermes_home_override
    monkeypatch.setattr(ledger,'_db_path',lambda:tmp_path/'state.db')
    callback=tmp_path/'followup_policy.py'
    callback.write_text("def decide(event, settings=None):\n p=event.get('provenance') or {}\n return {'allow': event['platform']=='discord' and not p or p.get('message_id')=='wamid.native'}\n")
    (tmp_path/'config.yaml').write_text(json.dumps({'outbound_policy':{'required':True,'callback_file':str(callback)}}))
    home=set_hermes_home_override(tmp_path)
    sent=[]
    class Native:
        _outbound_policy_home=tmp_path
        def _is_dm_allowed(self,actor):
            return actor=='27840000000'
        async def send(self,chat,content,reply_to=None):
            proof=current_provenance(content)
            assert (proof is not None)==(platform=='whatsapp_cloud')
            if proof:
                assert current_provenance(content+'extra') is None
                assert proof['message_id']=='wamid.native'
            assert reply_to==('wamid.native' if platform=='whatsapp_cloud' else '123456789')
            sent.append(content)
            return SendResult(success=content!='uncertain',message_id='native-receipt' if content!='uncertain' else None,error='network uncertain' if content=='uncertain' else None)
    try:
        with ledger._transaction() as conn:
            conn.execute('CREATE TABLE sessions(id TEXT,source TEXT,user_id TEXT,chat_id TEXT,chat_type TEXT,session_key TEXT,origin_json TEXT)')
            conn.execute('CREATE TABLE messages(id INTEGER,session_id TEXT,role TEXT,content TEXT,timestamp REAL,active INTEGER,platform_message_id TEXT)')
            conn.execute('INSERT INTO sessions VALUES(?,?,?,?,?,?,?)',('s','whatsapp_cloud','27840000000','27840000000','dm','native-session',json.dumps({'platform':'whatsapp_cloud','user_id':'27840000000','chat_id':'27840000000'})))
            conn.execute('INSERT INTO messages VALUES(?,?,?,?,?,?,?)',(1,'s','user','Please confirm here.',90,1,'wamid.native'))
            conn.execute('ALTER TABLE messages ADD COLUMN compacted INTEGER DEFAULT 0')
        kwargs={'source_row':1} if platform=='whatsapp_cloud' else {'discord_channel':'987654321','reply_to':'123456789'}
        async def scenario():
            assert (await follow_up(Native(),'requested answer',**kwargs))['state']=='validated'
            assert not sent
            if platform=='whatsapp_cloud':
                with ledger._transaction() as conn:
                    conn.execute('UPDATE messages SET active=0')
                with pytest.raises(ValueError,match='unavailable'):
                    await follow_up(Native(),'requested answer',send=True,**kwargs)
                with ledger._transaction() as conn:
                    conn.execute('UPDATE messages SET compacted=1')
                assert (await follow_up(Native(),'requested answer',**kwargs))['state']=='validated'
            result=await follow_up(Native(),'requested answer',send=True,**kwargs)
            assert result['state']=='delivered'
            assert (await follow_up(Native(),'requested answer',send=True,**kwargs))['state']=='already_delivered'
            assert sent==['requested answer']
            assert (await follow_up(Native(),'uncertain',send=True,**kwargs))['state']=='unknown'
            with pytest.raises(ValueError,match='already claimed'):
                await follow_up(Native(),'uncertain',send=True,**kwargs)
            assert sent==['requested answer','uncertain']
        asyncio.run(scenario())
    finally:
        reset_hermes_home_override(home)
