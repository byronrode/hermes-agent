"""Native persisted exact-reply recovery must never become a caller-labelled grant."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest


def test_native_held_reply_recovery_exact_once(tmp_path, monkeypatch):
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
    key='agent:main:whatsapp_cloud:dm:27840000000'
    oid=ledger.compute_obligation_id(key,'wamid.native',content)
    try:
        with ledger._transaction() as conn:
            conn.execute('CREATE TABLE sessions(id TEXT,source TEXT,user_id TEXT,chat_id TEXT,chat_type TEXT,session_key TEXT,origin_json TEXT)')
            conn.execute('CREATE TABLE messages(id INTEGER,session_id TEXT,role TEXT,content TEXT,timestamp REAL,active INTEGER,platform_message_id TEXT)')
            origin=json.dumps({'platform':'whatsapp_cloud','user_id':'27840000000','chat_id':'27840000000'})
            conn.execute('INSERT INTO sessions VALUES(?,?,?,?,?,?,?)',('s','whatsapp_cloud','27840000000','27840000000','dm',key,origin))
            conn.execute('INSERT INTO messages VALUES(?,?,?,?,?,?,?)',(1,'s','user','Please prepare a draft.',90,1,'wamid.native'))
            conn.execute('INSERT INTO messages VALUES(?,?,?,?,?,?,?)',(2,'s','assistant',content,100,1,None))
        ledger.record_obligation(obligation_id=oid,session_key=key,platform='whatsapp_cloud',chat_id='27840000000',thread_id=None,content=content)
        ledger.mark_failed(oid,'outbound held: WhatsApp only authenticated reply')
        with ledger._transaction() as conn:
            conn.execute('UPDATE delivery_obligations SET created_at=100,attempts=3 WHERE obligation_id=?',(oid,))
        adapter=WhatsAppCloudAdapter(PlatformConfig(enabled=True,extra={'phone_number_id':'123','access_token':'fake'}))
        adapter._allow_from={'27840000000'}
        adapter._dm_policy='allowlist'
        adapter._http_client=SimpleNamespace(post=AsyncMock(return_value=httpx.Response(200,json={'messages':[{'id':'provider-receipt'}]})))
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
