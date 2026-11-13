"""Exact native-inbound recovery for final replies definitively held before provider IO.

Operator route: python -m gateway.retry_reply --obligation ID [--send].
The default validates without sending. Ambiguous/claimed/delivered rows never retry.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
import json
import logging
import time

from gateway import delivery_ledger as ledger


def validate_reply(conn, obligation_id, adapter):
    conn.row_factory = __import__('sqlite3').Row
    row = conn.execute('SELECT * FROM delivery_obligations WHERE obligation_id=?', (obligation_id,)).fetchone()
    if not row or row['state'] != 'failed' or row['source_binding'] or row['native_message_id']:
        raise ValueError('Only an unclaimed definitively failed native reply can be recovered')
    if row['platform'] not in {'whatsapp_cloud','discord'} or row['adapter_profile'] != 'default':
        raise ValueError('Recovery supports the owning default WhatsApp Cloud/Discord profile only')
    if 'outbound held:' not in (row['last_error'] or '').lower():
        raise ValueError('Failure is not a definitive local policy hold')
    if row['platform'] == 'whatsapp_cloud' and not adapter._is_dm_allowed(row['chat_id']):
        raise ValueError('Recipient is no longer authorized by the native adapter')
    matches = conn.execute("""SELECT m.*,s.source,s.user_id,s.chat_id,s.chat_type,s.session_key,s.origin_json
        FROM messages m JOIN sessions s ON s.id=m.session_id
        WHERE s.session_key=? AND m.role='assistant' AND m.content=?
        AND abs(m.timestamp-?)<60 AND m.active=1""",
        (row['session_key'], row['content'], row['created_at'])).fetchall()
    if len(matches) != 1:
        raise ValueError('Exact persisted final reply is missing or ambiguous')
    reply = matches[0]
    origin = json.loads(reply['origin_json'] or '{}')
    actor = row['chat_id'] if row['platform']=='whatsapp_cloud' else '446319827712475137'
    if (reply['source'] != row['platform']
            or str(reply['user_id']) != actor or str(reply['chat_id']) != row['chat_id']
            or str(origin.get('user_id')) != actor or str(origin.get('chat_id')) != row['chat_id']
            or origin.get('platform') != row['platform']
            or str(origin.get('thread_id') or '') != str(row['thread_id'] or '')
            or (row['platform']=='whatsapp_cloud' and reply['chat_type']!='dm')):
        raise ValueError('Persisted native source does not bind this recipient')
    inbound = conn.execute("""SELECT * FROM messages WHERE session_id=? AND role='user'
        AND id<? AND active=1 ORDER BY id DESC LIMIT 1""", (reply['session_id'],reply['id'])).fetchone()
    if not inbound or not inbound['platform_message_id'] or inbound['timestamp'] > reply['timestamp']:
        raise ValueError('Original native inbound is missing')
    if ledger.compute_obligation_id(row['session_key'],inbound['platform_message_id'],row['content']) != obligation_id:
        raise ValueError('Obligation digest does not bind the original native inbound and exact final output')
    verified=dict(row)
    verified['authenticated_actor']=actor
    return verified, dict(inbound)


@contextmanager
def _native_reply_scope(row, inbound):
    from gateway import outbound_policy as policy
    from hermes_constants import get_hermes_home
    proof = {'kind':'authenticated_reply','home_scope':str(get_hermes_home()),
             'platform':row['platform'],'user_id':row['authenticated_actor'],'recipient':str(row['thread_id'] or row['chat_id']),
             'message_id':inbound['platform_message_id'],'source_text':inbound['content']}
    digest = policy._text_digest(row['content'])
    token = policy._provenance.set(lambda: dict(proof, _formatted_output_sha256s=(digest,)))
    try:
        yield
    finally:
        policy._provenance.reset(token)


async def retry_reply(obligation_id, adapter, *, send=False):
    """Validate native persisted custody, reserve once, use normal provider+policy, retain receipt."""
    from gateway.outbound_policy import evaluate
    with ledger._DB_LOCK, ledger._transaction() as conn:
        row, inbound = validate_reply(conn,obligation_id,adapter)
        with _native_reply_scope(row,inbound):
            admission = evaluate(row['platform'],str(row['thread_id'] or row['chat_id']),row['content'],profile_home=adapter._outbound_policy_home)
        if admission.get('allow') is not True:
            return admission
        if not send:
            return {'success':True,'state':'validated','delivered':False,'obligation_id':obligation_id}
        pid, started = ledger._owner_stamp()
        changed = conn.execute("""UPDATE delivery_obligations SET state='unknown',
            source_binding=?,updated_at=?,owner_pid=?,owner_started_at=?,attempts=attempts+1
            WHERE obligation_id=? AND state='failed' AND source_binding IS NULL""",
            ('native-reply-recovery:'+inbound['platform_message_id'],time.time(),pid,started,obligation_id)).rowcount
        if changed != 1:
            raise ValueError('Reply was claimed concurrently')
    # Once reserved, every uncertain outcome remains unknown. Never automatically duplicate it.
    with _native_reply_scope(row,inbound):
        result = await adapter.send(row['chat_id'],row['content'],
                                    reply_to=inbound['platform_message_id'],metadata={'thread_id':row['thread_id']} if row['thread_id'] else None)
    if not result.success or not result.message_id:
        return {'success':False,'state':'unknown','delivered':False,'error':result.error or 'Missing native receipt',
                'obligation_id':obligation_id}
    if not ledger.acknowledge_source_send(obligation_id,result.message_id):
        return {'success':False,'state':'unknown','delivered':True,'message_id':result.message_id,
                'error':'Provider accepted reply but durable acknowledgment failed','obligation_id':obligation_id}
    logging.getLogger('gateway.run').info('Recovered authenticated reply obligation=%s native_receipt=%s',
                                          obligation_id,result.message_id)
    return {'success':True,'state':'delivered','delivered':True,'message_id':result.message_id,'obligation_id':obligation_id}


class DiscordRecoveryAdapter:
    """Reuse the registered Discord standalone sender; no second gateway or bot login."""
    def __init__(self, config):
        from hermes_constants import get_hermes_home
        self.config=config
        self._outbound_policy_home=get_hermes_home()

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        from gateway.platforms.base import SendResult
        from plugins.platforms.discord.adapter import _standalone_send
        # A verified existing channel/thread is already known, so do not create a forum post.
        result=await _standalone_send(self.config,chat_id,content,
                                      thread_id=str((metadata or {}).get('thread_id') or chat_id))
        return SendResult(success=result.get('success') is True,message_id=result.get('message_id'),
                          error=result.get('error'))


def main():
    logger = logging.getLogger('gateway.run')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--obligation',required=True)
    parser.add_argument('--send',action='store_true',help='Reserve and deliver this verified exact reply once')
    args = parser.parse_args()
    async def run():
        import httpx
        from gateway.config import load_gateway_config, Platform
        from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter
        from agent.secret_scope import build_profile_secret_scope, set_secret_scope
        from hermes_constants import get_hermes_home
        from hermes_cli.send_cmd import _load_hermes_env
        set_secret_scope(build_profile_secret_scope(get_hermes_home()),profile_home=str(get_hermes_home()))
        _load_hermes_env()
        with ledger._transaction() as conn:
            native = conn.execute('SELECT platform FROM delivery_obligations WHERE obligation_id=?',(args.obligation,)).fetchone()
        if not native:
            raise ValueError('Unknown obligation')
        platform=Platform(native[0])
        config = load_gateway_config().platforms[platform]
        if platform==Platform.DISCORD:
            return await retry_reply(args.obligation,DiscordRecoveryAdapter(config),send=args.send)
        adapter = WhatsAppCloudAdapter(config)
        if not adapter._phone_number_id or not adapter._access_token:
            raise ValueError('Native WhatsApp Cloud credentials unavailable')
        async with httpx.AsyncClient(timeout=30) as client:
            adapter._http_client=client
            return await retry_reply(args.obligation,adapter,send=args.send)
    try:
        print(json.dumps(asyncio.run(run())))
    except Exception as exc:
        print(json.dumps({'success':False,'delivered':False,'state':'held','error':str(exc)}))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
