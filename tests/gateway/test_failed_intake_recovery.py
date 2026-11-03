"""Real HTTP, Discord SDK and SQLite custody; synthetic provider/network."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
import json
import multiprocessing
import os
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
import pytest

from tests.gateway.test_authorized_messaging_source import rig, execute
from gateway.accepted_source import current_gateway_source
from gateway.source_intake_recovery import current_intake_recovery
from gateway.platforms.api_server import APIServerAdapter
from gateway.config import PlatformConfig


def interrupted_owner(path, pipe):
    """Fault point: actual process owns the SQLite claim and lease, then exits."""
    from hermes_state import SessionDB
    from gateway.source_intake_recovery import failed_intake, _cas
    from gateway.status import get_process_start_time
    db=SessionDB(db_path=path)
    evidence,prior=failed_intake(db,'444','synthetic_intake')
    assert db.try_acquire_session_turn_lease('source-session',f'pid={os.getpid()}:recovery-test')
    _cas(db,evidence,prior,dict(intent_hash=evidence['intent_hash'],state='running',
        pid=os.getpid(),started=get_process_start_time(os.getpid()),attempt=1))
    pipe.send('claimed')
    pipe.recv()
    # Deliberately leave the durable claim/lease as a killed worker would.
    db.close()


async def fixture(tmp_path, monkeypatch):
    r = rig(tmp_path,monkeypatch)
    sources=[]
    await execute(r,monkeypatch,lambda:sources.append(current_gateway_source()))
    source=sources[0]
    args={'items':[{'number':1,'title':'Synthetic support request','brief':'Read only','domain':'ignis'}]}
    calls=[{'id':'original-call','function':{'name':'tool_call',
        'arguments':json.dumps({'calls':[{'name':'synthetic_intake','arguments':args}]})}}]
    r.db.append_message('source-session','assistant','',tool_calls=calls)
    result_row=r.db.append_message('source-session','tool',json.dumps(dict(blocked_phase='canonical_capture',
        execution_verified=False,delivery_verified=False)),tool_name='synthetic_intake',tool_call_id='original-call')
    attempts=[]
    def validate():
        assert current_gateway_source() is None
        cap=current_intake_recovery()
        assert cap['source']==source and cap['arguments']==args
    def retry():
        validate()
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(copy_context().run,current_intake_recovery).result() is None
        attempts.append(current_intake_recovery()['intent_hash'])
        return {'items':[{'issue_id':'synthetic-existing-native-issue','state':'executing'}]}
    r.provider.gateway_intake_tool='synthetic_intake'
    r.provider.validate_gateway_intake_recovery=validate
    r.provider.retry_gateway_intake=retry
    api=APIServerAdapter(PlatformConfig(enabled=True,extra={'key':'synthetic-operator-key'}))
    api.gateway_runner=r.runner
    app=web.Application()
    app.router.add_post('/v1/gateway-sources/{message_id}/retry-intake',api._handle_gateway_intake_retry)
    client=TestClient(TestServer(app));await client.start_server()
    return SimpleNamespace(r=r,source=source,args=args,result_row=result_row,attempts=attempts,client=client,api=api)


async def close(f):
    await f.client.close()
    await f.r.adapter._client.close()
    f.api._run_idempotency_store.close();f.api._response_store.close();f.r.db.close()


@pytest.mark.asyncio
async def test_authenticated_failed_intake_exact_replay_restart_and_current_permission(tmp_path,monkeypatch):
    f=await fixture(tmp_path,monkeypatch)
    try:
        url='/v1/gateway-sources/444/retry-intake'
        headers={'Authorization':'Bearer synthetic-operator-key'}
        assert (await f.client.post(url,json={})).status==401
        assert (await f.client.post(url,headers=headers,json={'source':f.source})).status==409
        assert (await f.client.post('/v1/gateway-sources/foreign/retry-intake',headers=headers,json={})).status==409
        responses=await asyncio.gather(*(f.client.post(url,headers=headers,json={}) for _ in range(2)))
        assert all(r.status in (200,409) for r in responses)
        r=next(r for r in responses if r.status==200)
        first=await r.json()
        assert first['source_run_id']==f.source['run_id'] and first['turn_id']==f.source['turn_id']
        # Lost HTTP acknowledgement and a replacement API owner use the same
        # durable completed receipt, never call the provider again.
        from hermes_state import SessionDB
        f.r.runner._session_db=SessionDB(db_path=tmp_path/'state.db')
        try:
            repeated=await f.client.post(url,headers=headers,json={})
            assert await repeated.json()==first
            assert len(f.attempts)==1
            monkeypatch.setenv('DISCORD_ALLOWED_USERS','999')
            assert (await f.client.post(url,headers=headers,json={})).status==409
            monkeypatch.setenv('DISCORD_ALLOWED_USERS','123')
            # A->B->A cannot borrow source capability from another profile.
            monkeypatch.setenv('HERMES_HOME',str(tmp_path/'foreign'))
            assert (await f.client.post(url,headers=headers,json={})).status==409
            monkeypatch.setenv('HERMES_HOME',str(tmp_path))
            assert (await f.client.post(url,headers=headers,json={})).status==200
            f.r.db._execute_write(lambda conn:conn.execute('UPDATE messages SET tool_calls=? WHERE role=?',
                (json.dumps([{'id':'original-call','function':{'name':'synthetic_intake',
                    'arguments':json.dumps(dict(f.args,dispatch=False))}}]),'assistant')))
            assert (await f.client.post(url,headers=headers,json={})).status==409
            assert len(f.attempts)==1
        finally:
            f.r.runner._session_db.close()
    finally:
        await close(f)


@pytest.mark.asyncio
async def test_live_owner_unqualified_failure_and_partial_retry_are_not_duplicate_admission(tmp_path,monkeypatch):
    f=await fixture(tmp_path,monkeypatch)
    try:
        url='/v1/gateway-sources/444/retry-intake';headers={'Authorization':'Bearer synthetic-operator-key'}
        assert f.r.db.try_acquire_session_turn_lease('source-session','live-native-turn')
        assert (await f.client.post(url,headers=headers,json={})).status==409
        f.r.db.release_session_turn_lease('source-session','live-native-turn')
        context=multiprocessing.get_context('spawn')
        parent,child=context.Pipe()
        process=context.Process(target=interrupted_owner,args=(tmp_path/'state.db',child))
        process.start()
        try:
            assert await asyncio.to_thread(parent.poll,20)
            assert parent.recv()=='claimed'
            assert (await f.client.post(url,headers=headers,json={})).status==409
            # Expiry/release of the conversation lease alone cannot steal
            # an in-flight recovery from a still-live process incarnation.
            f.r.db.release_session_turn_lease('source-session',f'pid={process.pid}:recovery-test')
            assert (await f.client.post(url,headers=headers,json={})).status==409
        finally:
            parent.send('exit')
            await asyncio.to_thread(process.join,20)
            if process.is_alive():process.terminate();process.join()
            parent.close();child.close()
        assert process.exitcode==0
        old=f.r.provider.retry_gateway_intake
        def partial():
            old()
            raise RuntimeError('Synthetic response lost after idempotent native capture')
        f.r.provider.retry_gateway_intake=partial
        assert (await f.client.post(url,headers=headers,json={})).status==409
        f.r.provider.retry_gateway_intake=old
        assert (await f.client.post(url,headers=headers,json={})).status==200
        assert f.attempts[0]==f.attempts[1]
        f.r.db._execute_write(lambda conn:conn.execute('UPDATE messages SET content=? WHERE id=?',
            (json.dumps({'blocked_phase':'source_authority','execution_verified':False,'delivery_verified':False}),f.result_row)))
        assert (await f.client.post(url,headers=headers,json={})).status==409
        assert current_intake_recovery() is None and current_gateway_source() is None
    finally:
        await close(f)
