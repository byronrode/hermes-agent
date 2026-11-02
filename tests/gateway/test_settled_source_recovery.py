"""Real owner-process exit, durable API reservation and original transcript.

External provider transport/model are synthetic. HTTP admission IDs, SQLite
source/status/result lifecycle and PID/start fingerprints are native owners.
"""
import asyncio
import copy
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

if __name__=='__main__':
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]))

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
import pytest
from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
from hermes_state import SessionDB


def setup(root, *, create=False):
    adapter=APIServerAdapter(PlatformConfig(enabled=True,extra={'key':'synthetic-api-key','accepted_source_provider':'settled-test'}))
    adapter._run_idempotency_store.close()
    adapter._run_idempotency_store=RunIdempotencyStore(str(root/'runs.db'))
    db=SessionDB(db_path=root/'sessions.db');adapter._session_db=db
    if create:
        db.create_session('original-session',source='api_server')
        db.append_message('original-session','user','qualified internal fixture')
    provider=SimpleNamespace(name='settled-test',qualify=lambda **kw:None if kw['message']=='unqualified fixture' else {'intent':'fixture'})
    async def unavailable(source):return {'state':'unavailable','accepted_source':source}
    provider.read_result=unavailable
    manager=PluginManager(scope_key=str(root));manager._discovered=True
    lease=PluginContext(PluginManifest(name='settled-test'),manager).register_api_source_provider(provider)
    app=web.Application()
    app.router.add_post('/v1/runs',adapter._handle_runs)
    app.router.add_get('/v1/runs/{run_id}/accepted-source',adapter._handle_run_source_receipt)
    app.router.add_post('/v1/runs/{run_id}/source-result',adapter._handle_run_source_result)
    return adapter,db,provider,manager,lease,app


async def seed(root,persist,mode=None):
    adapter,db,provider,manager,lease,app=setup(root,create=True)
    agent=MagicMock();agent._persist_disabled=False
    agent.session_id='original-session';agent.platform='api_server';agent.model='synthetic-model'
    agent.session_prompt_tokens=agent.session_completion_tokens=agent.session_total_tokens=0
    agent.run_conversation.return_value={'final_response':'Synthetic internal settled result'}
    if mode=='failed':agent.run_conversation.side_effect=RuntimeError('Synthetic failed model')
    headers={'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'original-source'}
    try:
        with patch.object(adapter,'_create_agent',return_value=agent),patch('hermes_cli.plugins.get_plugin_manager',return_value=manager):
            async with TestClient(TestServer(app)) as client:
                response=await client.post('/v1/runs',json={'session_id':'original-session','input':'unqualified fixture' if mode=='unqualified' else 'qualified internal fixture'},headers=headers)
                run=(await response.json())['run_id']
                task=adapter._active_run_tasks.get(run)
                if task:await task
                source_reply=await client.get(f'/v1/runs/{run}/accepted-source',headers=headers)
                source=await source_reply.json() if source_reply.status==200 else None
                from datetime import datetime,timezone
                now=datetime.now(timezone.utc).isoformat()
                delivery={'state':'answered','accepted_source':source,'identity':'settled:'+run,
                    'display_kind':'fixture_result','content':'Synthetic internal settled result',
                    'metadata':{'delegation_id':'settled:'+run,'source_run_id':run},
                    'authored_at':now,'validated_at':now}
                async def read(source):return copy.deepcopy(delivery)
                provider.read_result=read
                receipt=None
                if persist:receipt=await (await client.post(f'/v1/runs/{run}/source-result',json={},headers=headers)).json()
                (root/'original.json').write_text(json.dumps({'run_id':run,'source':source,'delivery':delivery,'receipt':receipt}))
                print('READY',flush=True)
                await asyncio.to_thread(sys.stdin.readline)
    finally:
        await asyncio.gather(*adapter._active_run_tasks.values(),return_exceptions=True)
        lease.dispose();adapter._run_idempotency_store.close();adapter._response_store.close();db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('persist',[False,True])
async def test_same_settled_source_after_real_owner_process_exit(tmp_path,persist):
    process=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),str(tmp_path),str(int(persist))],
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    adapter=db=lease=None
    try:
        ready=await asyncio.wait_for(asyncio.to_thread(process.stdout.readline),15)
        assert ready=='READY\n', process.stderr.read() if not ready else ready
        original=json.loads((tmp_path/'original.json').read_text());source=original['source'];run=source['run_id']
        assert source['owner_pid']==process.pid and source['owner_started']>0
        adapter,db,provider,manager,lease,app=setup(tmp_path)
        adapter._run_owner_revision='f'*40 # Different serving code must NOT replace original provenance.
        recoveries=[]
        async def recover(base):
            recoveries.append(base)
            return copy.deepcopy(source)
        async def read(actual):
            assert actual==source
            return copy.deepcopy(original['delivery'])
        provider.recover_source=recover;provider.read_result=read
        headers={'Authorization':'Bearer synthetic-api-key'}
        before=adapter._run_idempotency_store.accepted_source_for_run(source['auth_scope'],run)
        with patch.object(adapter,'_create_agent',side_effect=AssertionError('No duplicate execution')) as launch, \
             patch('hermes_cli.plugins.get_plugin_manager',return_value=manager):
            async with TestClient(TestServer(app)) as client:
                path=f'/v1/runs/{run}'
                # A second server cannot take custody while the original process is alive.
                assert (await client.get(path+'/accepted-source',headers=headers)).status==404
                assert not recoveries
                process.stdin.write('exit\n');process.stdin.flush()
                assert await asyncio.wait_for(asyncio.to_thread(process.wait),10)==0
                assert (await client.get(path+'/accepted-source',headers={'Authorization':'Bearer foreign'})).status==401
                restored=await client.get(path+'/accepted-source',headers=headers)
                assert restored.status==200
                assert await restored.json()==source
                assert adapter._run_owners[run]==source['auth_scope']
                from gateway.platforms.api_server_accepted_source import current_accepted_run, current_accepted_source
                assert current_accepted_run() is None and current_accepted_source() is None
                # Wrong protected-producer snapshot cannot substitute another source.
                if not persist:
                    async def foreign(base):return {**source,'session_id':'foreign'}
                    provider.recover_source=foreign
                    assert (await client.get(path+'/accepted-source',headers=headers)).status==404
                    provider.recover_source=recover
                receipt=await (await client.post(path+'/source-result',json={},headers=headers)).json()
                assert receipt['state']=='persisted' and receipt['run_id']==run
                assert receipt['session_id']=='original-session'
                if persist:assert receipt==original['receipt']
                count=len(recoveries)
                # Once pending validation is durable, use it rather than a new hint/reclassification.
                del provider.recover_source
                repeated=await (await client.post(path+'/source-result',json={},headers=headers)).json()
                assert repeated==receipt and len(recoveries)==count
                after=adapter._run_idempotency_store.accepted_source_for_run(source['auth_scope'],run)
                assert before==after # Original PID/start/source/fingerprint/status were NOT rewritten.
                assert (await (await client.get(path+'/accepted-source',headers=headers)).json())==source
                rows=[m for m in db.get_messages('original-session') if m.get('display_kind')=='fixture_result']
                assert len(rows)==1 and rows[0]['id']==receipt['message_id']
                assert rows[0]['content']==original['delivery']['content']
                launch.assert_not_called()
    finally:
        if process.poll() is None:
            process.stdin.write('exit\n');process.stdin.flush()
            await asyncio.wait_for(asyncio.to_thread(process.wait),10)
        for stream in (process.stdin,process.stdout,process.stderr):stream.close()
        if lease:lease.dispose()
        if adapter:adapter._run_idempotency_store.close();adapter._response_store.close()
        if db:db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['unqualified','failed'])
async def test_unqualified_or_failed_original_cannot_be_retroqualified_after_owner_exit(tmp_path,mode):
    process=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),str(tmp_path),'0',mode],
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    adapter=db=lease=None
    try:
        ready=await asyncio.wait_for(asyncio.to_thread(process.stdout.readline),15)
        assert ready=='READY\n', process.stderr.read() if not ready else ready
        original=json.loads((tmp_path/'original.json').read_text())
        assert original['source'] is None
        process.stdin.write('exit\n');process.stdin.flush()
        assert await asyncio.wait_for(asyncio.to_thread(process.wait),10)==0
        adapter,db,provider,manager,lease,app=setup(tmp_path)
        async def prohibited(base):raise AssertionError('No existing source: never ask producer to invent one')
        provider.recover_source=prohibited;provider.read_result=prohibited
        with patch.object(adapter,'_create_agent',side_effect=AssertionError('No duplicate execution')), \
             patch('hermes_cli.plugins.get_plugin_manager',return_value=manager):
            async with TestClient(TestServer(app)) as client:
                path=f"/v1/runs/{original['run_id']}"
                headers={'Authorization':'Bearer synthetic-api-key'}
                assert (await client.get(path+'/accepted-source',headers=headers)).status==404
                assert (await client.post(path+'/source-result',json={},headers=headers)).status==503
    finally:
        if process.poll() is None:
            process.stdin.write('exit\n');process.stdin.flush()
            await asyncio.wait_for(asyncio.to_thread(process.wait),10)
        for stream in (process.stdin,process.stdout,process.stderr):stream.close()
        if lease:lease.dispose()
        if adapter:adapter._run_idempotency_store.close();adapter._response_store.close()
        if db:db.close()


if __name__=='__main__':
    asyncio.run(seed(Path(sys.argv[1]),bool(int(sys.argv[2])),sys.argv[3] if len(sys.argv)>3 else None))
