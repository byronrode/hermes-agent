"""Generic HTTP admission, external owner and durable detached-delivery contracts."""
import asyncio
import copy
import threading
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from aiohttp.test_utils import make_mocked_request
import pytest
import pytest_asyncio

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
from hermes_state import SessionDB


@pytest_asyncio.fixture
async def owner(tmp_path):
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={'key':'synthetic-api-key',
        'accepted_source_provider':'external-source'}))
    adapter._run_idempotency_store.close()
    adapter._run_idempotency_store = RunIdempotencyStore(str(tmp_path / 'runs.db'))
    db = SessionDB(db_path=tmp_path / 'state.db')
    db.create_session('original-session', source='api_server')
    db.append_message('original-session', 'user', 'source request')
    adapter._session_db = db
    provider = SimpleNamespace(name='external-source', result=None)
    def qualify(*, message, profile):
        if message == 'invalid-classification':
            return {'owner_pid':'copied-model-claim'}
        if message == 'broken-classification':
            raise RuntimeError('optional external classifier unavailable')
        if profile == 'default' and message in {'first status','second status'}:
            return {'intent':'synthetic_status','subject':message.split()[0]}
    async def read_result(source):
        return copy.deepcopy(provider.result)
    provider.qualify, provider.read_result = qualify, read_result
    manager = PluginManager(scope_key=str(tmp_path))
    manager._discovered = True
    registration = PluginContext(PluginManifest(name='external-integration'), manager).register_api_source_provider(provider)
    agent = MagicMock()
    agent._persist_disabled = False
    agent.session_id, agent.platform, agent.model = 'original-session','api_server','synthetic-model'
    agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0
    finished = asyncio.Event()
    loop = asyncio.get_running_loop()
    def conversation(**kwargs):
        loop.call_soon_threadsafe(finished.set)
        return {'final_response':'synthetic completion'}
    agent.run_conversation.side_effect = conversation
    app = web.Application()
    app.router.add_post('/v1/runs',adapter._handle_runs)
    app.router.add_get('/v1/runs/{run_id}/accepted-source',adapter._handle_run_source_receipt)
    app.router.add_post('/v1/runs/{run_id}/source-result',adapter._handle_run_source_result)
    with patch.object(adapter,'_create_agent',return_value=agent), patch('hermes_cli.plugins.get_plugin_manager',return_value=manager):
        yield adapter,app,db,finished,provider,registration
        await asyncio.gather(*adapter._active_run_tasks.values(),return_exceptions=True)
        await asyncio.gather(*(entry['task'] for entry in getattr(adapter,'_source_result_transactions',{}).values()),return_exceptions=True)
    registration.dispose()
    adapter._run_idempotency_store.close();adapter._response_store.close();db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('message,forwarded', [('first status',False),('second status',False),
    ('ambiguous status',False),('invalid-classification',False),('broken-classification',False),
    ([{'role':'user','content':'first status'}],False),('first status',True)])
async def test_http_admission_is_current_scoped_source_with_external_classification(owner,message,forwarded,monkeypatch):
    adapter,app,db,finished,provider,registration = owner
    headers = {'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'request'}
    body = {'session_id':'original-session','input':message}
    if forwarded:
        monkeypatch.setattr(adapter,'_admit_to_live_bot_chat',AsyncMock(return_value=('synthetic',{'delivery_id':'actual-owner'})))
        monkeypatch.setattr('gateway.platforms.api_server_runs._execute_run_via_live_owner',AsyncMock())
    async with TestClient(TestServer(app)) as cli:
        assert (await cli.post('/v1/runs',json=body)).status == 401
        accepted = await cli.post('/v1/runs',json=body,headers=headers)
        assert accepted.status == 202
        run = (await accepted.json())['run_id']
        if not forwarded: await asyncio.wait_for(finished.wait(),5)
        assert (await (await cli.post('/v1/runs',json=body,headers=headers)).json())['run_id'] == run
        path = f'/v1/runs/{run}/accepted-source'
        assert (await cli.get(path)).status == 401
        response = await cli.get(path,headers=headers)
        if forwarded or message not in ('first status','second status'):
            assert response.status == 404
            return
        assert response.status == 200
        source = await response.json()
        assert source['provider'] == 'external-source' and source['subject'] == message.split()[0]
        assert source['run_id'] == run and source['session_id'] == 'original-session'
        assert len(source['body_fingerprint']) == 64
        status = adapter._run_idempotency_store.status_for_run(source['auth_scope'],run)['status']
        for changed in ({'session_id':'foreign'}, {'status':'cancelled'}, {'delivery_id':'forwarded'}):
            adapter._run_idempotency_store.update_status(run,{**status,**changed})
            assert (await cli.get(path,headers=headers)).status == 404
        adapter._run_idempotency_store.update_status(run,status)
        assert (await cli.get(path,headers=headers)).status == 200
        registration.dispose()
        assert (await cli.get(path,headers=headers)).status == 404


@pytest.mark.asyncio
async def test_first_validated_result_survives_busy_ageout_exact_retry_and_compression(owner):
    adapter,app,db,finished,provider,registration = owner
    provider.bind_gateway_receipt=AsyncMock(side_effect=AssertionError('Legacy result must not invoke work binding'))
    headers = {'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'request'}
    async with TestClient(TestServer(app)) as cli:
        admitted = await cli.post('/v1/runs',json={'session_id':'original-session','input':'first status'},headers=headers)
        assert admitted.status == 202
        run = (await admitted.json())['run_id']
        await asyncio.wait_for(finished.wait(),5)
        source = await (await cli.get(f'/v1/runs/{run}/accepted-source',headers=headers)).json()
        path = f'/v1/runs/{run}/source-result'
        assert (await cli.post(path,json={})).status == 401
        assert (await cli.post(path,json={'answer':'forged','session_id':'foreign'},headers=headers)).status == 503
        provider.result = {'state':'queued','accepted_source':source}
        assert (await (await cli.post(path,json={},headers=headers)).json())['state'] == 'queued'
        authored = datetime.now(timezone.utc)
        result = {'state':'answered','accepted_source':source,'identity':'external:immutable-result',
            'content':'Attributed external answer','display_kind':'external_result',
            'metadata':{'delegation_id':'external:immutable-result','source_run_id':run,'source_reference':'immutable:revision'},
            'authored_at':authored.isoformat(),'validated_at':authored.isoformat()}
        # An old first acceptance is refused, even through a registered producer.
        provider.result = {**result,'validated_at':(authored+timedelta(minutes=6)).isoformat()}
        class Later(datetime):
            @classmethod
            def now(cls,tz=None): return authored+timedelta(minutes=7)
        with patch('gateway.platforms.api_server_source_receipts.datetime',Later):
            assert (await cli.post(path,json={},headers=headers)).status == 503
        assert adapter._run_idempotency_store.source_result_for_run(source['auth_scope'],run) is None
        provider.result = result
        assert db.acquire_session_turn_lease('original-session','busy-client',ttl_seconds=3600,wait_seconds=0)
        try:
            assert (await cli.post(path,json={},headers=headers)).status == 503
            assert adapter._run_idempotency_store.source_result_for_run(source['auth_scope'],run) == result
            with patch('gateway.platforms.api_server_source_receipts.datetime',Later):
                assert (await cli.post(path,json={},headers=headers)).status == 503
        finally:
            db.release_session_turn_lease('original-session','busy-client')
        with patch('gateway.platforms.api_server_source_receipts.datetime',Later):
            first = await cli.post(path,json={},headers=headers)
            assert first.status == 200, await first.text()
            persisted = await first.json()
            retry = await cli.post(path,json={},headers=headers)
            assert (await retry.json())['message_id'] == persisted['message_id']
            provider.result = {**result,'content':'substituted under same identity'}
            assert (await cli.post(path,json={},headers=headers)).status == 503
            provider.result = result
            db.end_session('original-session','compression')
            db.create_session('continued-session',source='api_server',parent_session_id='original-session')
            replay = await cli.post(path,json={},headers=headers)
            assert replay.status == 200, await replay.text()
            assert (await replay.json())['message_id'] == persisted['message_id']
            assert db.get_messages('continued-session') == []
            adapter._run_owners[run] = 'revoked-principal'
            assert (await cli.post(path,json={},headers=headers)).status == 503
        rows = [m for m in db.get_messages('original-session') if m.get('display_kind')=='external_result']
        assert len(rows)==1 and rows[0]['role']=='assistant'
        assert rows[0]['content']=='Attributed external answer'
        provider.bind_gateway_receipt.assert_not_called()

@pytest.mark.asyncio
@pytest.mark.parametrize('message', ['first status','ambiguous status'])
async def test_current_qualified_source_uses_real_http_owner_and_revokes_copies(owner,message):
    from contextvars import copy_context
    from gateway.platforms.api_server_accepted_source import current_accepted_source
    adapter,app,db,finished,provider,registration=owner
    seen=[]
    agent=adapter._create_agent.return_value
    original=agent.run_conversation.side_effect
    def conversation(**kwargs):
        seen.append((current_accepted_source(session_id='original-session'),copy_context()))
        assert current_accepted_source(session_id='foreign') is None
        return original(**kwargs)
    agent.run_conversation.side_effect=conversation
    async with TestClient(TestServer(app)) as client:
        response=await client.post('/v1/runs',json={'session_id':'original-session','input':message},
            headers={'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'qualified-source'})
        run=(await response.json())['run_id']
        await asyncio.wait_for(finished.wait(),5)
        task=adapter._active_run_tasks.get(run)
        if task:await task
        source,context=seen[0]
        if message=='first status':
            assert source['run_id']==run and source['provider']=='external-source'
            assert source['owner_pid']==adapter._run_owner_pid
        else:
            assert source is None  # Authenticated admission alone is not qualification.
        assert current_accepted_source() is None
        assert context.run(current_accepted_source) is None

@pytest.mark.asyncio
async def test_registered_source_budget_covers_slow_owner_without_changing_default(owner):
    adapter,app,db,finished,provider,registration=owner
    provider.read_timeout_seconds=20
    async def slow(source):
        await asyncio.sleep(3.6)
        return {'state':'queued','accepted_source':source}
    provider.read_result=slow
    headers={'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'slow-source'}
    async with TestClient(TestServer(app)) as client:
        run=(await (await client.post('/v1/runs',json={'session_id':'original-session','input':'first status'},headers=headers)).json())['run_id']
        await asyncio.wait_for(finished.wait(),5)
        path=f'/v1/runs/{run}/source-result'
        assert (await (await client.post(path,json={},headers=headers)).json())['state']=='queued'
        del provider.read_timeout_seconds
        assert (await (await client.post(path,json={},headers=headers)).json())['state']=='unavailable'

@pytest.mark.asyncio
@pytest.mark.parametrize('budget',[0,31,float('nan'),True,'20'])
async def test_invalid_source_read_budget_fails_before_producer(owner,budget):
    adapter,app,db,finished,provider,registration=owner
    provider.read_timeout_seconds=budget
    provider.read_result=AsyncMock(side_effect=AssertionError('invalid budget must not invoke producer'))
    headers={'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'invalid-budget'}
    async with TestClient(TestServer(app)) as client:
        run=(await (await client.post('/v1/runs',json={'session_id':'original-session','input':'first status'},headers=headers)).json())['run_id']
        await asyncio.wait_for(finished.wait(),5)
        assert (await (await client.post(f'/v1/runs/{run}/source-result',json={},headers=headers)).json())['state']=='unavailable'
        provider.read_result.assert_not_called()

@pytest.mark.asyncio
@pytest.mark.parametrize('error,reason',[(ValueError('private prompt and credential bytes'),'source_read_failed'),
                                       (TimeoutError('private provider payload'),'source_read_timeout')])
async def test_authorized_result_diagnostic_is_phase_bounded_and_never_echoes_provider_text(owner,error,reason):
    adapter,app,db,finished,provider,registration=owner
    provider.read_result=AsyncMock(side_effect=error)
    headers={'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'safe-diagnostic'}
    async with TestClient(TestServer(app)) as client:
        run=(await (await client.post('/v1/runs',json={'session_id':'original-session','input':'first status'},headers=headers)).json())['run_id']
        await asyncio.wait_for(finished.wait(),5)
        response=await client.post(f'/v1/runs/{run}/source-result',json={},headers=headers)
        assert response.status==503
        assert await response.json()=={'state':'unavailable','diagnostic':{'phase':'producer_read','reason':reason,'error_type':type(error).__name__}}
        unauthorized=await client.get(f'/v1/runs/{run}/accepted-source')
        assert unauthorized.status==401
        assert 'diagnostic' not in await unauthorized.json()


@pytest.mark.asyncio
@pytest.mark.parametrize('result', [None, [], 'private malformed provider output'])
@pytest.mark.parametrize('protocol', [None, 'per_intent_v1'])
async def test_non_dictionary_provider_result_keeps_bounded_json_failure(owner,result,protocol):
    adapter,app,db,finished,provider,registration=owner
    provider.source_result_protocol=protocol
    provider.qualify=lambda **kwargs: {'intent':'mara_request','subject':'status'}
    provider.result=result
    headers={'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'malformed-result'}
    async with TestClient(TestServer(app)) as client:
        admitted=await client.post('/v1/runs',json={'session_id':'original-session','input':'first status'},headers=headers)
        run=(await admitted.json())['run_id']
        await asyncio.wait_for(finished.wait(),5)
        response=await client.post(f'/v1/runs/{run}/source-result',json={},headers=headers)
        assert response.status==503
        assert await response.json()=={'state':'unavailable','diagnostic':{
            'phase':'producer_validation','reason':'source_binding_mismatch','error_type':'ValueError'}}
        assert not [row for row in db.get_messages('original-session') if row.get('display_kind')=='external_result']


@pytest.mark.asyncio
@pytest.mark.parametrize('interruption',['timeout','cancel','shutdown'])
async def test_result_transaction_survives_http_lifetime_and_closes_after_binding(owner,monkeypatch,interruption):
    from gateway.platforms import api_server_source_receipts as receipts
    from gateway.run_shutdown import GatewayShutdownMixin
    adapter,app,db,finished,provider,registration=owner
    headers={'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'retained-result'}
    provider.source_result_protocol='per_intent_v1'
    provider.qualify=lambda **kwargs:{'intent':'mara_request','subject':'status'}
    started=asyncio.Event();binding=asyncio.Event();retries=asyncio.Event()
    release=threading.Event();bound=asyncio.Event();calls=[];central={};seen=set()
    loop=asyncio.get_running_loop();auth=adapter._check_auth
    def check(request):
        caller=request.headers.get('X-Test-Caller')
        if caller:
            seen.add(caller)
            if len(seen)==2:retries.set()
        return auth(request)
    monkeypatch.setattr(adapter,'_check_auth',check)
    def produce(source):
        calls.append(source['run_id']);loop.call_soon_threadsafe(started.set)
        assert release.wait(10),'test did not release producer'
        now=datetime.now(timezone.utc).isoformat()
        return dict(state='answered',accepted_source=source,identity='external:retained',
            content='Useful original-source result',display_kind='external_result',
            metadata={'delegation_id':'external:retained','source_run_id':source['run_id']},
            authored_at=now,validated_at=now)
    async def read(source):return await asyncio.to_thread(produce,source)
    async def bind(source,receipt):
        binding.set();await bound.wait()
        row=db.get_source_result_delivery(source['session_id'],'external:retained','external_result')
        assert row['id']==receipt['message_id']
        central[source['run_id']]=row['id']
    provider.read_result=read;provider.bind_gateway_receipt=bind
    monkeypatch.setattr(receipts,'source_read_budget',lambda provider:0.05)
    async with TestClient(TestServer(app)) as client:
        run=(await (await client.post('/v1/runs',json={'session_id':'original-session','input':'first status'},headers=headers)).json())['run_id']
        await asyncio.wait_for(finished.wait(),5)
        path=f'/v1/runs/{run}/source-result'
        request=make_mocked_request('POST',path,headers=headers,match_info={'run_id':run})
        request._read_bytes=b'{}'
        first=asyncio.create_task(adapter._handle_run_source_result(request))
        try:
            await asyncio.wait_for(started.wait(),5)
            if interruption=='cancel':
                first.cancel()
                with pytest.raises(asyncio.CancelledError):await first
            else:
                assert (await first).status==503
            assert adapter.active_agent_work_count()==1
            assert GatewayShutdownMixin._active_api_worker_count(SimpleNamespace())>=1
            if interruption=='shutdown':
                await adapter.disconnect()
                assert not adapter._session_db_cache_closed
                assert adapter._run_idempotency_store.accepted_source_for_run(adapter._run_owners[run],run)
            else:
                monkeypatch.setattr(receipts,'source_read_budget',lambda provider:30)
                pending=[asyncio.create_task(client.post(path,json={},headers={**headers,'X-Test-Caller':str(i)})) for i in range(2)]
                await asyncio.wait_for(retries.wait(),5)
                assert calls==[run]
            release.set()
            await asyncio.wait_for(binding.wait(),5)
            assert adapter.active_agent_work_count()==1
            assert run not in central
            bound.set()
            if interruption=='shutdown':
                task=next(iter(adapter._source_result_transactions.values()))['task']
                assert (await task)[1]==200
                await asyncio.sleep(0)  # run the registered resource-close callbacks
                assert adapter._session_db_cache_closed
            else:
                responses=await asyncio.gather(*pending)
                assert [response.status for response in responses]==[200,200]
                payloads=await asyncio.gather(*(response.json() for response in responses))
                assert payloads[0]['message_id']==payloads[1]['message_id']==central[run]
            rows=[row for row in db.get_messages('original-session') if row.get('display_kind')=='external_result']
            assert len(rows)==1 and rows[0]['id']==central[run]
            assert calls==[run]
            assert adapter.active_agent_work_count()==0
        finally:
            release.set();bound.set()
            if not first.done():await first


@pytest.mark.asyncio
@pytest.mark.parametrize('revoke',['owner','provider',None])
async def test_retained_result_revalidates_authority_and_advances_independent_intents(owner,monkeypatch,revoke):
    from gateway.platforms import api_server_source_receipts as receipts
    adapter,app,db,finished,provider,registration=owner
    provider.source_result_protocol='per_intent_v1'
    provider.qualify=lambda **kwargs:{'intent':'mara_request','subject':'status'}
    release=threading.Event();started=asyncio.Event();loop=asyncio.get_running_loop()
    calls=[];central={};item=[1];at=datetime.now(timezone.utc).isoformat()
    def produce(source):
        calls.append(item[0]);loop.call_soon_threadsafe(started.set)
        assert release.wait(10)
        identity=f'external:item{item[0]}'
        return dict(state='answered',accepted_source=source,identity=identity,
            content=f'Independent result {item[0]}',display_kind='external_result',more_results=item[0]==1,
            metadata={'delegation_id':identity,'source_run_id':source['run_id']},authored_at=at,validated_at=at)
    async def read(source):return await asyncio.to_thread(produce,source)
    async def bind(source,receipt):central[item[0]]=receipt['message_id']
    provider.read_result=read;provider.bind_gateway_receipt=bind
    monkeypatch.setattr(receipts,'source_read_budget',lambda provider:0.05)
    headers={'Authorization':'Bearer synthetic-api-key','Idempotency-Key':'independent-results'}
    async with TestClient(TestServer(app)) as client:
        run=(await (await client.post('/v1/runs',json={'session_id':'original-session','input':'first status'},headers=headers)).json())['run_id']
        await asyncio.wait_for(finished.wait(),5)
        at=datetime.now(timezone.utc).isoformat()
        path=f'/v1/runs/{run}/source-result'
        try:
            first=await client.post(path,json={},headers=headers)
            assert first.status==503
            await asyncio.wait_for(started.wait(),5)
            task=next(iter(adapter._source_result_transactions.values()))['task']
            if revoke=='owner':adapter._run_owners[run]='revoked-owner'
            if revoke=='provider':registration.dispose()
            release.set()
            payload,status=await asyncio.wait_for(asyncio.shield(task),5)
            if revoke:
                assert status==503 and payload['state']=='unavailable'
                assert not central
                assert not [row for row in db.get_messages('original-session') if row.get('display_kind')=='external_result']
            else:
                monkeypatch.setattr(receipts,'source_read_budget',lambda provider:30)
                response=await client.post(path,json={},headers=headers)
                first=await response.json()
                assert response.status==200 and first['more_results'] is True
                assert calls==[1] and first['message_id']==central[1]
                item[0]=2
                response=await client.post(path,json={},headers=headers)
                second=await response.json()
                assert response.status==200 and second['more_results'] is False
                assert second['message_id']==central[2]!=first['message_id']
                assert calls==[1,2]
                repeat=await client.post(path,json={},headers=headers)
                assert (await repeat.json())['message_id']==second['message_id']
                assert len([row for row in db.get_messages('original-session') if row.get('display_kind')=='external_result'])==2
        finally:release.set()
