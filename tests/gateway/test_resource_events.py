"""Native mutation owners and authenticated HTTP replay; no model or external transport."""
import json
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient,TestServer
from cron import jobs
from cron.jobs_resource_events import resource_changes
from gateway.platforms.api_server_resource_events import resource_events
from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore


@pytest.mark.asyncio
@pytest.mark.parametrize('legacy', [[], {'jobs': {'legacy-job': {'prompt': 'PRIVATE'}}}])
async def test_resource_endpoint_reads_legacy_cron_without_repair_or_losing_runs(tmp_path, legacy):
    store=RunIdempotencyStore(str(tmp_path/'runs.db'))
    owner=SimpleNamespace(_run_idempotency_store=store,_room_grant_token=lambda r:None,
        _run_idempotency_scope=lambda r:'scope-a',_check_auth=lambda r:None)
    with jobs.use_cron_store(tmp_path/'legacy'):
        path=jobs._current_cron_store().jobs_file
        path.parent.mkdir(parents=True,exist_ok=True)
        encoded=json.dumps(legacy);path.write_text(encoded)
        response=await resource_events(owner,SimpleNamespace(query={}))
        assert response.status==200
        assert json.loads(response.body)['events']==[]
        assert path.read_text()==encoded  # Reading does not repair or mutate the store.
        jobs.create_job(prompt='Synthetic',schedule='every 2h',paused=True)
        assert resource_changes()  # Normal locked mutation owns new records.
    store.close()

@pytest.mark.asyncio
async def test_native_mutations_replay_restart_and_foreign_denial(tmp_path):
    store=RunIdempotencyStore(str(tmp_path/'runs.db'))
    expected='Bearer synthetic-a'
    owner=SimpleNamespace(_run_idempotency_store=store,_room_grant_token=lambda r:None,
        _run_idempotency_scope=lambda r:'scope-a',
        _check_auth=lambda r:None if r.headers.get('Authorization')==expected else web.Response(status=401))
    @web.middleware
    async def scope(request,handler):
        with jobs.use_cron_store(tmp_path/'profile-a'):return await handler(request)
    async def handle(request):return await resource_events(owner,request)
    app=web.Application(middlewares=[scope]);app.router.add_get('/api/resource-events',handle)
    async with TestClient(TestServer(app)) as client:
        with jobs.use_cron_store(tmp_path/'profile-a'):
            assert (await client.get('/api/resource-events')).status==401
            headers={'Authorization':expected}
            initial=await (await client.get('/api/resource-events',headers=headers)).json()
            from gateway.platforms.api_server_source_receipts import admission_source
            source=admission_source(message='Synthetic mobile request',authenticated=True,profile='default',
                run_id='run_owned',session_id='synthetic-session',scope='scope-a',fingerprint='hash',created_at=1,
                history_delivery=True,provider=SimpleNamespace(name='synthetic',qualify=lambda **kw:{'intent':'mara_request'}))
            store.reserve('scope-a','key','hash','run_owned',{'status':'queued'},accepted_source=source)
            store.reserve('scope-b','foreign','hash','run_foreign',{'status':'queued'},accepted_source=source)
            job=jobs.create_job(prompt='SECRET',schedule='every 2h',paused=True,origin={'platform':'api_server','chat_id':'api'})
            jobs.pause_job(job['id'])
            hidden=jobs.create_job(prompt='PRIVATE-WORKSPACE',schedule='every 3h',paused=True,workdir=str(tmp_path))
            store.update_status('run_owned',{'status':'completed'})
            store.update_status('run_owned',{'status':'completed'})
            result=await (await client.get('/api/resource-events',headers=headers,params={'cursor':initial['cursor']})).json()
            assert {'automations','runs'}=={e['resource'] for e in result['events']}
            assert 'run_foreign' not in json.dumps(result) and 'SECRET' not in json.dumps(result)
            assert hidden['id'] not in json.dumps(result)
            assert sum(e['id']=='run_owned' for e in result['events'])==2
            store.close();store=RunIdempotencyStore(str(tmp_path/'runs.db'));owner._run_idempotency_store=store
            again=await (await client.get('/api/resource-events',headers=headers,params={'cursor':initial['cursor']})).json()
            assert again==result
            assert (await (await client.get('/api/resource-events',headers=headers,params={'cursor':result['cursor']})).json())['events']==[]
            assert (await client.get('/api/resource-events',headers=headers,params={'cursor':'ahead'})).status==409
            expected='Bearer revoked'
            assert (await client.get('/api/resource-events',headers=headers)).status==401
    store.close()

@pytest.mark.asyncio
async def test_scope_profile_alternation_and_legacy_cron(tmp_path):
    from gateway.platforms.api_server import _api_request_profile
    with jobs.use_cron_store(tmp_path/'a'):
        a=jobs.create_job(prompt='private-a',schedule='every 2h',paused=True)
        assert {e['id'] for e in resource_changes()}=={a['id']}
    with jobs.use_cron_store(tmp_path/'b'):
        assert resource_changes()==[]
        b=jobs.create_job(prompt='private-b',schedule='every 2h',paused=True)
        assert {e['id'] for e in resource_changes()}=={b['id']}
    with jobs.use_cron_store(tmp_path/'a'):
        assert {e['id'] for e in resource_changes()}=={a['id']}
    owner=SimpleNamespace(_check_auth=lambda r:None,_room_grant_token=lambda r:None)
    token=_api_request_profile.set('foreign')
    try:assert (await resource_events(owner,SimpleNamespace())).status==403
    finally:_api_request_profile.reset(token)
