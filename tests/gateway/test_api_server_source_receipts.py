"""HTTP admission/readback and detached result regression contracts (IGN-1663)."""
import asyncio
import copy
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
import pytest
import pytest_asyncio

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
from hermes_state import SessionDB


@pytest_asyncio.fixture
async def owner(tmp_path):
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={'key': 'synthetic-api-key'}))
    adapter._run_idempotency_store.close()
    adapter._run_idempotency_store = RunIdempotencyStore(str(tmp_path / 'runs.db'))
    db = SessionDB(db_path=tmp_path / 'state.db')
    db.create_session('original-session', source='api_server')
    db.append_message('original-session', 'user', 'CTO status')
    adapter._session_db = db
    agent = MagicMock()
    agent._persist_disabled = False
    agent.session_id, agent.platform, agent.model = 'original-session', 'api_server', 'synthetic-model'
    agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0
    finished = asyncio.Event()
    loop = asyncio.get_running_loop()
    def conversation(**kwargs):
        loop.call_soon_threadsafe(finished.set)
        return {'final_response': 'synthetic completion'}
    agent.run_conversation.side_effect = conversation
    app = web.Application()
    app.router.add_post('/v1/runs', adapter._handle_runs)
    app.router.add_get('/v1/runs/{run_id}/accepted-source', adapter._handle_run_source_receipt)
    app.router.add_post('/v1/runs/{run_id}/source-result', adapter._handle_run_source_result)
    app.router.add_get('/api/sessions/{session_id}/messages', adapter._handle_session_messages)
    with patch.object(adapter, '_create_agent', return_value=agent), patch.object(adapter, '_ensure_session_db', return_value=db):
        yield adapter, app, db, finished
        await asyncio.gather(*adapter._active_run_tasks.values(), return_exceptions=True)
    adapter._run_idempotency_store.close()
    adapter._response_store.close()
    db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('message,forwarded', [(message,False) for message in
    ['CTO status', 'CPO status', 'CISO status', 'support status', 'CTO and CPO status',
     [{'role': 'user', 'content': 'CTO status'}]]] + [('CTO status',True)])
async def test_http_accepted_source_is_original_admission_not_context_or_status_json(owner, message, forwarded, monkeypatch):
    adapter, app, db, finished = owner
    headers = {'Authorization': 'Bearer synthetic-api-key', 'Idempotency-Key': 'original-request'}
    body = {'session_id': 'original-session', 'input': message}
    if forwarded:
        monkeypatch.setattr(adapter, '_admit_to_live_bot_chat', AsyncMock(
            return_value=('synthetic-home', {'delivery_id':'actual-forwarded-owner'})))
        monkeypatch.setattr('gateway.platforms.api_server_runs._execute_run_via_live_owner', AsyncMock())
    async with TestClient(TestServer(app)) as cli:
        assert (await cli.post('/v1/runs', json=body)).status == 401
        accepted = await cli.post('/v1/runs', json=body, headers=headers)
        assert accepted.status == 202
        run = (await accepted.json())['run_id']
        if not forwarded:
            await asyncio.wait_for(finished.wait(), 5)
        replay = await cli.post('/v1/runs', json=body, headers=headers)
        assert (await replay.json())['run_id'] == run
        path = f'/v1/runs/{run}/accepted-source'
        assert (await cli.get(path)).status == 401
        response = await cli.get(path, headers=headers)
        if forwarded or not isinstance(message, str) or 'and' in message:
            assert response.status == 404
            return
        assert response.status == 200
        source = await response.json()
        assert source['run_id'] == run and source['session_id'] == body['session_id']
        assert source['intent'] == 'role_status' and source['role'] == message.split()[0].lower()
        assert len(source['body_fingerprint']) == 64
        assert source['owner_pid'] == adapter._run_owner_pid
        assert source['owner_started'] == adapter._run_owner_started
        assert 'synthetic-api-key' not in json.dumps(source)
        assert message not in json.dumps(source)
        status = adapter._run_idempotency_store.status_for_run(source['auth_scope'], run)['status']
        adapter._run_idempotency_store.update_status(run, {**status, 'accepted_source_json': {'role': 'foreign'}})
        assert await (await cli.get(path, headers=headers)).json() == source
        for changed in ({'session_id': 'foreign-session'}, {'status': 'cancelled'}, {'delivery_id': 'foreign-live-owner'}):
            adapter._run_idempotency_store.update_status(run, {**status, **changed})
            assert (await cli.get(path, headers=headers)).status == 404
        adapter._run_idempotency_store.update_status(run, status)
        assert (await cli.get(path, headers=headers)).status == 200
        adapter._run_owners[run] = 'another-principal'
        assert (await cli.get(path, headers=headers)).status == 404


@pytest.mark.asyncio
async def test_http_result_uses_owner_subprocess_original_session_and_atomic_delivery_readback(owner, tmp_path, monkeypatch):
    adapter, app, db, finished = owner
    headers = {'Authorization': 'Bearer synthetic-api-key', 'Idempotency-Key': 'original-request'}
    # A synthetic protected transport executable exercises the production subprocess
    # route. Native proof/key isolation is separately checked by the host HTTP suite.
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    fixture = tmp_path / '.local/share/mara-host-agent/current'
    fixture.parent.mkdir(parents=True)
    output = tmp_path / 'synthetic_native_result.json'
    fixture.write_text('#!' + sys.executable + '\nimport json,sys\nfrom pathlib import Path\nrequest=json.load(sys.stdin)\n'
                       'assert set(request)=={"run_id","phase"} and request["phase"]=="read"\n'
                       'print(Path(' + repr(str(output)) + ').read_text())\n')
    fixture.chmod(0o700)
    adapter.config.extra['source_result_transport'] = {'host': 'ignis-one'}
    async with TestClient(TestServer(app)) as cli:
        admitted = await cli.post('/v1/runs', json={'session_id':'original-session','input':'CTO status'}, headers=headers)
        assert admitted.status == 202
        run = (await admitted.json())['run_id']
        await asyncio.wait_for(finished.wait(), 5)
        source = await (await cli.get(f'/v1/runs/{run}/accepted-source', headers=headers)).json()
        path = f'/v1/runs/{run}/source-result'
        assert (await cli.post(path, json={})).status == 401
        assert (await cli.post(path, json={'answer':'forged','session_id':'foreign'}, headers=headers)).status == 503
        source_ref = 'document:00112233-4455-6677-8899-aabbccddeeff:00112233-4455-6677-8899-aabbccddeeff'
        result = {'state':'queued','accepted_source':source,'request_id':'00112233-4455-6677-8899-aabbccddeeff','source_reference':source_ref}
        output.write_text(json.dumps(result))
        assert (await (await cli.post(path,json={},headers=headers)).json())['state'] == 'queued'
        assert not [m for m in db.get_messages('original-session') if m.get('display_kind') == 'role_status_result']
        result.update(state='answered', answer={'agent_id':'00112233-4455-6677-8899-aabbccddeeff',
            'text':'Fresh CTO-authored answer','updated_at':datetime.now(timezone.utc).isoformat(),
            'source_reference':'run:00112233-4455-6677-8899-aabbccddeeff'})
        foreign = copy.deepcopy(result)
        foreign['accepted_source']['session_id'] = 'foreign-session'
        output.write_text(json.dumps(foreign))
        assert (await cli.post(path,json={},headers=headers)).status == 503
        output.write_text(json.dumps(result))
        assert db.acquire_session_turn_lease('original-session', 'actual-client-turn', wait_seconds=0)
        try:
            assert (await cli.post(path,json={},headers=headers)).status == 503
        finally:
            db.release_session_turn_lease('original-session', 'actual-client-turn')
        first = await cli.post(path,json={},headers=headers)
        assert first.status == 200, await first.text()
        persisted = await first.json()
        assert persisted['state'] == 'persisted' and persisted['session_id'] == 'original-session'
        second = await cli.post(path,json={},headers=headers)
        assert (await second.json())['message_id'] == persisted['message_id']
        rows = [m for m in db.get_messages('original-session') if m.get('display_kind') == 'role_status_result']
        assert len(rows) == 1 and rows[0]['role'] == 'assistant'
        assert rows[0]['content'] == 'CTO: Fresh CTO-authored answer'
        assert rows[0]['display_metadata']['source_run_id'] == run
        assert rows[0]['display_metadata']['source_reference'] == source_ref
        assert db.get_session('foreign-session') is None
        changed = copy.deepcopy(result)
        changed['answer']['text'] = 'Substituted content under the same request'
        output.write_text(json.dumps(changed))
        assert (await cli.post(path,json={},headers=headers)).status == 503
        output.write_text(json.dumps(result))
        db.end_session('original-session', 'compression')
        db.create_session('continued-session', source='api_server', parent_session_id='original-session')
        replay = await cli.post(path,json={},headers=headers)
        assert replay.status == 200, await replay.text()
        assert (await replay.json())['message_id'] == persisted['message_id']
        assert db.get_messages('continued-session') == []
