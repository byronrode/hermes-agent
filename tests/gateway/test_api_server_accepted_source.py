"""Accepted-run source provenance must come from authenticated owner records."""
import asyncio
from contextvars import copy_context
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
import pytest

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
from gateway.platforms.api_server_accepted_source import (
    accepted_run_scope, admission_resolver, current_accepted_run,
)


@pytest.mark.asyncio
@pytest.mark.parametrize('raw_input', ['CTO status', [{'role': 'user', 'content': 'CTO status'}]])
async def test_authenticated_http_admission_is_bound_before_model_and_replay_reuses_it(tmp_path, raw_input):
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "synthetic-api-key"}))
    adapter._run_idempotency_store.close()
    adapter._run_idempotency_store = RunIdempotencyStore(str(tmp_path / "runs.db"))
    app = web.Application()
    app.router.add_post("/v1/runs", adapter._handle_runs)
    seen = []
    hook_seen = []
    finished = asyncio.Event()
    loop = asyncio.get_running_loop()
    agent = MagicMock()
    agent._persist_disabled = False
    agent.session_id, agent.platform, agent.model = "synthetic-session", "api_server", "synthetic-model"
    agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0
    def conversation(**kwargs):
        seen.append(current_accepted_run())
        from agent.turn_context import _collect_pre_llm_call_context
        _collect_pre_llm_call_context(agent, effective_task_id="synthetic-session", turn_id="model-turn",
            original_user_message=kwargs['user_message'], messages=[], conversation_history=[])
        loop.call_soon_threadsafe(finished.set)
        return {"final_response": "synthetic answer"}
    agent.run_conversation.side_effect = conversation
    body = {"session_id": "synthetic-session", "input": raw_input}
    headers = {"Authorization": "Bearer synthetic-api-key", "Idempotency-Key": "synthetic-request"}
    try:
        with patch.object(adapter, "_create_agent", return_value=agent), \
                patch("hermes_cli.lifecycle.invoke_hook", side_effect=lambda name, **kw: hook_seen.append(kw) or []):
            async with TestClient(TestServer(app)) as cli:
                assert (await cli.post("/v1/runs", json=body)).status == 401
                accepted = await cli.post("/v1/runs", json=body, headers=headers)
                assert accepted.status == 202
                run_id = (await accepted.json())["run_id"]
                await asyncio.wait_for(finished.wait(), 5)
                replay = await cli.post("/v1/runs", json=body, headers=headers)
                assert (await replay.json())["run_id"] == run_id
                conflict = await cli.post("/v1/runs", json={**body, "input": "CPO status"}, headers=headers)
                assert conflict.status == 409
        assert len(seen) == 1
        source = seen[0]
        if not isinstance(raw_input, str):
            assert source is None
            assert 'accepted_api_run' not in hook_seen[0]
            return
        assert source["run_id"] == run_id and source["session_id"] == body["session_id"]
        from hermes_cli.profiles import get_active_profile_name
        assert source["profile"] == get_active_profile_name()
        assert source["message_sha256"] == hashlib.sha256(body["input"].encode()).hexdigest()
        assert hook_seen[0]["accepted_api_run"] == source
        assert "synthetic-api-key" not in json.dumps(source)
        assert current_accepted_run() is None
    finally:
        adapter._run_idempotency_store.close()


@pytest.mark.asyncio
async def test_owner_resolver_checks_durable_record_profile_scope_and_restores_nested_context(tmp_path):
    stores = [RunIdempotencyStore(str(tmp_path / f"{name}.db")) for name in ("a", "b")]
    try:
        resolvers = []
        for profile, store in zip(("default", "other"), stores):
            scope = hashlib.sha256(profile.encode()).hexdigest()
            run_id = "run_" + hashlib.md5(profile.encode()).hexdigest()
            status = {"run_id": run_id, "session_id": "same-session", "created_at": 123, "status": "queued"}
            store.reserve(scope, "same-key", "fingerprint", run_id, status)
            owner = SimpleNamespace(_run_idempotency_store=store, _run_owners={run_id: scope})
            args = dict(scope=scope, key="same-key", fingerprint="fingerprint", run_id=run_id,
                        session_id="same-session", profile=profile, message="CTO status", authenticated=True)
            resolvers.append(admission_resolver(owner, **args))
            assert admission_resolver(owner, **{**args, "authenticated": False})() is None
            assert admission_resolver(owner, **{**args, "scope": "other-auth"})() is None
            assert admission_resolver(owner, **{**args, "fingerprint": "changed"})() is None
            assert admission_resolver(owner, **{**args, "session_id": "other-session"})() is None
        with accepted_run_scope(resolvers[0]):
            first = current_accepted_run()
            copied_parent = copy_context()
            from agent.delegation_context import delegated_child_context
            from gateway.session_context import get_session_env
            from agent.turn_context import _collect_pre_llm_call_context
            def child():
                with delegated_child_context('actual-child-session'):
                    assert get_session_env('HERMES_SESSION_ID') == 'actual-child-session'
                    hook_seen = []
                    agent = SimpleNamespace(_persist_disabled=False, session_id='actual-child-session',
                        platform='api_server', model='child-model', _parent_session_id='same-session')
                    with patch('hermes_cli.lifecycle.invoke_hook', side_effect=lambda name, **kw: hook_seen.append(kw) or []):
                        _collect_pre_llm_call_context(agent, effective_task_id='actual-child-session',
                            turn_id='child-turn', original_user_message='different child goal', messages=[], conversation_history=[])
                    assert 'accepted_api_run' not in hook_seen[0]
                    assert current_accepted_run() is None
                assert current_accepted_run() == first
            copied_parent.run(child)
            mismatched_hook = []
            with patch('hermes_cli.lifecycle.invoke_hook', side_effect=lambda name, **kw: mismatched_hook.append(kw) or []):
                _collect_pre_llm_call_context(SimpleNamespace(_persist_disabled=False,
                    session_id='different-executing-session', platform='api_server', model='other-model'),
                    effective_task_id='different-executing-session', turn_id='other-turn',
                    original_user_message='different goal', messages=[], conversation_history=[])
            assert 'accepted_api_run' not in mismatched_hook[0]
            resume = asyncio.Event()
            async def retained_child():
                await resume.wait()
                assert current_accepted_run() is None
            retained = asyncio.create_task(retained_child())
            with accepted_run_scope(resolvers[1]):
                assert current_accepted_run()["profile"] == "other"
                copied_nested = copy_context()
            assert current_accepted_run() == first
            assert copied_nested.run(current_accepted_run) is None
            original_status = stores[0].status_for_run(first['auth_scope'], first['run_id'])['status']
            stores[0].update_status(first["run_id"], {"session_id": "changed"})
            assert current_accepted_run() is None
            stores[0].update_status(first['run_id'], original_status)
            assert current_accepted_run() == first
        assert current_accepted_run() is None
        assert copied_parent.run(current_accepted_run) is None
        resume.set()
        await retained
    finally:
        for store in stores:
            store.close()

@pytest.mark.asyncio
async def test_exact_source_text_remains_native_revocable_and_provider_qualified(tmp_path):
    from gateway.platforms.api_server_accepted_source import current_accepted_source_message
    from agent.delegation_context import delegated_child_context
    store=RunIdempotencyStore(str(tmp_path/'source.db'))
    message='Synthetic explicit company work request'
    status=dict(run_id='run_synthetic',session_id='source-session',created_at=1,status='running')
    store.reserve('scope','key','fingerprint','run_synthetic',status)
    owner=SimpleNamespace(_run_idempotency_store=store,_run_owners={'run_synthetic':'scope'})
    resolve=admission_resolver(owner,scope='scope',key='key',fingerprint='fingerprint',run_id='run_synthetic',
        session_id='source-session',profile='default',message=message,authenticated=True)
    # Provider transport substituted; native reservation, resolver and revocation
    # are real. The connected consumer tests use actual HTTP provider admission.
    qualified=dict(resolve())
    resolve.qualified_source=lambda:qualified
    try:
        with accepted_run_scope(resolve):
            assert current_accepted_source_message(session_id='source-session')==message
            assert current_accepted_source_message(session_id='foreign') is None
            copied=copy_context()
            with delegated_child_context('child'):
                assert current_accepted_source_message() is None
            qualified['message_sha256']='foreign'
            assert current_accepted_source_message() is None
            qualified['message_sha256']=hashlib.sha256(message.encode()).hexdigest()
            with accepted_run_scope(None):assert current_accepted_source_message() is None
            assert current_accepted_source_message()==message
            owner._run_owners.clear()
            assert current_accepted_source_message() is None
            owner._run_owners['run_synthetic']='scope'
        assert copied.run(current_accepted_source_message) is None
        assert current_accepted_source_message() is None
    finally:store.close()


def test_same_native_reservation_retains_distinct_immutable_intent_results(tmp_path):
    path=str(tmp_path/'multi.db');store=RunIdempotencyStore(path)
    source={'run_id':'run_synthetic','session_id':'source'}
    store.reserve('scope','key','fingerprint','run_synthetic',{'status':'completed'},accepted_source=source)
    first=dict(accepted_source=source,identity='intent-one',content='Synthetic one')
    second=dict(accepted_source=source,identity='intent-two',content='Synthetic two')
    try:
        store.remember_source_result('scope','run_synthetic',source,first)
        store.remember_source_result('scope','run_synthetic',source,second,allow_multiple=True)
        store.close();store=RunIdempotencyStore(path)
        assert store.source_result_for_run('scope','run_synthetic')==first
        assert store.source_result_for_run('scope','run_synthetic','intent-two')==second
        assert store.source_result_for_run('foreign','run_synthetic','intent-two') is None
        with pytest.raises(ValueError):store.remember_source_result('scope','run_synthetic',source,dict(first,content='changed'),allow_multiple=True)
        with pytest.raises(ValueError):store.remember_source_result('scope','run_synthetic',dict(source,session_id='foreign'),second,allow_multiple=True)
    finally:store.close()
