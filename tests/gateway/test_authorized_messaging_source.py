"""Real Discord SDK events, native auth/turn/SQLite owners; synthetic model and send transport."""
import asyncio
import sys
import threading
from contextvars import copy_context
from types import SimpleNamespace
from unittest.mock import AsyncMock

# The gateway conftest installs a lightweight Discord stub for adapter unit
# tests. This per-file-isolated integration deliberately requires the real SDK.
for _name in tuple(sys.modules):
    if _name == 'discord' or _name.startswith('discord.'):
        del sys.modules[_name]
import discord
import pytest

from agent.turn_context import _bind_turn_identity
from agent.delegation_context import delegated_child_context
from gateway.accepted_source import current_gateway_source
from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.event import MessageEvent
from gateway.platforms.base import SendResult
from gateway.run import GatewayRunner
from gateway import delivery_ledger as ledger
from gateway.source_delivery import drain_gateway_sources
from hermes_state import SessionDB
from plugins.platforms.discord.adapter import DiscordAdapter
from tools.approval_context import set_current_observability_context, reset_current_observability_context


def rig(tmp_path, monkeypatch, *, dm=False):
    monkeypatch.setenv('HERMES_HOME',str(tmp_path))
    monkeypatch.setenv('DISCORD_ALLOWED_USERS','123')
    monkeypatch.setattr(ledger,'_db_path',lambda:tmp_path/'state.db')
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner._primary_profile_name = 'default'
    adapter = DiscordAdapter(PlatformConfig(enabled=True))
    adapter.gateway_runner = runner
    adapter.platform = Platform.DISCORD
    adapter._running = True  # synthetic connected transport; no gateway/socket starts
    adapter._client = discord.Client(intents=discord.Intents.none())
    runner.adapters = {Platform.DISCORD:adapter};runner._profile_adapters = {}
    state = adapter._client._connection
    guild = discord.Guild(state=state,data={'id':'333','name':'Synthetic guild','roles':[], 'members':[]})
    parent = discord.TextChannel(state=state,guild=guild,data={'id':'221','name':'synthetic','type':0,'position':0,'permission_overwrites':[]})
    guild._channels[parent.id]=parent
    channel = discord.Thread(state=state,guild=guild,data={'id':'222','name':'synthetic thread','type':11,
        'parent_id':'221','owner_id':'123','rate_limit_per_user':0,'message_count':1,'member_count':1,
        'thread_metadata':{'archived':False,'locked':False,'auto_archive_duration':60,'archive_timestamp':'2026-11-03T00:00:00+00:00'}})
    if dm:
        channel = discord.DMChannel(state=state, me=SimpleNamespace(id=999), data={
            'id':'222','type':1,'recipients':[{'id':'123','username':'Synthetic','discriminator':'0','avatar':None}]})
    raw = discord.Message(state=state,channel=channel,data={'id':'444','content':'@mara ordinary report',
        'author':{'id':'123','username':'Synthetic','discriminator':'0','avatar':None},
        'timestamp':'2026-11-03T00:00:00+00:00','type':0,'attachments':[], 'embeds':[], 'mentions':[],
        'mention_roles':[], 'pinned':False,'mention_everyone':False,'tts':False})
    source = adapter.build_source(chat_id='222',thread_id='222',guild_id='333',user_id='123',chat_type='thread')
    if dm:
        raw.content = 'Explicit Ignis synthetic work request'
        source = adapter.build_source(chat_id='222',user_id='123',chat_type='dm')
    event = MessageEvent(text=raw.content,source=source,message_id=str(raw.id),raw_message=raw)
    db = SessionDB(db_path=tmp_path/'state.db');db.create_session('source-session',source='discord')
    runner._session_db = db;runner._running = True
    key = runner._session_key_for_source(source)
    generation = runner._begin_session_run_generation(key)
    agent = SimpleNamespace(session_id='source-session')
    _,turn = _bind_turn_identity(agent,'source-session',None,None,None,None)
    provider = SimpleNamespace(qualify_gateway_source=lambda source:{'intent':'ordinary'})
    manager = SimpleNamespace(_api_source_providers={'synthetic':provider},invoke_hook=lambda *args,**kwargs:[])
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager',lambda:manager)
    return SimpleNamespace(runner=runner,adapter=adapter,event=event,source=source,db=db,key=key,
        generation=generation,turn=turn,provider=provider,manager=manager)


async def execute(r, monkeypatch, inside):
    # Exercise the actual native adapter normalizer, including its non-public
    # live-vs-recovered SDK provenance. No HTTP/history fetch or send occurs.
    captured=[]
    async def capture(event):captured.append(event)
    r.adapter.config.extra['history_backfill']=False
    r.adapter._text_batch_delay_seconds=0.6
    await r.adapter._threads.mark_async('222')
    with pytest.MonkeyPatch.context() as normalization:
        normalization.setattr(r.adapter,'handle_message',capture)
        assert await r.adapter._handle_message(r.event.raw_message)
        for key in tuple(r.adapter._pending_text_batches):
            await r.adapter._flush_text_batch_now(key)
        timers=list(r.adapter._pending_text_batch_tasks.values())
        for timer in timers:timer.cancel()
        await asyncio.gather(*timers,return_exceptions=True)
    assert len(captured)==1
    r.event=captured[0];r.source=r.event.source
    async def synthetic_model(message,context_prompt,history,source,session_id,**kwargs):
        from gateway.turn_context import TurnContext
        from gateway.run_turn_runner import TurnRunner
        metadata=kwargs.get('persist_user_display_metadata')
        def conversation(message,**call):
            # Use the actual production stages, in production order: hooks run
            # before the crash-resilient inbound-row flush, never after it.
            from agent.turn_context import _collect_pre_llm_call_context, _persist_turn_start
            if not r.db.get_messages(session_id):
                assert current_gateway_source() is None
                assert current_gateway_source(admit=False), 'trusted inspection must work before flush'
            r.pre_llm_context=_collect_pre_llm_call_context(agent,effective_task_id=session_id,turn_id=r.turn,
                original_user_message=message,messages=[],conversation_history=[])
            before_tool=getattr(r,'before_tool',None)
            if before_tool:before_tool()
            _persist_turn_start(agent,[],[],None)
            tokens = set_current_observability_context(turn_id=r.turn,session_id=session_id,tool_call_id='synthetic-tool')
            try:
                value=inside()
                # Real turn-end persistence must preserve the source binding
                # added while the tool was running, without a second user row.
                r.db.replace_messages(session_id,[dict(role='user',content=message,
                    platform_message_id=r.event.message_id,display_metadata=metadata)])
                return value
            finally:
                reset_current_observability_context(tokens)
        agent=SimpleNamespace(run_conversation=conversation,session_id=session_id,platform='discord',_current_turn_id=r.turn)
        agent.model='synthetic-model'
        agent._ensure_db_session=lambda:None
        agent._persist_session=lambda messages,history:r.db.append_message(session_id,'user',message,
            platform_message_id=r.event.message_id,display_metadata=metadata)
        from agent.tool_guardrails import ToolCallGuardrailController
        from agent.tool_executor import _run_sequential_tool_execution_middleware
        agent._tool_worker_threads_lock=threading.Lock();agent._tool_worker_threads=set()
        agent._tool_guardrails=ToolCallGuardrailController()
        agent._interrupt_requested=False;agent._checkpoint_mgr=SimpleNamespace(enabled=False)
        agent._touch_activity=lambda text:None
        agent.quiet_mode=True;agent.verbose_logging=False
        agent.tool_progress_callback=None;agent.tool_start_callback=None
        def run_tool(name,args,call):
            return _run_sequential_tool_execution_middleware(agent,function_name=name,function_args=args,
                effective_task_id=session_id,tool_call_id='synthetic-native-tool',execute=call).result
        r.run_tool=run_tool
        ctx=TurnContext(source=source,session_id=session_id,session_key=r.key,message=message,
            inbound_message_id=r.event.message_id,persist_user_display_metadata=metadata)
        return TurnRunner(r.runner,ctx)._run_conversation_with_approval(agent,[],None,None,None)
    monkeypatch.setattr(r.runner,'_run_agent_inner',synthetic_model)
    return await r.runner._run_agent(r.event.text,'',[],r.source,getattr(r,'session_id','source-session'),
        session_key=r.key,run_generation=r.generation,authorized_event=r.event)


@pytest.mark.asyncio
async def test_only_authorized_native_external_turn_can_bind_and_scope_revokes(tmp_path, monkeypatch):
    r = rig(tmp_path,monkeypatch)
    try:
        # Existing PRE-AUTH hook still runs before authorization and sees no capability.
        seen = []
        async def pre(event,source):
            seen.append(current_gateway_source());return event
        monkeypatch.setattr(r.runner,'_hm_pre_gateway_dispatch_hook',pre)
        assert await r.runner._hm_admit_event(r.event)
        assert seen == [None]
        captured = []
        def inspect():
            source = current_gateway_source();assert source
            from gateway.accepted_source import current_gateway_source_message
            assert current_gateway_source_message() == r.event.text
            assert source['turn_id'] == r.turn and source['message_id'] == '444'
            captured.append(copy_context())
            # Exercise the actual bounded native hook worker, not a direct
            # callback on the agent thread. Only inspection is lent to it.
            from hermes_cli.plugins import PluginManager
            hooks=PluginManager()
            def hook():
                assert current_gateway_source(admit=False)==source
                assert current_gateway_source() is None
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    assert pool.submit(copy_context().run,lambda:current_gateway_source(admit=False)).result(timeout=5) is None
                return 'inspected'
            assert hooks._run_hook_callback_bounded('pre_llm_call',hook,{},5)=='inspected'
            release=threading.Event();finished=threading.Event();late=[]
            def abandoned():
                assert current_gateway_source(admit=False)==source
                release.wait(timeout=3)
                late.append(current_gateway_source(admit=False));finished.set()
            hooks._run_hook_callback_bounded('pre_llm_call',abandoned,{},0.05)
            release.set()
            assert finished.wait(timeout=3) and late==[None]
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                assert pool.submit(copy_context().run,current_gateway_source).result(timeout=5) is None
                assert pool.submit(copy_context().run,current_gateway_source_message).result(timeout=5) is None
            from hermes_constants import set_hermes_home_override,reset_hermes_home_override
            token = set_hermes_home_override(tmp_path/'profile-b')
            try:assert current_gateway_source() is None
            finally:reset_hermes_home_override(token)
            assert current_gateway_source() == source
            with delegated_child_context():
                assert current_gateway_source() is None
            r.manager._api_source_providers['synthetic']=SimpleNamespace(
                qualify_gateway_source=lambda source:{'intent':'ordinary'})
            assert current_gateway_source() is None
            r.manager._api_source_providers['synthetic']=r.provider
            assert current_gateway_source() is None  # registration A→B→A does not revive scope
            return source
        source = await execute(r,monkeypatch,inspect)
        assert captured[0].run(current_gateway_source) is None
        assert r.db.pending_gateway_sources() == [source]
        # Replayed native event cannot acquire a different core turn.
        _,r.turn = _bind_turn_identity(SimpleNamespace(session_id='source-session'),'source-session',None,None,None,None)
        async def no_extra_user(message,context_prompt,history,source,session_id,**kwargs):
            token=set_current_observability_context(turn_id=r.turn,session_id=session_id)
            from gateway.accepted_source import gateway_source_execution
            try:
                with gateway_source_execution(SimpleNamespace(session_id=session_id,_current_turn_id=r.turn)):
                    assert current_gateway_source() is None
            finally:reset_current_observability_context(token)
        monkeypatch.setattr(r.runner,'_run_agent_inner',no_extra_user)
        await r.runner._run_agent(r.event.text,'',[],r.source,'source-session',session_key=r.key,
            run_generation=r.generation,authorized_event=r.event)
        for deny in ('internal','foreign_user','foreign_profile','forged_native','historical','merged_text','stale_generation'):
            r.event.internal = deny == 'internal'
            r.source.user_id = '999' if deny == 'foreign_user' else '123'
            r.source.profile = 'other' if deny == 'foreign_profile' else None
            original = r.event.raw_message
            text = r.event.text
            if deny == 'merged_text':r.event.text += '\nCopied extra request'
            r.event._gateway_source_external = deny != 'historical'
            if deny == 'forged_native':r.event.raw_message = SimpleNamespace(id=444)
            if deny == 'stale_generation':r.runner._begin_session_run_generation(r.key)
            await r.runner._run_agent(r.event.text,'',[],r.source,'source-session',session_key=r.key,
                run_generation=r.generation,authorized_event=r.event)
            r.event.raw_message = original
            r.event.text = text
        assert r.db.pending_gateway_sources() == [source]
        assert captured[0].run(current_gateway_source) is None  # A→B→A cannot revive old context
        r.session_id='new-source-session';r.db.create_session(r.session_id,source='discord')
        _,r.turn=_bind_turn_identity(SimpleNamespace(session_id=r.session_id),r.session_id,None,None,None,None)
        r.generation=r.runner._begin_session_run_generation(r.key)
        r.event.internal=False;r.event._gateway_source_external=True
        assert await execute(r,monkeypatch,current_gateway_source) is None
        assert r.db.pending_gateway_sources()==[source]
        assert r.db.gateway_source_row(source)  # replay cannot poison original custody
    finally:
        r.db.close();await r.adapter._client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary,stage,reason,state',[
    ('poll','poll','exception',None),('provider','provider_lookup','provider_or_binder_unavailable',None),
    ('route','route_initial','exception',None),('timeout','producer_read','timeout',None),
    ('malformed','producer_validation','exception',None),
    ('identity','identity_before_commit','adapter_manager_or_provider_changed',None),
    ('route_changed','route_before_commit','exception',None),
    ('adapter','send_eligibility','adapter_unavailable','pending'),
    ('append','os_append','exception',None),('claim','send_claim','exception','pending'),
    ('before_send','identity_before_send','gateway_adapter_manager_or_provider_changed','unknown'),
    ('after_send','identity_after_send','gateway_adapter_manager_or_provider_changed','unknown'),
    ('send','send','exception','unknown'),('unknown','send_ack','unknown_ack','unknown'),
    ('ack','acknowledge','ack_not_committed','unknown'),
    ('bind','central_bind','exception','delivered')])
async def test_drain_diagnostics_are_sanitized_and_preserve_custody(tmp_path,monkeypatch,caplog,boundary,stage,reason,state):
    r=rig(tmp_path,monkeypatch)
    try:
        source=await execute(r,monkeypatch,current_gateway_source)
        delivery={'state':'answered','accepted_source':source,'identity':'synthetic-diagnostic',
            'content':'Synthetic result','metadata':{'delegation_id':'synthetic-diagnostic','source_run_id':source['run_id']},'display_kind':'source_result'}
        r.provider.read_result=AsyncMock(return_value=delivery)
        r.provider.bind_gateway_receipt=AsyncMock(return_value=False)
        r.adapter.send=AsyncMock(return_value=SendResult(success=True,message_id='native-diagnostic'))
        secret='SECRET-private-prompt-token'
        error=type('SECRETMaliciousClass',(ValueError,),{})(secret)
        def fail(*args,**kwargs):raise error
        if boundary=='poll':monkeypatch.setattr(r.db,'mark_gateway_source_polled',fail)
        if boundary=='provider':del r.provider.bind_gateway_receipt
        if boundary=='route':monkeypatch.setattr(r.runner,'_is_user_authorized_for_source',fail)
        if boundary=='timeout':r.provider.read_result=AsyncMock(side_effect=TimeoutError(secret))
        if boundary=='malformed':r.provider.read_result=AsyncMock(return_value=dict(delivery,accepted_source={'private':secret}))
        if boundary=='identity':
            async def replace(source):
                r.manager._api_source_providers['synthetic']=SimpleNamespace()
                return delivery
            r.provider.read_result=replace
        if boundary=='route_changed':
            async def revoke(source):
                monkeypatch.setattr(r.runner,'_is_user_authorized_for_source',fail)
                return delivery
            r.provider.read_result=revoke
        if boundary=='adapter':r.adapter._running=False
        if boundary=='append':monkeypatch.setattr(r.db,'append_source_result_delivery',fail)
        if boundary=='claim':monkeypatch.setattr(ledger,'begin_source_send',fail)
        if boundary=='before_send':
            claim=ledger.begin_source_send
            def stop_after_claim(oid):
                result=claim(oid);r.runner._running=False
                return result
            monkeypatch.setattr(ledger,'begin_source_send',stop_after_claim)
        if boundary=='after_send':
            async def stop_after_send(**kwargs):
                r.runner._running=False
                return SendResult(success=True,message_id='synthetic-late-ack')
            r.adapter.send=AsyncMock(side_effect=stop_after_send)
        if boundary=='send':r.adapter.send=AsyncMock(side_effect=error)
        if boundary=='unknown':r.adapter.send=AsyncMock(return_value=SendResult(success=False))
        if boundary=='ack':monkeypatch.setattr(ledger,'acknowledge_source_send',lambda *args:False)
        if boundary=='bind':r.provider.bind_gateway_receipt=AsyncMock(side_effect=error)
        await drain_gateway_sources(r.runner)
        records=[record for record in caplog.records if record.name=='gateway.source_delivery']
        assert any(f'stage={stage} reason={reason} ' in record.getMessage() for record in records)
        assert all('SECRET' not in record.getMessage() and not record.exc_info for record in records)
        assert r.db.pending_gateway_sources()==[source]
        with ledger._connect() as conn:
            rows=conn.execute('SELECT state FROM delivery_obligations WHERE source_binding IS NOT NULL').fetchall()
        assert [row[0] for row in rows]==([] if state is None else [state])
        if state in ('unknown','delivered'):
            await drain_gateway_sources(r.runner)
            assert r.adapter.send.await_count==(0 if boundary=='before_send' else 1)
            with ledger._connect() as conn:
                assert conn.execute('SELECT state FROM delivery_obligations WHERE source_binding IS NOT NULL').fetchone()[0]==state
    finally:
        r.db.close();await r.adapter._client.close()


@pytest.mark.asyncio
async def test_queued_and_already_delivered_skip_emit_no_error(tmp_path,monkeypatch,caplog):
    r=rig(tmp_path,monkeypatch)
    try:
        source=await execute(r,monkeypatch,current_gateway_source)
        r.provider.read_result=AsyncMock(return_value={'state':'queued','accepted_source':source})
        r.provider.bind_gateway_receipt=AsyncMock()
        r.adapter.send=AsyncMock()
        await drain_gateway_sources(r.runner)
        pending=ledger.source_obligation(source,'already-returned','Synthetic result',{})
        assert ledger.begin_source_send(pending['obligation_id'])
        assert ledger.acknowledge_source_send(pending['obligation_id'],'synthetic-existing-ack')
        await drain_gateway_sources(r.runner)
        assert ledger.source_delivery_receipt(source,'already-returned')['native_message_id']=='synthetic-existing-ack'
        assert not [record for record in caplog.records if record.name=='gateway.source_delivery']
        r.adapter.send.assert_not_awaited()
        assert r.db.pending_gateway_sources()==[source]
    finally:
        r.db.close();await r.adapter._client.close()


@pytest.mark.asyncio
async def test_native_ack_restart_unknown_and_late_owner_never_blindly_resend(tmp_path, monkeypatch):
    r=rig(tmp_path,monkeypatch)
    try:
        source=await execute(r,monkeypatch,current_gateway_source)
        delivery={'state':'answered','accepted_source':source,'identity':'synthetic-result',
            'content':'Synthetic result','metadata':{'delegation_id':'synthetic-result','source_run_id':source['run_id']},'display_kind':'source_result'}
        r.provider.read_result=AsyncMock(return_value=delivery)
        bindings=[]
        async def bind(source,receipt):
            bindings.append(receipt)
            if len(bindings)==1:raise ValueError('lost central acknowledgement')
        r.provider.bind_gateway_receipt=bind
        r.adapter.send=AsyncMock(return_value=SendResult(success=True,message_id='native-555'))
        r.adapter._running=False
        await drain_gateway_sources(r.runner)
        r.adapter.send.assert_not_awaited()
        assert ledger.source_obligation(source,delivery['identity'],delivery['content'],delivery['metadata'])['state']=='pending'
        r.adapter._running=True
        await drain_gateway_sources(r.runner)
        assert r.adapter.send.await_count == 1
        assert r.adapter.send.call_args.kwargs['metadata']['thread_id']=='222'
        r.db.close();r.db=SessionDB(db_path=tmp_path/'state.db');r.runner._session_db=r.db
        await drain_gateway_sources(r.runner)
        assert r.adapter.send.await_count == 1
        assert bindings[0]==bindings[1] and bindings[1]['native_message_id']=='native-555'
        assert r.db.pending_gateway_sources()==[]
        # A separate delivery identity represents an authorized result slot, not
        # another model execution. Crash/lost ACK stays unknown across restart.
        pending=ledger.source_obligation(source,'unknown-result','Unknown',{'delegation_id':'unknown-result'})
        assert ledger.begin_source_send(pending['obligation_id'])
        assert not ledger.begin_source_send(pending['obligation_id'])
        assert ledger.source_delivery_receipt(source,'unknown-result') is None
        assert ledger.sweep_recoverable()==[]
        assert not ledger.acknowledge_source_send('foreign-id','native-666')
        with pytest.raises(ValueError):
            ledger.source_obligation(source,'unknown-result','Different',{})
        # Late original owner after a replaced adapter must not bind a native ACK.
        with ledger._connect() as conn:
            conn.execute('UPDATE delivery_obligations SET owner_pid=-1 WHERE obligation_id=?',(pending['obligation_id'],))
        assert not ledger.acknowledge_source_send(pending['obligation_id'],'late-ack')
        late_home=tmp_path/'late';late_home.mkdir()
        late=rig(late_home,monkeypatch)
        try:
            late_source=await execute(late,monkeypatch,current_gateway_source)
            late.provider.read_result=AsyncMock(return_value={**delivery,'accepted_source':late_source,
                'metadata':{**delivery['metadata'],'source_run_id':late_source['run_id']}})
            late.provider.bind_gateway_receipt=AsyncMock()
            async def replaced_while_sending(**kwargs):
                late.runner.adapters[Platform.DISCORD]=r.adapter
                return SendResult(success=True,message_id='late-native-ack')
            late.adapter.send=AsyncMock(side_effect=replaced_while_sending)
            await drain_gateway_sources(late.runner)
            late.provider.bind_gateway_receipt.assert_not_awaited()
            assert ledger.source_delivery_receipt(late_source,delivery['identity']) is None
            # Rebind the owning adapter after an A→B→A replacement. The
            # previous unknown send remains held; no blind duplicate.
            late.runner.adapters[Platform.DISCORD]=late.adapter
            await drain_gateway_sources(late.runner)
            assert late.adapter.send.await_count==1
            late.provider.bind_gateway_receipt.assert_not_awaited()
        finally:
            late.db.close();await late.adapter._client.close()
        revoked_home=tmp_path/'revoked';revoked_home.mkdir()
        revoked=rig(revoked_home,monkeypatch)
        try:
            revoked_source=await execute(revoked,monkeypatch,current_gateway_source)
            async def revoke_during_read(source):
                monkeypatch.setenv('DISCORD_ALLOWED_USERS','999')
                return {**delivery,'accepted_source':revoked_source,
                    'metadata':{**delivery['metadata'],'source_run_id':revoked_source['run_id']}}
            revoked.provider.read_result=revoke_during_read
            revoked.provider.bind_gateway_receipt=AsyncMock()
            revoked.adapter.send=AsyncMock()
            await drain_gateway_sources(revoked.runner)
            revoked.adapter.send.assert_not_awaited()
            revoked.provider.bind_gateway_receipt.assert_not_awaited()
        finally:
            revoked.db.close();await revoked.adapter._client.close()
    finally:
        r.db.close();await r.adapter._client.close()


@pytest.mark.asyncio
async def test_held_sources_rotate_without_revoking_custody_or_resending(tmp_path, monkeypatch):
    r=rig(tmp_path,monkeypatch)
    try:
        source=await execute(r,monkeypatch,current_gateway_source)
        # Persist accepted custody on existing user rows, not a mock pending query.
        for number in range(101):
            held=dict(source,run_id=f'held-{number}',message_id=f'held-message-{number}',user_id='999')
            r.db.append_message('source-session','user','Synthetic held event',
                platform_message_id=held['message_id'],display_metadata={'gateway_accepted_source':held})
            obligation=ledger.source_obligation(held,'held-result','Held',{})
            assert ledger.begin_source_send(obligation['obligation_id'])
        # Put the deliverable source after the held page, preserving its exact snapshot.
        r.db._execute_write(lambda conn:conn.execute("UPDATE messages SET id=id+1000 WHERE platform_message_id='444'"))
        r.provider.read_result=AsyncMock(return_value={'state':'answered','accepted_source':source,
            'identity':'later-result','content':'Synthetic result','metadata':{'delegation_id':'later-result','source_run_id':source['run_id']},
            'display_kind':'source_result'})
        r.provider.bind_gateway_receipt=AsyncMock()
        r.adapter.send=AsyncMock(return_value=SendResult(success=True,message_id='later-ack'))
        await drain_gateway_sources(r.runner)
        r.db.close();r.db=SessionDB(db_path=tmp_path/'state.db');r.runner._session_db=r.db
        await drain_gateway_sources(r.runner)
        assert r.adapter.send.await_count==1, 'held first page must not starve a later source'
        assert r.db.gateway_source_row(source)
        pending=r.db.pending_gateway_sources()
        assert all(s['user_id']=='999' for s in pending)
        with ledger._connect() as conn:
            assert conn.execute("SELECT count(*) FROM delivery_obligations WHERE state='unknown'").fetchone()[0]==101
        r.provider.read_result.assert_awaited_once()  # revoked actors were never read or sent
    finally:
        r.db.close();await r.adapter._client.close()


@pytest.mark.asyncio
async def test_native_sdk_managed_role_mention_is_human_summons_only(tmp_path,monkeypatch):
    """Real discord.py role/tag/message objects; no network or live original-message replay."""
    r=rig(tmp_path,monkeypatch)
    try:
        state=r.adapter._client._connection
        state.user=discord.ClientUser(state=state,data={'id':'999','username':'Mara','discriminator':'0','avatar':None})
        message=r.event.raw_message
        guild=message.guild
        role=discord.Role(guild=guild,state=state,data={'id':'555','name':'Mara','managed':True,
            'permissions':'0','position':1,'color':0,'hoist':False,'mentionable':True,'tags':{'bot_id':'999'}})
        guild._roles[role.id]=role
        message.content='<@&555> Ask support for today’s status. Do not send customer replies.'
        message.mentions=[];message.role_mentions=[role]
        assert r.adapter._self_is_explicitly_mentioned(message)
        assert not r.adapter._self_is_raw_mentioned(message)
        r.adapter._allowed_user_ids={str(message.author.id)}
        assert r.adapter._discord_message_admission(message,claim=False)[0]
        r.adapter._allowed_user_ids={'foreign-actor'}
        assert not r.adapter._discord_message_admission(message,claim=False)[0]
        role.tags.bot_id=111
        assert not r.adapter._self_is_explicitly_mentioned(message)
        role.tags.bot_id=999
        # The real normalizer must preserve control-command semantics for the
        # newly admitted addressing form, rather than forwarding it to a model.
        r.adapter._allowed_user_ids={str(message.author.id)}
        message.content='<@&555> /stop'
        captured=[]
        async def capture(event):captured.append(event)
        monkeypatch.setattr(r.adapter,'handle_message',capture)
        r.adapter.config.extra['history_backfill']=False
        await r.adapter._threads.mark_async(str(message.channel.id))
        assert await r.adapter._handle_message(message)
        assert len(captured)==1
        assert captured[0].text=='/stop'
        assert captured[0].is_command()
        message.content='<@&555> Ask support for status'
        message.author.bot=True
        assert not r.adapter._self_is_explicitly_mentioned(message)
    finally:
        r.db.close();await r.adapter._client.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('revoked',[False,True])
async def test_slow_thread_survives_consumer_timeout_without_duplicate_read(tmp_path,monkeypatch,revoked):
    import threading
    import gateway.source_delivery as owner
    r=rig(tmp_path,monkeypatch)
    release=threading.Event();entered=threading.Event();calls=[]
    try:
        source=await execute(r,monkeypatch,current_gateway_source)
        delivery=dict(state='answered',accepted_source=source,identity='slow-source',content='Synthetic result',
            metadata=dict(delegation_id='slow-source',source_run_id=source['run_id']),display_kind='source_result')
        def slow():
            calls.append(1);entered.set();release.wait(10);return delivery
        async def read(source):return await asyncio.to_thread(slow)
        r.provider.read_result=read
        r.provider.bind_gateway_receipt=AsyncMock(return_value=False)
        r.adapter.send=AsyncMock(return_value=SendResult(success=True,message_id='slow-ack'))
        monkeypatch.setattr(owner,'_DRAIN_WAIT_SECONDS',0.02,raising=False)
        await owner.drain_gateway_sources(r.runner)
        assert await asyncio.to_thread(entered.wait,5)
        waiter=asyncio.create_task(owner.drain_gateway_sources(r.runner))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):await waiter
        await owner.drain_gateway_sources(r.runner)
        assert len(calls)==1
        if revoked:monkeypatch.setattr(r.runner,'_is_user_authorized_for_source',lambda *a,**kw:False)
        release.set()
        await asyncio.wait_for(asyncio.shield(r.runner._source_drain_task),5)
        assert r.adapter.send.await_count==(0 if revoked else 1)
        assert r.provider.bind_gateway_receipt.await_count==(0 if revoked else 1)
    finally:
        release.set()
        task=getattr(r.runner,'_source_drain_task',None)
        if task:await asyncio.gather(task,return_exceptions=True)
        r.db.close();await r.adapter._client.close()
