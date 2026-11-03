"""Consume accepted messaging sources through existing adapters and delivery obligations."""
import asyncio
from contextvars import copy_context
import time
from types import SimpleNamespace
from gateway.config import Platform
from gateway.session import SessionSource
from gateway import delivery_ledger as ledger


def _source_route(runner, db, source):
    from hermes_constants import get_hermes_home
    from gateway.accepted_source import _digest
    if (source.get('platform') != 'discord' or source.get('profile') != 'default'
            or source.get('home_scope') != _digest(str(get_hermes_home().resolve()))):
        raise ValueError('gateway_source_profile_unavailable')
    db.gateway_source_row(source)
    origin = SessionSource(platform=Platform.DISCORD,
        chat_id=source['channel_id'],thread_id=source['thread_id'] or None,
        scope_id=source['guild_id'] or None,user_id=source['user_id'],profile=source['profile'],
        chat_type=source['chat_type'],parent_chat_id=source['parent_channel_id'] or None)
    restored = runner._restored_source(SimpleNamespace(origin=origin,transport_profile=source['transport_profile']))
    if not runner._is_user_authorized_for_source(restored, allow_adapter_delegation=False):
        raise ValueError('gateway_source_authorization_revoked')
    adapter = runner._delivery_adapter_for(restored)
    return restored, adapter if adapter is not None and adapter.is_connected is True else None


async def drain_gateway_sources(runner):
    from hermes_cli.plugins import get_plugin_manager
    if getattr(runner,'_primary_profile_name',None) != 'default' or not getattr(runner,'_session_db',None):
        return
    db = getattr(runner._session_db,'_db',runner._session_db)
    manager = get_plugin_manager()
    deadline = time.monotonic() + 30
    for source in await asyncio.to_thread(db.pending_gateway_sources):
        if time.monotonic() >= deadline:
            break
        if not isinstance(source,dict):
            continue
        try:
            await asyncio.to_thread(db.mark_gateway_source_polled,source)
        except (ValueError,KeyError,TypeError):
            continue
        provider = manager._api_source_providers.get(source.get('provider'))
        consume = getattr(provider,'bind_gateway_receipt',None)
        if not provider or not callable(consume):
            continue
        try:
            restored, adapter = _source_route(runner,db,source)
            if callable(getattr(provider,'retry_gateway_intake',None)):
                # An opted-in provider may repair its exact persisted failed
                # intake under the same source custody. No model/event replay.
                from gateway.source_intake_recovery import retry_failed_intake
                try:
                    from gateway.platforms.api_server_runs import _submit_api_worker
                    context = copy_context()
                    work = _submit_api_worker(asyncio.get_running_loop(),
                        lambda: context.run(retry_failed_intake,runner,source['message_id']))
                    await asyncio.shield(work)
                except (ValueError,RuntimeError,OSError,KeyError,TypeError):
                    # Unqualified/live/exhausted sources cannot be retried;
                    # an already captured result still uses ordinary readback.
                    pass
            from gateway.platforms.api_server_source_receipts import _delivery
            result = await asyncio.wait_for(provider.read_result(source),timeout=min(
                30,max(0.1,float(getattr(provider,'read_timeout_seconds',3.5)))))
            delivery = _delivery(result,source)
            if delivery is None:
                continue
            _, live = _source_route(runner,db,source)
            if (live is not adapter or get_plugin_manager() is not manager
                    or manager._api_source_providers.get(source['provider']) is not provider):
                continue
            # Exact OS commit remains separate from platform ACK; retries use
            # the same detached row and cannot start a model turn.
            session = await asyncio.to_thread(db.resolve_resume_session_id,source['session_id']) or source['session_id']
            row = await asyncio.to_thread(db.append_source_result_delivery,session,
                delivery['content'],delivery['metadata'],display_kind=delivery['display_kind'])
            pending = await asyncio.to_thread(ledger.source_obligation,source,delivery['identity'],
                delivery['content'],delivery['metadata'])
            if (adapter is not None and pending['state'] == 'pending'
                    and await asyncio.to_thread(ledger.begin_source_send,pending['obligation_id'])):
                _, live = _source_route(runner,db,source)
                if (not getattr(runner,'_running',False) or live is not adapter
                        or get_plugin_manager() is not manager
                        or manager._api_source_providers.get(source['provider']) is not provider):
                    continue
                # No retry wrapper: Discord's unknown ACK must remain unknown.
                result = await asyncio.wait_for(adapter.send(chat_id=source['channel_id'],content=delivery['content'],
                    reply_to=source['message_id'],metadata={'thread_id':source['thread_id'] or None}),timeout=30)
                _, live = _source_route(runner,db,source)
                if (not getattr(runner,'_running',False) or live is not adapter or get_plugin_manager() is not manager
                        or manager._api_source_providers.get(source['provider']) is not provider):
                    continue
                if result.success and result.message_id:
                    await asyncio.to_thread(ledger.acknowledge_source_send,pending['obligation_id'],str(result.message_id))
            receipt = await asyncio.to_thread(ledger.source_delivery_receipt,source,delivery['identity'])
            if receipt:
                receipt['message_id'] = row
                consumed = await asyncio.wait_for(consume(source,receipt),timeout=30)
                if consumed is not False:  # a qualified interim notice keeps the final obligation open
                    await asyncio.to_thread(db.consume_gateway_source,source)
        except (OSError,ValueError,KeyError,TimeoutError,RuntimeError):
            # Existing durable user row/outbox retain this obligation for the
            # next tick. Unknown attempts are never automatically resent.
            continue
