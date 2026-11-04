"""Minimal invalidations retained by native run/cron mutation owners, not a new store."""
import hashlib
import json
import time
import uuid
import sqlite3


def change(resource, identity, **scope):
    return dict(event_id=uuid.uuid4().hex,at=time.time_ns(),resource=resource,id=identity,**scope)


def replay(records, cursor, namespace):
    genesis=hashlib.sha256(('resource-events-v1:'+namespace).encode()).hexdigest()
    chain=genesis;indexed=[]
    for record in sorted(records,key=lambda e:(e['at'],e['event_id'])):
        chain=hashlib.sha256((chain+json.dumps(record,sort_keys=True)).encode()).hexdigest()
        indexed.append((chain,record))
    if cursor is None:return chain,[]
    if cursor==genesis:return chain,indexed
    for index,(token,_) in enumerate(indexed):
        if token==cursor:return chain,indexed[index+1:]
    raise ValueError('cursor_reset_required')


async def resource_events(owner,request):
    from aiohttp import web
    error=owner._check_auth(request)
    if error:return error
    from gateway.platforms.api_server import _api_request_profile
    # First supported mobile surface. No profile-selection query or broad key proxy.
    if (_api_request_profile.get() or 'default')!='default' or owner._room_grant_token(request):
        return web.json_response({'error':'resource_scope_unavailable'},status=403)
    if set(request.query)-{'cursor','limit'}:return web.json_response({'error':'unsupported_options'},status=400)
    try:
        limit=int(request.query.get('limit','100'))
        if not 1<=limit<=100:raise ValueError('invalid_limit')
        store=owner._run_idempotency_store
        if not store.durable:raise OSError('durable_owner_unavailable')
        scope=owner._run_idempotency_scope(request)
        from cron.jobs_resource_events import resource_changes
        records=resource_changes()+store.resource_changes(scope)
        if len(records)>10000:raise OSError('bounded_feed_unavailable')
        try:cursor,events=replay(records,request.query.get('cursor'),scope)
        except ValueError:return web.json_response({'error':'cursor_reset_required'},status=409)
        events=events[:limit]
        if events:cursor=events[-1][0]
        # Auth may change while native owners are being read.
        error=owner._check_auth(request)
        if error:return error
        return web.json_response({'version':1,'cursor':cursor,'coverage':['automations','runs'],
            'events':[{'cursor':token,'resource':e['resource'],'id':e['id']} for token,e in events]})
    except (OSError,RuntimeError,ValueError,KeyError,TypeError,sqlite3.Error,ImportError):
        return web.json_response({'error':'resource_owner_unavailable'},status=503)
