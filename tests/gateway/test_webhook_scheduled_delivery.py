"""Scheduled signed webhooks retain profile ownership and the adapter's template rail."""
import hashlib
import hmac
import json
import re
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway import whatsapp_chat_window as windows
from gateway.authz_mixin import GatewayAuthorizationMixin
from gateway.config import GatewayConfig, HomeChannel, Platform, PlatformConfig
from gateway.platforms.base import SendResult
from gateway.platforms.webhook import WebhookAdapter
from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter


class Runner(GatewayAuthorizationMixin):
    def __init__(self, default, secondary, config):
        self.adapters = {Platform.WHATSAPP_CLOUD: default}
        self._profile_adapters = {"sec": {Platform.WHATSAPP_CLOUD: secondary}}
        self._primary_profile_name = "default"
        self.config = config


@pytest.fixture
def multiplex_scope(tmp_path):
    from agent.secret_scope import set_multiplex_active, set_secret_scope, reset_secret_scope
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    home = tmp_path / ".hermes"
    with ExitStack() as scope:
        scope.callback(reset_hermes_home_override, set_hermes_home_override(home))
        scope.callback(reset_secret_scope, set_secret_scope({}, profile_home=str(home)))
        set_multiplex_active(True)
        try:
            yield
        finally:
            set_multiplex_active(False)


@pytest.mark.asyncio
async def test_signed_scheduled_delivery_uses_profile_template_and_preserves_long_open_scope(tmp_path, monkeypatch, multiplex_scope):
    home = tmp_path / ".hermes"
    secondary_home = home / "profiles" / "sec"
    secondary_home.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr("hermes_cli.profiles._get_default_hermes_home", lambda: home)
    monkeypatch.setattr("hermes_cli.profiles._get_profiles_root", lambda: home / "profiles")
    (secondary_home / "config.yaml").write_text("platforms:\n  whatsapp_cloud:\n    enabled: true\n    home_channel:\n      platform: whatsapp_cloud\n      chat_id: secondary-chat\n")
    secret = "synthetic-scheduled-webhook-key"
    route = {"secret": secret, "deliver": "whatsapp_cloud", "deliver_only": True,
             "prompt": "{message}", "deliver_extra": {"job_id": "{event_id}", "job_name": "Weekly report"}}
    webhook = WebhookAdapter(PlatformConfig(enabled=True, extra={"host": "127.0.0.1", "port": 0,
        "routes": {"report": route, "secondary-report": {**route, "profile": "sec"}}}))
    def target(name):
        adapter = WhatsAppCloudAdapter(PlatformConfig(extra={"phone_number_id": name, "reply_prefix": "",
            "scheduled_template": {"name": name + "_requested_reminder", "language": "en"}}))
        adapter._http_client = MagicMock()
        adapter._http_client.post = AsyncMock(return_value=httpx.Response(200, json={"messages": [{"id": "wamid.synthetic"}]}))
        return adapter
    default = target("default")
    with webhook._profile_scope("sec"):
        secondary = target("secondary")
    config = GatewayConfig()
    config.multiplex_profiles = True
    config.platforms[Platform.WHATSAPP_CLOUD] = PlatformConfig(enabled=True,
        home_channel=HomeChannel(platform=Platform.WHATSAPP_CLOUD, chat_id="default-chat", name="Home"))
    webhook.gateway_runner = Runner(default, secondary, config)
    app = web.Application()
    app.router.add_post("/webhooks/{route_name}", webhook._handle_webhook)
    app.router.add_post("/p/{profile}/webhooks/{route_name}", webhook._handle_webhook)
    words = [f"scope{i:04d}" for i in range(800)]
    async with TestClient(TestServer(app)) as client:
        for index, (profile, adapter, chat, content, opened) in enumerate([
            (None, default, "default-chat", "Report available: https://example.test/report", False),
            ("sec", secondary, "secondary-chat", "Another report: https://example.test/report", False),
            (None, default, "default-chat", " ".join(words), True),
        ]):
            windows.record_inbound(adapter._window_home, adapter._phone_number_id, chat,
                                   int(windows.time.time()) - (1 if opened else 86401))
            adapter._http_client.post.reset_mock()
            payload = json.dumps({"message": content, "event_id": f"report-{index}"}).encode()
            path = "/p/sec/webhooks/secondary-report" if profile else "/webhooks/report"
            response = await client.post(path, data=payload, headers={"Content-Type": "application/json",
                "X-Hub-Signature-256": "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest(),
                "X-GitHub-Delivery": f"report-{index}"})
            assert response.status == 200
            assert (await response.json())["status"] == "delivered"
            sent = [call.kwargs["json"] for call in adapter._http_client.post.call_args_list]
            assert all(part["to"] == chat for part in sent)
            if not opened:
                assert len(sent) == 1 and sent[0]["type"] == "template"
                assert sent[0]["template"]["name"] == adapter._phone_number_id + "_requested_reminder"
            else:
                assert len(sent) > 1
                assert all(part["type"] == "text" and len(part["text"]["body"]) <= 4096 for part in sent)
                assert re.findall(r"scope\d{4}", " ".join(part["text"]["body"] for part in sent)) == words
    assert default._window_home != secondary._window_home


@pytest.mark.asyncio
async def test_scheduled_metadata_is_opt_in_and_invalid_metadata_never_sends():
    target = MagicMock()
    target.send = AsyncMock(return_value=SendResult(success=True))
    config = GatewayConfig()
    webhook = WebhookAdapter(PlatformConfig(enabled=True, extra={"host": "127.0.0.1", "port": 0, "routes": {}}))
    webhook.gateway_runner = Runner(target, target, config)
    for extra in ({"job_id": []}, {"job_id": " "}, {"job_id": "x" * 161}, {"job_name": "orphan"}):
        result = await webhook._deliver_cross_platform("whatsapp_cloud", "notice", {"deliver_extra": {"chat_id": "synthetic", **extra}})
        assert not result.success
    target.send.assert_not_awaited()
    assert (await webhook._deliver_cross_platform("whatsapp_cloud", "ordinary reply", {"deliver_extra": {"chat_id": "synthetic"}})).success
    target.send.assert_awaited_once_with("synthetic", "ordinary reply", metadata=None)
