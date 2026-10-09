"""Rotating app credentials survive restarts without crossing profile homes."""
import asyncio
import stat

import pytest

from hermes_cli.auth import _auth_store_lock, _load_auth_store, _save_auth_store
from plugins.platforms.slack.token_rotation import AppTokenRotation


class RenewalClient:
    def __init__(self):
        self.used = []

    async def oauth_v2_access(self, **kwargs):
        self.used.append(kwargs["refresh_token"])
        n = len(self.used)
        return {"ok": True, "token_type": "app_level", "access_token": f"xoxe.xapp-access-{n}",
                "refresh_token": f"xoxe-refresh-{n}", "expires_in": 43200}


def test_rotation_persists_new_refresh_and_isolates_profiles(tmp_path, monkeypatch):
    async def run():
        home_a, home_b = tmp_path / "a", tmp_path / "b"
        monkeypatch.setenv("HERMES_HOME", str(home_a))
        a = AppTokenRotation()
        a.configure("xoxe.xapp-original", "xoxe-original", "client-a", "secret-a")
        with _auth_store_lock(target_path=a.path):
            store = _load_auth_store(a.path)
            store["providers"]["unrelated"] = {"keep": True}
            _save_auth_store(store, a.path)
        client = RenewalClient()
        assert await a.token(client) == "xoxe.xapp-access-1"
        assert a.state()["refresh_token"] == "xoxe-refresh-1"
        assert stat.S_IMODE(a.path.stat().st_mode) == 0o600
        monkeypatch.setenv("HERMES_HOME", str(home_b))
        b = AppTokenRotation()
        assert b.state() == {}
        with pytest.raises(RuntimeError, match="refresh credentials"):
            await b.token(client)
        # The owning adapter stays bound to A even when ambient scope changes.
        assert await a.token(client) == "xoxe.xapp-access-1"
        monkeypatch.setenv("HERMES_HOME", str(home_a))
        restarted = AppTokenRotation()
        assert await restarted.token(client) == "xoxe.xapp-access-1"
        with _auth_store_lock(target_path=a.path):
            store = _load_auth_store(a.path)
            store["slack_app_rotation"]["expires_at"] = 0
            _save_auth_store(store, a.path)
        assert await restarted.token(client) == "xoxe.xapp-access-2"
        assert client.used == ["xoxe-original", "xoxe-refresh-1"]
        assert _load_auth_store(a.path)["providers"]["unrelated"] == {"keep": True}
    asyncio.run(run())


def test_failed_refresh_retains_credentials_without_disclosing_error(tmp_path, monkeypatch):
    class FailedClient:
        async def oauth_v2_access(self, **kwargs):
            raise RuntimeError("sensitive-response-data")
    async def run():
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        rotation = AppTokenRotation()
        rotation.configure("xoxe.xapp-original", "xoxe-original", "client", "secret")
        before = rotation.state()
        with pytest.raises(RuntimeError) as failure:
            await rotation.token(FailedClient())
        assert "sensitive-response-data" not in str(failure.value)
        assert rotation.state() == before
    asyncio.run(run())
