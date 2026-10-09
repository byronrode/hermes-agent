"""Renew Slack app-level credentials in the existing private Hermes auth store."""
import asyncio
import time
from pathlib import Path

from hermes_cli.auth import _auth_store_lock, _load_auth_store, _save_auth_store
from hermes_constants import get_hermes_home


class AppTokenRotation:
    def __init__(self):
        self.path = Path(get_hermes_home()) / "auth.json"
        self.lock = asyncio.Lock()

    def state(self):
        with _auth_store_lock(target_path=self.path):
            return dict(_load_auth_store(self.path).get("slack_app_rotation") or {})

    def configure(self, access_token, refresh_token, client_id, client_secret):
        if not access_token.startswith("xoxe.xapp-") or not refresh_token.startswith("xoxe-"):
            raise ValueError("Expected a rotating app access token and its refresh token")
        if not client_id or not client_secret:
            raise ValueError("Slack client ID and client secret are required")
        with _auth_store_lock(target_path=self.path):
            store = _load_auth_store(self.path)
            store["slack_app_rotation"] = {
                "access_token": access_token, "refresh_token": refresh_token,
                "client_id": client_id, "client_secret": client_secret,
                # Slack's UI does not expose issuance expiry to the setup wizard.
                # Refresh once on connection to establish a known expiry.
                "expires_at": 0,
            }
            _save_auth_store(store, self.path)

    async def token(self, client):
        async with self.lock:
            state = self.state()
            if not state:
                raise RuntimeError("Rotating Slack app token needs refresh credentials; run hermes gateway setup")
            if state.get("expires_at", 0) > time.time() + 600:
                return state["access_token"]
            try:
                response = await client.oauth_v2_access(
                    client_id=state["client_id"], client_secret=state["client_secret"],
                    grant_type="refresh_token", refresh_token=state["refresh_token"])
            except Exception:
                # SDK errors may include the OAuth response: never put it in logs.
                raise RuntimeError("Slack app-token renewal failed") from None
            if (not response.get("ok") or response.get("token_type") != "app_level"
                    or not str(response.get("access_token", "")).startswith("xoxe.xapp-")
                    or not str(response.get("refresh_token", "")).startswith("xoxe-")
                    or not isinstance(response.get("expires_in"), (int, float))
                    or response["expires_in"] <= 600):
                raise RuntimeError("Slack returned an invalid app-token renewal")
            state.update(access_token=response["access_token"],
                         refresh_token=response["refresh_token"],
                         expires_at=time.time() + response["expires_in"])
            # Persist both single-use refresh and access credentials atomically,
            # retaining unrelated provider credentials and binding the owning home.
            with _auth_store_lock(target_path=self.path):
                store = _load_auth_store(self.path)
                store["slack_app_rotation"] = state
                _save_auth_store(store, self.path)
            return state["access_token"]
