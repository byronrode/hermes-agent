import io
import json
import urllib.error
import urllib.request

import pytest

from hermes_cli.setup_whatsapp_cloud import run_whatsapp_cloud_registration


@pytest.mark.parametrize("failure", [False, True])
def test_registration_verifies_outcome_and_redacts_credentials(monkeypatch, tmp_path, capsys, failure):
    from hermes_cli.config import save_env_value
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    token, pin = "EAA-test-private-token", "357159"
    save_env_value("WHATSAPP_CLOUD_PHONE_NUMBER_ID", "1312743238595423")
    save_env_value("WHATSAPP_CLOUD_ACCESS_TOKEN", token)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("getpass.getpass", lambda prompt: pin)
    calls = []

    def respond(request, timeout):
        calls.append(request)
        if request.data:
            assert json.loads(request.data) == {"messaging_product": "whatsapp", "pin": pin}
            assert request.get_header("Authorization") == "Bearer " + token
            if failure:
                body = {"error": {"code": 133005, "message": f"Rejected {token} PIN {pin}"}}
                raise urllib.error.HTTPError(request.full_url, 400, "Bad request", {},
                                              io.BytesIO(json.dumps(body).encode()))
            result = {"success": True}
        else:
            result = {"platform_type": "CLOUD_API" if len(calls) > 1 else "NOT_APPLICABLE",
                      "code_verification_status": "VERIFIED"}
        return io.BytesIO(json.dumps(result).encode())

    monkeypatch.setattr(urllib.request, "urlopen", respond)
    assert run_whatsapp_cloud_registration(register=True) == (1 if failure else 0)
    output = capsys.readouterr().out
    assert token not in output and pin not in output
    assert pin not in (tmp_path / ".env").read_text()
    assert "133005" in output if failure else '"CLOUD_API"' in output


def test_registered_number_never_prompts_or_posts(monkeypatch, tmp_path):
    from hermes_cli.config import save_env_value
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    save_env_value("WHATSAPP_CLOUD_PHONE_NUMBER_ID", "1312743238595423")
    save_env_value("WHATSAPP_CLOUD_ACCESS_TOKEN", "EAA-test")
    def respond(request, timeout):
        assert request.data is None
        return io.BytesIO(b'{"platform_type":"CLOUD_API"}')
    monkeypatch.setattr(urllib.request, "urlopen", respond)
    assert run_whatsapp_cloud_registration(register=True) == 0
