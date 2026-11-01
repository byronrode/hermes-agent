"""Accepted source integrations must use the native scoped plugin lifecycle."""
from types import SimpleNamespace
from unittest.mock import patch

from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest


def test_external_source_provider_registration_is_profile_scoped_and_unloads(tmp_path):
    async def read_result(source):
        return {'state': 'unavailable', 'accepted_source': source}
    provider = SimpleNamespace(name='external-source', qualify=lambda **kw: {'intent':'synthetic'}, read_result=read_result)
    owners = [PluginManager(scope_key=str(tmp_path / name)) for name in ('a','b')]
    for manager in owners:
        manager._discovered = True
    context = PluginContext(PluginManifest(name='external-integration'), owners[0])
    handle = context.register_api_source_provider(provider)
    from gateway.platforms.api_server_source_receipts import source_provider
    adapter = SimpleNamespace(config=SimpleNamespace(extra={'accepted_source_provider':'external-source'}))
    for manager, expected in ((owners[0],provider),(owners[1],None),(owners[0],provider)):
        with patch('hermes_cli.plugins.get_plugin_manager', return_value=manager):
            assert source_provider(adapter) is expected
    assert owners[0].unload()
    assert not handle.active
    owners[0]._discovered = True
    with patch('hermes_cli.plugins.get_plugin_manager', return_value=owners[0]):
        assert source_provider(adapter) is None
