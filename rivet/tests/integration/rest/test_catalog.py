"""Integration tests for RestApiCatalogPlugin."""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import Catalog
from rivet_rest.catalog import RestApiCatalogPlugin


@pytest.fixture()
def plugin() -> RestApiCatalogPlugin:
    return RestApiCatalogPlugin()


def _valid_options(**overrides: object) -> dict[str, object]:
    opts: dict[str, object] = {
        "base_url": "https://api.example.com/v1",
        "endpoints": {
            "users": {"path": "/users"},
            "orders": {"path": "/orders"},
        },
    }
    opts.update(overrides)
    return opts


class TestValidate:
    def test_valid_config(self, plugin: RestApiCatalogPlugin) -> None:
        plugin.validate(_valid_options())

    def test_missing_base_url(self, plugin: RestApiCatalogPlugin) -> None:
        with pytest.raises(PluginValidationError, match="base_url is required"):
            plugin.validate({"endpoints": {"users": {"path": "/users"}}})

    def test_empty_base_url(self, plugin: RestApiCatalogPlugin) -> None:
        with pytest.raises(PluginValidationError, match="base_url is required"):
            plugin.validate({"base_url": "", "endpoints": {}})

    def test_invalid_url_scheme(self, plugin: RestApiCatalogPlugin) -> None:
        with pytest.raises(PluginValidationError, match="http:// or https://"):
            plugin.validate({"base_url": "ftp://example.com"})

    def test_endpoint_missing_path(self, plugin: RestApiCatalogPlugin) -> None:
        with pytest.raises(PluginValidationError, match="missing required 'path'"):
            plugin.validate(
                {
                    "base_url": "https://api.example.com",
                    "endpoints": {"bad": {"method": "GET"}},
                }
            )

    def test_invalid_auth_type(self, plugin: RestApiCatalogPlugin) -> None:
        with pytest.raises(PluginValidationError, match="Unknown auth type"):
            plugin.validate(_valid_options(auth="kerberos"))

    def test_invalid_response_format(self, plugin: RestApiCatalogPlugin) -> None:
        with pytest.raises(PluginValidationError, match="Unsupported response_format"):
            plugin.validate(_valid_options(response_format="xml"))


class TestListTables:
    def test_returns_catalog_nodes(self, plugin: RestApiCatalogPlugin) -> None:
        catalog = Catalog(
            name="my_api",
            type="rest_api",
            options=_valid_options(),
        )
        nodes = plugin.list_tables(catalog)
        assert len(nodes) == 2
        names = {n.name for n in nodes}
        assert names == {"users", "orders"}
        for node in nodes:
            assert node.node_type == "endpoint"
            assert node.is_container is False

    def test_empty_endpoints(self, plugin: RestApiCatalogPlugin) -> None:
        catalog = Catalog(
            name="my_api",
            type="rest_api",
            options={"base_url": "https://api.example.com", "endpoints": {}},
        )
        assert plugin.list_tables(catalog) == []


class TestDefaultTableReference:
    def test_returns_name_unchanged(self, plugin: RestApiCatalogPlugin) -> None:
        assert plugin.default_table_reference("orders", {}) == "orders"
        assert plugin.default_table_reference("my_table", {}) == "my_table"


class TestListChildren:
    def test_empty_path_matches_list_tables(self, plugin: RestApiCatalogPlugin) -> None:
        catalog = Catalog(
            name="my_api",
            type="rest_api",
            options=_valid_options(),
        )
        tables = plugin.list_tables(catalog)
        children = plugin.list_children(catalog, [])
        assert len(tables) == len(children)
        assert {n.name for n in tables} == {n.name for n in children}

    def test_non_empty_path_returns_empty(self, plugin: RestApiCatalogPlugin) -> None:
        catalog = Catalog(
            name="my_api",
            type="rest_api",
            options=_valid_options(),
        )
        assert plugin.list_children(catalog, ["users"]) == []


class TestGetMetadata:
    def test_returns_metadata_with_location(self, plugin: RestApiCatalogPlugin) -> None:
        catalog = Catalog(
            name="my_api",
            type="rest_api",
            options=_valid_options(),
        )
        meta = plugin.get_metadata(catalog, "users")
        assert meta is not None
        assert meta.format == "json"
        assert "users" in (meta.location or "")

    def test_unknown_endpoint_returns_none(self, plugin: RestApiCatalogPlugin) -> None:
        catalog = Catalog(
            name="my_api",
            type="rest_api",
            options=_valid_options(),
        )
        assert plugin.get_metadata(catalog, "nonexistent") is None


class TestInstantiate:
    def test_returns_catalog(self, plugin: RestApiCatalogPlugin) -> None:
        opts = _valid_options()
        catalog = plugin.instantiate("my_api", opts)
        assert catalog.name == "my_api"
        assert catalog.type == "rest_api"
        assert catalog.options == opts
