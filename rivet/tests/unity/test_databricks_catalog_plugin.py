"""Tests for DatabricksCatalogPlugin (tasks 24.1, 24.3, 3.3)."""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin
from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

_VALID_OPTIONS = {"workspace_url": "https://my.databricks.com", "catalog": "main"}


def test_catalog_type():
    assert DatabricksCatalogPlugin().type == "databricks"


def test_is_catalog_plugin():
    assert isinstance(DatabricksCatalogPlugin(), CatalogPlugin)


def test_required_options():
    plugin = DatabricksCatalogPlugin()
    assert "workspace_url" in plugin.required_options
    assert "catalog" in plugin.required_options


def test_optional_options():
    plugin = DatabricksCatalogPlugin()
    assert "schema" in plugin.optional_options
    assert "http_path" in plugin.optional_options


def test_credential_options():
    plugin = DatabricksCatalogPlugin()
    assert "token" in plugin.credential_options
    assert "client_id" in plugin.credential_options
    assert "client_secret" in plugin.credential_options
    assert "azure_tenant_id" in plugin.credential_options
    assert "azure_client_id" in plugin.credential_options
    assert "azure_client_secret" in plugin.credential_options


def test_validate_accepts_valid_options():
    DatabricksCatalogPlugin().validate(_VALID_OPTIONS)  # should not raise


def test_validate_rejects_missing_workspace_url():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "workspace_url"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "workspace_url" in exc_info.value.error.message


def test_validate_rejects_missing_catalog():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "catalog"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "catalog" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    opts = {**_VALID_OPTIONS, "unknown_key": "value"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_key" in exc_info.value.error.message


def test_validate_accepts_optional_schema():
    opts = {**_VALID_OPTIONS, "schema": "my_schema"}
    DatabricksCatalogPlugin().validate(opts)  # should not raise


def test_validate_accepts_optional_http_path():
    opts = {**_VALID_OPTIONS, "http_path": "/sql/1.0/warehouses/abc123"}
    DatabricksCatalogPlugin().validate(opts)  # should not raise


def test_validate_accepts_credential_options():
    opts = {**_VALID_OPTIONS, "token": "dapi123"}
    DatabricksCatalogPlugin().validate(opts)  # should not raise


def test_instantiate_returns_catalog():
    catalog = DatabricksCatalogPlugin().instantiate("my_db", _VALID_OPTIONS)
    assert isinstance(catalog, Catalog)
    assert catalog.name == "my_db"
    assert catalog.type == "databricks"


def test_default_table_reference_uses_catalog_schema():
    plugin = DatabricksCatalogPlugin()
    ref = plugin.default_table_reference("orders", {"catalog": "main", "schema": "default"})
    assert ref == "main.default.orders"


def test_default_table_reference_custom_schema():
    plugin = DatabricksCatalogPlugin()
    ref = plugin.default_table_reference("users", {"catalog": "dev", "schema": "sales"})
    assert ref == "dev.sales.users"


# --- Task 24.3: Credential options validation ---

def test_validate_accepts_token_credential():
    opts = {**_VALID_OPTIONS, "token": "dapi-abc123"}
    DatabricksCatalogPlugin().validate(opts)  # should not raise


def test_validate_accepts_oauth_m2m_credentials():
    opts = {**_VALID_OPTIONS, "client_id": "my-client", "client_secret": "my-secret"}
    DatabricksCatalogPlugin().validate(opts)  # should not raise


def test_validate_accepts_azure_entra_credentials():
    opts = {
        **_VALID_OPTIONS,
        "azure_tenant_id": "tenant-123",
        "azure_client_id": "client-456",
        "azure_client_secret": "secret-789",
    }
    DatabricksCatalogPlugin().validate(opts)  # should not raise


def test_validate_rejects_partial_oauth_m2m_missing_secret():
    opts = {**_VALID_OPTIONS, "client_id": "my-client"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-205"
    assert "client_secret" in exc_info.value.error.message


def test_validate_rejects_partial_oauth_m2m_missing_id():
    opts = {**_VALID_OPTIONS, "client_secret": "my-secret"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-205"
    assert "client_id" in exc_info.value.error.message


def test_validate_rejects_partial_azure_missing_client_secret():
    opts = {**_VALID_OPTIONS, "azure_tenant_id": "t", "azure_client_id": "c"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-205"
    assert "azure_client_secret" in exc_info.value.error.message


def test_validate_rejects_partial_azure_only_tenant():
    opts = {**_VALID_OPTIONS, "azure_tenant_id": "tenant-only"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-205"


def test_resolve_credentials_with_token():
    plugin = DatabricksCatalogPlugin()
    opts = {**_VALID_OPTIONS, "token": "dapi-xyz"}
    cred = plugin.resolve_credentials(opts)
    assert cred.auth_type == "pat"
    assert cred.token == "dapi-xyz"
    assert cred.source == "explicit_options"


def test_resolve_credentials_with_oauth_m2m():
    plugin = DatabricksCatalogPlugin()
    opts = {**_VALID_OPTIONS, "client_id": "cid", "client_secret": "csec"}
    cred = plugin.resolve_credentials(opts)
    assert cred.auth_type == "oauth_m2m"
    assert cred.client_id == "cid"
    assert cred.client_secret == "csec"
    assert cred.source == "explicit_options"


def test_resolve_credentials_with_azure_entra():
    plugin = DatabricksCatalogPlugin()
    opts = {
        **_VALID_OPTIONS,
        "azure_tenant_id": "tid",
        "azure_client_id": "cid",
        "azure_client_secret": "csec",
    }
    cred = plugin.resolve_credentials(opts)
    assert cred.auth_type == "azure_cli"
    assert cred.azure_tenant_id == "tid"
    assert cred.azure_client_id == "cid"
    assert cred.azure_client_secret == "csec"
    assert cred.source == "explicit_options"


def test_resolve_credentials_from_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token-abc")
    plugin = DatabricksCatalogPlugin()
    cred = plugin.resolve_credentials(_VALID_OPTIONS)
    assert cred.auth_type == "pat"
    assert cred.token == "env-token-abc"
    assert cred.source == "environment_variables"


def test_resolve_credentials_no_source_raises(monkeypatch, tmp_path):
    """When no credentials are available, RVT-201 is raised."""
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    plugin = DatabricksCatalogPlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.resolve_credentials(_VALID_OPTIONS, config_path=tmp_path / "nonexistent.cfg")
    assert exc_info.value.error.code == "RVT-201"


# --- Task 24.4: workspace_url scheme validation ---

def test_validate_rejects_workspace_url_without_scheme():
    opts = {**_VALID_OPTIONS, "workspace_url": "my.databricks.com"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-202"
    assert "https://" in exc_info.value.error.message


def test_validate_rejects_workspace_url_with_http_scheme():
    opts = {**_VALID_OPTIONS, "workspace_url": "http://my.databricks.com"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-202"
    assert "https://" in exc_info.value.error.message


def test_validate_accepts_workspace_url_with_https_scheme():
    opts = {**_VALID_OPTIONS, "workspace_url": "https://my.databricks.com"}
    DatabricksCatalogPlugin().validate(opts)  # should not raise


# --- Task 24.5: Introspection via Unity Catalog REST API ---

from datetime import UTC

from rivet_databricks.databricks_catalog import _parse_ts, _unity_type_to_arrow


class TestUnityTypeToArrow:
    def test_known_types(self):
        assert _unity_type_to_arrow("bigint") == "int64"
        assert _unity_type_to_arrow("long") == "int64"
        assert _unity_type_to_arrow("int") == "int32"
        assert _unity_type_to_arrow("integer") == "int32"
        assert _unity_type_to_arrow("smallint") == "int16"
        assert _unity_type_to_arrow("tinyint") == "int8"
        assert _unity_type_to_arrow("float") == "float32"
        assert _unity_type_to_arrow("double") == "float64"
        assert _unity_type_to_arrow("decimal") == "float64"
        assert _unity_type_to_arrow("boolean") == "bool"
        assert _unity_type_to_arrow("string") == "large_utf8"
        assert _unity_type_to_arrow("varchar") == "large_utf8"
        assert _unity_type_to_arrow("binary") == "large_binary"
        assert _unity_type_to_arrow("date") == "date32"
        assert _unity_type_to_arrow("timestamp") == "timestamp[us, UTC]"
        assert _unity_type_to_arrow("timestamp_ntz") == "timestamp[us]"
        assert _unity_type_to_arrow("array") == "large_utf8"
        assert _unity_type_to_arrow("struct") == "large_utf8"

    def test_case_insensitive(self):
        assert _unity_type_to_arrow("BIGINT") == "int64"
        assert _unity_type_to_arrow("String") == "large_utf8"

    def test_strips_precision(self):
        assert _unity_type_to_arrow("decimal(10,2)") == "float64"
        assert _unity_type_to_arrow("varchar(255)") == "large_utf8"

    def test_unknown_type_returns_large_utf8_with_warning(self):
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = _unity_type_to_arrow("interval")
        assert result == "large_utf8"
        assert len(w) == 1
        assert "interval" in str(w[0].message)


class TestParseTs:
    def test_none_returns_none(self):
        assert _parse_ts(None) is None

    def test_epoch_ms_int(self):
        dt = _parse_ts(1000)
        assert dt is not None
        assert dt.tzinfo == UTC

    def test_iso_string(self):
        dt = _parse_ts("2026-02-28T10:00:00Z")
        assert dt is not None
        assert dt.year == 2026

    def test_iso_string_with_offset(self):
        dt = _parse_ts("2026-02-28T10:00:00+00:00")
        assert dt is not None

    def test_invalid_string_returns_none(self):
        assert _parse_ts("not-a-date") is None


class TestDatabricksListTables:
    _VALID_OPTIONS = {"workspace_url": "https://my.databricks.com", "catalog": "main", "token": "dapi123"}

    def _make_catalog(self):
        return DatabricksCatalogPlugin().instantiate("db", self._VALID_OPTIONS)

    def test_list_tables_returns_catalog_nodes(self, monkeypatch):
        from rivet_core.introspection import CatalogNode

        schemas = [{"name": "default"}]
        tables = [
            {"name": "users", "table_type": "EXTERNAL", "data_source_format": "DELTA",
             "updated_at": None, "owner": "alice", "comment": "user table", "properties": {}},
        ]
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_schemas", lambda self, cn: schemas)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_tables", lambda self, cn, sn: tables)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        result = plugin.list_tables(catalog)

        assert len(result) == 1
        node = result[0]
        assert isinstance(node, CatalogNode)
        assert node.name == "users"
        assert node.node_type == "external"
        assert node.path == ["main", "default", "users"]
        assert node.is_container is False
        assert node.summary.format == "DELTA"
        assert node.summary.owner == "alice"
        assert node.summary.comment == "user table"

    def test_list_tables_multiple_schemas(self, monkeypatch):
        schemas = [{"name": "s1"}, {"name": "s2"}]
        tables_by_schema = {
            "s1": [{"name": "t1", "table_type": "MANAGED", "data_source_format": "DELTA",
                    "updated_at": None, "owner": None, "comment": None, "properties": {}}],
            "s2": [{"name": "t2", "table_type": "EXTERNAL", "data_source_format": "PARQUET",
                    "updated_at": None, "owner": None, "comment": None, "properties": {}}],
        }
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_schemas", lambda self, cn: schemas)
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_tables",
            lambda self, cn, sn: tables_by_schema[sn],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        result = plugin.list_tables(catalog)

        assert len(result) == 2
        names = {n.name for n in result}
        assert names == {"t1", "t2"}

    def test_list_tables_empty_schemas(self, monkeypatch):
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_schemas", lambda self, cn: [])
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_tables", lambda self, cn, sn: [])
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        result = plugin.list_tables(catalog)
        assert result == []

    def test_list_tables_uses_workspace_url_as_host(self, monkeypatch):
        hosts_used = []

        original_init = __import__("rivet_databricks.client", fromlist=["UnityCatalogClient"]).UnityCatalogClient.__init__

        def fake_init(self, host, credential):
            hosts_used.append(host)
            original_init(self, host, credential)

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.__init__", fake_init)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_schemas", lambda self, cn: [])
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_tables", lambda self, cn, sn: [])
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        plugin.list_tables(catalog)

        assert hosts_used[0] == "https://my.databricks.com"

    def test_list_tables_no_sql_execution(self, monkeypatch):
        """Introspection must not call any SQL execution methods."""
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_schemas", lambda self, cn: [])
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.list_tables", lambda self, cn, sn: [])
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        # Patch execute to raise if called
        def no_sql(*args, **kwargs):
            raise AssertionError("SQL execution must not be called during introspection")

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient._request", lambda self, method, path, **kw: (
            no_sql() if method == "POST" and "statements" in path else {"schemas": [], "tables": []}
        ))

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        plugin.list_tables(catalog)  # should not raise


class TestDatabricksGetSchema:
    _VALID_OPTIONS = {"workspace_url": "https://my.databricks.com", "catalog": "main", "token": "dapi123"}

    def _make_catalog(self):
        return DatabricksCatalogPlugin().instantiate("db", self._VALID_OPTIONS)

    def _raw_table(self):
        return {
            "name": "users",
            "full_name": "main.default.users",
            "table_type": "EXTERNAL",
            "comment": "user table",
            "columns": [
                {"name": "id", "type_text": "bigint", "nullable": False, "comment": "pk"},
                {"name": "name", "type_text": "string", "nullable": True, "comment": None},
                {"name": "score", "type_text": "double", "nullable": True, "comment": None},
            ],
            "partition_columns": [{"name": "id"}],
        }

    def test_get_schema_returns_object_schema(self, monkeypatch):
        from rivet_core.introspection import ObjectSchema

        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        schema = plugin.get_schema(catalog, "main.default.users")

        assert isinstance(schema, ObjectSchema)
        assert schema.path == ["main", "default", "users"]
        assert schema.node_type == "external"
        assert schema.comment == "user table"

    def test_get_schema_columns_mapped_to_arrow(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        schema = plugin.get_schema(catalog, "main.default.users")

        assert len(schema.columns) == 3
        assert schema.columns[0].name == "id"
        assert schema.columns[0].type == "int64"
        assert schema.columns[0].native_type == "bigint"
        assert schema.columns[0].nullable is False
        assert schema.columns[0].comment == "pk"
        assert schema.columns[1].type == "large_utf8"
        assert schema.columns[2].type == "float64"

    def test_get_schema_partition_key_flagged(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        schema = plugin.get_schema(catalog, "main.default.users")

        assert schema.columns[0].is_partition_key is True
        assert schema.columns[1].is_partition_key is False

    def test_get_schema_unknown_type_warns_and_returns_large_utf8(self, monkeypatch):
        import warnings

        raw = {
            "name": "t",
            "table_type": "MANAGED",
            "columns": [{"name": "x", "type_text": "interval", "nullable": True}],
        }
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = plugin.get_schema(catalog, "main.default.t")

        assert schema.columns[0].type == "large_utf8"
        assert any("interval" in str(warning.message) for warning in w)

    def test_get_schema_no_sql_execution(self, monkeypatch):
        """get_schema must not execute SQL."""
        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        # Should complete without error (no SQL path)
        schema = plugin.get_schema(catalog, "main.default.users")
        assert schema is not None


class TestDatabricksGetMetadata:
    _VALID_OPTIONS = {"workspace_url": "https://my.databricks.com", "catalog": "main", "token": "dapi123"}

    def _make_catalog(self):
        return DatabricksCatalogPlugin().instantiate("db", self._VALID_OPTIONS)

    def _raw_table(self):
        return {
            "name": "users",
            "full_name": "main.default.users",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": "s3://bucket/main/default/users",
            "owner": "alice",
            "comment": "user table",
            "created_at": 1000000,
            "updated_at": 2000000,
            "properties": {
                "delta.sizeInBytes": "1024",
                "delta.numRecords": "100",
            },
        }

    def test_get_metadata_returns_object_metadata(self, monkeypatch):
        from rivet_core.introspection import ObjectMetadata

        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        meta = plugin.get_metadata(catalog, "main.default.users")

        assert isinstance(meta, ObjectMetadata)

    def test_get_metadata_fields(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        meta = plugin.get_metadata(catalog, "main.default.users")

        assert meta.node_type == "external"
        assert meta.format == "DELTA"
        assert meta.location == "s3://bucket/main/default/users"
        assert meta.owner == "alice"
        assert meta.comment == "user table"
        assert meta.size_bytes == 1024
        assert meta.row_count == 100
        assert meta.path == ["main", "default", "users"]

    def test_get_metadata_timestamps_parsed(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        meta = plugin.get_metadata(catalog, "main.default.users")

        assert meta.created_at is not None
        assert meta.last_modified is not None

    def test_get_metadata_missing_optional_fields(self, monkeypatch):
        raw = {"name": "t", "table_type": "MANAGED", "full_name": "main.default.t"}
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        meta = plugin.get_metadata(catalog, "main.default.t")

        assert meta.size_bytes is None
        assert meta.row_count is None
        assert meta.created_at is None
        assert meta.last_modified is None
        assert meta.owner is None
        assert meta.comment is None
        assert meta.location is None
        assert meta.format is None

    def test_get_metadata_no_sql_execution(self, monkeypatch):
        """get_metadata must not execute SQL."""
        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        meta = plugin.get_metadata(catalog, "main.default.users")
        assert meta is not None

    def test_get_metadata_properties_included(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = DatabricksCatalogPlugin()
        catalog = self._make_catalog()
        meta = plugin.get_metadata(catalog, "main.default.users")

        assert "delta.sizeInBytes" in meta.properties
        assert meta.properties["delta.sizeInBytes"] == "1024"


# --- Task 3.3: Verify DatabricksCatalogPlugin attributes per spec ---


class TestDatabricksCatalogPluginAttributes:
    """Req 3.5–3.9: catalog_type, options, and validation plugin_name."""

    def test_catalog_type_is_databricks(self):
        assert DatabricksCatalogPlugin().type == "databricks"

    def test_required_options_exact(self):
        assert set(DatabricksCatalogPlugin().required_options) == {"workspace_url", "catalog"}

    def test_optional_options_exact(self):
        opts = DatabricksCatalogPlugin().optional_options
        assert set(opts) == {"schema", "http_path"}
        assert opts["schema"] == "default"

    def test_credential_options_exact(self):
        assert set(DatabricksCatalogPlugin().credential_options) == {
            "token", "client_id", "client_secret",
            "azure_tenant_id", "azure_client_id", "azure_client_secret",
        }

    def test_validation_error_carries_plugin_name_rivet_databricks(self):
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({"unknown_opt": "x"})
        assert exc_info.value.error.context["plugin_name"] == "rivet_databricks"

    def test_missing_required_error_carries_plugin_name(self):
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({})
        assert exc_info.value.error.context["plugin_name"] == "rivet_databricks"
        assert exc_info.value.error.context["plugin_type"] == "catalog"

    def test_invalid_workspace_url_error_carries_plugin_name(self):
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({"workspace_url": "http://bad", "catalog": "c"})
        assert exc_info.value.error.context["plugin_name"] == "rivet_databricks"
