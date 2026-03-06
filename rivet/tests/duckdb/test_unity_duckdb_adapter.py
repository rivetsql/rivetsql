"""Tests for UnityDuckDBAdapter: REST API metadata, credential vending, httpfs + storage read."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rivet_core.plugins import ComputeEngineAdapter
from rivet_duckdb.adapters.unity import (
    UnityDuckDBAdapter,
    _configure_duckdb_credentials,
    _resolve_full_name,
)


def _make_adapter_with_plugin(plugin_mock: MagicMock) -> UnityDuckDBAdapter:
    """Create an adapter with a mocked registry returning the given plugin."""
    adapter = UnityDuckDBAdapter()
    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin_mock
    adapter._registry = registry
    return adapter


class TestUnityDuckDBAdapterRegistration:
    """Verify adapter class attributes match the spec."""

    def test_target_engine_type(self):
        assert UnityDuckDBAdapter.target_engine_type == "duckdb"

    def test_catalog_type(self):
        assert UnityDuckDBAdapter.catalog_type == "unity"

    def test_read_capabilities(self):
        caps = UnityDuckDBAdapter.capabilities
        assert "projection_pushdown" in caps
        assert "predicate_pushdown" in caps
        assert "limit_pushdown" in caps

    def test_write_capabilities(self):
        caps = UnityDuckDBAdapter.capabilities
        assert "write_append" in caps
        assert "write_replace" in caps

    def test_no_extra_read_capabilities(self):
        """cast_pushdown, join, aggregation should NOT be present per spec."""
        caps = UnityDuckDBAdapter.capabilities
        assert "cast_pushdown" not in caps
        assert "join" not in caps
        assert "aggregation" not in caps

    def test_source_plugin(self):
        assert UnityDuckDBAdapter.source_plugin == "rivet_duckdb"

    def test_source(self):
        assert UnityDuckDBAdapter.source == "engine_plugin"


class TestResolveFullName:
    """Test _resolve_full_name helper."""

    def test_simple_table_name(self):
        joint = MagicMock()
        joint.table = "users"
        joint.name = "users_joint"
        catalog = MagicMock()
        catalog.options = {"catalog_name": "prod", "schema": "analytics"}
        assert _resolve_full_name(joint, catalog) == "prod.analytics.users"

    def test_default_schema(self):
        joint = MagicMock()
        joint.table = "orders"
        joint.name = "orders_joint"
        catalog = MagicMock()
        catalog.options = {"catalog_name": "main"}
        assert _resolve_full_name(joint, catalog) == "main.default.orders"

    def test_already_qualified(self):
        joint = MagicMock()
        joint.table = "prod.analytics.users"
        joint.name = "users_joint"
        catalog = MagicMock()
        catalog.options = {"catalog_name": "other", "schema": "other_schema"}
        assert _resolve_full_name(joint, catalog) == "prod.analytics.users"

    def test_falls_back_to_joint_name(self):
        joint = MagicMock()
        joint.table = None
        joint.name = "my_joint"
        catalog = MagicMock()
        catalog.options = {"catalog_name": "cat", "schema": "sch"}
        assert _resolve_full_name(joint, catalog) == "cat.sch.my_joint"


class TestConfigureDuckDBCredentials:
    """Test _configure_duckdb_credentials with various credential formats."""

    def test_aws_credentials(self):
        conn = MagicMock()
        creds = {
            "aws_temp_credentials": {
                "access_key_id": "AKIA...",
                "secret_access_key": "secret",
                "session_token": "token123",
            }
        }
        _configure_duckdb_credentials(conn, "s3://bucket/path", creds)
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "S3" in sql
        assert "AKIA..." in sql
        assert "secret" in sql
        assert "token123" in sql

    def test_azure_credentials(self):
        conn = MagicMock()
        creds = {
            "azure_user_delegation_sas": {
                "sas_token": "sv=2020&sig=abc",
            }
        }
        _configure_duckdb_credentials(conn, "abfss://container@account.dfs.core.windows.net/path", creds)
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "AZURE" in sql
        assert "sv=2020&sig=abc" in sql

    def test_gcs_credentials(self):
        conn = MagicMock()
        creds = {
            "gcp_oauth_token": {
                "oauth_token": "ya29.token",
            }
        }
        _configure_duckdb_credentials(conn, "gs://bucket/path", creds)
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "GCS" in sql
        assert "ya29.token" in sql

    def test_none_credentials_warns(self):
        conn = MagicMock()
        with pytest.warns(UserWarning, match="ambient cloud credentials"):
            _configure_duckdb_credentials(conn, "s3://bucket/path", None)
        conn.execute.assert_not_called()

    def test_unknown_credential_format_warns(self):
        conn = MagicMock()
        creds = {"some_unknown_provider": {"key": "val"}}
        with pytest.warns(UserWarning, match="Unrecognized credential format"):
            _configure_duckdb_credentials(conn, "s3://bucket/path", creds)
        conn.execute.assert_not_called()


class TestReadDispatch:
    """Test read_dispatch with mocked Unity catalog plugin via registry."""

    def test_read_dispatch_returns_deferred_material(self):
        plugin_mock = MagicMock()
        plugin_mock.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/tables/users",
            "file_format": "PARQUET",
            "columns": [],
            "partition_columns": [],
            "table_type": "MANAGED",
            "temporary_credentials": {
                "aws_temp_credentials": {
                    "access_key_id": "AK",
                    "secret_access_key": "SK",
                    "session_token": "ST",
                }
            },
        }

        adapter = _make_adapter_with_plugin(plugin_mock)
        joint = MagicMock()
        joint.table = "users"
        joint.name = "users_joint"
        catalog = MagicMock()
        catalog.name = "my_catalog"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        result = adapter.read_dispatch(engine=MagicMock(), catalog=catalog, joint=joint)

        assert result.material.name == "users_joint"
        assert result.material.catalog == "my_catalog"
        assert result.material.state == "deferred"
        assert result.material.materialized_ref is not None
        assert result.residual.predicates == []
        assert result.residual.limit is None
        assert result.residual.casts == []

    def test_read_dispatch_no_storage_location_raises(self):
        from rivet_core.errors import ExecutionError

        plugin_mock = MagicMock()
        plugin_mock.resolve_table_reference.return_value = {
            "storage_location": None,
            "file_format": "PARQUET",
        }

        adapter = _make_adapter_with_plugin(plugin_mock)
        joint = MagicMock()
        joint.table = "missing_table"
        joint.name = "missing_joint"
        catalog = MagicMock()
        catalog.name = "cat"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(engine=MagicMock(), catalog=catalog, joint=joint)
        assert exc_info.value.error.code == "RVT-503"

    def test_read_dispatch_csv_format(self):
        plugin_mock = MagicMock()
        plugin_mock.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/tables/data.csv",
            "file_format": "CSV",
            "temporary_credentials": None,
        }

        adapter = _make_adapter_with_plugin(plugin_mock)
        joint = MagicMock()
        joint.table = "data"
        joint.name = "data_joint"
        catalog = MagicMock()
        catalog.name = "cat"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        result = adapter.read_dispatch(engine=MagicMock(), catalog=catalog, joint=joint)
        # Verify the ref uses the CSV reader
        assert result.material.materialized_ref._reader_func == "read_csv_auto"

    def test_read_dispatch_no_registry_raises(self):
        from rivet_core.errors import ExecutionError

        adapter = UnityDuckDBAdapter()
        joint = MagicMock()
        joint.table = "t"
        joint.name = "j"
        catalog = MagicMock()
        catalog.options = {"catalog_name": "c", "schema": "s"}

        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(engine=MagicMock(), catalog=catalog, joint=joint)
        assert exc_info.value.error.code == "RVT-501"


class TestWriteDispatch:
    """Test write_dispatch with mocked Unity catalog plugin via registry."""

    def test_write_dispatch_no_storage_location_raises(self):
        from rivet_core.errors import ExecutionError

        plugin_mock = MagicMock()
        plugin_mock.vend_credentials.return_value = None
        plugin_mock.resolve_table_reference.return_value = {
            "storage_location": None,
            "file_format": "PARQUET",
        }

        adapter = _make_adapter_with_plugin(plugin_mock)
        joint = MagicMock()
        joint.table = "missing"
        joint.name = "missing_joint"
        joint.write_strategy = "replace"
        catalog = MagicMock()
        catalog.name = "cat"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        material = MagicMock()

        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine=MagicMock(), catalog=catalog, joint=joint, material=material)
        assert exc_info.value.error.code == "RVT-503"


class TestAdapterIsComputeEngineAdapter:
    """Verify UnityDuckDBAdapter is a proper ComputeEngineAdapter subclass."""

    def test_is_subclass(self):
        assert issubclass(UnityDuckDBAdapter, ComputeEngineAdapter)

    def test_has_read_dispatch(self):
        assert hasattr(UnityDuckDBAdapter, "read_dispatch")

    def test_has_write_dispatch(self):
        assert hasattr(UnityDuckDBAdapter, "write_dispatch")
