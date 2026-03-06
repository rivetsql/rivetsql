"""Tests for UnityPolarsAdapter: REST API + credential vending, deltalake for Delta tables."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rivet_core.plugins import ComputeEngineAdapter
from rivet_polars.adapters.unity import (
    UnityPolarsAdapter,
    _check_deltalake,
    _configure_storage_options,
    _resolve_full_name,
)


class TestUnityPolarsAdapterRegistration:
    """Verify adapter class attributes match the spec."""

    def test_target_engine_type(self):
        assert UnityPolarsAdapter.target_engine_type == "polars"

    def test_catalog_type(self):
        assert UnityPolarsAdapter.catalog_type == "unity"

    def test_read_capabilities(self):
        caps = UnityPolarsAdapter.capabilities
        assert "projection_pushdown" in caps
        assert "predicate_pushdown" in caps
        assert "limit_pushdown" in caps

    def test_write_capabilities(self):
        caps = UnityPolarsAdapter.capabilities
        assert "write_append" in caps
        assert "write_replace" in caps
        assert "write_merge" in caps
        assert "write_scd2" in caps
        assert "write_partition" in caps

    def test_no_extra_read_capabilities(self):
        """cast_pushdown, join, aggregation should NOT be present per spec."""
        caps = UnityPolarsAdapter.capabilities
        assert "cast_pushdown" not in caps
        assert "join" not in caps
        assert "aggregation" not in caps

    def test_source_plugin(self):
        assert UnityPolarsAdapter.source_plugin == "rivet_polars"

    def test_source(self):
        assert UnityPolarsAdapter.source == "engine_plugin"

    def test_is_subclass(self):
        assert issubclass(UnityPolarsAdapter, ComputeEngineAdapter)

    def test_has_read_dispatch(self):
        assert hasattr(UnityPolarsAdapter, "read_dispatch")

    def test_has_write_dispatch(self):
        assert hasattr(UnityPolarsAdapter, "write_dispatch")


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


class TestConfigureStorageOptions:
    """Test _configure_storage_options with various credential formats."""

    def test_aws_credentials(self):
        creds = {
            "aws_temp_credentials": {
                "access_key_id": "AKIA...",
                "secret_access_key": "secret",
                "session_token": "token123",
            }
        }
        opts = _configure_storage_options("s3://bucket/path", creds)
        assert opts["AWS_ACCESS_KEY_ID"] == "AKIA..."
        assert opts["AWS_SECRET_ACCESS_KEY"] == "secret"
        assert opts["AWS_SESSION_TOKEN"] == "token123"

    def test_azure_credentials(self):
        creds = {
            "azure_user_delegation_sas": {
                "sas_token": "sv=2020&sig=abc",
            }
        }
        opts = _configure_storage_options("abfss://container@account.dfs.core.windows.net/path", creds)
        assert opts["SAS_TOKEN"] == "sv=2020&sig=abc"

    def test_gcs_credentials(self):
        creds = {
            "gcp_oauth_token": {
                "oauth_token": "ya29.token",
            }
        }
        opts = _configure_storage_options("gs://bucket/path", creds)
        assert opts["GOOGLE_SERVICE_ACCOUNT_KEY"] == "ya29.token"

    def test_none_credentials_warns(self):
        with pytest.warns(UserWarning, match="ambient cloud credentials"):
            opts = _configure_storage_options("s3://bucket/path", None)
        assert opts == {}

    def test_unknown_credential_format_warns(self):
        creds = {"some_unknown_provider": {"key": "val"}}
        with pytest.warns(UserWarning, match="Unrecognized credential format"):
            opts = _configure_storage_options("s3://bucket/path", creds)
        assert opts == {}


class TestCheckDeltalake:
    """Test _check_deltalake validation helper."""

    def test_raises_when_deltalake_missing(self):
        from rivet_core.errors import PluginValidationError

        with patch.dict("sys.modules", {"deltalake": None}):
            with pytest.raises(PluginValidationError) as exc_info:
                _check_deltalake()
        assert exc_info.value.error.code == "RVT-201"
        assert "deltalake" in exc_info.value.error.message

    def test_passes_when_deltalake_present(self):
        mock_deltalake = MagicMock()
        with patch.dict("sys.modules", {"deltalake": mock_deltalake}):
            # Should not raise
            _check_deltalake()


def _make_adapter_with_mock_registry():
    """Create a UnityPolarsAdapter with a mocked plugin registry returning a mock Unity plugin."""
    plugin_instance = MagicMock()
    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin_instance
    adapter = UnityPolarsAdapter()
    adapter._registry = registry
    return adapter, plugin_instance


class TestReadDispatch:
    """Test read_dispatch with mocked Unity client."""

    def test_read_dispatch_returns_deferred_material(self):
        adapter, plugin_instance = _make_adapter_with_mock_registry()
        plugin_instance.resolve_table_reference.return_value = {
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

        adapter, plugin_instance = _make_adapter_with_mock_registry()
        plugin_instance.resolve_table_reference.return_value = {
            "storage_location": None,
            "file_format": "PARQUET",
        }

        joint = MagicMock()
        joint.table = "missing_table"
        joint.name = "missing_joint"
        catalog = MagicMock()
        catalog.name = "cat"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(engine=MagicMock(), catalog=catalog, joint=joint)
        assert exc_info.value.error.code == "RVT-503"

    def test_read_dispatch_delta_checks_deltalake(self):
        """Delta format triggers deltalake availability check at dispatch time."""
        from rivet_core.errors import PluginValidationError

        adapter, plugin_instance = _make_adapter_with_mock_registry()
        plugin_instance.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/tables/delta_table",
            "file_format": "DELTA",
            "temporary_credentials": None,
        }

        joint = MagicMock()
        joint.table = "delta_table"
        joint.name = "delta_joint"
        catalog = MagicMock()
        catalog.name = "cat"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        with patch.dict("sys.modules", {"deltalake": None}):
            with pytest.raises(PluginValidationError) as exc_info:
                adapter.read_dispatch(engine=MagicMock(), catalog=catalog, joint=joint)
        assert exc_info.value.error.code == "RVT-201"

    def test_read_dispatch_delta_succeeds_when_deltalake_present(self):
        """Delta format dispatch succeeds when deltalake is installed."""
        adapter, plugin_instance = _make_adapter_with_mock_registry()
        plugin_instance.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/tables/delta_table",
            "file_format": "DELTA",
            "temporary_credentials": {
                "aws_temp_credentials": {
                    "access_key_id": "AK",
                    "secret_access_key": "SK",
                    "session_token": "ST",
                }
            },
        }

        joint = MagicMock()
        joint.table = "delta_table"
        joint.name = "delta_joint"
        catalog = MagicMock()
        catalog.name = "cat"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        mock_deltalake = MagicMock()
        with patch.dict("sys.modules", {"deltalake": mock_deltalake}):
            result = adapter.read_dispatch(engine=MagicMock(), catalog=catalog, joint=joint)

        assert result.material.state == "deferred"
        assert result.material.materialized_ref._file_format == "DELTA"


class TestWriteDispatch:
    """Test write_dispatch with mocked Unity client."""

    def test_write_dispatch_no_storage_location_raises(self):
        from rivet_core.errors import ExecutionError

        adapter, plugin_instance = _make_adapter_with_mock_registry()
        plugin_instance.vend_credentials.return_value = None
        plugin_instance.resolve_table_reference.return_value = {
            "storage_location": None,
            "file_format": "PARQUET",
        }

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

    def test_write_dispatch_delta_checks_deltalake(self):
        """Delta write triggers deltalake availability check."""
        from rivet_core.errors import PluginValidationError

        adapter, plugin_instance = _make_adapter_with_mock_registry()
        plugin_instance.vend_credentials.return_value = None
        plugin_instance.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/tables/delta_table",
            "file_format": "DELTA",
        }

        joint = MagicMock()
        joint.table = "delta_table"
        joint.name = "delta_joint"
        joint.write_strategy = "append"
        catalog = MagicMock()
        catalog.name = "cat"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        material = MagicMock()

        with patch.dict("sys.modules", {"deltalake": None}):
            with pytest.raises(PluginValidationError) as exc_info:
                adapter.write_dispatch(engine=MagicMock(), catalog=catalog, joint=joint, material=material)
        assert exc_info.value.error.code == "RVT-201"

    def test_write_dispatch_parquet_calls_polars(self):
        """Parquet write uses Polars write_parquet."""
        import pyarrow as pa

        adapter, plugin_instance = _make_adapter_with_mock_registry()
        plugin_instance.vend_credentials.return_value = {
            "aws_temp_credentials": {
                "access_key_id": "AK",
                "secret_access_key": "SK",
                "session_token": "ST",
            }
        }
        plugin_instance.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/tables/users",
            "file_format": "PARQUET",
        }

        joint = MagicMock()
        joint.table = "users"
        joint.name = "users_joint"
        joint.write_strategy = "replace"
        catalog = MagicMock()
        catalog.name = "cat"
        catalog.options = {"catalog_name": "prod", "schema": "default"}

        arrow_table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        material = MagicMock()
        material.to_arrow.return_value = arrow_table

        mock_df = MagicMock()
        with patch("polars.from_arrow", return_value=mock_df):
            result = adapter.write_dispatch(engine=MagicMock(), catalog=catalog, joint=joint, material=material)

        mock_df.write_parquet.assert_called_once()
        assert result is material
