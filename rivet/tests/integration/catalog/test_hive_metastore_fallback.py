"""Example and edge-case tests for Hive Metastore legacy fallback.

Feature: hive-metastore-fallback
Tests cover validation, error handling, and connection test scenarios.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog
from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

# ── Helpers ───────────────────────────────────────────────────────────


def _base_options(**overrides: object) -> dict[str, object]:
    """Return a valid Databricks catalog options dict with overrides."""
    opts: dict[str, object] = {
        "workspace_url": "https://test.databricks.com",
        "catalog": "hive_metastore",
        "token": "fake-token",
    }
    opts.update(overrides)
    return opts


def _make_legacy_catalog(
    catalog_name: str = "hive_metastore",
    **extra: object,
) -> Catalog:
    return Catalog(
        name="test",
        type="databricks",
        options={
            "workspace_url": "https://test.databricks.com",
            "catalog": catalog_name,
            "legacy": True,
            "warehouse_id": "abc123",
            "token": "fake-token",
            **extra,
        },
    )


# ── 7.1  Validation tests ────────────────────────────────────────────


class TestLegacyValidation:
    """Property 1: Validation rejects legacy without warehouse_id.

    Validates: Requirements 1.2, 1.3, 1.4
    """

    def test_legacy_without_warehouse_id_raises(self) -> None:
        plugin = DatabricksCatalogPlugin()
        opts = _base_options(legacy=True)
        with pytest.raises(PluginValidationError, match="warehouse_id"):
            plugin.validate(opts)

    def test_legacy_with_empty_warehouse_id_raises(self) -> None:
        plugin = DatabricksCatalogPlugin()
        opts = _base_options(legacy=True, warehouse_id="")
        with pytest.raises(PluginValidationError, match="warehouse_id"):
            plugin.validate(opts)

    def test_legacy_with_warehouse_id_passes(self) -> None:
        plugin = DatabricksCatalogPlugin()
        opts = _base_options(legacy=True, warehouse_id="wh-123")
        plugin.validate(opts)  # should not raise

    def test_non_legacy_catalog_unchanged(self) -> None:
        plugin = DatabricksCatalogPlugin()
        opts = _base_options()
        plugin.validate(opts)  # should not raise, no legacy flag


# ── 7.2  Error handling tests ─────────────────────────────────────────


class TestLegacyErrorHandling:
    """Verify SQL failures are wrapped in ExecutionError with context.

    Validates: Requirements 2.3, 3.3, 4.5
    """

    def test_show_schemas_failure_includes_catalog_name(self) -> None:
        plugin = DatabricksCatalogPlugin()
        mock_api = MagicMock()
        mock_api.execute.side_effect = RuntimeError("connection refused")

        with pytest.raises(ExecutionError, match="hive_metastore"):
            plugin._legacy_list_schemas(mock_api, "hive_metastore")

    def test_show_schemas_execution_error_reraises(self) -> None:
        """ExecutionError from the API should propagate as-is."""
        plugin = DatabricksCatalogPlugin()
        mock_api = MagicMock()
        original = ExecutionError("original error")
        mock_api.execute.side_effect = original

        with pytest.raises(ExecutionError) as exc_info:
            plugin._legacy_list_schemas(mock_api, "hive_metastore")
        assert exc_info.value is original

    def test_show_tables_failure_includes_schema_name(self) -> None:
        plugin = DatabricksCatalogPlugin()
        mock_api = MagicMock()
        mock_api.execute.side_effect = RuntimeError("timeout")

        with pytest.raises(ExecutionError, match="my_schema"):
            plugin._legacy_list_tables(mock_api, "hive_metastore", "my_schema")

    def test_show_tables_execution_error_reraises(self) -> None:
        plugin = DatabricksCatalogPlugin()
        mock_api = MagicMock()
        original = ExecutionError("original error")
        mock_api.execute.side_effect = original

        with pytest.raises(ExecutionError) as exc_info:
            plugin._legacy_list_tables(mock_api, "hive_metastore", "my_schema")
        assert exc_info.value is original

    def test_describe_table_failure_includes_table_name(self) -> None:
        plugin = DatabricksCatalogPlugin()
        mock_api = MagicMock()
        mock_api.execute.side_effect = RuntimeError("parse error")

        with pytest.raises(ExecutionError, match="my_table"):
            plugin._legacy_describe_table(
                mock_api,
                "hive_metastore",
                "my_schema",
                "my_table",
            )

    def test_describe_table_execution_error_reraises(self) -> None:
        plugin = DatabricksCatalogPlugin()
        mock_api = MagicMock()
        original = ExecutionError("original error")
        mock_api.execute.side_effect = original

        with pytest.raises(ExecutionError) as exc_info:
            plugin._legacy_describe_table(
                mock_api,
                "hive_metastore",
                "my_schema",
                "my_table",
            )
        assert exc_info.value is original

    def test_describe_extended_failure_includes_table_name(self) -> None:
        plugin = DatabricksCatalogPlugin()
        mock_api = MagicMock()
        mock_api.execute.side_effect = RuntimeError("arrow decode error")

        with pytest.raises(ExecutionError, match="my_table"):
            plugin._legacy_describe_extended(
                mock_api,
                "hive_metastore",
                "my_schema",
                "my_table",
            )

    def test_describe_extended_execution_error_reraises(self) -> None:
        plugin = DatabricksCatalogPlugin()
        mock_api = MagicMock()
        original = ExecutionError("original error")
        mock_api.execute.side_effect = original

        with pytest.raises(ExecutionError) as exc_info:
            plugin._legacy_describe_extended(
                mock_api,
                "hive_metastore",
                "my_schema",
                "my_table",
            )
        assert exc_info.value is original


# ── 7.3  Connection test examples ─────────────────────────────────────


class TestLegacyConnectionTest:
    """Verify test_connection success and failure paths.

    Validates: Requirements 6.1, 6.2
    """

    def test_connection_success(self) -> None:
        plugin = DatabricksCatalogPlugin()
        cat = _make_legacy_catalog()
        mock_api = MagicMock()
        mock_api.execute.return_value = pa.table(
            {"databaseName": pa.array(["default"], type=pa.string())}
        )

        with patch.object(plugin, "_create_statement_api", return_value=mock_api):
            plugin._legacy_test_connection(cat)

        mock_api.execute.assert_called_once_with(
            "SHOW SCHEMAS IN hive_metastore",
            catalog="hive_metastore",
        )
        mock_api.close.assert_called_once()

    def test_connection_failure_includes_workspace_and_warehouse(self) -> None:
        plugin = DatabricksCatalogPlugin()
        cat = _make_legacy_catalog()
        mock_api = MagicMock()
        mock_api.execute.side_effect = RuntimeError("network unreachable")

        with patch.object(plugin, "_create_statement_api", return_value=mock_api):
            with pytest.raises(ExecutionError, match="test.databricks.com") as exc_info:
                plugin._legacy_test_connection(cat)
            assert "abc123" in str(exc_info.value)

        mock_api.close.assert_called_once()

    def test_connection_execution_error_reraises(self) -> None:
        plugin = DatabricksCatalogPlugin()
        cat = _make_legacy_catalog()
        mock_api = MagicMock()
        original = ExecutionError("auth failed")
        mock_api.execute.side_effect = original

        with patch.object(plugin, "_create_statement_api", return_value=mock_api):
            with pytest.raises(ExecutionError) as exc_info:
                plugin._legacy_test_connection(cat)
            assert exc_info.value is original

        mock_api.close.assert_called_once()
