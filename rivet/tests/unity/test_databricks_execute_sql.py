"""Tests for DatabricksComputeEnginePlugin.execute_sql (task 9.1)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import ComputeEngine
from rivet_databricks.engine import DatabricksComputeEnginePlugin


def _make_engine(**overrides: object) -> ComputeEngine:
    config = {
        "workspace_url": "https://example.databricks.com",
        "token": "dapi_test_token",
        "warehouse_id": "abc123",
        "catalog": "main",
        "schema": "default",
        **overrides,
    }
    return ComputeEngine(name="db_engine", engine_type="databricks", config=config)


class TestExecuteSqlDelegation:
    """Property 19: Databricks execute_sql delegates to Statement API."""

    def test_delegates_to_statement_api(self):
        plugin = DatabricksComputeEnginePlugin()
        engine = _make_engine()
        expected = pyarrow.table({"x": [1, 2]})

        mock_api = MagicMock()
        mock_api.execute.return_value = expected

        with patch.object(plugin, "create_statement_api", return_value=mock_api):
            result = plugin.execute_sql(engine, "SELECT 1", {})

        mock_api.execute.assert_called_once_with(
            "SELECT 1", catalog="main", schema="default"
        )
        assert result.equals(expected)

    def test_passes_catalog_and_schema_from_config(self):
        plugin = DatabricksComputeEnginePlugin()
        engine = _make_engine(catalog="analytics", schema="prod")

        mock_api = MagicMock()
        mock_api.execute.return_value = pyarrow.table({})

        with patch.object(plugin, "create_statement_api", return_value=mock_api):
            plugin.execute_sql(engine, "SELECT 1", {})

        mock_api.execute.assert_called_once_with(
            "SELECT 1", catalog="analytics", schema="prod"
        )

    def test_creates_api_with_engine_config(self):
        plugin = DatabricksComputeEnginePlugin()
        engine = _make_engine()

        mock_api = MagicMock()
        mock_api.execute.return_value = pyarrow.table({})

        with patch.object(plugin, "create_statement_api", return_value=mock_api) as mock_create:
            plugin.execute_sql(engine, "SELECT 1", {})

        mock_create.assert_called_once_with(
            "https://example.databricks.com",
            "dapi_test_token",
            engine.config,
        )


class TestExecuteSqlIgnoresInputTables:
    """input_tables are ignored — Databricks resolves references server-side."""

    def test_non_empty_input_tables_still_executes(self):
        plugin = DatabricksComputeEnginePlugin()
        engine = _make_engine()
        tables = {"upstream": pyarrow.table({"a": [1]})}
        expected = pyarrow.table({"x": [1]})

        mock_api = MagicMock()
        mock_api.execute.return_value = expected

        with patch.object(plugin, "create_statement_api", return_value=mock_api):
            result = plugin.execute_sql(engine, "SELECT 1", tables)

        mock_api.execute.assert_called_once()
        assert result.equals(expected)

    def test_empty_input_tables_does_not_raise(self):
        plugin = DatabricksComputeEnginePlugin()
        engine = _make_engine()

        mock_api = MagicMock()
        mock_api.execute.return_value = pyarrow.table({})

        with patch.object(plugin, "create_statement_api", return_value=mock_api):
            plugin.execute_sql(engine, "SELECT 1", {})


class TestExecuteSqlErrorWrapping:
    """Property 20: Databricks API error raises RVT-503."""

    def test_non_execution_error_wrapped_in_rvt503(self):
        plugin = DatabricksComputeEnginePlugin()
        engine = _make_engine()

        mock_api = MagicMock()
        mock_api.execute.side_effect = RuntimeError("connection timeout")

        with patch.object(plugin, "create_statement_api", return_value=mock_api):
            with pytest.raises(ExecutionError) as exc_info:
                plugin.execute_sql(engine, "SELECT * FROM t", {})

        err = exc_info.value.error
        assert err.code == "RVT-503"
        assert "connection timeout" in err.message
        assert err.context["warehouse_id"] == "abc123"
        assert err.context["sql"] == "SELECT * FROM t"

    def test_execution_error_not_double_wrapped(self):
        """ExecutionError from Statement API should propagate as-is."""
        plugin = DatabricksComputeEnginePlugin()
        engine = _make_engine()

        original_error = ExecutionError(
            MagicMock(code="RVT-502", message="original")
        )
        mock_api = MagicMock()
        mock_api.execute.side_effect = original_error

        with patch.object(plugin, "create_statement_api", return_value=mock_api):
            with pytest.raises(ExecutionError) as exc_info:
                plugin.execute_sql(engine, "SELECT 1", {})

        assert exc_info.value is original_error

    def test_sql_truncated_to_200_chars_in_context(self):
        plugin = DatabricksComputeEnginePlugin()
        engine = _make_engine()
        long_sql = "SELECT " + "x" * 300

        mock_api = MagicMock()
        mock_api.execute.side_effect = RuntimeError("fail")

        with patch.object(plugin, "create_statement_api", return_value=mock_api):
            with pytest.raises(ExecutionError) as exc_info:
                plugin.execute_sql(engine, long_sql, {})

        assert len(exc_info.value.error.context["sql"]) == 200
