"""Unit tests for DatabricksUnityAdapter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow
import pytest

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.models import Catalog, ComputeEngine, Joint, Material
from rivet_core.plugins import PluginRegistry
from rivet_core.strategies import _ArrowMaterializedRef


class TestAdapterRegistration:
    """Task 4.1: Load DatabricksPlugin, verify registry contains adapter under (databricks, unity) key."""

    def test_adapter_registered_under_databricks_unity_key(self):
        from rivet_databricks import DatabricksPlugin

        registry = PluginRegistry()
        DatabricksPlugin(registry)
        adapter = registry.get_adapter("databricks", "unity")
        assert adapter is not None, "Expected adapter registered under ('databricks', 'unity')"

    def test_registered_adapter_is_databricks_unity_adapter(self):
        from rivet_databricks import DatabricksPlugin
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        registry = PluginRegistry()
        DatabricksPlugin(registry)
        adapter = registry.get_adapter("databricks", "unity")
        assert isinstance(adapter, DatabricksUnityAdapter)


class TestStaticAttributes:
    """Task 4.2: Test static attributes on DatabricksUnityAdapter."""

    def test_target_engine_type(self):
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        assert DatabricksUnityAdapter.target_engine_type == "databricks"

    def test_catalog_type(self):
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        assert DatabricksUnityAdapter.catalog_type == "unity"

    def test_capabilities(self):
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        assert DatabricksUnityAdapter.capabilities == ["read", "write"]

    def test_source(self):
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        assert DatabricksUnityAdapter.source == "catalog_plugin"

    def test_source_plugin(self):
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        assert DatabricksUnityAdapter.source_plugin == "rivet_databricks"

    def test_is_compute_engine_adapter(self):
        from rivet_core.plugins import ComputeEngineAdapter
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        assert issubclass(DatabricksUnityAdapter, ComputeEngineAdapter)


class TestDatabricksUnityMaterializedRef:
    """Task 1.4: Test _DatabricksUnityMaterializedRef."""

    def _make_ref(self, table=None):
        from unittest.mock import MagicMock

        import pyarrow

        if table is None:
            table = pyarrow.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        api = MagicMock()
        api.execute.return_value = table
        from rivet_databricks.adapters.unity import _DatabricksUnityMaterializedRef

        ref = _DatabricksUnityMaterializedRef("SELECT * FROM c.s.t", api, "c", "s")
        return ref, api, table

    def test_to_arrow_calls_api_execute(self):
        ref, api, expected = self._make_ref()
        result = ref.to_arrow()
        api.execute.assert_called_once_with("SELECT * FROM c.s.t", catalog="c", schema="s")
        assert result.equals(expected)

    def test_to_arrow_is_deferred(self):
        ref, api, _ = self._make_ref()
        api.execute.assert_not_called()
        ref.to_arrow()
        api.execute.assert_called_once()

    def test_to_arrow_caches_result(self):
        ref, api, _ = self._make_ref()
        ref.to_arrow()
        ref.to_arrow()
        api.execute.assert_called_once()

    def test_storage_type(self):
        ref, _, _ = self._make_ref()
        assert ref.storage_type == "databricks"

    def test_row_count(self):
        ref, _, _ = self._make_ref()
        assert ref.row_count == 3

    def test_size_bytes(self):
        ref, _, table = self._make_ref()
        assert ref.size_bytes == table.nbytes

    def test_schema_columns(self):
        ref, _, _ = self._make_ref()
        schema = ref.schema
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "a"
        assert schema.columns[1].name == "b"

    def test_is_materialized_ref(self):
        from rivet_core.strategies import MaterializedRef
        from rivet_databricks.adapters.unity import _DatabricksUnityMaterializedRef

        assert issubclass(_DatabricksUnityMaterializedRef, MaterializedRef)

    def test_none_catalog_and_schema(self):
        from unittest.mock import MagicMock

        import pyarrow

        from rivet_databricks.adapters.unity import _DatabricksUnityMaterializedRef

        api = MagicMock()
        api.execute.return_value = pyarrow.table({"x": [1]})
        ref = _DatabricksUnityMaterializedRef("SELECT 1", api, None, None)
        ref.to_arrow()
        api.execute.assert_called_once_with("SELECT 1", catalog=None, schema=None)


class TestWriteDispatchDelegation:
    """Task 4.3: Test write dispatch delegates to DatabricksSink SQL generation (mock API, verify SQL calls)."""

    def _make_fixtures(self):
        """Create engine, catalog, joint, and material fixtures for write dispatch tests."""
        engine = ComputeEngine(
            name="db_engine",
            engine_type="databricks",
            config={
                "workspace_url": "https://test.databricks.net",
                "token": "tok-123",
                "warehouse_id": "wh-456",
            },
        )
        catalog = Catalog(
            name="my_catalog",
            type="unity",
            options={"catalog_name": "prod", "schema": "analytics"},
        )
        arrow_table = pyarrow.table({"id": [1, 2], "name": ["a", "b"]})
        ref = _ArrowMaterializedRef(arrow_table)
        joint = Joint(
            name="my_joint",
            joint_type="sink",
            table="prod.analytics.my_table",
            write_strategy="replace",
        )
        material = Material(
            name="my_joint",
            catalog="my_catalog",
            state="materialized",
            materialized_ref=ref,
        )
        return engine, catalog, joint, material

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_calls_create_table_sql(self, mock_api_cls):
        """Verify _create_table_sql is called — CREATE TABLE IF NOT EXISTS appears in API calls."""
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        mock_api = MagicMock()
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_fixtures()

        adapter = DatabricksUnityAdapter()
        adapter.write_dispatch(engine, catalog, joint, material)

        sql_calls = [call.args[0] for call in mock_api.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in sql_calls), (
            f"Expected CREATE TABLE SQL in API calls, got: {sql_calls}"
        )

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_calls_build_values_sql(self, mock_api_cls):
        """Verify _build_values_sql is called — staging view with VALUES appears in API calls."""
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        mock_api = MagicMock()
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_fixtures()

        adapter = DatabricksUnityAdapter()
        adapter.write_dispatch(engine, catalog, joint, material)

        sql_calls = [call.args[0] for call in mock_api.execute.call_args_list]
        assert any("VALUES" in s and "TEMPORARY VIEW" in s for s in sql_calls), (
            f"Expected staging VALUES SQL in API calls, got: {sql_calls}"
        )

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_calls_generate_write_sql(self, mock_api_cls):
        """Verify _generate_write_sql is called — write strategy SQL appears in API calls."""
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        mock_api = MagicMock()
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_fixtures()

        adapter = DatabricksUnityAdapter()
        adapter.write_dispatch(engine, catalog, joint, material)

        sql_calls = [call.args[0] for call in mock_api.execute.call_args_list]
        # replace strategy generates CREATE OR REPLACE TABLE ... AS SELECT * FROM staging
        assert any("CREATE OR REPLACE TABLE" in s for s in sql_calls), (
            f"Expected write strategy SQL in API calls, got: {sql_calls}"
        )

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_returns_material_unchanged(self, mock_api_cls):
        """Verify write_dispatch returns the input material (identity)."""
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        mock_api = MagicMock()
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_fixtures()

        adapter = DatabricksUnityAdapter()
        result = adapter.write_dispatch(engine, catalog, joint, material)

        assert result is material

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_uses_correct_table_name(self, mock_api_cls):
        """Verify the three-part table name appears in the generated SQL."""
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        mock_api = MagicMock()
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_fixtures()

        adapter = DatabricksUnityAdapter()
        adapter.write_dispatch(engine, catalog, joint, material)

        sql_calls = [call.args[0] for call in mock_api.execute.call_args_list]
        assert any("prod.analytics.my_table" in s for s in sql_calls)

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_closes_api(self, mock_api_cls):
        """Verify the API is closed after write dispatch."""
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        mock_api = MagicMock()
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_fixtures()

        adapter = DatabricksUnityAdapter()
        adapter.write_dispatch(engine, catalog, joint, material)

        mock_api.close.assert_called_once()


def _make_http_error(status: int) -> ExecutionError:
    """Create an ExecutionError mimicking DatabricksStatementAPI HTTP error."""
    return ExecutionError(
        plugin_error(
            "RVT-502",
            f"Databricks Statement API error (HTTP {status}): error",
            plugin_name="rivet_databricks",
            plugin_type="engine",
            remediation="Check the Databricks workspace URL and warehouse configuration.",
            status=status,
        )
    )


def _make_requests_http_error(status: int):
    """Create a real requests.HTTPError with a mocked response."""
    import requests

    resp = MagicMock()
    resp.status_code = status
    return requests.HTTPError(f"HTTP {status}", response=resp)


class TestHTTPErrorHandling:
    """Task 4.4: Test HTTP error handling — mock API returning 400, 401, 403, 500 → verify RVT-502 with status code."""

    def _make_write_fixtures(self):
        engine = ComputeEngine(
            name="db_engine",
            engine_type="databricks",
            config={
                "workspace_url": "https://test.databricks.net",
                "token": "tok-123",
                "warehouse_id": "wh-456",
            },
        )
        catalog = Catalog(
            name="my_catalog",
            type="unity",
            options={"catalog_name": "prod", "schema": "analytics"},
        )
        joint = Joint(
            name="my_joint",
            joint_type="sink",
            table="prod.analytics.my_table",
            write_strategy="replace",
        )
        arrow_table = pyarrow.table({"id": [1]})
        ref = _ArrowMaterializedRef(arrow_table)
        material = Material(
            name="my_joint",
            catalog="my_catalog",
            state="materialized",
            materialized_ref=ref,
        )
        return engine, catalog, joint, material

    @pytest.mark.parametrize("status", [400, 401, 403, 500])
    def test_read_to_arrow_execution_error_passthrough(self, status):
        """Read path: api.execute raises ExecutionError(RVT-502) → to_arrow() re-raises."""
        from rivet_databricks.adapters.unity import _DatabricksUnityMaterializedRef

        api = MagicMock()
        api.execute.side_effect = _make_http_error(status)
        ref = _DatabricksUnityMaterializedRef("SELECT * FROM c.s.t", api, "c", "s")

        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()
        assert exc_info.value.error.code == "RVT-502"
        assert str(status) in exc_info.value.error.message

    @pytest.mark.parametrize("status", [400, 401, 403, 500])
    def test_read_to_arrow_requests_http_error_raises_rvt502(self, status):
        """Read path: api.execute raises requests.HTTPError → to_arrow() wraps as RVT-502."""
        from rivet_databricks.adapters.unity import _DatabricksUnityMaterializedRef

        api = MagicMock()
        api.execute.side_effect = _make_requests_http_error(status)
        ref = _DatabricksUnityMaterializedRef("SELECT * FROM c.s.t", api, "c", "s")

        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()
        err = exc_info.value.error
        assert err.code == "RVT-502"
        assert str(status) in err.message
        assert err.context["plugin_name"] == "rivet_databricks"
        assert err.context["plugin_type"] == "adapter"
        assert err.context["adapter"] == "DatabricksUnityAdapter"

    @pytest.mark.parametrize("status", [400, 401, 403, 500])
    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_execution_error_passthrough(self, mock_api_cls, status):
        """Write path: api.execute raises ExecutionError(RVT-502) → write_dispatch re-raises."""
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        mock_api = MagicMock()
        mock_api.execute.side_effect = _make_http_error(status)
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_write_fixtures()

        adapter = DatabricksUnityAdapter()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-502"
        assert str(status) in exc_info.value.error.message

    @pytest.mark.parametrize("status", [400, 401, 403, 500])
    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_requests_http_error_raises_rvt502(self, mock_api_cls, status):
        """Write path: api.execute raises requests.HTTPError → write_dispatch wraps as RVT-502."""
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        mock_api = MagicMock()
        mock_api.execute.side_effect = _make_requests_http_error(status)
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_write_fixtures()

        adapter = DatabricksUnityAdapter()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        err = exc_info.value.error
        assert err.code == "RVT-502"
        assert str(status) in err.message
        assert err.context["plugin_name"] == "rivet_databricks"
        assert err.context["plugin_type"] == "adapter"
        assert err.context["adapter"] == "DatabricksUnityAdapter"


class TestTimeoutConnectionErrorHandling:
    """Task 4.5: Test timeout/connection error handling — mock ConnectionError/Timeout → verify RVT-501."""

    def _make_write_fixtures(self):
        engine = ComputeEngine(
            name="db_engine",
            engine_type="databricks",
            config={
                "workspace_url": "https://test.databricks.net",
                "token": "tok-123",
                "warehouse_id": "wh-456",
            },
        )
        catalog = Catalog(
            name="my_catalog",
            type="unity",
            options={"catalog_name": "prod", "schema": "analytics"},
        )
        joint = Joint(
            name="my_joint",
            joint_type="sink",
            table="prod.analytics.my_table",
            write_strategy="replace",
        )
        arrow_table = pyarrow.table({"id": [1]})
        ref = _ArrowMaterializedRef(arrow_table)
        material = Material(
            name="my_joint",
            catalog="my_catalog",
            state="materialized",
            materialized_ref=ref,
        )
        return engine, catalog, joint, material

    @pytest.mark.parametrize("exc_cls", ["ConnectionError", "Timeout"])
    def test_read_to_arrow_connection_error_raises_rvt501(self, exc_cls):
        """Read path: api.execute raises ConnectionError/Timeout → to_arrow() raises RVT-501."""
        import requests as req

        from rivet_databricks.adapters.unity import _DatabricksUnityMaterializedRef

        exc = getattr(req, exc_cls)("unreachable")
        api = MagicMock()
        api.execute.side_effect = exc
        ref = _DatabricksUnityMaterializedRef("SELECT * FROM c.s.t", api, "c", "s")

        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()
        err = exc_info.value.error
        assert err.code == "RVT-501"
        assert err.context["plugin_name"] == "rivet_databricks"
        assert err.context["plugin_type"] == "adapter"
        assert err.context["adapter"] == "DatabricksUnityAdapter"

    @pytest.mark.parametrize("exc_cls", ["ConnectionError", "Timeout"])
    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    def test_write_dispatch_connection_error_raises_rvt501(self, mock_api_cls, exc_cls):
        """Write path: api.execute raises ConnectionError/Timeout → write_dispatch raises RVT-501."""
        import requests as req

        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        exc = getattr(req, exc_cls)("unreachable")
        mock_api = MagicMock()
        mock_api.execute.side_effect = exc
        mock_api_cls.return_value = mock_api
        engine, catalog, joint, material = self._make_write_fixtures()

        adapter = DatabricksUnityAdapter()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        err = exc_info.value.error
        assert err.code == "RVT-501"
        assert err.context["plugin_name"] == "rivet_databricks"
        assert err.context["plugin_type"] == "adapter"
        assert err.context["adapter"] == "DatabricksUnityAdapter"


class TestEdgeCases:
    """Task 4.6: Test edge cases — version=0, empty table name, whitespace table name."""

    def test_version_zero_produces_version_as_of_0(self):
        """version=0 is a valid version and must produce VERSION AS OF 0 in SQL."""
        from rivet_databricks.adapters.unity import _build_read_sql
        from rivet_databricks.unity_source import UnityDeferredMaterializedRef

        deferred = UnityDeferredMaterializedRef(
            table="cat.sch.tbl", catalog=MagicMock(), version=0, timestamp=None, partition_filter=None,
        )
        sql, _ = _build_read_sql("cat.sch.tbl", deferred, None)
        assert sql == "SELECT * FROM cat.sch.tbl VERSION AS OF 0"

    def test_empty_table_name_raises_rvt503(self):
        """Empty string table name must raise RVT-503."""
        from rivet_databricks.adapters.unity import _resolve_table_name

        joint = Joint(name="j", joint_type="source", table="")
        catalog = Catalog(name="c", type="unity", options={"catalog_name": "cat", "schema": "sch"})

        with patch("rivet_databricks.unity_catalog.UnityCatalogPlugin") as mock_plugin_cls:
            mock_plugin_cls.return_value.default_table_reference.return_value = ""
            with pytest.raises(ExecutionError) as exc_info:
                _resolve_table_name(joint, catalog)
            assert exc_info.value.error.code == "RVT-503"

    def test_whitespace_table_name_raises_rvt503(self):
        """Whitespace-only table name must raise RVT-503."""
        from rivet_databricks.adapters.unity import _resolve_table_name

        joint = Joint(name="j", joint_type="source", table="   ")
        catalog = Catalog(name="c", type="unity", options={})

        with pytest.raises(ExecutionError) as exc_info:
            _resolve_table_name(joint, catalog)
        assert exc_info.value.error.code == "RVT-503"
