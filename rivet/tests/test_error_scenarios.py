"""Error scenario tests for all error codes (task 38.4).

Covers every RVT error code that is implemented in the plugin source:
  RVT-201  Plugin validation: missing/unknown/invalid option
  RVT-202  Plugin validation: invalid format constraint / workspace_url scheme
  RVT-203  Plugin validation: missing Delta jars (PySpark Unity)
  RVT-204  Plugin validation: http_path missing / credential vending disabled
  RVT-205  Plugin validation: partial credential set (Databricks)
  RVT-206  Plugin validation: invalid table format (Databricks)
  RVT-207  Plugin validation: missing merge_key
  RVT-208  Plugin validation: liquid_clustering on Parquet
  RVT-301  Assembly: duplicate joint / missing upstream
  RVT-401  Compiler: no engine resolved
  RVT-402  Compiler: engine does not support catalog type
  RVT-501  Execution: connection/query failure
  RVT-502  Execution: auth failure / SQL Warehouse errors / time travel on non-Delta
  RVT-503  Execution: table/schema not found
  RVT-504  Execution: time travel on non-Delta (Databricks) / DuckDB postgres extension failure
  RVT-505  Execution: JDBC driver missing
  RVT-506  Execution: target table missing (create_table=False)
  RVT-508  Execution: credential vending disabled with no fallback
  RVT-701  SQL: parse failure
  RVT-702  SQL: non-SELECT statement
  RVT-703  SQL: dialect transpilation failed
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from rivet_core.errors import (
    ExecutionError,
    PluginValidationError,
    SQLParseError,
)

# ── RVT-201: Plugin validation — missing/unknown/invalid option ────────────────


class TestRVT201:
    """RVT-201: Plugin validation errors for missing, unknown, or invalid options."""

    def test_duckdb_catalog_unknown_option(self):
        from rivet_duckdb.catalog import DuckDBCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            DuckDBCatalogPlugin().validate({"unknown_opt": True})
        assert exc_info.value.error.code == "RVT-201"

    def test_duckdb_engine_invalid_threads(self):
        from rivet_duckdb.engine import DuckDBComputeEnginePlugin
        with pytest.raises(PluginValidationError) as exc_info:
            DuckDBComputeEnginePlugin().validate({"threads": "not_an_int"})
        assert exc_info.value.error.code == "RVT-201"

    def test_postgres_catalog_missing_host(self):
        from rivet_postgres.catalog import PostgresCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            PostgresCatalogPlugin().validate({"database": "db", "user": "u", "password": "p"})
        assert exc_info.value.error.code == "RVT-201"

    def test_postgres_catalog_missing_database(self):
        from rivet_postgres.catalog import PostgresCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            PostgresCatalogPlugin().validate({"host": "h", "user": "u", "password": "p"})
        assert exc_info.value.error.code == "RVT-201"

    def test_s3_catalog_missing_bucket(self):
        from rivet_aws.s3_catalog import S3CatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            S3CatalogPlugin().validate({})
        assert exc_info.value.error.code == "RVT-201"

    def test_glue_catalog_invalid_auth_type(self):
        from rivet_aws.glue_catalog import GlueCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            GlueCatalogPlugin().validate({"auth_type": "bad"})
        assert exc_info.value.error.code == "RVT-201"

    def test_unity_catalog_missing_host(self):
        from rivet_databricks.unity_catalog import UnityCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            UnityCatalogPlugin().validate({"catalog_name": "prod"})
        assert exc_info.value.error.code == "RVT-201"

    def test_unity_catalog_missing_catalog_name(self):
        from rivet_databricks.unity_catalog import UnityCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            UnityCatalogPlugin().validate({"host": "https://host.databricks.com"})
        assert exc_info.value.error.code == "RVT-201"

    def test_databricks_catalog_missing_workspace_url(self):
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({"catalog": "main"})
        assert exc_info.value.error.code == "RVT-201"

    def test_polars_engine_unknown_option(self):
        from rivet_polars.engine import PolarsComputeEnginePlugin
        with pytest.raises(PluginValidationError) as exc_info:
            PolarsComputeEnginePlugin().validate({"bad_opt": True})
        assert exc_info.value.error.code == "RVT-201"

    def test_pyspark_engine_unknown_option(self):
        from rivet_pyspark.engine import PySparkComputeEnginePlugin
        with pytest.raises(PluginValidationError) as exc_info:
            PySparkComputeEnginePlugin().validate({"bad_opt": True})
        assert exc_info.value.error.code == "RVT-201"

    def test_error_has_remediation(self):
        from rivet_aws.s3_catalog import S3CatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            S3CatalogPlugin().validate({})
        err = exc_info.value.error
        assert err.remediation is not None and err.remediation != ""

    def test_error_has_plugin_name_in_context(self):
        from rivet_postgres.catalog import PostgresCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            PostgresCatalogPlugin().validate({})
        assert exc_info.value.error.context.get("plugin_name") == "rivet_postgres"


# ── RVT-202: Plugin validation — invalid format / workspace_url scheme ─────────


class TestRVT202:
    """RVT-202: Invalid format constraint or workspace_url missing https:// scheme."""

    def test_databricks_catalog_http_scheme_rejected(self):
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({"workspace_url": "http://bad.com", "catalog": "main"})
        assert exc_info.value.error.code == "RVT-202"

    def test_databricks_catalog_no_scheme_rejected(self):
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({"workspace_url": "bad.com", "catalog": "main"})
        assert exc_info.value.error.code == "RVT-202"

    def test_s3_sink_merge_without_delta_rejected(self):
        from rivet_aws.s3_sink import _parse_sink_options
        cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "write_strategy": "merge", "format": "parquet"}}
        with pytest.raises(PluginValidationError) as exc_info:
            _parse_sink_options(cat_opts, SimpleNamespace(table=None, write_strategy=None))
        assert exc_info.value.error.code == "RVT-202"

    def test_s3_sink_scd2_without_delta_rejected(self):
        from rivet_aws.s3_sink import _parse_sink_options
        cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "write_strategy": "scd2", "format": "parquet"}}
        with pytest.raises(PluginValidationError) as exc_info:
            _parse_sink_options(cat_opts, SimpleNamespace(table=None, write_strategy=None))
        assert exc_info.value.error.code == "RVT-202"

    def test_glue_sink_merge_rejected(self):
        from rivet_aws.glue_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "merge"})
        assert exc_info.value.error.code == "RVT-202"

    def test_glue_sink_scd2_rejected(self):
        from rivet_aws.glue_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "scd2"})
        assert exc_info.value.error.code == "RVT-202"


# ── RVT-203: Plugin validation — missing Delta jars (PySpark Unity) ────────────


class TestRVT203:
    """RVT-203: Missing Delta Lake JARs for PySpark Unity adapter."""

    def _make_session_no_delta(self):
        session = MagicMock()
        jvm = MagicMock()
        def class_for_name(cls_name):
            if "delta" in cls_name.lower() or "DeltaTable" in cls_name:
                raise Exception("Class not found")
            return MagicMock()
        jvm.java.lang.Class.forName = class_for_name
        session._jvm = jvm
        return session

    def _make_unity_catalog(self):
        return SimpleNamespace(
            name="uc",
            options={"host": "https://host.databricks.com", "catalog_name": "prod", "token": "tok"},
        )

    def test_read_dispatch_fails_rvt203_when_delta_missing(self):
        from rivet_pyspark.adapters.unity import UnityPySparkAdapter
        session = self._make_session_no_delta()
        engine = SimpleNamespace(get_session=lambda: session)
        with pytest.raises(PluginValidationError) as exc_info:
            UnityPySparkAdapter().read_dispatch(engine, self._make_unity_catalog(), SimpleNamespace(name="j", table=None, sql=None))
        assert exc_info.value.error.code == "RVT-203"

    def test_write_dispatch_fails_rvt203_when_delta_missing(self):
        from rivet_pyspark.adapters.unity import UnityPySparkAdapter
        session = self._make_session_no_delta()
        engine = SimpleNamespace(get_session=lambda: session)
        material = SimpleNamespace(materialized_ref=SimpleNamespace(to_arrow=lambda: MagicMock()))
        with pytest.raises(PluginValidationError) as exc_info:
            UnityPySparkAdapter().write_dispatch(engine, self._make_unity_catalog(), SimpleNamespace(name="j", table=None, sql=None), material)
        assert exc_info.value.error.code == "RVT-203"


# ── RVT-204: Plugin validation — credential vending disabled ───────────────────


class TestRVT204:
    """RVT-204: Credential vending disabled for PySpark Unity adapter."""

    def _make_session_with_delta(self):
        session = MagicMock()
        jvm = MagicMock()
        jvm.java.lang.Class.forName = lambda _: MagicMock()
        session._jvm = jvm
        return session

    def _make_unity_catalog(self):
        return SimpleNamespace(
            name="uc",
            options={"host": "https://host.databricks.com", "catalog_name": "prod", "token": "tok"},
        )

    def test_read_dispatch_fails_rvt204_when_no_credentials(self):
        from rivet_pyspark.adapters.unity import UnityPySparkAdapter
        mock_plugin = MagicMock()
        mock_plugin.resolve_table_reference.return_value = {
            "storage_location": "s3://b/p",
            "file_format": "DELTA",
            "columns": [],
            "partition_columns": [],
            "table_type": "MANAGED",
            "temporary_credentials": None,
        }
        session = self._make_session_with_delta()
        engine = SimpleNamespace(get_session=lambda: session)
        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.read_dispatch(engine, self._make_unity_catalog(), SimpleNamespace(name="j", table=None, sql=None))
        assert exc_info.value.error.code == "RVT-204"


# ── RVT-205: Plugin validation — partial credential set ────────────────────────


class TestRVT205:
    """RVT-205: Partial credential set for Databricks authentication."""

    def test_client_id_without_secret_rejected(self):
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({
                "workspace_url": "https://host.databricks.com",
                "catalog": "main",
                "client_id": "id_only",
            })
        assert exc_info.value.error.code == "RVT-205"

    def test_client_secret_without_id_rejected(self):
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({
                "workspace_url": "https://host.databricks.com",
                "catalog": "main",
                "client_secret": "secret_only",
            })
        assert exc_info.value.error.code == "RVT-205"

    def test_azure_tenant_without_client_id_rejected(self):
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksCatalogPlugin().validate({
                "workspace_url": "https://host.databricks.com",
                "catalog": "main",
                "azure_tenant_id": "tenant",
            })
        assert exc_info.value.error.code == "RVT-205"


# ── RVT-206: Plugin validation — invalid table format (Databricks) ─────────────


class TestRVT206:
    """RVT-206: Invalid table format for Databricks sink (not delta/parquet)."""

    def test_invalid_format_rejected(self):
        from rivet_databricks.databricks_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "format": "orc"})
        assert exc_info.value.error.code == "RVT-206"

    def test_csv_format_rejected(self):
        from rivet_databricks.databricks_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "format": "csv"})
        assert exc_info.value.error.code == "RVT-206"


# ── RVT-207: Plugin validation — missing merge_key ─────────────────────────────


class TestRVT207:
    """RVT-207: Missing merge_key for merge/delete_insert/scd2 strategies."""

    def test_databricks_sink_merge_without_key(self):
        from rivet_databricks.databricks_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "merge"})
        assert exc_info.value.error.code == "RVT-207"

    def test_databricks_sink_scd2_without_key(self):
        from rivet_databricks.databricks_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "scd2"})
        assert exc_info.value.error.code == "RVT-207"

    def test_unity_sink_merge_without_key(self):
        from rivet_databricks.unity_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "merge"})
        assert exc_info.value.error.code == "RVT-207"

    def test_unity_sink_delete_insert_without_key(self):
        from rivet_databricks.unity_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "delete_insert"})
        assert exc_info.value.error.code == "RVT-207"


# ── RVT-208: Plugin validation — liquid_clustering on Parquet ─────────────────


class TestRVT208:
    """RVT-208: liquid_clustering specified on a Parquet-format Databricks table."""

    def test_liquid_clustering_on_parquet_rejected(self):
        from rivet_databricks.databricks_sink import _validate_sink_options
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({
                "table": "t",
                "format": "parquet",
                "liquid_clustering": ["col1"],
            })
        assert exc_info.value.error.code == "RVT-208"


# ── RVT-301: Assembly — duplicate joint / missing upstream ─────────────────────


class TestRVT301:
    """RVT-301: Assembly errors for duplicate joints or missing upstream references."""

    def test_duplicate_joint_name_raises_rvt301(self):
        from rivet_core.assembly import Assembly, AssemblyError
        from rivet_core.models import Joint
        j1 = Joint(name="dup", joint_type="source", catalog="c")
        j2 = Joint(name="dup", joint_type="source", catalog="c")
        with pytest.raises(AssemblyError) as exc_info:
            Assembly(joints=[j1, j2])
        assert exc_info.value.error.code == "RVT-301"

    def test_missing_upstream_raises_rvt302(self):
        from rivet_core.assembly import Assembly, AssemblyError
        from rivet_core.models import Joint
        j = Joint(name="j1", joint_type="sql", catalog="c", upstream=["nonexistent"])
        with pytest.raises(AssemblyError) as exc_info:
            Assembly(joints=[j])
        assert exc_info.value.error.code == "RVT-302"


# ── RVT-401/402: Compiler — engine resolution failures ────────────────────────


class TestRVT401_402:
    """RVT-401: No engine resolved. RVT-402: Engine does not support catalog type."""

    def _make_registry(self):
        from rivet_core.plugins import PluginRegistry
        reg = PluginRegistry()
        reg.register_builtins()
        return reg

    def test_no_engine_resolved_produces_rvt401(self):
        from rivet_core.assembly import Assembly
        from rivet_core.compiler import compile
        from rivet_core.models import Catalog, Joint
        catalog = Catalog(name="c", type="arrow", options={})
        joint = Joint(name="j1", joint_type="source", catalog="c")
        assembly = Assembly(joints=[joint])
        result = compile(assembly=assembly, catalogs=[catalog], engines=[], registry=self._make_registry())
        assert any(e.code == "RVT-401" for e in result.errors)

    def test_engine_does_not_support_catalog_type_produces_rvt402(self):
        from rivet_core.assembly import Assembly
        from rivet_core.compiler import compile
        from rivet_core.models import Catalog, ComputeEngine, Joint
        from rivet_polars.engine import PolarsComputeEnginePlugin
        reg = self._make_registry()
        reg.register_engine_plugin(PolarsComputeEnginePlugin())
        # Polars does not natively support duckdb catalog type
        catalog = Catalog(name="ddb", type="duckdb", options={})
        engine = ComputeEngine(name="polars_eng", engine_type="polars")
        joint = Joint(name="j1", joint_type="source", catalog="ddb", engine="polars_eng")
        assembly = Assembly(joints=[joint])
        result = compile(assembly=assembly, catalogs=[catalog], engines=[engine], registry=reg)
        assert any(e.code == "RVT-402" for e in result.errors)


# ── RVT-501: Execution — connection/query failure ──────────────────────────────


class TestRVT501:
    """RVT-501: Execution failures for connection or query errors."""

    def test_duckdb_filesystem_unrecognized_extension(self):
        from rivet_duckdb.engine import infer_filesystem_reader
        with pytest.raises(ExecutionError) as exc_info:
            infer_filesystem_reader("data.xyz")
        assert exc_info.value.error.code == "RVT-501"

    def test_postgres_source_connectivity_error(self):
        from rivet_postgres.source import PostgresDeferredMaterializedRef
        ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT 1")
        mock_psycopg = MagicMock()
        from unittest.mock import AsyncMock
        mock_psycopg.AsyncConnection.connect = AsyncMock(
            side_effect=Exception("could not connect to server: Connection refused")
        )
        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            with pytest.raises(ExecutionError) as exc_info:
                ref.to_arrow()
        assert exc_info.value.error.code == "RVT-501"

    def test_polars_sql_context_unregistered_table(self):
        from rivet_polars.engine import PolarsComputeEnginePlugin
        plugin = PolarsComputeEnginePlugin()
        # Execute SQL referencing a table not registered in the SQLContext
        with pytest.raises(ExecutionError) as exc_info:
            material = plugin.execute_sql_lazy("SELECT * FROM nonexistent_table", upstream_frames={})
            material.to_arrow()
        assert exc_info.value.error.code == "RVT-501"


# ── RVT-502: Execution — auth failure / SQL Warehouse errors ──────────────────


class TestRVT502:
    """RVT-502: Auth failures, SQL Warehouse FAILED/CANCELED, time travel on non-Delta."""

    def test_postgres_source_auth_error(self):
        from rivet_postgres.source import PostgresDeferredMaterializedRef
        ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT 1")
        mock_psycopg = MagicMock()
        from unittest.mock import AsyncMock
        mock_psycopg.AsyncConnection.connect = AsyncMock(
            side_effect=Exception("FATAL: password authentication failed for user \"alice\"")
        )
        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            with pytest.raises(ExecutionError) as exc_info:
                ref.to_arrow()
        assert exc_info.value.error.code == "RVT-502"

    def test_databricks_statement_api_failed_state(self):
        from rivet_databricks.engine import DatabricksStatementAPI
        api = DatabricksStatementAPI(
            workspace_url="https://host.databricks.com",
            token="tok",
            warehouse_id="wh1",
        )
        submit_resp = MagicMock()
        submit_resp.ok = True
        submit_resp.json.return_value = {"statement_id": "s1", "status": {"state": "PENDING"}}
        poll_resp = MagicMock()
        poll_resp.ok = True
        poll_resp.json.return_value = {
            "statement_id": "s1",
            "status": {"state": "FAILED", "error": {"message": "Syntax error"}},
        }
        with patch.object(api._session, "post", return_value=submit_resp):
            with patch.object(api._session, "get", return_value=poll_resp):
                with pytest.raises(ExecutionError) as exc_info:
                    api.execute("SELECT bad")
        assert exc_info.value.error.code == "RVT-502"

    def test_databricks_statement_api_canceled_state(self):
        from rivet_databricks.engine import DatabricksStatementAPI
        api = DatabricksStatementAPI(
            workspace_url="https://host.databricks.com",
            token="tok",
            warehouse_id="wh1",
        )
        submit_resp = MagicMock()
        submit_resp.ok = True
        submit_resp.json.return_value = {"statement_id": "s2", "status": {"state": "PENDING"}}
        poll_resp = MagicMock()
        poll_resp.ok = True
        poll_resp.json.return_value = {"statement_id": "s2", "status": {"state": "CANCELED"}}
        with patch.object(api._session, "post", return_value=submit_resp):
            with patch.object(api._session, "get", return_value=poll_resp):
                with pytest.raises(ExecutionError) as exc_info:
                    api.execute("SELECT 1")
        assert exc_info.value.error.code == "RVT-502"

    def test_duckdb_extension_load_failure(self):
        import duckdb

        from rivet_duckdb.extensions import ensure_extension
        conn = duckdb.connect(":memory:")
        with pytest.raises(ExecutionError) as exc_info:
            ensure_extension(conn, "nonexistent_extension_xyz_abc")
        assert exc_info.value.error.code == "RVT-502"

    def test_unity_source_time_travel_on_non_delta(self):
        from rivet_databricks.unity_source import UnityDeferredMaterializedRef
        catalog = SimpleNamespace(name="uc", options={})
        ref = UnityDeferredMaterializedRef(
            table="prod.default.users",
            catalog=catalog,
            version=5,
            timestamp=None,
            partition_filter=None,
        )
        with pytest.raises(ExecutionError) as exc_info:
            ref.check_time_travel_format("PARQUET")
        assert exc_info.value.error.code == "RVT-502"


# ── RVT-503: Execution — table/schema not found ────────────────────────────────


class TestRVT503:
    """RVT-503: Table or schema not found in metadata API."""

    def test_glue_source_table_not_found(self):
        from rivet_aws.glue_source import GlueSource
        from rivet_core.models import Catalog, Joint
        catalog = Catalog(name="glue_cat", type="glue", options={"database": "mydb", "region": "us-east-1"})
        joint = Joint(name="j1", joint_type="source", catalog="glue_cat", table="nonexistent_table")
        mock_client = MagicMock()
        mock_client.get_table.side_effect = Exception("EntityNotFoundException: Table not found")
        with patch("rivet_aws.glue_catalog._make_glue_client", return_value=mock_client):
            with pytest.raises(ExecutionError) as exc_info:
                GlueSource().read(catalog, joint, {})
        assert exc_info.value.error.code == "RVT-503"

    def test_unity_client_404_raises_rvt503(self):
        import requests

        from rivet_databricks.auth import AUTH_TYPE_PAT, ResolvedCredential
        from rivet_databricks.client import UnityCatalogClient
        cred = ResolvedCredential(auth_type=AUTH_TYPE_PAT, token="tok", source="explicit")
        client = UnityCatalogClient(host="https://host.databricks.com", credential=cred)
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.json.return_value = {"message": "Not found"}
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        with patch.object(client._session, "request", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.list_catalogs()
        assert exc_info.value.error.code == "RVT-503"


# ── RVT-504: Execution — time travel on non-Delta (Databricks) ────────────────


class TestRVT504:
    """RVT-504: Time travel on non-Delta table (Databricks source) or DuckDB postgres extension failure."""

    def test_databricks_source_time_travel_on_non_delta(self):
        from rivet_databricks.databricks_source import DatabricksDeferredMaterializedRef
        ref = DatabricksDeferredMaterializedRef(
            table="main.default.users",
            sql="SELECT * FROM main.default.users VERSION AS OF 5",
            version=5,
            change_data_feed=False,
        )
        with pytest.raises(ExecutionError) as exc_info:
            ref.check_time_travel_format("PARQUET")
        assert exc_info.value.error.code == "RVT-504"

    def test_postgres_duckdb_adapter_extension_failure(self):
        from rivet_core.models import Catalog, Joint
        from rivet_postgres.adapters.duckdb import PostgresDuckDBAdapter
        catalog = Catalog(
            name="pg",
            type="postgres",
            options={"host": "localhost", "database": "db", "user": "u", "password": "p"},
        )
        joint = Joint(name="j1", joint_type="source", catalog="pg", sql="SELECT 1")
        adapter = PostgresDuckDBAdapter()
        result = adapter.read_dispatch(MagicMock(), catalog, joint)
        material = result.material
        # Patch _ensure_duckdb_extension at its source module to raise inside the try block → triggers RVT-504
        with patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension", side_effect=Exception("postgres extension not available")):
            with pytest.raises(ExecutionError) as exc_info:
                material.materialized_ref.to_arrow()
        assert exc_info.value.error.code == "RVT-504"


# ── RVT-505: Execution — JDBC driver missing / CDF without enablement ──────────


class TestRVT505:
    """RVT-505: JDBC driver missing (PostgresPySparkAdapter) or CDF not enabled."""

    def test_postgres_pyspark_adapter_missing_jdbc_jar(self):
        from rivet_postgres.adapters.pyspark import PostgresPySparkAdapter
        session = MagicMock()
        jvm = MagicMock()
        jvm.java.lang.Class.forName.side_effect = Exception("Class not found")
        session._jvm = jvm
        engine = SimpleNamespace(get_session=lambda: session)
        catalog = SimpleNamespace(
            name="pg",
            options={"host": "localhost", "database": "db", "user": "u", "password": "p"},
        )
        joint = SimpleNamespace(name="j1", table=None, sql="SELECT 1")
        with pytest.raises(ExecutionError) as exc_info:
            PostgresPySparkAdapter().read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-505"

    def test_databricks_source_cdf_not_enabled(self):
        from rivet_databricks.databricks_source import DatabricksDeferredMaterializedRef
        ref = DatabricksDeferredMaterializedRef(
            table="main.default.events",
            sql="SELECT * FROM table_changes('main.default.events')",
            version=None,
            change_data_feed=True,
        )
        with pytest.raises(ExecutionError) as exc_info:
            ref.check_cdf_enabled(cdf_enabled=False)
        assert exc_info.value.error.code == "RVT-505"


# ── RVT-506: Execution — target table missing (create_table=False) ─────────────


class TestRVT506:
    """RVT-506: Target table does not exist and create_table=False."""

    def test_glue_sink_table_missing_no_create(self):
        import pyarrow as pa

        from rivet_aws.glue_sink import GlueSink
        from rivet_core.models import Catalog, Joint, Material
        from rivet_core.strategies import MaterializedRef

        class _Ref(MaterializedRef):
            def to_arrow(self): return pa.table({"id": [1]})
            @property
            def schema(self): return None
            @property
            def row_count(self): return 1
            @property
            def size_bytes(self): return None
            @property
            def storage_type(self): return "test"

        catalog = Catalog(name="glue_cat", type="glue", options={"database": "mydb", "region": "us-east-1"})
        joint = Joint(name="j1", joint_type="sink", catalog="glue_cat", table="missing_table")
        joint.sink_options = {"table": "missing_table", "create_table": False, "write_strategy": "replace"}
        material = Material(name="j1", catalog="glue_cat", materialized_ref=_Ref(), state="materialized")

        mock_client = MagicMock()
        mock_client.get_table.side_effect = Exception("EntityNotFoundException")
        with patch("rivet_aws.glue_catalog._make_glue_client", return_value=mock_client):
            with pytest.raises(ExecutionError) as exc_info:
                GlueSink().write(catalog, joint, material, "replace")
        assert exc_info.value.error.code == "RVT-506"


# ── RVT-508: Execution — credential vending disabled with no fallback ──────────


class TestRVT508:
    """RVT-508: Credential vending disabled and no fallback available."""

    def test_unity_client_vend_credentials_disabled(self):
        import requests

        from rivet_databricks.auth import AUTH_TYPE_PAT, ResolvedCredential
        from rivet_databricks.client import UnityCatalogClient
        cred = ResolvedCredential(auth_type=AUTH_TYPE_PAT, token="tok", source="explicit")
        client = UnityCatalogClient(host="https://host.databricks.com", credential=cred)
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.json.return_value = {"error_code": "CREDENTIAL_VENDING_DISABLED", "message": "Disabled"}
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
        with patch.object(client._session, "request", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.vend_credentials("main.default.users", "READ")
        assert exc_info.value.error.code == "RVT-508"


# ── RVT-701/702/703: SQL errors ────────────────────────────────────────────────


class TestSQLErrors:
    """RVT-701: SQL parse failure. RVT-702: Non-SELECT statement. RVT-703: Transpilation failed."""

    def _parser(self):
        from rivet_core.sql_parser import SQLParser
        return SQLParser()

    def test_rvt701_empty_sql(self):
        with pytest.raises(SQLParseError) as exc_info:
            self._parser().parse("")
        assert exc_info.value.error.code in ("RVT-701", "RVT-702")

    def test_rvt702_drop_table_rejected(self):
        with pytest.raises(SQLParseError) as exc_info:
            self._parser().parse("DROP TABLE users")
        assert exc_info.value.error.code == "RVT-702"

    def test_rvt702_insert_statement_rejected(self):
        with pytest.raises(SQLParseError) as exc_info:
            self._parser().parse("INSERT INTO t VALUES (1)")
        assert exc_info.value.error.code == "RVT-702"

    def test_rvt702_update_statement_rejected(self):
        with pytest.raises(SQLParseError) as exc_info:
            self._parser().parse("UPDATE t SET col = 1")
        assert exc_info.value.error.code == "RVT-702"

    def test_rvt703_transpilation_empty_output(self):
        """RVT-703 raised when translation produces no output."""
        parser = self._parser()
        ast = parser.parse("SELECT 1")
        # Patch sqlglot.transpile to return empty list
        with patch("sqlglot.transpile", return_value=[]):
            with pytest.raises(SQLParseError) as exc_info:
                parser.translate(ast, source_dialect="duckdb", target_dialect="postgres")
        assert exc_info.value.error.code == "RVT-703"

    def test_rvt703_transpilation_exception(self):
        """RVT-703 raised when sqlglot.transpile raises."""
        parser = self._parser()
        ast = parser.parse("SELECT 1")
        with patch("sqlglot.transpile", side_effect=Exception("transpile error")):
            with pytest.raises(SQLParseError) as exc_info:
                parser.translate(ast, source_dialect="duckdb", target_dialect="postgres")
        assert exc_info.value.error.code == "RVT-703"
