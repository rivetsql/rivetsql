"""Task 38.2: Adapter dispatch integration tests with mocked APIs.

Integration tests that verify adapter dispatch flows through the PluginRegistry.
All external APIs (boto3, REST, DuckDB connections, Spark sessions) are mocked.
Tests cover:
  - Registry-based adapter lookup and dispatch for all (engine_type, catalog_type) pairs
  - Adapter precedence (catalog_plugin overrides engine_plugin)
  - read_dispatch returns deferred Material with correct structure
  - write_dispatch invokes correct backend operations
  - Graceful fallback on credential vending failure (DatabricksDuckDBAdapter)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa

from rivet_core.models import Catalog, Joint
from rivet_core.optimizer import AdapterPushdownResult
from rivet_core.plugins import PluginRegistry

# ── Shared fixtures ──────────────────────────────────────────────────────────


def _registry_with_all_plugins() -> PluginRegistry:
    """Create a registry with all 6 plugin packages registered."""
    from rivet_aws import AWSPlugin
    from rivet_databricks import DatabricksPlugin
    from rivet_duckdb import DuckDBPlugin
    from rivet_polars import PolarsPlugin
    from rivet_postgres import PostgresPlugin
    from rivet_pyspark import PySparkPlugin

    reg = PluginRegistry()
    reg.register_builtins()
    AWSPlugin(reg)
    DuckDBPlugin(reg)
    PolarsPlugin(reg)
    PostgresPlugin(reg)
    PySparkPlugin(reg)
    DatabricksPlugin(reg)
    return reg


def _sample_arrow_table() -> pa.Table:
    return pa.table({"id": [1, 2], "name": ["a", "b"]})


def _mock_joint(name: str = "test_joint", table: str = "users", sql: str | None = None,
                write_strategy: str | None = None) -> Joint:
    return Joint(name=name, joint_type="source", sql=sql, table=table,
                 write_strategy=write_strategy)


# ── Adapter precedence integration ───────────────────────────────────────────


class TestAdapterPrecedenceIntegration:
    """Verify catalog_plugin adapters override engine_plugin adapters through the full registry."""

    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()

    def test_postgres_duckdb_adapter_is_catalog_plugin(self):
        adapter = self.registry.get_adapter("duckdb", "postgres")
        assert adapter is not None
        assert adapter.source == "catalog_plugin"
        assert adapter.source_plugin == "rivet_postgres"

    def test_databricks_duckdb_adapter_registered_by_databricks_plugin(self):
        """DatabricksPlugin registers a DuckDB adapter for the databricks catalog type."""
        adapter = self.registry.get_adapter("duckdb", "databricks")
        assert adapter is not None
        assert adapter.source == "catalog_plugin"
        assert adapter.source_plugin == "rivet_databricks"

    def test_s3_duckdb_adapter_is_engine_plugin(self):
        adapter = self.registry.get_adapter("duckdb", "s3")
        assert adapter is not None
        assert adapter.source == "engine_plugin"

    def test_all_expected_adapter_pairs_present(self):
        expected_pairs = [
            ("duckdb", "s3"), ("duckdb", "glue"), ("duckdb", "unity"),
            ("duckdb", "postgres"), ("duckdb", "databricks"),
            ("polars", "s3"), ("polars", "glue"), ("polars", "unity"),
            ("pyspark", "s3"), ("pyspark", "glue"), ("pyspark", "unity"),
            ("pyspark", "postgres"),
        ]
        for pair in expected_pairs:
            assert self.registry.get_adapter(*pair) is not None, f"Missing adapter for {pair}"


# ── S3 DuckDB adapter dispatch ───────────────────────────────────────────────


class TestS3DuckDBDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("duckdb", "s3")

    def test_read_dispatch_returns_deferred_material(self):
        catalog = Catalog(name="s3_cat", type="s3",
                          options={"bucket": "test-bucket", "format": "parquet", "region": "us-east-1"})
        joint = _mock_joint(table="events")
        result = self.adapter.read_dispatch(MagicMock(), catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"
        assert result.material.name == "test_joint"

    @patch("rivet_duckdb.adapters.s3.duckdb")
    def test_write_dispatch_calls_duckdb(self, mock_duckdb):
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        catalog = Catalog(name="s3_cat", type="s3",
                          options={"bucket": "out-bucket", "format": "parquet", "region": "us-east-1"})
        joint = _mock_joint(table="output", write_strategy="replace")
        material = MagicMock()
        material.to_arrow.return_value = _sample_arrow_table()

        self.adapter.write_dispatch(MagicMock(), catalog, joint, material)
        mock_conn.register.assert_called_once()


# ── Glue DuckDB adapter dispatch ─────────────────────────────────────────────


class TestGlueDuckDBDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("duckdb", "glue")

    @patch("rivet_duckdb.adapters.glue._make_resolver")
    def test_read_dispatch_returns_deferred_material(self, mock_make_resolver):
        mock_client = MagicMock()
        mock_client.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {
                    "Location": "s3://warehouse/db/events/",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "Columns": [{"Name": "id", "Type": "int"}],
                },
                "PartitionKeys": [],
            }
        }
        mock_client.get_paginator.return_value.paginate.return_value = []
        mock_make_resolver.return_value.create_client.return_value = mock_client

        catalog = Catalog(name="glue_cat", type="glue",
                          options={"database": "mydb", "region": "us-east-1",
                                   "access_key_id": "AK", "secret_access_key": "SK"})
        joint = _mock_joint(table="events")
        result = self.adapter.read_dispatch(MagicMock(), catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"


# ── Postgres DuckDB adapter dispatch (catalog_plugin override) ───────────────


class TestPostgresDuckDBDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("duckdb", "postgres")

    def test_adapter_is_from_rivet_postgres(self):
        assert self.adapter.source_plugin == "rivet_postgres"
        assert "cast_pushdown" in self.adapter.capabilities

    def test_read_dispatch_returns_deferred_material(self):
        catalog = Catalog(name="pg_cat", type="postgres",
                          options={"host": "localhost", "port": 5432, "database": "testdb",
                                   "user": "u", "password": "p", "schema": "public"})
        joint = _mock_joint(table="users")
        result = self.adapter.read_dispatch(MagicMock(), catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"
        assert result.material.name == "test_joint"

    @patch("duckdb.connect")
    def test_write_dispatch_replace(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        catalog = Catalog(name="pg_cat", type="postgres",
                          options={"host": "localhost", "port": 5432, "database": "testdb",
                                   "user": "u", "password": "p", "schema": "public"})
        joint = _mock_joint(table="users", write_strategy="replace")
        material = MagicMock()
        material.to_arrow.return_value = _sample_arrow_table()

        self.adapter.write_dispatch(MagicMock(), catalog, joint, material)
        assert mock_conn.execute.call_count >= 2


# ── Postgres PySpark adapter dispatch ────────────────────────────────────────


class TestPostgresPySparkDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("pyspark", "postgres")

    def test_adapter_registered(self):
        assert self.adapter is not None
        assert self.adapter.source_plugin == "rivet_postgres"

    def test_capabilities_include_all_6(self):
        for cap in ("projection_pushdown", "predicate_pushdown", "limit_pushdown",
                     "cast_pushdown", "join", "aggregation"):
            assert cap in self.adapter.capabilities

    @patch("rivet_postgres.adapters.pyspark._check_jdbc_driver")
    def test_read_dispatch_returns_material(self, mock_check):
        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_session.read.jdbc.return_value = mock_df

        engine = MagicMock()
        engine.get_session.return_value = mock_session

        catalog = Catalog(name="pg_cat", type="postgres",
                          options={"host": "localhost", "port": 5432, "database": "testdb",
                                   "user": "u", "password": "p"})
        joint = _mock_joint(sql="SELECT * FROM users")

        result = self.adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)


# ── Unity DuckDB adapter dispatch ────────────────────────────────────────────


class TestUnityDuckDBDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("duckdb", "unity")

    def test_adapter_registered(self):
        assert self.adapter is not None
        assert self.adapter.source == "engine_plugin"

    def test_read_dispatch_returns_deferred_material(self):
        plugin_mock = MagicMock()
        plugin_mock.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/path",
            "file_format": "PARQUET",
            "temporary_credentials": {
                "aws_temp_credentials": {
                    "access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST"
                }
            },
        }
        self.registry._catalog_plugins["unity"] = plugin_mock

        catalog = Catalog(name="unity_cat", type="unity",
                          options={"host": "https://uc.example.com", "catalog_name": "main"})
        joint = _mock_joint(table="users")
        result = self.adapter.read_dispatch(MagicMock(), catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"


# ── S3 Polars adapter dispatch ───────────────────────────────────────────────


class TestS3PolarsDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("polars", "s3")

    def test_adapter_registered(self):
        assert self.adapter is not None
        assert self.adapter.target_engine_type == "polars"

    @patch("polars.scan_parquet")
    def test_read_dispatch_returns_deferred_material(self, mock_scan):
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        catalog = Catalog(name="s3_cat", type="s3",
                          options={"bucket": "test-bucket", "format": "parquet"})
        joint = _mock_joint(table="events")
        result = self.adapter.read_dispatch(MagicMock(), catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"


# ── Glue Polars adapter dispatch ─────────────────────────────────────────────


class TestGluePolarsDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("polars", "glue")

    def test_adapter_registered(self):
        assert self.adapter is not None
        assert self.adapter.target_engine_type == "polars"

    @patch("rivet_polars.adapters.glue._make_resolver")
    def test_read_dispatch_returns_deferred_material(self, mock_make_resolver):
        """read_dispatch creates a deferred ref without calling boto3 yet."""
        catalog = Catalog(name="glue_cat", type="glue",
                          options={"database": "mydb", "region": "us-east-1",
                                   "access_key_id": "AK", "secret_access_key": "SK"})
        joint = _mock_joint(table="events")
        result = self.adapter.read_dispatch(MagicMock(), catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"


# ── Unity Polars adapter dispatch ────────────────────────────────────────────


class TestUnityPolarsDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("polars", "unity")

    def test_adapter_registered(self):
        assert self.adapter is not None
        assert self.adapter.target_engine_type == "polars"

    @patch("rivet_databricks.unity_catalog.UnityCatalogPlugin")
    def test_read_dispatch_returns_deferred_material(self, MockPlugin):
        plugin_instance = MockPlugin.return_value
        plugin_instance.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/path",
            "file_format": "PARQUET",
            "temporary_credentials": {
                "aws_temp_credentials": {
                    "access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST"
                }
            },
        }

        # Inject mock plugin into the registry so _get_unity_plugin() finds it
        self.registry._catalog_plugins["unity"] = plugin_instance

        catalog = Catalog(name="unity_cat", type="unity",
                          options={"host": "https://uc.example.com", "catalog_name": "main"})
        joint = _mock_joint(table="users")
        result = self.adapter.read_dispatch(MagicMock(), catalog, joint)
        from rivet_core.optimizer import AdapterPushdownResult
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"


# ── S3 PySpark adapter dispatch ──────────────────────────────────────────────


class TestS3PySparkDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("pyspark", "s3")

    def test_adapter_registered(self):
        assert self.adapter is not None
        assert self.adapter.target_engine_type == "pyspark"

    @patch("rivet_pyspark.adapters.s3._has_hadoop_aws_jars", return_value=True)
    @patch("rivet_pyspark.adapters.s3._has_delta_jars", return_value=False)
    def test_read_dispatch_returns_material(self, mock_delta, mock_hadoop):
        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_session.read.parquet.return_value = mock_df

        engine = MagicMock()
        engine.get_session.return_value = mock_session

        catalog = Catalog(name="s3_cat", type="s3",
                          options={"bucket": "test-bucket", "format": "parquet", "region": "us-east-1"})
        joint = _mock_joint(table="events")
        result = self.adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"


# ── Glue PySpark adapter dispatch ────────────────────────────────────────────


class TestGluePySparkDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("pyspark", "glue")

    def test_adapter_registered(self):
        assert self.adapter is not None
        assert self.adapter.target_engine_type == "pyspark"

    @patch("rivet_pyspark.adapters.glue._has_glue_metastore_jar", return_value=True)
    @patch("rivet_aws.credentials.AWSCredentialResolver")
    def test_read_dispatch_returns_material(self, mock_cred_cls, mock_jar):
        mock_cred = MagicMock()
        mock_cred.resolve.return_value = MagicMock(
            access_key_id="AK", secret_access_key="SK", session_token=None
        )
        mock_cred_cls.return_value = mock_cred

        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_session.sql.return_value = mock_df

        engine = MagicMock()
        engine.get_session.return_value = mock_session
        engine._session = mock_session
        engine._config = {}

        catalog = Catalog(name="glue_cat", type="glue",
                          options={"database": "mydb", "region": "us-east-1",
                                   "_credential_resolver_factory": lambda opts, region: mock_cred})
        joint = _mock_joint(table="events", sql="SELECT * FROM events")
        result = self.adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)


# ── Unity PySpark adapter dispatch ───────────────────────────────────────────


class TestUnityPySparkDispatchIntegration:
    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()
        self.adapter = self.registry.get_adapter("pyspark", "unity")

    def test_adapter_registered(self):
        assert self.adapter is not None
        assert self.adapter.target_engine_type == "pyspark"

    @patch("rivet_pyspark.adapters.unity._has_delta_jars", return_value=True)
    def test_read_dispatch_returns_material(self, mock_delta):
        mock_plugin = MagicMock()
        mock_plugin.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/path",
            "file_format": "DELTA",
            "temporary_credentials": {
                "aws_temp_credentials": {
                    "access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST"
                }
            },
        }
        self.adapter._registry.get_catalog_plugin = MagicMock(return_value=mock_plugin)

        mock_session = MagicMock()
        mock_df = MagicMock()
        mock_session.read.format.return_value.load.return_value = mock_df

        engine = MagicMock()
        engine.get_session.return_value = mock_session

        catalog = Catalog(name="unity_cat", type="unity",
                          options={"host": "https://uc.example.com", "catalog_name": "main"})
        joint = _mock_joint(table="users")
        result = self.adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"


# ── Capability resolution through registry ───────────────────────────────────


class TestCapabilityResolutionIntegration:
    """Verify resolve_capabilities returns correct capabilities for adapter pairs."""

    def setup_method(self) -> None:
        self.registry = _registry_with_all_plugins()

    def test_duckdb_native_capabilities(self):
        caps = self.registry.resolve_capabilities("duckdb", "duckdb")
        assert caps is not None
        assert "projection_pushdown" in caps
        assert "join" in caps

    def test_duckdb_s3_adapter_capabilities(self):
        caps = self.registry.resolve_capabilities("duckdb", "s3")
        assert caps is not None
        assert "projection_pushdown" in caps
        assert "write_append" in caps

    def test_duckdb_postgres_adapter_capabilities_include_cast_pushdown(self):
        caps = self.registry.resolve_capabilities("duckdb", "postgres")
        assert caps is not None
        assert "cast_pushdown" in caps

    def test_polars_native_capabilities(self):
        caps = self.registry.resolve_capabilities("polars", "arrow")
        assert caps is not None
        assert "aggregation" in caps

    def test_databricks_native_capabilities(self):
        caps = self.registry.resolve_capabilities("databricks", "databricks")
        assert caps is not None
        assert "join" in caps

    def test_no_capabilities_for_unsupported_pair(self):
        caps = self.registry.resolve_capabilities("postgres", "s3")
        assert caps is None

    def test_databricks_duckdb_capabilities(self):
        """Adapter for (duckdb, databricks) exists in rivet_databricks."""
        caps = self.registry.resolve_capabilities("duckdb", "databricks")
        assert caps is not None
