"""Tests for GluePySparkAdapter: AWSGlueDataCatalogHiveClientFactory before session creation."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Material
from rivet_pyspark.adapters.glue import (
    GLUE_METASTORE_FACTORY,
    GluePySparkAdapter,
    _ensure_glue_config,
    _glue_spark_config,
)

# --- Fixtures ---

def _make_creds(
    access_key_id: str = "AKID",
    secret_access_key: str = "SECRET",
    session_token: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
    )


def _mock_resolver_factory(creds: SimpleNamespace | None = None) -> Any:
    """Return a factory function that produces a mock CredentialResolver."""
    if creds is None:
        creds = _make_creds()
    resolver = SimpleNamespace(resolve=lambda: creds)
    return lambda options, region: resolver


def _make_catalog(options: dict[str, Any] | None = None, creds: SimpleNamespace | None = None) -> SimpleNamespace:
    opts: dict[str, Any] = {"database": "mydb", "region": "us-west-2"}
    if options:
        opts.update(options)
    opts.setdefault("_credential_resolver_factory", _mock_resolver_factory(creds))
    return SimpleNamespace(name="glue_cat", options=opts)


def _make_joint(name: str = "my_table", table: str | None = None, **kwargs: Any) -> SimpleNamespace:
    return SimpleNamespace(name=name, table=table, sql=None, **kwargs)


def _make_engine(session_exists: bool = False) -> SimpleNamespace:
    session = MagicMock()
    engine = SimpleNamespace(
        _session=session if session_exists else None,
        _config={"config": {}},
        get_session=lambda: session,
    )
    return engine


def _make_material() -> SimpleNamespace:
    mock_arrow = MagicMock()
    mock_arrow.to_pandas.return_value = MagicMock()
    ref = SimpleNamespace(to_arrow=lambda: mock_arrow)
    return SimpleNamespace(materialized_ref=ref)


# --- Registration tests ---

class TestGluePySparkAdapterRegistration:
    def test_target_engine_type(self) -> None:
        assert GluePySparkAdapter.target_engine_type == "pyspark"

    def test_catalog_type(self) -> None:
        assert GluePySparkAdapter.catalog_type == "glue"

    def test_source(self) -> None:
        assert GluePySparkAdapter.source == "engine_plugin"
        assert GluePySparkAdapter.source_plugin == "rivet_pyspark"

    def test_capabilities(self) -> None:
        adapter = GluePySparkAdapter()
        for cap in [
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
            "write_append", "write_replace", "write_partition", "write_delete_insert",
        ]:
            assert cap in adapter.capabilities


# --- Glue Spark config generation ---

class TestGlueSparkConfig:
    def test_basic_config(self) -> None:
        opts: dict[str, Any] = {"database": "db", "region": "eu-west-1", "_credential_resolver_factory": _mock_resolver_factory()}
        conf = _glue_spark_config(opts)
        assert conf["spark.sql.catalogImplementation"] == "hive"
        assert conf["hive.metastore.client.factory.class"] == GLUE_METASTORE_FACTORY
        assert conf["spark.hadoop.aws.glue.catalog.region"] == "eu-west-1"
        assert conf["spark.hadoop.fs.s3a.access.key"] == "AKID"
        assert conf["spark.hadoop.fs.s3a.secret.key"] == "SECRET"

    def test_with_catalog_id(self) -> None:
        opts: dict[str, Any] = {"database": "db", "region": "us-east-1", "catalog_id": "123456", "_credential_resolver_factory": _mock_resolver_factory()}
        conf = _glue_spark_config(opts)
        assert conf["spark.hadoop.aws.glue.catalog.catalogId"] == "123456"

    def test_with_session_token(self) -> None:
        creds = _make_creds(session_token="TOK")
        opts: dict[str, Any] = {"database": "db", "region": "us-east-1", "_credential_resolver_factory": _mock_resolver_factory(creds)}
        conf = _glue_spark_config(opts)
        assert conf["spark.hadoop.fs.s3a.session.token"] == "TOK"
        assert "TemporaryAWSCredentialsProvider" in conf["spark.hadoop.fs.s3a.aws.credentials.provider"]

    def test_default_region(self) -> None:
        opts: dict[str, Any] = {"database": "db", "_credential_resolver_factory": _mock_resolver_factory()}
        conf = _glue_spark_config(opts)
        assert conf["spark.hadoop.aws.glue.catalog.region"] == "us-east-1"


# --- Config injection ---

class TestEnsureGlueConfig:
    def test_injects_into_engine_config_before_session(self) -> None:
        engine = _make_engine(session_exists=False)
        opts: dict[str, Any] = {"database": "db", "region": "us-east-1", "_credential_resolver_factory": _mock_resolver_factory()}
        _ensure_glue_config(engine, opts)
        assert engine._config["config"]["spark.sql.catalogImplementation"] == "hive"
        assert engine._config["config"]["hive.metastore.client.factory.class"] == GLUE_METASTORE_FACTORY

    def test_sets_hadoop_config_on_existing_session(self) -> None:
        engine = _make_engine(session_exists=True)
        hadoop_conf = MagicMock()
        engine._session.sparkContext._jsc.hadoopConfiguration.return_value = hadoop_conf
        opts: dict[str, Any] = {"database": "db", "region": "us-east-1", "_credential_resolver_factory": _mock_resolver_factory()}
        _ensure_glue_config(engine, opts)
        # Hadoop properties should be set (stripped of spark.hadoop. prefix)
        hadoop_conf.set.assert_any_call("aws.glue.catalog.region", "us-east-1")
        hadoop_conf.set.assert_any_call("fs.s3a.access.key", "AKID")


# --- read_dispatch ---

class TestReadDispatch:
    def test_read_returns_deferred_material(self) -> None:
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        catalog = _make_catalog()
        joint = _make_joint()
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result.material, Material)
        assert result.material.name == "my_table"
        assert result.material.state == "deferred"

    def test_read_uses_database_qualified_name(self) -> None:
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        session = engine.get_session()
        catalog = _make_catalog()
        joint = _make_joint(name="events", table="events")
        adapter.read_dispatch(engine, catalog, joint)
        session.sql.assert_called_with("SELECT * FROM mydb.events")

    def test_read_uses_joint_table_over_name(self) -> None:
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        session = engine.get_session()
        catalog = _make_catalog()
        joint = _make_joint(name="j1", table="actual_table")
        adapter.read_dispatch(engine, catalog, joint)
        session.sql.assert_called_with("SELECT * FROM mydb.actual_table")

    def test_read_failure_raises_execution_error(self) -> None:
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        session = engine.get_session()
        session.sql.side_effect = RuntimeError("table not found")
        catalog = _make_catalog()
        joint = _make_joint()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-501"
        assert "GluePySparkAdapter read failed" in exc_info.value.error.message


# --- write_dispatch ---

class TestWriteDispatch:
    def test_write_replace(self) -> None:
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="replace")
        material = _make_material()
        # Should not raise
        adapter.write_dispatch(engine, catalog, joint, material)
        session = engine.get_session()
        session.createDataFrame.return_value.write.mode.assert_called_with("overwrite")

    def test_write_append(self) -> None:
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="append")
        material = _make_material()
        adapter.write_dispatch(engine, catalog, joint, material)
        session = engine.get_session()
        session.createDataFrame.return_value.write.mode.assert_called_with("append")

    def test_write_delete_insert_uses_append_mode(self) -> None:
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="delete_insert")
        material = _make_material()
        adapter.write_dispatch(engine, catalog, joint, material)
        session = engine.get_session()
        session.createDataFrame.return_value.write.mode.assert_called_with("append")

    def test_write_failure_raises_execution_error(self) -> None:
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        session = engine.get_session()
        session.createDataFrame.return_value.write.mode.return_value.saveAsTable.side_effect = (
            RuntimeError("write failed")
        )
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="replace")
        material = _make_material()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-501"
        assert "GluePySparkAdapter write failed" in exc_info.value.error.message


# --- Config injection before session creation ---

class TestConfigBeforeSessionCreation:
    def test_glue_config_injected_before_get_session(self) -> None:
        """Verify that Glue metastore config is injected into engine._config
        before the session is created (engine._session is None)."""
        adapter = GluePySparkAdapter()
        engine = _make_engine(session_exists=False)
        catalog = _make_catalog()
        joint = _make_joint()

        adapter.read_dispatch(engine, catalog, joint)

        # Config should have been injected
        config = engine._config["config"]
        assert config["hive.metastore.client.factory.class"] == GLUE_METASTORE_FACTORY
        assert config["spark.sql.catalogImplementation"] == "hive"
