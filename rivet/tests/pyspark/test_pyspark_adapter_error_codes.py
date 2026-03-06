"""Tests for task 33.4: PySpark adapter error codes RVT-201/202/203/204.

- RVT-201: S3PySparkAdapter fails when Hadoop AWS JARs are missing
- RVT-202: GluePySparkAdapter fails when Glue metastore factory JAR is missing
- RVT-203: UnityPySparkAdapter fails when Delta Lake JARs are missing
- RVT-204: UnityPySparkAdapter fails when credential vending is disabled
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from rivet_core.errors import PluginValidationError
from rivet_pyspark.adapters.glue import (
    GLUE_METASTORE_FACTORY,
    GluePySparkAdapter,
    _has_glue_metastore_jar,
)
from rivet_pyspark.adapters.s3 import S3PySparkAdapter
from rivet_pyspark.adapters.unity import UnityPySparkAdapter

# --- Shared fixtures ---

def _make_s3_catalog() -> SimpleNamespace:
    return SimpleNamespace(name="s3_cat", options={"bucket": "b", "format": "parquet"})


def _make_glue_catalog() -> SimpleNamespace:
    mock_resolver = MagicMock()
    mock_resolver.resolve.return_value = SimpleNamespace(
        access_key_id="AK", secret_access_key="SK", session_token=None
    )
    return SimpleNamespace(name="glue_cat", options={
        "database": "mydb",
        "region": "us-east-1",
        "_credential_resolver_factory": lambda opts, region: mock_resolver,
    })


def _make_unity_catalog() -> SimpleNamespace:
    return SimpleNamespace(name="unity_cat", options={
        "host": "https://unity.example.com",
        "catalog_name": "prod",
        "schema": "default",
        "token": "tok",
    })


def _make_joint(name: str = "tbl") -> SimpleNamespace:
    return SimpleNamespace(name=name, table=None, sql=None)


def _make_session(
    has_hadoop: bool = True,
    has_glue_metastore: bool = True,
    has_delta: bool = True,
) -> MagicMock:
    session = MagicMock()
    jvm = MagicMock()

    def class_for_name(cls_name: str) -> Any:
        if cls_name == "org.apache.hadoop.fs.s3a.S3AFileSystem" and not has_hadoop:
            raise Exception("Class not found")
        if cls_name == GLUE_METASTORE_FACTORY and not has_glue_metastore:
            raise Exception("Class not found")
        if not has_delta and cls_name in (
            "io.delta.tables.DeltaTable",
            "org.apache.spark.sql.delta.DeltaLog",
        ):
            raise Exception("Class not found")
        return MagicMock()

    jvm.java.lang.Class.forName = class_for_name
    session._jvm = jvm
    session.sparkContext._jsc.hadoopConfiguration.return_value = MagicMock()
    return session


def _make_engine(session: MagicMock) -> SimpleNamespace:
    return SimpleNamespace(
        _session=None,
        _config={"config": {}},
        get_session=lambda: session,
    )


# --- RVT-201: S3PySparkAdapter missing Hadoop AWS JARs ---

class TestRVT201MissingHadoopJars:
    def test_read_dispatch_fails_with_rvt201(self) -> None:
        session = _make_session(has_hadoop=False)
        engine = _make_engine(session)
        adapter = S3PySparkAdapter()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.read_dispatch(engine, _make_s3_catalog(), _make_joint())
        assert exc_info.value.error.code == "RVT-201"
        assert "Hadoop AWS" in exc_info.value.error.message
        assert exc_info.value.error.remediation is not None

    def test_write_dispatch_fails_with_rvt201(self) -> None:
        session = _make_session(has_hadoop=False)
        engine = _make_engine(session)
        adapter = S3PySparkAdapter()
        ref = SimpleNamespace(to_arrow=lambda: MagicMock())
        material = SimpleNamespace(materialized_ref=ref)
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.write_dispatch(engine, _make_s3_catalog(), _make_joint(), material)
        assert exc_info.value.error.code == "RVT-201"


# --- RVT-202: GluePySparkAdapter missing Glue metastore factory JAR ---

class TestRVT202MissingGlueMetastoreJar:
    def test_has_glue_metastore_jar_false_when_missing(self) -> None:
        session = _make_session(has_glue_metastore=False)
        assert _has_glue_metastore_jar(session) is False

    def test_has_glue_metastore_jar_true_when_present(self) -> None:
        session = _make_session(has_glue_metastore=True)
        assert _has_glue_metastore_jar(session) is True

    def test_read_dispatch_fails_with_rvt202(self) -> None:
        session = _make_session(has_glue_metastore=False)
        engine = _make_engine(session)
        adapter = GluePySparkAdapter()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.read_dispatch(engine, _make_glue_catalog(), _make_joint())
        assert exc_info.value.error.code == "RVT-202"
        assert "Glue" in exc_info.value.error.message
        assert exc_info.value.error.remediation is not None
        assert "spark.jars.packages" in exc_info.value.error.remediation

    def test_write_dispatch_fails_with_rvt202(self) -> None:
        session = _make_session(has_glue_metastore=False)
        engine = _make_engine(session)
        adapter = GluePySparkAdapter()
        ref = SimpleNamespace(to_arrow=lambda: MagicMock())
        material = SimpleNamespace(materialized_ref=ref)
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.write_dispatch(engine, _make_glue_catalog(), _make_joint(), material)
        assert exc_info.value.error.code == "RVT-202"

    def test_read_dispatch_succeeds_when_jar_present(self) -> None:
        session = _make_session(has_glue_metastore=True)
        engine = _make_engine(session)
        adapter = GluePySparkAdapter()
        # Should not raise RVT-202
        result = adapter.read_dispatch(engine, _make_glue_catalog(), _make_joint())
        from rivet_core.models import Material
        from rivet_core.optimizer import AdapterPushdownResult
        assert isinstance(result, AdapterPushdownResult)
        assert isinstance(result.material, Material)


# --- RVT-203: UnityPySparkAdapter missing Delta Lake JARs ---

class TestRVT203MissingDeltaJars:
    def test_read_dispatch_fails_with_rvt203(self) -> None:
        session = _make_session(has_delta=False)
        engine = _make_engine(session)
        adapter = UnityPySparkAdapter()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.read_dispatch(engine, _make_unity_catalog(), _make_joint())
        assert exc_info.value.error.code == "RVT-203"
        assert "Delta" in exc_info.value.error.message
        assert exc_info.value.error.remediation is not None

    def test_write_dispatch_fails_with_rvt203(self) -> None:
        session = _make_session(has_delta=False)
        engine = _make_engine(session)
        adapter = UnityPySparkAdapter()
        ref = SimpleNamespace(to_arrow=lambda: MagicMock())
        material = SimpleNamespace(materialized_ref=ref)
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.write_dispatch(engine, _make_unity_catalog(), _make_joint(), material)
        assert exc_info.value.error.code == "RVT-203"


# --- RVT-204: UnityPySparkAdapter credential vending disabled ---

class TestRVT204CredentialVendingDisabled:
    def test_read_dispatch_fails_with_rvt204_when_credentials_none(self) -> None:
        mock_plugin = MagicMock()
        mock_plugin.resolve_table_reference.return_value = {
            "storage_location": "s3://bucket/path",
            "file_format": "DELTA",
            "columns": [],
            "partition_columns": [],
            "table_type": "MANAGED",
            "temporary_credentials": None,
        }

        session = _make_session(has_delta=True)
        engine = _make_engine(session)
        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.read_dispatch(engine, _make_unity_catalog(), _make_joint())
        assert exc_info.value.error.code == "RVT-204"
        assert "credential vending" in exc_info.value.error.message.lower()
        assert exc_info.value.error.remediation is not None

    def test_write_dispatch_fails_with_rvt204_when_vend_returns_none(self) -> None:
        mock_plugin = MagicMock()
        mock_plugin.vend_credentials.return_value = None

        session = _make_session(has_delta=True)
        engine = _make_engine(session)
        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        ref = SimpleNamespace(to_arrow=lambda: MagicMock())
        material = SimpleNamespace(materialized_ref=ref)
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.write_dispatch(engine, _make_unity_catalog(), _make_joint(), material)
        assert exc_info.value.error.code == "RVT-204"
