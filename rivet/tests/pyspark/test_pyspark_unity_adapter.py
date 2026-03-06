"""Tests for UnityPySparkAdapter: REST API + credential vending + Delta read/write."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow
import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Material
from rivet_core.optimizer import AdapterPushdownResult
from rivet_pyspark.adapters.unity import (
    UnityPySparkAdapter,
    _configure_spark_credentials,
    _delta_write_mode,
    _has_delta_jars,
    _resolve_full_name,
)

# --- Fixtures ---

def _make_catalog(options: dict[str, Any] | None = None) -> SimpleNamespace:
    opts: dict[str, Any] = {
        "host": "https://unity.example.com",
        "catalog_name": "prod",
        "schema": "default",
        "token": "test-token",
    }
    if options:
        opts.update(options)
    return SimpleNamespace(name="unity_cat", options=opts)


def _make_joint(name: str = "my_table", table: str | None = None, **kwargs: Any) -> SimpleNamespace:
    return SimpleNamespace(name=name, table=table, sql=None, **kwargs)


def _make_engine(has_delta: bool = True) -> SimpleNamespace:
    session = MagicMock()
    jvm = MagicMock()

    def class_for_name(cls_name: str) -> Any:
        if not has_delta and cls_name in (
            "io.delta.tables.DeltaTable",
            "org.apache.spark.sql.delta.DeltaLog",
        ):
            raise Exception("Class not found")
        return MagicMock()

    jvm.java.lang.Class.forName = class_for_name
    session._jvm = jvm
    session.sparkContext._jsc.hadoopConfiguration.return_value = MagicMock()
    return SimpleNamespace(get_session=lambda: session)


def _make_table_meta(
    storage_location: str = "s3://bucket/path",
    credentials: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if credentials is None:
        credentials = {"aws_temp_credentials": {
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
            "session_token": "TOK",
        }}
    return {
        "storage_location": storage_location,
        "file_format": "DELTA",
        "columns": [],
        "partition_columns": [],
        "table_type": "MANAGED",
        "temporary_credentials": credentials,
    }


def _make_material() -> SimpleNamespace:
    table = pyarrow.table({"a": [1, 2], "b": ["x", "y"]})
    ref = SimpleNamespace(to_arrow=lambda: table)
    return SimpleNamespace(materialized_ref=ref)


# --- Registration tests ---

class TestUnityPySparkAdapterRegistration:
    def test_target_engine_type(self) -> None:
        assert UnityPySparkAdapter.target_engine_type == "pyspark"

    def test_catalog_type(self) -> None:
        assert UnityPySparkAdapter.catalog_type == "unity"

    def test_source(self) -> None:
        assert UnityPySparkAdapter.source == "engine_plugin"
        assert UnityPySparkAdapter.source_plugin == "rivet_pyspark"

    def test_capabilities(self) -> None:
        adapter = UnityPySparkAdapter()
        expected = [
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "write_append", "write_replace", "write_partition",
            "write_merge", "write_scd2",
        ]
        for cap in expected:
            assert cap in adapter.capabilities


# --- Delta JAR detection ---

class TestDeltaJarDetection:
    def test_has_delta_jars_true(self) -> None:
        engine = _make_engine(has_delta=True)
        assert _has_delta_jars(engine.get_session()) is True

    def test_has_delta_jars_false(self) -> None:
        engine = _make_engine(has_delta=False)
        assert _has_delta_jars(engine.get_session()) is False


# --- Full name resolution ---

class TestResolveFullName:
    def test_simple_name(self) -> None:
        joint = _make_joint(name="users")
        catalog = _make_catalog()
        assert _resolve_full_name(joint, catalog) == "prod.default.users"

    def test_dotted_name_passthrough(self) -> None:
        joint = _make_joint(name="x", table="cat.sch.tbl")
        catalog = _make_catalog()
        assert _resolve_full_name(joint, catalog) == "cat.sch.tbl"

    def test_uses_joint_table_over_name(self) -> None:
        joint = _make_joint(name="j1", table="actual_table")
        catalog = _make_catalog()
        assert _resolve_full_name(joint, catalog) == "prod.default.actual_table"


# --- Spark credential configuration ---

class TestConfigureSparkCredentials:
    def test_aws_credentials(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        creds = {"aws_temp_credentials": {
            "access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST",
        }}
        _configure_spark_credentials(session, creds)
        conf.set.assert_any_call("fs.s3a.access.key", "AK")
        conf.set.assert_any_call("fs.s3a.secret.key", "SK")
        conf.set.assert_any_call("fs.s3a.session.token", "ST")

    def test_azure_credentials(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        creds = {"azure_user_delegation_sas": {"sas_token": "sas123"}}
        _configure_spark_credentials(session, creds)
        conf.set.assert_any_call("fs.azure.sas.token", "sas123")

    def test_gcs_credentials(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        creds = {"gcp_oauth_token": {"oauth_token": "gcp-tok"}}
        _configure_spark_credentials(session, creds)
        conf.set.assert_any_call("fs.gs.auth.access.token", "gcp-tok")

    def test_unknown_credentials_logs_warning(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        creds = {"unknown_provider": {"key": "val"}}
        with patch("rivet_pyspark.adapters.unity._logger") as mock_logger:
            _configure_spark_credentials(session, creds)
            mock_logger.warning.assert_called_once()


# --- Delta write mode mapping ---

class TestDeltaWriteMode:
    def test_append(self) -> None:
        assert _delta_write_mode("append") == "append"

    def test_replace(self) -> None:
        assert _delta_write_mode("replace") == "overwrite"

    def test_merge(self) -> None:
        assert _delta_write_mode("merge") == "overwrite"

    def test_scd2(self) -> None:
        assert _delta_write_mode("scd2") == "overwrite"

    def test_partition(self) -> None:
        assert _delta_write_mode("partition") == "overwrite"

    def test_unknown_defaults_overwrite(self) -> None:
        assert _delta_write_mode("unknown") == "overwrite"


# --- read_dispatch ---

class TestReadDispatch:
    def test_fails_without_delta_jars(self) -> None:
        adapter = UnityPySparkAdapter()
        engine = _make_engine(has_delta=False)
        catalog = _make_catalog()
        joint = _make_joint()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-203"
        assert "Delta" in exc_info.value.error.message

    def test_fails_when_no_storage_location(self) -> None:
        mock_plugin = MagicMock()
        mock_plugin.resolve_table_reference.return_value = _make_table_meta(storage_location="")

        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        engine = _make_engine(has_delta=True)
        catalog = _make_catalog()
        joint = _make_joint()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-503"

    def test_fails_when_credential_vending_disabled(self) -> None:
        meta = _make_table_meta()
        meta["temporary_credentials"] = None
        mock_plugin = MagicMock()
        mock_plugin.resolve_table_reference.return_value = meta

        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        engine = _make_engine(has_delta=True)
        catalog = _make_catalog()
        joint = _make_joint()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-204"

    def test_successful_read(self) -> None:
        mock_plugin = MagicMock()
        mock_plugin.resolve_table_reference.return_value = _make_table_meta()

        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        engine = _make_engine(has_delta=True)
        catalog = _make_catalog()
        joint = _make_joint()
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert isinstance(result.material, Material)
        assert result.material.name == "my_table"
        assert result.material.state == "deferred"

        session = engine.get_session()
        session.read.format.assert_called_with("delta")
        session.read.format().load.assert_called_with("s3://bucket/path")


# --- write_dispatch ---

class TestWriteDispatch:
    def test_fails_without_delta_jars(self) -> None:
        adapter = UnityPySparkAdapter()
        engine = _make_engine(has_delta=False)
        catalog = _make_catalog()
        joint = _make_joint()
        material = _make_material()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-203"

    def test_fails_when_credential_vending_disabled(self) -> None:
        mock_plugin = MagicMock()
        mock_plugin.vend_credentials.return_value = None

        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        engine = _make_engine(has_delta=True)
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="replace")
        material = _make_material()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-204"

    def test_fails_when_no_storage_location(self) -> None:
        mock_plugin = MagicMock()
        mock_plugin.vend_credentials.return_value = {"aws_temp_credentials": {
            "access_key_id": "AK", "secret_access_key": "SK",
        }}
        meta = _make_table_meta(storage_location="")
        mock_plugin.resolve_table_reference.return_value = meta

        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        engine = _make_engine(has_delta=True)
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="replace")
        material = _make_material()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-503"

    def test_successful_write(self) -> None:
        mock_plugin = MagicMock()
        mock_plugin.vend_credentials.return_value = {"aws_temp_credentials": {
            "access_key_id": "AK", "secret_access_key": "SK",
        }}
        mock_plugin.resolve_table_reference.return_value = _make_table_meta()

        adapter = UnityPySparkAdapter()
        adapter._registry = MagicMock()
        adapter._registry.get_catalog_plugin.return_value = mock_plugin
        engine = _make_engine(has_delta=True)
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="replace")
        # Use a mock arrow table to avoid pandas dependency
        mock_arrow = MagicMock()
        mock_arrow.to_pandas.return_value = MagicMock()
        ref = SimpleNamespace(to_arrow=lambda: mock_arrow)
        material = SimpleNamespace(materialized_ref=ref)
        result = adapter.write_dispatch(engine, catalog, joint, material)
        assert result is material
