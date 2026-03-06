"""Tests for S3PySparkAdapter: S3A Hadoop properties, Delta capability promotion."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pyarrow
import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.optimizer import AdapterPushdownResult
from rivet_pyspark.adapters.s3 import (
    BASE_CAPABILITIES,
    DELTA_WRITE_CAPABILITIES,
    S3PySparkAdapter,
    _build_s3a_path,
    _configure_s3a,
    _delta_write_mode,
    _has_delta_jars,
    _has_hadoop_aws_jars,
)

# --- Fixtures ---

def _make_catalog(options: dict[str, Any] | None = None) -> SimpleNamespace:
    opts = {"bucket": "test-bucket", "prefix": "data", "format": "parquet", "region": "us-east-1"}
    if options:
        opts.update(options)
    return SimpleNamespace(name="s3_cat", options=opts)


def _make_joint(name: str = "my_table", table: str | None = None, **kwargs: Any) -> SimpleNamespace:
    j = SimpleNamespace(name=name, table=table, sql=None, **kwargs)
    return j


def _make_engine(has_hadoop: bool = True, has_delta: bool = False) -> SimpleNamespace:
    session = MagicMock()
    jvm = MagicMock()

    def class_for_name(cls_name: str) -> Any:
        if cls_name == "org.apache.hadoop.fs.s3a.S3AFileSystem" and not has_hadoop:
            raise Exception("Class not found")
        if cls_name in ("io.delta.tables.DeltaTable", "org.apache.spark.sql.delta.DeltaLog") and not has_delta:
            raise Exception("Class not found")
        return MagicMock()

    jvm.java.lang.Class.forName = class_for_name
    session._jvm = jvm
    session.sparkContext._jsc.hadoopConfiguration.return_value = MagicMock()

    engine = SimpleNamespace(get_session=lambda: session)
    return engine


def _make_material() -> SimpleNamespace:
    table = pyarrow.table({"a": [1, 2], "b": ["x", "y"]})
    ref = SimpleNamespace(to_arrow=lambda: table)
    return SimpleNamespace(materialized_ref=ref)


# --- Registration tests ---

class TestS3PySparkAdapterRegistration:
    def test_target_engine_type(self) -> None:
        assert S3PySparkAdapter.target_engine_type == "pyspark"

    def test_catalog_type(self) -> None:
        assert S3PySparkAdapter.catalog_type == "s3"

    def test_source(self) -> None:
        assert S3PySparkAdapter.source == "engine_plugin"
        assert S3PySparkAdapter.source_plugin == "rivet_pyspark"

    def test_base_capabilities(self) -> None:
        adapter = S3PySparkAdapter()
        for cap in ["projection_pushdown", "predicate_pushdown", "limit_pushdown",
                     "cast_pushdown", "join", "aggregation",
                     "write_append", "write_replace", "write_partition"]:
            assert cap in adapter.capabilities

    def test_delta_capabilities_not_in_static(self) -> None:
        adapter = S3PySparkAdapter()
        for cap in DELTA_WRITE_CAPABILITIES:
            assert cap not in adapter.capabilities


# --- Delta capability promotion ---

class TestDeltaCapabilityPromotion:
    def test_promotes_delta_when_jars_present(self) -> None:
        engine = _make_engine(has_hadoop=True, has_delta=True)
        adapter = S3PySparkAdapter()
        caps = adapter.get_capabilities(engine)
        for cap in DELTA_WRITE_CAPABILITIES:
            assert cap in caps

    def test_no_delta_promotion_without_jars(self) -> None:
        engine = _make_engine(has_hadoop=True, has_delta=False)
        adapter = S3PySparkAdapter()
        caps = adapter.get_capabilities(engine)
        for cap in DELTA_WRITE_CAPABILITIES:
            assert cap not in caps

    def test_base_capabilities_always_present(self) -> None:
        engine = _make_engine(has_hadoop=True, has_delta=False)
        adapter = S3PySparkAdapter()
        caps = adapter.get_capabilities(engine)
        for cap in BASE_CAPABILITIES:
            assert cap in caps


# --- Hadoop AWS JAR detection ---

class TestHadoopJarDetection:
    def test_has_hadoop_aws_jars_true(self) -> None:
        engine = _make_engine(has_hadoop=True)
        assert _has_hadoop_aws_jars(engine.get_session()) is True

    def test_has_hadoop_aws_jars_false(self) -> None:
        engine = _make_engine(has_hadoop=False)
        assert _has_hadoop_aws_jars(engine.get_session()) is False

    def test_has_delta_jars_true(self) -> None:
        engine = _make_engine(has_delta=True)
        assert _has_delta_jars(engine.get_session()) is True

    def test_has_delta_jars_false(self) -> None:
        engine = _make_engine(has_delta=False)
        assert _has_delta_jars(engine.get_session()) is False


# --- S3A configuration ---

class TestConfigureS3A:
    def test_sets_impl(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        _configure_s3a(session, {"bucket": "b"})
        conf.set.assert_any_call("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    def test_sets_credentials(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        _configure_s3a(session, {
            "bucket": "b",
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
        })
        conf.set.assert_any_call("fs.s3a.access.key", "AKID")
        conf.set.assert_any_call("fs.s3a.secret.key", "SECRET")

    def test_sets_session_token_and_provider(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        _configure_s3a(session, {
            "bucket": "b",
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
            "session_token": "TOK",
        })
        conf.set.assert_any_call("fs.s3a.session.token", "TOK")
        conf.set.assert_any_call(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )

    def test_sets_endpoint(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        _configure_s3a(session, {"bucket": "b", "endpoint_url": "http://minio:9000"})
        conf.set.assert_any_call("fs.s3a.endpoint", "http://minio:9000")

    def test_sets_path_style_access(self) -> None:
        session = MagicMock()
        conf = MagicMock()
        session.sparkContext._jsc.hadoopConfiguration.return_value = conf
        _configure_s3a(session, {"bucket": "b", "path_style_access": True})
        conf.set.assert_any_call("fs.s3a.path.style.access", "true")


# --- S3A path building ---

class TestBuildS3APath:
    def test_parquet_with_prefix(self) -> None:
        opts = {"bucket": "mybucket", "prefix": "raw", "format": "parquet"}
        assert _build_s3a_path(opts, "users") == "s3a://mybucket/raw/users.parquet"

    def test_parquet_no_prefix(self) -> None:
        opts = {"bucket": "mybucket", "prefix": "", "format": "parquet"}
        assert _build_s3a_path(opts, "users") == "s3a://mybucket/users.parquet"

    def test_delta_no_extension(self) -> None:
        opts = {"bucket": "mybucket", "prefix": "lake", "format": "delta"}
        assert _build_s3a_path(opts, "events") == "s3a://mybucket/lake/events"

    def test_csv_format(self) -> None:
        opts = {"bucket": "mybucket", "prefix": "", "format": "csv"}
        assert _build_s3a_path(opts, "data") == "s3a://mybucket/data.csv"


# --- read_dispatch ---

class TestReadDispatch:
    def test_fails_without_hadoop_jars(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=False)
        catalog = _make_catalog()
        joint = _make_joint()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-201"
        assert "Hadoop AWS" in exc_info.value.error.message

    def test_read_parquet(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=True)
        catalog = _make_catalog()
        joint = _make_joint()
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.name == "my_table"
        assert result.material.state == "deferred"

    def test_read_csv(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=True)
        catalog = _make_catalog({"format": "csv"})
        joint = _make_joint()
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)

    def test_read_json(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=True)
        catalog = _make_catalog({"format": "json"})
        joint = _make_joint()
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)

    def test_read_orc(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=True)
        catalog = _make_catalog({"format": "orc"})
        joint = _make_joint()
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)

    def test_read_delta(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=True)
        catalog = _make_catalog({"format": "delta"})
        joint = _make_joint()
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)

    def test_read_unsupported_format(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=True)
        catalog = _make_catalog({"format": "avro"})
        joint = _make_joint()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-501"

    def test_uses_joint_table_over_name(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=True)
        catalog = _make_catalog()
        joint = _make_joint(name="j1", table="actual_table")
        adapter.read_dispatch(engine, catalog, joint)
        # Verify the session.read.parquet was called with the right path
        session = engine.get_session()
        session.read.parquet.assert_called_with("s3a://test-bucket/data/actual_table.parquet")


# --- write_dispatch ---

class TestWriteDispatch:
    def test_fails_without_hadoop_jars(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=False)
        catalog = _make_catalog()
        joint = _make_joint()
        material = _make_material()
        with pytest.raises(PluginValidationError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-201"

    def test_write_unsupported_format(self) -> None:
        adapter = S3PySparkAdapter()
        engine = _make_engine(has_hadoop=True)
        catalog = _make_catalog({"format": "avro"})
        joint = _make_joint(write_strategy="replace")
        # Use a mock arrow table to avoid pandas dependency
        mock_arrow = MagicMock()
        mock_arrow.to_pandas.return_value = MagicMock()
        ref = SimpleNamespace(to_arrow=lambda: mock_arrow)
        material = SimpleNamespace(materialized_ref=ref)
        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-501"


# --- Delta write mode mapping ---

class TestDeltaWriteMode:
    def test_append(self) -> None:
        assert _delta_write_mode("append") == "append"

    def test_replace(self) -> None:
        assert _delta_write_mode("replace") == "overwrite"

    def test_merge(self) -> None:
        assert _delta_write_mode("merge") == "overwrite"

    def test_incremental_append(self) -> None:
        assert _delta_write_mode("incremental_append") == "append"

    def test_unknown_defaults_overwrite(self) -> None:
        assert _delta_write_mode("unknown") == "overwrite"
