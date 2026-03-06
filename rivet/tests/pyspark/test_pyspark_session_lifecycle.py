"""Tests for task 32.4: SparkSession lifecycle — lazy creation, singleton per process, teardown."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from rivet_core.models import ComputeEngine
from rivet_pyspark.engine import PySparkComputeEngine, PySparkComputeEnginePlugin


@pytest.fixture()
def mock_pyspark():
    """Install a fake pyspark.sql module with a mock SparkSession."""
    mock_spark_session = MagicMock()
    mock_sql = MagicMock()
    mock_sql.SparkSession = mock_spark_session
    mock_pyspark_mod = MagicMock()
    mock_pyspark_mod.sql = mock_sql

    with patch.dict(sys.modules, {
        "pyspark": mock_pyspark_mod,
        "pyspark.sql": mock_sql,
    }):
        yield mock_spark_session


class TestPySparkComputeEngineCreation:
    def test_create_engine_returns_pyspark_compute_engine(self):
        plugin = PySparkComputeEnginePlugin()
        engine = plugin.create_engine("spark1", {"master": "local[2]"})
        assert isinstance(engine, PySparkComputeEngine)

    def test_create_engine_is_compute_engine(self):
        plugin = PySparkComputeEnginePlugin()
        engine = plugin.create_engine("spark1", {})
        assert isinstance(engine, ComputeEngine)

    def test_engine_has_correct_name_and_type(self):
        plugin = PySparkComputeEnginePlugin()
        engine = plugin.create_engine("my_spark", {})
        assert engine.name == "my_spark"
        assert engine.engine_type == "pyspark"

    def test_session_is_none_before_get_session(self):
        engine = PySparkComputeEngine("spark1", {})
        assert engine._session is None

    def test_not_externally_managed_by_default(self):
        engine = PySparkComputeEngine("spark1", {})
        assert engine._externally_managed is False


class TestLazySessionCreation:
    def test_get_session_creates_session_lazily(self, mock_pyspark):
        mock_builder = MagicMock()
        mock_session = MagicMock()
        mock_pyspark.builder = mock_builder
        mock_pyspark.getActiveSession.return_value = None
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        engine = PySparkComputeEngine("spark1", {"master": "local[4]", "app_name": "test"})
        session = engine.get_session()

        assert session is mock_session
        mock_builder.master.assert_called_once_with("local[4]")
        mock_builder.appName.assert_called_once_with("test")
        mock_builder.getOrCreate.assert_called_once()

    def test_get_session_uses_defaults(self, mock_pyspark):
        mock_builder = MagicMock()
        mock_session = MagicMock()
        mock_pyspark.builder = mock_builder
        mock_pyspark.getActiveSession.return_value = None
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        engine = PySparkComputeEngine("spark1", {})
        engine.get_session()

        mock_builder.master.assert_called_once_with("local[*]")
        mock_builder.appName.assert_called_once_with("rivet")

    def test_get_session_applies_config(self, mock_pyspark):
        mock_builder = MagicMock()
        mock_session = MagicMock()
        mock_pyspark.builder = mock_builder
        mock_pyspark.getActiveSession.return_value = None
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        engine = PySparkComputeEngine("spark1", {
            "config": {"spark.executor.memory": "2g", "spark.sql.shuffle.partitions": "10"},
        })
        engine.get_session()

        assert mock_builder.config.call_count == 2


class TestSingletonBehavior:
    def test_get_session_returns_same_instance(self, mock_pyspark):
        mock_builder = MagicMock()
        mock_session = MagicMock()
        mock_pyspark.builder = mock_builder
        mock_pyspark.getActiveSession.return_value = None
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        engine = PySparkComputeEngine("spark1", {})
        s1 = engine.get_session()
        s2 = engine.get_session()

        assert s1 is s2
        mock_builder.getOrCreate.assert_called_once()


class TestExternalSessionDetection:
    def test_reuses_existing_active_session(self, mock_pyspark):
        external_session = MagicMock()
        mock_pyspark.getActiveSession.return_value = external_session

        engine = PySparkComputeEngine("spark1", {})
        session = engine.get_session()

        assert session is external_session
        assert engine._externally_managed is True

    def test_own_session_not_externally_managed(self, mock_pyspark):
        mock_builder = MagicMock()
        mock_pyspark.builder = mock_builder
        mock_pyspark.getActiveSession.return_value = None
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        engine = PySparkComputeEngine("spark1", {})
        engine.get_session()

        assert engine._externally_managed is False


class TestTeardown:
    def test_teardown_calls_stop_on_own_session(self, mock_pyspark):
        mock_builder = MagicMock()
        mock_session = MagicMock()
        mock_pyspark.builder = mock_builder
        mock_pyspark.getActiveSession.return_value = None
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        engine = PySparkComputeEngine("spark1", {})
        engine.get_session()
        engine.teardown()

        mock_session.stop.assert_called_once()

    def test_teardown_clears_session_reference(self, mock_pyspark):
        mock_builder = MagicMock()
        mock_pyspark.builder = mock_builder
        mock_pyspark.getActiveSession.return_value = None
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        engine = PySparkComputeEngine("spark1", {})
        engine.get_session()
        engine.teardown()

        assert engine._session is None

    def test_teardown_skips_stop_for_external_session(self, mock_pyspark):
        external_session = MagicMock()
        mock_pyspark.getActiveSession.return_value = external_session

        engine = PySparkComputeEngine("spark1", {})
        engine.get_session()
        engine.teardown()

        external_session.stop.assert_not_called()

    def test_teardown_noop_when_no_session(self):
        engine = PySparkComputeEngine("spark1", {})
        engine.teardown()  # should not raise


class TestSparkConnectMode:
    """Task 32.5: use pyspark.sql.connect.SparkSession when connect_url set."""

    def test_connect_mode_uses_connect_spark_session(self):
        mock_connect_session = MagicMock()
        mock_connect_builder = MagicMock()
        mock_connect_builder.remote.return_value = mock_connect_builder
        mock_connect_builder.getOrCreate.return_value = mock_connect_session

        mock_connect_cls = MagicMock()
        mock_connect_cls.builder = mock_connect_builder

        mock_connect_module = MagicMock()
        mock_connect_module.SparkSession = mock_connect_cls

        with patch.dict(sys.modules, {"pyspark.sql.connect": mock_connect_module}):
            engine = PySparkComputeEngine("spark1", {"connect_url": "sc://localhost:15002"})
            session = engine.get_session()

        assert session is mock_connect_session
        mock_connect_builder.remote.assert_called_once_with("sc://localhost:15002")
        mock_connect_builder.getOrCreate.assert_called_once()

    def test_connect_mode_does_not_call_pyspark_sql_spark_session(self):
        mock_connect_session = MagicMock()
        mock_connect_builder = MagicMock()
        mock_connect_builder.remote.return_value = mock_connect_builder
        mock_connect_builder.getOrCreate.return_value = mock_connect_session

        mock_connect_cls = MagicMock()
        mock_connect_cls.builder = mock_connect_builder

        mock_connect_module = MagicMock()
        mock_connect_module.SparkSession = mock_connect_cls

        mock_classic_session = MagicMock()
        mock_classic_sql = MagicMock()
        mock_classic_sql.SparkSession = mock_classic_session

        with patch.dict(sys.modules, {
            "pyspark.sql.connect": mock_connect_module,
            "pyspark.sql": mock_classic_sql,
        }):
            engine = PySparkComputeEngine("spark1", {"connect_url": "sc://localhost:15002"})
            engine.get_session()

        # classic SparkSession should NOT be used
        mock_classic_session.getActiveSession.assert_not_called()
        mock_classic_session.builder.assert_not_called()

    def test_connect_mode_ignores_master(self):
        mock_connect_session = MagicMock()
        mock_connect_builder = MagicMock()
        mock_connect_builder.remote.return_value = mock_connect_builder
        mock_connect_builder.getOrCreate.return_value = mock_connect_session

        mock_connect_cls = MagicMock()
        mock_connect_cls.builder = mock_connect_builder

        mock_connect_module = MagicMock()
        mock_connect_module.SparkSession = mock_connect_cls

        with patch.dict(sys.modules, {"pyspark.sql.connect": mock_connect_module}):
            engine = PySparkComputeEngine("spark1", {
                "connect_url": "sc://localhost:15002",
                "master": "local[*]",
            })
            engine.get_session()

        # .master() should NOT be called on the connect builder
        mock_connect_builder.master.assert_not_called()

    def test_connect_mode_session_is_singleton(self):
        mock_connect_session = MagicMock()
        mock_connect_builder = MagicMock()
        mock_connect_builder.remote.return_value = mock_connect_builder
        mock_connect_builder.getOrCreate.return_value = mock_connect_session

        mock_connect_cls = MagicMock()
        mock_connect_cls.builder = mock_connect_builder

        mock_connect_module = MagicMock()
        mock_connect_module.SparkSession = mock_connect_cls

        with patch.dict(sys.modules, {"pyspark.sql.connect": mock_connect_module}):
            engine = PySparkComputeEngine("spark1", {"connect_url": "sc://localhost:15002"})
            s1 = engine.get_session()
            s2 = engine.get_session()

        assert s1 is s2
        mock_connect_builder.getOrCreate.assert_called_once()

    def test_connect_mode_teardown_calls_stop(self):
        mock_connect_session = MagicMock()
        mock_connect_builder = MagicMock()
        mock_connect_builder.remote.return_value = mock_connect_builder
        mock_connect_builder.getOrCreate.return_value = mock_connect_session

        mock_connect_cls = MagicMock()
        mock_connect_cls.builder = mock_connect_builder

        mock_connect_module = MagicMock()
        mock_connect_module.SparkSession = mock_connect_cls

        with patch.dict(sys.modules, {"pyspark.sql.connect": mock_connect_module}):
            engine = PySparkComputeEngine("spark1", {"connect_url": "sc://localhost:15002"})
            engine.get_session()
            engine.teardown()

        mock_connect_session.stop.assert_called_once()
        assert engine._session is None

    def test_no_connect_url_uses_classic_spark_session(self, mock_pyspark):
        """Without connect_url, classic pyspark.sql.SparkSession is used."""
        mock_builder = MagicMock()
        mock_session = MagicMock()
        mock_pyspark.builder = mock_builder
        mock_pyspark.getActiveSession.return_value = None
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        engine = PySparkComputeEngine("spark1", {})
        session = engine.get_session()

        assert session is mock_session
