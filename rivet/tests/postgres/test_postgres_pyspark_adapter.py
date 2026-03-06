"""Tests for PostgresPySparkAdapter registration and JDBC read dispatch (tasks 13.1, 13.2).

Verifies the adapter registers correctly and implements JDBC read dispatch
with parallel partitioned reads support.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from rivet_core.errors import ExecutionError, RivetError
from rivet_core.models import Material
from rivet_core.optimizer import AdapterPushdownResult
from rivet_postgres.adapters.pyspark import (
    PostgresPySparkAdapter,
    _build_jdbc_properties,
    _build_jdbc_url,
    _check_jdbc_driver,
)

# --- Fixtures ---


def _make_catalog(overrides: dict[str, Any] | None = None) -> SimpleNamespace:
    opts: dict[str, Any] = {
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "user": "pguser",
        "password": "pgpass",
        "schema": "public",
        "ssl_mode": "prefer",
    }
    if overrides:
        opts.update(overrides)
    return SimpleNamespace(name="pg_cat", options=opts)


def _make_joint(name: str = "users", **kwargs: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "name": name,
        "table": None,
        "sql": None,
        "jdbc_partition_column": None,
        "jdbc_lower_bound": None,
        "jdbc_upper_bound": None,
        "jdbc_num_partitions": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _make_engine(has_jdbc_driver: bool = True) -> SimpleNamespace:
    session = MagicMock()
    jvm = MagicMock()

    def class_for_name(cls_name: str) -> Any:
        if cls_name == "org.postgresql.Driver" and not has_jdbc_driver:
            raise Exception("Class not found")
        return MagicMock()

    jvm.java.lang.Class.forName = class_for_name
    session._jvm = jvm
    return SimpleNamespace(get_session=lambda: session)


# --- Registration tests (task 13.1) ---


class TestPostgresPySparkAdapterRegistration:
    """Verify adapter class attributes match the spec."""

    def test_target_engine_type(self):
        adapter = PostgresPySparkAdapter()
        assert adapter.target_engine_type == "pyspark"

    def test_catalog_type(self):
        adapter = PostgresPySparkAdapter()
        assert adapter.catalog_type == "postgres"

    def test_source_is_catalog_plugin(self):
        adapter = PostgresPySparkAdapter()
        assert adapter.source == "catalog_plugin"

    def test_source_plugin(self):
        adapter = PostgresPySparkAdapter()
        assert adapter.source_plugin == "rivet_postgres"

    def test_all_6_capabilities(self):
        adapter = PostgresPySparkAdapter()
        expected = [
            "projection_pushdown",
            "predicate_pushdown",
            "limit_pushdown",
            "cast_pushdown",
            "join",
            "aggregation",
        ]
        assert adapter.capabilities == expected

    def test_is_compute_engine_adapter(self):
        from rivet_core.plugins import ComputeEngineAdapter

        adapter = PostgresPySparkAdapter()
        assert isinstance(adapter, ComputeEngineAdapter)

    def test_registry_accepts_adapter(self):
        """Adapter can be registered in PluginRegistry without error."""
        from rivet_core.plugins import PluginRegistry

        registry = PluginRegistry()
        adapter = PostgresPySparkAdapter()
        registry.register_adapter(adapter)
        resolved = registry.get_adapter("pyspark", "postgres")
        assert resolved is adapter


# --- JDBC URL and properties ---


class TestBuildJdbcUrl:
    def test_default_url(self):
        url = _build_jdbc_url({"host": "db.example.com", "port": 5432, "database": "mydb", "ssl_mode": "prefer"})
        assert url == "jdbc:postgresql://db.example.com:5432/mydb?sslmode=prefer"

    def test_custom_port(self):
        url = _build_jdbc_url({"host": "db.example.com", "port": 5433, "database": "mydb", "ssl_mode": "disable"})
        assert url == "jdbc:postgresql://db.example.com:5433/mydb?sslmode=disable"

    def test_default_port_when_missing(self):
        url = _build_jdbc_url({"host": "localhost", "database": "test"})
        assert "5432" in url


class TestBuildJdbcProperties:
    def test_includes_driver(self):
        props = _build_jdbc_properties({})
        assert props["driver"] == "org.postgresql.Driver"

    def test_includes_user_password(self):
        props = _build_jdbc_properties({"user": "admin", "password": "secret"})
        assert props["user"] == "admin"
        assert props["password"] == "secret"

    def test_omits_missing_credentials(self):
        props = _build_jdbc_properties({})
        assert "user" not in props
        assert "password" not in props


# --- JDBC driver check ---


class TestCheckJdbcDriver:
    def test_passes_when_driver_present(self):
        engine = _make_engine(has_jdbc_driver=True)
        _check_jdbc_driver(engine.get_session())  # should not raise

    def test_fails_with_rvt_505_when_missing(self):
        engine = _make_engine(has_jdbc_driver=False)
        with pytest.raises(ExecutionError) as exc_info:
            _check_jdbc_driver(engine.get_session())
        assert exc_info.value.error.code == "RVT-505"
        assert "JDBC" in exc_info.value.error.message
        assert "postgresql" in exc_info.value.error.remediation.lower()


# --- read_dispatch ---


class TestReadDispatch:
    def test_fails_without_jdbc_driver(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine(has_jdbc_driver=False)
        catalog = _make_catalog()
        joint = _make_joint()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-505"

    def test_simple_read_returns_material(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint()
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert isinstance(result.material, Material)
        assert result.material.name == "users"
        assert result.material.catalog == "pg_cat"
        assert result.material.state == "deferred"

    def test_uses_table_attribute(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint(name="j1", table="orders")
        result = adapter.read_dispatch(engine, catalog, joint)
        assert result.material.name == "j1"
        # Verify jdbc was called with the right table
        session = engine.get_session()
        call_args = session.read.jdbc.call_args
        assert "public.orders" in str(call_args)

    def test_uses_sql_as_subquery(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint(sql="SELECT id, name FROM users WHERE active = true")
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        session = engine.get_session()
        call_args = session.read.jdbc.call_args
        assert "_rivet_subquery" in str(call_args)

    def test_uses_schema_from_catalog(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog({"schema": "analytics"})
        joint = _make_joint(name="events")
        adapter.read_dispatch(engine, catalog, joint)
        session = engine.get_session()
        call_args = session.read.jdbc.call_args
        assert "analytics.events" in str(call_args)

    def test_parallel_partitioned_read(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint(
            jdbc_partition_column="id",
            jdbc_lower_bound=1,
            jdbc_upper_bound=10000,
            jdbc_num_partitions=4,
        )
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        session = engine.get_session()
        call_args = session.read.jdbc.call_args
        assert call_args.kwargs.get("column") == "id" or call_args[1].get("column") == "id"

    def test_partitioned_read_with_defaults(self):
        """When lower/upper bounds are not set, defaults are used."""
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint(
            jdbc_partition_column="id",
            jdbc_num_partitions=2,
        )
        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        session = engine.get_session()
        call_args = session.read.jdbc.call_args
        # Should have been called with partitioning params
        assert call_args.kwargs.get("numPartitions") == 2 or (
            len(call_args[0]) > 2 if call_args[0] else False
        )

    def test_jdbc_read_exception_wraps_as_rvt_501(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        session = engine.get_session()
        session.read.jdbc.side_effect = RuntimeError("Connection refused")
        catalog = _make_catalog()
        joint = _make_joint()
        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(engine, catalog, joint)
        assert exc_info.value.error.code == "RVT-501"
        assert "Connection refused" in exc_info.value.error.message

    def test_jdbc_url_includes_ssl_mode(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog({"ssl_mode": "require"})
        joint = _make_joint()
        adapter.read_dispatch(engine, catalog, joint)
        session = engine.get_session()
        call_args = session.read.jdbc.call_args
        url_arg = call_args.kwargs.get("url") or call_args[1].get("url") or call_args[0][0]
        assert "sslmode=require" in url_arg


# --- write_dispatch (task 13.3) ---


def _make_material() -> SimpleNamespace:
    """Create a mock material with a materialized_ref that returns a mock Arrow table."""
    mock_arrow = MagicMock()
    mock_arrow.to_pandas.return_value = MagicMock()  # mock pandas df
    ref = SimpleNamespace(to_arrow=lambda: mock_arrow)
    return SimpleNamespace(materialized_ref=ref)


class TestWriteDispatchSimple:
    """Test simple JDBC writes: append and replace (task 13.3)."""

    def test_append_mode(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="append")
        material = _make_material()

        adapter.write_dispatch(engine, catalog, joint, material)

        session = engine.get_session()
        call_args = session.createDataFrame.return_value.write.jdbc.call_args
        assert call_args.kwargs.get("mode") == "append"

    def test_replace_mode(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="replace")
        material = _make_material()

        adapter.write_dispatch(engine, catalog, joint, material)

        session = engine.get_session()
        call_args = session.createDataFrame.return_value.write.jdbc.call_args
        assert call_args.kwargs.get("mode") == "overwrite"

    def test_default_strategy_is_replace(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint()  # no write_strategy
        material = _make_material()

        adapter.write_dispatch(engine, catalog, joint, material)

        session = engine.get_session()
        call_args = session.createDataFrame.return_value.write.jdbc.call_args
        assert call_args.kwargs.get("mode") == "overwrite"

    def test_uses_correct_table_reference(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog({"schema": "analytics"})
        joint = _make_joint(name="events", write_strategy="append")
        material = _make_material()

        adapter.write_dispatch(engine, catalog, joint, material)

        session = engine.get_session()
        call_args = session.createDataFrame.return_value.write.jdbc.call_args
        assert call_args.kwargs.get("table") == "analytics.events"

    def test_uses_joint_table_attribute(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint(name="j1", table="orders", write_strategy="append")
        material = _make_material()

        adapter.write_dispatch(engine, catalog, joint, material)

        session = engine.get_session()
        call_args = session.createDataFrame.return_value.write.jdbc.call_args
        assert call_args.kwargs.get("table") == "public.orders"

    def test_jdbc_url_passed_correctly(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="append")
        material = _make_material()

        adapter.write_dispatch(engine, catalog, joint, material)

        session = engine.get_session()
        call_args = session.createDataFrame.return_value.write.jdbc.call_args
        url = call_args.kwargs.get("url")
        assert "jdbc:postgresql://localhost:5432/testdb" in url

    def test_fails_without_jdbc_driver(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine(has_jdbc_driver=False)
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="append")
        material = _make_material()

        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-505"

    def test_jdbc_write_exception_wraps_as_rvt_501(self):
        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        session = engine.get_session()
        session.createDataFrame.return_value.write.jdbc.side_effect = RuntimeError("Permission denied")
        catalog = _make_catalog()
        joint = _make_joint(write_strategy="append")
        material = _make_material()

        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-501"
        assert "Permission denied" in exc_info.value.error.message

    def test_complex_strategies_delegate_to_side_channel(self):
        """Complex strategies should call psycopg3 side-channel, not raise NotImplementedError."""
        from unittest.mock import patch

        adapter = PostgresPySparkAdapter()
        engine = _make_engine()
        catalog = _make_catalog()
        material = _make_material()

        for strategy in ["truncate_insert", "merge", "delete_insert", "incremental_append", "scd2"]:
            joint = _make_joint(write_strategy=strategy)
            with patch("rivet_postgres.adapters.pyspark._psycopg3_side_channel") as mock_sc:
                adapter.write_dispatch(engine, catalog, joint, material)
                mock_sc.assert_called_once_with(catalog, joint, material, strategy)


# --- psycopg3 side-channel tests (task 13.4) ---


class TestPsycopg3SideChannel:
    """Test complex write strategies via psycopg3 side-channel."""

    def test_side_channel_does_not_require_jdbc_driver(self):
        """Complex strategies bypass Spark JDBC entirely, so no JDBC driver check."""
        from unittest.mock import patch

        adapter = PostgresPySparkAdapter()
        engine = _make_engine(has_jdbc_driver=False)
        catalog = _make_catalog()
        material = _make_material()

        for strategy in ["truncate_insert", "merge", "delete_insert", "incremental_append", "scd2"]:
            joint = _make_joint(write_strategy=strategy)
            with patch("rivet_postgres.adapters.pyspark._psycopg3_side_channel"):
                # Should NOT raise RVT-505 since JDBC is not used
                adapter.write_dispatch(engine, catalog, joint, material)

    def test_side_channel_calls_execute_strategy(self):
        """Verify _psycopg3_side_channel delegates to sink._execute_strategy."""
        from unittest.mock import AsyncMock, patch

        catalog = _make_catalog()
        joint = _make_joint(write_strategy="truncate_insert", table="orders")
        material = _make_material()

        mock_execute = AsyncMock()
        with patch("rivet_postgres.sink._execute_strategy", mock_execute):
            from rivet_postgres.adapters.pyspark import _psycopg3_side_channel

            _psycopg3_side_channel(catalog, joint, material, "truncate_insert")

        mock_execute.assert_called_once()
        args = mock_execute.call_args[0]
        assert "host=localhost" in args[0]  # conninfo
        assert args[1] == "public.orders"  # qualified table
        assert args[3] == "truncate_insert"  # strategy

    def test_side_channel_uses_schema_from_catalog(self):
        """Verify side-channel qualifies table with catalog schema."""
        from unittest.mock import AsyncMock, patch

        catalog = _make_catalog({"schema": "analytics"})
        joint = _make_joint(write_strategy="merge", table="events")
        material = _make_material()

        mock_execute = AsyncMock()
        with patch("rivet_postgres.sink._execute_strategy", mock_execute):
            from rivet_postgres.adapters.pyspark import _psycopg3_side_channel

            _psycopg3_side_channel(catalog, joint, material, "merge")

        args = mock_execute.call_args[0]
        assert args[1] == "analytics.events"

    def test_side_channel_uses_joint_name_when_no_table(self):
        """When joint.table is None, use joint.name as table."""
        from unittest.mock import AsyncMock, patch

        catalog = _make_catalog()
        joint = _make_joint(name="users", write_strategy="scd2")
        material = _make_material()

        mock_execute = AsyncMock()
        with patch("rivet_postgres.sink._execute_strategy", mock_execute):
            from rivet_postgres.adapters.pyspark import _psycopg3_side_channel

            _psycopg3_side_channel(catalog, joint, material, "scd2")

        args = mock_execute.call_args[0]
        assert args[1] == "public.users"

    def test_side_channel_wraps_execution_error(self):
        """Non-ExecutionError exceptions are wrapped as RVT-501."""
        from unittest.mock import AsyncMock, patch

        catalog = _make_catalog()
        joint = _make_joint(write_strategy="delete_insert")
        material = _make_material()

        mock_execute = AsyncMock(side_effect=RuntimeError("connection refused"))
        with patch("rivet_postgres.sink._execute_strategy", mock_execute):
            from rivet_postgres.adapters.pyspark import _psycopg3_side_channel

            with pytest.raises(ExecutionError) as exc_info:
                _psycopg3_side_channel(catalog, joint, material, "delete_insert")
            assert exc_info.value.error.code == "RVT-501"
            assert "psycopg3 side-channel" in exc_info.value.error.message

    def test_side_channel_reraises_execution_error(self):
        """ExecutionError from sink is re-raised directly."""
        from unittest.mock import AsyncMock, patch

        catalog = _make_catalog()
        joint = _make_joint(write_strategy="incremental_append")
        material = _make_material()

        original_error = ExecutionError(
            RivetError(code="RVT-501", message="table not found", remediation="Create the table.")
        )
        mock_execute = AsyncMock(side_effect=original_error)
        with patch("rivet_postgres.sink._execute_strategy", mock_execute):
            from rivet_postgres.adapters.pyspark import _psycopg3_side_channel

            with pytest.raises(ExecutionError) as exc_info:
                _psycopg3_side_channel(catalog, joint, material, "incremental_append")
            assert exc_info.value is original_error

    def test_all_five_complex_strategies_supported(self):
        """All 5 complex strategies route through the side-channel."""
        from rivet_postgres.adapters.pyspark import _SIDE_CHANNEL_STRATEGIES

        assert {"truncate_insert", "merge", "delete_insert", "incremental_append", "scd2"} == _SIDE_CHANNEL_STRATEGIES

    def test_side_channel_materializes_arrow_from_ref(self):
        """Verify the side-channel calls materialized_ref.to_arrow()."""
        from unittest.mock import AsyncMock, MagicMock, patch

        catalog = _make_catalog()
        joint = _make_joint(write_strategy="truncate_insert")

        mock_arrow = MagicMock()
        mock_ref = MagicMock()
        mock_ref.to_arrow.return_value = mock_arrow
        material = SimpleNamespace(materialized_ref=mock_ref)

        mock_execute = AsyncMock()
        with patch("rivet_postgres.sink._execute_strategy", mock_execute):
            from rivet_postgres.adapters.pyspark import _psycopg3_side_channel

            _psycopg3_side_channel(catalog, joint, material, "truncate_insert")

        mock_ref.to_arrow.assert_called_once()
        # The arrow table should be passed as the 3rd arg to _execute_strategy
        assert mock_execute.call_args[0][2] is mock_arrow
