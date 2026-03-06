"""Tests for PostgresComputeEnginePlugin registration and option validation."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin, PluginRegistry
from rivet_postgres.engine import PostgresComputeEngine, PostgresComputeEnginePlugin


def test_engine_type():
    plugin = PostgresComputeEnginePlugin()
    assert plugin.engine_type == "postgres"


def test_dialect():
    plugin = PostgresComputeEnginePlugin()
    assert plugin.dialect == "postgres"


def test_is_compute_engine_plugin():
    assert isinstance(PostgresComputeEnginePlugin(), ComputeEnginePlugin)


def test_create_engine_returns_postgres_compute_engine():
    plugin = PostgresComputeEnginePlugin()
    engine = plugin.create_engine("my_pg", {"pool_min_size": 2, "pool_max_size": 5})
    assert isinstance(engine, PostgresComputeEngine)
    assert isinstance(engine, ComputeEngine)
    assert engine.name == "my_pg"
    assert engine.engine_type == "postgres"
    assert engine.config == {"pool_min_size": 2, "pool_max_size": 5}


def test_pool_is_none_initially():
    engine = PostgresComputeEngine("pg", {})
    assert engine._pool is None


def test_get_pool_creates_pool_lazily():
    mock_pool = AsyncMock()
    mock_pool.open = AsyncMock()
    mock_module = MagicMock()
    mock_module.AsyncConnectionPool = MagicMock(return_value=mock_pool)

    with patch.dict("sys.modules", {"psycopg_pool": mock_module}):
        engine = PostgresComputeEngine("pg", {
            "conninfo": "host=localhost dbname=test",
            "pool_min_size": 2,
            "pool_max_size": 5,
        })
        pool = asyncio.run(engine.get_pool())
        assert pool is mock_pool
        mock_module.AsyncConnectionPool.assert_called_once_with(
            conninfo="host=localhost dbname=test",
            min_size=2,
            max_size=5,
            open=False,
        )
        mock_pool.open.assert_awaited_once()


def test_get_pool_returns_same_pool_on_second_call():
    mock_pool = AsyncMock()
    mock_pool.open = AsyncMock()
    mock_module = MagicMock()
    mock_module.AsyncConnectionPool = MagicMock(return_value=mock_pool)

    with patch.dict("sys.modules", {"psycopg_pool": mock_module}):
        engine = PostgresComputeEngine("pg", {"conninfo": "host=localhost"})

        async def _run():
            p1 = await engine.get_pool()
            p2 = await engine.get_pool()
            return p1, p2

        pool1, pool2 = asyncio.run(_run())
        assert pool1 is pool2
        assert mock_module.AsyncConnectionPool.call_count == 1


def test_teardown_closes_pool():
    mock_pool = AsyncMock()
    mock_pool.open = AsyncMock()
    mock_pool.close = AsyncMock()
    mock_module = MagicMock()
    mock_module.AsyncConnectionPool = MagicMock(return_value=mock_pool)

    with patch.dict("sys.modules", {"psycopg_pool": mock_module}):
        engine = PostgresComputeEngine("pg", {"conninfo": "host=localhost"})

        async def _run():
            await engine.get_pool()
            await engine.teardown()

        asyncio.run(_run())
        mock_pool.close.assert_awaited_once()
        assert engine._pool is None


def test_teardown_without_pool_is_noop():
    engine = PostgresComputeEngine("pg", {})
    asyncio.run(engine.teardown())
    assert engine._pool is None


def test_get_pool_uses_defaults_when_config_missing():
    mock_pool = AsyncMock()
    mock_pool.open = AsyncMock()
    mock_module = MagicMock()
    mock_module.AsyncConnectionPool = MagicMock(return_value=mock_pool)

    with patch.dict("sys.modules", {"psycopg_pool": mock_module}):
        engine = PostgresComputeEngine("pg", {})
        asyncio.run(engine.get_pool())
        mock_module.AsyncConnectionPool.assert_called_once_with(
            conninfo="",
            min_size=1,
            max_size=10,
            open=False,
        )


def test_validate_accepts_valid_options():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({"pool_max_size": 20, "statement_timeout": 30000})  # should not raise


def test_validate_rejects_unknown_option():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"unknown_key": "value"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_empty_options():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({})  # should not raise


def test_registry_can_register_plugin():
    registry = PluginRegistry()
    plugin = PostgresComputeEnginePlugin()
    registry.register_engine_plugin(plugin)
    assert registry.get_engine_plugin("postgres") is plugin


def test_compiler_reads_dialect():
    plugin = PostgresComputeEnginePlugin()
    assert getattr(plugin, "dialect", None) == "postgres"


def test_native_postgres_support_all_6_capabilities():
    plugin = PostgresComputeEnginePlugin()
    assert "postgres" in plugin.supported_catalog_types
    caps = plugin.supported_catalog_types["postgres"]
    assert set(caps) == {
        "projection_pushdown",
        "predicate_pushdown",
        "limit_pushdown",
        "cast_pushdown",
        "join",
        "aggregation",
    }


def test_only_postgres_catalog_type_declared():
    plugin = PostgresComputeEnginePlugin()
    assert list(plugin.supported_catalog_types.keys()) == ["postgres"]


# --- Task 10.3: option type validation ---

def test_default_optional_options():
    plugin = PostgresComputeEnginePlugin()
    assert plugin.optional_options["statement_timeout"] is None
    assert plugin.optional_options["pool_min_size"] == 1
    assert plugin.optional_options["pool_max_size"] == 10
    assert plugin.optional_options["application_name"] == "rivet"
    assert plugin.optional_options["connect_timeout"] == 30
    assert plugin.optional_options["fetch_batch_size"] == 10000


def test_validate_statement_timeout_none():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({"statement_timeout": None})  # should not raise


def test_validate_statement_timeout_int():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({"statement_timeout": 5000})  # should not raise


def test_validate_statement_timeout_invalid_type():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"statement_timeout": "5000ms"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_pool_min_size_valid():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({"pool_min_size": 2})  # should not raise


def test_validate_pool_min_size_invalid_type():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"pool_min_size": "2"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_pool_min_size_negative():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"pool_min_size": -1})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_pool_max_size_valid():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({"pool_max_size": 20})  # should not raise


def test_validate_pool_max_size_invalid_type():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"pool_max_size": 10.5})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_pool_max_size_zero():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"pool_max_size": 0})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_application_name_valid():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({"application_name": "my-app"})  # should not raise


def test_validate_application_name_invalid_type():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"application_name": 123})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_connect_timeout_valid():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({"connect_timeout": 60})  # should not raise


def test_validate_connect_timeout_invalid_type():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"connect_timeout": "30s"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_connect_timeout_negative():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"connect_timeout": -5})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_fetch_batch_size_valid():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({"fetch_batch_size": 500})  # should not raise


def test_validate_fetch_batch_size_invalid_type():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"fetch_batch_size": "1000"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_fetch_batch_size_zero():
    plugin = PostgresComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"fetch_batch_size": 0})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_all_options_together():
    plugin = PostgresComputeEnginePlugin()
    plugin.validate({
        "statement_timeout": 30000,
        "pool_min_size": 2,
        "pool_max_size": 20,
        "application_name": "my-pipeline",
        "connect_timeout": 60,
        "fetch_batch_size": 5000,
    })  # should not raise


# --- Task 10.5: Declare all 8 write strategies for postgres catalog ---

def test_supported_write_strategies_declared():
    plugin = PostgresComputeEnginePlugin()
    assert hasattr(plugin, "supported_write_strategies")


def test_supported_write_strategies_has_postgres_catalog():
    plugin = PostgresComputeEnginePlugin()
    assert "postgres" in plugin.supported_write_strategies


def test_supported_write_strategies_all_8():
    plugin = PostgresComputeEnginePlugin()
    strategies = set(plugin.supported_write_strategies["postgres"])
    expected = {
        "append",
        "replace",
        "truncate_insert",
        "merge",
        "delete_insert",
        "incremental_append",
        "scd2",
        "partition",
    }
    assert strategies == expected


def test_supported_write_strategies_only_postgres_catalog():
    plugin = PostgresComputeEnginePlugin()
    assert list(plugin.supported_write_strategies.keys()) == ["postgres"]


# --- Task 10.6: Arrow streaming via server-side cursor ---

def test_stream_arrow_method_exists():
    engine = PostgresComputeEngine("pg", {})
    assert hasattr(engine, "stream_arrow")
    import inspect
    assert inspect.ismethod(engine.stream_arrow) or callable(engine.stream_arrow)


def test_stream_arrow_uses_fetch_batch_size_from_config():
    """stream_arrow fetches rows in batches of fetch_batch_size."""
    import pyarrow as pa

    rows_batch1 = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    rows_batch2 = [{"id": 3, "val": "c"}]

    mock_cursor = AsyncMock()
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)
    mock_cursor.fetchmany = AsyncMock(side_effect=[rows_batch1, rows_batch2, []])
    mock_cursor.description = [
        MagicMock(name="id"),
        MagicMock(name="val"),
    ]
    mock_cursor.description[0].name = "id"
    mock_cursor.description[1].name = "val"

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_cursor)

    mock_pool = AsyncMock()
    mock_pool.open = AsyncMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    mock_module = MagicMock()
    mock_module.AsyncConnectionPool = MagicMock(return_value=mock_pool)

    with patch.dict("sys.modules", {"psycopg_pool": mock_module}):
        engine = PostgresComputeEngine("pg", {
            "conninfo": "host=localhost",
            "fetch_batch_size": 2,
        })
        engine._pool = mock_pool

        async def _run():
            batches = []
            async for batch in engine.stream_arrow("SELECT id, val FROM t"):
                batches.append(batch)
            return batches

        batches = asyncio.run(_run())

    assert len(batches) == 2
    assert all(isinstance(b, pa.RecordBatch) for b in batches)
    assert batches[0].num_rows == 2
    assert batches[1].num_rows == 1


def test_stream_arrow_default_fetch_batch_size():
    """stream_arrow uses default fetch_batch_size of 10000 when not configured."""
    mock_cursor = AsyncMock()
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)
    mock_cursor.fetchmany = AsyncMock(side_effect=[[]])
    mock_cursor.description = []

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_cursor)

    mock_pool = AsyncMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = PostgresComputeEngine("pg", {})
    engine._pool = mock_pool

    async def _run():
        async for _ in engine.stream_arrow("SELECT 1"):
            pass

    asyncio.run(_run())
    mock_cursor.fetchmany.assert_awaited_once_with(10000)


def test_stream_arrow_uses_server_side_named_cursor():
    """stream_arrow opens a named server-side cursor."""
    mock_cursor = AsyncMock()
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)
    mock_cursor.fetchmany = AsyncMock(side_effect=[[]])
    mock_cursor.description = []

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_cursor)

    mock_pool = AsyncMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = PostgresComputeEngine("pg", {})
    engine._pool = mock_pool

    async def _run():
        async for _ in engine.stream_arrow("SELECT 1"):
            pass

    asyncio.run(_run())
    # cursor must be opened with a name (server-side cursor)
    call_kwargs = mock_conn.cursor.call_args
    assert call_kwargs is not None
    # name argument must be a non-empty string
    name_arg = call_kwargs.kwargs.get("name") or (call_kwargs.args[0] if call_kwargs.args else None)
    assert name_arg is not None and isinstance(name_arg, str) and len(name_arg) > 0


def test_stream_arrow_executes_sql():
    """stream_arrow executes the provided SQL query."""
    mock_cursor = AsyncMock()
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)
    mock_cursor.fetchmany = AsyncMock(side_effect=[[]])
    mock_cursor.description = []

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_cursor)

    mock_pool = AsyncMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = PostgresComputeEngine("pg", {})
    engine._pool = mock_pool

    sql = "SELECT id, name FROM users WHERE active = true"

    async def _run():
        async for _ in engine.stream_arrow(sql):
            pass

    asyncio.run(_run())
    mock_cursor.execute.assert_awaited_once_with(sql)


def test_stream_arrow_yields_no_batches_for_empty_result():
    """stream_arrow yields nothing when query returns no rows."""
    mock_cursor = AsyncMock()
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)
    mock_cursor.fetchmany = AsyncMock(return_value=[])
    mock_cursor.description = []

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_cursor)

    mock_pool = AsyncMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = PostgresComputeEngine("pg", {})
    engine._pool = mock_pool

    async def _run():
        batches = []
        async for batch in engine.stream_arrow("SELECT 1 WHERE false"):
            batches.append(batch)
        return batches

    batches = asyncio.run(_run())
    assert batches == []


# --- Task 10.4: execute_sql implementation ---


def test_execute_sql_rejects_non_empty_input_tables():
    """execute_sql raises RVT-502 when input_tables is non-empty."""
    import pyarrow as pa

    from rivet_core.errors import ExecutionError

    plugin = PostgresComputeEnginePlugin()
    engine = PostgresComputeEngine("pg", {})
    input_tables = {"upstream": pa.table({"x": [1, 2]})}

    with pytest.raises(ExecutionError) as exc_info:
        plugin.execute_sql(engine, "SELECT 1", input_tables)
    assert exc_info.value.error.code == "RVT-502"
    assert "upstream" in exc_info.value.error.message


def test_execute_sql_accepts_empty_input_tables():
    """execute_sql delegates to stream_arrow when input_tables is empty."""
    import pyarrow as pa

    plugin = PostgresComputeEnginePlugin()
    engine = PostgresComputeEngine("pg", {"conninfo": "host=localhost"})

    batch = pa.record_batch({"id": [1, 2], "val": ["a", "b"]})

    async def _mock_stream(sql):
        yield batch

    with patch.object(engine, "stream_arrow", side_effect=_mock_stream):
        result = plugin.execute_sql(engine, "SELECT id, val FROM t", {})

    assert isinstance(result, pa.Table)
    assert result.num_rows == 2
    assert result.column_names == ["id", "val"]


def test_execute_sql_returns_empty_table_for_no_results():
    """execute_sql returns empty table when stream_arrow yields nothing."""
    import pyarrow as pa

    plugin = PostgresComputeEnginePlugin()
    engine = PostgresComputeEngine("pg", {"conninfo": "host=localhost"})

    async def _mock_stream(sql):
        return
        yield  # make it an async generator

    with patch.object(engine, "stream_arrow", side_effect=_mock_stream):
        result = plugin.execute_sql(engine, "SELECT 1 WHERE false", {})

    assert isinstance(result, pa.Table)
    assert result.num_rows == 0


def test_execute_sql_wraps_errors_in_rvt_503():
    """execute_sql wraps connection/query errors in RVT-503."""
    from rivet_core.errors import ExecutionError

    plugin = PostgresComputeEnginePlugin()
    engine = PostgresComputeEngine("pg", {"conninfo": "host=localhost"})

    async def _mock_stream(sql):
        raise ConnectionError("connection refused")
        yield  # make it an async generator

    with patch.object(engine, "stream_arrow", side_effect=_mock_stream):
        with pytest.raises(ExecutionError) as exc_info:
            plugin.execute_sql(engine, "SELECT 1", {})
    assert exc_info.value.error.code == "RVT-503"
    assert "connection refused" in exc_info.value.error.message


def test_execute_sql_truncates_sql_in_error_context():
    """RVT-503 error context contains truncated SQL."""
    from rivet_core.errors import ExecutionError

    plugin = PostgresComputeEnginePlugin()
    engine = PostgresComputeEngine("pg", {"conninfo": "host=localhost"})
    long_sql = "SELECT " + "x" * 300

    async def _mock_stream(sql):
        raise RuntimeError("query failed")
        yield

    with patch.object(engine, "stream_arrow", side_effect=_mock_stream):
        with pytest.raises(ExecutionError) as exc_info:
            plugin.execute_sql(engine, long_sql, {})
    assert exc_info.value.error.code == "RVT-503"
    assert len(exc_info.value.error.context["sql"]) <= 200


def test_execute_sql_concatenates_multiple_batches():
    """execute_sql concatenates multiple record batches into one table."""
    import pyarrow as pa

    plugin = PostgresComputeEnginePlugin()
    engine = PostgresComputeEngine("pg", {"conninfo": "host=localhost"})

    batch1 = pa.record_batch({"id": [1, 2]})
    batch2 = pa.record_batch({"id": [3]})

    async def _mock_stream(sql):
        yield batch1
        yield batch2

    with patch.object(engine, "stream_arrow", side_effect=_mock_stream):
        result = plugin.execute_sql(engine, "SELECT id FROM t", {})

    assert result.num_rows == 3
    assert result.column("id").to_pylist() == [1, 2, 3]


def test_execute_sql_does_not_wrap_execution_error():
    """execute_sql re-raises ExecutionError without double-wrapping."""
    from rivet_core.errors import ExecutionError, RivetError

    plugin = PostgresComputeEnginePlugin()
    engine = PostgresComputeEngine("pg", {"conninfo": "host=localhost"})

    original = ExecutionError(RivetError(code="RVT-999", message="original"))

    async def _mock_stream(sql):
        raise original
        yield

    with patch.object(engine, "stream_arrow", side_effect=_mock_stream):
        with pytest.raises(ExecutionError) as exc_info:
            plugin.execute_sql(engine, "SELECT 1", {})
    assert exc_info.value.error.code == "RVT-999"
