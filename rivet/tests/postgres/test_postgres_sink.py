"""Tests for task 11.2: PostgreSQL Sink — binary COPY for append/truncate_insert, ON CONFLICT for merge, transaction-wrapped."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import PluginRegistry, SinkPlugin
from rivet_core.strategies import _ArrowMaterializedRef
from rivet_postgres.sink import (
    _KNOWN_SINK_OPTIONS,
    _STRATEGIES_REQUIRING_CONFLICT_KEY,
    _VALID_ON_CONFLICT_ACTIONS,
    SUPPORTED_STRATEGIES,
    PostgresSink,
    _build_conninfo,
    _col_names,
    _create_table_sql,
    _get_merge_keys,
    _pg_type,
    _quote,
)

# ── Helpers ────────────────────────────────────────────────────────────────


def _make_catalog(options: dict | None = None) -> Catalog:
    return Catalog(
        name="pgdb",
        type="postgres",
        options=options or {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "user",
            "password": "pass",
        },
    )


def _make_joint(
    name: str = "sink_j",
    table: str | None = "target_table",
    strategy: str = "append",
    write_strategy_config: dict | None = None,
) -> Joint:
    j = Joint(name=name, joint_type="sink", catalog="pgdb", table=table, write_strategy=strategy)
    if write_strategy_config:
        j.write_strategy_config = write_strategy_config  # type: ignore[attr-defined]
    return j


def _make_material(data: pa.Table | None = None) -> Material:
    if data is None:
        data = pa.table({"id": [1, 2], "name": ["alice", "bob"]})
    ref = _ArrowMaterializedRef(data)
    return Material(name="mat", catalog="pgdb", materialized_ref=ref, state="materialized")


def _mock_psycopg_module():
    """Build a mock psycopg module with AsyncConnection that tracks calls."""
    mock_copy = AsyncMock()
    mock_copy.__aenter__ = AsyncMock(return_value=mock_copy)
    mock_copy.__aexit__ = AsyncMock(return_value=False)
    mock_copy.set_types = MagicMock()
    mock_copy.write_row = AsyncMock()

    mock_cursor = AsyncMock()
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)
    mock_cursor.execute = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)
    mock_cursor.copy = MagicMock(return_value=mock_copy)

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_cursor)
    mock_conn.commit = AsyncMock()

    mock_psycopg = MagicMock()
    mock_psycopg.AsyncConnection.connect = AsyncMock(return_value=mock_conn)

    return mock_psycopg, mock_conn, mock_cursor, mock_copy


# ── Registration ───────────────────────────────────────────────────────────


def test_sink_has_catalog_type():
    assert PostgresSink.catalog_type == "postgres"


def test_sink_is_sink_plugin():
    assert isinstance(PostgresSink(), SinkPlugin)


def test_registry_can_register_sink():
    registry = PluginRegistry()
    registry.register_sink(PostgresSink())
    assert registry._sinks.get("postgres") is not None


def test_supported_strategies_all_eight():
    expected = {"append", "replace", "truncate_insert", "merge", "delete_insert", "incremental_append", "scd2", "partition"}
    assert expected == SUPPORTED_STRATEGIES


# ── Utility functions ──────────────────────────────────────────────────────


def test_build_conninfo():
    opts = {"host": "db.example.com", "port": 5433, "database": "mydb", "user": "u", "password": "p"}
    result = _build_conninfo(opts)
    assert "host=db.example.com" in result
    assert "port=5433" in result
    assert "dbname=mydb" in result


def test_quote():
    assert _quote("my_col") == '"my_col"'


def test_col_names():
    t = pa.table({"a": [1], "b": ["x"]})
    assert _col_names(t) == ["a", "b"]


def test_pg_type_mapping():
    assert _pg_type(pa.int32()) == "INTEGER"
    assert _pg_type(pa.int64()) == "BIGINT"
    assert _pg_type(pa.float64()) == "DOUBLE PRECISION"
    assert _pg_type(pa.float32()) == "REAL"
    assert _pg_type(pa.string()) == "TEXT"
    assert _pg_type(pa.bool_()) == "BOOLEAN"
    assert _pg_type(pa.date32()) == "DATE"
    assert _pg_type(pa.timestamp("us")) == "TIMESTAMP"
    assert _pg_type(pa.binary()) == "BYTEA"


def test_create_table_sql():
    schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
    sql = _create_table_sql("my_table", schema)
    assert "CREATE TABLE IF NOT EXISTS my_table" in sql
    assert '"id" INTEGER' in sql
    assert '"name" TEXT' in sql


def test_get_merge_keys_from_write_strategy_config():
    j = _make_joint(write_strategy_config={"merge_key": ["id"]})
    assert _get_merge_keys(j) == ["id"]


def test_get_merge_keys_empty():
    j = _make_joint()
    assert _get_merge_keys(j) == []


# ── Unsupported strategy ──────────────────────────────────────────────────


def test_unsupported_strategy_raises():
    sink = PostgresSink()
    with pytest.raises(ExecutionError) as exc_info:
        sink.write(_make_catalog(), _make_joint(), _make_material(), "invalid_strategy")
    assert exc_info.value.error.code == "RVT-501"


# ── Read-only catalog ─────────────────────────────────────────────────────


def test_read_only_catalog_raises():
    sink = PostgresSink()
    catalog = _make_catalog({"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p", "read_only": True})
    with pytest.raises(ExecutionError) as exc_info:
        sink.write(catalog, _make_joint(), _make_material(), "append")
    assert exc_info.value.error.code == "RVT-201"


# ── Append strategy (binary COPY) ─────────────────────────────────────────


def test_append_uses_binary_copy():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1, 2], "val": ["a", "b"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="append"), _make_material(data), "append")

    # Verify binary COPY was used
    copy_calls = mock_cursor.copy.call_args_list
    assert len(copy_calls) > 0
    copy_sql = copy_calls[0][0][0]
    assert "COPY" in copy_sql
    assert "FORMAT BINARY" in copy_sql


def test_append_writes_all_rows():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1, 2, 3]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="append"), _make_material(data), "append")

    assert mock_copy.write_row.await_count == 3


def test_append_commits_transaction():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="append"), _make_material(), "append")

    mock_conn.commit.assert_awaited()


# ── Replace strategy ──────────────────────────────────────────────────────


def test_replace_drops_and_creates():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="replace"), _make_material(), "replace")

    [str(c) for c in mock_cursor.execute.call_args_list]
    # Should have DROP TABLE and CREATE TABLE
    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    assert any("DROP TABLE" in s for s in sql_strs)
    assert any("CREATE TABLE" in s for s in sql_strs)


def test_replace_commits():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="replace"), _make_material(), "replace")

    mock_conn.commit.assert_awaited()


# ── Truncate_insert strategy (binary COPY) ─────────────────────────────────


def test_truncate_insert_truncates_then_copies():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="truncate_insert"), _make_material(data), "truncate_insert")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    assert any("TRUNCATE" in s for s in sql_strs)

    # Verify binary COPY was used
    copy_calls = mock_cursor.copy.call_args_list
    assert len(copy_calls) > 0
    copy_sql = copy_calls[0][0][0]
    assert "FORMAT BINARY" in copy_sql


def test_truncate_insert_commits():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="truncate_insert"), _make_material(), "truncate_insert")

    mock_conn.commit.assert_awaited()


# ── Merge strategy (ON CONFLICT) ──────────────────────────────────────────


def test_merge_uses_on_conflict():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1, 2], "val": ["a", "b"]})
    joint = _make_joint(strategy="merge", write_strategy_config={"merge_key": ["id"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "merge")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    on_conflict_calls = [s for s in sql_strs if "ON CONFLICT" in s]
    assert len(on_conflict_calls) > 0
    assert "DO UPDATE SET" in on_conflict_calls[0]


def test_merge_without_keys_falls_back_to_append():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="merge"), _make_material(data), "merge")

    # Should use binary COPY (fallback to append)
    copy_calls = mock_cursor.copy.call_args_list
    assert len(copy_calls) > 0


def test_merge_commits():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1], "val": ["a"]})
    joint = _make_joint(strategy="merge", write_strategy_config={"merge_key": ["id"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "merge")

    mock_conn.commit.assert_awaited()


# ── Delete_insert strategy ─────────────────────────────────────────────────


def test_delete_insert_deletes_by_key():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1, 2], "val": ["a", "b"]})
    joint = _make_joint(strategy="delete_insert", write_strategy_config={"merge_key": ["id"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "delete_insert")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    assert any("DELETE FROM" in s for s in sql_strs)


def test_delete_insert_without_keys_truncates():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="delete_insert"), _make_material(data), "delete_insert")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    assert any("TRUNCATE" in s for s in sql_strs)


def test_delete_insert_uses_binary_copy():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1], "val": ["a"]})
    joint = _make_joint(strategy="delete_insert", write_strategy_config={"merge_key": ["id"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "delete_insert")

    copy_calls = mock_cursor.copy.call_args_list
    assert len(copy_calls) > 0
    assert "FORMAT BINARY" in copy_calls[0][0][0]


# ── Incremental_append strategy ────────────────────────────────────────────


def test_incremental_append_uses_on_conflict_do_nothing():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1, 2], "val": ["a", "b"]})
    joint = _make_joint(strategy="incremental_append", write_strategy_config={"merge_key": ["id"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "incremental_append")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    on_conflict_calls = [s for s in sql_strs if "ON CONFLICT" in s]
    assert len(on_conflict_calls) > 0
    assert "DO NOTHING" in on_conflict_calls[0]


# ── SCD2 strategy ─────────────────────────────────────────────────────────


def test_scd2_creates_table_with_scd2_columns():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1], "val": ["a"]})
    joint = _make_joint(strategy="scd2", write_strategy_config={"merge_key": ["id"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "scd2")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    create_calls = [s for s in sql_strs if "CREATE TABLE" in s]
    assert len(create_calls) > 0
    assert "valid_from" in create_calls[0]
    assert "valid_to" in create_calls[0]
    assert "is_current" in create_calls[0]


def test_scd2_commits():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1], "val": ["a"]})
    joint = _make_joint(strategy="scd2", write_strategy_config={"merge_key": ["id"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "scd2")

    mock_conn.commit.assert_awaited()


# ── Partition strategy ─────────────────────────────────────────────────────


def test_partition_deletes_matching_partitions():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"region": ["us", "eu"], "val": [1, 2]})
    joint = _make_joint(strategy="partition", write_strategy_config={"partition_by": ["region"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "partition")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    assert any("DELETE FROM" in s for s in sql_strs)


def test_partition_without_cols_falls_back_to_replace():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="partition"), _make_material(data), "partition")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    assert any("DROP TABLE" in s for s in sql_strs)


def test_partition_uses_binary_copy():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"region": ["us"], "val": [1]})
    joint = _make_joint(strategy="partition", write_strategy_config={"partition_by": ["region"]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "partition")

    copy_calls = mock_cursor.copy.call_args_list
    assert len(copy_calls) > 0
    assert "FORMAT BINARY" in copy_calls[0][0][0]


# ── Transaction wrapping ──────────────────────────────────────────────────


@pytest.mark.parametrize("strategy", sorted(SUPPORTED_STRATEGIES))
def test_all_strategies_commit(strategy):
    """Every strategy must commit the transaction."""
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1], "val": ["a"]})
    wsc = {}
    if strategy in ("merge", "delete_insert", "scd2"):
        wsc = {"merge_key": ["id"]}
    if strategy == "partition":
        wsc = {"partition_by": ["id"]}
    joint = _make_joint(strategy=strategy, write_strategy_config=wsc)

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), strategy)

    mock_conn.commit.assert_awaited()


# ── Table name from joint ─────────────────────────────────────────────────


def test_uses_joint_table_name():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1]})
    joint = _make_joint(table="custom_table", strategy="replace")

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "replace")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    assert any("custom_table" in s for s in sql_strs)


def test_uses_joint_name_when_no_table():
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"id": [1]})
    joint = _make_joint(name="fallback_name", table=None, strategy="replace")

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), joint, _make_material(data), "replace")

    sql_strs = [c[0][0] if c[0] else "" for c in mock_cursor.execute.call_args_list]
    assert any("fallback_name" in s for s in sql_strs)


# ── Error handling ─────────────────────────────────────────────────────────


def test_connection_failure_raises_execution_error():
    mock_psycopg = MagicMock()
    mock_psycopg.AsyncConnection.connect = AsyncMock(side_effect=Exception("connection refused"))

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        with pytest.raises(ExecutionError) as exc_info:
            sink.write(_make_catalog(), _make_joint(), _make_material(), "append")

    assert exc_info.value.error.code == "RVT-501"


# ── Property 33: Binary COPY for append and truncate_insert ────────────────


def test_property33_append_uses_binary_copy_protocol():
    """Property 33: append strategy uses psycopg3 binary COPY protocol."""
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"x": [1, 2, 3]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="append"), _make_material(data), "append")

    copy_calls = mock_cursor.copy.call_args_list
    assert len(copy_calls) == 1
    assert "FORMAT BINARY" in copy_calls[0][0][0]
    assert mock_copy.write_row.await_count == 3


def test_property33_truncate_insert_uses_binary_copy_protocol():
    """Property 33: truncate_insert strategy uses psycopg3 binary COPY protocol."""
    mock_psycopg, mock_conn, mock_cursor, mock_copy = _mock_psycopg_module()
    data = pa.table({"x": [10, 20]})

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        sink = PostgresSink()
        sink.write(_make_catalog(), _make_joint(strategy="truncate_insert"), _make_material(data), "truncate_insert")

    copy_calls = mock_cursor.copy.call_args_list
    assert len(copy_calls) == 1
    assert "FORMAT BINARY" in copy_calls[0][0][0]
    assert mock_copy.write_row.await_count == 2


# ── Task 11.3: Sink options validation ────────────────────────────────────


def test_known_sink_options_set():
    assert {
        "table", "write_strategy", "create_table", "batch_size",
        "on_conflict_action", "on_conflict_key",
    } == _KNOWN_SINK_OPTIONS


def test_valid_on_conflict_actions():
    assert {"error", "update", "nothing"} == _VALID_ON_CONFLICT_ACTIONS


def test_strategies_requiring_conflict_key():
    assert {"merge", "delete_insert", "scd2"} == _STRATEGIES_REQUIRING_CONFLICT_KEY


def test_validate_options_accepts_valid_minimal():
    sink = PostgresSink()
    sink.validate_options({"table": "my_table"})  # no error


def test_validate_options_accepts_all_valid_options():
    sink = PostgresSink()
    sink.validate_options({
        "table": "my_table",
        "write_strategy": "merge",
        "create_table": True,
        "batch_size": 5000,
        "on_conflict_action": "update",
        "on_conflict_key": ["id"],
    })  # no error


def test_validate_options_rejects_unknown_option():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "unknown_opt": "x"})
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_opt" in str(exc_info.value)


def test_validate_options_rejects_multiple_unknown_options():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"foo": 1, "bar": 2})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_options_rejects_non_string_table():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": 123})
    assert exc_info.value.error.code == "RVT-201"
    assert "table" in str(exc_info.value)


def test_validate_options_rejects_invalid_write_strategy():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "write_strategy": "invalid_strategy"})
    assert exc_info.value.error.code == "RVT-201"
    assert "invalid_strategy" in str(exc_info.value)


def test_validate_options_accepts_all_valid_strategies():
    sink = PostgresSink()
    for strategy in SUPPORTED_STRATEGIES:
        opts: dict = {"table": "t", "write_strategy": strategy}
        if strategy in _STRATEGIES_REQUIRING_CONFLICT_KEY:
            opts["on_conflict_key"] = ["id"]
        sink.validate_options(opts)  # no error


def test_validate_options_rejects_non_bool_create_table():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "create_table": "yes"})
    assert exc_info.value.error.code == "RVT-201"
    assert "create_table" in str(exc_info.value)


def test_validate_options_accepts_create_table_false():
    sink = PostgresSink()
    sink.validate_options({"table": "t", "create_table": False})  # no error


def test_validate_options_rejects_zero_batch_size():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "batch_size": 0})
    assert exc_info.value.error.code == "RVT-201"
    assert "batch_size" in str(exc_info.value)


def test_validate_options_rejects_negative_batch_size():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "batch_size": -1})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_options_rejects_non_int_batch_size():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "batch_size": "1000"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_options_accepts_valid_batch_size():
    sink = PostgresSink()
    sink.validate_options({"table": "t", "batch_size": 10000})  # no error


def test_validate_options_rejects_invalid_on_conflict_action():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "on_conflict_action": "ignore"})
    assert exc_info.value.error.code == "RVT-201"
    assert "ignore" in str(exc_info.value)


def test_validate_options_accepts_valid_on_conflict_actions():
    sink = PostgresSink()
    for action in _VALID_ON_CONFLICT_ACTIONS:
        sink.validate_options({"table": "t", "on_conflict_action": action})  # no error


def test_validate_options_requires_on_conflict_key_for_merge():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "write_strategy": "merge"})
    assert exc_info.value.error.code == "RVT-201"
    assert "on_conflict_key" in str(exc_info.value)


def test_validate_options_requires_on_conflict_key_for_delete_insert():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "write_strategy": "delete_insert"})
    assert exc_info.value.error.code == "RVT-201"
    assert "on_conflict_key" in str(exc_info.value)


def test_validate_options_requires_on_conflict_key_for_scd2():
    sink = PostgresSink()
    with pytest.raises(PluginValidationError) as exc_info:
        sink.validate_options({"table": "t", "write_strategy": "scd2"})
    assert exc_info.value.error.code == "RVT-201"
    assert "on_conflict_key" in str(exc_info.value)


def test_validate_options_accepts_on_conflict_key_as_list():
    sink = PostgresSink()
    sink.validate_options({
        "table": "t",
        "write_strategy": "merge",
        "on_conflict_key": ["id", "tenant_id"],
    })  # no error


def test_validate_options_accepts_on_conflict_key_as_string():
    sink = PostgresSink()
    sink.validate_options({
        "table": "t",
        "write_strategy": "merge",
        "on_conflict_key": "id",
    })  # no error


def test_validate_options_default_write_strategy_is_replace():
    """Default write_strategy is 'replace', which does not require on_conflict_key."""
    sink = PostgresSink()
    sink.validate_options({"table": "t"})  # no error — default strategy is replace


def test_validate_options_empty_dict_is_valid():
    """Empty options dict is valid — all options have defaults."""
    sink = PostgresSink()
    sink.validate_options({})  # no error
