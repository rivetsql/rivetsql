"""Tests for task 8.1: DuckDBComputeEnginePlugin.execute_sql round-trip."""

from __future__ import annotations

import pyarrow as pa
import pytest

from rivet_duckdb.engine import DuckDBComputeEnginePlugin


@pytest.fixture
def plugin():
    return DuckDBComputeEnginePlugin()


@pytest.fixture
def engine(plugin):
    return plugin.create_engine("test_duckdb", {})


def test_execute_sql_select_from_input_table(plugin, engine):
    table = pa.table({"x": [1, 2, 3]})
    result = plugin.execute_sql(engine, "SELECT x FROM t ORDER BY x", {"t": table})
    assert isinstance(result, pa.Table)
    assert result.column("x").to_pylist() == [1, 2, 3]


def test_execute_sql_join_two_tables(plugin, engine):
    t1 = pa.table({"id": [1, 2], "val": [10, 20]})
    t2 = pa.table({"id": [1, 2], "label": ["a", "b"]})
    result = plugin.execute_sql(
        engine,
        "SELECT t1.id, t1.val, t2.label FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id",
        {"t1": t1, "t2": t2},
    )
    assert result.num_rows == 2
    assert result.column("label").to_pylist() == ["a", "b"]


def test_execute_sql_aggregation(plugin, engine):
    table = pa.table({"grp": ["a", "a", "b"], "val": [1, 2, 3]})
    result = plugin.execute_sql(
        engine,
        "SELECT grp, SUM(val) as total FROM data GROUP BY grp ORDER BY grp",
        {"data": table},
    )
    assert result.column("grp").to_pylist() == ["a", "b"]
    assert result.column("total").to_pylist() == [3, 3]


def test_execute_sql_empty_input_tables(plugin, engine):
    result = plugin.execute_sql(engine, "SELECT 42 AS answer", {})
    assert result.column("answer").to_pylist() == [42]


def test_execute_sql_invalid_sql_raises(plugin, engine):
    with pytest.raises(Exception):  # noqa: B017
        plugin.execute_sql(engine, "INVALID SQL QUERY", {})


def test_execute_sql_returns_arrow_table(plugin, engine):
    result = plugin.execute_sql(engine, "SELECT 1 AS n", {})
    assert isinstance(result, pa.Table)
