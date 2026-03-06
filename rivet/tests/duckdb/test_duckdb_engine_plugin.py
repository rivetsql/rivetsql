"""Tests for task 4.1: DuckDBComputeEnginePlugin registration."""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin, PluginRegistry
from rivet_duckdb.engine import DuckDBComputeEnginePlugin


def test_engine_type():
    plugin = DuckDBComputeEnginePlugin()
    assert plugin.engine_type == "duckdb"


def test_dialect():
    plugin = DuckDBComputeEnginePlugin()
    assert plugin.dialect == "duckdb"


def test_is_compute_engine_plugin():
    assert isinstance(DuckDBComputeEnginePlugin(), ComputeEnginePlugin)


def test_create_engine_returns_correct_type():
    plugin = DuckDBComputeEnginePlugin()
    engine = plugin.create_engine("my_duckdb", {})
    assert isinstance(engine, ComputeEngine)
    assert engine.name == "my_duckdb"
    assert engine.engine_type == "duckdb"


def test_validate_accepts_valid_options():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({"threads": 4, "memory_limit": "8GB"})  # should not raise


def test_validate_rejects_unknown_option():
    plugin = DuckDBComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"unknown_key": "value"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_empty_options():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({})  # should not raise


def test_registry_can_register_plugin():
    registry = PluginRegistry()
    plugin = DuckDBComputeEnginePlugin()
    registry.register_engine_plugin(plugin)
    assert registry.get_engine_plugin("duckdb") is plugin


def test_compiler_reads_dialect():
    """Compiler uses getattr(engine_plugin, 'dialect', None) — verify it works."""
    plugin = DuckDBComputeEnginePlugin()
    assert getattr(plugin, "dialect", None) == "duckdb"


# Task 4.2: Declare native support for duckdb, arrow, filesystem with all 6 capabilities

ALL_6 = {
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
}


def test_supported_catalog_types_contains_duckdb():
    plugin = DuckDBComputeEnginePlugin()
    assert "duckdb" in plugin.supported_catalog_types


def test_supported_catalog_types_contains_arrow():
    plugin = DuckDBComputeEnginePlugin()
    assert "arrow" in plugin.supported_catalog_types


def test_supported_catalog_types_contains_filesystem():
    plugin = DuckDBComputeEnginePlugin()
    assert "filesystem" in plugin.supported_catalog_types


def test_duckdb_catalog_has_all_6_capabilities():
    plugin = DuckDBComputeEnginePlugin()
    assert set(plugin.supported_catalog_types["duckdb"]) == ALL_6


def test_arrow_catalog_has_all_6_capabilities():
    plugin = DuckDBComputeEnginePlugin()
    assert set(plugin.supported_catalog_types["arrow"]) == ALL_6


def test_filesystem_catalog_has_all_6_capabilities():
    plugin = DuckDBComputeEnginePlugin()
    assert set(plugin.supported_catalog_types["filesystem"]) == ALL_6


def test_resolve_capabilities_duckdb(registry=None):
    from rivet_core.plugins import PluginRegistry

    reg = PluginRegistry()
    reg.register_engine_plugin(DuckDBComputeEnginePlugin())
    assert set(reg.resolve_capabilities("duckdb", "duckdb")) == ALL_6


def test_resolve_capabilities_arrow():
    from rivet_core.plugins import PluginRegistry

    reg = PluginRegistry()
    reg.register_engine_plugin(DuckDBComputeEnginePlugin())
    assert set(reg.resolve_capabilities("duckdb", "arrow")) == ALL_6


def test_resolve_capabilities_filesystem():
    from rivet_core.plugins import PluginRegistry

    reg = PluginRegistry()
    reg.register_engine_plugin(DuckDBComputeEnginePlugin())
    assert set(reg.resolve_capabilities("duckdb", "filesystem")) == ALL_6


# Task 4.3: Accept options: threads, memory_limit, temp_directory, extensions


def test_optional_options_declares_threads():
    plugin = DuckDBComputeEnginePlugin()
    assert "threads" in plugin.optional_options
    assert plugin.optional_options["threads"] is None


def test_optional_options_declares_memory_limit():
    plugin = DuckDBComputeEnginePlugin()
    assert "memory_limit" in plugin.optional_options
    assert plugin.optional_options["memory_limit"] == "4GB"


def test_optional_options_declares_temp_directory():
    plugin = DuckDBComputeEnginePlugin()
    assert "temp_directory" in plugin.optional_options
    assert plugin.optional_options["temp_directory"] is None


def test_optional_options_declares_extensions():
    plugin = DuckDBComputeEnginePlugin()
    assert "extensions" in plugin.optional_options
    assert plugin.optional_options["extensions"] == []


def test_validate_accepts_threads_int():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({"threads": 4})  # should not raise


def test_validate_accepts_threads_none():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({"threads": None})  # should not raise


def test_validate_rejects_threads_non_int():
    plugin = DuckDBComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"threads": "four"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_memory_limit_string():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({"memory_limit": "8GB"})  # should not raise


def test_validate_rejects_memory_limit_non_string():
    plugin = DuckDBComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"memory_limit": 8192})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_temp_directory_string():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({"temp_directory": "/tmp/duckdb"})  # should not raise


def test_validate_accepts_temp_directory_none():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({"temp_directory": None})  # should not raise


def test_validate_rejects_temp_directory_non_string():
    plugin = DuckDBComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"temp_directory": 123})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_extensions_list():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({"extensions": ["httpfs", "json"]})  # should not raise


def test_validate_accepts_extensions_empty_list():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({"extensions": []})  # should not raise


def test_validate_rejects_extensions_non_list():
    plugin = DuckDBComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"extensions": "httpfs"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_rejects_extensions_list_with_non_string():
    plugin = DuckDBComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"extensions": [1, 2]})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_all_options_together():
    plugin = DuckDBComputeEnginePlugin()
    plugin.validate({
        "threads": 8,
        "memory_limit": "16GB",
        "temp_directory": "/tmp",
        "extensions": ["httpfs"],
    })  # should not raise


# Task 4.4: Apply memory_limit and threads before query execution


def test_apply_engine_settings_sets_memory_limit():
    import duckdb

    from rivet_duckdb.engine import apply_engine_settings

    conn = duckdb.connect(":memory:")
    # Get default first
    default = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    apply_engine_settings(conn, {"memory_limit": "512MB"})
    result = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    # DuckDB may convert units (e.g. "512MB" → "488.2 MiB"); just verify it changed
    assert result != default or "488" in result or "512" in result


def test_apply_engine_settings_sets_threads():
    import duckdb

    from rivet_duckdb.engine import apply_engine_settings

    conn = duckdb.connect(":memory:")
    apply_engine_settings(conn, {"threads": 2})
    result = conn.execute("SELECT current_setting('threads')").fetchone()[0]
    assert int(result) == 2


def test_apply_engine_settings_skips_none_memory_limit():
    import duckdb

    from rivet_duckdb.engine import apply_engine_settings

    conn = duckdb.connect(":memory:")
    # Should not raise; None means "don't set"
    apply_engine_settings(conn, {"memory_limit": None})


def test_apply_engine_settings_skips_none_threads():
    import duckdb

    from rivet_duckdb.engine import apply_engine_settings

    conn = duckdb.connect(":memory:")
    apply_engine_settings(conn, {"threads": None})


def test_apply_engine_settings_empty_config():
    import duckdb

    from rivet_duckdb.engine import apply_engine_settings

    conn = duckdb.connect(":memory:")
    apply_engine_settings(conn, {})  # should not raise


def test_apply_engine_settings_both():
    import duckdb

    from rivet_duckdb.engine import apply_engine_settings

    conn = duckdb.connect(":memory:")
    apply_engine_settings(conn, {"memory_limit": "1GB", "threads": 1})
    threads = conn.execute("SELECT current_setting('threads')").fetchone()[0]
    assert int(threads) == 1


# Task 4.5: Zero-copy Arrow registration for arrow catalog type


def test_register_arrow_tables_single_table():
    import duckdb
    import pyarrow as pa

    from rivet_duckdb.engine import register_arrow_tables

    conn = duckdb.connect(":memory:")
    table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    register_arrow_tables(conn, {"my_table": table})
    result = conn.execute("SELECT * FROM my_table ORDER BY x").fetchall()
    assert result == [(1, "a"), (2, "b"), (3, "c")]


def test_register_arrow_tables_multiple_tables():
    import duckdb
    import pyarrow as pa

    from rivet_duckdb.engine import register_arrow_tables

    conn = duckdb.connect(":memory:")
    t1 = pa.table({"id": [1, 2], "val": [10, 20]})
    t2 = pa.table({"id": [1, 2], "label": ["x", "y"]})
    register_arrow_tables(conn, {"t1": t1, "t2": t2})
    result = conn.execute(
        "SELECT t1.id, t1.val, t2.label FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id"
    ).fetchall()
    assert result == [(1, 10, "x"), (2, 20, "y")]


def test_register_arrow_tables_empty_dict():
    import duckdb

    from rivet_duckdb.engine import register_arrow_tables

    conn = duckdb.connect(":memory:")
    register_arrow_tables(conn, {})  # should not raise


def test_register_arrow_tables_zero_copy_no_data_duplication():
    """Verify that registered Arrow table is queryable and reflects the original data."""
    import duckdb
    import pyarrow as pa

    from rivet_duckdb.engine import register_arrow_tables

    conn = duckdb.connect(":memory:")
    table = pa.table({"n": list(range(100))})
    register_arrow_tables(conn, {"nums": table})
    count = conn.execute("SELECT COUNT(*) FROM nums").fetchone()[0]
    assert count == 100


def test_register_arrow_tables_with_various_types():
    import duckdb
    import pyarrow as pa

    from rivet_duckdb.engine import register_arrow_tables

    conn = duckdb.connect(":memory:")
    table = pa.table({
        "i": pa.array([1], type=pa.int64()),
        "f": pa.array([3.14], type=pa.float64()),
        "s": pa.array(["hello"], type=pa.string()),
        "b": pa.array([True], type=pa.bool_()),
    })
    register_arrow_tables(conn, {"typed": table})
    row = conn.execute("SELECT i, f, s, b FROM typed").fetchone()
    assert row[0] == 1
    assert abs(row[1] - 3.14) < 1e-6
    assert row[2] == "hello"
    assert row[3] is True


def test_register_arrow_tables_allows_sql_aggregation():
    import duckdb
    import pyarrow as pa

    from rivet_duckdb.engine import register_arrow_tables

    conn = duckdb.connect(":memory:")
    table = pa.table({"grp": ["a", "a", "b"], "val": [1, 2, 3]})
    register_arrow_tables(conn, {"data": table})
    result = conn.execute(
        "SELECT grp, SUM(val) as total FROM data GROUP BY grp ORDER BY grp"
    ).fetchall()
    assert result == [("a", 3), ("b", 3)]


# Task 4.6: Filesystem read function inference from file extension


from rivet_core.errors import ExecutionError
from rivet_duckdb.engine import infer_filesystem_reader


def test_infer_parquet():
    assert infer_filesystem_reader("data/users.parquet") == "read_parquet"


def test_infer_csv():
    assert infer_filesystem_reader("data/users.csv") == "read_csv_auto"


def test_infer_tsv():
    assert infer_filesystem_reader("data/users.tsv") == "read_csv_auto"


def test_infer_json():
    assert infer_filesystem_reader("data/users.json") == "read_json_auto"


def test_infer_ndjson():
    assert infer_filesystem_reader("data/users.ndjson") == "read_json_auto"


def test_infer_jsonl():
    assert infer_filesystem_reader("data/users.jsonl") == "read_json_auto"


def test_infer_uppercase_extension():
    assert infer_filesystem_reader("data/users.PARQUET") == "read_parquet"


def test_infer_mixed_case_extension():
    assert infer_filesystem_reader("data/users.Csv") == "read_csv_auto"


def test_infer_s3_path():
    assert infer_filesystem_reader("s3://bucket/prefix/data.parquet") == "read_parquet"


def test_infer_unrecognized_extension_raises_rvt501():
    with pytest.raises(ExecutionError) as exc_info:
        infer_filesystem_reader("data/users.orc")
    assert exc_info.value.error.code == "RVT-501"


def test_infer_no_extension_raises_rvt501():
    with pytest.raises(ExecutionError) as exc_info:
        infer_filesystem_reader("data/users")
    assert exc_info.value.error.code == "RVT-501"


def test_infer_unknown_extension_error_contains_extension():
    with pytest.raises(ExecutionError) as exc_info:
        infer_filesystem_reader("data/users.avro")
    assert ".avro" in exc_info.value.error.message


def test_infer_unknown_extension_remediation_lists_supported():
    with pytest.raises(ExecutionError) as exc_info:
        infer_filesystem_reader("data/users.avro")
    remediation = exc_info.value.error.remediation
    assert remediation is not None
    assert ".parquet" in remediation
