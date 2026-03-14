"""Integration tests for coherence remaining fixes.

Tests DuckDB catalog test_connection with real in-process DuckDB connections.
"""

from __future__ import annotations

import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog
from rivet_duckdb.catalog import DuckDBCatalogPlugin

# ── DuckDB test_connection ───────────────────────────────────────────


def _make_duckdb_catalog(path: str = ":memory:") -> Catalog:
    return Catalog(
        name="test_duckdb",
        type="duckdb",
        options={"path": path},
    )


def test_duckdb_test_connection_memory():
    """test_connection succeeds with :memory: database."""
    plugin = DuckDBCatalogPlugin()
    catalog = _make_duckdb_catalog(":memory:")
    plugin.test_connection(catalog)  # should not raise


def test_duckdb_test_connection_inaccessible_path():
    """test_connection raises ExecutionError for an inaccessible path."""
    plugin = DuckDBCatalogPlugin()
    catalog = _make_duckdb_catalog("/nonexistent/dir/impossible.duckdb")
    with pytest.raises(ExecutionError) as exc_info:
        plugin.test_connection(catalog)

    assert exc_info.value.error.code == "RVT-501"
    assert "rivet_duckdb" in exc_info.value.error.context.get("plugin_name", "")


# ── Arrow test_connection ────────────────────────────────────────────


def test_arrow_test_connection_succeeds():
    """test_connection succeeds unconditionally for in-memory Arrow catalog."""
    from rivet_core.builtins.arrow_catalog import ArrowCatalogPlugin

    plugin = ArrowCatalogPlugin()
    catalog = Catalog(name="test_arrow", type="arrow", options={})
    plugin.test_connection(catalog)  # should not raise


# ── Filesystem test_connection ───────────────────────────────────────


def test_filesystem_test_connection_existing_path(tmp_path: object) -> None:
    """test_connection succeeds for an existing directory."""
    from rivet_core.builtins.filesystem_catalog import FilesystemCatalogPlugin

    plugin = FilesystemCatalogPlugin()
    catalog = Catalog(
        name="test_fs",
        type="filesystem",
        options={"path": str(tmp_path)},
    )
    plugin.test_connection(catalog)  # should not raise


def test_filesystem_test_connection_nonexistent_path() -> None:
    """test_connection raises ExecutionError for a nonexistent path."""
    from rivet_core.builtins.filesystem_catalog import FilesystemCatalogPlugin

    plugin = FilesystemCatalogPlugin()
    catalog = Catalog(
        name="test_fs",
        type="filesystem",
        options={"path": "/nonexistent/path"},
    )
    with pytest.raises(ExecutionError) as exc_info:
        plugin.test_connection(catalog)

    assert exc_info.value.error.code == "RVT-501"
    assert exc_info.value.error.context.get("plugin_name") == "filesystem"


# ── DuckDB filesystem sink _read_file error handling ─────────────────


def test_read_file_unsupported_format_raises_execution_error(tmp_path: object) -> None:
    """_read_file raises ExecutionError (not ValueError) for unsupported format."""
    from pathlib import Path

    from rivet_duckdb.filesystem_sink import _read_file

    dummy_file = Path(str(tmp_path)) / "data.xyz"
    dummy_file.write_text("dummy")

    with pytest.raises(ExecutionError) as exc_info:
        _read_file(dummy_file, "xyz")

    err = exc_info.value.error
    assert err.code == "RVT-501"
    assert err.context.get("plugin_name") == "rivet_duckdb"
    assert err.context.get("plugin_type") == "sink"


# ── Databricks DeferredMaterializedRef error handling ────────────────


def test_databricks_deferred_schema_raises_execution_error() -> None:
    """schema raises ExecutionError (not NotImplementedError) on DatabricksDeferredMaterializedRef."""
    from rivet_databricks.databricks_source import DatabricksDeferredMaterializedRef

    ref = DatabricksDeferredMaterializedRef(
        table="test_table",
        sql="SELECT * FROM test_table",
        version=None,
        change_data_feed=False,
    )
    with pytest.raises(ExecutionError) as exc_info:
        _ = ref.schema

    err = exc_info.value.error
    assert err.code == "RVT-501"
    assert err.context.get("plugin_name") == "rivet_databricks"
    assert err.context.get("plugin_type") == "source"


def test_databricks_deferred_row_count_raises_execution_error() -> None:
    """row_count raises ExecutionError (not NotImplementedError) on DatabricksDeferredMaterializedRef."""
    from rivet_databricks.databricks_source import DatabricksDeferredMaterializedRef

    ref = DatabricksDeferredMaterializedRef(
        table="test_table",
        sql="SELECT * FROM test_table",
        version=None,
        change_data_feed=False,
    )
    with pytest.raises(ExecutionError) as exc_info:
        _ = ref.row_count

    err = exc_info.value.error
    assert err.code == "RVT-501"
    assert err.context.get("plugin_name") == "rivet_databricks"
    assert err.context.get("plugin_type") == "source"


# ── Unity DeferredMaterializedRef error handling ─────────────────────


def test_unity_deferred_schema_raises_execution_error() -> None:
    """schema raises ExecutionError (not NotImplementedError) on UnityDeferredMaterializedRef."""
    from rivet_databricks.unity_source import UnityDeferredMaterializedRef

    ref = UnityDeferredMaterializedRef(
        table="test_table",
        catalog=None,
        version=None,
        timestamp=None,
        partition_filter=None,
    )
    with pytest.raises(ExecutionError) as exc_info:
        _ = ref.schema

    err = exc_info.value.error
    assert err.code == "RVT-501"
    assert err.context.get("plugin_name") == "rivet_databricks"
    assert err.context.get("plugin_type") == "source"


def test_unity_deferred_row_count_raises_execution_error() -> None:
    """row_count raises ExecutionError (not NotImplementedError) on UnityDeferredMaterializedRef."""
    from rivet_databricks.unity_source import UnityDeferredMaterializedRef

    ref = UnityDeferredMaterializedRef(
        table="test_table",
        catalog=None,
        version=None,
        timestamp=None,
        partition_filter=None,
    )
    with pytest.raises(ExecutionError) as exc_info:
        _ = ref.row_count

    err = exc_info.value.error
    assert err.code == "RVT-501"
    assert err.context.get("plugin_name") == "rivet_databricks"
    assert err.context.get("plugin_type") == "source"


# ── S3 catalog and source error handling ─────────────────────────────


def test_s3_catalog_get_schema_delta_raises_execution_error() -> None:
    """get_schema raises ExecutionError (not NotImplementedError) for delta format."""
    from rivet_aws.s3_catalog import S3CatalogPlugin

    plugin = S3CatalogPlugin()
    catalog = Catalog(
        name="test_s3",
        type="s3",
        options={"bucket": "test-bucket", "format": "delta"},
    )
    with pytest.raises(ExecutionError) as exc_info:
        plugin.get_schema(catalog, "some_table")

    err = exc_info.value.error
    assert err.code == "RVT-501"
    assert err.context.get("plugin_name") == "rivet_aws"
    assert err.context.get("plugin_type") == "catalog"


def test_s3_deferred_to_arrow_delta_raises_execution_error() -> None:
    """to_arrow raises ExecutionError (not NotImplementedError) for delta format."""
    from rivet_aws.s3_source import S3DeferredMaterializedRef

    ref = S3DeferredMaterializedRef(
        s3_path="s3://test-bucket/some_table",
        fmt="delta",
        catalog_options={"bucket": "test-bucket", "region": "us-east-1"},
        partition_columns=None,
        schema=None,
    )
    with pytest.raises(ExecutionError) as exc_info:
        ref.to_arrow()

    err = exc_info.value.error
    assert err.code == "RVT-501"
    assert err.context.get("plugin_name") == "rivet_aws"
    assert err.context.get("plugin_type") == "source"
