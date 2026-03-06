"""Property-based tests for write strategy declaration and validation.

Task 37.3: Write strategy declaration and validation properties

Properties verified:
- Every sink plugin declares `supported_strategies` as a class attribute
- Declared strategies are a subset of the canonical write strategy types
- Strategies declared as supported are accepted without error
- Strategies NOT in `supported_strategies` raise an error
- The declared strategies match the spec (Write Strategy Support Matrix)
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.write_strategies import VALID_WRITE_STRATEGY_TYPES

# All canonical write strategy types (8 core + partition)
ALL_WRITE_STRATEGIES = frozenset(VALID_WRITE_STRATEGY_TYPES) | {"partition"}

# ── Import sink plugins ────────────────────────────────────────────────────────

from rivet_aws.glue_sink import SUPPORTED_STRATEGIES as GLUE_STRATEGIES
from rivet_aws.glue_sink import GlueSink
from rivet_aws.s3_sink import S3Sink
from rivet_databricks.databricks_sink import SUPPORTED_STRATEGIES as DATABRICKS_STRATEGIES
from rivet_databricks.databricks_sink import DatabricksSink
from rivet_databricks.unity_sink import SUPPORTED_STRATEGIES as UNITY_STRATEGIES
from rivet_databricks.unity_sink import UnitySink
from rivet_duckdb.filesystem_sink import FILESYSTEM_SUPPORTED_STRATEGIES, FilesystemSink
from rivet_duckdb.sink import SUPPORTED_STRATEGIES as DUCKDB_STRATEGIES
from rivet_duckdb.sink import DuckDBSink
from rivet_postgres.sink import SUPPORTED_STRATEGIES as POSTGRES_STRATEGIES
from rivet_postgres.sink import PostgresSink

# ── Spec-defined strategy sets (from Write Strategy Support Matrix) ────────────

_SPEC_DUCKDB_DUCKDB = frozenset(
    {"append", "replace", "truncate_insert", "merge", "delete_insert",
     "incremental_append", "scd2", "partition"}
)
_SPEC_DUCKDB_FILESYSTEM = frozenset({"append", "replace", "partition"})
_SPEC_POSTGRES_POSTGRES = frozenset(
    {"append", "replace", "truncate_insert", "merge", "delete_insert",
     "incremental_append", "scd2", "partition"}
)
_SPEC_GLUE = frozenset({"append", "replace", "delete_insert", "incremental_append", "truncate_insert"})
_SPEC_UNITY = frozenset(
    {"append", "replace", "merge", "truncate_insert", "delete_insert",
     "incremental_append", "scd2", "partition"}
)
_SPEC_DATABRICKS = frozenset(
    {"append", "replace", "truncate_insert", "merge", "delete_insert",
     "incremental_append", "scd2", "partition"}
)

# All sink plugins with their supported_strategies attribute
_SINK_PLUGINS = [
    (DuckDBSink(), "duckdb"),
    (FilesystemSink(), "filesystem"),
    (PostgresSink(), "postgres"),
    (S3Sink(), "s3"),
    (GlueSink(), "glue"),
    (UnitySink(), "unity"),
    (DatabricksSink(), "databricks"),
]


# ── Property 1: Every sink plugin declares supported_strategies ────────────────

def test_all_sink_plugins_declare_supported_strategies():
    """Every sink plugin must have a supported_strategies class attribute."""
    for plugin, catalog_type in _SINK_PLUGINS:
        assert hasattr(plugin, "supported_strategies"), (
            f"{type(plugin).__name__} (catalog_type={catalog_type!r}) "
            f"must declare supported_strategies"
        )
        assert isinstance(plugin.supported_strategies, frozenset), (
            f"{type(plugin).__name__}.supported_strategies must be a frozenset"
        )
        assert len(plugin.supported_strategies) > 0, (
            f"{type(plugin).__name__}.supported_strategies must not be empty"
        )


# ── Property 2: Declared strategies match spec ────────────────────────────────

def test_duckdb_sink_strategies_match_spec():
    assert DUCKDB_STRATEGIES == _SPEC_DUCKDB_DUCKDB


def test_filesystem_sink_strategies_match_spec():
    assert FILESYSTEM_SUPPORTED_STRATEGIES == _SPEC_DUCKDB_FILESYSTEM


def test_postgres_sink_strategies_match_spec():
    assert POSTGRES_STRATEGIES == _SPEC_POSTGRES_POSTGRES


def test_glue_sink_strategies_match_spec():
    assert GLUE_STRATEGIES == _SPEC_GLUE


def test_unity_sink_strategies_match_spec():
    assert UNITY_STRATEGIES == _SPEC_UNITY


def test_databricks_sink_strategies_match_spec():
    assert DATABRICKS_STRATEGIES == _SPEC_DATABRICKS


# ── Property 3: Supported strategies are a subset of canonical types ──────────

@pytest.mark.parametrize("plugin,catalog_type", _SINK_PLUGINS)
def test_supported_strategies_are_canonical(plugin, catalog_type):
    """All declared strategies must be recognized strategy names."""
    # Canonical names include the 8 core types plus write_* capability names
    canonical = ALL_WRITE_STRATEGIES | {
        "write_append", "write_replace", "write_partition", "write_merge",
        "write_scd2", "write_incremental_append", "write_delete_insert",
        "truncate_insert", "delete_insert", "incremental_append",
    }
    for strategy in plugin.supported_strategies:
        assert strategy in canonical, (
            f"{type(plugin).__name__}.supported_strategies contains unknown strategy {strategy!r}"
        )


# ── Property 4: Supported strategies are accepted (membership check) ──────────

@settings(max_examples=50)
@given(strategy=st.sampled_from(sorted(DUCKDB_STRATEGIES)))
def test_duckdb_sink_accepts_supported_strategies(strategy):
    """DuckDB sink: every declared strategy is in supported_strategies."""
    assert strategy in DuckDBSink.supported_strategies


@settings(max_examples=50)
@given(strategy=st.sampled_from(sorted(POSTGRES_STRATEGIES)))
def test_postgres_sink_accepts_supported_strategies(strategy):
    """PostgreSQL sink: every declared strategy is in supported_strategies."""
    assert strategy in PostgresSink.supported_strategies


@settings(max_examples=50)
@given(strategy=st.sampled_from(sorted(GLUE_STRATEGIES)))
def test_glue_sink_accepts_supported_strategies(strategy):
    """Glue sink: every declared strategy is in supported_strategies."""
    assert strategy in GlueSink.supported_strategies


@settings(max_examples=50)
@given(strategy=st.sampled_from(sorted(UNITY_STRATEGIES)))
def test_unity_sink_accepts_supported_strategies(strategy):
    """Unity sink: every declared strategy is in supported_strategies."""
    assert strategy in UnitySink.supported_strategies


@settings(max_examples=50)
@given(strategy=st.sampled_from(sorted(DATABRICKS_STRATEGIES)))
def test_databricks_sink_accepts_supported_strategies(strategy):
    """Databricks sink: every declared strategy is in supported_strategies."""
    assert strategy in DatabricksSink.supported_strategies


@settings(max_examples=50)
@given(strategy=st.sampled_from(sorted(FILESYSTEM_SUPPORTED_STRATEGIES)))
def test_filesystem_sink_accepts_supported_strategies(strategy):
    """Filesystem sink: every declared strategy is in supported_strategies."""
    assert strategy in FilesystemSink.supported_strategies


# ── Property 5: Unsupported strategies are rejected ──────────────────────────

# Strategies that are clearly invalid (not in any sink's supported set)
_CLEARLY_INVALID = st.text(min_size=1, max_size=30).filter(
    lambda s: (
        s not in ALL_WRITE_STRATEGIES
        and s not in {"write_append", "write_replace", "write_partition", "write_merge",
                      "write_scd2", "write_incremental_append", "write_delete_insert",
                      "truncate_insert", "delete_insert", "incremental_append"}
        and s.strip() == s
        and len(s) > 0
    )
)


def _make_catalog(catalog_type: str, options: dict | None = None):
    """Create a minimal mock catalog object."""
    class _Catalog:
        pass
    c = _Catalog()
    c.name = f"test_{catalog_type}"
    c.type = catalog_type
    c.options = options or {}
    return c


def _make_joint(name: str = "test_joint", table: str = "test_table",
                strategy: str = "replace", sink_options: dict | None = None):
    """Create a minimal mock joint object."""
    class _Joint:
        pass
    j = _Joint()
    j.name = name
    j.table = table
    j.write_strategy = strategy
    j.write_strategy_config = {}
    j.sql = f"SELECT * FROM {table}"
    j.sink_options = sink_options or {}
    return j


def _make_material():
    """Create a minimal mock material object."""
    import pyarrow as pa

    class _Material:
        def to_arrow(self):
            return pa.table({"id": [1, 2], "val": ["a", "b"]})
    return _Material()


@settings(max_examples=50)
@given(strategy=_CLEARLY_INVALID)
def test_duckdb_sink_rejects_unsupported_strategies(strategy):
    """DuckDB sink: strategies not in supported_strategies raise an error."""
    sink = DuckDBSink()
    catalog = _make_catalog("duckdb", {"path": ":memory:", "read_only": False})
    joint = _make_joint(strategy=strategy)
    material = _make_material()

    with pytest.raises((ExecutionError, PluginValidationError, ValueError)):
        sink.write(catalog, joint, material, strategy)


@settings(max_examples=50)
@given(strategy=_CLEARLY_INVALID)
def test_glue_sink_rejects_unsupported_strategies(strategy):
    """Glue sink: strategies not in supported_strategies raise an error."""
    sink = GlueSink()
    catalog = _make_catalog("glue", {"database": "test_db", "region": "us-east-1"})
    joint = _make_joint(
        strategy=strategy,
        sink_options={"table": "test_table", "write_strategy": strategy},
    )
    material = _make_material()

    with pytest.raises((ExecutionError, PluginValidationError, ValueError)):
        sink.write(catalog, joint, material, strategy)


@settings(max_examples=50)
@given(strategy=_CLEARLY_INVALID)
def test_unity_sink_rejects_unsupported_strategies(strategy):
    """Unity sink: strategies not in supported_strategies raise an error."""
    sink = UnitySink()
    catalog = _make_catalog("unity", {
        "host": "https://unity.example.com",
        "catalog_name": "prod",
        "schema": "default",
    })
    joint = _make_joint(
        strategy=strategy,
        sink_options={"table": "test_table", "write_strategy": strategy},
    )
    material = _make_material()

    with pytest.raises((ExecutionError, PluginValidationError, ValueError)):
        sink.write(catalog, joint, material, strategy)


@settings(max_examples=50)
@given(strategy=_CLEARLY_INVALID)
def test_databricks_sink_rejects_unsupported_strategies(strategy):
    """Databricks sink: strategies not in supported_strategies raise an error."""
    sink = DatabricksSink()
    catalog = _make_catalog("databricks", {
        "workspace_url": "https://adb-123.azuredatabricks.net",
        "catalog": "main",
        "schema": "default",
    })
    joint = _make_joint(
        strategy=strategy,
        sink_options={"table": "test_table", "write_strategy": strategy},
    )
    material = _make_material()

    with pytest.raises((ExecutionError, PluginValidationError, ValueError)):
        sink.write(catalog, joint, material, strategy)


# ── Property 6: Glue explicitly rejects merge and scd2 ───────────────────────

@pytest.mark.parametrize("strategy", ["merge", "scd2"])
def test_glue_sink_does_not_declare_merge_or_scd2(strategy):
    """Glue sink must not declare merge/scd2 per spec (no Delta transaction log)."""
    assert strategy not in GLUE_STRATEGIES


@pytest.mark.parametrize("strategy", ["merge", "scd2"])
def test_glue_sink_raises_on_merge_and_scd2(strategy):
    """Glue sink raises PluginValidationError for merge/scd2 strategies."""
    sink = GlueSink()
    catalog = _make_catalog("glue", {"database": "test_db", "region": "us-east-1"})
    joint = _make_joint(
        strategy=strategy,
        sink_options={"table": "test_table", "write_strategy": strategy},
    )
    material = _make_material()

    with pytest.raises((ExecutionError, PluginValidationError)):
        sink.write(catalog, joint, material, strategy)


# ── Property 7: DuckDB filesystem sink only supports append/replace/partition ──

def test_filesystem_sink_only_supports_three_strategies():
    """Filesystem sink must support exactly: append, replace, partition."""
    assert {"append", "replace", "partition"} == FILESYSTEM_SUPPORTED_STRATEGIES


@pytest.mark.parametrize("strategy", ["truncate_insert", "merge", "delete_insert", "incremental_append", "scd2"])
def test_filesystem_sink_does_not_declare_complex_strategies(strategy):
    """Filesystem sink must not declare complex strategies."""
    assert strategy not in FILESYSTEM_SUPPORTED_STRATEGIES


# ── Property 8: Databricks sink requires merge_key for merge/delete_insert/scd2

@pytest.mark.parametrize("strategy", ["merge", "delete_insert", "scd2"])
def test_databricks_sink_requires_merge_key(strategy):
    """Databricks sink must reject merge/delete_insert/scd2 without merge_key."""
    sink = DatabricksSink()
    catalog = _make_catalog("databricks", {
        "workspace_url": "https://adb-123.azuredatabricks.net",
        "catalog": "main",
        "schema": "default",
    })
    joint = _make_joint(
        strategy=strategy,
        # No merge_key in sink_options
        sink_options={"table": "test_table", "write_strategy": strategy},
    )
    material = _make_material()

    with pytest.raises((ExecutionError, PluginValidationError)):
        sink.write(catalog, joint, material, strategy)


# ── Property 9: Unity sink requires merge_key for merge/delete_insert/scd2 ────

@pytest.mark.parametrize("strategy", ["merge", "delete_insert", "scd2"])
def test_unity_sink_requires_merge_key(strategy):
    """Unity sink must reject merge/delete_insert/scd2 without merge_key."""
    sink = UnitySink()
    catalog = _make_catalog("unity", {
        "host": "https://unity.example.com",
        "catalog_name": "prod",
        "schema": "default",
    })
    joint = _make_joint(
        strategy=strategy,
        sink_options={"table": "test_table", "write_strategy": strategy},
    )
    material = _make_material()

    with pytest.raises((ExecutionError, PluginValidationError)):
        sink.write(catalog, joint, material, strategy)
