"""Property-based tests: Introspection Fault Isolation (Property 9).

Property 9: Introspection Fault Isolation
  For any set of source joints where a subset fails or times out during
  introspection, the successfully introspected sources shall have their
  output_schema and source_stats populated identically to a run where no
  failures occurred, and each failed source shall produce a warning without
  blocking others.
"""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.compiler import compile
from rivet_core.introspection import ColumnDetail, ObjectMetadata, ObjectSchema
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEnginePlugin,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)

# ── Helpers ─────────────────────────────────────────────────────────────────────


class _StubCatalogPlugin(CatalogPlugin):
    type = "stub"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


class _IntrospectableCatalogPlugin(CatalogPlugin):
    """Catalog plugin that succeeds or fails based on table name convention.

    Tables whose name is in ``fail_tables`` raise RuntimeError during
    ``get_schema``.  All others return a deterministic two-column schema.
    """

    type = "introspectable"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def __init__(self, fail_tables: set[str] | None = None) -> None:
        self.fail_tables: set[str] = fail_tables or set()
        self.schema_call_count = 0
        self.metadata_call_count = 0

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        self.schema_call_count += 1
        if table in self.fail_tables:
            raise RuntimeError(f"Simulated failure for {table}")
        return ObjectSchema(
            path=[table],
            node_type="table",
            columns=[
                ColumnDetail(
                    name="id", type="int64", native_type="INTEGER",
                    nullable=False, default=None, comment=None,
                    is_primary_key=True, is_partition_key=False,
                ),
                ColumnDetail(
                    name="value", type="utf8", native_type="VARCHAR",
                    nullable=True, default=None, comment=None,
                    is_primary_key=False, is_partition_key=False,
                ),
            ],
            primary_key=["id"],
            comment=None,
        )

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        self.metadata_call_count += 1
        if table in self.fail_tables:
            raise RuntimeError(f"Simulated metadata failure for {table}")
        return ObjectMetadata(
            path=[table],
            node_type="table",
            row_count=100,
            size_bytes=4096,
            last_modified=None,
            created_at=None,
            format="parquet",
            compression=None,
            owner=None,
            comment=None,
            location=None,
            column_statistics=[],
            partitioning=None,
        )


class _StubEnginePlugin(ComputeEnginePlugin):
    engine_type = "stub"
    supported_catalog_types: dict[str, list[str]] = {
        "stub": ["projection_pushdown"],
        "introspectable": ["projection_pushdown"],
    }

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
        raise NotImplementedError


class _StubSource(SourcePlugin):
    catalog_type = "introspectable"

    def read(self, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        return None


class _StubSink(SinkPlugin):
    catalog_type = "introspectable"

    def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
        pass


def _make_registry(catalog_plugin: CatalogPlugin) -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_catalog_plugin(catalog_plugin)
    reg.register_engine_plugin(_StubEnginePlugin())
    eng = _StubEnginePlugin().create_engine("stub-engine", {})
    reg.register_compute_engine(eng)
    reg.register_source(_StubSource())
    reg.register_sink(_StubSink())
    return reg


# ── Strategies ──────────────────────────────────────────────────────────────────

_source_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
    min_size=2,
    max_size=6,
)


@st.composite
def introspection_scenario(draw: st.DrawFn):
    """Generate a set of source joints with a random subset marked to fail.

    Returns (source_names, fail_set) where:
    - source_names: list of unique source joint names
    - fail_set: subset of source_names that should fail introspection
    """
    source_names = draw(
        st.lists(_source_name, min_size=2, max_size=6, unique=True)
    )
    # Pick a random subset to fail (at least 0, at most all-1 so we have some successes)
    max_fail = max(0, len(source_names) - 1)
    n_fail = draw(st.integers(min_value=0, max_value=max_fail))
    fail_set = set(draw(st.sampled_from(
        [frozenset(combo) for combo in _combinations(source_names, n_fail)]
    )))
    return source_names, fail_set


def _combinations(items: list[str], k: int) -> list[tuple[str, ...]]:
    """Generate all k-combinations of items."""
    from itertools import combinations
    return list(combinations(items, k))


# ── Property 9: Introspection Fault Isolation ───────────────────────────────────


@given(scenario=introspection_scenario())
@settings(max_examples=100)
def test_property9_successful_sources_have_schema_and_stats(scenario: tuple) -> None:
    """Successful sources get output_schema and source_stats populated."""
    source_names, fail_set = scenario

    catalog_plugin = _IntrospectableCatalogPlugin(fail_tables=fail_set)
    reg = _make_registry(catalog_plugin)

    joints = [
        Joint(
            name=name,
            joint_type="source",
            catalog="c",
            engine="stub-engine",
            table=name,
        )
        for name in source_names
    ]
    assembly = Assembly(joints)
    catalogs = [Catalog(name="c", type="introspectable")]
    engines = [ComputeEngine(name="stub-engine", engine_type="stub")]

    result = compile(assembly, catalogs, engines, reg)

    for cj in result.joints:
        if cj.name not in fail_set:
            # Successful sources must have schema populated
            assert cj.output_schema is not None, (
                f"Source '{cj.name}' should have output_schema but got None"
            )
            assert len(cj.output_schema.columns) == 2
            assert cj.output_schema.columns[0].name == "id"
            assert cj.output_schema.columns[1].name == "value"
            # Successful sources must have source_stats populated
            assert cj.source_stats is not None, (
                f"Source '{cj.name}' should have source_stats but got None"
            )
            assert cj.source_stats.row_count == 100


@given(scenario=introspection_scenario())
@settings(max_examples=100)
def test_property9_failed_sources_produce_warnings(scenario: tuple) -> None:
    """Failed sources produce warnings and do not block successful ones."""
    source_names, fail_set = scenario

    catalog_plugin = _IntrospectableCatalogPlugin(fail_tables=fail_set)
    reg = _make_registry(catalog_plugin)

    joints = [
        Joint(
            name=name,
            joint_type="source",
            catalog="c",
            engine="stub-engine",
            table=name,
        )
        for name in source_names
    ]
    assembly = Assembly(joints)
    catalogs = [Catalog(name="c", type="introspectable")]
    engines = [ComputeEngine(name="stub-engine", engine_type="stub")]

    result = compile(assembly, catalogs, engines, reg)

    # Each failed source should produce at least one warning
    for failed_name in fail_set:
        assert any(
            failed_name in w for w in result.warnings
        ), f"Expected a warning mentioning '{failed_name}' but found none in {result.warnings}"

    # Failed sources should NOT have output_schema
    for cj in result.joints:
        if cj.name in fail_set:
            assert cj.output_schema is None, (
                f"Failed source '{cj.name}' should not have output_schema"
            )


@given(scenario=introspection_scenario())
@settings(max_examples=100)
def test_property9_successful_results_identical_regardless_of_failures(
    scenario: tuple,
) -> None:
    """Successful sources produce identical schema/stats whether or not other sources fail."""
    source_names, fail_set = scenario

    # Run with failures
    catalog_plugin_with_failures = _IntrospectableCatalogPlugin(fail_tables=fail_set)
    reg_with_failures = _make_registry(catalog_plugin_with_failures)

    # Run without failures (all succeed)
    catalog_plugin_no_failures = _IntrospectableCatalogPlugin(fail_tables=set())
    reg_no_failures = _make_registry(catalog_plugin_no_failures)

    joints = [
        Joint(
            name=name,
            joint_type="source",
            catalog="c",
            engine="stub-engine",
            table=name,
        )
        for name in source_names
    ]
    catalogs = [Catalog(name="c", type="introspectable")]
    engines = [ComputeEngine(name="stub-engine", engine_type="stub")]

    result_with_failures = compile(
        Assembly(joints), catalogs, engines, reg_with_failures
    )
    result_no_failures = compile(
        Assembly(joints), catalogs, engines, reg_no_failures
    )

    # Build lookup maps
    cj_map_failures = {cj.name: cj for cj in result_with_failures.joints}
    cj_map_clean = {cj.name: cj for cj in result_no_failures.joints}

    # For every source that succeeded in both runs, schema and stats must match
    for name in source_names:
        if name not in fail_set:
            cj_f = cj_map_failures[name]
            cj_c = cj_map_clean[name]
            assert cj_f.output_schema == cj_c.output_schema, (
                f"Schema mismatch for '{name}': {cj_f.output_schema} != {cj_c.output_schema}"
            )
            assert cj_f.source_stats == cj_c.source_stats, (
                f"Stats mismatch for '{name}': {cj_f.source_stats} != {cj_c.source_stats}"
            )
