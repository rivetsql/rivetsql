"""Task 38.3: Cross-plugin pipeline integration tests.

Tests that multiple plugins work together in heterogeneous pipelines:
- Multi-plugin registry with all 6 packages
- Cross-catalog pipeline compilation (e.g., Arrow source → DuckDB transform → DuckDB sink)
- Engine instance change triggers materialization at boundaries
- Adapter capability resolution across plugins
- Adapter precedence in multi-plugin scenarios
- MaterializedRef contract (.to_arrow()) across plugin boundaries
- Write strategy validation across plugin boundaries
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from rivet_core.assembly import Assembly
from rivet_core.compiler import CompiledAssembly, compile
from rivet_core.models import Catalog, ComputeEngine, Joint, Material
from rivet_core.plugins import PluginRegistry

# ── Helpers ──────────────────────────────────────────────────────────────────


def _multi_plugin_registry() -> PluginRegistry:
    """Create a registry with all 6 plugin packages registered."""
    from rivet_aws import AWSPlugin
    from rivet_databricks import DatabricksPlugin
    from rivet_duckdb import DuckDBPlugin
    from rivet_polars import PolarsPlugin
    from rivet_postgres import PostgresPlugin
    from rivet_pyspark import PySparkPlugin

    registry = PluginRegistry()
    registry.register_builtins()
    # Register in alphabetical order per spec
    AWSPlugin(registry)
    DuckDBPlugin(registry)
    PolarsPlugin(registry)
    PostgresPlugin(registry)
    PySparkPlugin(registry)
    DatabricksPlugin(registry)
    return registry


def _compile_pipeline(
    registry: PluginRegistry,
    joints: list[Joint],
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
) -> CompiledAssembly:
    assembly = Assembly(joints)
    return compile(assembly, catalogs, engines, registry)


# ── Test: Multi-plugin registry ──────────────────────────────────────────────


class TestMultiPluginRegistry:
    """Verify all 6 plugins register without conflicts in a single registry."""

    def setup_method(self):
        self.registry = _multi_plugin_registry()

    def test_all_catalog_types_registered(self):
        for ct in ("duckdb", "postgres", "s3", "glue", "unity", "databricks"):
            assert self.registry.get_catalog_plugin(ct) is not None, f"Missing catalog: {ct}"

    def test_all_engine_types_registered(self):
        for et in ("duckdb", "postgres", "polars", "pyspark", "databricks"):
            assert self.registry.get_engine_plugin(et) is not None, f"Missing engine: {et}"

    def test_builtin_catalog_types_present(self):
        assert self.registry.get_catalog_plugin("arrow") is not None
        assert self.registry.get_catalog_plugin("filesystem") is not None

    def test_duckdb_adapters_for_external_catalogs(self):
        for ct in ("s3", "glue", "unity"):
            adapter = self.registry.get_adapter("duckdb", ct)
            assert adapter is not None, f"Missing duckdb adapter for {ct}"

    def test_polars_adapters_for_external_catalogs(self):
        for ct in ("s3", "glue", "unity"):
            adapter = self.registry.get_adapter("polars", ct)
            assert adapter is not None, f"Missing polars adapter for {ct}"

    def test_pyspark_adapters_for_external_catalogs(self):
        for ct in ("s3", "glue", "unity"):
            adapter = self.registry.get_adapter("pyspark", ct)
            assert adapter is not None, f"Missing pyspark adapter for {ct}"

    def test_postgres_adapter_override_for_duckdb(self):
        """PostgresDuckDBAdapter (catalog_plugin) overrides DuckDB's baseline."""
        adapter = self.registry.get_adapter("duckdb", "postgres")
        assert adapter is not None
        assert adapter.source == "catalog_plugin"
        assert adapter.source_plugin == "rivet_postgres"

    def test_postgres_adapter_for_pyspark(self):
        adapter = self.registry.get_adapter("pyspark", "postgres")
        assert adapter is not None
        assert adapter.source == "catalog_plugin"

    def test_databricks_duckdb_adapter_exists(self):
        """DuckDB adapter for databricks catalog exists in rivet_databricks."""
        adapter = self.registry.get_adapter("duckdb", "databricks")
        assert adapter is not None


# ── Test: Cross-catalog pipeline compilation ─────────────────────────────────


class TestCrossCatalogPipelineCompilation:
    """Compile pipelines that span multiple catalog types and engines."""

    def setup_method(self):
        self.registry = _multi_plugin_registry()

    def test_duckdb_source_to_duckdb_sink(self):
        """Simple same-engine pipeline: duckdb source → sql → duckdb sink."""
        catalogs = [Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})]
        engine = ComputeEngine(name="ddb", engine_type="duckdb")
        self.registry.register_compute_engine(engine)

        joints = [
            Joint(name="src", joint_type="source", catalog="mydb", sql="SELECT 1 AS x"),
            Joint(name="transform", joint_type="sql", upstream=["src"], sql="SELECT x + 1 AS y FROM src"),
            Joint(name="sink", joint_type="sink", catalog="mydb", upstream=["transform"], table="output", write_strategy="replace"),
        ]
        result = _compile_pipeline(self.registry, joints, catalogs, [engine])
        assert result.success, f"Errors: {result.errors}"
        assert len(result.joints) == 3

    def test_arrow_source_duckdb_transform_duckdb_sink(self):
        """Arrow source → DuckDB SQL transform → DuckDB sink."""
        catalogs = [
            Catalog(name="arrow_cat", type="arrow"),
            Catalog(name="ddb_cat", type="duckdb", options={"path": ":memory:"}),
        ]
        engine = ComputeEngine(name="ddb", engine_type="duckdb")
        self.registry.register_compute_engine(engine)

        joints = [
            Joint(name="src", joint_type="source", catalog="arrow_cat"),
            Joint(name="transform", joint_type="sql", upstream=["src"], sql="SELECT * FROM src"),
            Joint(name="sink", joint_type="sink", catalog="ddb_cat", upstream=["transform"], table="output", write_strategy="replace"),
        ]
        result = _compile_pipeline(self.registry, joints, catalogs, [engine])
        assert result.success, f"Errors: {result.errors}"

    def test_s3_source_via_duckdb_adapter(self):
        """S3 source compiled with DuckDB engine uses adapter."""
        catalogs = [
            Catalog(name="s3_data", type="s3", options={"bucket": "test-bucket"}),
        ]
        engine = ComputeEngine(name="ddb", engine_type="duckdb")
        self.registry.register_compute_engine(engine)

        joints = [
            Joint(name="src", joint_type="source", catalog="s3_data"),
        ]
        result = _compile_pipeline(self.registry, joints, catalogs, [engine])
        assert result.success, f"Errors: {result.errors}"
        compiled_src = result.joints[0]
        assert compiled_src.adapter == "duckdb:s3"


# ── Test: Engine instance change triggers materialization ────────────────────


class TestEngineInstanceChangeMaterialization:
    """Verify materialization is triggered when engine instance changes between joints."""

    def setup_method(self):
        self.registry = _multi_plugin_registry()

    def test_two_duckdb_engines_trigger_materialization(self):
        """Different engine instances of the same type trigger materialization."""
        catalogs = [Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})]
        engine1 = ComputeEngine(name="ddb1", engine_type="duckdb")
        engine2 = ComputeEngine(name="ddb2", engine_type="duckdb")
        self.registry.register_compute_engine(engine1)
        self.registry.register_compute_engine(engine2)

        joints = [
            Joint(name="src", joint_type="source", catalog="mydb", engine="ddb1", sql="SELECT 1 AS x"),
            Joint(name="sink", joint_type="sink", catalog="mydb", upstream=["src"], engine="ddb2", table="out", write_strategy="replace"),
        ]
        result = _compile_pipeline(self.registry, joints, catalogs, [engine1, engine2])
        assert result.success, f"Errors: {result.errors}"
        mat_triggers = [m.trigger for m in result.materializations]
        assert "engine_instance_change" in mat_triggers

    def test_same_engine_instance_no_extra_materialization(self):
        """Same engine instance does not trigger engine_instance_change materialization."""
        catalogs = [Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})]
        engine = ComputeEngine(name="ddb", engine_type="duckdb")
        self.registry.register_compute_engine(engine)

        joints = [
            Joint(name="src", joint_type="source", catalog="mydb", sql="SELECT 1 AS x"),
            Joint(name="transform", joint_type="sql", upstream=["src"], sql="SELECT x FROM src"),
            Joint(name="sink", joint_type="sink", catalog="mydb", upstream=["transform"], table="out", write_strategy="replace"),
        ]
        result = _compile_pipeline(self.registry, joints, catalogs, [engine])
        assert result.success, f"Errors: {result.errors}"
        engine_change_mats = [m for m in result.materializations if m.trigger == "engine_instance_change"]
        assert len(engine_change_mats) == 0


# ── Test: Adapter capability resolution ──────────────────────────────────────


class TestAdapterCapabilityResolution:
    """Verify capability resolution across plugin boundaries."""

    def setup_method(self):
        self.registry = _multi_plugin_registry()

    def test_duckdb_native_capabilities(self):
        caps = self.registry.resolve_capabilities("duckdb", "duckdb")
        assert caps is not None
        assert set(caps) == {
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
        }

    def test_duckdb_s3_adapter_capabilities(self):
        caps = self.registry.resolve_capabilities("duckdb", "s3")
        assert caps is not None
        assert "projection_pushdown" in caps

    def test_duckdb_postgres_adapter_capabilities_include_cast_pushdown(self):
        """PostgresDuckDBAdapter provides all 6 capabilities including cast_pushdown."""
        caps = self.registry.resolve_capabilities("duckdb", "postgres")
        assert caps is not None
        assert "cast_pushdown" in caps
        assert set(caps) == {
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
        }

    def test_polars_native_capabilities(self):
        caps = self.registry.resolve_capabilities("polars", "arrow")
        assert caps is not None
        assert "join" in caps

    def test_pyspark_native_capabilities(self):
        caps = self.registry.resolve_capabilities("pyspark", "arrow")
        assert caps is not None
        assert "aggregation" in caps

    def test_no_capabilities_for_unsupported_pair(self):
        """Engine that doesn't support a catalog type returns None."""
        caps = self.registry.resolve_capabilities("postgres", "s3")
        assert caps is None

    def test_databricks_native_capabilities(self):
        caps = self.registry.resolve_capabilities("databricks", "databricks")
        assert caps is not None
        assert set(caps) == {
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
        }

    def test_duckdb_databricks_adapter_capabilities(self):
        """Adapter for (duckdb, databricks) exists in rivet_databricks."""
        caps = self.registry.resolve_capabilities("duckdb", "databricks")
        assert caps is not None


# ── Test: MaterializedRef contract ───────────────────────────────────────────


class TestMaterializedRefContract:
    """Verify MaterializedRef .to_arrow() works across plugin boundaries."""

    def test_duckdb_source_produces_valid_arrow(self):
        """DuckDB source MaterializedRef produces a valid pyarrow.Table."""
        from rivet_duckdb.source import DuckDBDeferredMaterializedRef

        ref = DuckDBDeferredMaterializedRef(path=":memory:", read_only=False, sql="SELECT 1 AS x, 'hello' AS y")
        table = ref.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 1
        assert "x" in table.column_names
        assert "y" in table.column_names

    def test_duckdb_source_material_to_arrow(self):
        """Material from DuckDB source supports .to_arrow()."""
        from rivet_duckdb.source import DuckDBSource

        source = DuckDBSource()
        catalog = Catalog(name="test", type="duckdb", options={"path": ":memory:"})
        joint = Joint(name="src", joint_type="source", catalog="test", sql="SELECT 42 AS val")
        material = source.read(catalog, joint, None)
        assert isinstance(material, Material)
        table = material.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 1
        assert table.column("val")[0].as_py() == 42

    def test_duckdb_sink_consumes_arrow_material(self):
        """DuckDB sink can consume a Material backed by Arrow data."""
        from rivet_core.strategies import ArrowMaterialization, MaterializationContext
        from rivet_duckdb.sink import DuckDBSink

        arrow_table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        mat_strategy = ArrowMaterialization()
        ref = mat_strategy.materialize(arrow_table, MaterializationContext(
            joint_name="test", strategy_name="arrow", options={}
        ))
        material = Material(name="test", catalog="mydb", materialized_ref=ref, state="materialized")

        sink = DuckDBSink()
        catalog = Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})
        joint = Joint(name="sink", joint_type="sink", catalog="mydb", upstream=["test"], table="output", write_strategy="replace")
        # Should not raise
        sink.write(catalog, joint, material, "replace")

    def test_cross_plugin_arrow_roundtrip(self):
        """Data flows: DuckDB source → Arrow materialization → DuckDB sink."""
        from rivet_core.strategies import ArrowMaterialization, MaterializationContext
        from rivet_duckdb.sink import DuckDBSink
        from rivet_duckdb.source import DuckDBSource

        # Source produces data
        source = DuckDBSource()
        src_catalog = Catalog(name="src_db", type="duckdb", options={"path": ":memory:"})
        src_joint = Joint(name="src", joint_type="source", catalog="src_db", sql="SELECT 1 AS id, 'test' AS val")
        material = source.read(src_catalog, src_joint, None)

        # Materialize to Arrow (simulating engine boundary)
        arrow_data = material.to_arrow()
        mat_strategy = ArrowMaterialization()
        ref = mat_strategy.materialize(arrow_data, MaterializationContext(
            joint_name="src", strategy_name="arrow", options={}
        ))
        materialized = Material(name="src", catalog="src_db", materialized_ref=ref, state="materialized")

        # Sink consumes materialized data
        sink = DuckDBSink()
        sink_catalog = Catalog(name="sink_db", type="duckdb", options={"path": ":memory:"})
        sink_joint = Joint(name="sink", joint_type="sink", catalog="sink_db", upstream=["src"], table="output", write_strategy="replace")
        sink.write(sink_catalog, sink_joint, materialized, "replace")


# ── Test: Write strategy validation across plugins ───────────────────────────


class TestCrossPluginWriteStrategyValidation:
    """Verify write strategy declarations are consistent across plugins."""

    def test_duckdb_sink_supports_all_8_strategies(self):
        from rivet_duckdb.sink import DuckDBSink
        sink = DuckDBSink()
        expected = {"append", "replace", "truncate_insert", "merge", "delete_insert", "incremental_append", "scd2", "partition"}
        assert sink.supported_strategies == expected

    def test_postgres_sink_supports_all_8_strategies(self):
        from rivet_postgres.sink import PostgresSink
        sink = PostgresSink()
        expected = {"append", "replace", "truncate_insert", "merge", "delete_insert", "incremental_append", "scd2", "partition"}
        assert sink.supported_strategies == expected

    def test_filesystem_sink_supports_limited_strategies(self):
        from rivet_duckdb.filesystem_sink import FilesystemSink
        sink = FilesystemSink()
        expected = {"append", "replace", "partition"}
        assert sink.supported_strategies == expected

    def test_databricks_sink_supports_all_8_strategies(self):
        from rivet_databricks.databricks_sink import DatabricksSink
        sink = DatabricksSink()
        expected = {"append", "replace", "truncate_insert", "merge", "delete_insert", "incremental_append", "scd2", "partition"}
        assert sink.supported_strategies == expected

    def test_duckdb_sink_rejects_unsupported_strategy(self):
        from rivet_core.errors import ExecutionError
        from rivet_core.strategies import ArrowMaterialization, MaterializationContext
        from rivet_duckdb.sink import DuckDBSink

        arrow_table = pa.table({"x": [1]})
        ref = ArrowMaterialization().materialize(arrow_table, MaterializationContext(
            joint_name="t", strategy_name="arrow", options={}
        ))
        material = Material(name="t", catalog="db", materialized_ref=ref, state="materialized")
        sink = DuckDBSink()
        catalog = Catalog(name="db", type="duckdb", options={"path": ":memory:"})
        joint = Joint(name="sink", joint_type="sink", catalog="db", upstream=["t"], table="out")
        with pytest.raises(ExecutionError):
            sink.write(catalog, joint, material, "nonexistent_strategy")


# ── Test: Multi-plugin pipeline with adapter precedence ──────────────────────


class TestAdapterPrecedenceInPipeline:
    """Verify adapter precedence works correctly in compiled pipelines."""

    def setup_method(self):
        self.registry = _multi_plugin_registry()

    def test_postgres_catalog_uses_override_adapter_with_duckdb_engine(self):
        """When DuckDB engine accesses postgres catalog, the catalog-plugin adapter is used."""
        catalogs = [
            Catalog(name="pg", type="postgres", options={"host": "localhost", "database": "test", "user": "u", "password": "p"}),
        ]
        engine = ComputeEngine(name="ddb", engine_type="duckdb")
        self.registry.register_compute_engine(engine)

        joints = [
            Joint(name="src", joint_type="source", catalog="pg", sql="SELECT 1"),
        ]
        result = _compile_pipeline(self.registry, joints, catalogs, [engine])
        assert result.success, f"Errors: {result.errors}"
        compiled_src = result.joints[0]
        assert compiled_src.adapter == "duckdb:postgres"

        # Verify the adapter in registry is the catalog_plugin one
        adapter = self.registry.get_adapter("duckdb", "postgres")
        assert adapter.source == "catalog_plugin"

    def test_s3_catalog_uses_engine_plugin_adapter_with_duckdb(self):
        """S3 adapter from DuckDB plugin (engine_plugin source) is used."""
        adapter = self.registry.get_adapter("duckdb", "s3")
        assert adapter is not None
        assert adapter.source == "engine_plugin"

    def test_s3_catalog_uses_polars_adapter_with_polars_engine(self):
        """Polars engine uses its own S3 adapter."""
        adapter = self.registry.get_adapter("polars", "s3")
        assert adapter is not None
        assert adapter.target_engine_type == "polars"
        assert adapter.catalog_type == "s3"


# ── Test: Multi-consumer materialization ─────────────────────────────────────


class TestMultiConsumerMaterialization:
    """Verify materialization when a joint has multiple downstream consumers."""

    def setup_method(self):
        self.registry = _multi_plugin_registry()

    def test_multi_consumer_triggers_materialization(self):
        catalogs = [Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})]
        engine = ComputeEngine(name="ddb", engine_type="duckdb")
        self.registry.register_compute_engine(engine)

        joints = [
            Joint(name="src", joint_type="source", catalog="mydb", sql="SELECT 1 AS x"),
            Joint(name="branch_a", joint_type="sql", upstream=["src"], sql="SELECT x FROM src"),
            Joint(name="branch_b", joint_type="sql", upstream=["src"], sql="SELECT x * 2 AS x FROM src"),
            Joint(name="sink_a", joint_type="sink", catalog="mydb", upstream=["branch_a"], table="out_a", write_strategy="replace"),
            Joint(name="sink_b", joint_type="sink", catalog="mydb", upstream=["branch_b"], table="out_b", write_strategy="replace"),
        ]
        result = _compile_pipeline(self.registry, joints, catalogs, [engine])
        assert result.success, f"Errors: {result.errors}"
        multi_consumer_mats = [m for m in result.materializations if m.trigger == "multi_consumer"]
        assert len(multi_consumer_mats) > 0
        assert any(m.from_joint == "src" for m in multi_consumer_mats)


# ── Test: Compilation error for unsupported engine-catalog pair ──────────────


class TestUnsupportedEngineCatalogPair:
    """Verify compilation errors for unsupported engine-catalog combinations."""

    def setup_method(self):
        self.registry = _multi_plugin_registry()

    def test_postgres_engine_rejects_s3_catalog(self):
        """PostgreSQL engine cannot access S3 catalog (no adapter)."""
        catalogs = [
            Catalog(name="s3_data", type="s3", options={"bucket": "test"}),
        ]
        engine = ComputeEngine(name="pg_eng", engine_type="postgres")
        self.registry.register_compute_engine(engine)

        joints = [
            Joint(name="src", joint_type="source", catalog="s3_data", engine="pg_eng"),
        ]
        result = _compile_pipeline(self.registry, joints, catalogs, [engine])
        assert not result.success
        assert any("RVT-402" in e.code for e in result.errors)
