"""Property tests for downstream checkpoint resolution via _read_sources_into.

Covers Properties 5, 6 from the checkpoint-source-resolution design document.

- Property 5: Downstream adapter-based resolution for checkpoint DeferredRefs
- Property 6: Source plugin fallback when no adapter is available

Writes data to a DuckDB filesystem catalog via SinkPlugin, constructs a
DeferredRef, builds a FusedGroup with checkpoint_sources, and invokes
_read_sources_into to verify resolution through adapter and fallback paths.
"""

from __future__ import annotations

import asyncio
import tempfile
from typing import Any

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.builtins.filesystem_catalog import FilesystemSource
from rivet_core.compiler import CompiledCatalog, CompiledJoint
from rivet_core.executor import Executor
from rivet_core.models import Catalog, ComputeEngine, Joint, Material
from rivet_core.optimizer import (
    AdapterPushdownResult,
    CheckpointSourceInfo,
    FusedGroup,
    PushdownPlan,
    ResidualPlan,
)
from rivet_core.plugins import (
    ComputeEngineAdapter,
    ComputeEnginePlugin,
    PluginRegistry,
)
from rivet_core.strategies import DeferredRef, MaterializedRef, _ArrowMaterializedRef
from rivet_duckdb.filesystem_sink import FilesystemSink

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def _arrow_table(draw: st.DrawFn) -> pa.Table:
    """Generate an Arrow table with a random mix of int, float, and string columns."""
    n_int = draw(st.integers(min_value=0, max_value=3))
    n_float = draw(st.integers(min_value=0, max_value=3))
    n_str = draw(st.integers(min_value=0, max_value=3))

    total = n_int + n_float + n_str
    if total == 0:
        n_int = 1

    n_rows = draw(st.integers(min_value=1, max_value=50))

    columns: dict[str, Any] = {}
    col_idx = 0

    for _ in range(n_int):
        name = f"col_int_{col_idx}"
        values = draw(
            st.lists(
                st.integers(min_value=-(2**31), max_value=2**31 - 1),
                min_size=n_rows,
                max_size=n_rows,
            )
        )
        columns[name] = pa.array(values, type=pa.int64())
        col_idx += 1

    for _ in range(n_float):
        name = f"col_float_{col_idx}"
        values = draw(
            st.lists(
                st.floats(allow_nan=False, allow_infinity=False, min_value=-1e10, max_value=1e10),
                min_size=n_rows,
                max_size=n_rows,
            )
        )
        columns[name] = pa.array(values, type=pa.float64())
        col_idx += 1

    for _ in range(n_str):
        name = f"col_str_{col_idx}"
        values = draw(
            st.lists(
                st.text(
                    min_size=0,
                    max_size=20,
                    alphabet=st.characters(whitelist_categories=("L", "N", "Z")),
                ),
                min_size=n_rows,
                max_size=n_rows,
            )
        )
        columns[name] = pa.array(values, type=pa.utf8())
        col_idx += 1

    return pa.table(columns)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _write_to_filesystem(table: pa.Table, tmpdir: str, table_name: str = "cp_test") -> None:
    """Write an Arrow table to a filesystem catalog via SinkPlugin."""
    catalog = Catalog(
        name="local",
        type="filesystem",
        options={"path": tmpdir, "format": "parquet"},
    )
    joint = Joint(
        name=table_name,
        joint_type="checkpoint",
        catalog="local",
        table=table_name,
        upstream=["fake_upstream"],
    )
    material = Material(
        name=table_name,
        catalog="local",
        state="materialized",
        materialized_ref=_ArrowMaterializedRef(table),
    )
    sink = FilesystemSink()
    sink.write(catalog, joint, material, "replace")


class _FilesystemReadAdapter(ComputeEngineAdapter):
    """Test adapter that reads from filesystem catalogs via FilesystemSource.

    Simulates what a real adapter would do: read data from the catalog
    and return it as an AdapterPushdownResult.
    """

    target_engine_type = "duckdb"
    catalog_type = "filesystem"
    capabilities: list[str] = []
    source = "engine_plugin"

    def read_dispatch(
        self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None
    ) -> AdapterPushdownResult:
        fs_source = FilesystemSource()
        mat = fs_source.read(catalog, joint, None)
        return AdapterPushdownResult(
            material=mat,
            residual=ResidualPlan(),
        )

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        return None


class _StubEnginePlugin(ComputeEnginePlugin):
    """Minimal engine plugin for test registry setup."""

    engine_type = "duckdb"
    dialect = "duckdb"
    supported_catalog_types: dict[str, list[str]] = {"filesystem": []}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type="duckdb")

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: str, input_tables: dict[str, Any] | None = None) -> Any:
        return pa.table({})

    def collect_metrics(self, context: dict[str, Any]) -> Any:
        return None


def _make_registry(*, with_adapter: bool = True) -> PluginRegistry:
    """Create a PluginRegistry with filesystem source and optionally an adapter."""
    reg = PluginRegistry()
    reg.register_source(FilesystemSource())
    plugin = _StubEnginePlugin()
    reg.register_engine_plugin(plugin)
    engine = plugin.create_engine("default", {})
    reg.register_compute_engine(engine)
    if with_adapter:
        reg.register_adapter(_FilesystemReadAdapter())
    return reg


def _make_checkpoint_compiled_joint(table_name: str = "cp_test") -> CompiledJoint:
    """Create a minimal CompiledJoint for a checkpoint."""
    return CompiledJoint(
        name=table_name,
        type="checkpoint",
        catalog="local",
        catalog_type="filesystem",
        engine="default",
        engine_resolution="project_default",
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=["fake_upstream"],
        eager=False,
        table=table_name,
        write_strategy="replace",
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id="downstream_group",
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_downstream_group(
    checkpoint_name: str = "cp_test",
    *,
    with_adapter: bool = True,
) -> FusedGroup:
    """Create a FusedGroup that consumes a checkpoint upstream."""
    adapter_name = "duckdb:filesystem" if with_adapter else None
    return FusedGroup(
        id="downstream_group",
        joints=["downstream_sql"],
        engine="default",
        engine_type="duckdb",
        adapters={},
        fused_sql=f"SELECT * FROM {checkpoint_name}",
        entry_joints=["downstream_sql"],
        exit_joints=["downstream_sql"],
        checkpoint_sources={
            checkpoint_name: CheckpointSourceInfo(
                checkpoint_joint=checkpoint_name,
                catalog="local",
                catalog_type="filesystem",
                table=checkpoint_name,
                adapter=adapter_name,
            ),
        },
    )


def _make_downstream_compiled_joint(checkpoint_name: str = "cp_test") -> CompiledJoint:
    """Create a minimal CompiledJoint for the downstream SQL joint."""
    return CompiledJoint(
        name="downstream_sql",
        type="sql",
        catalog=None,
        catalog_type=None,
        engine="default",
        engine_resolution="project_default",
        adapter=None,
        sql=f"SELECT * FROM {checkpoint_name}",
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[checkpoint_name],
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id="downstream_group",
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


# ---------------------------------------------------------------------------
# Property 5: Downstream adapter-based resolution for checkpoint DeferredRefs
# ---------------------------------------------------------------------------


@given(table=_arrow_table())
@settings(max_examples=30)
def test_property5_downstream_adapter_resolution(table: pa.Table) -> None:
    """When a downstream fused group references an upstream checkpoint and an
    adapter is registered for (downstream_engine_type, checkpoint_catalog_type),
    _read_sources_into resolves the DeferredRef via the adapter and places the
    result in input_tables.

    Validates: Requirements 3.1, 3.2, 5.1, 5.2
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_to_filesystem(table, tmpdir)

        registry = _make_registry(with_adapter=True)
        executor = Executor(registry=registry)

        deferred = DeferredRef(
            catalog_name="local",
            catalog_type="filesystem",
            table_name="cp_test",
            catalog_options={"path": tmpdir, "format": "parquet"},
            registry=registry,
        )

        group = _make_downstream_group(with_adapter=True)
        cp_cj = _make_checkpoint_compiled_joint()
        ds_cj = _make_downstream_compiled_joint()
        joint_map = {cp_cj.name: cp_cj, ds_cj.name: ds_cj}
        catalog_map = {
            "local": CompiledCatalog(
                name="local",
                type="filesystem",
                options={"path": tmpdir, "format": "parquet"},
            ),
        }
        ref_materials: dict[str, MaterializedRef] = {"cp_test": deferred}

        input_tables: dict[str, pa.Table] = {}
        asyncio.run(
            executor._read_sources_into(
                input_tables,
                group,
                joint_map,
                catalog_map,
                ref_materials=ref_materials,
            )
        )

        # Checkpoint was resolved into input_tables
        assert "cp_test" in input_tables
        resolved = input_tables["cp_test"]

        # Data matches what was written
        assert resolved.num_rows == table.num_rows
        assert resolved.column_names == table.column_names
        for col_name in table.column_names:
            assert resolved.column(col_name).to_pylist() == table.column(col_name).to_pylist()


# ---------------------------------------------------------------------------
# Property 6: Source plugin fallback when no adapter is available
# ---------------------------------------------------------------------------


@given(table=_arrow_table())
@settings(max_examples=30)
def test_property6_source_plugin_fallback(table: pa.Table) -> None:
    """When a downstream fused group references an upstream checkpoint and no
    adapter is registered, _read_sources_into falls back to the SourcePlugin
    for the checkpoint's catalog type and places the result in input_tables.

    Validates: Requirements 3.2, 4.2, 5.1, 5.2
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_to_filesystem(table, tmpdir)

        registry = _make_registry(with_adapter=False)
        executor = Executor(registry=registry)

        deferred = DeferredRef(
            catalog_name="local",
            catalog_type="filesystem",
            table_name="cp_test",
            catalog_options={"path": tmpdir, "format": "parquet"},
            registry=registry,
        )

        group = _make_downstream_group(with_adapter=False)
        cp_cj = _make_checkpoint_compiled_joint()
        ds_cj = _make_downstream_compiled_joint()
        joint_map = {cp_cj.name: cp_cj, ds_cj.name: ds_cj}
        catalog_map = {
            "local": CompiledCatalog(
                name="local",
                type="filesystem",
                options={"path": tmpdir, "format": "parquet"},
            ),
        }
        ref_materials: dict[str, MaterializedRef] = {"cp_test": deferred}

        input_tables: dict[str, pa.Table] = {}
        asyncio.run(
            executor._read_sources_into(
                input_tables,
                group,
                joint_map,
                catalog_map,
                ref_materials=ref_materials,
            )
        )

        # Checkpoint was resolved into input_tables via source plugin fallback
        assert "cp_test" in input_tables
        resolved = input_tables["cp_test"]

        # Data matches what was written
        assert resolved.num_rows == table.num_rows
        assert resolved.column_names == table.column_names
        for col_name in table.column_names:
            assert resolved.column(col_name).to_pylist() == table.column(col_name).to_pylist()
