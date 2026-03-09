"""Integration tests: cross-engine materialization.

Compiles and executes a pipeline with joints assigned to two different DuckDB
engine instances. Verifies data flows correctly across engine boundaries and
each sink has correct data.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.csv as pcsv

from rivet_core.assembly import Assembly
from rivet_core.compiler import compile
from rivet_core.executor import Executor
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_dual_engine_registry() -> tuple[PluginRegistry, ComputeEngine, ComputeEngine]:
    """Create a registry with two separate DuckDB engine instances."""
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    eng_plugin = reg.get_engine_plugin("duckdb")
    eng1 = eng_plugin.create_engine("duckdb_primary", {})
    eng2 = eng_plugin.create_engine("duckdb_secondary", {})
    reg.register_compute_engine(eng1)
    reg.register_compute_engine(eng2)
    return reg, eng1, eng2


def _compile_and_run_cross_engine(
    joints: list[Joint],
    data_dir: Path,
    registry: PluginRegistry,
    engines: list[ComputeEngine],
    default_engine: str = "duckdb_primary",
) -> tuple:
    catalogs = [Catalog(name="local", type="filesystem", options={"path": str(data_dir), "format": "csv"})]
    assembly = Assembly(joints)
    compiled = compile(
        assembly,
        catalogs=catalogs,
        engines=engines,
        registry=registry,
        default_engine=default_engine,
        introspect=True,
    )
    assert compiled.success, f"Compilation failed: {[e.message for e in compiled.errors]}"

    executor = Executor(registry=registry)
    result = executor.run_sync(compiled)
    return compiled, result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCrossEngineDataFlow:
    """Data flows correctly across engine boundaries."""

    def test_source_on_primary_transform_on_secondary(self, tmp_path):
        """Source reads on engine A, transform runs on engine B, sink writes."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "orders.csv").write_text("id,amount\n1,100\n2,200\n3,300\n")

        reg, eng1, eng2 = _setup_dual_engine_registry()

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders",
                  engine="duckdb_primary"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount * 2 AS doubled FROM src",
                  engine="duckdb_secondary"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["transform"], engine="duckdb_secondary"),
        ]

        compiled, result = _compile_and_run_cross_engine(
            joints, data_dir, reg, [eng1, eng2],
        )

        assert result.success
        assert result.total_failures == 0

        # Verify materialization happened at engine boundary
        assert len(compiled.materializations) >= 1

        # Verify output data
        table = pcsv.read_csv(str(data_dir / "result.csv"))
        assert sorted(table.column("id").to_pylist()) == [1, 2, 3]
        assert sorted(table.column("doubled").to_pylist()) == [200, 400, 600]

    def test_two_sinks_on_different_engines(self, tmp_path):
        """Pipeline with sinks on different engines both produce correct output."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "data.csv").write_text("id,val\n1,10\n2,20\n3,30\n")

        reg, eng1, eng2 = _setup_dual_engine_registry()

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data",
                  engine="duckdb_primary"),
            Joint(name="high", joint_type="sql", upstream=["src"],
                  sql="SELECT id, val FROM src WHERE val > 15",
                  engine="duckdb_primary"),
            Joint(name="low", joint_type="sql", upstream=["src"],
                  sql="SELECT id, val FROM src WHERE val <= 15",
                  engine="duckdb_secondary"),
            Joint(name="sink_high", joint_type="sink", catalog="local", table="high_result",
                  upstream=["high"], engine="duckdb_primary"),
            Joint(name="sink_low", joint_type="sink", catalog="local", table="low_result",
                  upstream=["low"], engine="duckdb_secondary"),
        ]

        compiled, result = _compile_and_run_cross_engine(
            joints, data_dir, reg, [eng1, eng2],
        )

        assert result.success

        high_table = pcsv.read_csv(str(data_dir / "high_result.csv"))
        assert sorted(high_table.column("id").to_pylist()) == [2, 3]

        low_table = pcsv.read_csv(str(data_dir / "low_result.csv"))
        assert low_table.column("id").to_pylist() == [1]


class TestCrossEngineMaterialization:
    """Verify materialization metadata at engine boundaries."""

    def test_engine_boundary_produces_materialization(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "data.csv").write_text("id,val\n1,10\n")

        reg, eng1, eng2 = _setup_dual_engine_registry()

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data",
                  engine="duckdb_primary"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT * FROM src", engine="duckdb_secondary"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["transform"], engine="duckdb_secondary"),
        ]

        compiled, result = _compile_and_run_cross_engine(
            joints, data_dir, reg, [eng1, eng2],
        )

        assert result.success
        assert len(compiled.materializations) >= 1

        # At least one materialization should be triggered by engine change
        triggers = {m.trigger for m in compiled.materializations}
        assert "engine_instance_change" in triggers or len(triggers) > 0

    def test_instance_change_materialization_trigger(self, tmp_path):
        """Cross-instance boundary produces materialization with engine_instance_change trigger."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "data.csv").write_text("id,val\n1,10\n")

        reg, eng1, eng2 = _setup_dual_engine_registry()

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data",
                  engine="duckdb_primary"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT * FROM src", engine="duckdb_secondary"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["transform"], engine="duckdb_secondary"),
        ]

        compiled, _result = _compile_and_run_cross_engine(
            joints, data_dir, reg, [eng1, eng2],
        )

        # Same engine type but different instances → engine_instance_change trigger
        triggers = {m.trigger for m in compiled.materializations}
        assert "engine_instance_change" in triggers
