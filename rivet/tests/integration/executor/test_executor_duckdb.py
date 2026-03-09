"""Integration tests: executor + real DuckDB engine.

Compiles a plan, executes it through the real DuckDB engine plugin, and
verifies output data. Exercises the full executor → engine plugin → DuckDB
path with real components — no mocks.
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


def _setup_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg


def _compile_and_run(
    joints: list[Joint],
    data_dir: Path,
    *,
    default_engine: str = "duckdb_primary",
    engines: list[ComputeEngine] | None = None,
    registry: PluginRegistry | None = None,
) -> tuple:
    """Compile and execute a pipeline, returning (compiled, exec_result)."""
    if registry is None:
        registry = _setup_registry()
    catalogs = [Catalog(name="local", type="filesystem", options={"path": str(data_dir), "format": "csv"})]
    if engines is None:
        eng = registry.get_engine_plugin("duckdb").create_engine("duckdb_primary", {})
        engines = [eng]
    for e in engines:
        if e.name not in {ce.name for ce in registry._compute_engines.values()}:
            registry.register_compute_engine(e)

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


class TestSimplePipeline:
    """Source → filter → sink through real DuckDB."""

    def test_filter_produces_correct_rows(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "orders.csv").write_text("id,amount\n1,100\n2,-50\n3,75\n4,0\n5,200\n")

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount FROM src WHERE amount > 0"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["transform"]),
        ]

        _compiled, result = _compile_and_run(joints, data_dir)

        assert result.success
        assert result.status == "success"
        assert result.total_failures == 0

        # Verify sink output
        sink_csv = data_dir / "result.csv"
        assert sink_csv.exists()
        table = pcsv.read_csv(str(sink_csv))
        assert sorted(table.column("id").to_pylist()) == [1, 3, 5]
        assert sorted(table.column("amount").to_pylist()) == [75, 100, 200]

    def test_aggregation_produces_correct_result(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "sales.csv").write_text("region,amount\nnorth,100\nsouth,200\nnorth,150\nsouth,50\n")

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="sales"),
            Joint(name="agg", joint_type="sql", upstream=["src"],
                  sql="SELECT region, SUM(amount) AS total FROM src GROUP BY region"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["agg"]),
        ]

        _compiled, result = _compile_and_run(joints, data_dir)

        assert result.success
        table = pcsv.read_csv(str(data_dir / "result.csv"))
        regions = table.column("region").to_pylist()
        totals = table.column("total").to_pylist()
        row_map = dict(zip(regions, totals))
        assert row_map["north"] == 250
        assert row_map["south"] == 250


class TestMultiStepPipeline:
    """Source → transform → transform → sink (chained SQL)."""

    def test_chained_transforms_produce_correct_output(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "items.csv").write_text("id,price,qty\n1,10,5\n2,20,3\n3,5,10\n")

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="items"),
            Joint(name="with_total", joint_type="sql", upstream=["src"],
                  sql="SELECT id, price * qty AS total FROM src"),
            Joint(name="filtered", joint_type="sql", upstream=["with_total"],
                  sql="SELECT id, total FROM with_total WHERE total > 50"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["filtered"]),
        ]

        _compiled, result = _compile_and_run(joints, data_dir)

        assert result.success
        table = pcsv.read_csv(str(data_dir / "result.csv"))
        # price*qty: 50, 60, 50 → only id=2 (60) passes > 50
        assert table.column("id").to_pylist() == [2]
        assert table.column("total").to_pylist() == [60]


class TestJoinPipeline:
    """Two sources joined together through DuckDB."""

    def test_join_produces_merged_output(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "customers.csv").write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")
        (data_dir / "orders.csv").write_text("customer_id,amount\n1,100\n2,200\n1,50\n")

        joints = [
            Joint(name="customers", joint_type="source", catalog="local", table="customers"),
            Joint(name="orders", joint_type="source", catalog="local", table="orders"),
            Joint(name="joined", joint_type="sql", upstream=["customers", "orders"],
                  sql="SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["joined"]),
        ]

        _compiled, result = _compile_and_run(joints, data_dir)

        assert result.success
        table = pcsv.read_csv(str(data_dir / "result.csv"))
        assert len(table) == 3
        names = sorted(table.column("name").to_pylist())
        assert names == ["Alice", "Alice", "Bob"]


class TestExecutionResult:
    """Verify ExecutionResult metadata is populated correctly."""

    def test_joint_results_populated(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "data.csv").write_text("id,val\n1,10\n2,20\n")

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["src"]),
        ]

        _compiled, result = _compile_and_run(joints, data_dir)

        assert result.success
        joint_names = {jr.name for jr in result.joint_results}
        assert "src" in joint_names
        assert "sink" in joint_names

    def test_group_results_populated(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "data.csv").write_text("id,val\n1,10\n2,20\n")

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(name="sink", joint_type="sink", catalog="local", table="result",
                  upstream=["src"]),
        ]

        _compiled, result = _compile_and_run(joints, data_dir)

        assert result.success
        assert len(result.group_results) >= 1
        for gr in result.group_results:
            assert gr.success
