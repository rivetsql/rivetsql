"""Integration tests for sink native SQL write without fused SQL.

Exercises the fix for sink joints that have no fused SQL but have upstream
materialized tables. When the upstream is not fused with the sink (e.g.
eager materialization), the sink ends up in its own group with no fused SQL.
The executor should construct SELECT * FROM {upstream} and use native SQL
write instead of falling back to Arrow.

Requirements: bugfix.md 2.1, 2.3
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from rivet_core.assembly import Assembly
from rivet_core.compiler import compile
from rivet_core.executor import Executor
from rivet_core.models import Catalog, Joint
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin


def _setup_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg


def _compile_and_run(
    joints: list[Joint],
    catalogs: list[Catalog],
    registry: PluginRegistry | None = None,
) -> tuple:
    """Compile and execute a pipeline, returning (compiled, exec_result)."""
    if registry is None:
        registry = _setup_registry()

    eng = registry.get_engine_plugin("duckdb").create_engine("duckdb_primary", {})
    registry.register_compute_engine(eng)

    assembly = Assembly(joints)
    compiled = compile(
        assembly,
        catalogs=catalogs,
        engines=[eng],
        registry=registry,
        default_engine="duckdb_primary",
        introspect=True,
    )
    assert compiled.success, (
        f"Compilation failed: {[e.message for e in compiled.diagnostics.errors]}"
    )

    executor = Executor(registry=registry)
    result = executor.run_sync(compiled)
    return compiled, result


def _make_catalogs(tmp_path: Path) -> tuple[list[Catalog], Path]:
    """Create filesystem source + DuckDB sink catalogs."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "products.csv").write_text("id,name,price\n1,Widget,10\n2,Gadget,25\n3,Gizmo,50\n")

    sink_db_path = tmp_path / "sink.duckdb"
    duckdb.connect(str(sink_db_path)).close()

    catalogs = [
        Catalog(
            name="local",
            type="filesystem",
            options={"path": str(data_dir), "format": "csv"},
        ),
        Catalog(
            name="sink_db",
            type="duckdb",
            options={"path": str(sink_db_path)},
        ),
    ]
    return catalogs, sink_db_path


def _read_sink_table(sink_db_path: Path, table: str):
    """Read a table from the sink DuckDB database."""
    conn = duckdb.connect(str(sink_db_path), read_only=True)
    try:
        result = conn.execute(f"SELECT * FROM {table}").arrow()
        if hasattr(result, "read_all"):
            result = result.read_all()
        return result
    finally:
        conn.close()


class TestSinkNativeSqlWriteWithoutFusedSql:
    """Sink without fused SQL (not fused with upstream) → native SQL write."""

    def test_sink_native_write_when_upstream_is_eager(self, tmp_path: Path) -> None:
        """Sink in own group (due to eager upstream) uses native SQL write.

        The upstream transform has eager=True, which forces materialization
        and prevents fusion with the sink. The sink ends up in its own fused
        group with no fused SQL. The executor should construct
        SELECT * FROM {upstream} and dispatch native SQL write.
        """
        catalogs, sink_db_path = _make_catalogs(tmp_path)

        joints = [
            Joint(name="src_products", joint_type="source", catalog="local", table="products"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src_products"],
                sql="SELECT id, name, price * 2 AS doubled FROM src_products",
                eager=True,
            ),
            Joint(
                name="sink_out",
                joint_type="sink",
                catalog="sink_db",
                table="output",
                upstream=["transform"],
                write_strategy="replace",
            ),
        ]

        _compiled, result = _compile_and_run(joints, catalogs)

        assert result.success, f"Execution failed: {result.status}"

        # Verify native SQL write path was used
        sink_jr = next(jr for jr in result.joint_results if jr.name == "sink_out")
        assert sink_jr.write_path == "native_sql", (
            f"Expected native_sql write path, got {sink_jr.write_path}"
        )

        # Verify data correctness
        table = _read_sink_table(sink_db_path, "output")
        assert table.num_rows == 3
        assert sorted(table.column("id").to_pylist()) == [1, 2, 3]
        assert sorted(table.column("doubled").to_pylist()) == [20.0, 50.0, 100.0]

    def test_sink_fused_with_upstream_uses_native_write(self, tmp_path: Path) -> None:
        """Sink fused with upstream (no barrier) also uses native SQL write.

        Preservation: when the sink fuses with its upstream transform, the
        group has fused SQL and native SQL write should still work.
        """
        catalogs, sink_db_path = _make_catalogs(tmp_path)

        joints = [
            Joint(name="src_products", joint_type="source", catalog="local", table="products"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src_products"],
                sql="SELECT id, name, price * 2 AS doubled FROM src_products",
            ),
            Joint(
                name="sink_out",
                joint_type="sink",
                catalog="sink_db",
                table="output",
                upstream=["transform"],
                write_strategy="replace",
            ),
        ]

        _compiled, result = _compile_and_run(joints, catalogs)

        assert result.success
        sink_jr = next(jr for jr in result.joint_results if jr.name == "sink_out")
        assert sink_jr.write_path == "native_sql"

        table = _read_sink_table(sink_db_path, "output")
        assert table.num_rows == 3
        assert sorted(table.column("doubled").to_pylist()) == [20.0, 50.0, 100.0]
