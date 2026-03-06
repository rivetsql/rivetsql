"""Tests for Mermaid renderer (rendering/mermaid.py)."""

from __future__ import annotations

import re

from rivet_cli.rendering.mermaid import render_mermaid
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
    Materialization,
    OptimizationResult,
)
from rivet_core.optimizer import FusedGroup

ANSI_RE = re.compile(r"\033\[")


def _joint(name: str, type: str = "sql", *, catalog: str | None = None,
           engine: str = "duckdb_1", upstream: list[str] | None = None,
           fused_group_id: str | None = None,
           optimizations: list[OptimizationResult] | None = None) -> CompiledJoint:
    return CompiledJoint(
        name=name, type=type, catalog=catalog, catalog_type=None,
        engine=engine, engine_resolution="project_default", adapter=None,
        sql=None, sql_translated=None, sql_resolved=None, sql_dialect=None,
        engine_dialect=None, upstream=upstream or [], eager=False, table=None,
        write_strategy=None, function=None, source_file=None, logical_plan=None,
        output_schema=None, column_lineage=[], optimizations=optimizations or [],
        checks=[], fused_group_id=fused_group_id, tags=[], description=None,
        fusion_strategy_override=None, materialization_strategy_override=None,
    )


def _assembly(joints, fused_groups=None, materializations=None):
    return CompiledAssembly(
        success=True, profile_name="default",
        catalogs=[CompiledCatalog(name="cat", type="fs")],
        engines=[CompiledEngine(name="duckdb_1", engine_type="duckdb", native_catalog_types=[])],
        adapters=[], joints=joints, fused_groups=fused_groups or [],
        materializations=materializations or [], execution_order=[], errors=[], warnings=[],
    )


class TestRenderMermaid:
    def test_starts_with_graph_td(self):
        out = render_mermaid(_assembly([]))
        assert out.startswith("graph TD")

    def test_contains_subgraph_for_fused_group(self):
        j1 = _joint("a", fused_group_id="g1")
        j2 = _joint("b", upstream=["a"], fused_group_id="g1")
        fg = FusedGroup(id="g1", joints=["a", "b"], engine="duckdb_1", engine_type="duckdb",
                        adapters={"a": None, "b": None}, fused_sql=None)
        out = render_mermaid(_assembly([j1, j2], [fg]))
        assert "subgraph" in out
        assert "duckdb_1" in out
        assert "end" in out

    def test_joints_as_nodes(self):
        j = _joint("my_joint", catalog="cat1")
        out = render_mermaid(_assembly([j]))
        assert "my_joint" in out
        assert "type: sql" in out

    def test_edges_from_upstream(self):
        j1 = _joint("src")
        j2 = _joint("transform", upstream=["src"])
        out = render_mermaid(_assembly([j1, j2]))
        assert "src --> transform" in out

    def test_materialization_edge(self):
        j1 = _joint("a")
        j2 = _joint("b", upstream=["a"])
        mat = Materialization(from_joint="a", to_joint="b", trigger="engine_change",
                              detail="x", strategy="arrow")
        out = render_mermaid(_assembly([j1, j2], materializations=[mat]))
        assert "⚡ materialize" in out

    def test_optimization_count_in_label(self):
        j = _joint("j1", optimizations=[
            OptimizationResult(rule="pushdown", status="applied", detail="ok"),
        ])
        out = render_mermaid(_assembly([j]))
        assert "optimizations: 1" in out

    def test_no_ansi_codes(self):
        """Mermaid output should never contain ANSI codes regardless."""
        j = _joint("j1")
        out = render_mermaid(_assembly([j]))
        assert not ANSI_RE.search(out)
