"""Tests for lineage traversal (task 17.1)."""

from __future__ import annotations

from rivet_core.compiler import CompiledAssembly, CompiledJoint
from rivet_core.lineage import (
    ColumnLineage,
    ColumnOrigin,
    trace_column_backward,
    trace_column_forward,
)


def _joint(
    name: str,
    upstream: list[str] | None = None,
    lineage: list[ColumnLineage] | None = None,
    fused_group_id: str | None = None,
) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type="sql",
        catalog=None,
        catalog_type=None,
        engine="arrow",
        engine_resolution="project_default",
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=upstream or [],
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=lineage or [],
        optimizations=[],
        checks=[],
        fused_group_id=fused_group_id,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _assembly(joints: list[CompiledJoint]) -> CompiledAssembly:
    return CompiledAssembly(
        success=True,
        profile_name="test",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=joints,
        fused_groups=[],
        materializations=[],
        execution_order=[],
        errors=[],
        warnings=[],
    )


class TestTraceColumnBackward:
    def test_single_source_joint(self) -> None:
        """Source joint with no origins is the terminal."""
        j = _joint("src", lineage=[
            ColumnLineage("id", "source", [], None),
        ])
        compiled = _assembly([j])
        result = trace_column_backward(compiled, "src", "id")
        assert result == [ColumnOrigin(joint="src", column="id")]

    def test_direct_chain(self) -> None:
        """A -> B: B.col traces back to A.col."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
        ])
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("x", "direct", [ColumnOrigin("A", "x")], None),
        ])
        compiled = _assembly([a, b])
        result = trace_column_backward(compiled, "B", "x")
        assert result == [ColumnOrigin(joint="A", column="x")]

    def test_multi_hop_chain(self) -> None:
        """A -> B -> C: C.col traces back through B to A."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
        ])
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("y", "renamed", [ColumnOrigin("A", "x")], None),
        ])
        c = _joint("C", upstream=["B"], lineage=[
            ColumnLineage("z", "renamed", [ColumnOrigin("B", "y")], None),
        ])
        compiled = _assembly([a, b, c])
        result = trace_column_backward(compiled, "C", "z")
        assert result == [ColumnOrigin(joint="A", column="x")]

    def test_multi_origin_expression(self) -> None:
        """Expression with multiple source columns traces to all origins."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
            ColumnLineage("y", "source", [], None),
        ])
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("sum", "expression",
                          [ColumnOrigin("A", "x"), ColumnOrigin("A", "y")],
                          "x + y"),
        ])
        compiled = _assembly([a, b])
        result = trace_column_backward(compiled, "B", "sum")
        origins = {(o.joint, o.column) for o in result}
        assert origins == {("A", "x"), ("A", "y")}

    def test_column_not_in_lineage(self) -> None:
        """Column with no lineage info is treated as terminal origin."""
        a = _joint("A", lineage=[])
        compiled = _assembly([a])
        result = trace_column_backward(compiled, "A", "unknown")
        assert result == [ColumnOrigin(joint="A", column="unknown")]

    def test_through_fused_group(self) -> None:
        """Per-joint lineage within fused groups is navigable."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
        ], fused_group_id="g1")
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("x", "direct", [ColumnOrigin("A", "x")], None),
        ], fused_group_id="g1")
        compiled = _assembly([a, b])
        result = trace_column_backward(compiled, "B", "x")
        assert result == [ColumnOrigin(joint="A", column="x")]

    def test_literal_is_terminal(self) -> None:
        """Literal columns have no origins and are terminal."""
        a = _joint("A", lineage=[
            ColumnLineage("lit", "literal", [], None),
        ])
        compiled = _assembly([a])
        result = trace_column_backward(compiled, "A", "lit")
        assert result == [ColumnOrigin(joint="A", column="lit")]


class TestTraceColumnForward:
    def test_single_downstream(self) -> None:
        """A.x -> B.x: forward from A.x finds B.x."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
        ])
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("x", "direct", [ColumnOrigin("A", "x")], None),
        ])
        compiled = _assembly([a, b])
        result = trace_column_forward(compiled, "A", "x")
        assert ColumnOrigin(joint="B", column="x") in result

    def test_multi_hop_forward(self) -> None:
        """A.x -> B.y -> C.z: forward from A.x finds both B.y and C.z."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
        ])
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("y", "renamed", [ColumnOrigin("A", "x")], None),
        ])
        c = _joint("C", upstream=["B"], lineage=[
            ColumnLineage("z", "renamed", [ColumnOrigin("B", "y")], None),
        ])
        compiled = _assembly([a, b, c])
        result = trace_column_forward(compiled, "A", "x")
        result_set = {(o.joint, o.column) for o in result}
        assert ("B", "y") in result_set
        assert ("C", "z") in result_set

    def test_fan_out(self) -> None:
        """A.x used by both B and C."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
        ])
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("b_col", "direct", [ColumnOrigin("A", "x")], None),
        ])
        c = _joint("C", upstream=["A"], lineage=[
            ColumnLineage("c_col", "direct", [ColumnOrigin("A", "x")], None),
        ])
        compiled = _assembly([a, b, c])
        result = trace_column_forward(compiled, "A", "x")
        result_set = {(o.joint, o.column) for o in result}
        assert ("B", "b_col") in result_set
        assert ("C", "c_col") in result_set

    def test_no_downstream(self) -> None:
        """Leaf joint with no downstream returns empty."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
        ])
        compiled = _assembly([a])
        result = trace_column_forward(compiled, "A", "x")
        assert result == []

    def test_through_fused_group(self) -> None:
        """Forward traversal works through fused groups."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
        ], fused_group_id="g1")
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("x", "direct", [ColumnOrigin("A", "x")], None),
        ], fused_group_id="g1")
        compiled = _assembly([a, b])
        result = trace_column_forward(compiled, "A", "x")
        assert ColumnOrigin(joint="B", column="x") in result

    def test_unaffected_column_not_included(self) -> None:
        """Downstream columns not depending on the source are excluded."""
        a = _joint("A", lineage=[
            ColumnLineage("x", "source", [], None),
            ColumnLineage("y", "source", [], None),
        ])
        b = _joint("B", upstream=["A"], lineage=[
            ColumnLineage("bx", "direct", [ColumnOrigin("A", "x")], None),
            ColumnLineage("by", "direct", [ColumnOrigin("A", "y")], None),
        ])
        compiled = _assembly([a, b])
        result = trace_column_forward(compiled, "A", "x")
        result_set = {(o.joint, o.column) for o in result}
        assert ("B", "bx") in result_set
        assert ("B", "by") not in result_set
