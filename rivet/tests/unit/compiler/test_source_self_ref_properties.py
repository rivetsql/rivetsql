"""Bug condition exploration: Source self-reference creates circular CTE.

Property 1 (Fault Condition): When a source joint has inline SQL transforms
(columns, filter, limit), SQLGenerator emits FROM {joint.name}. When the
optimizer fuses this source into a CTE group with a downstream transform,
the resulting SQL contains a circular reference:
    WITH raw_orders AS (SELECT ... FROM raw_orders ...)

These tests MUST FAIL on unfixed code — failure confirms the bug exists.
They encode the expected (correct) behavior and will pass after the fix.
"""

from __future__ import annotations

import re
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_bridge.sql_gen import SQLGenerator
from rivet_config import ColumnDecl, JointDeclaration
from rivet_core.optimizer import FusionJoint, _compose_cte, fusion_pass

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_JOINT_NAMES = st.sampled_from(
    ["raw_orders", "events", "users", "transactions", "products", "sessions"]
)

_TABLE_FQNS = st.sampled_from(
    [
        "myschema.raw_orders",
        "analytics.events",
        "public.users",
        "warehouse.transactions",
        "catalog.products",
        "tracking.sessions",
    ]
)

_FILTER_EXPRS = st.sampled_from(
    [
        "status = 'active'",
        "amount > 0",
        "is_deleted = false",
        "created_at > '2024-01-01'",
        "country = 'US'",
    ]
)

_COLUMN_NAMES = st.sampled_from(
    ["id", "name", "amount", "status", "created_at", "email", "price", "quantity"]
)


@st.composite
def column_decl_list(draw: st.DrawFn) -> list[ColumnDecl]:
    """Generate a list of 1-4 unique column declarations."""
    n = draw(st.integers(min_value=1, max_value=4))
    used: set[str] = set()
    cols: list[ColumnDecl] = []
    for _ in range(n):
        name = draw(_COLUMN_NAMES.filter(lambda x: x not in used))
        used.add(name)
        cols.append(ColumnDecl(name=name, expression=None))
    return cols


@st.composite
def source_decl_with_filter(draw: st.DrawFn) -> tuple[JointDeclaration, str]:
    """Source declaration with a filter and a distinct table FQN."""
    name = draw(_JOINT_NAMES)
    table = draw(_TABLE_FQNS)
    filt = draw(_FILTER_EXPRS)
    decl = JointDeclaration(
        name=name,
        joint_type="source",
        source_path=Path("sources") / f"{name}.yaml",
        table=table,
        filter=filt,
    )
    return decl, table


@st.composite
def source_decl_with_columns(draw: st.DrawFn) -> tuple[JointDeclaration, str]:
    """Source declaration with column projections and a distinct table FQN."""
    name = draw(_JOINT_NAMES)
    table = draw(_TABLE_FQNS)
    cols = draw(column_decl_list())
    decl = JointDeclaration(
        name=name,
        joint_type="source",
        source_path=Path("sources") / f"{name}.yaml",
        table=table,
        columns=cols,
    )
    return decl, table


# ---------------------------------------------------------------------------
# Property 1a: SQLGenerator emits FROM __self for source joints
# ---------------------------------------------------------------------------


class TestSQLGeneratorEmitsSelf:
    """SQLGenerator.generate() should produce FROM __self for source joints."""

    @given(data=source_decl_with_filter())
    @settings(max_examples=30)
    def test_source_filter_uses_self_ref(self, data: tuple[JointDeclaration, str]) -> None:
        decl, _table = data
        gen = SQLGenerator()
        sql, errors = gen.generate(decl, joint_names=set())
        assert not errors, f"Generation errors: {errors}"
        # The generated SQL should use FROM __self, not FROM {joint.name}
        assert "__self" in sql, f"Expected FROM __self in generated SQL, got: {sql}"
        assert f"FROM {decl.name}" not in sql.upper().replace(
            f"FROM {decl.name}".upper(), f"FROM {decl.name}"
        ), f"Generated SQL still references joint name '{decl.name}': {sql}"

    @given(data=source_decl_with_columns())
    @settings(max_examples=30)
    def test_source_columns_uses_self_ref(self, data: tuple[JointDeclaration, str]) -> None:
        decl, _table = data
        gen = SQLGenerator()
        sql, errors = gen.generate(decl, joint_names=set())
        assert not errors, f"Generation errors: {errors}"
        assert "__self" in sql, f"Expected FROM __self in generated SQL, got: {sql}"


# ---------------------------------------------------------------------------
# Property 1b: Fused CTE has no circular self-reference
# ---------------------------------------------------------------------------

_CIRCULAR_CTE_RE = re.compile(
    r"WITH\s+(\w+)\s+AS\s*\(\s*SELECT\s+[^)]*?\bFROM\s+\1\b",
    re.IGNORECASE | re.DOTALL,
)


class TestFusedCTENoCircularRef:
    """When a source with inline SQL is fused with a downstream transform,
    the composed CTE must NOT contain a circular reference."""

    @given(data=source_decl_with_filter())
    @settings(max_examples=30)
    def test_fused_cte_filter_no_circular(self, data: tuple[JointDeclaration, str]) -> None:
        decl, table = data
        gen = SQLGenerator()
        source_sql, errors = gen.generate(decl, joint_names=set())
        assert not errors

        # Simulate fusion: source joint + downstream transform
        transform_sql = f"SELECT *, 1 AS flag FROM {decl.name}"
        result = _compose_cte(
            group_joints=[decl.name, "transform_1"],
            joint_sql={decl.name: source_sql, "transform_1": transform_sql},
        )
        assert result is not None
        fused = result.fused_sql

        # The fused CTE must NOT have a circular reference
        assert not _CIRCULAR_CTE_RE.search(fused), f"Circular CTE detected in fused SQL:\n{fused}"

    @given(data=source_decl_with_columns())
    @settings(max_examples=30)
    def test_fused_cte_columns_no_circular(self, data: tuple[JointDeclaration, str]) -> None:
        decl, table = data
        gen = SQLGenerator()
        source_sql, errors = gen.generate(decl, joint_names=set())
        assert not errors

        transform_sql = f"SELECT *, 1 AS flag FROM {decl.name}"
        result = _compose_cte(
            group_joints=[decl.name, "transform_1"],
            joint_sql={decl.name: source_sql, "transform_1": transform_sql},
        )
        assert result is not None
        fused = result.fused_sql

        assert not _CIRCULAR_CTE_RE.search(fused), f"Circular CTE detected in fused SQL:\n{fused}"


# ---------------------------------------------------------------------------
# Property 1c: Full fusion_pass produces no circular CTE for source + transform
# ---------------------------------------------------------------------------


class TestFusionPassNoCircularCTE:
    """End-to-end: fusion_pass with a source joint and downstream transform
    should not produce a circular CTE."""

    @given(data=source_decl_with_filter())
    @settings(max_examples=20)
    def test_fusion_pass_filter_no_circular(self, data: tuple[JointDeclaration, str]) -> None:
        decl, table = data
        gen = SQLGenerator()
        source_sql, errors = gen.generate(decl, joint_names=set())
        assert not errors

        transform_sql = f"SELECT *, 1 AS flag FROM {decl.name}"

        joints = [
            FusionJoint(
                name=decl.name,
                joint_type="source",
                upstream=[],
                engine="eng1",
                engine_type="duckdb",
                sql=source_sql,
            ),
            FusionJoint(
                name="transform_1",
                joint_type="sql",
                upstream=[decl.name],
                engine="eng1",
                engine_type="duckdb",
                sql=transform_sql,
            ),
        ]

        groups = fusion_pass(joints, fusion_strategy="cte")
        for group in groups:
            if group.fused_sql:
                assert not _CIRCULAR_CTE_RE.search(group.fused_sql), (
                    f"Circular CTE in fused group {group.id}:\n{group.fused_sql}"
                )
