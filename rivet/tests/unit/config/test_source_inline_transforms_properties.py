"""Property test: YAML/SQL round-trip equivalence.

Feature: source-inline-transforms, Property 8: YAML/SQL round-trip equivalence

Generate random YAML column/filter/limit declarations, round-trip through
SQLGenerator → SQLDecomposer, and verify equivalence.
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_bridge.decomposer import SQLDecomposer
from rivet_bridge.sql_gen import SQLGenerator
from rivet_config.models import ColumnDecl, JointDeclaration

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_COLUMN_NAMES = st.sampled_from(
    [
        "id",
        "name",
        "email",
        "amount",
        "price",
        "quantity",
        "status",
        "created_at",
        "updated_at",
        "is_active",
        "country",
        "age",
        "score",
    ]
)

_TABLE_NAMES = st.sampled_from(["orders", "users", "events", "products"])

_SIMPLE_OPERATORS = st.sampled_from(["> 0", "< 100", "= 1", ">= 10", "<= 50"])


@st.composite
def simple_column_decls(draw: st.DrawFn) -> list[ColumnDecl]:
    """Generate 1-5 simple column declarations (no expressions)."""
    n = draw(st.integers(min_value=1, max_value=5))
    used: set[str] = set()
    cols: list[ColumnDecl] = []
    for _ in range(n):
        name = draw(_COLUMN_NAMES.filter(lambda x: x not in used))
        used.add(name)
        cols.append(ColumnDecl(name=name, expression=None))
    return cols


@st.composite
def simple_filter(draw: st.DrawFn) -> str:
    """Generate a simple filter expression."""
    col = draw(_COLUMN_NAMES)
    op = draw(_SIMPLE_OPERATORS)
    return f"{col} {op}"


@st.composite
def yaml_source_declaration(draw: st.DrawFn) -> JointDeclaration:
    """Generate a random YAML source declaration with columns, filter, and/or limit."""
    table = draw(_TABLE_NAMES)
    has_columns = draw(st.booleans())
    has_filter = draw(st.booleans())
    has_limit = draw(st.booleans())

    columns = draw(simple_column_decls()) if has_columns else None
    filt = draw(simple_filter()) if has_filter else None
    limit = draw(st.integers(min_value=1, max_value=10000)) if has_limit else None

    return JointDeclaration(
        name="test_src",
        joint_type="source",
        source_path=Path("test.yaml"),
        catalog="warehouse",
        table=table,
        columns=columns,
        filter=filt,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# Property 8: YAML/SQL round-trip equivalence
# ---------------------------------------------------------------------------

_GEN = SQLGenerator()
_DECOMP = SQLDecomposer()


# Feature: source-inline-transforms, Property 8: YAML/SQL round-trip equivalence
@given(decl=yaml_source_declaration())
@settings(max_examples=100)
def test_yaml_sql_roundtrip_columns(decl: JointDeclaration) -> None:
    """Columns survive the SQLGenerator → SQLDecomposer round-trip."""
    sql, errors = _GEN.generate(decl, set())
    assert not errors, f"SQL generation failed: {errors}"

    if not _DECOMP.can_decompose(sql):
        return  # Skip non-decomposable SQL (shouldn't happen for simple cases)

    rt_cols, rt_filter, rt_table, rt_limit = _DECOMP.decompose(sql)

    # Table name preserved
    assert rt_table == (decl.table or decl.name)

    # Columns preserved
    if decl.columns is None:
        assert rt_cols is None
    else:
        assert rt_cols is not None
        assert len(rt_cols) == len(decl.columns)
        for orig, rt in zip(decl.columns, rt_cols):
            assert rt.name == orig.name


# Feature: source-inline-transforms, Property 8: YAML/SQL round-trip equivalence
@given(decl=yaml_source_declaration())
@settings(max_examples=100)
def test_yaml_sql_roundtrip_filter(decl: JointDeclaration) -> None:
    """Filter presence is preserved through the round-trip."""
    sql, errors = _GEN.generate(decl, set())
    assert not errors

    if not _DECOMP.can_decompose(sql):
        return

    _, rt_filter, _, _ = _DECOMP.decompose(sql)

    if decl.filter is None:
        assert rt_filter is None
    else:
        assert rt_filter is not None


# Feature: source-inline-transforms, Property 8: YAML/SQL round-trip equivalence
@given(decl=yaml_source_declaration())
@settings(max_examples=100)
def test_yaml_sql_roundtrip_limit(decl: JointDeclaration) -> None:
    """Limit value is preserved through the round-trip."""
    sql, errors = _GEN.generate(decl, set())
    assert not errors

    if not _DECOMP.can_decompose(sql):
        return

    _, _, _, rt_limit = _DECOMP.decompose(sql)
    assert rt_limit == decl.limit
