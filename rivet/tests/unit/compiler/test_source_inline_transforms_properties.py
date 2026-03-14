"""Property tests for source inline transform validation in the compiler.

Tests _validate_source_inline_transforms and _compute_source_transform_schema
using Hypothesis to generate random valid SQL and projections.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import (
    _compute_source_transform_schema,
    _validate_source_inline_transforms,
)
from rivet_core.errors import RivetError
from rivet_core.models import Column, Schema
from rivet_core.sql_parser import LogicalPlan, Predicate, Projection

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

_ARROW_TYPES = st.sampled_from(
    [
        "int32",
        "int64",
        "float64",
        "utf8",
        "bool",
        "timestamp[us]",
        "large_utf8",
        "double",
    ]
)


@st.composite
def simple_column_projection(draw: st.DrawFn) -> Projection:
    """Generate a simple column reference projection (no expression)."""
    col = draw(_COLUMN_NAMES)
    return Projection(expression=col, alias=None, source_columns=[col])


@st.composite
def aliased_column_projection(draw: st.DrawFn) -> Projection:
    """Generate a column reference with an alias (rename)."""
    col = draw(_COLUMN_NAMES)
    alias = draw(st.sampled_from(["renamed", "alias_col", "out_col", "new_name"]))
    return Projection(expression=col, alias=alias, source_columns=[col])


@st.composite
def cast_projection(draw: st.DrawFn) -> tuple[Projection, str]:
    """Generate a CAST expression projection. Returns (projection, target_type)."""
    col = draw(_COLUMN_NAMES)
    target_sql_type = draw(st.sampled_from(["DOUBLE", "INT", "VARCHAR", "BIGINT"]))
    target_arrow_map = {
        "DOUBLE": "double",
        "INT": "int",
        "VARCHAR": "varchar",
        "BIGINT": "bigint",
    }
    expr = f"CAST({col} AS {target_sql_type})"
    alias = draw(st.sampled_from(["casted_col", "typed_col", "converted"]))
    return (
        Projection(expression=expr, alias=alias, source_columns=[col]),
        target_arrow_map[target_sql_type],
    )


@st.composite
def catalog_schema(draw: st.DrawFn) -> Schema:
    """Generate a random catalog schema with 2-8 columns."""
    n = draw(st.integers(min_value=2, max_value=8))
    used_names: set[str] = set()
    columns: list[Column] = []
    for _ in range(n):
        name = draw(_COLUMN_NAMES.filter(lambda x: x not in used_names))
        used_names.add(name)
        col_type = draw(_ARROW_TYPES)
        columns.append(Column(name=name, type=col_type, nullable=True))
    return Schema(columns=columns)


@st.composite
def valid_single_table_logical_plan(draw: st.DrawFn) -> LogicalPlan:
    """Generate a valid single-table LogicalPlan (no joins, CTEs, subqueries)."""
    from rivet_core.sql_parser import (
        Limit,
        Ordering,
        TableReference,
    )

    table_name = draw(st.sampled_from(["orders", "users", "events", "products"]))

    # Random projections (0 = SELECT *)
    n_proj = draw(st.integers(min_value=0, max_value=4))
    if n_proj == 0:
        projections = [Projection(expression="*", alias=None, source_columns=[])]
    else:
        projections = draw(st.lists(simple_column_projection(), min_size=1, max_size=n_proj))

    # Random predicates
    n_pred = draw(st.integers(min_value=0, max_value=2))
    predicates = []
    for _ in range(n_pred):
        col = draw(_COLUMN_NAMES)
        predicates.append(Predicate(expression=f"{col} > 0", columns=[col], location="where"))

    # Optional LIMIT
    has_limit = draw(st.booleans())
    limit = (
        Limit(count=draw(st.integers(min_value=1, max_value=1000)), offset=None)
        if has_limit
        else None
    )

    # Optional ORDER BY
    has_order = draw(st.booleans())
    ordering = None
    if has_order:
        col = draw(_COLUMN_NAMES)
        direction = draw(st.sampled_from(["asc", "desc"]))
        ordering = Ordering(columns=[(col, direction)])

    # Optional DISTINCT
    distinct = draw(st.booleans())

    return LogicalPlan(
        projections=projections,
        predicates=predicates,
        joins=[],  # No joins — single table
        aggregations=None,
        limit=limit,
        ordering=ordering,
        distinct=distinct,
        source_tables=[
            TableReference(name=table_name, schema=None, alias=None, source_type="from")
        ],
    )


# ---------------------------------------------------------------------------
# Property 9: Valid single-table SELECTs are accepted
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 9: Valid single-table SELECTs are accepted
@given(lp=valid_single_table_logical_plan())
@settings(max_examples=100)
def test_valid_single_table_selects_accepted(lp: LogicalPlan) -> None:
    """Valid single-table SELECTs produce no single-table constraint errors."""
    errors: list[RivetError] = []
    warnings: list[str] = []

    _validate_source_inline_transforms(
        "test_source",
        lp,
        None,
        errors,
        warnings,
    )

    # No RVT-760, RVT-761, or RVT-762 errors
    constraint_errors = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
    assert constraint_errors == [], (
        f"Valid single-table SELECT produced constraint errors: {constraint_errors}"
    )


# ---------------------------------------------------------------------------
# Property 10: Transformed output schema matches projected columns
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 10: Transformed output schema matches projected columns
@given(schema=catalog_schema())
@settings(max_examples=100)
def test_transformed_schema_matches_simple_projections(schema: Schema) -> None:
    """When projecting a subset of columns, output schema has exactly those columns."""
    if len(schema.columns) < 2:
        return  # Need at least 2 columns to project a subset

    # Pick a random subset of columns to project
    import random

    cols = list(schema.columns)
    n = random.randint(1, len(cols))
    projected_cols = random.sample(cols, n)

    projections = [
        Projection(expression=c.name, alias=None, source_columns=[c.name]) for c in projected_cols
    ]

    warnings: list[str] = []
    result = _compute_source_transform_schema("test_source", projections, schema, warnings)

    assert result is not None
    assert len(result.columns) == len(projected_cols)
    for i, proj_col in enumerate(projected_cols):
        assert result.columns[i].name == proj_col.name
        assert result.columns[i].type == proj_col.type


# Feature: source-inline-transforms, Property 10: Transformed output schema matches projected columns
@given(schema=catalog_schema())
@settings(max_examples=100)
def test_transformed_schema_aliases_use_alias_names(schema: Schema) -> None:
    """When projecting with aliases, output schema uses alias names with source types."""
    if not schema.columns:
        return

    col = schema.columns[0]
    alias = "renamed_col"
    projections = [Projection(expression=col.name, alias=alias, source_columns=[col.name])]

    warnings: list[str] = []
    result = _compute_source_transform_schema("test_source", projections, schema, warnings)

    assert result is not None
    assert len(result.columns) == 1
    assert result.columns[0].name == alias
    assert result.columns[0].type == col.type


# Feature: source-inline-transforms, Property 10: Transformed output schema matches projected columns
@given(schema=catalog_schema())
@settings(max_examples=100)
def test_transformed_schema_cast_uses_target_type(schema: Schema) -> None:
    """When projecting with CAST, output schema uses the target type."""
    if not schema.columns:
        return

    col = schema.columns[0]
    projections = [
        Projection(
            expression=f"CAST({col.name} AS DOUBLE)",
            alias="casted",
            source_columns=[col.name],
        )
    ]

    warnings: list[str] = []
    result = _compute_source_transform_schema("test_source", projections, schema, warnings)

    assert result is not None
    assert len(result.columns) == 1
    assert result.columns[0].name == "casted"
    assert result.columns[0].type == "double"


# Feature: source-inline-transforms, Property 10: Transformed output schema matches projected columns
@given(lp=valid_single_table_logical_plan(), schema=catalog_schema())
@settings(max_examples=100)
def test_select_star_preserves_catalog_schema(lp: LogicalPlan, schema: Schema) -> None:
    """SELECT * returns the catalog schema unchanged."""
    star_plan = LogicalPlan(
        projections=[Projection(expression="*", alias=None, source_columns=[])],
        predicates=lp.predicates,
        joins=[],
        aggregations=None,
        limit=lp.limit,
        ordering=lp.ordering,
        distinct=lp.distinct,
        source_tables=lp.source_tables,
    )

    errors: list[RivetError] = []
    warnings: list[str] = []
    result = _validate_source_inline_transforms(
        "test_source",
        star_plan,
        schema,
        errors,
        warnings,
    )

    assert result is schema  # Same object — no transformation
