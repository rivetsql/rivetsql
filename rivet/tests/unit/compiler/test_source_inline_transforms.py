"""Unit tests for source inline transform validation in the compiler.

Tests _validate_source_inline_transforms, _compute_source_transform_schema,
and _warn_unresolved_column_refs with specific examples and edge cases.
"""

from __future__ import annotations

from rivet_core.compiler import (
    _compute_source_transform_schema,
    _validate_source_inline_transforms,
)
from rivet_core.errors import RivetError
from rivet_core.models import Column, Schema
from rivet_core.sql_parser import (
    Join,
    Limit,
    LogicalPlan,
    Ordering,
    Predicate,
    Projection,
    TableReference,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _schema(*cols: tuple[str, str]) -> Schema:
    return Schema(columns=[Column(name=n, type=t, nullable=True) for n, t in cols])


def _plan(
    projections: list[Projection] | None = None,
    predicates: list[Predicate] | None = None,
    joins: list[Join] | None = None,
    source_tables: list[TableReference] | None = None,
    limit: Limit | None = None,
    ordering: Ordering | None = None,
    distinct: bool = False,
) -> LogicalPlan:
    return LogicalPlan(
        projections=projections or [],
        predicates=predicates or [],
        joins=joins or [],
        aggregations=None,
        limit=limit,
        ordering=ordering,
        distinct=distinct,
        source_tables=source_tables
        or [TableReference(name="orders", schema=None, alias=None, source_type="from")],
    )


# ===================================================================
# 9.1 — Single-table constraint violations
# ===================================================================


class TestSingleTableConstraintJoin:
    """RVT-760: JOINs are rejected."""

    def test_join_produces_rvt_760(self) -> None:
        lp = _plan(
            joins=[
                Join(
                    type="inner",
                    left_table="orders",
                    right_table="customers",
                    condition="orders.id = customers.order_id",
                    columns=[],
                ),
            ]
        )
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [])
        codes = [e.code for e in errors]
        assert "RVT-760" in codes

    def test_join_error_mentions_joint_name(self) -> None:
        lp = _plan(
            joins=[
                Join(
                    type="left",
                    left_table="a",
                    right_table="b",
                    condition="a.id = b.id",
                    columns=[],
                ),
            ]
        )
        errors: list[RivetError] = []
        _validate_source_inline_transforms("my_source", lp, None, errors, [])
        assert any("my_source" in e.message for e in errors if e.code == "RVT-760")


class TestSingleTableConstraintCTE:
    """RVT-761: CTEs are rejected (requires sql kwarg for AST re-parse)."""

    def test_cte_produces_rvt_761(self) -> None:
        sql = "WITH cte AS (SELECT id FROM orders) SELECT * FROM cte"
        # The LogicalPlan won't have joins, but the SQL has a CTE.
        lp = _plan()
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        codes = [e.code for e in errors]
        assert "RVT-761" in codes

    def test_cte_error_mentions_joint_name(self) -> None:
        sql = "WITH x AS (SELECT 1) SELECT * FROM x"
        lp = _plan()
        errors: list[RivetError] = []
        _validate_source_inline_transforms("my_cte_src", lp, None, errors, [], sql=sql)
        assert any("my_cte_src" in e.message for e in errors if e.code == "RVT-761")


class TestSingleTableConstraintSubquery:
    """RVT-762: Subqueries in WHERE are rejected."""

    def test_subquery_produces_rvt_762(self) -> None:
        sql = "SELECT * FROM orders WHERE id IN (SELECT order_id FROM returns)"
        lp = _plan()
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        codes = [e.code for e in errors]
        assert "RVT-762" in codes

    def test_subquery_error_mentions_joint_name(self) -> None:
        sql = "SELECT * FROM t WHERE x = (SELECT MAX(x) FROM t2)"
        lp = _plan()
        errors: list[RivetError] = []
        _validate_source_inline_transforms("sub_src", lp, None, errors, [], sql=sql)
        assert any("sub_src" in e.message for e in errors if e.code == "RVT-762")


class TestValidSingleTableSelects:
    """Valid single-table SELECTs produce no constraint errors."""

    def test_simple_select_star(self) -> None:
        sql = "SELECT * FROM orders"
        lp = _plan(projections=[Projection(expression="*", alias=None, source_columns=[])])
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        constraint = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
        assert constraint == []

    def test_select_with_where(self) -> None:
        sql = "SELECT id, name FROM orders WHERE status = 'active'"
        lp = _plan(
            projections=[
                Projection(expression="id", alias=None, source_columns=["id"]),
                Projection(expression="name", alias=None, source_columns=["name"]),
            ],
            predicates=[
                Predicate(expression="status = 'active'", columns=["status"], location="where")
            ],
        )
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        constraint = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
        assert constraint == []

    def test_select_with_order_by(self) -> None:
        sql = "SELECT id FROM orders ORDER BY id DESC"
        lp = _plan(
            projections=[Projection(expression="id", alias=None, source_columns=["id"])],
            ordering=Ordering(columns=[("id", "desc")]),
        )
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        constraint = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
        assert constraint == []

    def test_select_with_limit(self) -> None:
        sql = "SELECT * FROM orders LIMIT 100"
        lp = _plan(
            projections=[Projection(expression="*", alias=None, source_columns=[])],
            limit=Limit(count=100, offset=None),
        )
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        constraint = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
        assert constraint == []

    def test_select_with_distinct(self) -> None:
        sql = "SELECT DISTINCT country FROM orders"
        lp = _plan(
            projections=[Projection(expression="country", alias=None, source_columns=["country"])],
            distinct=True,
        )
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        constraint = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
        assert constraint == []

    def test_select_with_projections_where_order_limit_distinct(self) -> None:
        sql = "SELECT DISTINCT id, name FROM orders WHERE id > 0 ORDER BY name LIMIT 50"
        lp = _plan(
            projections=[
                Projection(expression="id", alias=None, source_columns=["id"]),
                Projection(expression="name", alias=None, source_columns=["name"]),
            ],
            predicates=[Predicate(expression="id > 0", columns=["id"], location="where")],
            ordering=Ordering(columns=[("name", "asc")]),
            limit=Limit(count=50, offset=None),
            distinct=True,
        )
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        constraint = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
        assert constraint == []


class TestNoSqlNoColumnsFilter:
    """Source with no SQL and no columns/filter → full table read, no errors."""

    def test_none_logical_plan_returns_original_schema(self) -> None:
        schema = _schema(("id", "int64"), ("name", "utf8"))
        errors: list[RivetError] = []
        result = _validate_source_inline_transforms("src", None, schema, errors, [])
        assert result is schema
        assert errors == []

    def test_none_logical_plan_none_schema(self) -> None:
        errors: list[RivetError] = []
        result = _validate_source_inline_transforms("src", None, None, errors, [])
        assert result is None
        assert errors == []


# ===================================================================
# 9.3 — Invalid filter/column expression syntax
# ===================================================================
# Note: RVT-764 and RVT-765 are not yet implemented in the compiler.
# The current implementation relies on SQL parsing failure being non-fatal
# (best-effort). These tests document the current behavior: when SQL
# parsing fails, no LogicalPlan is produced, and _validate_source_inline_transforms
# receives None, returning the original schema without errors.


# ===================================================================
# 9.4 — Column reference warnings with introspection
# ===================================================================


class TestFilterColumnReferenceWarnings:
    """Warnings for filter references to unknown columns."""

    def test_filter_unknown_column_warns(self) -> None:
        schema = _schema(("id", "int64"), ("name", "utf8"))
        lp = _plan(
            predicates=[Predicate(expression="bogus > 0", columns=["bogus"], location="where")],
        )
        warnings: list[str] = []
        _validate_source_inline_transforms("src", lp, schema, [], warnings)
        assert any("bogus" in w and "filter" in w for w in warnings)

    def test_filter_known_column_no_warning(self) -> None:
        schema = _schema(("id", "int64"), ("name", "utf8"))
        lp = _plan(
            predicates=[Predicate(expression="id > 0", columns=["id"], location="where")],
        )
        warnings: list[str] = []
        _validate_source_inline_transforms("src", lp, schema, [], warnings)
        filter_warnings = [w for w in warnings if "filter" in w]
        assert filter_warnings == []

    def test_filter_multiple_unknown_columns_warns_each(self) -> None:
        schema = _schema(("id", "int64"))
        lp = _plan(
            predicates=[
                Predicate(expression="foo > 0", columns=["foo"], location="where"),
                Predicate(expression="bar = 1", columns=["bar"], location="where"),
            ],
        )
        warnings: list[str] = []
        _validate_source_inline_transforms("src", lp, schema, [], warnings)
        assert any("foo" in w for w in warnings)
        assert any("bar" in w for w in warnings)


class TestColumnExpressionReferenceWarnings:
    """Warnings for column expression references to unknown columns."""

    def test_projection_unknown_source_column_warns(self) -> None:
        schema = _schema(("id", "int64"))
        lp = _plan(
            projections=[
                Projection(expression="missing_col", alias="out", source_columns=["missing_col"]),
            ],
        )
        warnings: list[str] = []
        _validate_source_inline_transforms("src", lp, schema, [], warnings)
        assert any("missing_col" in w for w in warnings)

    def test_projection_known_source_column_no_warning(self) -> None:
        schema = _schema(("id", "int64"), ("price", "float64"))
        lp = _plan(
            projections=[
                Projection(expression="price", alias="cost", source_columns=["price"]),
            ],
        )
        warnings: list[str] = []
        _validate_source_inline_transforms("src", lp, schema, [], warnings)
        ref_warnings = [w for w in warnings if "expression references" in w]
        assert ref_warnings == []


class TestCannotInferTypeWarning:
    """Warning when expression type cannot be inferred."""

    def test_computed_expression_without_catalog_warns(self) -> None:
        """No catalog schema → cannot infer type → warning for each column."""
        lp = _plan(
            projections=[
                Projection(expression="a + b", alias="total", source_columns=["a", "b"]),
            ],
        )
        warnings: list[str] = []
        _validate_source_inline_transforms("src", lp, None, [], warnings)
        assert any("cannot infer output type" in w for w in warnings)

    def test_computed_expression_with_catalog_warns_for_complex_expr(self) -> None:
        """Complex expression that isn't a simple column ref or CAST → warning."""
        schema = _schema(("a", "int64"), ("b", "int64"))
        lp = _plan(
            projections=[
                Projection(expression="a + b", alias="total", source_columns=["a", "b"]),
            ],
        )
        warnings: list[str] = []
        _validate_source_inline_transforms("src", lp, schema, [], warnings)
        assert any("cannot infer output type" in w for w in warnings)

    def test_no_warnings_without_introspection(self) -> None:
        """No introspected schema → no column reference warnings (only type warnings)."""
        lp = _plan(
            predicates=[Predicate(expression="bogus > 0", columns=["bogus"], location="where")],
            projections=[Projection(expression="*", alias=None, source_columns=[])],
        )
        warnings: list[str] = []
        _validate_source_inline_transforms("src", lp, None, [], warnings)
        ref_warnings = [w for w in warnings if "not found in catalog schema" in w]
        assert ref_warnings == []


# ===================================================================
# 9.5 — Schema propagation
# ===================================================================


class TestSchemaExplicitProjections:
    """Output schema has projected columns only."""

    def test_subset_projection(self) -> None:
        schema = _schema(("id", "int64"), ("name", "utf8"), ("email", "utf8"))
        lp = _plan(
            projections=[
                Projection(expression="id", alias=None, source_columns=["id"]),
                Projection(expression="name", alias=None, source_columns=["name"]),
            ]
        )
        errors: list[RivetError] = []
        result = _validate_source_inline_transforms("src", lp, schema, errors, [])
        assert result is not None
        assert len(result.columns) == 2
        assert result.columns[0].name == "id"
        assert result.columns[1].name == "name"

    def test_single_column_projection(self) -> None:
        schema = _schema(("id", "int64"), ("name", "utf8"), ("email", "utf8"))
        lp = _plan(
            projections=[
                Projection(expression="email", alias=None, source_columns=["email"]),
            ]
        )
        result = _validate_source_inline_transforms("src", lp, schema, [], [])
        assert result is not None
        assert len(result.columns) == 1
        assert result.columns[0].name == "email"
        assert result.columns[0].type == "utf8"


class TestSchemaAliasedColumns:
    """Output schema uses alias names."""

    def test_rename_uses_alias(self) -> None:
        schema = _schema(("id", "int64"), ("name", "utf8"))
        lp = _plan(
            projections=[
                Projection(expression="id", alias="order_id", source_columns=["id"]),
            ]
        )
        result = _validate_source_inline_transforms("src", lp, schema, [], [])
        assert result is not None
        assert result.columns[0].name == "order_id"
        assert result.columns[0].type == "int64"

    def test_multiple_renames(self) -> None:
        schema = _schema(("id", "int64"), ("name", "utf8"))
        lp = _plan(
            projections=[
                Projection(expression="id", alias="oid", source_columns=["id"]),
                Projection(expression="name", alias="label", source_columns=["name"]),
            ]
        )
        result = _validate_source_inline_transforms("src", lp, schema, [], [])
        assert result is not None
        assert [c.name for c in result.columns] == ["oid", "label"]


class TestSchemaCastExpressions:
    """Output schema has target types for CAST expressions."""

    def test_cast_double(self) -> None:
        schema = _schema(("amount", "utf8"))
        lp = _plan(
            projections=[
                Projection(
                    expression="CAST(amount AS DOUBLE)", alias="amount_f", source_columns=["amount"]
                ),
            ]
        )
        result = _validate_source_inline_transforms("src", lp, schema, [], [])
        assert result is not None
        assert result.columns[0].name == "amount_f"
        assert result.columns[0].type == "double"

    def test_cast_int(self) -> None:
        schema = _schema(("score", "utf8"))
        lp = _plan(
            projections=[
                Projection(
                    expression="CAST(score AS INT)", alias="score_i", source_columns=["score"]
                ),
            ]
        )
        result = _validate_source_inline_transforms("src", lp, schema, [], [])
        assert result is not None
        assert result.columns[0].name == "score_i"


class TestSchemaSelectStar:
    """SELECT * → output schema is full catalog schema."""

    def test_select_star_returns_catalog_schema(self) -> None:
        schema = _schema(("id", "int64"), ("name", "utf8"), ("email", "utf8"))
        lp = _plan(projections=[Projection(expression="*", alias=None, source_columns=[])])
        result = _validate_source_inline_transforms("src", lp, schema, [], [])
        assert result is schema

    def test_select_star_no_catalog_returns_none_schema(self) -> None:
        """SELECT * with no catalog schema → returns None (no transform)."""
        lp = _plan(projections=[Projection(expression="*", alias=None, source_columns=[])])
        result = _validate_source_inline_transforms("src", lp, None, [], [])
        assert result is None


class TestSchemaComputeSourceTransformDirect:
    """Direct tests of _compute_source_transform_schema."""

    def test_empty_projections_returns_none(self) -> None:
        schema = _schema(("id", "int64"))
        result = _compute_source_transform_schema("src", [], schema, [])
        assert result is None

    def test_star_returns_none(self) -> None:
        schema = _schema(("id", "int64"))
        result = _compute_source_transform_schema(
            "src",
            [Projection(expression="*", alias=None, source_columns=[])],
            schema,
            [],
        )
        assert result is None

    def test_no_catalog_schema_uses_large_binary_fallback(self) -> None:
        warnings: list[str] = []
        result = _compute_source_transform_schema(
            "src",
            [Projection(expression="id", alias=None, source_columns=["id"])],
            None,
            warnings,
        )
        assert result is not None
        assert result.columns[0].type == "large_binary"
        assert any("cannot infer" in w for w in warnings)

    def test_mixed_projections_schema(self) -> None:
        """Mix of simple ref, rename, and CAST in one schema."""
        schema = _schema(("id", "int64"), ("name", "utf8"), ("raw_amount", "utf8"))
        projections = [
            Projection(expression="id", alias=None, source_columns=["id"]),
            Projection(expression="name", alias="label", source_columns=["name"]),
            Projection(
                expression="CAST(raw_amount AS DOUBLE)",
                alias="amount",
                source_columns=["raw_amount"],
            ),
        ]
        warnings: list[str] = []
        result = _compute_source_transform_schema("src", projections, schema, warnings)
        assert result is not None
        assert len(result.columns) == 3
        assert result.columns[0].name == "id"
        assert result.columns[0].type == "int64"
        assert result.columns[1].name == "label"
        assert result.columns[1].type == "utf8"
        assert result.columns[2].name == "amount"
        assert result.columns[2].type == "double"
