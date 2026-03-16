"""Property-based tests for sink SQL parsing.

Covers Properties 1–8 from the sink SQL parsing design document.

- Property 1: Sink SQL parsing equivalence with sql-type joints
  Validates: Requirements 1.1, 1.4, 2.1, 2.2, 2.3, 3.1, 7.1, 7.3
- Property 2: Invalid sink SQL produces None LogicalPlan without blocking compilation
  Validates: Requirements 1.2, 1.3, 2.4
- Property 3: SQL-inferred schema takes precedence over upstream-inferred schema
  Validates: Requirements 3.2, 3.3
- Property 4: Sink predicate pushdown to upstream source groups
  Validates: Requirements 4.1, 4.2, 4.3
- Property 5: Sink projection pushdown to upstream source groups
  Validates: Requirements 5.1, 5.2, 5.3, 8.2
- Property 6: Sink limit pushdown to upstream source groups
  Validates: Requirements 6.1, 6.2, 6.3
- Property 7: Consumer-side LogicalPlan is never modified by pushdown
  Validates: Requirements 8.1, 8.3
- Property 8: Every sink pushdown decision produces an OptimizationResult
  Validates: Requirements 9.1, 9.2, 9.3, 9.4
"""

from __future__ import annotations

import copy
import uuid
from dataclasses import replace

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import (
    CompiledJoint,
    _compile_sql_joint,
    _infer_sink_schemas,
)
from rivet_core.errors import RivetError
from rivet_core.lineage import ColumnLineage
from rivet_core.models import Column, Joint, Schema
from rivet_core.optimizer import (
    FusedGroup,
    cross_group_pushdown_pass,
)
from rivet_core.plugins import PluginRegistry
from rivet_core.sql_parser import SQLParser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _schema(*cols: tuple[str, str]) -> Schema:
    return Schema(columns=[Column(name=n, type=t, nullable=True) for n, t in cols])


def _joint(
    name: str,
    joint_type: str,
    sql: str | None = None,
    upstream: list[str] | None = None,
    dialect: str | None = None,
) -> Joint:
    return Joint(
        name=name,
        joint_type=joint_type,
        sql=sql,
        upstream=upstream or [],
        dialect=dialect,
    )


def _compiled_joint_stub(
    name: str,
    joint_type: str = "source",
    output_schema: Schema | None = None,
    upstream: list[str] | None = None,
    logical_plan: object | None = None,
    column_lineage: list[ColumnLineage] | None = None,
) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type=joint_type,
        catalog=None,
        catalog_type=None,
        engine="duckdb_local",
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
        logical_plan=logical_plan,
        output_schema=output_schema,
        column_lineage=column_lineage or [],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _compile_joint_sql(
    joint_type: str,
    sql: str,
    upstream_schemas: dict[str, Schema],
    upstream: list[str] | None = None,
) -> tuple[object, list[ColumnLineage], str | None, str | None, Schema | None, list[RivetError]]:
    """Compile SQL for a joint of the given type and return all outputs."""
    joint = _joint(
        name=f"test_{joint_type}",
        joint_type=joint_type,
        sql=sql,
        upstream=upstream or list(upstream_schemas.keys()),
    )
    parser = SQLParser()
    registry = PluginRegistry()
    errors: list[RivetError] = []
    warnings: list[str] = []
    lp, lineage, sql_translated, engine_dialect, sql_schema = _compile_sql_joint(
        joint,
        "",
        registry,
        parser,
        upstream_schemas,
        errors,
        warnings,
    )
    return lp, lineage, sql_translated, engine_dialect, sql_schema, errors


def _make_group(
    joints: list[str],
    *,
    group_id: str | None = None,
    engine: str = "eng1",
    engine_type: str = "databricks",
    entry_joints: list[str] | None = None,
    exit_joints: list[str] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id or str(uuid.uuid4()),
        joints=joints,
        engine=engine,
        engine_type=engine_type,
        adapters={j: None for j in joints},
        fused_sql=None,
        entry_joints=entry_joints or joints[:1],
        exit_joints=exit_joints or joints[-1:],
    )


_FULL_CAPS: dict[str, list[str]] = {
    "databricks": ["predicate_pushdown", "projection_pushdown", "limit_pushdown"],
}
_EMPTY_CATALOG_TYPES: dict[str, str | None] = {}

# Column name strategy: simple lowercase identifiers, excluding SQL keywords
_SQL_KEYWORDS = frozenset(
    {
        "all",
        "and",
        "any",
        "are",
        "asc",
        "avg",
        "bit",
        "by",
        "day",
        "dec",
        "end",
        "for",
        "from",
        "get",
        "has",
        "int",
        "key",
        "lag",
        "max",
        "min",
        "mod",
        "new",
        "not",
        "now",
        "old",
        "or",
        "out",
        "own",
        "pad",
        "raw",
        "ref",
        "row",
        "set",
        "sql",
        "sum",
        "top",
        "use",
        "var",
        "abs",
        "add",
        "as",
        "at",
        "case",
        "cast",
        "char",
        "cube",
        "data",
        "date",
        "desc",
        "drop",
        "each",
        "else",
        "exec",
        "exit",
        "full",
        "goto",
        "hour",
        "into",
        "join",
        "last",
        "left",
        "like",
        "loop",
        "map",
        "name",
        "next",
        "none",
        "null",
        "only",
        "open",
        "over",
        "path",
        "plan",
        "read",
        "real",
        "role",
        "rows",
        "rule",
        "save",
        "self",
        "size",
        "some",
        "then",
        "time",
        "trim",
        "true",
        "type",
        "user",
        "view",
        "when",
        "with",
        "work",
        "year",
        "zone",
        "both",
        "call",
        "cross",
        "false",
        "fetch",
        "first",
        "float",
        "grant",
        "group",
        "having",
        "inner",
        "input",
        "level",
        "limit",
        "local",
        "match",
        "month",
        "natural",
        "order",
        "outer",
        "prior",
        "range",
        "right",
        "select",
        "space",
        "start",
        "state",
        "table",
        "union",
        "unique",
        "upper",
        "using",
        "value",
        "where",
        "write",
    }
)
_col_name_st = st.from_regex(r"[a-z]{3,8}", fullmatch=True).filter(lambda s: s not in _SQL_KEYWORDS)


# ---------------------------------------------------------------------------
# Property 1: Sink SQL parsing equivalence with sql-type joints
# Feature: sink-sql-parsing
# Validates: Requirements 1.1, 1.4, 2.1, 2.2, 2.3, 3.1, 7.1, 7.3
# ---------------------------------------------------------------------------


@st.composite
def _equivalence_scenario(draw: st.DrawFn) -> dict:
    """Generate a valid SELECT statement with upstream schemas.

    Compiles the same SQL as both a sink and sql joint type, returning both
    results for comparison.
    """
    # Generate 1–4 unique column names
    cols = draw(st.lists(_col_name_st, min_size=1, max_size=4, unique=True))
    col_type = draw(st.sampled_from(["int64", "utf8", "float64"]))
    upstream_name = "upstream_src"
    upstream_schema = _schema(*[(c, col_type) for c in cols])

    # Build a SELECT with a random subset of columns
    num_proj = draw(st.integers(min_value=1, max_value=len(cols)))
    proj_cols = cols[:num_proj]

    # Optionally add a WHERE clause
    add_where = draw(st.booleans())
    where_clause = ""
    if add_where and col_type == "int64":
        threshold = draw(st.integers(min_value=0, max_value=100))
        where_clause = f" WHERE {proj_cols[0]} > {threshold}"

    # Optionally add a LIMIT
    add_limit = draw(st.booleans())
    limit_clause = ""
    if add_limit:
        limit_val = draw(st.integers(min_value=1, max_value=1000))
        limit_clause = f" LIMIT {limit_val}"

    select_list = ", ".join(proj_cols)
    sql = f"SELECT {select_list} FROM {upstream_name}{where_clause}{limit_clause}"

    upstream_schemas = {upstream_name: upstream_schema}

    # Compile as sink
    sink_lp, sink_lineage, sink_translated, sink_dialect, sink_schema, sink_errors = (
        _compile_joint_sql("sink", sql, upstream_schemas)
    )
    # Compile as sql
    sql_lp, sql_lineage, sql_translated, sql_dialect, sql_schema, sql_errors = _compile_joint_sql(
        "sql", sql, upstream_schemas
    )

    return {
        "sql": sql,
        "sink_lp": sink_lp,
        "sink_lineage": sink_lineage,
        "sink_translated": sink_translated,
        "sink_dialect": sink_dialect,
        "sink_schema": sink_schema,
        "sink_errors": sink_errors,
        "sql_lp": sql_lp,
        "sql_lineage": sql_lineage,
        "sql_translated": sql_translated,
        "sql_dialect": sql_dialect,
        "sql_schema": sql_schema,
        "sql_errors": sql_errors,
    }


@given(scenario=_equivalence_scenario())
@settings(max_examples=100)
def test_property1_sink_sql_parsing_equivalence(scenario: dict) -> None:
    """For any valid SELECT, compiling as sink produces the same LogicalPlan,
    lineage, schema, and dialect translation as compiling as sql joint."""
    # Both should parse successfully
    assert scenario["sink_lp"] is not None, f"Sink failed to parse: {scenario['sql']}"
    assert scenario["sql_lp"] is not None, f"SQL failed to parse: {scenario['sql']}"

    sink_lp = scenario["sink_lp"]
    sql_lp = scenario["sql_lp"]

    # LogicalPlan equivalence
    assert sink_lp.projections == sql_lp.projections
    assert sink_lp.predicates == sql_lp.predicates
    assert sink_lp.joins == sql_lp.joins
    assert sink_lp.aggregations == sql_lp.aggregations
    assert sink_lp.limit == sql_lp.limit
    assert sink_lp.ordering == sql_lp.ordering
    assert sink_lp.distinct == sql_lp.distinct
    assert sink_lp.source_tables == sql_lp.source_tables

    # Lineage equivalence (compare output columns and origins)
    sink_lin_map = {l.output_column: l for l in scenario["sink_lineage"]}
    sql_lin_map = {l.output_column: l for l in scenario["sql_lineage"]}
    assert set(sink_lin_map.keys()) == set(sql_lin_map.keys())
    for col in sink_lin_map:
        assert sink_lin_map[col].transform == sql_lin_map[col].transform
        assert set(sink_lin_map[col].origins) == set(sql_lin_map[col].origins)

    # Schema equivalence
    assert scenario["sink_schema"] == scenario["sql_schema"]

    # Dialect translation equivalence
    assert scenario["sink_translated"] == scenario["sql_translated"]
    assert scenario["sink_dialect"] == scenario["sql_dialect"]


# ---------------------------------------------------------------------------
# Property 2: Invalid sink SQL produces None LogicalPlan without blocking compilation
# Feature: sink-sql-parsing
# Validates: Requirements 1.2, 1.3, 2.4
# ---------------------------------------------------------------------------

# Strategy for generating invalid SQL strings — DML/DDL statements and garbage
# that the parser should reject or that _compile_sql_joint should error on.
_invalid_sql_st = st.one_of(
    # Random garbage text (no SELECT prefix)
    st.from_regex(r"[A-Z ]{5,30}", fullmatch=True).filter(
        lambda s: not s.strip().upper().startswith("SELECT")
    ),
    # DML/DDL statements that _compile_sql_joint rejects
    st.sampled_from(
        [
            "INSERT INTO foo VALUES (1)",
            "DROP TABLE foo",
            "CREATE TABLE foo (id INT)",
            "UPDATE foo SET bar = 1",
            "DELETE FROM foo",
            "ALTER TABLE foo ADD COLUMN bar INT",
            "NOT VALID SQL AT ALL",
            ";;;",
            "",
        ]
    ),
)


@given(invalid_sql=_invalid_sql_st)
@settings(max_examples=100)
def test_property2_invalid_sink_sql_produces_none_plan(invalid_sql: str) -> None:
    """For any non-SELECT SQL string, compiling a sink joint produces
    logical_plan=None, empty lineage, and at least one error."""
    lp, lineage, sql_translated, engine_dialect, sql_schema, errors = _compile_joint_sql(
        "sink", invalid_sql, {}
    )
    assert lp is None
    assert lineage == []
    # Errors should be recorded (non-fatal)
    assert len(errors) >= 1


# ---------------------------------------------------------------------------
# Property 3: SQL-inferred schema takes precedence over upstream-inferred schema
# Feature: sink-sql-parsing
# Validates: Requirements 3.2, 3.3
# ---------------------------------------------------------------------------


@st.composite
def _schema_precedence_scenario(draw: st.DrawFn) -> dict:
    """Generate a sink with or without a SQL-inferred schema, plus an upstream
    source with a known schema, to test _infer_sink_schemas precedence."""
    # Generate upstream columns (superset)
    num_cols = draw(st.integers(min_value=2, max_value=5))
    all_cols = draw(
        st.lists(_col_name_st, min_size=num_cols, max_size=num_cols, unique=True),
    )
    col_type = draw(st.sampled_from(["int64", "utf8", "float64"]))
    upstream_schema = _schema(*[(c, col_type) for c in all_cols])

    # Decide whether the sink has a SQL-inferred schema (subset) or None
    has_sql_schema = draw(st.booleans())
    if has_sql_schema:
        num_sql_cols = draw(st.integers(min_value=1, max_value=len(all_cols)))
        sql_cols = all_cols[:num_sql_cols]
        sql_schema: Schema | None = _schema(*[(c, col_type) for c in sql_cols])
    else:
        sql_schema = None

    src_cj = _compiled_joint_stub("src", joint_type="source", output_schema=upstream_schema)
    sink_cj = _compiled_joint_stub(
        "my_sink",
        joint_type="sink",
        output_schema=sql_schema,
        upstream=["src"],
    )

    return {
        "compiled_joints": [src_cj, sink_cj],
        "has_sql_schema": has_sql_schema,
        "sql_schema": sql_schema,
        "upstream_schema": upstream_schema,
    }


@given(scenario=_schema_precedence_scenario())
@settings(max_examples=100)
def test_property3_sql_schema_precedence(scenario: dict) -> None:
    """SQL-inferred schema takes precedence when present; upstream schema
    is used as fallback when SQL schema is None."""
    result = _infer_sink_schemas(scenario["compiled_joints"], [])
    sink_result = [cj for cj in result if cj.name == "my_sink"][0]

    if scenario["has_sql_schema"]:
        # SQL schema should be preserved unchanged
        assert sink_result.output_schema == scenario["sql_schema"]
    else:
        # Should fall back to upstream schema
        assert sink_result.output_schema == scenario["upstream_schema"]


# ---------------------------------------------------------------------------
# Property 4: Sink predicate pushdown to upstream source groups
# Feature: sink-sql-parsing
# Validates: Requirements 4.1, 4.2, 4.3
# ---------------------------------------------------------------------------


@st.composite
def _predicate_pushdown_scenario(draw: st.DrawFn) -> dict:
    """Generate a sink with a WHERE predicate referencing a single upstream source."""
    col = draw(_col_name_st)
    threshold = draw(st.integers(min_value=0, max_value=1000))
    op = draw(st.sampled_from([">", "<", "=", ">=", "<="]))

    upstream_name = "src"
    upstream_schema = _schema((col, "int64"), ("extra", "utf8"))
    sql = f"SELECT {col} FROM {upstream_name} WHERE {col} {op} {threshold}"

    lp, lineage, _, _, _, errors = _compile_joint_sql(
        "sink",
        sql,
        {upstream_name: upstream_schema},
        upstream=[upstream_name],
    )
    assert lp is not None, f"Failed to parse: {sql}"
    assert not errors

    sink_cj = _compiled_joint_stub(
        "my_sink",
        joint_type="sink",
        upstream=[upstream_name],
    )
    sink_cj = replace(sink_cj, logical_plan=lp, column_lineage=lineage)

    src_cj = _compiled_joint_stub(
        upstream_name,
        joint_type="source",
        output_schema=upstream_schema,
    )

    src_group = _make_group([upstream_name], group_id="src_grp")
    sink_group = _make_group(
        ["my_sink"],
        group_id="sink_grp",
        entry_joints=["my_sink"],
        exit_joints=["my_sink"],
    )

    return {
        "src_cj": src_cj,
        "sink_cj": sink_cj,
        "src_group": src_group,
        "sink_group": sink_group,
        "col": col,
        "threshold": threshold,
        "op": op,
    }


@given(scenario=_predicate_pushdown_scenario())
@settings(max_examples=100)
def test_property4_sink_predicate_pushdown(scenario: dict) -> None:
    """WHERE predicates on sink joints tracing to a single upstream source
    are pushed to that source group's per_joint_predicates."""
    cj_map = {
        scenario["src_cj"].name: scenario["src_cj"],
        scenario["sink_cj"].name: scenario["sink_cj"],
    }
    groups = [scenario["src_group"], scenario["sink_group"]]
    new_groups, results = cross_group_pushdown_pass(
        groups,
        cj_map,
        _FULL_CAPS,
        _EMPTY_CATALOG_TYPES,
    )

    src_grp = [g for g in new_groups if g.id == "src_grp"][0]
    assert "src" in src_grp.per_joint_predicates
    pushed = src_grp.per_joint_predicates["src"]
    assert len(pushed) >= 1

    # At least one applied result for predicate pushdown
    applied = [
        r for r in results if r.status == "applied" and r.rule == "cross_group_predicate_pushdown"
    ]
    assert len(applied) >= 1


# ---------------------------------------------------------------------------
# Property 5: Sink projection pushdown to upstream source groups
# Feature: sink-sql-parsing
# Validates: Requirements 5.1, 5.2, 5.3, 8.2
# ---------------------------------------------------------------------------


@st.composite
def _projection_pushdown_scenario(draw: st.DrawFn) -> dict:
    """Generate a sink with either explicit projections or SELECT *."""
    # Generate 2–5 unique columns
    all_cols = draw(st.lists(_col_name_st, min_size=2, max_size=5, unique=True))
    upstream_name = "src"
    upstream_schema = _schema(*[(c, "int64") for c in all_cols])

    use_star = draw(st.booleans())
    if use_star:
        sql = f"SELECT * FROM {upstream_name}"
        projected_cols: list[str] = []
    else:
        num_proj = draw(st.integers(min_value=1, max_value=len(all_cols)))
        projected_cols = all_cols[:num_proj]
        select_list = ", ".join(projected_cols)
        sql = f"SELECT {select_list} FROM {upstream_name}"

    lp, lineage, _, _, _, errors = _compile_joint_sql(
        "sink",
        sql,
        {upstream_name: upstream_schema},
        upstream=[upstream_name],
    )
    assert lp is not None
    assert not errors

    sink_cj = _compiled_joint_stub(
        "my_sink",
        joint_type="sink",
        upstream=[upstream_name],
    )
    sink_cj = replace(sink_cj, logical_plan=lp, column_lineage=lineage)

    src_cj = _compiled_joint_stub(
        upstream_name,
        joint_type="source",
        output_schema=upstream_schema,
    )

    src_group = _make_group([upstream_name], group_id="src_grp")
    sink_group = _make_group(
        ["my_sink"],
        group_id="sink_grp",
        entry_joints=["my_sink"],
        exit_joints=["my_sink"],
    )

    return {
        "src_cj": src_cj,
        "sink_cj": sink_cj,
        "src_group": src_group,
        "sink_group": sink_group,
        "use_star": use_star,
        "projected_cols": projected_cols,
        "all_cols": all_cols,
    }


@given(scenario=_projection_pushdown_scenario())
@settings(max_examples=100)
def test_property5_sink_projection_pushdown(scenario: dict) -> None:
    """Explicit projections are pushed; SELECT * skips projection pushdown."""
    cj_map = {
        scenario["src_cj"].name: scenario["src_cj"],
        scenario["sink_cj"].name: scenario["sink_cj"],
    }
    groups = [scenario["src_group"], scenario["sink_group"]]
    new_groups, results = cross_group_pushdown_pass(
        groups,
        cj_map,
        _FULL_CAPS,
        _EMPTY_CATALOG_TYPES,
    )

    src_grp = [g for g in new_groups if g.id == "src_grp"][0]

    if scenario["use_star"]:
        # SELECT * → no projection pushdown
        assert "src" not in src_grp.per_joint_projections
        skipped = [
            r
            for r in results
            if r.status == "skipped" and r.rule == "cross_group_projection_pushdown"
        ]
        assert len(skipped) >= 1
    else:
        # Explicit projections → pushed columns should be a subset of all columns
        assert "src" in src_grp.per_joint_projections
        pushed = set(src_grp.per_joint_projections["src"])
        expected = set(scenario["projected_cols"])
        assert expected.issubset(pushed)
        # Pushed columns should not include columns not referenced by the sink
        assert pushed.issubset(set(scenario["all_cols"]))
        applied = [
            r
            for r in results
            if r.status == "applied" and r.rule == "cross_group_projection_pushdown"
        ]
        assert len(applied) >= 1


# ---------------------------------------------------------------------------
# Property 6: Sink limit pushdown to upstream source groups
# Feature: sink-sql-parsing
# Validates: Requirements 6.1, 6.2, 6.3
# ---------------------------------------------------------------------------


@st.composite
def _limit_pushdown_scenario(draw: st.DrawFn) -> dict:
    """Generate a sink with LIMIT and optional blocking conditions."""
    col = draw(_col_name_st)
    upstream_name = "src"
    upstream_schema = _schema((col, "int64"))
    limit_val = draw(st.integers(min_value=1, max_value=1000))

    # Choose a blocking condition or none
    blocking = draw(st.sampled_from(["none", "aggregation", "join", "distinct"]))

    if blocking == "none":
        sql = f"SELECT {col} FROM {upstream_name} LIMIT {limit_val}"
        upstream_names = [upstream_name]
        upstream_schemas = {upstream_name: upstream_schema}
    elif blocking == "aggregation":
        sql = f"SELECT COUNT({col}) FROM {upstream_name} LIMIT {limit_val}"
        upstream_names = [upstream_name]
        upstream_schemas = {upstream_name: upstream_schema}
    elif blocking == "join":
        other_col = draw(_col_name_st.filter(lambda c: c != col))
        other_name = "src_b"
        upstream_schemas = {
            upstream_name: _schema(("id", "int64"), (col, "int64")),
            other_name: _schema(("id", "int64"), (other_col, "int64")),
        }
        sql = (
            f"SELECT a.{col}, b.{other_col} FROM {upstream_name} a "
            f"JOIN {other_name} b ON a.id = b.id LIMIT {limit_val}"
        )
        upstream_names = [upstream_name, other_name]
    elif blocking == "distinct":
        sql = f"SELECT DISTINCT {col} FROM {upstream_name} LIMIT {limit_val}"
        upstream_names = [upstream_name]
        upstream_schemas = {upstream_name: upstream_schema}
    else:
        raise ValueError(f"Unknown blocking: {blocking}")

    lp, lineage, _, _, _, errors = _compile_joint_sql(
        "sink",
        sql,
        upstream_schemas,
        upstream=upstream_names,
    )
    assert lp is not None
    assert not errors

    sink_cj = _compiled_joint_stub(
        "my_sink",
        joint_type="sink",
        upstream=upstream_names,
    )
    sink_cj = replace(sink_cj, logical_plan=lp, column_lineage=lineage)

    # Build source groups
    source_cjs = []
    source_groups = []
    for name in upstream_names:
        src_cj = _compiled_joint_stub(
            name,
            joint_type="source",
            output_schema=upstream_schemas[name],
        )
        source_cjs.append(src_cj)
        source_groups.append(_make_group([name], group_id=f"{name}_grp"))

    sink_group = _make_group(
        ["my_sink"],
        group_id="sink_grp",
        entry_joints=["my_sink"],
        exit_joints=["my_sink"],
    )

    return {
        "source_cjs": source_cjs,
        "sink_cj": sink_cj,
        "source_groups": source_groups,
        "sink_group": sink_group,
        "blocking": blocking,
        "limit_val": limit_val,
        "upstream_names": upstream_names,
    }


@given(scenario=_limit_pushdown_scenario())
@settings(max_examples=100)
def test_property6_sink_limit_pushdown(scenario: dict) -> None:
    """LIMIT is pushed when no blocking constructs; blocked by aggregation,
    join, or DISTINCT."""
    cj_map = {cj.name: cj for cj in scenario["source_cjs"]}
    cj_map[scenario["sink_cj"].name] = scenario["sink_cj"]
    groups = scenario["source_groups"] + [scenario["sink_group"]]

    new_groups, results = cross_group_pushdown_pass(
        groups,
        cj_map,
        _FULL_CAPS,
        _EMPTY_CATALOG_TYPES,
    )

    blocking = scenario["blocking"]

    if blocking == "none":
        # Limit should be pushed to the single source group
        src_grp = [g for g in new_groups if g.id == "src_grp"][0]
        assert "src" in src_grp.per_joint_limits
        assert src_grp.per_joint_limits["src"] == scenario["limit_val"]
        applied = [
            r for r in results if r.status == "applied" and r.rule == "cross_group_limit_pushdown"
        ]
        assert len(applied) >= 1
    else:
        # Limit should NOT be pushed to any source group
        for src_grp in [g for g in new_groups if g.id != "sink_grp"]:
            for name in scenario["upstream_names"]:
                assert name not in src_grp.per_joint_limits
        skipped = [
            r for r in results if r.status == "skipped" and r.rule == "cross_group_limit_pushdown"
        ]
        assert len(skipped) >= 1


# ---------------------------------------------------------------------------
# Property 7: Consumer-side LogicalPlan is never modified by pushdown
# Feature: sink-sql-parsing
# Validates: Requirements 8.1, 8.3
# ---------------------------------------------------------------------------


@st.composite
def _immutability_scenario(draw: st.DrawFn) -> dict:
    """Generate a sink with predicates, projections, and/or limit, then
    snapshot the LogicalPlan before running pushdown."""
    cols = draw(st.lists(_col_name_st, min_size=1, max_size=4, unique=True))
    upstream_name = "src"
    upstream_schema = _schema(*[(c, "int64") for c in cols])

    # Build SQL with a mix of features
    select_list = ", ".join(cols)
    clauses = [f"SELECT {select_list} FROM {upstream_name}"]

    add_where = draw(st.booleans())
    if add_where:
        threshold = draw(st.integers(min_value=0, max_value=100))
        clauses.append(f"WHERE {cols[0]} > {threshold}")

    add_limit = draw(st.booleans())
    if add_limit:
        limit_val = draw(st.integers(min_value=1, max_value=1000))
        clauses.append(f"LIMIT {limit_val}")

    sql = " ".join(clauses)

    lp, lineage, _, _, _, errors = _compile_joint_sql(
        "sink",
        sql,
        {upstream_name: upstream_schema},
        upstream=[upstream_name],
    )
    assert lp is not None
    assert not errors

    sink_cj = _compiled_joint_stub(
        "my_sink",
        joint_type="sink",
        upstream=[upstream_name],
    )
    sink_cj = replace(sink_cj, logical_plan=lp, column_lineage=lineage)

    src_cj = _compiled_joint_stub(
        upstream_name,
        joint_type="source",
        output_schema=upstream_schema,
    )

    src_group = _make_group([upstream_name], group_id="src_grp")
    sink_group = _make_group(
        ["my_sink"],
        group_id="sink_grp",
        entry_joints=["my_sink"],
        exit_joints=["my_sink"],
    )

    # Deep copy the LogicalPlan before the pass
    lp_snapshot = copy.deepcopy(lp)

    return {
        "src_cj": src_cj,
        "sink_cj": sink_cj,
        "src_group": src_group,
        "sink_group": sink_group,
        "lp_snapshot": lp_snapshot,
    }


@given(scenario=_immutability_scenario())
@settings(max_examples=100)
def test_property7_logical_plan_immutability(scenario: dict) -> None:
    """The sink's LogicalPlan is identical before and after pushdown."""
    cj_map = {
        scenario["src_cj"].name: scenario["src_cj"],
        scenario["sink_cj"].name: scenario["sink_cj"],
    }
    groups = [scenario["src_group"], scenario["sink_group"]]
    cross_group_pushdown_pass(groups, cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES)

    lp_after = scenario["sink_cj"].logical_plan
    lp_before = scenario["lp_snapshot"]

    assert lp_after is not None
    assert lp_after.projections == lp_before.projections
    assert lp_after.predicates == lp_before.predicates
    assert lp_after.limit == lp_before.limit
    assert lp_after.joins == lp_before.joins
    assert lp_after.aggregations == lp_before.aggregations
    assert lp_after.ordering == lp_before.ordering
    assert lp_after.distinct == lp_before.distinct


# ---------------------------------------------------------------------------
# Property 8: Every sink pushdown decision produces an OptimizationResult
# Feature: sink-sql-parsing
# Validates: Requirements 9.1, 9.2, 9.3, 9.4
# ---------------------------------------------------------------------------


@st.composite
def _optimization_result_scenario(draw: st.DrawFn) -> dict:
    """Generate a sink with various pushdown features and verify that every
    decision produces an OptimizationResult."""
    cols = draw(st.lists(_col_name_st, min_size=1, max_size=4, unique=True))
    upstream_name = "src"
    upstream_schema = _schema(*[(c, "int64") for c in cols])

    # Build SQL with a random combination of features
    select_star = draw(st.booleans())
    if select_star:
        select_list = "*"
    else:
        num_proj = draw(st.integers(min_value=1, max_value=len(cols)))
        select_list = ", ".join(cols[:num_proj])

    clauses = [f"SELECT {select_list} FROM {upstream_name}"]

    has_where = draw(st.booleans())
    if has_where:
        threshold = draw(st.integers(min_value=0, max_value=100))
        clauses.append(f"WHERE {cols[0]} > {threshold}")

    has_limit = draw(st.booleans())
    if has_limit:
        limit_val = draw(st.integers(min_value=1, max_value=1000))
        clauses.append(f"LIMIT {limit_val}")

    sql = " ".join(clauses)

    lp, lineage, _, _, _, errors = _compile_joint_sql(
        "sink",
        sql,
        {upstream_name: upstream_schema},
        upstream=[upstream_name],
    )
    assert lp is not None
    assert not errors

    sink_cj = _compiled_joint_stub(
        "my_sink",
        joint_type="sink",
        upstream=[upstream_name],
    )
    sink_cj = replace(sink_cj, logical_plan=lp, column_lineage=lineage)

    src_cj = _compiled_joint_stub(
        upstream_name,
        joint_type="source",
        output_schema=upstream_schema,
    )

    src_group = _make_group([upstream_name], group_id="src_grp")
    sink_group = _make_group(
        ["my_sink"],
        group_id="sink_grp",
        entry_joints=["my_sink"],
        exit_joints=["my_sink"],
    )

    return {
        "src_cj": src_cj,
        "sink_cj": sink_cj,
        "src_group": src_group,
        "sink_group": sink_group,
        "lp": lp,
        "has_where": has_where,
        "has_limit": has_limit,
        "select_star": select_star,
    }


@given(scenario=_optimization_result_scenario())
@settings(max_examples=100)
def test_property8_optimization_result_completeness(scenario: dict) -> None:
    """Every pushdown decision (predicate, projection, limit) produces
    at least one OptimizationResult."""
    cj_map = {
        scenario["src_cj"].name: scenario["src_cj"],
        scenario["sink_cj"].name: scenario["sink_cj"],
    }
    groups = [scenario["src_group"], scenario["sink_group"]]
    _, results = cross_group_pushdown_pass(
        groups,
        cj_map,
        _FULL_CAPS,
        _EMPTY_CATALOG_TYPES,
    )

    lp = scenario["lp"]

    # Every predicate conjunct should produce at least one result
    if scenario["has_where"] and lp.predicates:
        pred_results = [r for r in results if r.rule == "cross_group_predicate_pushdown"]
        assert len(pred_results) >= 1

    # Projection decision should always produce a result (applied or skipped)
    proj_results = [r for r in results if r.rule == "cross_group_projection_pushdown"]
    assert len(proj_results) >= 1

    # Limit decision should produce a result when LIMIT is present
    if scenario["has_limit"] and lp.limit is not None:
        limit_results = [r for r in results if r.rule == "cross_group_limit_pushdown"]
        assert len(limit_results) >= 1
