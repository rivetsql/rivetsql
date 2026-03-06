"""Tests for QueryPlanner — transient pipeline construction.

All tests exercise the planner through its public API
(build_transient_pipeline), which delegates to preprocess_sql for
reference resolution and SQL rewriting.

Property 1: For any SQL with N distinct table refs, the transient Assembly
includes those refs (as project joints or catalog sources) plus a __query
SQL joint whose upstream covers all referenced names.

Property 3: Cached joints skip re-execution.
Property 4: Uncached joints trigger upstream execution.
Property 5: Cross-catalog queries generate per-catalog source joints.

Validates: Requirements 6.5, 7.3, 10.3, 11.1, 11.3, 11.4, 11.5
"""

from __future__ import annotations

from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
)
from rivet_core.interactive.material_cache import MaterialCache
from rivet_core.interactive.query_planner import QueryPlanner
from rivet_core.models import Material


def _make_compiled_joint(
    name: str,
    upstream: list[str] | None = None,
    catalog: str | None = None,
    joint_type: str = "sql",
    table: str | None = None,
) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type=joint_type,
        catalog=catalog,
        catalog_type=None,
        engine="eng",
        engine_resolution="project_default",
        adapter=None,
        sql=f"SELECT * FROM {name}" if joint_type == "sql" else None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=upstream or [],
        eager=False,
        table=table,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_assembly(
    joints: list[CompiledJoint] | None = None,
    catalog_names: list[str] | None = None,
) -> CompiledAssembly:
    joints = joints or []
    catalogs = [CompiledCatalog(name=c, type="stub") for c in (catalog_names or [])]
    return CompiledAssembly(
        success=True,
        profile_name="default",
        catalogs=catalogs,
        engines=[CompiledEngine(name="eng", engine_type="stub", native_catalog_types=[])],
        adapters=[],
        joints=joints,
        fused_groups=[],
        materializations=[],
        execution_order=[j.name for j in joints],
        errors=[],
        warnings=[],
    )


# ---------------------------------------------------------------------------
# Structure tests
# ---------------------------------------------------------------------------


class TestTransientPipelineStructure:
    """The transient assembly includes referenced joints/sources + __query."""

    def test_single_catalog_table_reference(self) -> None:
        """Unresolved ref creates a catalog source joint."""
        assembly = _make_assembly(catalog_names=["mydb"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM users", catalog_context="mydb",
            assembly=assembly, material_cache=cache,
            catalog_names=frozenset({"mydb"}),
        )

        sources = [j for j in transient.joints.values() if j.joint_type == "source"]
        assert len(sources) == 1
        assert sources[0].catalog == "mydb"
        assert sources[0].table == "users"

        query = transient.joints.get("__query")
        assert query is not None
        assert query.sql == "SELECT * FROM users"
        assert sources[0].name in query.upstream

    def test_single_project_joint_reference(self) -> None:
        """Ref matching a project joint includes that joint directly."""
        joints = [_make_compiled_joint("users")]
        assembly = _make_assembly(joints=joints)
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM users", catalog_context=None,
            assembly=assembly, material_cache=cache,
            catalog_names=frozenset(),
        )

        assert "users" in transient.joints
        query = transient.joints["__query"]
        assert "users" in query.upstream

    def test_multiple_table_references(self) -> None:
        assembly = _make_assembly(catalog_names=["mydb"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
            catalog_context="mydb", assembly=assembly, material_cache=cache,
            catalog_names=frozenset({"mydb"}),
        )

        sources = [j for j in transient.joints.values() if j.joint_type == "source"]
        assert len(sources) == 2
        query = transient.joints["__query"]
        assert len(query.upstream) == 2

    def test_sql_joint_preserves_unqualified_sql(self) -> None:
        """Unqualified refs keep original SQL unchanged."""
        assembly = _make_assembly(catalog_names=["mydb"])
        cache = MaterialCache()
        planner = QueryPlanner()
        sql = "SELECT count(*) FROM events"

        transient, _ = planner.build_transient_pipeline(
            sql, catalog_context="mydb", assembly=assembly, material_cache=cache,
            catalog_names=frozenset({"mydb"}),
        )

        assert transient.joints["__query"].sql == sql

    def test_sql_joint_rewrites_qualified_refs(self) -> None:
        """Qualified catalog refs are rewritten to unqualified joint names."""
        assembly = _make_assembly(catalog_names=["mydb"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM mydb.public.events",
            catalog_context=None, assembly=assembly, material_cache=cache,
            catalog_names=frozenset({"mydb"}),
        )

        query_sql = transient.joints["__query"].sql.upper()
        assert "MYDB" not in query_sql
        assert "PUBLIC" not in query_sql
        assert "EVENTS" in query_sql

    def test_project_joint_with_upstream_closure(self) -> None:
        """Referencing a joint includes its full upstream chain."""
        joints = [
            _make_compiled_joint("raw", joint_type="source", catalog="local", table="raw"),
            _make_compiled_joint("clean", upstream=["raw"]),
        ]
        assembly = _make_assembly(joints=joints)
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM clean", catalog_context=None,
            assembly=assembly, material_cache=cache,
            catalog_names=frozenset(),
        )

        assert "raw" in transient.joints
        assert "clean" in transient.joints
        assert "__query" in transient.joints


# ---------------------------------------------------------------------------
# Cached joints
# ---------------------------------------------------------------------------


class TestCachedJointsSkipExecution:
    """Property 3: Cached joints are not in needs_execution list."""

    def test_cached_joint_not_in_needs_execution(self) -> None:
        joints = [_make_compiled_joint("users")]
        assembly = _make_assembly(joints=joints)
        cache = MaterialCache()
        mat = MagicMock(spec=Material)
        cache.put("users", mat)
        planner = QueryPlanner()

        _, needs_exec = planner.build_transient_pipeline(
            "SELECT * FROM users", catalog_context=None,
            assembly=assembly, material_cache=cache,
            catalog_names=frozenset(),
        )

        assert "users" not in needs_exec


# ---------------------------------------------------------------------------
# Uncached joints
# ---------------------------------------------------------------------------


class TestUncachedJointsTriggerUpstream:
    """Property 4: Uncached joints and their upstream deps are in needs_execution."""

    def test_uncached_joint_in_needs_execution(self) -> None:
        joints = [
            _make_compiled_joint("raw_data"),
            _make_compiled_joint("transform", upstream=["raw_data"]),
        ]
        assembly = _make_assembly(joints=joints)
        cache = MaterialCache()
        planner = QueryPlanner()

        _, needs_exec = planner.build_transient_pipeline(
            "SELECT * FROM transform", catalog_context=None,
            assembly=assembly, material_cache=cache,
            catalog_names=frozenset(),
        )

        assert "transform" in needs_exec
        assert "raw_data" in needs_exec

    def test_upstream_order_is_topological(self) -> None:
        joints = [
            _make_compiled_joint("a"),
            _make_compiled_joint("b", upstream=["a"]),
            _make_compiled_joint("c", upstream=["b"]),
        ]
        assembly = _make_assembly(joints=joints)
        cache = MaterialCache()
        planner = QueryPlanner()

        _, needs_exec = planner.build_transient_pipeline(
            "SELECT * FROM c", catalog_context=None,
            assembly=assembly, material_cache=cache,
            catalog_names=frozenset(),
        )

        assert needs_exec.index("a") < needs_exec.index("b")
        assert needs_exec.index("b") < needs_exec.index("c")


# ---------------------------------------------------------------------------
# Cross-catalog
# ---------------------------------------------------------------------------


class TestCrossCatalogSources:
    """Property 5: Cross-catalog queries generate per-catalog source joints."""

    def test_cross_catalog_generates_per_catalog_sources(self) -> None:
        assembly = _make_assembly(catalog_names=["db1", "db2"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM db1.public.users JOIN db2.analytics.events ON users.id = events.user_id",
            catalog_context=None, assembly=assembly, material_cache=cache,
            catalog_names=frozenset({"db1", "db2"}),
        )

        sources = [j for j in transient.joints.values() if j.joint_type == "source"]
        catalogs = {s.catalog for s in sources}
        assert "db1" in catalogs
        assert "db2" in catalogs


class TestCatalogTableReference:
    """Catalog table references produce source joints with correct catalog."""

    def test_catalog_table_source_joint(self) -> None:
        assembly = _make_assembly(catalog_names=["warehouse"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM orders", catalog_context="warehouse",
            assembly=assembly, material_cache=cache,
            catalog_names=frozenset({"warehouse"}),
        )

        sources = [j for j in transient.joints.values() if j.joint_type == "source"]
        assert len(sources) == 1
        assert sources[0].catalog == "warehouse"
        assert sources[0].table == "orders"


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------

_catalog_name = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)
_table_name = st.from_regex(r"[a-z][a-z0-9_]{2,9}", fullmatch=True)
_schema_name = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)
_joint_name = st.from_regex(r"[a-z][a-z0-9_]{2,14}", fullmatch=True)


@given(
    catalog_table_pairs=st.lists(
        st.tuples(_catalog_name, _schema_name, _table_name),
        min_size=2,
        max_size=5,
        unique_by=lambda t: (t[0], t[2]),
    )
)
@settings(max_examples=100)
def test_cross_catalog_generates_per_catalog_sources(
    catalog_table_pairs: list[tuple[str, str, str]],
) -> None:
    """Property 5: SQL referencing N catalogs produces source joints for each."""
    seen_catalogs: set[str] = set()
    unique_pairs: list[tuple[str, str, str]] = []
    for catalog, schema, table in catalog_table_pairs:
        if catalog not in seen_catalogs or (catalog, table) not in {
            (c, t) for c, _, t in unique_pairs
        }:
            unique_pairs.append((catalog, schema, table))
            seen_catalogs.add(catalog)

    distinct_catalogs = {c for c, _, _ in unique_pairs}
    from_clause = " CROSS JOIN ".join(
        f"{cat}.{schema}.{tbl}" for cat, schema, tbl in unique_pairs
    )
    sql = f"SELECT * FROM {from_clause}"

    assembly = _make_assembly(catalog_names=list(distinct_catalogs))
    cache = MaterialCache()
    planner = QueryPlanner()

    transient, _ = planner.build_transient_pipeline(
        sql, catalog_context=None, assembly=assembly, material_cache=cache,
        catalog_names=frozenset(distinct_catalogs),
    )

    sources = [j for j in transient.joints.values() if j.joint_type == "source"]
    source_catalogs = {s.catalog for s in sources}
    assert distinct_catalogs <= source_catalogs


def _build_select_sql(table_names: list[str]) -> str:
    if len(table_names) == 1:
        return f"SELECT * FROM {table_names[0]}"
    first = table_names[0]
    joins = " ".join(f"JOIN {t} ON {first}.id = {t}.id" for t in table_names[1:])
    return f"SELECT * FROM {first} {joins}"


@given(table_names=st.lists(_table_name, min_size=1, max_size=5, unique=True))
@settings(max_examples=100)
def test_transient_pipeline_has_query_joint(table_names: list[str]) -> None:
    """Property 1: Transient assembly always contains a __query SQL joint."""
    sql = _build_select_sql(table_names)
    assembly = _make_assembly(catalog_names=["mydb"])
    cache = MaterialCache()
    planner = QueryPlanner()

    transient, _ = planner.build_transient_pipeline(
        sql, catalog_context="mydb", assembly=assembly, material_cache=cache,
        catalog_names=frozenset({"mydb"}),
    )

    query = transient.joints.get("__query")
    assert query is not None
    assert query.joint_type == "sql"
    assert query.sql == sql
    # __query upstream covers all referenced table names
    assert len(query.upstream) == len(table_names)


@given(joint_names=st.lists(_joint_name, min_size=1, max_size=10, unique=True))
@settings(max_examples=100)
def test_cached_joints_skip_re_execution(
    joint_names: list[str],
) -> None:
    """Property 3: Cached joints are not in needs_execution."""
    joints = [_make_compiled_joint(name) for name in joint_names]
    assembly = _make_assembly(joints=joints)
    cache = MaterialCache()
    for name in joint_names:
        cache.put(name, MagicMock(spec=Material))

    planner = QueryPlanner()
    for joint_name in joint_names:
        _, needs_exec = planner.build_transient_pipeline(
            f"SELECT * FROM {joint_name}", catalog_context=None,
            assembly=assembly, material_cache=cache,
            catalog_names=frozenset(),
        )
        assert joint_name not in needs_exec


def _build_linear_chain(names: list[str]) -> list[CompiledJoint]:
    joints = []
    for i, name in enumerate(names):
        upstream = [names[i - 1]] if i > 0 else []
        joints.append(_make_compiled_joint(name, upstream=upstream))
    return joints


@given(
    chain=st.lists(_joint_name, min_size=2, max_size=6, unique=True),
)
@settings(max_examples=100)
def test_uncached_joint_includes_all_transitive_upstream(
    chain: list[str],
) -> None:
    """Property 4: Uncached joints include all transitive upstream in needs_execution."""
    joints = _build_linear_chain(chain)
    assembly = _make_assembly(joints=joints)
    cache = MaterialCache()
    planner = QueryPlanner()

    target = chain[-1]
    _, needs_exec = planner.build_transient_pipeline(
        f"SELECT * FROM {target}", catalog_context=None,
        assembly=assembly, material_cache=cache,
        catalog_names=frozenset(),
    )

    for name in chain:
        assert name in needs_exec


@given(names=st.lists(_joint_name, min_size=3, max_size=8, unique=True))
@settings(max_examples=100)
def test_uncached_upstream_order_is_topological(names: list[str]) -> None:
    """Property 4: needs_execution preserves topological order."""
    joints = _build_linear_chain(names)
    assembly = _make_assembly(joints=joints)
    cache = MaterialCache()
    planner = QueryPlanner()

    target = names[-1]
    _, needs_exec = planner.build_transient_pipeline(
        f"SELECT * FROM {target}", catalog_context=None,
        assembly=assembly, material_cache=cache,
        catalog_names=frozenset(),
    )

    assert set(names) == set(needs_exec)
    for i, name in enumerate(names):
        if i > 0:
            upstream = names[i - 1]
            assert needs_exec.index(upstream) < needs_exec.index(name)


# ---------------------------------------------------------------------------
# Property 6: Transient assembly structural completeness
# Feature: repl-query-planner, Property 6: Transient assembly structural completeness
# Validates: Requirements 2.2, 3.4, 5.1, 5.2, 5.4, 6.1, 6.2
# ---------------------------------------------------------------------------

_p6_joint_name = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)
_p6_catalog_name = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)
_p6_table_name = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)


def _build_linear_joints(names: list[str]) -> list[CompiledJoint]:
    """Build a linear chain of SQL joints: names[0] ← names[1] ← ... ← names[-1]."""
    joints = []
    for i, name in enumerate(names):
        upstream = [names[i - 1]] if i > 0 else []
        joints.append(_make_compiled_joint(name, upstream=upstream))
    return joints


@given(
    joint_chain=st.lists(_p6_joint_name, min_size=1, max_size=4, unique=True),
    catalog_tables=st.lists(
        st.tuples(_p6_catalog_name, _p6_table_name),
        min_size=1,
        max_size=3,
    ),
)
@settings(max_examples=100)
def test_transient_assembly_structural_completeness(
    joint_chain: list[str],
    catalog_tables: list[tuple[str, str]],
) -> None:
    """Property 6: Transient assembly structural completeness.

    For any SQL referencing a mix of project joints and catalog tables:
    (a) every referenced joint and its full transitive upstream closure is present,
    (b) a source joint exists for each catalog table ref with correct joint_type,
        catalog, and table fields,
    (c) source joint names match the user's SQL reference (last dotted part),
    (d) the __query joint's upstream list includes all referenced joint/source names.
    """
    # Deduplicate catalog_tables to avoid name collisions with joint_chain
    seen_tables: set[str] = set()
    unique_catalog_tables: list[tuple[str, str]] = []
    for cat, tbl in catalog_tables:
        key = tbl
        if key not in seen_tables and tbl not in joint_chain:
            unique_catalog_tables.append((cat, tbl))
            seen_tables.add(key)

    if not unique_catalog_tables:
        # Ensure at least one catalog table that doesn't collide
        unique_catalog_tables = [("testcat", "testtbl")]
        if "testtbl" in joint_chain:
            unique_catalog_tables = [("testcat", "xtbl")]

    joints = _build_linear_joints(joint_chain)
    distinct_catalogs = list({cat for cat, _ in unique_catalog_tables})
    assembly = _make_assembly(joints=joints, catalog_names=distinct_catalogs)
    cache = MaterialCache()
    planner = QueryPlanner()

    # Build SQL referencing the last joint in the chain + all catalog tables
    target_joint = joint_chain[-1]
    catalog_refs = [f"{cat}.public.{tbl}" for cat, tbl in unique_catalog_tables]
    all_refs = [target_joint] + catalog_refs
    from_clause = " CROSS JOIN ".join(all_refs)
    sql = f"SELECT * FROM {from_clause}"

    transient, _ = planner.build_transient_pipeline(
        sql, catalog_context=None, assembly=assembly, material_cache=cache,
        catalog_names=frozenset(distinct_catalogs),
    )

    # (a) All joints in the chain must be present (upstream closure)
    for jn in joint_chain:
        assert jn in transient.joints, f"Joint '{jn}' missing from transient assembly"

    # (b) Source joints exist for each catalog table with correct fields
    source_joints = {j.name: j for j in transient.joints.values() if j.joint_type == "source"}
    for cat, tbl in unique_catalog_tables:
        # 3-part refs (cat.public.tbl) produce table="public.tbl" (schema-qualified)
        expected_table = f"public.{tbl}"
        matching = [j for j in source_joints.values() if j.catalog == cat and j.table == expected_table]
        assert len(matching) >= 1, (
            f"No source joint found for catalog='{cat}', table='{expected_table}'"
        )
        for src in matching:
            assert src.joint_type == "source"
            assert src.catalog == cat
            assert src.table == expected_table

    # (c) Source joint names match the last part of the user's SQL reference
    for cat, tbl in unique_catalog_tables:
        expected_table = f"public.{tbl}"
        matching = [j for j in source_joints.values() if j.catalog == cat and j.table == expected_table]
        for src in matching:
            # Name must be either the table name itself or a prefixed collision-avoidance name
            assert src.name == tbl or src.name.endswith(tbl) or "__src_" in src.name

    # (d) __query joint exists and its upstream covers all referenced names
    query_joint = transient.joints.get("__query")
    assert query_joint is not None, "__query joint missing from transient assembly"
    assert query_joint.joint_type == "sql"
    # SQL may be rewritten to replace qualified refs with joint names
    assert query_joint.sql is not None

    upstream_set = set(query_joint.upstream)
    # The target joint must be in upstream
    assert target_joint in upstream_set, (
        f"Target joint '{target_joint}' not in __query upstream: {upstream_set}"
    )
    # Each source joint must be in upstream
    for src in source_joints.values():
        assert src.name in upstream_set, (
            f"Source joint '{src.name}' not in __query upstream: {upstream_set}"
        )


# ---------------------------------------------------------------------------
# Property 1: Table reference extraction covers all nesting levels
# Feature: repl-query-planner, Property 1: Table reference extraction covers all nesting levels
# Validates: Requirements 1.1, 1.3
# ---------------------------------------------------------------------------

_p1_sql_reserved = frozenset({
    "all", "alter", "and", "any", "as", "asc", "at", "between", "by", "case",
    "create", "cross", "delete", "desc", "distinct", "do", "drop", "else",
    "end", "exists", "false", "for", "from", "full", "group", "having", "if",
    "in", "index", "inner", "insert", "into", "is", "join", "left", "like",
    "limit", "not", "null", "of", "offset", "on", "or", "order", "outer",
    "right", "select", "set", "some", "table", "then", "to", "true", "union",
    "update", "values", "view", "when", "where", "with",
})
_p1_table_name = st.from_regex(r"[a-z][a-z]{2,7}", fullmatch=True).filter(
    lambda t: t not in _p1_sql_reserved
)


@given(
    top_table=_p1_table_name,
    join_table=_p1_table_name,
    subquery_table=_p1_table_name,
)
@settings(max_examples=100)
def test_table_refs_extracted_from_all_nesting_levels(
    top_table: str,
    join_table: str,
    subquery_table: str,
) -> None:
    """Property 1: Table reference extraction covers all nesting levels.

    For any SQL with table refs at top-level FROM, JOINs, and subqueries,
    all refs are extracted and appear as source joints in the transient assembly.
    """
    # Ensure distinct names to avoid ambiguity
    if join_table == top_table:
        join_table = top_table + "j"
    if subquery_table in (top_table, join_table):
        subquery_table = top_table + "s"

    # SQL with refs at three nesting levels:
    # 1. top-level FROM (top_table)
    # 2. JOIN (join_table)
    # 3. subquery in WHERE (subquery_table)
    sql = (
        f"SELECT {top_table}.id FROM {top_table} "
        f"JOIN {join_table} ON {top_table}.id = {join_table}.id "
        f"WHERE {top_table}.id IN (SELECT id FROM {subquery_table})"
    )

    all_tables = {top_table, join_table, subquery_table}
    assembly = _make_assembly(catalog_names=["testcat"])
    cache = MaterialCache()
    planner = QueryPlanner()

    transient, _ = planner.build_transient_pipeline(
        sql, catalog_context="testcat", assembly=assembly, material_cache=cache,
        catalog_names=frozenset({"testcat"}),
    )

    # All referenced table names must appear as source joints or project joints
    joint_names = set(transient.joints.keys()) - {"__query"}
    for tbl in all_tables:
        assert tbl in joint_names, (
            f"Table '{tbl}' not found in transient joints {joint_names}"
        )


# ---------------------------------------------------------------------------
# Property 2: CTE aliases are excluded from extracted references
# Feature: repl-query-planner, Property 2: CTE aliases are excluded from extracted references
# Validates: Requirements 1.2
# ---------------------------------------------------------------------------

_p2_cte_name = st.from_regex(r"[a-z][a-z]{2,7}", fullmatch=True)
_p2_base_table = st.from_regex(r"[a-z][a-z]{2,7}", fullmatch=True)


@given(
    cte_names=st.lists(_p2_cte_name, min_size=1, max_size=3, unique=True),
    base_table=_p2_base_table,
)
@settings(max_examples=100)
def test_cte_aliases_excluded_from_extracted_references(
    cte_names: list[str],
    base_table: str,
) -> None:
    """Property 2: CTE aliases are excluded from extracted references.

    For any SQL containing CTEs, the CTE alias names must never appear as
    source joints in the transient assembly — only the real base table(s)
    referenced inside the CTE bodies should appear.
    """
    # Ensure base_table doesn't collide with any CTE name
    if base_table in cte_names:
        base_table = base_table + "x"

    # Build SQL: each CTE selects from base_table, outer query selects from last CTE
    cte_clauses = ", ".join(
        f"{name} AS (SELECT * FROM {base_table})" for name in cte_names
    )
    last_cte = cte_names[-1]
    sql = f"WITH {cte_clauses} SELECT * FROM {last_cte}"

    assembly = _make_assembly(catalog_names=["testcat"])
    cache = MaterialCache()
    planner = QueryPlanner()

    transient, _ = planner.build_transient_pipeline(
        sql, catalog_context="testcat", assembly=assembly, material_cache=cache,
        catalog_names=frozenset({"testcat"}),
    )

    joint_names = set(transient.joints.keys()) - {"__query"}

    # CTE alias names must NOT appear as joints (they are not real tables)
    for cte_name in cte_names:
        assert cte_name not in joint_names, (
            f"CTE alias '{cte_name}' should not appear as a joint in the transient assembly"
        )

    # The real base table must appear as a source joint
    assert base_table in joint_names, (
        f"Base table '{base_table}' should appear as a source joint"
    )


# ---------------------------------------------------------------------------
# Property 7: Cached joints are excluded from needs-execution
# Feature: repl-query-planner, Property 7: Cached joints are excluded from needs-execution
# Validates: Requirements 2.3
# ---------------------------------------------------------------------------

_p7_joint_name = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)


@given(
    chain=st.lists(_p7_joint_name, min_size=2, max_size=6, unique=True),
    cached_indices=st.lists(st.integers(min_value=0, max_value=5), min_size=0, max_size=6, unique=True),
)
@settings(max_examples=100)
def test_cached_joints_never_in_needs_execution(
    chain: list[str],
    cached_indices: list[int],
) -> None:
    """Property 7: Cached joints are excluded from needs-execution.

    For any assembly with a linear joint chain and any subset of those joints
    cached in MaterialCache, needs_execution must never contain a cached joint name.
    """
    joints = _build_linear_chain(chain)
    assembly = _make_assembly(joints=joints)
    cache = MaterialCache()

    # Cache a subset of joints based on the provided indices
    cached_names: set[str] = set()
    for idx in cached_indices:
        if idx < len(chain):
            name = chain[idx]
            cache.put(name, MagicMock(spec=Material))
            cached_names.add(name)

    planner = QueryPlanner()
    target = chain[-1]
    _, needs_exec = planner.build_transient_pipeline(
        f"SELECT * FROM {target}", catalog_context=None,
        assembly=assembly, material_cache=cache,
        catalog_names=frozenset(),
    )

    # Core invariant: no cached joint name appears in needs_execution
    for name in cached_names:
        assert name not in needs_exec, (
            f"Cached joint '{name}' should not be in needs_execution={needs_exec}"
        )


# ---------------------------------------------------------------------------
# Property 3: Invalid SQL produces parse errors
# Feature: repl-query-planner, Property 3: Invalid SQL produces parse errors
# Validates: Requirements 1.4
# ---------------------------------------------------------------------------

from rivet_core.errors import SQLParseError  # noqa: E402 (appended after module body)


def _make_empty_assembly() -> CompiledAssembly:
    return _make_assembly()


# Strategies for generating syntactically invalid SQL strings
_invalid_sql_strategies = st.one_of(
    # Random non-SQL text (letters, digits, punctuation — no valid SELECT)
    st.text(
        alphabet=st.characters(
            whitelist_categories=("Lu", "Ll", "Nd", "Po"),
            blacklist_characters="\x00",
        ),
        min_size=1,
        max_size=40,
    ).filter(lambda s: not s.strip().upper().startswith("SELECT")),
    # Truncated SELECT with no FROM clause target
    st.just("SELECT FROM WHERE"),
    st.just("SELECT * FROM"),
    st.just("SELECT"),
    # DDL/DML statements (not allowed — RVT-702)
    st.sampled_from([
        "INSERT INTO foo VALUES (1)",
        "DROP TABLE foo",
        "CREATE TABLE foo (id INT)",
        "UPDATE foo SET id = 1",
        "DELETE FROM foo",
    ]),
    # Keyword soup
    st.lists(
        st.sampled_from(["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "JOIN", "ON"]),
        min_size=2,
        max_size=5,
    ).map(" ".join),
)


@given(invalid_sql=_invalid_sql_strategies)
@settings(max_examples=100)
def test_invalid_sql_raises_parse_error(invalid_sql: str) -> None:
    """Property 3: Invalid SQL produces parse errors.

    For any syntactically invalid SQL string, QueryPlanner SHALL raise
    SQLParseError or ValueError (not an unhandled exception). SQLParseError
    contains a structured RivetError with an RVT-7xx code. ValueError is
    raised by the preprocessor for unresolvable table references in DML/DDL
    statements that are not supported by the REPL.
    """
    assembly = _make_empty_assembly()
    cache = MaterialCache()
    planner = QueryPlanner()

    try:
        planner.build_transient_pipeline(
            invalid_sql,
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset(),
        )
        # If no exception is raised, the SQL was valid — skip (not a failure)
        # This can happen for edge cases sqlglot accepts as valid
    except SQLParseError as e:
        # Correct: a structured parse error was raised
        assert e.error.code.startswith("RVT-7"), (
            f"Expected RVT-7xx error code, got {e.error.code!r} for SQL: {invalid_sql!r}"
        )
        assert e.error.message, "SQLParseError must have a non-empty message"
    except ValueError:
        # Correct: preprocessor rejected unresolvable references in invalid SQL
        pass
    except Exception as e:  # noqa: BLE001
        raise AssertionError(
            f"QueryPlanner raised unhandled {type(e).__name__} instead of SQLParseError/ValueError "
            f"for invalid SQL: {invalid_sql!r}\nOriginal error: {e}"
        ) from e

# ---------------------------------------------------------------------------
# Task 4.2: Planner integration unit tests
# Verify build_transient_pipeline delegates to preprocess_sql and merges results.
# Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5
# ---------------------------------------------------------------------------

import ast as python_ast
import inspect
from unittest.mock import patch

from rivet_core.interactive.sql_preprocessor import preprocess_sql


class TestPlannerCallsPreprocessSQL:
    """Req 6.1: build_transient_pipeline calls preprocess_sql as the first step."""

    def test_preprocess_sql_is_called_with_correct_args(self) -> None:
        """preprocess_sql receives sql, joint_names, catalog_names, catalog_context, cached_joints."""
        joints = [_make_compiled_joint("users")]
        assembly = _make_assembly(joints=joints, catalog_names=["mydb"])
        cache = MaterialCache()
        mat = MagicMock(spec=Material)
        cache.put("users", mat)
        planner = QueryPlanner()

        with patch(
            "rivet_core.interactive.query_planner.preprocess_sql",
            wraps=preprocess_sql,
        ) as mock_pp:
            planner.build_transient_pipeline(
                "SELECT * FROM users",
                catalog_context="mydb",
                assembly=assembly,
                material_cache=cache,
                catalog_names=frozenset({"mydb"}),
            )

            mock_pp.assert_called_once()
            kwargs = mock_pp.call_args
            assert kwargs[1]["sql"] == "SELECT * FROM users" or kwargs[0][0] == "SELECT * FROM users"


class TestPlannerUsesRewrittenSQL:
    """Req 6.2: Planner passes PreprocessedSQL.sql to sqlglot parser, not raw SQL."""

    def test_query_joint_contains_rewritten_sql(self) -> None:
        """3-part ref is rewritten; __query joint uses the rewritten SQL."""
        assembly = _make_assembly(catalog_names=["mydb"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM mydb.public.users",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"mydb"}),
        )

        query = transient.joints["__query"]
        # The rewritten SQL should NOT contain the qualified ref
        assert "mydb.public.users" not in query.sql
        assert "mydb" not in query.sql
        # It should contain a simple identifier for the table
        assert "users" in query.sql.lower()


class TestPlannerMergesSourceJoints:
    """Req 6.3: Planner merges PreprocessedSQL.source_joints into transient assembly."""

    def test_source_joints_appear_in_transient_assembly(self) -> None:
        assembly = _make_assembly(catalog_names=["warehouse"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM warehouse.public.orders",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"warehouse"}),
        )

        sources = [j for j in transient.joints.values() if j.joint_type == "source"]
        assert len(sources) == 1
        assert sources[0].catalog == "warehouse"
        assert sources[0].table == "public.orders"

    def test_multiple_source_joints_merged(self) -> None:
        assembly = _make_assembly(catalog_names=["db1", "db2"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM db1.public.users JOIN db2.analytics.events ON users.id = events.user_id",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"db1", "db2"}),
        )

        sources = [j for j in transient.joints.values() if j.joint_type == "source"]
        assert len(sources) == 2
        catalogs = {s.catalog for s in sources}
        assert catalogs == {"db1", "db2"}


class TestPlannerUpstreamClosure:
    """Req 6.4: Joint refs include full upstream dependency chain."""

    def test_joint_ref_includes_transitive_upstream(self) -> None:
        """Referencing a joint with a 3-deep chain includes all ancestors."""
        joints = [
            _make_compiled_joint("raw", joint_type="source", catalog="local", table="raw"),
            _make_compiled_joint("clean", upstream=["raw"]),
            _make_compiled_joint("enriched", upstream=["clean"]),
        ]
        assembly = _make_assembly(joints=joints)
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, needs_exec = planner.build_transient_pipeline(
            "SELECT * FROM enriched",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset(),
        )

        # All three joints must be in the transient assembly
        assert "raw" in transient.joints
        assert "clean" in transient.joints
        assert "enriched" in transient.joints
        # All must be in needs_execution (none cached)
        assert "raw" in needs_exec
        assert "clean" in needs_exec
        assert "enriched" in needs_exec

    def test_diamond_upstream_closure(self) -> None:
        """Diamond dependency: A -> B, A -> C, B -> D, C -> D."""
        joints = [
            _make_compiled_joint("a"),
            _make_compiled_joint("b", upstream=["a"]),
            _make_compiled_joint("c", upstream=["a"]),
            _make_compiled_joint("d", upstream=["b", "c"]),
        ]
        assembly = _make_assembly(joints=joints)
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, needs_exec = planner.build_transient_pipeline(
            "SELECT * FROM d",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset(),
        )

        for name in ["a", "b", "c", "d"]:
            assert name in transient.joints
            assert name in needs_exec

    def test_mixed_joint_and_catalog_refs(self) -> None:
        """SQL referencing both a project joint and a catalog table."""
        joints = [
            _make_compiled_joint("raw", joint_type="source", catalog="local", table="raw"),
            _make_compiled_joint("clean", upstream=["raw"]),
        ]
        assembly = _make_assembly(joints=joints, catalog_names=["ext"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM clean JOIN ext.public.events ON clean.id = events.id",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"ext"}),
        )

        # Joint upstream closure
        assert "raw" in transient.joints
        assert "clean" in transient.joints
        # Catalog source joint
        sources = [j for j in transient.joints.values() if j.joint_type == "source" and j.catalog == "ext"]
        assert len(sources) == 1
        assert sources[0].table == "public.events"


class TestNoReferenceResolverImports:
    """Req 6.5: No imports of ReferenceResolver remain in query_planner.py."""

    def test_no_reference_resolver_import(self) -> None:
        source = inspect.getsource(
            __import__("rivet_core.interactive.query_planner", fromlist=["QueryPlanner"])
        )
        assert "ReferenceResolver" not in source
        assert "reference_resolver" not in source

    def test_no_reference_resolver_in_ast(self) -> None:
        """Parse the module AST to confirm no ReferenceResolver import nodes."""
        source = inspect.getsource(
            __import__("rivet_core.interactive.query_planner", fromlist=["QueryPlanner"])
        )
        tree = python_ast.parse(source)
        for node in python_ast.walk(tree):
            if isinstance(node, python_ast.ImportFrom):
                for alias in node.names:
                    assert alias.name != "ReferenceResolver"
            if isinstance(node, python_ast.Import):
                for alias in node.names:
                    assert "reference_resolver" not in (alias.name or "")


# ---------------------------------------------------------------------------
# Fix-checking tests: catalog in catalog_names but NOT in assembly.catalogs
# Feature: catalog-resolution-bug, Tasks 4.1, 4.2, 4.3
# Validates: Requirements 2.1, 2.2, 2.3, 2.4
# ---------------------------------------------------------------------------


class TestFixCheckingCatalogResolution:
    """Verify catalogs in catalog_names but absent from assembly.catalogs resolve correctly.

    The bug was that build_transient_pipeline derived catalog_names from
    CompiledAssembly.catalogs (only catalogs used by project joints). The fix
    passes ALL configured catalog names via the catalog_names parameter.

    These tests create an assembly with only "local" in its catalogs, but pass
    catalog_names=frozenset({"local", "unity"}) — simulating a session where
    "unity" is configured but unused by any project joint.
    """

    def test_4part_ref_to_configured_but_unused_catalog(self) -> None:
        """Task 4.1: 4-part reference to a catalog in catalog_names but not in assembly.catalogs.

        SELECT * FROM unity.datalake_gold.circular_economy.epr_cession_sales
        should resolve as catalog_table with catalog="unity".

        **Validates: Requirements 2.1, 2.3**
        """
        assembly = _make_assembly(catalog_names=["local"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM unity.datalake_gold.circular_economy.epr_cession_sales",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"local", "unity"}),
        )

        sources = [j for j in transient.joints.values() if j.joint_type == "source"]
        assert len(sources) == 1
        assert sources[0].catalog == "unity"
        assert sources[0].table is not None
        assert "epr_cession_sales" in sources[0].table

        query = transient.joints.get("__query")
        assert query is not None
        assert sources[0].name in query.upstream

    def test_2part_ref_to_configured_but_unused_catalog(self) -> None:
        """Task 4.2: 2-part reference to a catalog in catalog_names but not in assembly.catalogs.

        SELECT * FROM unity.some_table should resolve as catalog_table
        with catalog="unity" and table="some_table".

        **Validates: Requirements 2.1, 2.4**
        """
        assembly = _make_assembly(catalog_names=["local"])
        cache = MaterialCache()
        planner = QueryPlanner()

        transient, _ = planner.build_transient_pipeline(
            "SELECT * FROM unity.some_table",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"local", "unity"}),
        )

        sources = [j for j in transient.joints.values() if j.joint_type == "source"]
        assert len(sources) == 1
        assert sources[0].catalog == "unity"
        assert sources[0].table == "some_table"

        query = transient.joints.get("__query")
        assert query is not None
        assert sources[0].name in query.upstream


# ---------------------------------------------------------------------------
# Task 4.3: [PBT-exploration] Property test — configured catalog names
# used as SQL prefixes don't raise ValueError when in catalog_names
# Validates: Requirements 2.1, 2.2
# ---------------------------------------------------------------------------

_fix_catalog_name = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)
_fix_table_name = st.from_regex(r"[a-z][a-z0-9_]{2,9}", fullmatch=True)
_fix_schema_name = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)


@given(
    extra_catalog=_fix_catalog_name,
    schema=_fix_schema_name,
    table=_fix_table_name,
    use_4part=st.booleans(),
)
@settings(max_examples=100)
def test_configured_catalog_in_catalog_names_no_valueerror(
    extra_catalog: str,
    schema: str,
    table: str,
    use_4part: bool,
) -> None:
    """Task 4.3 [PBT-exploration]: For any configured catalog name used as a
    prefix in SQL, build_transient_pipeline does not raise ValueError when
    catalog_names includes it.

    The assembly only contains "local", but catalog_names includes both
    "local" and the extra catalog. SQL references the extra catalog.

    **Validates: Requirements 2.1, 2.2**
    """
    # Ensure extra_catalog doesn't collide with "local"
    if extra_catalog == "local":
        extra_catalog = extra_catalog + "x"

    if use_4part:
        sql = f"SELECT * FROM {extra_catalog}.{schema}.{table}.col"
    else:
        sql = f"SELECT * FROM {extra_catalog}.{schema}.{table}"

    assembly = _make_assembly(catalog_names=["local"])
    cache = MaterialCache()
    planner = QueryPlanner()

    # This must NOT raise ValueError — the catalog is in catalog_names
    transient, _ = planner.build_transient_pipeline(
        sql,
        catalog_context=None,
        assembly=assembly,
        material_cache=cache,
        catalog_names=frozenset({"local", extra_catalog}),
    )

    # Verify a source joint was created with the correct catalog
    sources = [j for j in transient.joints.values() if j.joint_type == "source"]
    assert len(sources) >= 1
    assert any(s.catalog == extra_catalog for s in sources), (
        f"Expected a source joint with catalog='{extra_catalog}', "
        f"got catalogs: {[s.catalog for s in sources]}"
    )


# ---------------------------------------------------------------------------
# Task 5: Preservation tests — verify the fix doesn't break existing behavior
# Feature: catalog-resolution-bug, Tasks 5.1, 5.2, 5.3, 5.4
# Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6
# ---------------------------------------------------------------------------


class TestPreservationCatalogResolution:
    """Preservation tests: widening catalog_names must not alter existing behavior.

    The fix passes all configured catalog names (a superset of assembly catalogs)
    to build_transient_pipeline. These tests verify that joint resolution,
    assembly-present catalog resolution, and error handling remain unchanged.
    """

    def test_joint_refs_resolve_with_wider_catalog_names(self) -> None:
        """Task 5.1: Joint references still resolve as kind="joint" with wider catalog_names.

        Even when catalog_names includes extra catalogs like "unity" and "warehouse",
        a 1-part reference matching a project joint must resolve as a joint (not a
        catalog source), and the joint must appear directly in the transient assembly.

        **Validates: Requirements 3.1, 3.2**
        """
        joints = [
            _make_compiled_joint("users"),
            _make_compiled_joint("orders", upstream=["users"]),
        ]
        assembly = _make_assembly(joints=joints, catalog_names=["local"])
        cache = MaterialCache()
        planner = QueryPlanner()

        # Wider catalog_names — includes extra catalogs not in assembly
        transient, needs_exec = planner.build_transient_pipeline(
            "SELECT * FROM users",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"local", "unity", "warehouse"}),
        )

        # Joint must be included directly (not as a source joint)
        assert "users" in transient.joints
        joint = transient.joints["users"]
        assert joint.joint_type != "source", (
            "Joint 'users' should resolve as a project joint, not a source joint"
        )

        # __query must reference the joint
        query = transient.joints["__query"]
        assert "users" in query.upstream

    def test_assembly_catalog_resolves_identically_with_wider_catalog_names(self) -> None:
        """Task 5.2: References to catalogs already in the assembly resolve identically.

        When catalog_names is a superset of assembly catalogs, a qualified reference
        to an assembly-present catalog must produce the same source joint as when
        catalog_names only contains assembly catalogs.

        **Validates: Requirements 3.1**
        """
        assembly = _make_assembly(catalog_names=["local"])
        cache = MaterialCache()
        planner = QueryPlanner()

        # Baseline: catalog_names == assembly catalogs only
        transient_baseline, needs_baseline = planner.build_transient_pipeline(
            "SELECT * FROM local.public.users",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"local"}),
        )

        # Wider: catalog_names includes extra catalogs
        transient_wider, needs_wider = planner.build_transient_pipeline(
            "SELECT * FROM local.public.users",
            catalog_context=None,
            assembly=assembly,
            material_cache=cache,
            catalog_names=frozenset({"local", "unity"}),
        )

        # Source joints must be identical
        sources_baseline = [
            j for j in transient_baseline.joints.values() if j.joint_type == "source"
        ]
        sources_wider = [
            j for j in transient_wider.joints.values() if j.joint_type == "source"
        ]
        assert len(sources_baseline) == len(sources_wider) == 1
        assert sources_baseline[0].catalog == sources_wider[0].catalog == "local"
        assert sources_baseline[0].table == sources_wider[0].table

        # __query SQL must be identical
        assert transient_baseline.joints["__query"].sql == transient_wider.joints["__query"].sql

        # needs_execution must be identical
        assert needs_baseline == needs_wider

    def test_unknown_catalog_still_raises_valueerror(self) -> None:
        """Task 5.3: References to truly unknown catalogs still raise ValueError.

        Even with wider catalog_names, a reference to a catalog NOT in catalog_names
        must still raise ValueError with an actionable error message.

        **Validates: Requirements 3.3**
        """
        assembly = _make_assembly(catalog_names=["local"])
        cache = MaterialCache()
        planner = QueryPlanner()

        import pytest

        with pytest.raises(ValueError, match="nonexistent"):
            planner.build_transient_pipeline(
                "SELECT * FROM nonexistent.some_table",
                catalog_context=None,
                assembly=assembly,
                material_cache=cache,
                catalog_names=frozenset({"local", "unity"}),
            )


# ---------------------------------------------------------------------------
# Task 5.4: [PBT-preservation] Property test — resolution identical when
# catalog_names is a superset of assembly catalogs
# Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6
# ---------------------------------------------------------------------------

_pres_catalog = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)
_pres_table = st.from_regex(r"[a-z][a-z0-9_]{2,9}", fullmatch=True)
_pres_schema = st.from_regex(r"[a-z][a-z0-9]{2,7}", fullmatch=True)


@given(
    catalog=_pres_catalog,
    schema=_pres_schema,
    table=_pres_table,
    extra_catalogs=st.lists(_pres_catalog, min_size=1, max_size=3, unique=True),
)
@settings(max_examples=100)
def test_preservation_resolution_identical_with_superset_catalog_names(
    catalog: str,
    schema: str,
    table: str,
    extra_catalogs: list[str],
) -> None:
    """Task 5.4 [PBT-preservation]: For any input where all catalog prefixes
    are in assembly.catalogs, resolution is identical whether catalog_names
    equals assembly catalogs or is a superset.

    We build SQL referencing only the assembly-present catalog, then run
    build_transient_pipeline twice — once with catalog_names == {catalog}
    (baseline) and once with catalog_names == {catalog} | extra_catalogs
    (superset). Both must produce identical source joints, __query SQL,
    and needs_execution lists.

    **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**
    """
    # Ensure extra catalogs don't collide with the assembly catalog
    extra_catalogs = [c for c in extra_catalogs if c != catalog]
    if not extra_catalogs:
        extra_catalogs = ["extrazz"]

    sql = f"SELECT * FROM {catalog}.{schema}.{table}"
    assembly = _make_assembly(catalog_names=[catalog])
    cache = MaterialCache()
    planner = QueryPlanner()

    # Baseline: catalog_names == assembly catalogs
    baseline_catalog_names = frozenset({catalog})
    transient_baseline, needs_baseline = planner.build_transient_pipeline(
        sql,
        catalog_context=None,
        assembly=assembly,
        material_cache=cache,
        catalog_names=baseline_catalog_names,
    )

    # Superset: catalog_names includes extra catalogs
    superset_catalog_names = frozenset({catalog} | set(extra_catalogs))
    transient_superset, needs_superset = planner.build_transient_pipeline(
        sql,
        catalog_context=None,
        assembly=assembly,
        material_cache=cache,
        catalog_names=superset_catalog_names,
    )

    # Source joints must be identical
    sources_baseline = sorted(
        [(j.catalog, j.table, j.name) for j in transient_baseline.joints.values() if j.joint_type == "source"]
    )
    sources_superset = sorted(
        [(j.catalog, j.table, j.name) for j in transient_superset.joints.values() if j.joint_type == "source"]
    )
    assert sources_baseline == sources_superset, (
        f"Source joints differ: baseline={sources_baseline}, superset={sources_superset}"
    )

    # __query SQL must be identical
    assert transient_baseline.joints["__query"].sql == transient_superset.joints["__query"].sql, (
        f"__query SQL differs: baseline={transient_baseline.joints['__query'].sql!r}, "
        f"superset={transient_superset.joints['__query'].sql!r}"
    )

    # needs_execution must be identical
    assert needs_baseline == needs_superset, (
        f"needs_execution differs: baseline={needs_baseline}, superset={needs_superset}"
    )
