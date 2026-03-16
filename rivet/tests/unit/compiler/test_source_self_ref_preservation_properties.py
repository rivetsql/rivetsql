"""Preservation property tests for source self-reference normalization bugfix.

Property 2 (Preservation): Non-source joints, pure source joints, existing
validations, and SQLDecomposer round-trips remain unchanged after the fix.

These tests capture baseline behavior on UNFIXED code and MUST PASS both
before and after the fix is applied.
"""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_bridge.decomposer import SQLDecomposer
from rivet_config import ColumnDecl
from rivet_core.compiler import (
    CompiledJoint,
    _compile_joint,
    _validate_source_inline_transforms,
)
from rivet_core.errors import RivetError
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEnginePlugin,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)
from rivet_core.sql_parser import SQLParser

# ---------------------------------------------------------------------------
# Stubs (same pattern as test_compiler.py)
# ---------------------------------------------------------------------------


class _StubCatalogPlugin(CatalogPlugin):
    type = "stub"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


class _StubEnginePlugin(ComputeEnginePlugin):
    engine_type = "stub"
    supported_catalog_types: dict[str, list[str]] = {
        "stub": ["projection_pushdown", "predicate_pushdown", "limit_pushdown"],
    }

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
        raise NotImplementedError


class _StubSource(SourcePlugin):
    catalog_type = "stub"

    def read(self, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        return None


class _StubSink(SinkPlugin):
    catalog_type = "stub"

    def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
        pass


def _make_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_catalog_plugin(_StubCatalogPlugin())
    reg.register_engine_plugin(_StubEnginePlugin())
    eng = ComputeEngine(name="eng", engine_type="stub")
    reg.register_compute_engine(eng)
    reg.register_source(_StubSource())
    reg.register_sink(_StubSink())
    return reg


_CATALOG_MAP: dict[str, Catalog] = {"c": Catalog(name="c", type="stub")}
_ENGINE_MAP: dict[str, ComputeEngine] = {"eng": ComputeEngine(name="eng", engine_type="stub")}


def _compile(joint: Joint) -> tuple[CompiledJoint, list[RivetError], list[str]]:
    """Compile a single joint with stub infrastructure, no introspection."""
    errors: list[RivetError] = []
    warnings: list[str] = []
    cj = _compile_joint(
        joint,
        _CATALOG_MAP,
        _ENGINE_MAP,
        _make_registry(),
        "eng",
        SQLParser(),
        {},
        errors,
        warnings,
        introspect=False,
    )
    return cj, errors, warnings


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
    ]
)

_COLUMN_NAMES = st.sampled_from(
    ["id", "name", "amount", "status", "created_at", "email", "price", "quantity"]
)

_FILTER_EXPRS = st.sampled_from(
    [
        "status = 'active'",
        "amount > 0",
        "is_deleted = false",
        "created_at > '2024-01-01'",
    ]
)

_NON_SOURCE_TYPES = st.sampled_from(["sql", "sink", "checkpoint", "python"])


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
def non_source_joint(draw: st.DrawFn) -> Joint:
    """Generate a non-source joint (sql, sink, checkpoint, python)."""
    jtype = draw(_NON_SOURCE_TYPES)
    name = draw(_JOINT_NAMES)
    upstream = ["upstream_1"]

    if jtype == "sql":
        sql = "SELECT *, 1 AS flag FROM upstream_1"
        return Joint(name=name, joint_type=jtype, upstream=upstream, sql=sql, engine="eng")
    elif jtype in ("sink", "checkpoint"):
        return Joint(
            name=name,
            joint_type=jtype,
            upstream=upstream,
            catalog="c",
            table=draw(_TABLE_FQNS),
            engine="eng",
        )
    else:  # python
        return Joint(
            name=name,
            joint_type=jtype,
            upstream=upstream,
            function="mymod.transform",
            engine="eng",
        )


@st.composite
def pure_source_joint(draw: st.DrawFn) -> Joint:
    """Generate a source joint with NO inline SQL (no columns, filter, limit, sql)."""
    name = draw(_JOINT_NAMES)
    table = draw(_TABLE_FQNS)
    return Joint(name=name, joint_type="source", catalog="c", table=table, engine="eng")


@st.composite
def simple_self_sql(draw: st.DrawFn) -> str:
    """Generate a simple SQL string using FROM __self with optional WHERE and columns."""
    cols = draw(
        st.one_of(
            st.just("*"),
            st.lists(_COLUMN_NAMES, min_size=1, max_size=3, unique=True).map(
                lambda cs: ", ".join(cs)
            ),
        )
    )
    has_where = draw(st.booleans())
    has_limit = draw(st.booleans())

    sql = f"SELECT {cols} FROM __self"
    if has_where:
        filt = draw(_FILTER_EXPRS)
        sql += f" WHERE {filt}"
    if has_limit:
        limit = draw(st.integers(min_value=1, max_value=1000))
        sql += f" LIMIT {limit}"
    return sql


# ---------------------------------------------------------------------------
# Property 2a: Non-source joints compile identically (unchanged)
# ---------------------------------------------------------------------------


class TestNonSourceJointsUnchanged:
    """Non-source joints (sql, sink, checkpoint, python) compile without
    any behavior change from the __self substitution logic."""

    @given(joint=non_source_joint())
    @settings(max_examples=50)
    def test_non_source_compiles_without_self_errors(self, joint: Joint) -> None:
        cj, errors, warnings = _compile(joint)

        # Core identity preserved
        assert cj.name == joint.name
        assert cj.type == joint.joint_type
        assert cj.engine == "eng"

        # No __self reference should appear in any SQL field
        for sql_field in (cj.sql, cj.sql_translated, cj.sql_resolved):
            if sql_field is not None:
                assert "__self" not in sql_field, (
                    f"Non-source joint {joint.joint_type} has __self in SQL: {sql_field}"
                )

        # Sink/checkpoint get default write_strategy
        if joint.joint_type in ("sink", "checkpoint"):
            assert cj.write_strategy == "replace"

        # Python joints have opaque lineage
        if joint.joint_type == "python":
            assert cj.function == joint.function

    @given(joint=non_source_joint())
    @settings(max_examples=30)
    def test_non_source_no_source_validation_errors(self, joint: Joint) -> None:
        """Non-source joints never produce RVT-760/761/762 source constraint errors."""
        _, errors, _ = _compile(joint)
        source_errors = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
        assert source_errors == [], (
            f"Non-source joint {joint.joint_type} produced source constraint errors: "
            f"{[e.code for e in source_errors]}"
        )


# ---------------------------------------------------------------------------
# Property 2b: Pure source joints (no inline SQL) compile unchanged
# ---------------------------------------------------------------------------


class TestPureSourceJointsUnchanged:
    """Source joints without inline SQL compile as pure sources referencing
    the table FQN directly, with no SQL or logical plan."""

    @given(joint=pure_source_joint())
    @settings(max_examples=50)
    def test_pure_source_has_no_sql(self, joint: Joint) -> None:
        cj, errors, _ = _compile(joint)

        assert cj.type == "source"
        assert cj.sql is None
        assert cj.sql_translated is None
        assert cj.logical_plan is None
        assert cj.table == joint.table

        # No compilation errors for a valid pure source
        fatal = [e for e in errors if e.code not in ("RVT-753",)]
        assert fatal == [], f"Pure source produced errors: {[e.code for e in fatal]}"

    @given(joint=pure_source_joint())
    @settings(max_examples=30)
    def test_pure_source_no_source_constraint_errors(self, joint: Joint) -> None:
        """Pure source joints never trigger single-table constraint errors."""
        _, errors, _ = _compile(joint)
        source_errors = [e for e in errors if e.code in ("RVT-760", "RVT-761", "RVT-762")]
        assert source_errors == [], (
            f"Pure source produced constraint errors: {[e.code for e in source_errors]}"
        )


# ---------------------------------------------------------------------------
# Property 2c: Single-table constraint validations still reject invalid SQL
# ---------------------------------------------------------------------------


class TestSourceValidationsPreserved:
    """Existing single-table constraint validations (RVT-760, RVT-761, RVT-762)
    continue to reject JOINs, CTEs, and subqueries in source SQL."""

    _JOIN_SQLS = st.sampled_from(
        [
            "SELECT a.id FROM orders a JOIN users b ON a.user_id = b.id",
            "SELECT * FROM orders, users WHERE orders.id = users.order_id",
        ]
    )

    _CTE_SQLS = st.sampled_from(
        [
            "WITH tmp AS (SELECT 1 AS x) SELECT * FROM tmp",
            "WITH a AS (SELECT id FROM orders) SELECT * FROM a",
        ]
    )

    _SUBQUERY_SQLS = st.sampled_from(
        [
            "SELECT * FROM orders WHERE id IN (SELECT id FROM users)",
            "SELECT * FROM (SELECT 1 AS x) AS sub",
        ]
    )

    @given(sql=_JOIN_SQLS)
    @settings(max_examples=10)
    def test_join_produces_rvt_760(self, sql: str) -> None:
        parser = SQLParser()
        ast = parser.parse(sql)
        ast = parser.normalize(ast)
        lp = parser.extract_logical_plan(ast)
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        codes = [e.code for e in errors]
        assert "RVT-760" in codes, f"JOIN SQL did not produce RVT-760: {codes}"

    @given(sql=_CTE_SQLS)
    @settings(max_examples=10)
    def test_cte_produces_rvt_761(self, sql: str) -> None:
        parser = SQLParser()
        ast = parser.parse(sql)
        ast = parser.normalize(ast)
        lp = parser.extract_logical_plan(ast)
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        codes = [e.code for e in errors]
        assert "RVT-761" in codes, f"CTE SQL did not produce RVT-761: {codes}"

    @given(sql=_SUBQUERY_SQLS)
    @settings(max_examples=10)
    def test_subquery_produces_rvt_762(self, sql: str) -> None:
        parser = SQLParser()
        ast = parser.parse(sql)
        ast = parser.normalize(ast)
        lp = parser.extract_logical_plan(ast)
        errors: list[RivetError] = []
        _validate_source_inline_transforms("src", lp, None, errors, [], sql=sql)
        codes = [e.code for e in errors]
        assert "RVT-762" in codes, f"Subquery SQL did not produce RVT-762: {codes}"


# ---------------------------------------------------------------------------
# Property 2d: SQLDecomposer round-trip with FROM __self
# ---------------------------------------------------------------------------


class TestSQLDecomposerSelfRoundTrip:
    """SQLDecomposer.can_decompose() returns True and decompose() extracts
    __self as the table reference for simple SQL using FROM __self."""

    @given(sql=simple_self_sql())
    @settings(max_examples=50)
    def test_can_decompose_self_sql(self, sql: str) -> None:
        decomposer = SQLDecomposer()
        assert decomposer.can_decompose(sql), f"SQLDecomposer rejected simple __self SQL: {sql}"

    @given(sql=simple_self_sql())
    @settings(max_examples=50)
    def test_decompose_extracts_self_as_table(self, sql: str) -> None:
        decomposer = SQLDecomposer()
        cols, filt, table_name, limit = decomposer.decompose(sql)
        assert table_name == "__self", f"Expected table='__self', got '{table_name}' for SQL: {sql}"

    @given(
        cols=st.lists(_COLUMN_NAMES, min_size=1, max_size=3, unique=True),
        filt=st.one_of(st.none(), _FILTER_EXPRS),
        limit=st.one_of(st.none(), st.integers(min_value=1, max_value=1000)),
    )
    @settings(max_examples=50)
    def test_decompose_round_trip_fields(
        self,
        cols: list[str],
        filt: str | None,
        limit: int | None,
    ) -> None:
        """Generate SQL from parts, decompose it, verify extracted fields match."""
        col_str = ", ".join(cols)
        sql = f"SELECT {col_str} FROM __self"
        if filt:
            sql += f" WHERE {filt}"
        if limit is not None:
            sql += f" LIMIT {limit}"

        decomposer = SQLDecomposer()
        dec_cols, dec_filt, dec_table, dec_limit = decomposer.decompose(sql)

        assert dec_table == "__self"
        assert dec_limit == limit

        # Columns should match
        assert dec_cols is not None
        dec_col_names = [c.name for c in dec_cols]
        assert dec_col_names == cols, f"Expected cols {cols}, got {dec_col_names}"

        # Filter presence should match
        if filt is None:
            assert dec_filt is None
        else:
            assert dec_filt is not None

    def test_decompose_star_from_self(self) -> None:
        """SELECT * FROM __self decomposes to cols=None, table=__self."""
        decomposer = SQLDecomposer()
        cols, filt, table, limit = decomposer.decompose("SELECT * FROM __self")
        assert cols is None
        assert filt is None
        assert table == "__self"
        assert limit is None
