"""Tests for the SQL namespace preprocessor.

Covers property tests (tasks 2.5, 2.6, 2.7) and unit tests (task 2.8).
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest
import sqlglot
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from rivet_core.interactive.sql_preprocessor import (
    TableRefSpan,
    preprocess_sql,
)
from rivet_core.introspection import CatalogNode

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# SQL keywords that sqlglot tokenizes specially — must be excluded from generated identifiers
_SQL_KEYWORDS = frozenset({
    "as", "on", "in", "is", "or", "by", "if", "do", "no", "of", "to", "at",
    "all", "and", "any", "asc", "end", "for", "int", "key", "map", "not",
    "set", "top", "div", "row", "add",
    "case", "cast", "copy", "cube", "date", "desc", "drop", "else", "enum",
    "exec", "from", "full", "into", "join", "kill", "left", "like", "load",
    "lock", "next", "null", "only", "open", "over", "rows", "show", "some",
    "then", "time", "true", "type", "view", "when", "with",
    "alter", "begin", "cache", "check", "cross", "false", "fetch", "first",
    "grant", "group", "index", "inner", "limit", "merge", "order", "outer",
    "pivot", "range", "right", "table", "union", "using", "where", "while",
    "column", "commit", "create", "cursor", "delete", "escape", "except",
    "exists", "filter", "format", "having", "insert", "keep", "offset",
    "rename", "return", "revoke", "schema", "select", "unique", "update",
    "values", "window",
    "between", "collate", "comment", "default", "execute", "natural",
    "percent", "primary", "replace", "returns", "rollback", "unpivot",
    "volatile",
    "database", "describe", "distinct", "function", "overwrite", "settings",
    "temporary", "procedure",
})

_ident = st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True).filter(
    lambda s: s not in _SQL_KEYWORDS
)


def _make_explorer(tables_by_catalog: dict[str, list[tuple[str, str]]]):
    """Build a mock CatalogExplorer with _tables_cache and _connection_status."""
    cache: dict[str, list[CatalogNode]] = {}
    for cat, entries in tables_by_catalog.items():
        nodes = []
        for schema, table in entries:
            nodes.append(CatalogNode(
                name=table,
                node_type="table",
                path=[cat, schema, table],
                is_container=False,
                children_count=None,
                summary=None,
            ))
        cache[cat] = nodes
    explorer = SimpleNamespace(
        _tables_cache=cache,
        _connection_status={cat: (True, None) for cat in tables_by_catalog},
    )
    return explorer


# ---------------------------------------------------------------------------
# Task 2.5: Property tests for PreprocessedSQL and preprocessor core
# ---------------------------------------------------------------------------


class TestPreprocessedSQLAndCore:
    """Property tests for PreprocessedSQL and preprocessor core (task 2.5)."""

    # Feature: sql-namespace-preprocessor, Property 1: Output SQL contains only simple identifiers in table positions
    @given(
        catalog=_ident,
        schema=_ident,
        table=_ident,
    )
    @settings(max_examples=100)
    def test_output_sql_simple_identifiers(self, catalog: str, schema: str, table: str) -> None:
        """After preprocessing, sqlglot parse produces no dotted table refs."""
        sql = f"SELECT * FROM {catalog}.{schema}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        ast = sqlglot.parse_one(result.sql)
        for t in ast.find_all(sqlglot.exp.Table):
            assert t.catalog is None or t.catalog == ""
            assert t.db is None or t.db == ""

    # Feature: sql-namespace-preprocessor, Property 2: Joint-only SQL identity
    @given(joint_name=_ident)
    @settings(max_examples=100)
    def test_joint_only_sql_identity(self, joint_name: str) -> None:
        """Joint-only SQL is returned unchanged."""
        sql = f"SELECT * FROM {joint_name}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({joint_name}),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        assert result.sql == sql
        assert result.source_joints == []
        assert all(r.kind == "joint" for r in result.resolved_refs.values())

    # Feature: sql-namespace-preprocessor, Property 5: Source joint name uniqueness
    @given(
        joint_name=_ident,
        catalog=_ident,
        table=_ident,
    )
    @settings(max_examples=100)
    def test_source_joint_name_uniqueness(self, joint_name: str, catalog: str, table: str) -> None:
        """Source joint names don't collide with joint_names or each other."""
        assume(joint_name != catalog)
        sql = f"SELECT * FROM {catalog}.public.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({joint_name}),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        source_names = {j.name for j in result.source_joints}
        assert source_names.isdisjoint({joint_name})
        assert len(source_names) == len(result.source_joints)

    # Feature: sql-namespace-preprocessor, Property 10: No-reference SQL identity
    def test_no_reference_sql_identity(self) -> None:
        """SQL with no table refs is returned unchanged."""
        sql = "SELECT 1 + 2"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        assert result.sql == sql
        assert result.source_joints == []
        assert result.resolved_refs == {}

    # Feature: sql-namespace-preprocessor, Property 18: Determinism
    @given(
        catalog=_ident,
        schema=_ident,
        table=_ident,
    )
    @settings(max_examples=100)
    def test_determinism(self, catalog: str, schema: str, table: str) -> None:
        """Calling preprocess_sql twice with same args gives identical results."""
        sql = f"SELECT * FROM {catalog}.{schema}.{table}"
        kwargs = dict(
            sql=sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        r1 = preprocess_sql(**kwargs)
        r2 = preprocess_sql(**kwargs)
        assert r1.sql == r2.sql
        assert len(r1.source_joints) == len(r2.source_joints)
        assert r1.resolved_refs == r2.resolved_refs


# ---------------------------------------------------------------------------
# Task 2.6: Property tests for resolution logic
# ---------------------------------------------------------------------------


class TestResolutionLogic:
    """Property tests for resolution logic (task 2.6)."""

    # Feature: sql-namespace-preprocessor, Property 3: Resolution priority by part count
    @given(name=_ident, catalog=_ident)
    @settings(max_examples=100)
    def test_1part_joint_wins(self, name: str, catalog: str) -> None:
        """1-part ref matching joint_names resolves as joint."""
        sql = f"SELECT * FROM {name}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({name}),
            catalog_names=frozenset({catalog}),
            catalog_context=catalog,
        )
        assert result.resolved_refs[name].kind == "joint"
        assert result.resolved_refs[name].joint_name == name

    @given(name=_ident, catalog=_ident)
    @settings(max_examples=100)
    def test_1part_catalog_context_fallback(self, name: str, catalog: str) -> None:
        """1-part ref not matching joints falls back to catalog_context."""
        assume(name != catalog)
        sql = f"SELECT * FROM {name}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset(),
            catalog_context=catalog,
        )
        ref = result.resolved_refs[name]
        assert ref.kind == "catalog_table"
        assert ref.catalog == catalog
        assert ref.table == name

    @given(catalog=_ident, table=_ident)
    @settings(max_examples=100)
    def test_2part_catalog_prefix(self, catalog: str, table: str) -> None:
        """2-part ref where first part is catalog name."""
        assume(catalog != table)
        sql = f"SELECT * FROM {catalog}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        ref = result.resolved_refs[f"{catalog}.{table}"]
        assert ref.kind == "catalog_table"
        assert ref.catalog == catalog
        assert ref.table == table

    @given(schema=_ident, table=_ident, catalog=_ident)
    @settings(max_examples=100)
    def test_2part_schema_table_with_context(self, schema: str, table: str, catalog: str) -> None:
        """2-part ref where first part is not a catalog uses catalog_context. (Req 1.6)"""
        assume(schema != catalog and schema != table)
        sql = f"SELECT * FROM {schema}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=catalog,
        )
        ref = result.resolved_refs[f"{schema}.{table}"]
        assert ref.kind == "catalog_table"
        assert ref.catalog == catalog
        assert ref.schema == schema
        assert ref.table == table

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_3part_resolution(self, catalog: str, schema: str, table: str) -> None:
        """3-part ref resolves as catalog.schema.table."""
        sql = f"SELECT * FROM {catalog}.{schema}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        ref = result.resolved_refs[f"{catalog}.{schema}.{table}"]
        assert ref.kind == "catalog_table"
        assert ref.catalog == catalog
        assert ref.schema == schema
        assert ref.table == table

    @given(catalog=_ident, p2=_ident, p3=_ident, table=_ident)
    @settings(max_examples=100)
    def test_4plus_part_catalog_prefix(self, catalog: str, p2: str, p3: str, table: str) -> None:
        """4+ part ref with first part in catalog_names decomposes correctly. (Req 1.8)"""
        sql = f"SELECT * FROM {catalog}.{p2}.{p3}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        ref_str = f"{catalog}.{p2}.{p3}.{table}"
        ref = result.resolved_refs[ref_str]
        assert ref.kind == "catalog_table"
        assert ref.catalog == catalog
        assert ref.schema == f"{p2}.{p3}"
        assert ref.table == table

    # Feature: sql-namespace-preprocessor, Property 4: Source joint field correctness
    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_source_joint_fields(self, catalog: str, schema: str, table: str) -> None:
        """Source joints have correct joint_type, catalog, and table fields."""
        sql = f"SELECT * FROM {catalog}.{schema}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        assert len(result.source_joints) == 1
        sj = result.source_joints[0]
        assert sj.joint_type == "source"
        assert sj.catalog == catalog
        assert sj.table == f"{schema}.{table}"

    @given(catalog=_ident, table=_ident)
    @settings(max_examples=100)
    def test_source_joint_fields_no_schema(self, catalog: str, table: str) -> None:
        """Source joint without schema has just table name. (Req 2.5)"""
        assume(catalog != table)
        sql = f"SELECT * FROM {catalog}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        assert len(result.source_joints) == 1
        sj = result.source_joints[0]
        assert sj.joint_type == "source"
        assert sj.catalog == catalog
        assert sj.table == table

    # Feature: sql-namespace-preprocessor, Property 6: Joint references produce no source joints
    @given(j1=_ident, j2=_ident)
    @settings(max_examples=100)
    def test_joint_refs_no_source_joints(self, j1: str, j2: str) -> None:
        """Joint-only refs produce no source joints."""
        assume(j1 != j2)
        sql = f"SELECT * FROM {j1} JOIN {j2} ON 1=1"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({j1, j2}),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        assert result.source_joints == []

    # Feature: sql-namespace-preprocessor, Property 9: Path/URI resolution correctness
    def test_path_resolution(self) -> None:
        """IDENT."quoted" resolves with catalog and opaque path."""
        sql = 'SELECT * FROM my_fs."/data/orders.csv"'
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({"my_fs"}),
            catalog_context=None,
        )
        ref_key = [k for k in result.resolved_refs if "my_fs" in k][0]
        ref = result.resolved_refs[ref_key]
        assert ref.kind == "catalog_table"
        assert ref.catalog == "my_fs"
        assert ref.table == "/data/orders.csv"

    def test_uri_resolution(self) -> None:
        """IDENT."s3://..." resolves with catalog and opaque URI. (Req 4.3)"""
        sql = 'SELECT * FROM my_s3."s3://bucket/prefix/file.parquet"'
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({"my_s3"}),
            catalog_context=None,
        )
        ref_key = [k for k in result.resolved_refs if "my_s3" in k][0]
        ref = result.resolved_refs[ref_key]
        assert ref.kind == "catalog_table"
        assert ref.catalog == "my_s3"
        assert ref.table == "s3://bucket/prefix/file.parquet"

    # Feature: sql-namespace-preprocessor, Property 11: Error messages include original reference string
    def test_error_includes_original_ref(self) -> None:
        """ValueError includes the original reference string."""
        with pytest.raises(ValueError, match="unknown_table"):
            preprocess_sql(
                "SELECT * FROM unknown_table",
                joint_names=frozenset(),
                catalog_names=frozenset(),
                catalog_context=None,
            )

    def test_error_4part_unknown_catalog(self) -> None:
        """4+ part ref with unknown catalog lists known catalogs."""
        with pytest.raises(ValueError, match="bad_cat") as exc_info:
            preprocess_sql(
                "SELECT * FROM bad_cat.a.b.c",
                joint_names=frozenset(),
                catalog_names=frozenset({"my_pg"}),
                catalog_context=None,
            )
        assert "my_pg" in str(exc_info.value)

    def test_error_ambiguous_fuzzy_lists_candidates(self) -> None:
        """Ambiguous fuzzy match error includes original ref and candidate FQNs. (Req 9.2)"""
        explorer = _make_explorer({
            "my_pg": [("public", "users")],
            "my_unity": [("default", "users")],
        })
        with pytest.raises(ValueError, match="users") as exc_info:
            preprocess_sql(
                "SELECT * FROM users",
                joint_names=frozenset(),
                catalog_names=frozenset(),
                catalog_context=None,
                catalog_explorer=explorer,
            )
        msg = str(exc_info.value)
        assert "my_pg" in msg
        assert "my_unity" in msg

    # Feature: sql-namespace-preprocessor, Property 12: Fuzzy resolution correctness
    def test_fuzzy_single_match(self) -> None:
        """Fuzzy resolution with exactly one match resolves correctly."""
        explorer = _make_explorer({"my_pg": [("public", "users")]})
        result = preprocess_sql(
            "SELECT * FROM users",
            joint_names=frozenset(),
            catalog_names=frozenset(),
            catalog_context=None,
            catalog_explorer=explorer,
        )
        ref = result.resolved_refs["users"]
        assert ref.kind == "catalog_table"
        assert ref.catalog == "my_pg"
        assert ref.table == "users"

    def test_fuzzy_multiple_matches_error(self) -> None:
        """Fuzzy resolution with multiple matches raises ValueError."""
        explorer = _make_explorer({
            "my_pg": [("public", "users")],
            "my_unity": [("default", "users")],
        })
        with pytest.raises(ValueError, match="users"):
            preprocess_sql(
                "SELECT * FROM users",
                joint_names=frozenset(),
                catalog_names=frozenset(),
                catalog_context=None,
                catalog_explorer=explorer,
            )

    def test_fuzzy_zero_matches_error(self) -> None:
        """Fuzzy resolution with zero matches raises ValueError."""
        explorer = _make_explorer({"my_pg": [("public", "orders")]})
        with pytest.raises(ValueError, match="users"):
            preprocess_sql(
                "SELECT * FROM users",
                joint_names=frozenset(),
                catalog_names=frozenset(),
                catalog_context=None,
                catalog_explorer=explorer,
            )

    # Feature: sql-namespace-preprocessor, Property 13: Untokenizable SQL passthrough
    def test_untokenizable_passthrough(self) -> None:
        """Untokenizable SQL returns unchanged. (Req 9.4)"""
        # sqlglot tokenizer is very permissive; use an unclosed string literal
        sql = "SELECT 'unclosed string"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        assert result.sql == sql
        assert result.source_joints == []
        assert result.resolved_refs == {}


# ---------------------------------------------------------------------------
# Task 2.7: Property tests for SQL preservation and cross-catalog
# ---------------------------------------------------------------------------


class TestSQLPreservationAndCrossCatalog:
    """Property tests for SQL preservation and cross-catalog (task 2.7).

    Validates: Requirements 2.4, 3.5, 10.1, 10.2, 12.1, 12.3, 12.4, 12.6, 13.2, 13.4
    """

    # Feature: sql-namespace-preprocessor, Property 7: Same-table deduplication
    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_same_table_deduplication(self, catalog: str, schema: str, table: str) -> None:
        """Multiple refs to same catalog table produce exactly one source joint,
        and all occurrences use the same replacement name. (Req 2.4)"""
        sql = f"SELECT * FROM {catalog}.{schema}.{table} JOIN {catalog}.{schema}.{table} AS u2 ON 1=1"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        # Exactly one source joint for the deduplicated table
        assert len(result.source_joints) == 1
        sj_name = result.source_joints[0].name
        # The replacement name appears at least twice in the output SQL (both occurrences)
        assert result.sql.count(sj_name) >= 2

    # Feature: sql-namespace-preprocessor, Property 8: Non-table-reference SQL preservation
    @given(catalog=_ident, schema=_ident, table=_ident, alias=_ident)
    @settings(max_examples=100)
    def test_aliases_preserved(self, catalog: str, schema: str, table: str, alias: str) -> None:
        """Table aliases are preserved in output SQL. (Req 3.5, 12.3)"""
        assume(alias != catalog and alias != schema and alias != table)
        sql = f"SELECT * FROM {catalog}.{schema}.{table} AS {alias}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        assert f"AS {alias}" in result.sql or f"as {alias}" in result.sql.lower()

    @given(literal=st.from_regex(r"[a-z][a-z0-9. _]{0,20}", fullmatch=True))
    @settings(max_examples=100)
    def test_string_literals_preserved(self, literal: str) -> None:
        """String literals are not modified. (Req 12.1)"""
        sql = f"SELECT '{literal}' FROM my_joint"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({"my_joint"}),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        assert f"'{literal}'" in result.sql

    def test_comments_preserved(self) -> None:
        """Comments are not modified. (Req 12.6)"""
        sql = "SELECT * FROM my_joint -- my_pg.public.users"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({"my_joint"}),
            catalog_names=frozenset({"my_pg"}),
            catalog_context=None,
        )
        assert "-- my_pg.public.users" in result.sql

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_subquery_structure_preserved(self, catalog: str, schema: str, table: str) -> None:
        """Subquery structure is preserved. (Req 3.5)"""
        sql = f"SELECT * FROM (SELECT * FROM {catalog}.{schema}.{table})"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        assert result.sql.startswith("SELECT * FROM (SELECT * FROM ")
        assert result.sql.endswith(")")

    # Feature: sql-namespace-preprocessor, Property 14: Cross-catalog source joint independence
    @given(cat1=_ident, cat2=_ident, table1=_ident, table2=_ident)
    @settings(max_examples=100)
    def test_cross_catalog_independence(self, cat1: str, cat2: str, table1: str, table2: str) -> None:
        """Refs to N distinct catalogs produce at least one source joint per catalog,
        each with correct catalog field. (Req 10.1, 10.2)"""
        assume(cat1 != cat2)
        sql = f"SELECT * FROM {cat1}.public.{table1} JOIN {cat2}.public.{table2} ON 1=1"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({cat1, cat2}),
            catalog_context=None,
        )
        catalogs = {j.catalog for j in result.source_joints}
        assert cat1 in catalogs
        assert cat2 in catalogs
        # Each source joint's catalog matches the catalog it was resolved from
        for sj in result.source_joints:
            assert sj.catalog in {cat1, cat2}

    @given(catalog=_ident, table=_ident, joint=_ident)
    @settings(max_examples=100)
    def test_mixed_joint_and_catalog_refs(self, catalog: str, table: str, joint: str) -> None:
        """Mixed joint + catalog refs: joints produce no source joints, catalogs do. (Req 10.2)"""
        assume(joint != catalog and joint != table)
        sql = f"SELECT * FROM {joint} JOIN {catalog}.public.{table} ON 1=1"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({joint}),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        assert result.resolved_refs[joint].kind == "joint"
        cat_ref = f"{catalog}.public.{table}"
        assert result.resolved_refs[cat_ref].kind == "catalog_table"
        # Only catalog ref produces a source joint
        assert len(result.source_joints) == 1
        assert result.source_joints[0].catalog == catalog

    # Feature: sql-namespace-preprocessor, Property 15: Round-trip reconstruction
    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_round_trip_reconstruction(self, catalog: str, schema: str, table: str) -> None:
        """resolved_refs + source_joints contain full catalog/schema/table info. (Req 13.2)"""
        sql = f"SELECT * FROM {catalog}.{schema}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        ref = result.resolved_refs[f"{catalog}.{schema}.{table}"]
        assert ref.catalog == catalog
        assert ref.schema == schema
        assert ref.table == table
        # Source joint also carries the info
        assert len(result.source_joints) == 1
        assert result.source_joints[0].catalog == catalog
        assert result.source_joints[0].table == f"{schema}.{table}"

    # Feature: sql-namespace-preprocessor, Property 16: Source joint name ↔ SQL consistency
    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_source_joint_name_sql_consistency(self, catalog: str, schema: str, table: str) -> None:
        """Source joint names in SQL exactly equal names in source_joints list. (Req 13.4)"""
        sql = f"SELECT * FROM {catalog}.{schema}.{table}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({catalog}),
            catalog_context=None,
        )
        source_names = {j.name for j in result.source_joints}
        for name in source_names:
            assert name in result.sql

    @given(cat1=_ident, cat2=_ident, t1=_ident, t2=_ident)
    @settings(max_examples=100)
    def test_source_joint_name_sql_consistency_multi(self, cat1: str, cat2: str, t1: str, t2: str) -> None:
        """Multi-table: source joint names in SQL match source_joints list. (Req 13.4)"""
        assume(cat1 != cat2)
        sql = f"SELECT * FROM {cat1}.public.{t1} JOIN {cat2}.public.{t2} ON 1=1"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({cat1, cat2}),
            catalog_context=None,
        )
        source_names = {j.name for j in result.source_joints}
        for name in source_names:
            assert name in result.sql

    # Feature: sql-namespace-preprocessor, Property 17: CTE names excluded from resolution
    @given(cte_name=_ident)
    @settings(max_examples=100)
    def test_cte_names_excluded(self, cte_name: str) -> None:
        """CTE names are not treated as table references. (Req 12.4)"""
        sql = f"WITH {cte_name} AS (SELECT 1) SELECT * FROM {cte_name}"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        assert cte_name not in result.resolved_refs
        assert result.source_joints == []

    def test_cte_name_not_confused_with_catalog_table(self) -> None:
        """CTE name matching a catalog name is still excluded. (Req 12.4)"""
        sql = "WITH my_pg AS (SELECT 1) SELECT * FROM my_pg"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({"my_pg"}),
            catalog_context=None,
        )
        assert "my_pg" not in result.resolved_refs
        assert result.source_joints == []


# ---------------------------------------------------------------------------
# Task 2.8: Unit tests for edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Unit tests for edge cases (task 2.8)."""

    def test_frozen_dataclass_immutability(self) -> None:
        """PreprocessedSQL is immutable."""
        result = preprocess_sql(
            "SELECT 1",
            joint_names=frozenset(),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        with pytest.raises(FrozenInstanceError):
            result.sql = "SELECT 2"  # type: ignore[misc]

    def test_tablerefspan_frozen(self) -> None:
        """TableRefSpan is immutable."""
        span = TableRefSpan(ref_str="a", parts=["a"], start_offset=0, end_offset=1, is_path_ref=False)
        with pytest.raises(FrozenInstanceError):
            span.ref_str = "b"  # type: ignore[misc]

    def test_4part_ref_decomposition(self) -> None:
        """4-part ref: my_unity.main.default.users."""
        result = preprocess_sql(
            "SELECT * FROM my_unity.main.default.users",
            joint_names=frozenset(),
            catalog_names=frozenset({"my_unity"}),
            catalog_context=None,
        )
        ref = result.resolved_refs["my_unity.main.default.users"]
        assert ref.catalog == "my_unity"
        assert ref.schema == "main.default"
        assert ref.table == "users"

    def test_5part_ref(self) -> None:
        """5-part ref: my_unity.main.default.schema.users."""
        result = preprocess_sql(
            "SELECT * FROM my_unity.main.default.schema.users",
            joint_names=frozenset(),
            catalog_names=frozenset({"my_unity"}),
            catalog_context=None,
        )
        ref = result.resolved_refs["my_unity.main.default.schema.users"]
        assert ref.catalog == "my_unity"
        assert ref.schema == "main.default.schema"
        assert ref.table == "users"

    def test_path_with_dots(self) -> None:
        """Path with dots: my_fs."subdir/orders.v2.parquet" is opaque."""
        sql = 'SELECT * FROM my_fs."subdir/orders.v2.parquet"'
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({"my_fs"}),
            catalog_context=None,
        )
        ref_key = [k for k in result.resolved_refs if "my_fs" in k][0]
        ref = result.resolved_refs[ref_key]
        assert ref.table == "subdir/orders.v2.parquet"

    def test_collision_avoidance(self) -> None:
        """Ref 'users' when 'users' already in joint_names → __src_users."""
        result = preprocess_sql(
            "SELECT * FROM my_pg.public.users",
            joint_names=frozenset({"users"}),
            catalog_names=frozenset({"my_pg"}),
            catalog_context=None,
        )
        assert result.source_joints[0].name == "__src_users"
        assert "__src_users" in result.sql

    def test_mixed_query(self) -> None:
        """Mixed query: joint ref + catalog ref."""
        sql = "SELECT j.*, c.* FROM my_joint j JOIN my_pg.public.users c ON j.id = c.id"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({"my_joint"}),
            catalog_names=frozenset({"my_pg"}),
            catalog_context=None,
        )
        assert result.resolved_refs["my_joint"].kind == "joint"
        assert result.resolved_refs["my_pg.public.users"].kind == "catalog_table"
        assert len(result.source_joints) == 1
        assert "my_joint" in result.sql

    def test_cte_scope_in_subquery(self) -> None:
        """CTE ref in subquery is not resolved as external table."""
        sql = "WITH cte AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM cte)"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        assert "cte" not in result.resolved_refs
        assert result.source_joints == []

    def test_multiple_from_tables(self) -> None:
        """FROM t1, t2 — both are table refs."""
        sql = "SELECT * FROM my_pg.public.users, my_pg.public.orders"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({"my_pg"}),
            catalog_context=None,
        )
        assert len(result.source_joints) == 2

    def test_insert_into(self) -> None:
        """INSERT INTO table is in table position."""
        sql = "INSERT INTO my_pg.public.users VALUES (1)"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({"my_pg"}),
            catalog_context=None,
        )
        assert "my_pg.public.users" in result.resolved_refs

    def test_select_no_from(self) -> None:
        """SELECT without FROM has no table refs."""
        sql = "SELECT 1 + 2, 'hello'"
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        assert result.sql == sql
        assert result.resolved_refs == {}

    def test_column_refs_not_resolved(self) -> None:
        """Column refs in SELECT/WHERE are not treated as table refs."""
        sql = "SELECT a.b FROM my_joint a WHERE a.c = 1"
        result = preprocess_sql(
            sql,
            joint_names=frozenset({"my_joint"}),
            catalog_names=frozenset(),
            catalog_context=None,
        )
        # Only my_joint should be resolved, not a.b or a.c
        assert "my_joint" in result.resolved_refs
        assert len(result.resolved_refs) == 1

    def test_backtick_quoting_path(self) -> None:
        """Backtick quoting resolves same as double-quote. (Req 4.5)"""
        sql_backtick = 'SELECT * FROM my_fs.`/data/orders.csv`'
        sql_double = 'SELECT * FROM my_fs."/data/orders.csv"'
        kwargs = dict(
            joint_names=frozenset(),
            catalog_names=frozenset({"my_fs"}),
            catalog_context=None,
        )
        r_bt = preprocess_sql(sql_backtick, **kwargs)
        r_dq = preprocess_sql(sql_double, **kwargs)
        # Both should resolve to the same catalog table
        bt_ref = list(r_bt.resolved_refs.values())[0]
        dq_ref = list(r_dq.resolved_refs.values())[0]
        assert bt_ref.kind == dq_ref.kind == "catalog_table"
        assert bt_ref.catalog == dq_ref.catalog == "my_fs"
        assert bt_ref.table == dq_ref.table == "/data/orders.csv"

    def test_quoted_identifiers_in_dotted_ref(self) -> None:
        """Quoted parts in dotted refs resolve correctly. (Req 12.2)"""
        sql = 'SELECT * FROM "my_unity"."main"."users"'
        result = preprocess_sql(
            sql,
            joint_names=frozenset(),
            catalog_names=frozenset({"my_unity"}),
            catalog_context=None,
        )
        ref = list(result.resolved_refs.values())[0]
        assert ref.kind == "catalog_table"
        assert ref.catalog == "my_unity"
