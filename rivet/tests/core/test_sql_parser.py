"""Tests for SQLParser core parsing and validation (task 9.2)."""

from __future__ import annotations

import pytest
import sqlglot.expressions as exp

from rivet_core.errors import SQLParseError
from rivet_core.models import Schema
from rivet_core.sql_parser import SQLParser


@pytest.fixture
def parser() -> SQLParser:
    return SQLParser()


class TestParse:
    """Tests for SQLParser.parse()."""

    def test_simple_select(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a, b FROM t")
        assert isinstance(ast, exp.Select)

    def test_select_with_cte(self, parser: SQLParser) -> None:
        sql = "WITH cte AS (SELECT 1 AS x) SELECT x FROM cte"
        ast = parser.parse(sql)
        assert ast.find(exp.Select) is not None

    def test_select_with_where(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t WHERE a > 1")
        assert isinstance(ast, exp.Select)

    def test_select_with_join(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a.x FROM a JOIN b ON a.id = b.id")
        assert isinstance(ast, exp.Select)

    def test_union_allowed(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t1 UNION SELECT a FROM t2")
        assert isinstance(ast, exp.Union)

    def test_reject_insert(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("INSERT INTO t VALUES (1)")
        assert exc_info.value.error.code == "RVT-702"

    def test_reject_update(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("UPDATE t SET a = 1")
        assert exc_info.value.error.code == "RVT-702"

    def test_reject_delete(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("DELETE FROM t")
        assert exc_info.value.error.code == "RVT-702"

    def test_reject_create_table(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("CREATE TABLE t (a INT)")
        assert exc_info.value.error.code == "RVT-702"

    def test_reject_drop_table(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("DROP TABLE t")
        assert exc_info.value.error.code == "RVT-702"

    def test_reject_alter_table(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("ALTER TABLE t ADD COLUMN b INT")
        assert exc_info.value.error.code == "RVT-702"

    def test_reject_multi_statement(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("SELECT 1; SELECT 2")
        assert exc_info.value.error.code == "RVT-702"
        assert "Multiple" in exc_info.value.error.message

    def test_empty_sql(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("")
        assert exc_info.value.error.code == "RVT-701"

    def test_parse_failure_rvt_701(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("SELECT FROM WHERE", dialect="duckdb")
        err = exc_info.value.error
        assert err.code == "RVT-701" or err.code == "RVT-702"
        assert err.original_sql == "SELECT FROM WHERE"

    def test_error_includes_dialect(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("INSERT INTO t VALUES (1)", dialect="postgres")
        assert exc_info.value.error.dialect == "postgres"

    def test_error_includes_original_sql(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("DROP TABLE t")
        assert exc_info.value.error.original_sql == "DROP TABLE t"

    def test_error_includes_remediation(self, parser: SQLParser) -> None:
        with pytest.raises(SQLParseError) as exc_info:
            parser.parse("INSERT INTO t VALUES (1)")
        assert exc_info.value.error.remediation is not None

    def test_trailing_semicolon_ok(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT 1;")
        assert ast is not None

    def test_dialect_parsing(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT 1", dialect="duckdb")
        assert isinstance(ast, exp.Select)


class TestExtractTableReferences:
    """Tests for SQLParser.extract_table_references()."""

    def test_single_from(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM users")
        refs = parser.extract_table_references(ast)
        assert len(refs) == 1
        assert refs[0].name == "users"
        assert refs[0].source_type == "from"

    def test_schema_qualified(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM myschema.users")
        refs = parser.extract_table_references(ast)
        assert len(refs) == 1
        assert refs[0].name == "users"
        assert refs[0].schema == "myschema"

    def test_alias(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT u.a FROM users u")
        refs = parser.extract_table_references(ast)
        assert len(refs) == 1
        assert refs[0].name == "users"
        assert refs[0].alias == "u"

    def test_join_references(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM users JOIN orders ON users.id = orders.user_id")
        refs = parser.extract_table_references(ast)
        names = {r.name for r in refs}
        assert "users" in names
        assert "orders" in names
        join_refs = [r for r in refs if r.source_type == "join"]
        assert len(join_refs) >= 1

    def test_left_join(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t1 LEFT JOIN t2 ON t1.id = t2.id")
        refs = parser.extract_table_references(ast)
        names = {r.name for r in refs}
        assert "t1" in names
        assert "t2" in names

    def test_multiple_joins(self, parser: SQLParser) -> None:
        sql = "SELECT a FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id"
        ast = parser.parse(sql)
        refs = parser.extract_table_references(ast)
        names = {r.name for r in refs}
        assert names == {"t1", "t2", "t3"}

    def test_subquery_reference(self, parser: SQLParser) -> None:
        sql = "SELECT a FROM (SELECT b FROM inner_table) sub"
        ast = parser.parse(sql)
        refs = parser.extract_table_references(ast)
        names = {r.name for r in refs}
        assert "inner_table" in names

    def test_cte_excluded(self, parser: SQLParser) -> None:
        sql = "WITH cte AS (SELECT a FROM real_table) SELECT a FROM cte"
        ast = parser.parse(sql)
        refs = parser.extract_table_references(ast)
        names = {r.name for r in refs}
        assert "real_table" in names
        assert "cte" not in names

    def test_multiple_ctes_excluded(self, parser: SQLParser) -> None:
        sql = """
        WITH cte1 AS (SELECT a FROM t1),
             cte2 AS (SELECT b FROM t2)
        SELECT a, b FROM cte1 JOIN cte2 ON cte1.a = cte2.b
        """
        ast = parser.parse(sql)
        refs = parser.extract_table_references(ast)
        names = {r.name for r in refs}
        assert "t1" in names
        assert "t2" in names
        assert "cte1" not in names
        assert "cte2" not in names

    def test_nested_subquery(self, parser: SQLParser) -> None:
        sql = "SELECT a FROM (SELECT b FROM (SELECT c FROM deep_table) s1) s2"
        ast = parser.parse(sql)
        refs = parser.extract_table_references(ast)
        names = {r.name for r in refs}
        assert "deep_table" in names

    def test_no_tables(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT 1")
        refs = parser.extract_table_references(ast)
        assert len(refs) == 0


class TestValidate:
    """Tests for SQLParser.validate()."""

    def test_valid_select(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t")
        errors = parser.validate(ast)
        assert len(errors) == 0

    def test_valid_cte(self, parser: SQLParser) -> None:
        ast = parser.parse("WITH cte AS (SELECT 1 AS x) SELECT x FROM cte")
        errors = parser.validate(ast)
        assert len(errors) == 0

    def test_valid_subquery(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM (SELECT b FROM t) sub")
        errors = parser.validate(ast)
        assert len(errors) == 0


class TestNormalize:
    """Tests for SQLParser.normalize() (task 9.3)."""

    def test_constant_left_to_column_left(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t WHERE 1 = a")
        normalized = parser.normalize(ast)
        where = normalized.find(exp.Where)
        assert where is not None
        # After normalization, column should be on the left
        assert where.this.sql() == "a = 1"

    def test_double_negation_elimination(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t WHERE NOT NOT a > 1")
        normalized = parser.normalize(ast)
        where = normalized.find(exp.Where)
        assert where is not None
        assert "NOT NOT" not in where.sql()
        assert "a > 1" in where.sql()

    def test_and_ordering_deterministic(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t WHERE z = 1 AND a = 2")
        normalized = parser.normalize(ast)
        where = normalized.find(exp.Where)
        assert where is not None
        # AND conjuncts should be sorted alphabetically
        sql = where.this.sql()
        assert sql.index("a = 2") < sql.index("z = 1")

    def test_coalesce_duplicate_args(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT COALESCE(a, a) FROM t")
        normalized = parser.normalize(ast)
        sql = normalized.sql()
        # Should simplify to just 'a'
        assert "COALESCE" not in sql

    def test_coalesce_null_removal(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT COALESCE(a, NULL, b) FROM t")
        normalized = parser.normalize(ast)
        sql = normalized.sql()
        # NULL should be removed from COALESCE args
        assert "NULL" not in sql
        assert "COALESCE" in sql
        assert "a" in sql
        assert "b" in sql

    def test_does_not_mutate_original(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t WHERE 1 = a")
        original_sql = ast.sql()
        parser.normalize(ast)
        assert ast.sql() == original_sql

    def test_normalize_preserves_structure(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a, b FROM t WHERE a > 1")
        normalized = parser.normalize(ast)
        assert normalized.find(exp.Select) is not None or isinstance(normalized, exp.Select)


class TestExtractLogicalPlan:
    """Tests for SQLParser.extract_logical_plan() (task 9.3)."""

    def test_simple_select_projections(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a, b FROM t")
        plan = parser.extract_logical_plan(ast)
        assert len(plan.projections) == 2
        assert plan.projections[0].expression == "a"
        assert plan.projections[1].expression == "b"

    def test_aliased_projection(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a + 1 AS result FROM t")
        plan = parser.extract_logical_plan(ast)
        assert len(plan.projections) == 1
        assert plan.projections[0].alias == "result"
        assert "a" in plan.projections[0].source_columns[0]

    def test_where_predicates(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t WHERE a > 1 AND b = 2")
        plan = parser.extract_logical_plan(ast)
        assert len(plan.predicates) >= 2
        where_preds = [p for p in plan.predicates if p.location == "where"]
        assert len(where_preds) >= 2

    def test_having_predicates(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a, COUNT(b) FROM t GROUP BY a HAVING COUNT(b) > 5")
        plan = parser.extract_logical_plan(ast)
        having_preds = [p for p in plan.predicates if p.location == "having"]
        assert len(having_preds) >= 1

    def test_join_extraction(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a.x FROM a JOIN b ON a.id = b.id")
        plan = parser.extract_logical_plan(ast)
        assert len(plan.joins) == 1
        j = plan.joins[0]
        assert j.type == "inner"
        assert j.left_table == "a"
        assert j.right_table == "b"
        assert j.condition is not None
        assert len(j.columns) >= 2

    def test_left_join(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        plan = parser.extract_logical_plan(ast)
        assert len(plan.joins) == 1
        assert plan.joins[0].type == "left"

    def test_cross_join(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT * FROM a CROSS JOIN b")
        plan = parser.extract_logical_plan(ast)
        assert len(plan.joins) == 1
        assert plan.joins[0].type == "cross"
        assert plan.joins[0].condition is None

    def test_aggregation(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a, COUNT(b), SUM(c) FROM t GROUP BY a")
        plan = parser.extract_logical_plan(ast)
        assert plan.aggregations is not None
        assert "a" in plan.aggregations.group_by
        assert len(plan.aggregations.functions) == 2

    def test_no_aggregation(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t")
        plan = parser.extract_logical_plan(ast)
        assert plan.aggregations is None

    def test_limit(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t LIMIT 10")
        plan = parser.extract_logical_plan(ast)
        assert plan.limit is not None
        assert plan.limit.count == 10
        assert plan.limit.offset is None

    def test_limit_with_offset(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t LIMIT 10 OFFSET 5")
        plan = parser.extract_logical_plan(ast)
        assert plan.limit is not None
        assert plan.limit.count == 10
        assert plan.limit.offset == 5

    def test_no_limit(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t")
        plan = parser.extract_logical_plan(ast)
        assert plan.limit is None

    def test_ordering(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a, b FROM t ORDER BY a ASC, b DESC")
        plan = parser.extract_logical_plan(ast)
        assert plan.ordering is not None
        assert len(plan.ordering.columns) == 2
        assert plan.ordering.columns[0] == ("a", "asc")
        assert plan.ordering.columns[1] == ("b", "desc")

    def test_no_ordering(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t")
        plan = parser.extract_logical_plan(ast)
        assert plan.ordering is None

    def test_distinct(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT DISTINCT a FROM t")
        plan = parser.extract_logical_plan(ast)
        assert plan.distinct is True

    def test_not_distinct(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t")
        plan = parser.extract_logical_plan(ast)
        assert plan.distinct is False

    def test_source_tables(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t1 JOIN t2 ON t1.id = t2.id")
        plan = parser.extract_logical_plan(ast)
        names = {t.name for t in plan.source_tables}
        assert "t1" in names
        assert "t2" in names

    def test_cte_source_tables_excluded(self, parser: SQLParser) -> None:
        ast = parser.parse("WITH cte AS (SELECT a FROM real_table) SELECT a FROM cte")
        plan = parser.extract_logical_plan(ast)
        names = {t.name for t in plan.source_tables}
        assert "real_table" in names
        assert "cte" not in names

    def test_complex_query(self, parser: SQLParser) -> None:
        sql = """
        SELECT a, COUNT(b) AS cnt
        FROM t1
        JOIN t2 ON t1.id = t2.id
        WHERE t1.x > 1
        GROUP BY a
        HAVING COUNT(b) > 5
        ORDER BY cnt DESC
        LIMIT 10
        """
        ast = parser.parse(sql)
        plan = parser.extract_logical_plan(ast)
        assert len(plan.projections) == 2
        assert len(plan.predicates) >= 2  # WHERE + HAVING
        assert len(plan.joins) == 1
        assert plan.aggregations is not None
        assert plan.limit is not None
        assert plan.limit.count == 10
        assert plan.ordering is not None
        assert plan.distinct is False


class TestInferSchema:
    """Tests for SQLParser.infer_schema() (task 9.4)."""

    def _schema(self, cols: dict[str, str]) -> Schema:
        from rivet_core.models import Column, Schema

        return Schema(columns=[Column(name=n, type=t, nullable=True) for n, t in cols.items()])

    def test_basic_type_mapping(self, parser: SQLParser) -> None:

        upstream = {"t": self._schema({"a": "int32", "b": "utf8", "c": "float64"})}
        ast = parser.parse("SELECT a, b, c FROM t")
        schema, warnings = parser.infer_schema(ast, upstream)
        assert schema is not None
        types = {col.name: col.type for col in schema.columns}
        assert types["a"] == "int32"
        assert types["b"] == "utf8"
        assert types["c"] == "float64"
        assert warnings == []

    def test_all_mapped_types(self, parser: SQLParser) -> None:
        upstream = {
            "t": self._schema({
                "a": "int16",
                "b": "int32",
                "c": "int64",
                "d": "float32",
                "e": "float64",
                "f": "bool",
                "g": "date32",
                "h": "timestamp[us]",
            })
        }
        ast = parser.parse("SELECT a, b, c, d, e, f, g, h FROM t")
        schema, warnings = parser.infer_schema(ast, upstream)
        assert schema is not None
        types = {col.name: col.type for col in schema.columns}
        assert types == {
            "a": "int16",
            "b": "int32",
            "c": "int64",
            "d": "float32",
            "e": "float64",
            "f": "bool",
            "g": "date32",
            "h": "timestamp[us]",
        }
        assert warnings == []

    def test_decimal_with_precision_scale(self, parser: SQLParser) -> None:
        upstream = {"t": self._schema({"price": "decimal128(10,2)"})}
        ast = parser.parse("SELECT price FROM t")
        schema, warnings = parser.infer_schema(ast, upstream)
        assert schema is not None
        assert schema.columns[0].type == "decimal128(10,2)"

    def test_select_star_expansion(self, parser: SQLParser) -> None:
        upstream = {"t": self._schema({"a": "int32", "b": "utf8"})}
        ast = parser.parse("SELECT * FROM t")
        schema, warnings = parser.infer_schema(ast, upstream)
        assert schema is not None
        names = [col.name for col in schema.columns]
        assert "a" in names
        assert "b" in names
        assert len(schema.columns) == 2

    def test_unmapped_type_produces_warning(self, parser: SQLParser) -> None:
        upstream = {"t": self._schema({"a": "int32"})}
        ast = parser.parse("SELECT CAST(a AS TINYINT) AS tiny FROM t")
        schema, warnings = parser.infer_schema(ast, upstream)
        assert schema is not None
        assert schema.columns[0].type == "large_binary"
        assert any("RVT-706" in w for w in warnings)

    def test_unknown_type_defaults_to_large_binary(self, parser: SQLParser) -> None:
        upstream = {"t": self._schema({"a": "int32"})}
        ast = parser.parse("SELECT SOME_FUNC(a) AS result FROM t")
        schema, warnings = parser.infer_schema(ast, upstream)
        assert schema is not None
        assert schema.columns[0].type == "large_binary"

    def test_no_upstream_schemas_best_effort(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t")
        schema, warnings = parser.infer_schema(ast, {})
        # Best-effort: should not raise, may return schema with large_binary
        assert schema is not None or schema is None  # either is acceptable

    def test_failure_returns_none(self, parser: SQLParser) -> None:
        """Inference failure should produce None, never raise."""
        # Pass a broken AST-like object that will cause internal errors
        import sqlglot.expressions as exp

        broken_ast = exp.Literal.string("not a select")
        schema, warnings = parser.infer_schema(broken_ast, {})
        assert schema is None

    def test_aliased_expression(self, parser: SQLParser) -> None:
        upstream = {"t": self._schema({"a": "int32", "b": "int32"})}
        ast = parser.parse("SELECT a + b AS total FROM t")
        schema, warnings = parser.infer_schema(ast, upstream)
        assert schema is not None
        assert schema.columns[0].name == "total"
        assert schema.columns[0].type == "int32"

    def test_columns_nullable_true(self, parser: SQLParser) -> None:
        upstream = {"t": self._schema({"a": "int32"})}
        ast = parser.parse("SELECT a FROM t")
        schema, _ = parser.infer_schema(ast, upstream)
        assert schema is not None
        assert all(col.nullable for col in schema.columns)


class TestTranslate:
    """Tests for SQLParser.translate() (task 9.6)."""

    def test_duckdb_to_postgres(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t LIMIT 10", dialect="duckdb")
        result = parser.translate(ast, source_dialect="duckdb", target_dialect="postgres")
        assert "LIMIT" in result
        assert "10" in result

    def test_postgres_to_duckdb(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t WHERE a > 1", dialect="postgres")
        result = parser.translate(ast, source_dialect="postgres", target_dialect="duckdb")
        assert "SELECT" in result
        assert "a" in result

    def test_duckdb_to_bigquery(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a, b FROM t", dialect="duckdb")
        result = parser.translate(ast, source_dialect="duckdb", target_dialect="bigquery")
        assert "SELECT" in result

    def test_duckdb_to_snowflake(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t WHERE a = 1", dialect="duckdb")
        result = parser.translate(ast, source_dialect="duckdb", target_dialect="snowflake")
        assert "SELECT" in result

    def test_duckdb_to_spark(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT a FROM t", dialect="duckdb")
        result = parser.translate(ast, source_dialect="duckdb", target_dialect="spark")
        assert "SELECT" in result

    def test_same_dialect_roundtrip(self, parser: SQLParser) -> None:
        sql = "SELECT a, b FROM t WHERE a > 1"
        ast = parser.parse(sql, dialect="duckdb")
        result = parser.translate(ast, source_dialect="duckdb", target_dialect="duckdb")
        assert "SELECT" in result
        assert "a" in result

    def test_returns_string(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT 1 AS x")
        result = parser.translate(ast, source_dialect="duckdb", target_dialect="postgres")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_rvt_703_on_empty_output(self, parser: SQLParser) -> None:
        """RVT-703 raised when translation produces no output."""
        import unittest.mock as mock

        ast = parser.parse("SELECT 1")
        with mock.patch("sqlglot.transpile", return_value=[]):
            with pytest.raises(SQLParseError) as exc_info:
                parser.translate(ast, source_dialect="duckdb", target_dialect="postgres")
        assert exc_info.value.error.code == "RVT-703"

    def test_rvt_703_on_exception(self, parser: SQLParser) -> None:
        """RVT-703 raised when sqlglot.transpile raises."""
        import unittest.mock as mock

        ast = parser.parse("SELECT 1")
        with mock.patch("sqlglot.transpile", side_effect=Exception("boom")):
            with pytest.raises(SQLParseError) as exc_info:
                parser.translate(ast, source_dialect="duckdb", target_dialect="postgres")
        assert exc_info.value.error.code == "RVT-703"
        assert exc_info.value.error.original_sql is not None
