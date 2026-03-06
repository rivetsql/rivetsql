"""Tests for _scan_table_refs — token-level table reference scanning.

Validates Requirements: 1.1, 3.4, 4.1, 4.4, 4.5, 12.1, 12.3, 12.4, 12.5, 12.6
"""

from __future__ import annotations

from sqlglot.tokens import Tokenizer

from rivet_core.interactive.sql_preprocessor import TableRefSpan, _scan_table_refs


def _scan(sql: str) -> list[TableRefSpan]:
    return _scan_table_refs(list(Tokenizer().tokenize(sql)), sql)


# --- Pattern A: N-part dotted identifier sequences ---


class TestPatternA:
    """Dotted identifier sequences (1-part through N-part)."""

    def test_1_part_ref(self):
        refs = _scan("SELECT * FROM my_table")
        assert len(refs) == 1
        assert refs[0].parts == ["my_table"]
        assert refs[0].is_path_ref is False

    def test_2_part_ref(self):
        refs = _scan("SELECT * FROM public.users")
        assert len(refs) == 1
        assert refs[0].parts == ["public", "users"]
        assert refs[0].ref_str == "public.users"

    def test_3_part_ref(self):
        refs = _scan("SELECT * FROM my_pg.public.users")
        assert len(refs) == 1
        assert refs[0].parts == ["my_pg", "public", "users"]
        assert refs[0].ref_str == "my_pg.public.users"

    def test_4_part_ref(self):
        refs = _scan("SELECT * FROM my_unity.main.default.users")
        assert len(refs) == 1
        assert refs[0].parts == ["my_unity", "main", "default", "users"]
        assert refs[0].ref_str == "my_unity.main.default.users"

    def test_5_part_ref(self):
        refs = _scan("SELECT * FROM my_unity.main.default.schema.users")
        assert len(refs) == 1
        assert refs[0].parts == ["my_unity", "main", "default", "schema", "users"]

    def test_quoted_identifiers(self):
        """Req 12.2: quoted parts should be unquoted in parts list."""
        refs = _scan('SELECT * FROM "my_unity"."main"."default"."users"')
        assert len(refs) == 1
        assert refs[0].parts == ["my_unity", "main", "default", "users"]
        assert refs[0].is_path_ref is True


# --- Pattern B: path/URI references ---


class TestPatternB:
    """IDENTIFIER."quoted_string" patterns (path/URI refs)."""

    def test_path_ref(self):
        """Req 4.1: IDENTIFIER.QUOTED_STRING in table position."""
        refs = _scan('SELECT * FROM my_fs."/data/orders.csv"')
        assert len(refs) == 1
        assert refs[0].parts == ["my_fs", "/data/orders.csv"]
        assert refs[0].is_path_ref is True
        assert refs[0].ref_str == 'my_fs./data/orders.csv'

    def test_path_with_dots(self):
        """Req 4.4: dots inside quoted string are opaque."""
        refs = _scan('SELECT * FROM my_fs."subdir/orders.v2.parquet"')
        assert len(refs) == 1
        assert refs[0].parts == ["my_fs", "subdir/orders.v2.parquet"]
        assert refs[0].is_path_ref is True

    def test_uri_ref(self):
        """Req 4.3: URI inside quoted string."""
        refs = _scan('SELECT * FROM my_s3."s3://bucket/file.parquet"')
        assert len(refs) == 1
        assert refs[0].parts == ["my_s3", "s3://bucket/file.parquet"]
        assert refs[0].is_path_ref is True

    def test_path_ref_with_alias(self):
        """Req 12.6: path ref with alias preserved."""
        refs = _scan('SELECT * FROM my_fs."/data/orders.csv" AS orders')
        assert len(refs) == 1
        assert refs[0].parts == ["my_fs", "/data/orders.csv"]

    def test_path_ref_with_implicit_alias(self):
        refs = _scan('SELECT * FROM my_fs."/data/orders.csv" orders')
        assert len(refs) == 1
        assert refs[0].parts == ["my_fs", "/data/orders.csv"]


# --- Table position heuristic ---


class TestTablePositionHeuristic:
    """Only match refs after FROM, JOIN, INTO, or at DML start."""

    def test_from_keyword(self):
        refs = _scan("SELECT * FROM t1")
        assert len(refs) == 1
        assert refs[0].parts == ["t1"]

    def test_join_keyword(self):
        refs = _scan("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        assert len(refs) == 2
        assert refs[0].parts == ["t1"]
        assert refs[1].parts == ["t2"]

    def test_into_keyword(self):
        refs = _scan("INSERT INTO my_table VALUES (1)")
        assert len(refs) == 1
        assert refs[0].parts == ["my_table"]

    def test_update_dml_start(self):
        refs = _scan("UPDATE my_table SET x=1")
        assert len(refs) == 1
        assert refs[0].parts == ["my_table"]

    def test_delete_from(self):
        refs = _scan("DELETE FROM my_table WHERE x=1")
        assert len(refs) == 1
        assert refs[0].parts == ["my_table"]

    def test_column_refs_excluded(self):
        """Column refs in SELECT/WHERE/ON should not be picked up."""
        refs = _scan("SELECT a.col1, b.col2 FROM a JOIN b ON a.id = b.id")
        assert [r.parts for r in refs] == [["a"], ["b"]]

    def test_no_table_refs(self):
        refs = _scan("SELECT 1 + 2")
        assert refs == []

    def test_comma_separated_tables(self):
        refs = _scan("SELECT * FROM t1, t2, my_pg.public.users")
        assert len(refs) == 3
        assert refs[2].parts == ["my_pg", "public", "users"]

    def test_left_outer_join(self):
        refs = _scan("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id")
        assert [r.parts for r in refs] == [["t1"], ["t2"]]

    def test_right_join(self):
        refs = _scan("SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id")
        assert [r.parts for r in refs] == [["t1"], ["t2"]]

    def test_full_outer_join(self):
        refs = _scan("SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id")
        assert [r.parts for r in refs] == [["t1"], ["t2"]]

    def test_inner_join(self):
        refs = _scan("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id")
        assert [r.parts for r in refs] == [["t1"], ["t2"]]

    def test_natural_join(self):
        refs = _scan("SELECT * FROM t1 NATURAL JOIN t2")
        assert [r.parts for r in refs] == [["t1"], ["t2"]]

    def test_subquery_table_ref(self):
        refs = _scan("SELECT * FROM (SELECT * FROM inner_table) sub")
        ref_names = [r.ref_str for r in refs]
        assert "inner_table" in ref_names


# --- Alias skipping ---


class TestAliasSkipping:
    """Skip identifiers after AS keyword (aliases)."""

    def test_explicit_alias(self):
        """Req 12.3: alias after AS preserved, not treated as ref."""
        refs = _scan("SELECT * FROM my_pg.public.users AS u")
        assert len(refs) == 1
        assert refs[0].parts == ["my_pg", "public", "users"]

    def test_implicit_alias(self):
        refs = _scan("SELECT * FROM my_pg.public.users u")
        assert len(refs) == 1
        assert refs[0].parts == ["my_pg", "public", "users"]

    def test_mixed_aliases(self):
        refs = _scan(
            "SELECT * FROM my_joint j JOIN my_pg.public.users c ON j.id = c.id"
        )
        assert [r.parts for r in refs] == [["my_joint"], ["my_pg", "public", "users"]]


# --- CTE exclusion ---


class TestCTEExclusion:
    """Req 12.4: CTE names excluded from resolution."""

    def test_single_cte(self):
        refs = _scan("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert refs == []

    def test_multiple_ctes(self):
        refs = _scan(
            "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a JOIN b"
        )
        assert refs == []

    def test_cte_in_subquery(self):
        """Req 12.5: CTE ref in subquery also excluded."""
        refs = _scan(
            "WITH cte AS (SELECT 1) SELECT * FROM (SELECT * FROM cte) sub"
        )
        # cte should be excluded; 'sub' may appear as a subquery alias
        ref_names = [r.ref_str for r in refs]
        assert "cte" not in ref_names

    def test_cte_does_not_exclude_catalog_ref(self):
        """Non-CTE refs should still be found alongside CTEs."""
        refs = _scan(
            "WITH cte AS (SELECT 1) SELECT * FROM cte JOIN my_pg.public.users"
        )
        assert len(refs) == 1
        assert refs[0].parts == ["my_pg", "public", "users"]


# --- Character offset correctness ---


class TestOffsets:
    """Record character offsets from token positions for surgical replacement."""

    def test_simple_ref_offset(self):
        sql = "SELECT * FROM my_table"
        refs = _scan(sql)
        assert sql[refs[0].start_offset : refs[0].end_offset] == "my_table"

    def test_dotted_ref_offset(self):
        sql = "SELECT * FROM my_pg.public.users"
        refs = _scan(sql)
        assert sql[refs[0].start_offset : refs[0].end_offset] == "my_pg.public.users"

    def test_4_part_ref_offset(self):
        sql = "SELECT * FROM my_unity.main.default.users"
        refs = _scan(sql)
        assert (
            sql[refs[0].start_offset : refs[0].end_offset]
            == "my_unity.main.default.users"
        )

    def test_path_ref_offset(self):
        sql = 'SELECT * FROM my_fs."/data/orders.csv"'
        refs = _scan(sql)
        assert (
            sql[refs[0].start_offset : refs[0].end_offset]
            == 'my_fs."/data/orders.csv"'
        )

    def test_quoted_ident_offset(self):
        sql = 'SELECT * FROM "my_unity"."main"."default"."users"'
        refs = _scan(sql)
        assert (
            sql[refs[0].start_offset : refs[0].end_offset]
            == '"my_unity"."main"."default"."users"'
        )

    def test_multiple_refs_offsets(self):
        sql = "SELECT * FROM t1 JOIN my_pg.public.users ON t1.id = my_pg.public.users.id"
        refs = _scan(sql)
        assert sql[refs[0].start_offset : refs[0].end_offset] == "t1"
        assert (
            sql[refs[1].start_offset : refs[1].end_offset] == "my_pg.public.users"
        )


# --- String literals and comments (Req 12.1) ---


class TestStringLiteralsAndComments:
    """Req 12.1: dotted identifiers inside strings/comments not modified."""

    def test_string_literal_not_scanned(self):
        refs = _scan("SELECT * FROM t WHERE name = 'my_pg.public.users'")
        assert len(refs) == 1
        assert refs[0].parts == ["t"]

    def test_no_refs_in_pure_expression(self):
        refs = _scan("SELECT 'hello' AS greeting")
        assert refs == []
