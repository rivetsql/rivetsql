"""Property tests for rivet_core.interactive.formatter.

# Feature: cli-repl, Property 9: SQL format round-trip preserves semantics
# Validates: Requirements 16.1, 16.3
"""

from __future__ import annotations

import pytest
import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.formatter import SqlFormatError, format_sql

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# A small set of valid SQL statements that sqlglot can reliably parse and
# round-trip.  We use a fixed pool rather than fully random SQL because
# generating syntactically valid SQL from scratch is complex; the property
# we care about is that format_sql() preserves semantics, not that it handles
# every possible SQL dialect.
_VALID_SQL_POOL = [
    "SELECT 1",
    "SELECT id FROM users",
    "SELECT id, name FROM users WHERE id = 1",
    "SELECT a, b, c FROM t ORDER BY a",
    "SELECT COUNT(*) FROM orders",
    "SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1",
    "SELECT a FROM t LIMIT 10",
    "SELECT a FROM t WHERE a IS NOT NULL",
    "SELECT a FROM t WHERE a IN (1, 2, 3)",
    "SELECT a FROM t1 JOIN t2 ON t1.id = t2.id",
    "SELECT a FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
    "SELECT a FROM t WHERE a BETWEEN 1 AND 10",
    "SELECT DISTINCT a FROM t",
    "SELECT a AS alias FROM t",
    "SELECT 1; SELECT 2",
]

_INVALID_SQL_POOL = [
    "SELECT FROM WHERE ))))",
    "NOT VALID SQL AT ALL !!!",
    "SELECT * FROM",
    "INSERT INTO",
    "( ( ( ( (",
]

valid_sql_st = st.sampled_from(_VALID_SQL_POOL)
invalid_sql_st = st.sampled_from(_INVALID_SQL_POOL)


# ---------------------------------------------------------------------------
# Property 9a: format_sql on valid SQL produces equivalent AST
# ---------------------------------------------------------------------------

@given(sql=valid_sql_st)
@settings(max_examples=100)
def test_format_round_trip_preserves_semantics(sql: str) -> None:
    """Property 9: format_sql(sql) produces a string whose AST is equivalent
    to the original SQL's AST (same logical plan).
    """
    formatted = format_sql(sql)
    assert isinstance(formatted, str)
    assert formatted.strip() != ""

    # Parse both original and formatted; compare AST representations.
    original_stmts = sqlglot.parse(sql)
    formatted_stmts = sqlglot.parse(formatted)

    # Same number of statements
    original_non_none = [s for s in original_stmts if s is not None]
    formatted_non_none = [s for s in formatted_stmts if s is not None]
    assert len(original_non_none) == len(formatted_non_none)

    # Each statement's canonical SQL (no pretty-print) must be identical,
    # meaning the AST is semantically equivalent.
    for orig, fmt in zip(original_non_none, formatted_non_none):
        orig_canonical = orig.sql(pretty=False, normalize=True)
        fmt_canonical = fmt.sql(pretty=False, normalize=True)
        assert orig_canonical == fmt_canonical, (
            f"Semantic mismatch after formatting.\n"
            f"Original canonical: {orig_canonical!r}\n"
            f"Formatted canonical: {fmt_canonical!r}"
        )


# ---------------------------------------------------------------------------
# Property 9b: format_sql on invalid SQL raises SqlFormatError
# ---------------------------------------------------------------------------

@given(sql=invalid_sql_st)
@settings(max_examples=100)
def test_format_invalid_sql_raises(sql: str) -> None:
    """Property 9: format_sql() raises SqlFormatError for invalid SQL."""
    with pytest.raises(SqlFormatError):
        format_sql(sql)


# ---------------------------------------------------------------------------
# Property 9c: format_sql is idempotent (formatting twice == formatting once)
# ---------------------------------------------------------------------------

@given(sql=valid_sql_st)
@settings(max_examples=100)
def test_format_idempotent(sql: str) -> None:
    """Formatting already-formatted SQL produces the same output."""
    once = format_sql(sql)
    twice = format_sql(once)
    assert once == twice
