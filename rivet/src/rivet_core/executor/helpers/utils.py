"""General utility functions for the executor.

Progress callback notification, SQL table reference extraction,
cached Arrow material retrieval, and fused SQL resolution.
"""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.executor.models import logger
from rivet_core.optimizer import FusedGroup
from rivet_core.strategies import MaterializedRef


def _notify(callback: Any, *args: Any) -> None:
    """Invoke a progress callback method, swallowing any exceptions."""
    try:
        callback(*args)
    except Exception:
        logger.debug("Progress callback error", exc_info=True)


# ---------------------------------------------------------------------------
# SQL table reference extraction
# ---------------------------------------------------------------------------


def _extract_table_references(sql_sources: list[str]) -> set[str]:
    """Extract all table references from SQL statements.

    Handles:
    - Simple FROM/JOIN clauses
    - CTEs (WITH clauses)
    - Subqueries
    - Multiple table references in the same clause

    Returns a set of table names that could be materialized tables from previous waves.
    """
    import re

    import sqlglot
    from sqlglot import exp

    referenced_tables: set[str] = set()

    for sql_text in sql_sources:
        if not sql_text:
            continue

        try:
            statements = sqlglot.parse(sql_text)
        except Exception:
            statements = []

        if statements:
            for statement in statements:
                if statement is None:
                    continue
                referenced_tables.update(
                    table.name for table in statement.find_all(exp.Table) if table.name
                )
            continue

        # Remove string literals to avoid false matches
        # Replace single-quoted strings with empty strings
        cleaned_sql = re.sub(r"'[^']*'", "''", sql_text)
        # Replace double-quoted identifiers (keep them as they might be table names)
        # but remove their quotes for matching
        cleaned_sql = re.sub(r'"([^"]+)"', r"\1", cleaned_sql)

        # Pattern 1: FROM/JOIN table_name (with optional alias)
        # Matches: FROM table_name, JOIN table_name, FROM table_name AS alias
        # Excludes: FROM (subquery), FROM function_name(...)
        pattern1 = (
            r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:AS\s+[a-zA-Z_][a-zA-Z0-9_]*|[,\s]|$)"
        )
        matches1 = re.findall(pattern1, cleaned_sql, re.IGNORECASE)
        referenced_tables.update(matches1)

        # Pattern 2: FROM/JOIN without AS but with whitespace before alias
        # Matches: FROM table_name alias_name
        pattern2 = r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(?!AS\b|ON\b|WHERE\b|GROUP\b|ORDER\b|LIMIT\b|HAVING\b|UNION\b|EXCEPT\b|INTERSECT\b)([a-zA-Z_][a-zA-Z0-9_]*)"
        matches2 = re.findall(pattern2, cleaned_sql, re.IGNORECASE)
        referenced_tables.update(m[0] for m in matches2)

        # Pattern 3: Simple FROM/JOIN at end of statement or before WHERE/GROUP/ORDER
        # Matches: FROM table_name WHERE, FROM table_name GROUP BY
        pattern3 = r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:WHERE|GROUP|ORDER|HAVING|LIMIT|UNION|EXCEPT|INTERSECT|$)"
        matches3 = re.findall(pattern3, cleaned_sql, re.IGNORECASE)
        referenced_tables.update(matches3)

    # Filter out SQL keywords that might have been captured
    sql_keywords = {
        "SELECT",
        "FROM",
        "WHERE",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "CROSS",
        "ON",
        "AS",
        "AND",
        "OR",
        "NOT",
        "IN",
        "EXISTS",
        "BETWEEN",
        "LIKE",
        "IS",
        "NULL",
        "GROUP",
        "BY",
        "ORDER",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "DISTINCT",
        "ALL",
        "ANY",
        "SOME",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "WITH",
        "RECURSIVE",
        "VALUES",
        "TABLE",
        "LATERAL",
        "UNNEST",
    }

    return {t for t in referenced_tables if t.upper() not in sql_keywords}


def _cached_arrow_materials(
    materials: dict[str, MaterializedRef],
    *,
    checkpoint_sources: set[str] | None = None,
) -> dict[str, pyarrow.Table]:
    """Return already-available Arrow tables without forcing deferred reads."""
    skipped_checkpoint_sources = checkpoint_sources or set()
    cached: dict[str, pyarrow.Table] = {}
    for name, ref in materials.items():
        if name in skipped_checkpoint_sources and not ref.has_cached_arrow():
            continue
        table = ref.to_arrow_if_cached()
        if table is not None:
            cached[name] = table
    return cached


def _resolve_fused_sql(group: FusedGroup) -> str | None:
    """Extract the best available fused SQL from a group."""
    if group.fusion_result:
        sql = group.fusion_result.resolved_fused_sql or group.fusion_result.fused_sql
        if sql:
            return sql
    return group.resolved_sql or group.fused_sql
