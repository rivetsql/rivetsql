"""Headless autocomplete service for the interactive layer.

Merges five completion sources: catalog introspection, pipeline joints,
SQL keywords, Rivet annotations, and snippets. Context-aware (FROM clause,
dot-trigger, annotation). Graceful degradation on any error.

Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9
"""

from __future__ import annotations

import re
from typing import Any

from rivet_core.fuzzy import fuzzy_match
from rivet_core.interactive.types import Completion, CompletionKind

# Sort key tiers: lower = higher priority
_SORT_JOINT = 100
_SORT_CATALOG = 200
_SORT_KEYWORD = 300
_SORT_ANNOTATION = 400
_SORT_SNIPPET = 500

_SQL_KEYWORDS: list[str] = [
    "SELECT",
    "FROM",
    "WHERE",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "FULL",
    "CROSS",
    "ON",
    "AND",
    "OR",
    "NOT",
    "IN",
    "EXISTS",
    "BETWEEN",
    "LIKE",
    "IS",
    "NULL",
    "AS",
    "ORDER",
    "BY",
    "GROUP",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "ALL",
    "DISTINCT",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "TABLE",
    "DROP",
    "ALTER",
    "INDEX",
    "VIEW",
    "WITH",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "CAST",
    "COALESCE",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "ASC",
    "DESC",
    "TRUE",
    "FALSE",
    "OVER",
    "PARTITION",
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "LAG",
    "LEAD",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTILE",
    "ROWS",
    "RANGE",
    "UNBOUNDED",
    "PRECEDING",
    "FOLLOWING",
    "CURRENT",
    "ROW",
    "FILTER",
    "QUALIFY",
    "PIVOT",
    "UNPIVOT",
    "LATERAL",
    "FLATTEN",
    "EXCEPT",
    "INTERSECT",
    "FETCH",
    "NEXT",
    "ONLY",
    "RECURSIVE",
    "MATERIALIZED",
    "TEMPORARY",
    "TEMP",
    "IF",
    "REPLACE",
    "CASCADE",
    "RESTRICT",
    "NULLS",
    "FIRST",
    "LAST",
]

_ANNOTATION_TYPES: list[str] = [
    "assert:not_null",
    "assert:unique",
    "assert:accepted_values",
    "assert:relationships",
    "assert:expression",
    "assert:row_count",
    "assert:freshness",
    "assert:custom",
    "audit:not_null",
    "audit:unique",
    "audit:accepted_values",
    "audit:relationships",
    "audit:expression",
    "audit:row_count",
    "audit:freshness",
    "audit:custom",
]

_SNIPPETS: list[tuple[str, str]] = [
    ("sel*", "SELECT * FROM "),
    ("selc", "SELECT\n  $1\nFROM\n  $2"),
    ("cte", "WITH cte AS (\n  SELECT $1\n  FROM $2\n)\nSELECT * FROM cte"),
    ("join", "JOIN $1 ON $2"),
    ("left", "LEFT JOIN $1 ON $2"),
    ("ins", "INSERT INTO $1 ($2) VALUES ($3)"),
    ("upd", "UPDATE $1 SET $2 WHERE $3"),
]

# Pattern to detect if cursor is in a FROM/JOIN clause context
_FROM_CONTEXT_RE = re.compile(r"(?:FROM|JOIN)\s+(?:\S+\s*,\s*)*$", re.IGNORECASE)

# Pattern to detect annotation context
_ANNOTATION_RE = re.compile(r"--\s*rivet:\s*$", re.IGNORECASE)


class _CatalogEntry:
    """An indexed catalog item for completion."""

    __slots__ = ("catalog", "schema", "table", "columns")

    def __init__(
        self,
        catalog: str,
        schema: str | None = None,
        table: str | None = None,
        columns: list[tuple[str, str]] | None = None,
    ) -> None:
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.columns = columns  # list of (name, type_str)


class _JointEntry:
    """An indexed joint for completion."""

    __slots__ = ("name", "joint_type", "engine", "columns")

    def __init__(
        self,
        name: str,
        joint_type: str,
        engine: str | None = None,
        columns: list[tuple[str, str]] | None = None,
    ) -> None:
        self.name = name
        self.joint_type = joint_type
        self.engine = engine
        self.columns = columns  # list of (name, type_str)


class CompletionEngine:
    """Headless autocomplete service.

    Call update_catalogs() and update_assembly() to rebuild the index,
    then complete() to get ranked completions.
    """

    def __init__(self) -> None:
        self._catalog_entries: list[_CatalogEntry] = []
        self._joint_entries: list[_JointEntry] = []

    def update_catalogs(self, catalogs: list[dict[str, Any]]) -> None:
        """Rebuild catalog completion index.

        Each dict has keys: catalog (str), schema (str|None),
        table (str|None), columns (list[tuple[str,str]]|None).
        """
        self._catalog_entries = [
            _CatalogEntry(
                catalog=c["catalog"],
                schema=c.get("schema"),
                table=c.get("table"),
                columns=c.get("columns"),
            )
            for c in catalogs
        ]

    def update_assembly(self, joints: list[dict[str, Any]]) -> None:
        """Rebuild joint completion index.

        Each dict has keys: name (str), joint_type (str),
        engine (str|None), columns (list[tuple[str,str]]|None).
        """
        self._joint_entries = [
            _JointEntry(
                name=j["name"],
                joint_type=j["joint_type"],
                engine=j.get("engine"),
                columns=j.get("columns"),
            )
            for j in joints
        ]

    def complete(
        self,
        sql: str,
        cursor_pos: int,
        catalog_context: str | None = None,
    ) -> list[Completion]:
        """Return ranked completions. Graceful degradation: empty list on error."""
        try:
            return self._complete_impl(sql, cursor_pos, catalog_context)
        except Exception:
            return []

    def complete_annotation(self, line: str, cursor_pos: int) -> list[Completion]:
        """Return completions for Rivet annotations (-- rivet:...)."""
        try:
            prefix = line[:cursor_pos]
            # Extract text after "-- rivet:"
            match = re.search(r"--\s*rivet:\s*(.*)$", prefix, re.IGNORECASE)
            if not match:
                return []
            typed = match.group(1)
            return self._filter_and_sort(
                typed,
                [
                    Completion(
                        label=a,
                        insert_text=a,
                        kind=CompletionKind.ANNOTATION,
                        detail="Rivet annotation",
                        sort_key=_SORT_ANNOTATION,
                        source=None,
                    )
                    for a in _ANNOTATION_TYPES
                ],
            )
        except Exception:
            return []

    def _complete_impl(
        self,
        sql: str,
        cursor_pos: int,
        catalog_context: str | None,
    ) -> list[Completion]:
        text_before = sql[:cursor_pos]

        # Check annotation context
        current_line = text_before.rsplit("\n", 1)[-1]
        if _ANNOTATION_RE.search(current_line):
            return self.complete_annotation(current_line, len(current_line))

        # Extract the word being typed (prefix)
        prefix_match = re.search(r"([\w.]+)$", text_before)
        prefix = prefix_match.group(1) if prefix_match else ""

        # Dot-trigger: scope completions
        if "." in prefix:
            return self._complete_dot(prefix, catalog_context)

        # Determine context: check text before the current word
        text_before_prefix = text_before[: len(text_before) - len(prefix)]
        is_from_context = bool(_FROM_CONTEXT_RE.search(text_before_prefix))

        candidates: list[Completion] = []

        # Source 1: Pipeline joints
        candidates.extend(self._joint_completions(prefix))

        # Source 2: Catalog tables (prioritized in FROM context)
        candidates.extend(self._catalog_completions(prefix, catalog_context))

        # Source 3: SQL keywords (deprioritized in FROM context)
        if not is_from_context:
            candidates.extend(self._keyword_completions(prefix))

        # Source 4: Snippets
        candidates.extend(self._snippet_completions(prefix))

        return self._filter_and_sort(prefix, candidates)

    def _complete_dot(self, prefix: str, catalog_context: str | None) -> list[Completion]:
        """Handle dot-triggered completions: catalog.schema, catalog.schema.table, table.column."""
        parts = prefix.split(".")
        typed = parts[-1]  # text after last dot
        scope = parts[:-1]

        candidates: list[Completion] = []

        if len(scope) == 1:
            scope_name = scope[0]
            # Could be catalog name → show schemas
            # Or could be table/joint name → show columns
            candidates.extend(self._schema_completions(scope_name, typed))
            candidates.extend(self._column_completions(scope_name, typed))

        elif len(scope) == 2:
            catalog_name, schema_name = scope
            # catalog.schema. → show tables
            candidates.extend(self._table_completions(catalog_name, schema_name, typed))

        elif len(scope) == 3:
            catalog_name, schema_name, table_name = scope
            # catalog.schema.table. → show columns
            candidates.extend(
                self._catalog_column_completions(catalog_name, schema_name, table_name, typed)
            )

        return self._filter_and_sort(typed, candidates)

    def _joint_completions(self, prefix: str) -> list[Completion]:
        results: list[Completion] = []
        for j in self._joint_entries:
            icon = {"source": "⚪", "sql": "🔵", "python": "🟣", "sink": "🟢"}.get(j.joint_type, "")
            detail = f"{icon} joint — {j.joint_type}"
            if j.engine:
                detail += f", {j.engine}"
            results.append(
                Completion(
                    label=j.name,
                    insert_text=j.name,
                    kind=CompletionKind.JOINT,
                    detail=detail,
                    sort_key=_SORT_JOINT,
                    source=None,
                )
            )
        return results

    def _catalog_completions(self, prefix: str, catalog_context: str | None) -> list[Completion]:
        results: list[Completion] = []
        seen: set[str] = set()
        for entry in self._catalog_entries:
            if entry.table and entry.table not in seen:
                seen.add(entry.table)
                qualified = (
                    f"{entry.catalog}.{entry.schema}.{entry.table}"
                    if entry.schema
                    else f"{entry.catalog}.{entry.table}"
                )
                results.append(
                    Completion(
                        label=entry.table,
                        insert_text=entry.table,
                        kind=CompletionKind.CATALOG_TABLE,
                        detail=f"📁 {qualified}",
                        sort_key=_SORT_CATALOG,
                        source=entry.catalog,
                    )
                )
            # Also offer catalog names
            if entry.catalog not in seen:
                seen.add(entry.catalog)
                results.append(
                    Completion(
                        label=entry.catalog,
                        insert_text=entry.catalog,
                        kind=CompletionKind.CATALOG_NAME,
                        detail="catalog",
                        sort_key=_SORT_CATALOG,
                        source=entry.catalog,
                    )
                )
        return results

    def _keyword_completions(self, prefix: str) -> list[Completion]:
        return [
            Completion(
                label=kw,
                insert_text=kw,
                kind=CompletionKind.SQL_KEYWORD,
                detail="SQL keyword",
                sort_key=_SORT_KEYWORD,
                source=None,
            )
            for kw in _SQL_KEYWORDS
        ]

    def _snippet_completions(self, prefix: str) -> list[Completion]:
        return [
            Completion(
                label=trigger,
                insert_text=body,
                kind=CompletionKind.SNIPPET,
                detail="snippet",
                sort_key=_SORT_SNIPPET,
                source=None,
            )
            for trigger, body in _SNIPPETS
        ]

    def _schema_completions(self, catalog_name: str, typed: str) -> list[Completion]:
        """Schemas within a catalog."""
        results: list[Completion] = []
        seen: set[str] = set()
        for entry in self._catalog_entries:
            if (
                entry.catalog.lower() == catalog_name.lower()
                and entry.schema
                and entry.schema not in seen
            ):
                seen.add(entry.schema)
                results.append(
                    Completion(
                        label=entry.schema,
                        insert_text=entry.schema,
                        kind=CompletionKind.CATALOG_SCHEMA,
                        detail=f"schema in {entry.catalog}",
                        sort_key=_SORT_CATALOG,
                        source=entry.catalog,
                    )
                )
        return results

    def _column_completions(self, table_or_joint: str, typed: str) -> list[Completion]:
        """Columns for a joint or catalog table by short name."""
        results: list[Completion] = []
        # Check joints first
        for j in self._joint_entries:
            if j.name.lower() == table_or_joint.lower() and j.columns:
                for col_name, col_type in j.columns:
                    results.append(
                        Completion(
                            label=col_name,
                            insert_text=col_name,
                            kind=CompletionKind.COLUMN,
                            detail=col_type,
                            sort_key=_SORT_JOINT,
                            source=j.name,
                        )
                    )
                return results
        # Check catalog tables
        for entry in self._catalog_entries:
            if entry.table and entry.table.lower() == table_or_joint.lower() and entry.columns:
                for col_name, col_type in entry.columns:
                    results.append(
                        Completion(
                            label=col_name,
                            insert_text=col_name,
                            kind=CompletionKind.COLUMN,
                            detail=col_type,
                            sort_key=_SORT_CATALOG,
                            source=entry.catalog,
                        )
                    )
                return results
        return results

    def _table_completions(
        self, catalog_name: str, schema_name: str, typed: str
    ) -> list[Completion]:
        """Tables within a catalog.schema."""
        results: list[Completion] = []
        for entry in self._catalog_entries:
            if (
                entry.catalog.lower() == catalog_name.lower()
                and entry.schema
                and entry.schema.lower() == schema_name.lower()
                and entry.table
            ):
                results.append(
                    Completion(
                        label=entry.table,
                        insert_text=entry.table,
                        kind=CompletionKind.CATALOG_TABLE,
                        detail=f"table in {catalog_name}.{schema_name}",
                        sort_key=_SORT_CATALOG,
                        source=entry.catalog,
                    )
                )
        return results

    def _catalog_column_completions(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        typed: str,
    ) -> list[Completion]:
        """Columns for a fully-qualified catalog table."""
        results: list[Completion] = []
        for entry in self._catalog_entries:
            if (
                entry.catalog.lower() == catalog_name.lower()
                and entry.schema
                and entry.schema.lower() == schema_name.lower()
                and entry.table
                and entry.table.lower() == table_name.lower()
                and entry.columns
            ):
                for col_name, col_type in entry.columns:
                    results.append(
                        Completion(
                            label=col_name,
                            insert_text=col_name,
                            kind=CompletionKind.COLUMN,
                            detail=col_type,
                            sort_key=_SORT_CATALOG,
                            source=entry.catalog,
                        )
                    )
        return results

    @staticmethod
    def _filter_and_sort(prefix: str, candidates: list[Completion]) -> list[Completion]:
        """Filter by fuzzy match on prefix, then sort by (sort_key, match_score)."""
        if not prefix:
            candidates.sort(key=lambda c: (c.sort_key, c.label.lower()))
            return candidates

        scored: list[tuple[float, Completion]] = []
        for c in candidates:
            result = fuzzy_match(prefix, c.label)
            if result is not None:
                ms, _positions = result
                scored.append((ms, c))

        scored.sort(key=lambda x: (x[1].sort_key, x[0], x[1].label.lower()))
        return [c for _, c in scored]
