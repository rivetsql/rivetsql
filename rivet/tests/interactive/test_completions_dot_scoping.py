"""Property test for CompletionEngine — dot-triggered completion scoping.

# Feature: cli-repl, Property 12: Dot-triggered completion scoping
# Validates: Requirements 8.4
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.completions import CompletionEngine
from rivet_core.interactive.types import CompletionKind

# Strategy for valid identifiers (no dots, no spaces)
_ident = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)


def _make_catalog_entry(
    catalog: str, schema: str, table: str, columns: list[tuple[str, str]] | None = None
) -> dict:
    return {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "columns": columns,
    }


class TestDotTriggeredCompletionScoping:
    """Property 12: Dot-triggered completion scoping (Requirement 8.4)."""

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_catalog_dot_returns_schemas(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """Typing <catalog>. scopes completions to schemas within that catalog."""
        engine = CompletionEngine()
        engine.update_catalogs([_make_catalog_entry(catalog, schema, table)])

        sql = f"SELECT * FROM {catalog}."
        completions = engine.complete(sql, len(sql))

        schema_labels = [c.label for c in completions if c.kind == CompletionKind.CATALOG_SCHEMA]
        assert schema in schema_labels

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_catalog_dot_returns_only_schemas_for_that_catalog(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """Typing <catalog>. returns schemas only from that catalog, not others."""
        engine = CompletionEngine()
        other_catalog = catalog + "_other"
        other_schema = schema + "_other"
        engine.update_catalogs([
            _make_catalog_entry(catalog, schema, table),
            _make_catalog_entry(other_catalog, other_schema, table),
        ])

        sql = f"SELECT * FROM {catalog}."
        completions = engine.complete(sql, len(sql))

        schema_labels = [c.label for c in completions if c.kind == CompletionKind.CATALOG_SCHEMA]
        assert schema in schema_labels
        assert other_schema not in schema_labels

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_catalog_schema_dot_returns_tables(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """Typing <catalog>.<schema>. scopes completions to tables within that schema."""
        engine = CompletionEngine()
        engine.update_catalogs([_make_catalog_entry(catalog, schema, table)])

        sql = f"SELECT * FROM {catalog}.{schema}."
        completions = engine.complete(sql, len(sql))

        table_labels = [c.label for c in completions if c.kind == CompletionKind.CATALOG_TABLE]
        assert table in table_labels

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_catalog_schema_dot_returns_only_tables_for_that_schema(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """Typing <catalog>.<schema>. returns tables only from that schema."""
        engine = CompletionEngine()
        other_schema = schema + "_other"
        other_table = table + "_other"
        engine.update_catalogs([
            _make_catalog_entry(catalog, schema, table),
            _make_catalog_entry(catalog, other_schema, other_table),
        ])

        sql = f"SELECT * FROM {catalog}.{schema}."
        completions = engine.complete(sql, len(sql))

        table_labels = [c.label for c in completions if c.kind == CompletionKind.CATALOG_TABLE]
        assert table in table_labels
        assert other_table not in table_labels

    @given(catalog=_ident, schema=_ident, table=_ident, col=_ident)
    @settings(max_examples=100)
    def test_table_dot_returns_columns(
        self, catalog: str, schema: str, table: str, col: str
    ) -> None:
        """Typing <table>. returns column completions for that table."""
        engine = CompletionEngine()
        engine.update_catalogs([
            _make_catalog_entry(catalog, schema, table, columns=[(col, "int64")])
        ])

        sql = f"SELECT {table}."
        completions = engine.complete(sql, len(sql))

        col_labels = [c.label for c in completions if c.kind == CompletionKind.COLUMN]
        assert col in col_labels

    @given(catalog=_ident, schema=_ident, table=_ident, col=_ident)
    @settings(max_examples=100)
    def test_joint_dot_returns_columns(
        self, catalog: str, schema: str, table: str, col: str
    ) -> None:
        """Typing <joint_name>. returns column completions for that joint."""
        joint_name = "my_joint_" + table
        engine = CompletionEngine()
        engine.update_assembly([
            {"name": joint_name, "joint_type": "sql", "columns": [(col, "string")]}
        ])

        sql = f"SELECT {joint_name}."
        completions = engine.complete(sql, len(sql))

        col_labels = [c.label for c in completions if c.kind == CompletionKind.COLUMN]
        assert col in col_labels
