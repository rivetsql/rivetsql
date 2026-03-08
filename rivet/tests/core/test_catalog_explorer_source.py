"""Unit and property tests for CatalogExplorer.generate_source().

Also contains:
Feature: catalog-explorer, Property 15: Source generation YAML round-trip
Feature: catalog-explorer, Property 16: Column filtering in source generation

Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.5, 7.6
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.catalog_explorer import (
    RVT_874,
    CatalogExplorer,
    CatalogExplorerError,
    GeneratedSource,
    sanitize_name,
)
from rivet_core.introspection import ColumnDetail, ObjectSchema
from rivet_core.models import Catalog

# ── Helpers ───────────────────────────────────────────────────────────────


def _make_plugin(*, list_tables_return=None, schema=None):
    plugin = MagicMock()
    plugin.list_tables.return_value = list_tables_return or []
    plugin.list_children.side_effect = Exception("not overridden")
    if schema is not None:
        plugin.get_schema.return_value = schema
    else:
        plugin.get_schema.side_effect = Exception("no schema")
    return plugin


def _make_registry(*catalog_plugins):
    registry = MagicMock()
    plugin_map = {t: p for t, p in catalog_plugins}
    registry.get_catalog_plugin.side_effect = lambda t: plugin_map.get(t)
    return registry


def _col(name, type_="string", nullable=True):
    return ColumnDetail(
        name=name, type=type_, native_type=None, nullable=nullable,
        default=None, comment=None, is_primary_key=False, is_partition_key=False,
    )


def _schema(columns, path=None):
    return ObjectSchema(
        path=path or ["public", "users"],
        node_type="table",
        columns=columns,
        primary_key=None,
        comment=None,
    )


# ── YAML generation tests ────────────────────────────────────────────────


class TestGenerateSourceYaml:
    def test_basic_yaml(self):
        cols = [_col("id", "int64"), _col("name", "string")]
        schema = _schema(cols)
        plugin = _make_plugin(list_tables_return=[], schema=schema)
        registry = _make_registry(("postgres", plugin))
        catalog = Catalog(name="mydb", type="postgres")
        explorer = CatalogExplorer({"mydb": catalog}, {}, registry)

        result = explorer.generate_source(["mydb", "public", "users"], format="yaml")

        assert isinstance(result, GeneratedSource)
        assert result.format == "yaml"
        assert result.catalog_name == "mydb"
        assert result.table_name == "public.users"
        assert result.column_count == 2
        assert result.suggested_filename == "raw_users.yaml"
        assert "name: raw_users" in result.content
        assert "type: source" in result.content
        assert "catalog: mydb" in result.content
        assert "table: public.users" in result.content
        assert "  - name: id" in result.content
        assert "    type: int64" in result.content
        assert "  - name: name" in result.content
        assert "    type: string" in result.content
        assert "upstream: []" in result.content

    def test_yaml_is_default_format(self):
        cols = [_col("x")]
        plugin = _make_plugin(list_tables_return=[], schema=_schema(cols))
        registry = _make_registry(("postgres", plugin))
        catalog = Catalog(name="db", type="postgres")
        explorer = CatalogExplorer({"db": catalog}, {}, registry)

        result = explorer.generate_source(["db", "public", "users"])
        assert result.format == "yaml"


# ── SQL generation tests ─────────────────────────────────────────────────


class TestGenerateSourceSql:
    def test_basic_sql(self):
        cols = [_col("id", "int64"), _col("email", "string")]
        schema = _schema(cols)
        plugin = _make_plugin(list_tables_return=[], schema=schema)
        registry = _make_registry(("postgres", plugin))
        catalog = Catalog(name="mydb", type="postgres")
        explorer = CatalogExplorer({"mydb": catalog}, {}, registry)

        result = explorer.generate_source(["mydb", "public", "users"], format="sql")

        assert result.format == "sql"
        assert result.suggested_filename == "raw_users.sql"
        assert "-- rivet:name: raw_users" in result.content
        assert "-- rivet:type: source" in result.content
        assert "-- rivet:catalog: mydb" in result.content
        assert "-- rivet:table: public.users" in result.content


# ── Column filtering tests ───────────────────────────────────────────────


class TestGenerateSourceColumnFiltering:
    def test_filter_columns(self):
        cols = [_col("id"), _col("name"), _col("email")]
        plugin = _make_plugin(list_tables_return=[], schema=_schema(cols))
        registry = _make_registry(("postgres", plugin))
        catalog = Catalog(name="db", type="postgres")
        explorer = CatalogExplorer({"db": catalog}, {}, registry)

        result = explorer.generate_source(
            ["db", "public", "users"], columns=["id", "email"],
        )
        assert result.column_count == 2
        assert "  - name: id" in result.content
        assert "  - name: email" in result.content
        assert "  - name: name" not in result.content

    def test_unrecognized_columns_warned(self, caplog):
        cols = [_col("id"), _col("name")]
        plugin = _make_plugin(list_tables_return=[], schema=_schema(cols))
        registry = _make_registry(("postgres", plugin))
        catalog = Catalog(name="db", type="postgres")
        explorer = CatalogExplorer({"db": catalog}, {}, registry)

        with caplog.at_level(logging.WARNING):
            result = explorer.generate_source(
                ["db", "public", "users"], columns=["id", "bogus"],
            )
        assert result.column_count == 1  # only "id" recognized
        assert "bogus" in caplog.text

    def test_all_unrecognized_columns_produces_empty(self, caplog):
        cols = [_col("id")]
        plugin = _make_plugin(list_tables_return=[], schema=_schema(cols))
        registry = _make_registry(("postgres", plugin))
        catalog = Catalog(name="db", type="postgres")
        explorer = CatalogExplorer({"db": catalog}, {}, registry)

        with caplog.at_level(logging.WARNING):
            result = explorer.generate_source(
                ["db", "public", "users"], columns=["nope"],
            )
        assert result.column_count == 0


# ── Name sanitization tests ──────────────────────────────────────────────


class TestNameSanitization:
    def test_basic_sanitization(self):
        assert sanitize_name("Users") == "raw_users"

    def test_special_chars_replaced(self):
        assert sanitize_name("my-table.v2") == "raw_my_table_v2"

    def test_prefix_raw(self):
        assert sanitize_name("orders").startswith("raw_")

    def test_truncation_128(self):
        long_name = "a" * 200
        result = sanitize_name(long_name)
        assert len(result) <= 128

    def test_unicode_sanitized(self):
        result = sanitize_name("café_données")
        assert result == "raw_caf__donn_es"
        assert result.isascii() or all(c.isalnum() or c == "_" for c in result)


# ── Error handling tests ─────────────────────────────────────────────────


class TestGenerateSourceErrors:
    def test_no_schema_raises_rvt874(self):
        plugin = _make_plugin(list_tables_return=[])
        registry = _make_registry(("postgres", plugin))
        catalog = Catalog(name="db", type="postgres")
        explorer = CatalogExplorer({"db": catalog}, {}, registry)

        with pytest.raises(CatalogExplorerError) as exc_info:
            explorer.generate_source(["db", "public", "missing"])
        assert exc_info.value.error.code == RVT_874

    def test_disconnected_catalog_raises_rvt874(self):
        plugin = MagicMock()
        plugin.list_tables.side_effect = Exception("connection refused")
        plugin.get_schema.side_effect = Exception("connection refused")
        registry = _make_registry(("postgres", plugin))
        catalog = Catalog(name="db", type="postgres")
        explorer = CatalogExplorer({"db": catalog}, {}, registry)

        with pytest.raises(CatalogExplorerError) as exc_info:
            explorer.generate_source(["db", "public", "t"])
        assert exc_info.value.error.code == RVT_874


# ── Property tests ────────────────────────────────────────────────────────

# Hypothesis strategies
_YAML_RESERVED = frozenset({
    "null", "true", "false", "yes", "no", "on", "off",
    "NULL", "True", "False", "YES", "NO", "ON", "OFF",
    "TRUE", "FALSE",
    "Null", "Yes", "No", "On", "Off",
    "y", "Y", "n", "N",
    "~",
})

_col_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=32,
).filter(lambda s: s[0].isalpha() and s not in _YAML_RESERVED)

_col_type = st.sampled_from(["string", "int64", "float64", "bool", "date", "timestamp"])

_column_strategy = st.builds(
    lambda name, type_: _col(name, type_),
    name=_col_name,
    type_=_col_type,
)

_columns_strategy = st.lists(_column_strategy, min_size=1, max_size=10).map(
    lambda cols: list({c.name: c for c in cols}.values())  # deduplicate by name
)


def _make_explorer_with_schema(columns):
    schema = _schema(columns, path=["public", "tbl"])
    plugin = _make_plugin(list_tables_return=[], schema=schema)
    registry = _make_registry(("postgres", plugin))
    catalog = Catalog(name="mydb", type="postgres")
    return CatalogExplorer({"mydb": catalog}, {}, registry)


@settings(max_examples=100)
@given(columns=_columns_strategy)
def test_property_15_yaml_round_trip(columns):
    """Feature: catalog-explorer, Property 15: Source generation YAML round-trip.

    For any valid table path with a known schema, generating a YAML source via
    generate_source(path, format="yaml") and parsing the output should produce a
    valid source joint declaration with correct catalog, table, and column information.

    Validates: Requirements 7.1, 7.6
    """
    explorer = _make_explorer_with_schema(columns)
    result = explorer.generate_source(["mydb", "public", "tbl"], format="yaml")

    # Must parse as valid YAML
    parsed = yaml.safe_load(result.content)
    assert isinstance(parsed, dict)

    # Must have required fields for a source joint declaration
    assert parsed["type"] == "source"
    assert "name" in parsed
    assert parsed["catalog"] == "mydb"
    assert parsed["table"] == "public.tbl"

    # Columns must be present and match the schema
    assert "columns" in parsed
    assert isinstance(parsed["columns"], list)
    assert len(parsed["columns"]) == len(columns)

    col_names_in_yaml = {c["name"] for c in parsed["columns"]}
    expected_names = {c.name for c in columns}
    assert col_names_in_yaml == expected_names

    # Each column must have name and type
    for col_entry in parsed["columns"]:
        assert "name" in col_entry
        assert "type" in col_entry


@settings(max_examples=100)
@given(columns=_columns_strategy)
def test_property_16_column_filtering(columns):
    """Feature: catalog-explorer, Property 16: Column filtering in source generation.

    For any valid column subset of a table's schema, generate_source(path, columns=subset)
    should produce a source declaration containing exactly those columns and no others.

    Validates: Requirements 7.4
    """
    explorer = _make_explorer_with_schema(columns)

    # Pick a non-empty subset of column names
    all_names = [c.name for c in columns]
    # Use first half (at least 1) as the subset
    subset_size = max(1, len(all_names) // 2)
    subset = all_names[:subset_size]

    result = explorer.generate_source(
        ["mydb", "public", "tbl"], format="yaml", columns=subset,
    )

    assert result.column_count == len(subset)

    parsed = yaml.safe_load(result.content)
    col_names_in_yaml = {c["name"] for c in parsed["columns"]}
    assert col_names_in_yaml == set(subset)

    # Columns NOT in subset must not appear
    excluded = set(all_names) - set(subset)
    for name in excluded:
        assert name not in col_names_in_yaml
