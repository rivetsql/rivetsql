"""Tests for catalog_describe handler.

Also contains:
Feature: catalog-explorer, Property 22: Describe output contains all columns and metadata

Property 22: Describe output contains all columns and metadata.
For any table with a known schema, `rivet catalog describe` should include every column
name, type, and nullability from the schema, plus metadata fields when available.
Validates: Requirements 10.1

Requirements: 10.1, 10.2, 10.3, 10.4
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.catalog import catalog_describe
from rivet_cli.exit_codes import SUCCESS, USAGE_ERROR
from rivet_core.catalog_explorer import CatalogInfo, ExplorerNode, NodeDetail
from rivet_core.introspection import ColumnDetail, ObjectMetadata, ObjectSchema


def _globals(**overrides) -> GlobalOptions:
    defaults = dict(profile="default", project_path=Path("."), verbosity=0, color=False)
    defaults.update(overrides)
    return GlobalOptions(**defaults)


def _make_explorer(
    catalogs: list[CatalogInfo] | None = None,
    detail: NodeDetail | None = None,
    stats: ObjectMetadata | None = None,
) -> MagicMock:
    explorer = MagicMock()
    explorer.list_catalogs.return_value = catalogs or [
        CatalogInfo(name="mycat", catalog_type="duckdb", connected=True, error=None)
    ]
    explorer.get_node_detail.return_value = detail
    explorer.get_table_stats.return_value = stats
    return explorer


def _make_schema(*col_names: str) -> ObjectSchema:
    cols = [
        ColumnDetail(
            name=n, type="text", native_type=None, nullable=True,
            default=None, comment=None, is_primary_key=False, is_partition_key=False,
        )
        for n in col_names
    ]
    return ObjectSchema(path=[], node_type="table", columns=cols, primary_key=None, comment=None)


def _make_node(path: list[str]) -> ExplorerNode:
    return ExplorerNode(
        name=path[-1],
        node_type="table",
        path=path,
        is_expandable=False,
        depth=len(path) - 1,
        summary=None,
        depth_limit_reached=False,
    )


def _make_detail(path: list[str], col_names: tuple[str, ...] = ("id", "name")) -> NodeDetail:
    schema = _make_schema(*col_names)
    return NodeDetail(
        node=_make_node(path),
        schema=schema,
        metadata=None,
        children_count=len(col_names),
    )


class TestCatalogDescribePathParsing:
    """Tests for path argument parsing (Req 10.4)."""

    def test_single_segment_path_returns_usage_error(self, capsys):
        """Path with only one segment (no dot) → USAGE_ERROR with RVT-871."""
        explorer = _make_explorer()
        code = catalog_describe(explorer, "mycat", stats=False, format="text", globals=_globals())
        assert code == USAGE_ERROR
        err = capsys.readouterr().err
        assert "RVT-871" in err

    def test_unknown_catalog_returns_usage_error(self, capsys):
        """Unknown catalog name → USAGE_ERROR with RVT-871."""
        explorer = _make_explorer(
            catalogs=[CatalogInfo(name="mycat", catalog_type="duckdb", connected=True, error=None)]
        )
        code = catalog_describe(explorer, "other.schema.table", stats=False, format="text", globals=_globals())
        assert code == USAGE_ERROR
        err = capsys.readouterr().err
        assert "RVT-871" in err

    def test_get_node_detail_exception_returns_usage_error(self, capsys):
        """get_node_detail raises → USAGE_ERROR with RVT-871."""
        explorer = _make_explorer()
        explorer.get_node_detail.side_effect = RuntimeError("not found")
        code = catalog_describe(explorer, "mycat.schema.table", stats=False, format="text", globals=_globals())
        assert code == USAGE_ERROR
        err = capsys.readouterr().err
        assert "RVT-871" in err

    def test_none_schema_returns_usage_error(self, capsys):
        """get_node_detail returns NodeDetail with schema=None → USAGE_ERROR."""
        path = ["mycat", "schema", "table"]
        detail = NodeDetail(node=_make_node(path), schema=None, metadata=None, children_count=None)
        explorer = _make_explorer(detail=detail)
        code = catalog_describe(explorer, "mycat.schema.table", stats=False, format="text", globals=_globals())
        assert code == USAGE_ERROR
        err = capsys.readouterr().err
        assert "RVT-871" in err


class TestCatalogDescribeTextFormat:
    """Tests for text format output (Req 10.1)."""

    def test_displays_column_names(self, capsys):
        """Text output includes column names (Req 10.1)."""
        path = ["mycat", "public", "users"]
        detail = _make_detail(path, ("id", "email"))
        explorer = _make_explorer(detail=detail)

        code = catalog_describe(explorer, "mycat.public.users", stats=False, format="text", globals=_globals())

        assert code == SUCCESS
        out = capsys.readouterr().out
        assert "id" in out
        assert "email" in out

    def test_calls_get_node_detail_with_parsed_path(self):
        """get_node_detail called with correct path segments."""
        path = ["mycat", "public", "orders"]
        detail = _make_detail(path)
        explorer = _make_explorer(detail=detail)

        catalog_describe(explorer, "mycat.public.orders", stats=False, format="text", globals=_globals())

        explorer.get_node_detail.assert_called_once_with(["mycat", "public", "orders"])

    def test_two_segment_path_works(self, capsys):
        """Two-segment path (catalog.table) is valid."""
        path = ["mycat", "mytable"]
        detail = _make_detail(path, ("col1",))
        explorer = _make_explorer(detail=detail)

        code = catalog_describe(explorer, "mycat.mytable", stats=False, format="text", globals=_globals())

        assert code == SUCCESS
        out = capsys.readouterr().out
        assert "col1" in out

    def test_returns_success(self):
        """Successful describe returns SUCCESS."""
        path = ["mycat", "s", "t"]
        detail = _make_detail(path)
        explorer = _make_explorer(detail=detail)

        code = catalog_describe(explorer, "mycat.s.t", stats=False, format="text", globals=_globals())
        assert code == SUCCESS


class TestCatalogDescribeJsonFormat:
    """Tests for JSON format output (Req 10.3)."""

    def test_json_output_is_valid_json(self, capsys):
        """--format json produces valid JSON (Req 10.3)."""
        path = ["mycat", "public", "users"]
        detail = _make_detail(path, ("id", "name"))
        explorer = _make_explorer(detail=detail)

        code = catalog_describe(explorer, "mycat.public.users", stats=False, format="json", globals=_globals())

        assert code == SUCCESS
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert "columns" in parsed

    def test_json_output_contains_all_columns(self, capsys):
        """JSON output contains all column names (Req 10.1, 10.3)."""
        path = ["mycat", "public", "events"]
        detail = _make_detail(path, ("event_id", "user_id", "ts"))
        explorer = _make_explorer(detail=detail)

        catalog_describe(explorer, "mycat.public.events", stats=False, format="json", globals=_globals())

        out = capsys.readouterr().out
        parsed = json.loads(out)
        col_names = [c["name"] for c in parsed["columns"]]
        assert "event_id" in col_names
        assert "user_id" in col_names
        assert "ts" in col_names


class TestCatalogDescribeStats:
    """Tests for --stats flag (Req 10.2)."""

    def test_stats_flag_calls_get_table_stats(self):
        """--stats triggers get_table_stats call (Req 10.2)."""
        path = ["mycat", "public", "users"]
        detail = _make_detail(path)
        explorer = _make_explorer(detail=detail)

        catalog_describe(explorer, "mycat.public.users", stats=True, format="text", globals=_globals())

        explorer.get_table_stats.assert_called_once_with(["mycat", "public", "users"])

    def test_no_stats_flag_skips_get_table_stats(self):
        """Without --stats, get_table_stats is not called."""
        path = ["mycat", "public", "users"]
        detail = _make_detail(path)
        explorer = _make_explorer(detail=detail)

        catalog_describe(explorer, "mycat.public.users", stats=False, format="text", globals=_globals())

        explorer.get_table_stats.assert_not_called()

    def test_stats_metadata_used_in_output(self, capsys):
        """When stats returns metadata with row_count, it appears in text output."""
        path = ["mycat", "public", "users"]
        detail = _make_detail(path)
        stats_meta = ObjectMetadata(
            path=path,
            node_type="table",
            row_count=42,
            size_bytes=None,
            last_modified=None,
            created_at=None,
            format=None,
            compression=None,
            owner=None,
            comment=None,
            location=None,
            column_statistics=[],
            partitioning=None,
        )
        explorer = _make_explorer(detail=detail, stats=stats_meta)

        code = catalog_describe(explorer, "mycat.public.users", stats=True, format="text", globals=_globals())

        assert code == SUCCESS
        out = capsys.readouterr().out
        assert "42" in out


# ---------------------------------------------------------------------------
# Property 22: Describe output contains all columns and metadata
# ---------------------------------------------------------------------------

_SAFE_TEXT = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=20,
)

_COLUMN_TYPES = st.sampled_from(["int64", "utf8", "float64", "bool", "date32", "timestamp"])


@st.composite
def _column_detail_strategy(draw):
    name = draw(_SAFE_TEXT)
    col_type = draw(_COLUMN_TYPES)
    nullable = draw(st.booleans())
    return ColumnDetail(
        name=name,
        type=col_type,
        native_type=None,
        nullable=nullable,
        default=None,
        comment=None,
        is_primary_key=False,
        is_partition_key=False,
    )


@st.composite
def _schema_strategy(draw):
    cols = draw(st.lists(_column_detail_strategy(), min_size=1, max_size=10))
    # Ensure unique column names
    seen: set[str] = set()
    unique_cols = []
    for col in cols:
        if col.name not in seen:
            seen.add(col.name)
            unique_cols.append(col)
    if not unique_cols:
        unique_cols = [ColumnDetail(name="id", type="int64", native_type=None, nullable=False, default=None, comment=None, is_primary_key=True, is_partition_key=False)]
    return ObjectSchema(path=["cat", "s", "t"], node_type="table", columns=unique_cols, primary_key=None, comment=None)


def _run_describe(schema: ObjectSchema, fmt: str) -> tuple[int, str]:
    """Run catalog_describe and capture stdout, returning (exit_code, output)."""
    import io
    import sys

    path = ["cat", "s", "t"]
    node = ExplorerNode(
        name="t", node_type="table", path=path,
        is_expandable=False, depth=2, summary=None, depth_limit_reached=False,
    )
    detail = NodeDetail(node=node, schema=schema, metadata=None, children_count=len(schema.columns))
    explorer = MagicMock()
    explorer.list_catalogs.return_value = [
        CatalogInfo(name="cat", catalog_type="duckdb", connected=True, error=None)
    ]
    explorer.get_node_detail.return_value = detail

    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        code = catalog_describe(explorer, "cat.s.t", stats=False, format=fmt, globals=_globals())
    finally:
        sys.stdout = old_stdout
    return code, buf.getvalue()


class TestProperty22DescribeCompleteness:
    """Feature: catalog-explorer, Property 22: Describe output contains all columns and metadata.

    For any table with a known schema, rivet catalog describe should include every column
    name, type, and nullability from the schema in both text and JSON output.
    Validates: Requirements 10.1
    """

    @given(schema=_schema_strategy())
    @settings(max_examples=50)
    def test_text_output_contains_all_column_names(self, schema):
        """Text output includes every column name from the schema (Req 10.1)."""
        code, out = _run_describe(schema, "text")
        assert code == SUCCESS
        for col in schema.columns:
            assert col.name in out, f"Column '{col.name}' missing from text output"

    @given(schema=_schema_strategy())
    @settings(max_examples=50)
    def test_text_output_contains_all_column_types(self, schema):
        """Text output includes every column type from the schema (Req 10.1)."""
        code, out = _run_describe(schema, "text")
        assert code == SUCCESS
        for col in schema.columns:
            assert col.type in out, f"Column type '{col.type}' missing from text output"

    @given(schema=_schema_strategy())
    @settings(max_examples=50)
    def test_json_output_contains_all_columns(self, schema):
        """JSON output contains all column names, types, and nullability (Req 10.1, 10.3)."""
        code, out = _run_describe(schema, "json")
        assert code == SUCCESS
        parsed = json.loads(out)
        assert "columns" in parsed
        col_names_in_output = [c["name"] for c in parsed["columns"]]
        for col in schema.columns:
            assert col.name in col_names_in_output, f"Column '{col.name}' missing from JSON output"
            matching = next(c for c in parsed["columns"] if c["name"] == col.name)
            assert matching["type"] == col.type
            assert matching["nullable"] == col.nullable
