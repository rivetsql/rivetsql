"""Tests for catalog_json.py rendering module.

Also contains:
Feature: catalog-explorer, Property 20: CLI JSON output is valid JSON

Property 20: CLI JSON output is valid JSON with expected schema.
Validates: Requirements 9.4, 10.3, 11.3
"""

from __future__ import annotations

import json

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.rendering.catalog_json import (
    render_describe_json,
    render_list_json,
    render_search_json,
)
from rivet_core.catalog_explorer import (
    CatalogInfo,
    ExplorerNode,
    NodeDetail,
    SearchResult,
)
from rivet_core.introspection import ColumnDetail, ObjectMetadata, ObjectSchema

# ---------------------------------------------------------------------------
# render_list_json
# ---------------------------------------------------------------------------


def test_render_list_json_valid_json():
    catalogs = [
        CatalogInfo(name="pg", catalog_type="postgres", connected=True, error=None),
        CatalogInfo(name="s3", catalog_type="s3", connected=False, error="timeout"),
    ]
    output = render_list_json(catalogs)
    parsed = json.loads(output)
    assert "catalogs" in parsed
    assert len(parsed["catalogs"]) == 2


def test_render_list_json_schema():
    catalogs = [CatalogInfo(name="pg", catalog_type="postgres", connected=True, error=None)]
    parsed = json.loads(render_list_json(catalogs))
    entry = parsed["catalogs"][0]
    assert entry["name"] == "pg"
    assert entry["type"] == "postgres"
    assert entry["connected"] is True
    assert "children" in entry


def test_render_list_json_with_children():
    catalogs = [CatalogInfo(name="pg", catalog_type="postgres", connected=True, error=None)]
    node = ExplorerNode(
        name="public",
        node_type="schema",
        path=["pg", "public"],
        is_expandable=True,
        depth=1,
        summary=None,
        depth_limit_reached=False,
    )
    parsed = json.loads(render_list_json(catalogs, children={"pg": [node]}))
    children = parsed["catalogs"][0]["children"]
    assert len(children) == 1
    assert children[0]["name"] == "public"
    assert children[0]["node_type"] == "schema"


def test_render_list_json_empty():
    parsed = json.loads(render_list_json([]))
    assert parsed == {"catalogs": []}


# ---------------------------------------------------------------------------
# render_describe_json
# ---------------------------------------------------------------------------


def _make_detail(with_schema: bool = True, with_metadata: bool = True) -> NodeDetail:
    node = ExplorerNode(
        name="orders",
        node_type="table",
        path=["pg", "public", "orders"],
        is_expandable=False,
        depth=2,
        summary=None,
        depth_limit_reached=False,
    )
    schema = None
    if with_schema:
        schema = ObjectSchema(
            path=["pg", "public", "orders"],
            node_type="table",
            columns=[
                ColumnDetail(
                    name="id",
                    type="int64",
                    native_type="integer",
                    nullable=False,
                    default=None,
                    comment=None,
                    is_primary_key=True,
                    is_partition_key=False,
                ),
                ColumnDetail(
                    name="amount",
                    type="float64",
                    native_type="numeric",
                    nullable=True,
                    default=None,
                    comment=None,
                    is_primary_key=False,
                    is_partition_key=False,
                ),
            ],
            primary_key=["id"],
            comment=None,
        )
    metadata = None
    if with_metadata:
        metadata = ObjectMetadata(
            path=["pg", "public", "orders"],
            node_type="table",
            row_count=1000,
            size_bytes=4096,
            last_modified=None,
            created_at=None,
            format=None,
            compression=None,
            owner="admin",
            comment=None,
            location=None,
            column_statistics=[],
            partitioning=None,
        )
    return NodeDetail(node=node, schema=schema, metadata=metadata, children_count=2)


def test_render_describe_json_valid_json():
    output = render_describe_json(_make_detail())
    parsed = json.loads(output)
    assert "columns" in parsed
    assert "metadata" in parsed


def test_render_describe_json_columns():
    parsed = json.loads(render_describe_json(_make_detail()))
    cols = parsed["columns"]
    assert len(cols) == 2
    assert cols[0]["name"] == "id"
    assert cols[0]["type"] == "int64"
    assert cols[1]["name"] == "amount"


def test_render_describe_json_metadata():
    parsed = json.loads(render_describe_json(_make_detail()))
    meta = parsed["metadata"]
    assert meta["row_count"] == 1000
    assert meta["owner"] == "admin"


def test_render_describe_json_no_schema():
    detail = _make_detail(with_schema=False)
    parsed = json.loads(render_describe_json(detail))
    assert parsed["columns"] == []


def test_render_describe_json_no_metadata():
    detail = _make_detail(with_metadata=False)
    parsed = json.loads(render_describe_json(detail))
    assert parsed["metadata"] == {}


# ---------------------------------------------------------------------------
# render_search_json
# ---------------------------------------------------------------------------


def test_render_search_json_valid_json():
    results = [
        SearchResult(
            kind="table",
            qualified_name="pg.public.orders",
            short_name="orders",
            parent="pg.public",
            match_positions=[0, 1],
            score=-5.0,
            node_type="table",
        )
    ]
    output = render_search_json(results)
    parsed = json.loads(output)
    assert isinstance(parsed, list)
    assert len(parsed) == 1


def test_render_search_json_schema():
    results = [
        SearchResult(
            kind="table",
            qualified_name="pg.public.orders",
            short_name="orders",
            parent="pg.public",
            match_positions=[3, 7, 10],
            score=-3.5,
            node_type="table",
        )
    ]
    parsed = json.loads(render_search_json(results))
    entry = parsed[0]
    assert entry["qualified_name"] == "pg.public.orders"
    assert entry["kind"] == "table"
    assert entry["score"] == -3.5
    assert entry["match_positions"] == [3, 7, 10]


def test_render_search_json_empty():
    parsed = json.loads(render_search_json([]))
    assert parsed == []


def test_render_search_json_multiple():
    results = [
        SearchResult(
            kind="table",
            qualified_name=f"pg.public.table_{i}",
            short_name=f"table_{i}",
            parent="pg.public",
            match_positions=[0],
            score=float(i),
            node_type="table",
        )
        for i in range(5)
    ]
    parsed = json.loads(render_search_json(results))
    assert len(parsed) == 5
    assert all("qualified_name" in e for e in parsed)
    assert all("kind" in e for e in parsed)
    assert all("score" in e for e in parsed)
    assert all("match_positions" in e for e in parsed)


# ---------------------------------------------------------------------------
# Property 20: CLI JSON output is valid JSON with expected schema
# ---------------------------------------------------------------------------

_catalog_name = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz_",
    min_size=1,
    max_size=12,
)

_catalog_type = st.sampled_from(["postgres", "duckdb", "s3", "glue", "arrow"])

_col_name = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz_",
    min_size=1,
    max_size=10,
)

_col_type = st.sampled_from(["int64", "float64", "utf8", "bool", "date32"])


@given(
    catalog_names=st.lists(_catalog_name, min_size=0, max_size=5, unique=True),
    catalog_types=st.lists(_catalog_type, min_size=0, max_size=5),
    connected_flags=st.lists(st.booleans(), min_size=0, max_size=5),
)
@settings(max_examples=100)
def test_property_20_render_list_json_is_valid_json(
    catalog_names: list[str],
    catalog_types: list[str],
    connected_flags: list[bool],
) -> None:
    """Property 20 (list): render_list_json always produces valid JSON with expected schema.

    For any catalog data, render_list_json should produce output that parses as valid
    JSON with a 'catalogs' array where each entry has name, type, connected, children.

    Validates: Requirements 9.4
    """
    n = min(len(catalog_names), len(catalog_types), len(connected_flags))
    catalogs = [
        CatalogInfo(
            name=catalog_names[i],
            catalog_type=catalog_types[i],
            connected=connected_flags[i],
            error=None if connected_flags[i] else "connection refused",
        )
        for i in range(n)
    ]

    output = render_list_json(catalogs)
    parsed = json.loads(output)  # must not raise

    assert "catalogs" in parsed
    assert isinstance(parsed["catalogs"], list)
    assert len(parsed["catalogs"]) == n

    for entry in parsed["catalogs"]:
        assert "name" in entry
        assert "type" in entry
        assert "connected" in entry
        assert "children" in entry
        assert isinstance(entry["children"], list)


@given(
    col_names=st.lists(_col_name, min_size=0, max_size=8, unique=True),
    col_types=st.lists(_col_type, min_size=0, max_size=8),
    nullable_flags=st.lists(st.booleans(), min_size=0, max_size=8),
)
@settings(max_examples=100)
def test_property_20_render_describe_json_is_valid_json(
    col_names: list[str],
    col_types: list[str],
    nullable_flags: list[bool],
) -> None:
    """Property 20 (describe): render_describe_json always produces valid JSON with expected schema.

    For any schema data, render_describe_json should produce output that parses as valid
    JSON with 'columns' array and 'metadata' object.

    Validates: Requirements 10.3
    """
    n = min(len(col_names), len(col_types), len(nullable_flags))
    columns = [
        ColumnDetail(
            name=col_names[i],
            type=col_types[i],
            native_type=None,
            nullable=nullable_flags[i],
            default=None,
            comment=None,
            is_primary_key=False,
            is_partition_key=False,
        )
        for i in range(n)
    ]

    node = ExplorerNode(
        name="t",
        node_type="table",
        path=["cat", "schema", "t"],
        is_expandable=False,
        depth=2,
        summary=None,
        depth_limit_reached=False,
    )
    schema = ObjectSchema(
        path=["cat", "schema", "t"],
        node_type="table",
        columns=columns,
        primary_key=None,
        comment=None,
    ) if n > 0 else None
    detail = NodeDetail(node=node, schema=schema, metadata=None, children_count=n)

    output = render_describe_json(detail)
    parsed = json.loads(output)  # must not raise

    assert "columns" in parsed
    assert "metadata" in parsed
    assert isinstance(parsed["columns"], list)
    assert isinstance(parsed["metadata"], dict)
    assert len(parsed["columns"]) == n

    for col_entry in parsed["columns"]:
        assert "name" in col_entry
        assert "type" in col_entry


@given(
    qualified_names=st.lists(
        st.text(alphabet="abcdefghijklmnopqrstuvwxyz._", min_size=1, max_size=20),
        min_size=0,
        max_size=10,
    ),
    scores=st.lists(st.floats(min_value=-100.0, max_value=0.0, allow_nan=False), min_size=0, max_size=10),
)
@settings(max_examples=100)
def test_property_20_render_search_json_is_valid_json(
    qualified_names: list[str],
    scores: list[float],
) -> None:
    """Property 20 (search): render_search_json always produces valid JSON array.

    For any search results, render_search_json should produce output that parses as
    valid JSON array where each entry has qualified_name, kind, score, match_positions.

    Validates: Requirements 11.3
    """
    n = min(len(qualified_names), len(scores))
    results = [
        SearchResult(
            kind="table",
            qualified_name=qualified_names[i],
            short_name=qualified_names[i].split(".")[-1],
            parent=None,
            match_positions=[],
            score=scores[i],
            node_type="table",
        )
        for i in range(n)
    ]

    output = render_search_json(results)
    parsed = json.loads(output)  # must not raise

    assert isinstance(parsed, list)
    assert len(parsed) == n

    for entry in parsed:
        assert "qualified_name" in entry
        assert "kind" in entry
        assert "score" in entry
        assert "match_positions" in entry
        assert isinstance(entry["match_positions"], list)
