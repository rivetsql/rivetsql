"""Property-based tests for coherence remaining fixes.

Feature: coherence-remaining-fixes
"""

from __future__ import annotations

import pyarrow
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.builtins.arrow_catalog import ArrowCatalogPlugin
from rivet_core.models import Catalog

# ── Strategies ────────────────────────────────────────────────────────

# Table names: non-empty alphanumeric identifiers (no duplicates handled via sets)
_table_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=30,
).filter(lambda s: s[0].isalpha() or s[0] == "_")


# ── Property 1 ────────────────────────────────────────────────────────
# Feature: coherence-remaining-fixes, Property 1: Arrow list_children root returns all registered tables


@given(table_names=st.lists(_table_name_strategy, min_size=0, max_size=10, unique=True))
@settings(max_examples=100)
def test_arrow_list_children_root_returns_all_registered_tables(
    table_names: list[str],
) -> None:
    """Property 1: Arrow list_children root returns all registered tables.

    For any set of Arrow tables registered under a catalog name, calling
    list_children(catalog, []) returns exactly one CatalogNode per registered
    table, with node_type="table" and names matching the registered table names.

    **Validates: Requirements 2.2**
    """
    catalog_name = "test_catalog"
    dummy_table = pyarrow.table({"x": [1]})

    plugin = ArrowCatalogPlugin()
    # Clear the shared store to isolate this test iteration
    plugin._tables.clear()

    # Register tables under our catalog name
    for name in table_names:
        plugin._tables[(catalog_name, name)] = dummy_table

    catalog = Catalog(name=catalog_name, type="arrow", options={})
    nodes = plugin.list_children(catalog, [])

    # Verify count matches
    assert len(nodes) == len(table_names), f"Expected {len(table_names)} nodes, got {len(nodes)}"

    # Verify all names match
    returned_names = {n.name for n in nodes}
    assert returned_names == set(table_names), (
        f"Expected names {set(table_names)}, got {returned_names}"
    )

    # Verify all nodes have node_type="table"
    for node in nodes:
        assert node.node_type == "table", (
            f"Expected node_type='table', got '{node.node_type}' for '{node.name}'"
        )


# ── Strategies for Property 2 ─────────────────────────────────────────

_simple_arrow_types = st.sampled_from(
    [
        pyarrow.int64(),
        pyarrow.float64(),
        pyarrow.utf8(),
        pyarrow.bool_(),
    ]
)

_column_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=30,
).filter(lambda s: s[0].isalpha() or s[0] == "_")

_arrow_schema_strategy = st.lists(
    st.tuples(_column_name_strategy, _simple_arrow_types),
    min_size=1,
    max_size=10,
    unique_by=lambda t: t[0],  # unique column names
).map(lambda fields: pyarrow.schema([pyarrow.field(name, typ) for name, typ in fields]))


# ── Property 2 ────────────────────────────────────────────────────────
# Feature: coherence-remaining-fixes, Property 2: Arrow list_children table path returns columns matching schema


@given(schema=_arrow_schema_strategy)
@settings(max_examples=100)
def test_arrow_list_children_table_path_returns_columns_matching_schema(
    schema: pyarrow.Schema,
) -> None:
    """Property 2: Arrow list_children table path returns columns matching schema.

    For any Arrow table with an arbitrary schema (random column names and types),
    calling list_children(catalog, [table_name]) returns one CatalogNode per column
    in the table's schema, with node_type="column" and names matching the schema
    field names.

    **Validates: Requirements 2.3**
    """
    catalog_name = "test_catalog"
    table_name = "test_table"

    # Build a table with the generated schema (one row of nulls is enough)
    arrays = [pyarrow.nulls(1, type=field.type) for field in schema]
    table = pyarrow.table(dict(zip([f.name for f in schema], arrays)))

    plugin = ArrowCatalogPlugin()
    plugin._tables.clear()
    plugin._tables[(catalog_name, table_name)] = table

    catalog = Catalog(name=catalog_name, type="arrow", options={})
    nodes = plugin.list_children(catalog, [table_name])

    # Verify count matches schema field count
    assert len(nodes) == len(schema), f"Expected {len(schema)} column nodes, got {len(nodes)}"

    # Verify all column names match
    returned_names = [n.name for n in nodes]
    expected_names = [field.name for field in schema]
    assert returned_names == expected_names, (
        f"Expected column names {expected_names}, got {returned_names}"
    )

    # Verify all nodes have node_type="column"
    for node in nodes:
        assert node.node_type == "column", (
            f"Expected node_type='column', got '{node.node_type}' for '{node.name}'"
        )


# ── Strategies for Property 3 ─────────────────────────────────────────

import tempfile
from pathlib import Path

from rivet_core.builtins.filesystem_catalog import FilesystemCatalogPlugin

# Recognized extensions that map to supported formats
_RECOGNIZED_EXTENSIONS = [".parquet", ".csv", ".json", ".ipc"]

# File stem strategy: valid identifiers that work as file names
_file_stem_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=20,
).filter(lambda s: s[0].isalpha() or s[0] == "_")


# ── Property 3 ────────────────────────────────────────────────────────
# Feature: coherence-remaining-fixes, Property 3: Filesystem list_children root returns entries for data files


@given(
    stems_and_exts=st.lists(
        st.tuples(
            _file_stem_strategy,
            st.sampled_from(_RECOGNIZED_EXTENSIONS),
        ),
        min_size=0,
        max_size=10,
        unique_by=lambda t: t[0] + t[1],  # unique filenames
    ),
)
@settings(max_examples=100)
def test_filesystem_list_children_root_returns_entries_for_data_files(
    stems_and_exts: list[tuple[str, str]],
) -> None:
    """Property 3: Filesystem list_children root returns entries for data files.

    For any directory containing an arbitrary mix of recognized data files
    (parquet, csv, json, ipc), calling list_children(catalog, []) returns a
    CatalogNode for each recognized file, with names derived from file stems.

    **Validates: Requirements 3.3**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create empty files with recognized extensions
        for stem, ext in stems_and_exts:
            (Path(tmp_dir) / f"{stem}{ext}").touch()

        plugin = FilesystemCatalogPlugin()
        catalog = Catalog(name="test_fs", type="filesystem", options={"path": tmp_dir})
        nodes = plugin.list_children(catalog, [])

        # Each file produces one node; the node name is the file stem.
        assert len(nodes) == len(stems_and_exts), (
            f"Expected {len(stems_and_exts)} nodes, got {len(nodes)}"
        )

        # Verify returned names match the expected file stems (as a multiset)
        returned_names = sorted(n.name for n in nodes)
        expected_names = sorted(stem for stem, _ in stems_and_exts)
        assert returned_names == expected_names, (
            f"Expected names {expected_names}, got {returned_names}"
        )

        # Verify all nodes have node_type="table"
        for node in nodes:
            assert node.node_type == "table", (
                f"Expected node_type='table', got '{node.node_type}' for '{node.name}'"
            )


# ── Property 4 ────────────────────────────────────────────────────────
# Feature: coherence-remaining-fixes, Property 4: Filesystem list_children file path returns columns matching schema

import pyarrow.parquet as pq


@given(schema=_arrow_schema_strategy)
@settings(max_examples=100)
def test_filesystem_list_children_file_path_returns_columns_matching_schema(
    schema: pyarrow.Schema,
) -> None:
    """Property 4: Filesystem list_children file path returns columns matching schema.

    For any data file with an arbitrary Arrow schema, calling
    list_children(catalog, [file_name]) returns one CatalogNode per column
    in the file's schema, with node_type="column" and names matching the
    schema field names.

    **Validates: Requirements 3.4**
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Write a parquet file with the generated schema (one row of nulls)
        arrays = [pyarrow.nulls(1, type=field.type) for field in schema]
        table = pyarrow.table(
            {field.name: arr for field, arr in zip(schema, arrays)},
            schema=schema,
        )
        file_path = Path(tmp_dir) / "test_data.parquet"
        pq.write_table(table, file_path)

        file_stem = file_path.stem  # "test_data"

        plugin = FilesystemCatalogPlugin()
        catalog = Catalog(name="test_fs", type="filesystem", options={"path": tmp_dir})
        nodes = plugin.list_children(catalog, [file_stem])

        # Verify count matches schema field count
        assert len(nodes) == len(schema), f"Expected {len(schema)} column nodes, got {len(nodes)}"

        # Verify column names match in order
        returned_names = [n.name for n in nodes]
        expected_names = [field.name for field in schema]
        assert returned_names == expected_names, (
            f"Expected column names {expected_names}, got {returned_names}"
        )

        # Verify all nodes have node_type="column"
        for node in nodes:
            assert node.node_type == "column", (
                f"Expected node_type='column', got '{node.node_type}' for '{node.name}'"
            )


# ── Strategies for Property 6 ─────────────────────────────────────────

from rivet_core.errors import PluginValidationError
from rivet_rest.auth import create_auth
from rivet_rest.pagination import create_paginator

_VALID_AUTH_TYPES = {"none", "bearer", "basic", "api_key", "oauth2"}
_VALID_PAGINATION_STRATEGIES = {"none", "offset", "cursor", "page_number", "link_header"}

_invalid_auth_type_strategy = st.text(min_size=1, max_size=50).filter(
    lambda s: s not in _VALID_AUTH_TYPES
)
_invalid_pagination_strategy = st.text(min_size=1, max_size=50).filter(
    lambda s: s not in _VALID_PAGINATION_STRATEGIES
)


# ── Property 6 ────────────────────────────────────────────────────────
# Feature: coherence-remaining-fixes, Property 6: Unrecognized REST factory inputs raise PluginValidationError


@given(auth_type=_invalid_auth_type_strategy)
@settings(max_examples=100)
def test_create_auth_unrecognized_type_raises_plugin_validation_error(
    auth_type: str,
) -> None:
    """Property 6a: Unrecognized auth types raise PluginValidationError.

    For any string not in the valid auth types set, create_auth should raise
    a PluginValidationError with plugin_name="rivet_rest" and
    plugin_type="catalog".

    **Validates: Requirements 8.1, 8.2**
    """
    try:
        create_auth(auth_type, {})
        raise AssertionError(f"Expected PluginValidationError for auth_type={auth_type!r}")
    except PluginValidationError as exc:
        assert exc.error.context["plugin_name"] == "rivet_rest", (
            f"Expected plugin_name='rivet_rest', got {exc.error.context.get('plugin_name')!r}"
        )
        assert exc.error.context["plugin_type"] == "catalog", (
            f"Expected plugin_type='catalog', got {exc.error.context.get('plugin_type')!r}"
        )


@given(strategy=_invalid_pagination_strategy)
@settings(max_examples=100)
def test_create_paginator_unrecognized_strategy_raises_plugin_validation_error(
    strategy: str,
) -> None:
    """Property 6b: Unrecognized pagination strategies raise PluginValidationError.

    For any string not in the valid pagination strategies set, create_paginator
    should raise a PluginValidationError with plugin_name="rivet_rest" and
    plugin_type="catalog".

    **Validates: Requirements 8.1, 8.2**
    """
    try:
        create_paginator({"strategy": strategy})
        raise AssertionError(f"Expected PluginValidationError for strategy={strategy!r}")
    except PluginValidationError as exc:
        assert exc.error.context["plugin_name"] == "rivet_rest", (
            f"Expected plugin_name='rivet_rest', got {exc.error.context.get('plugin_name')!r}"
        )
        assert exc.error.context["plugin_type"] == "catalog", (
            f"Expected plugin_type='catalog', got {exc.error.context.get('plugin_type')!r}"
        )


# ── Strategies for Property 5 ─────────────────────────────────────────

from rivet_core.builtins.filesystem_catalog import _read_schema_lightweight, _read_table
from rivet_core.errors import ExecutionError
from rivet_duckdb.filesystem_sink import _read_file

_SUPPORTED_FORMATS = {"parquet", "csv", "json", "ipc"}

_unsupported_format_strategy = st.text(min_size=1, max_size=50).filter(
    lambda s: s not in _SUPPORTED_FORMATS
)


# ── Property 5 ────────────────────────────────────────────────────────
# Feature: coherence-remaining-fixes, Property 5: Unsupported format raises ExecutionError in filesystem functions


@given(fmt=_unsupported_format_strategy)
@settings(max_examples=100)
def test_read_table_unsupported_format_raises_execution_error(
    fmt: str,
) -> None:
    """Property 5a: _read_table raises ExecutionError for unsupported formats.

    For any format string not in {"parquet", "csv", "json", "ipc"}, calling
    _read_table with that format raises an ExecutionError with
    plugin_name="filesystem" and plugin_type="source".

    **Validates: Requirements 9.1**
    """
    dummy_path = Path("/nonexistent/dummy_file.dat")
    try:
        _read_table(dummy_path, fmt, {})
        raise AssertionError(f"Expected ExecutionError for format={fmt!r}")
    except ExecutionError as exc:
        assert exc.error.context["plugin_name"] == "filesystem", (
            f"Expected plugin_name='filesystem', got {exc.error.context.get('plugin_name')!r}"
        )
        assert exc.error.context["plugin_type"] == "source", (
            f"Expected plugin_type='source', got {exc.error.context.get('plugin_type')!r}"
        )


@given(fmt=_unsupported_format_strategy)
@settings(max_examples=100)
def test_read_schema_lightweight_unsupported_format_raises_execution_error(
    fmt: str,
) -> None:
    """Property 5b: _read_schema_lightweight raises ExecutionError for unsupported formats.

    For any format string not in {"parquet", "csv", "json", "ipc"}, calling
    _read_schema_lightweight with that format raises an ExecutionError with
    plugin_name="filesystem" and plugin_type="catalog".

    **Validates: Requirements 9.2**
    """
    dummy_path = Path("/nonexistent/dummy_file.dat")
    try:
        _read_schema_lightweight(dummy_path, fmt, {})
        raise AssertionError(f"Expected ExecutionError for format={fmt!r}")
    except ExecutionError as exc:
        assert exc.error.context["plugin_name"] == "filesystem", (
            f"Expected plugin_name='filesystem', got {exc.error.context.get('plugin_name')!r}"
        )
        assert exc.error.context["plugin_type"] == "catalog", (
            f"Expected plugin_type='catalog', got {exc.error.context.get('plugin_type')!r}"
        )


@given(fmt=_unsupported_format_strategy)
@settings(max_examples=100)
def test_read_file_unsupported_format_raises_execution_error(
    fmt: str,
) -> None:
    """Property 5c: _read_file raises ExecutionError for unsupported formats.

    For any format string not in {"parquet", "csv", "json", "ipc"}, calling
    _read_file with that format raises an ExecutionError with
    plugin_name="rivet_duckdb" and plugin_type="sink".

    **Validates: Requirements 4.1**
    """
    dummy_path = Path("/nonexistent/dummy_file.dat")
    try:
        _read_file(dummy_path, fmt)
        raise AssertionError(f"Expected ExecutionError for format={fmt!r}")
    except ExecutionError as exc:
        assert exc.error.context["plugin_name"] == "rivet_duckdb", (
            f"Expected plugin_name='rivet_duckdb', got {exc.error.context.get('plugin_name')!r}"
        )
        assert exc.error.context["plugin_type"] == "sink", (
            f"Expected plugin_type='sink', got {exc.error.context.get('plugin_type')!r}"
        )
