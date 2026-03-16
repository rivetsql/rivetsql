"""Property-based tests for Hive Metastore legacy fallback.

Feature: hive-metastore-fallback
Tests verify SQL-based introspection helpers produce correct CatalogNode mappings.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.models import Catalog
from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin


def _make_legacy_catalog(
    catalog_name: str = "hive_metastore",
) -> Catalog:
    """Create a Catalog configured for legacy introspection."""
    return Catalog(
        name="test",
        type="databricks",
        options={
            "workspace_url": "https://test.databricks.com",
            "catalog": catalog_name,
            "legacy": True,
            "warehouse_id": "abc123",
            "token": "fake-token",
        },
    )


# ── Strategies ────────────────────────────────────────────────────────

# Schema names: non-empty ASCII identifiers (no dots/spaces to keep SQL valid)
_schema_name_st = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=30,
)

_schema_name_lists_st = st.lists(_schema_name_st, min_size=0, max_size=20, unique=True)


# ── Property 2: Schema listing produces correct CatalogNodes ─────────


@given(schema_names=_schema_name_lists_st)
@settings(max_examples=100)
def test_legacy_schema_listing_produces_correct_catalog_nodes(
    schema_names: list[str],
) -> None:
    """Property 2: Schema listing produces correct CatalogNodes.

    **Validates: Requirements 2.1, 2.2**

    For any set of schema names returned by SHOW SCHEMAS, the legacy
    list_children(path=[]) returns CatalogNode objects where each node's
    name matches a schema name, node_type is "schema", and is_container is True.
    """
    # Build a mock Arrow table mimicking SHOW SCHEMAS IN <catalog>
    arrow_table = pa.table({"databaseName": pa.array(schema_names, type=pa.string())})

    plugin = DatabricksCatalogPlugin()
    mock_api = MagicMock()
    mock_api.execute.return_value = arrow_table

    catalog_name = "hive_metastore"
    nodes = plugin._legacy_list_schemas(mock_api, catalog_name)

    # Verify count matches
    assert len(nodes) == len(schema_names), f"Expected {len(schema_names)} nodes, got {len(nodes)}"

    # Verify each node
    for node, expected_name in zip(nodes, schema_names):
        assert node.name == expected_name
        assert node.node_type == "schema"
        assert node.is_container is True
        assert node.path == [expected_name]

    # Verify the SQL executed
    mock_api.execute.assert_called_once_with(
        f"SHOW SCHEMAS IN {catalog_name}", catalog=catalog_name
    )


# ── Strategies for table listing ──────────────────────────────────────

_table_name_st = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=30,
)

_table_entry_st = st.tuples(_table_name_st, st.booleans())

_table_entries_st = st.lists(
    _table_entry_st,
    min_size=0,
    max_size=20,
    unique_by=lambda x: x[0],
)


# ── Property 3: Table listing produces correct CatalogNodes with node_type ──


@given(table_entries=_table_entries_st)
@settings(max_examples=100)
def test_legacy_table_listing_produces_correct_catalog_nodes_with_node_type(
    table_entries: list[tuple[str, bool]],
) -> None:
    """Property 3: Table listing produces correct CatalogNodes with node_type.

    **Validates: Requirements 3.1, 3.2**

    For any set of (tableName, isTemporary) pairs returned by SHOW TABLES,
    the legacy list_children(path=[schema]) returns CatalogNode objects where
    each node's name matches tableName, is_container is False, and node_type
    is "temporary_table" when isTemporary is true, otherwise "table".
    """
    table_names = [name for name, _ in table_entries]
    is_temp_flags = [is_temp for _, is_temp in table_entries]

    # Build a mock Arrow table mimicking SHOW TABLES IN <catalog>.<schema>
    arrow_table = pa.table(
        {
            "tableName": pa.array(table_names, type=pa.string()),
            "isTemporary": pa.array(is_temp_flags, type=pa.bool_()),
        }
    )

    plugin = DatabricksCatalogPlugin()
    mock_api = MagicMock()
    mock_api.execute.return_value = arrow_table

    catalog_name = "hive_metastore"
    schema_name = "my_schema"
    nodes = plugin._legacy_list_tables(mock_api, catalog_name, schema_name)

    # Verify count matches
    assert len(nodes) == len(table_entries), (
        f"Expected {len(table_entries)} nodes, got {len(nodes)}"
    )

    # Verify each node
    for node, (expected_name, expected_temp) in zip(nodes, table_entries):
        assert node.name == expected_name
        expected_type = "temporary_table" if expected_temp else "table"
        assert node.node_type == expected_type, (
            f"Expected node_type '{expected_type}' for isTemporary={expected_temp}, "
            f"got '{node.node_type}'"
        )
        assert node.is_container is False
        assert node.path == [schema_name, expected_name]

    # Verify the SQL executed
    mock_api.execute.assert_called_once_with(
        f"SHOW TABLES IN {catalog_name}.{schema_name}", catalog=catalog_name
    )


# ── Strategies for describe table ─────────────────────────────────────

# Column names: non-empty ASCII identifiers
_col_name_st = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=30,
)

# Data types drawn from the _UNITY_TO_ARROW mapping keys
_UNITY_TYPE_KEYS = [
    "bigint",
    "long",
    "int",
    "integer",
    "smallint",
    "short",
    "tinyint",
    "byte",
    "float",
    "double",
    "decimal",
    "boolean",
    "string",
    "varchar",
    "char",
    "binary",
    "date",
    "timestamp",
    "timestamp_ntz",
    "void",
]

_data_type_st = st.sampled_from(_UNITY_TYPE_KEYS)

# Optional comment: either None or a short string
_comment_st = st.one_of(st.none(), st.text(min_size=1, max_size=50))

# A single column entry: (col_name, data_type, comment)
_column_entry_st = st.tuples(_col_name_st, _data_type_st, _comment_st)

# List of column entries with unique names
_column_entries_st = st.lists(
    _column_entry_st,
    min_size=1,
    max_size=15,
    unique_by=lambda x: x[0],
)


# ── Property 4: Describe table maps columns with correct types ───────


@given(column_entries=_column_entries_st)
@settings(max_examples=100)
def test_legacy_describe_table_maps_columns_with_correct_types(
    column_entries: list[tuple[str, str, str | None]],
) -> None:
    """Property 4: Describe table maps columns with correct types.

    **Validates: Requirements 4.1, 4.2, 4.3**

    For any set of (col_name, data_type, comment) rows returned by
    DESCRIBE TABLE (before the partition separator), the legacy get_schema()
    returns an ObjectSchema whose columns list has matching name, native_type,
    comment, and type equal to parse_type(data_type, _UNITY_TO_ARROW).
    """
    from rivet_core.type_parser import parse_type
    from rivet_databricks.databricks_catalog import _UNITY_TO_ARROW

    col_names = [name for name, _, _ in column_entries]
    data_types = [dtype for _, dtype, _ in column_entries]
    comments = [c if c is not None else "" for _, _, c in column_entries]

    # Build a mock Arrow table mimicking DESCRIBE TABLE output (no partition section)
    arrow_table = pa.table(
        {
            "col_name": pa.array(col_names, type=pa.string()),
            "data_type": pa.array(data_types, type=pa.string()),
            "comment": pa.array(comments, type=pa.string()),
        }
    )

    plugin = DatabricksCatalogPlugin()
    mock_api = MagicMock()
    mock_api.execute.return_value = arrow_table

    catalog_name = "hive_metastore"
    schema_name = "my_schema"
    table_name = "my_table"

    schema = plugin._legacy_describe_table(
        mock_api,
        catalog_name,
        schema_name,
        table_name,
    )

    # Verify column count
    assert len(schema.columns) == len(column_entries), (
        f"Expected {len(column_entries)} columns, got {len(schema.columns)}"
    )

    # Verify each column
    for col_detail, (expected_name, expected_dtype, expected_comment) in zip(
        schema.columns, column_entries
    ):
        assert col_detail.name == expected_name
        assert col_detail.native_type == expected_dtype
        expected_type = parse_type(expected_dtype, _UNITY_TO_ARROW)
        assert col_detail.type == expected_type, (
            f"For native_type '{expected_dtype}', expected type '{expected_type}', "
            f"got '{col_detail.type}'"
        )
        # The implementation strips whitespace; whitespace-only → None
        expected_comment_val = expected_comment.strip() if expected_comment else None
        if expected_comment_val == "":
            expected_comment_val = None
        assert col_detail.comment == expected_comment_val
        assert col_detail.is_partition_key is False
        assert col_detail.is_primary_key is False

    # Verify path and node_type
    assert schema.path == [catalog_name, schema_name, table_name]
    assert schema.node_type == "table"

    # Verify the SQL executed
    fqn = f"{catalog_name}.{schema_name}.{table_name}"
    mock_api.execute.assert_called_once_with(
        f"DESCRIBE TABLE {fqn}",
        catalog=catalog_name,
    )


# ── Property 5: Partition columns detected from separator ────────────


@given(
    regular_cols=st.lists(
        _column_entry_st,
        min_size=1,
        max_size=10,
        unique_by=lambda x: x[0],
    ),
    partition_cols=st.lists(
        _column_entry_st,
        min_size=1,
        max_size=5,
        unique_by=lambda x: x[0],
    ),
)
@settings(max_examples=100)
def test_legacy_describe_table_detects_partition_columns_from_separator(
    regular_cols: list[tuple[str, str, str | None]],
    partition_cols: list[tuple[str, str, str | None]],
) -> None:
    """Property 5: Partition columns detected from separator.

    **Validates: Requirements 4.4**

    For any DESCRIBE TABLE result containing a # Partition Information
    separator row followed by partition column rows, the columns whose names
    appear after the separator have is_partition_key=True, and all other
    columns have is_partition_key=False.
    """
    from hypothesis import assume

    # Ensure no name overlap between regular and partition columns
    regular_names = {name for name, _, _ in regular_cols}
    partition_names = {name for name, _, _ in partition_cols}
    assume(regular_names.isdisjoint(partition_names))

    # Build the DESCRIBE TABLE result with partition separator
    col_names: list[str] = []
    data_types: list[str] = []
    comments: list[str] = []

    # Regular columns
    for name, dtype, comment in regular_cols:
        col_names.append(name)
        data_types.append(dtype)
        comments.append(comment if comment is not None else "")

    # Partition separator
    col_names.append("# Partition Information")
    data_types.append("")
    comments.append("")

    # Partition header row (col_name, data_type, comment)
    col_names.append("# col_name")
    data_types.append("data_type")
    comments.append("comment")

    # Partition columns
    for name, dtype, comment in partition_cols:
        col_names.append(name)
        data_types.append(dtype)
        comments.append(comment if comment is not None else "")

    arrow_table = pa.table(
        {
            "col_name": pa.array(col_names, type=pa.string()),
            "data_type": pa.array(data_types, type=pa.string()),
            "comment": pa.array(comments, type=pa.string()),
        }
    )

    plugin = DatabricksCatalogPlugin()
    mock_api = MagicMock()
    mock_api.execute.return_value = arrow_table

    schema = plugin._legacy_describe_table(
        mock_api,
        "hive_metastore",
        "my_schema",
        "my_table",
    )

    # Only regular columns should appear (partition cols are listed in the
    # partition section but the actual column rows are before the separator)
    assert len(schema.columns) == len(regular_cols), (
        f"Expected {len(regular_cols)} columns, got {len(schema.columns)}"
    )

    # All regular columns should have is_partition_key matching whether
    # their name appears in the partition section
    for col_detail in schema.columns:
        if col_detail.name in partition_names:
            assert col_detail.is_partition_key is True, (
                f"Column '{col_detail.name}' should be a partition key"
            )
        else:
            assert col_detail.is_partition_key is False, (
                f"Column '{col_detail.name}' should not be a partition key"
            )


# ── Strategies for flat table listing ─────────────────────────────────

# A schema with its tables: (schema_name, [(table_name, is_temporary), ...])
_schema_tables_st = st.dictionaries(
    keys=_schema_name_st,
    values=st.lists(
        st.tuples(_table_name_st, st.booleans()),
        min_size=0,
        max_size=10,
        unique_by=lambda x: x[0],
    ),
    min_size=0,
    max_size=8,
)


# ── Property 6: Flat table list has correct paths ────────────────────


@given(schema_tables=_schema_tables_st)
@settings(max_examples=100)
def test_legacy_flat_table_list_has_correct_paths(
    schema_tables: dict[str, list[tuple[str, bool]]],
) -> None:
    """Property 6: Flat table list has correct paths.

    **Validates: Requirements 5.1, 5.2**

    For any set of schemas each containing a set of tables, the legacy
    list_tables() returns a flat list where every CatalogNode has path
    equal to [schema_name, table_name] and the total count equals the
    sum of tables across all schemas.
    """
    schema_names = list(schema_tables.keys())

    # Build the Arrow table for SHOW SCHEMAS
    schemas_arrow = pa.table({"databaseName": pa.array(schema_names, type=pa.string())})

    # Build Arrow tables for SHOW TABLES per schema
    tables_arrows: dict[str, pa.Table] = {}
    for schema_name, entries in schema_tables.items():
        tbl_names = [name for name, _ in entries]
        is_temps = [is_temp for _, is_temp in entries]
        tables_arrows[schema_name] = pa.table(
            {
                "tableName": pa.array(tbl_names, type=pa.string()),
                "isTemporary": pa.array(is_temps, type=pa.bool_()),
            }
        )

    # Mock execute to return different results based on the SQL query
    def mock_execute(sql: str, catalog: str = "") -> pa.Table:
        if sql.startswith("SHOW SCHEMAS"):
            return schemas_arrow
        # SHOW TABLES IN hive_metastore.<schema>
        for sname in schema_names:
            if sql == f"SHOW TABLES IN hive_metastore.{sname}":
                return tables_arrows[sname]
        raise ValueError(f"Unexpected SQL: {sql}")

    mock_api = MagicMock()
    mock_api.execute.side_effect = mock_execute

    plugin = DatabricksCatalogPlugin()
    cat = _make_legacy_catalog()

    with patch.object(plugin, "_create_statement_api", return_value=mock_api):
        nodes = plugin._legacy_list_tables_all(cat)

    # Total count equals sum of tables across all schemas
    expected_total = sum(len(entries) for entries in schema_tables.values())
    assert len(nodes) == expected_total, f"Expected {expected_total} nodes, got {len(nodes)}"

    # Verify each node has correct path [schema_name, table_name]
    idx = 0
    for schema_name in schema_names:
        for table_name, is_temp in schema_tables[schema_name]:
            node = nodes[idx]
            assert node.name == table_name, f"Expected name '{table_name}', got '{node.name}'"
            assert node.path == [schema_name, table_name], (
                f"Expected path [{schema_name}, {table_name}], got {node.path}"
            )
            expected_type = "temporary_table" if is_temp else "table"
            assert node.node_type == expected_type
            assert node.is_container is False
            idx += 1


# ── Strategies for metadata extraction ────────────────────────────────

# Non-empty strings for metadata property values
_metadata_value_st = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-/:."),
    min_size=1,
    max_size=60,
)

# Provider/Type key: one of the two recognized property names
_format_key_st = st.sampled_from(["Provider", "Type"])


# ── Property 7: Metadata extraction maps extended properties ─────────


@given(
    location=_metadata_value_st,
    owner=_metadata_value_st,
    fmt_key=_format_key_st,
    fmt_value=_metadata_value_st,
    regular_cols=st.lists(
        _column_entry_st,
        min_size=0,
        max_size=5,
        unique_by=lambda x: x[0],
    ),
)
@settings(max_examples=100)
def test_legacy_metadata_extraction_maps_extended_properties(
    location: str,
    owner: str,
    fmt_key: str,
    fmt_value: str,
    regular_cols: list[tuple[str, str, str | None]],
) -> None:
    """Property 7: Metadata extraction maps extended properties.

    **Validates: Requirements 7.1, 7.2, 7.3, 7.4**

    For any DESCRIBE TABLE EXTENDED result containing Location, Owner, and
    Provider/Type property rows in the detailed info section, the legacy
    get_metadata() returns an ObjectMetadata with location, owner, and format
    fields matching those property values.
    """
    # Build the DESCRIBE TABLE EXTENDED result:
    # 1. Regular column rows
    # 2. # Detailed Table Information separator
    # 3. Property rows (Location, Owner, Provider/Type)
    col_names: list[str] = []
    data_types: list[str] = []
    comments: list[str] = []

    for name, dtype, comment in regular_cols:
        col_names.append(name)
        data_types.append(dtype)
        comments.append(comment if comment is not None else "")

    # Detailed table information separator
    col_names.append("# Detailed Table Information")
    data_types.append("")
    comments.append("")

    # Property rows
    col_names.append("Location")
    data_types.append(location)
    comments.append("")

    col_names.append("Owner")
    data_types.append(owner)
    comments.append("")

    col_names.append(fmt_key)
    data_types.append(fmt_value)
    comments.append("")

    arrow_table = pa.table(
        {
            "col_name": pa.array(col_names, type=pa.string()),
            "data_type": pa.array(data_types, type=pa.string()),
            "comment": pa.array(comments, type=pa.string()),
        }
    )

    plugin = DatabricksCatalogPlugin()
    mock_api = MagicMock()
    mock_api.execute.return_value = arrow_table

    catalog_name = "hive_metastore"
    schema_name = "my_schema"
    table_name = "my_table"

    metadata = plugin._legacy_describe_extended(
        mock_api,
        catalog_name,
        schema_name,
        table_name,
    )

    # Verify mapped fields
    assert metadata.location == location
    assert metadata.owner == owner
    assert metadata.format == fmt_value

    # Verify structural fields
    assert metadata.path == [catalog_name, schema_name, table_name]
    assert metadata.node_type == "table"
    assert metadata.row_count is None
    assert metadata.size_bytes is None
    assert metadata.column_statistics == []

    # Verify the SQL executed
    fqn = f"{catalog_name}.{schema_name}.{table_name}"
    mock_api.execute.assert_called_once_with(
        f"DESCRIBE TABLE EXTENDED {fqn}",
        catalog=catalog_name,
    )
