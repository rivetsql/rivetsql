"""Property test for Glue partition metadata consistency.

Feature: cross-storage-adapters, Property 12: Partition metadata consistency
Generate mock Glue PartitionKeys; verify partition_columns key format consistent with S3.
Validates: Requirements 8.2, 8.4, 8.5
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.glue_catalog import GlueCatalogPlugin
from rivet_core.models import Catalog

# Strategy: a valid Glue column name (identifier)
_col_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)

# Strategy: a Glue type string
_glue_type_st = st.sampled_from(["string", "bigint", "int", "double", "date", "boolean"])

# Strategy: a list of unique partition key names (0–4 keys)
_partition_keys_st = st.lists(
    st.fixed_dictionaries({"Name": _col_name_st, "Type": _glue_type_st}),
    min_size=0,
    max_size=4,
).filter(lambda keys: len({k["Name"] for k in keys}) == len(keys))  # unique names

_plugin = GlueCatalogPlugin()


def _make_catalog() -> Catalog:
    return Catalog(
        name="glue_cat",
        type="glue",
        options={"database": "my_db", "access_key_id": "AKID", "secret_access_key": "SECRET"},
    )


def _mock_glue_client_for_table(partition_keys: list[dict]) -> MagicMock:
    client = MagicMock()
    client.get_table.return_value = {
        "Table": {
            "Name": "test_table",
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "bigint"}],
            },
            "PartitionKeys": partition_keys,
        }
    }
    return client


@settings(max_examples=100, deadline=None)
@given(partition_keys=_partition_keys_st)
def test_property_glue_partition_columns_format(partition_keys: list[dict]) -> None:
    """Property 12 (Glue side): partition column names from get_schema() are a list of strings
    matching the PartitionKeys names, consistent with the partition_columns format used by S3.
    """
    catalog = _make_catalog()
    client = _mock_glue_client_for_table(partition_keys)

    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        schema = _plugin.get_schema(catalog, "test_table")

    # Extract partition column names from schema (columns with is_partition_key=True)
    partition_col_names = [col.name for col in schema.columns if col.is_partition_key]

    # Must be a list of strings
    assert isinstance(partition_col_names, list)
    assert all(isinstance(name, str) for name in partition_col_names)

    # Must match the PartitionKeys names exactly
    expected_names = [pk["Name"] for pk in partition_keys]
    assert partition_col_names == expected_names, (
        f"Expected partition columns {expected_names!r}, got {partition_col_names!r}"
    )


@settings(max_examples=100, deadline=None)
@given(partition_keys=_partition_keys_st)
def test_property_glue_partition_columns_consistent_with_s3_format(
    partition_keys: list[dict],
) -> None:
    """Property 12: partition_columns extracted from Glue schema is a list[str] of column names,
    the same format as S3 partition_columns metadata (list of column name strings).
    """
    catalog = _make_catalog()
    client = _mock_glue_client_for_table(partition_keys)

    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        schema = _plugin.get_schema(catalog, "test_table")

    # The partition_columns format (as used by S3) is: list[str] of column names
    partition_columns = [col.name for col in schema.columns if col.is_partition_key]

    # Verify format: list of non-empty strings
    assert isinstance(partition_columns, list)
    for name in partition_columns:
        assert isinstance(name, str) and len(name) > 0

    # Verify count matches PartitionKeys
    assert len(partition_columns) == len(partition_keys)
