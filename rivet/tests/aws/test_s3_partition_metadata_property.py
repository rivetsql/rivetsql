"""Property test for S3 partition metadata consistency.

Feature: cross-storage-adapters, Property 12: Partition metadata consistency
Generate mock S3 Hive-style partitions; verify `partition_columns` key format.
Validates: Requirements 8.1, 8.3, 8.5
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pyarrow.fs as pafs
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.s3_catalog import S3CatalogPlugin
from rivet_core.models import Catalog

_PLUGIN = S3CatalogPlugin()

# Strategy: valid Python identifier for partition column names
_col_name_st = st.from_regex(r"[a-zA-Z_][a-zA-Z0-9_]{0,19}", fullmatch=True)

# Strategy: non-empty partition value (no slashes)
_col_value_st = st.from_regex(r"[a-zA-Z0-9][a-zA-Z0-9_\-]{0,19}", fullmatch=True)

# Strategy: list of 1-4 unique partition column names
_partition_cols_st = st.lists(_col_name_st, min_size=1, max_size=4, unique=True)


def _make_catalog(bucket: str = "my-bucket") -> Catalog:
    return Catalog(name="test_s3", type="s3", options={"bucket": bucket})


def _make_dir_info(path: str, base_name: str | None = None) -> MagicMock:
    fi = MagicMock()
    fi.type = pafs.FileType.Directory
    fi.path = path
    fi.base_name = base_name or path.split("/")[-1]
    fi.mtime = datetime(2024, 1, 1, tzinfo=UTC)
    return fi


@settings(max_examples=100, deadline=None)
@given(
    col_names=_partition_cols_st,
    col_values=st.lists(_col_value_st, min_size=1, max_size=4),
)
def test_property_hive_partition_exposes_partition_columns(
    col_names: list[str], col_values: list[str]
) -> None:
    """Property 12 (S3): list_children on Hive-style partition dirs exposes partition_columns.

    For any set of Hive-style partition directories (col=val), list_children must:
    - Return a container node for each partition directory
    - Include 'partition_columns' key in the node's metadata
    - The value must be a list of strings (column names)
    - The first column name must match the partition directory's column name
    """
    # Use first col_name and first col_value to form a Hive partition dir
    col = col_names[0]
    val = col_values[0] if col_values else "2024"
    partition_dir_name = f"{col}={val}"

    catalog = _make_catalog()
    fi_partition = _make_dir_info(f"my-bucket/{partition_dir_name}", partition_dir_name)

    mock_fs = MagicMock()
    # First call: list_children at root returns the partition dir
    # Second call: _detect_partition_columns scans inside the partition dir (returns empty)
    mock_fs.get_file_info.side_effect = [
        [fi_partition],
        [],  # no nested partitions
    ]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = _PLUGIN.list_children(catalog, ["my-bucket"])

    assert len(nodes) == 1
    node = nodes[0]

    # Node must be a container
    assert node.is_container is True
    assert node.node_type == "container"

    # partition_columns must be present in metadata
    assert "partition_columns" in node.metadata, (
        f"Expected 'partition_columns' in metadata for Hive dir {partition_dir_name!r}, "
        f"got metadata: {node.metadata!r}"
    )

    partition_columns = node.metadata["partition_columns"]

    # Must be a list
    assert isinstance(partition_columns, list), (
        f"Expected partition_columns to be a list, got {type(partition_columns)}"
    )

    # Must be non-empty
    assert len(partition_columns) > 0, "partition_columns must not be empty"

    # All entries must be strings
    for entry in partition_columns:
        assert isinstance(entry, str), (
            f"Expected all partition_columns entries to be strings, got {type(entry)}: {entry!r}"
        )

    # First column must match the partition directory's column name
    assert partition_columns[0] == col, (
        f"Expected first partition column to be {col!r}, got {partition_columns[0]!r}"
    )


@settings(max_examples=100, deadline=None)
@given(
    col_names=_partition_cols_st,
    col_values=st.lists(_col_value_st, min_size=2, max_size=4),
)
def test_property_nested_hive_partitions_include_all_columns(
    col_names: list[str], col_values: list[str]
) -> None:
    """Property 12 (S3): nested Hive partitions include all detected column names.

    When a partition directory contains nested Hive-style subdirectories,
    all partition column names must appear in partition_columns.
    """
    if len(col_names) < 2 or len(col_values) < 2:
        return  # skip if not enough data

    outer_col, inner_col = col_names[0], col_names[1]
    outer_val, inner_val = col_values[0], col_values[1]

    outer_dir = f"{outer_col}={outer_val}"
    inner_dir = f"{inner_col}={inner_val}"

    catalog = _make_catalog()
    fi_outer = _make_dir_info(f"my-bucket/{outer_dir}", outer_dir)
    fi_inner = _make_dir_info(f"my-bucket/{outer_dir}/{inner_dir}", inner_dir)

    mock_fs = MagicMock()
    mock_fs.get_file_info.side_effect = [
        [fi_outer],   # root listing
        [fi_inner],   # _detect_partition_columns inside outer dir
    ]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = _PLUGIN.list_children(catalog, ["my-bucket"])

    assert len(nodes) == 1
    node = nodes[0]

    assert "partition_columns" in node.metadata
    partition_columns = node.metadata["partition_columns"]

    assert isinstance(partition_columns, list)
    assert outer_col in partition_columns
    assert inner_col in partition_columns


@settings(max_examples=50, deadline=None)
@given(
    dir_name=st.from_regex(r"[a-zA-Z][a-zA-Z0-9_\-]{0,19}", fullmatch=True).filter(
        lambda s: "=" not in s
    ),
)
def test_property_non_hive_dirs_have_no_partition_columns(dir_name: str) -> None:
    """Property 12 (S3): non-Hive directories must NOT have partition_columns in metadata.

    Directories without the 'col=val' pattern must not expose partition_columns.
    """
    catalog = _make_catalog()
    fi_dir = _make_dir_info(f"my-bucket/{dir_name}", dir_name)

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi_dir]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = _PLUGIN.list_children(catalog, ["my-bucket"])

    assert len(nodes) == 1
    node = nodes[0]

    assert "partition_columns" not in node.metadata, (
        f"Non-Hive dir {dir_name!r} should not have partition_columns, "
        f"got metadata: {node.metadata!r}"
    )


@settings(max_examples=50, deadline=None)
@given(
    col_names=_partition_cols_st,
    col_values=st.lists(_col_value_st, min_size=1, max_size=4),
)
def test_property_partition_columns_format_consistent(
    col_names: list[str], col_values: list[str]
) -> None:
    """Property 12 (S3): partition_columns is always a list of strings (consistent format).

    The format must be consistent: a list of column name strings under the
    'partition_columns' key, matching the spec requirement 8.5.
    """
    col = col_names[0]
    val = col_values[0] if col_values else "v"
    partition_dir_name = f"{col}={val}"

    catalog = _make_catalog()
    fi_partition = _make_dir_info(f"my-bucket/{partition_dir_name}", partition_dir_name)

    mock_fs = MagicMock()
    mock_fs.get_file_info.side_effect = [[fi_partition], []]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = _PLUGIN.list_children(catalog, ["my-bucket"])

    assert len(nodes) == 1
    partition_columns = nodes[0].metadata.get("partition_columns")

    # Must be a list of strings — consistent with Glue format (Requirement 8.5)
    assert isinstance(partition_columns, list)
    assert all(isinstance(c, str) for c in partition_columns)
