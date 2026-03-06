"""Property test for S3 list_children immediate children and node types.

Feature: cross-storage-adapters, Property 7: S3 list_children returns correctly typed immediate children
Generate mock S3 listings with mixed file types and prefixes; verify immediate-children-only,
correct node types, unrecognized extensions hidden.
Validates: Requirements 4.5, 4.6, 4.7, 10.3
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pyarrow.fs as pafs
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.s3_catalog import _RECOGNIZED_EXTENSIONS, S3CatalogPlugin
from rivet_core.models import Catalog

_PLUGIN = S3CatalogPlugin()

_RECOGNIZED_EXTS = sorted(_RECOGNIZED_EXTENSIONS)  # e.g. [".csv", ".ipc", ".json", ".orc", ".parquet"]

# Strategies for generating file/dir names
_identifier_st = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
_recognized_ext_st = st.sampled_from(_RECOGNIZED_EXTS)
_unrecognized_ext_st = st.from_regex(r"\.[a-z]{2,6}", fullmatch=True).filter(
    lambda e: e not in _RECOGNIZED_EXTENSIONS
)


def _make_mock_file(name: str, size: int = 100) -> MagicMock:
    fi = MagicMock()
    fi.type = pafs.FileType.File
    fi.base_name = name
    fi.path = f"my-bucket/{name}"
    fi.size = size
    fi.mtime = datetime(2024, 1, 1, tzinfo=UTC)
    return fi


def _make_mock_dir(name: str) -> MagicMock:
    fi = MagicMock()
    fi.type = pafs.FileType.Directory
    fi.base_name = name
    fi.path = f"my-bucket/{name}"
    fi.size = None
    fi.mtime = datetime(2024, 1, 1, tzinfo=UTC)
    return fi


def _make_catalog(bucket: str = "my-bucket") -> Catalog:
    return Catalog(name="test_s3", type="s3", options={"bucket": bucket})


# ── Property 7: list_children returns correctly typed immediate children ──────

@settings(max_examples=100, deadline=None)
@given(
    recognized_names=st.lists(
        st.tuples(_identifier_st, _recognized_ext_st),
        min_size=0,
        max_size=5,
    ),
    unrecognized_names=st.lists(
        st.tuples(_identifier_st, _unrecognized_ext_st),
        min_size=0,
        max_size=5,
    ),
    dir_names=st.lists(_identifier_st, min_size=0, max_size=5),
)
def test_property_list_children_node_types(
    recognized_names: list[tuple[str, str]],
    unrecognized_names: list[tuple[str, str]],
    dir_names: list[str],
) -> None:
    """Property 7: list_children returns correctly typed immediate children.

    - Files with recognized extensions → table nodes (is_container=False)
    - Directories → container nodes (is_container=True)
    - Files with unrecognized extensions → hidden (not returned)
    - Only immediate children (recursive=False)
    """
    catalog = _make_catalog()

    # Build mock file infos
    mock_items = []
    expected_recognized = set()
    expected_dirs = set()

    for stem, ext in recognized_names:
        name = f"{stem}{ext}"
        mock_items.append(_make_mock_file(name))
        expected_recognized.add(name)

    for stem, ext in unrecognized_names:
        name = f"{stem}{ext}"
        mock_items.append(_make_mock_file(name))
        # These should NOT appear in results

    for dname in dir_names:
        mock_items.append(_make_mock_dir(dname))
        expected_dirs.add(dname)

    mock_fs = MagicMock()
    # list_children calls get_file_info once for the listing,
    # then _detect_partition_columns may call it again for hive dirs.
    # Non-hive dirs won't trigger extra calls.
    mock_fs.get_file_info.return_value = mock_items

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = _PLUGIN.list_children(catalog, ["my-bucket"])

    returned_names = {n.name for n in nodes}
    returned_tables = {n.name for n in nodes if not n.is_container}
    returned_containers = {n.name for n in nodes if n.is_container}

    # All recognized-extension files must appear as table nodes
    for stem, ext in recognized_names:
        name = f"{stem}{ext}"
        assert name in returned_names, (
            f"Expected recognized file {name!r} in results, got: {returned_names}"
        )
        assert name in returned_tables, (
            f"Expected {name!r} to be a table node (is_container=False)"
        )

    # All unrecognized-extension files must NOT appear
    for stem, ext in unrecognized_names:
        name = f"{stem}{ext}"
        assert name not in returned_names, (
            f"Unrecognized file {name!r} should be hidden, but appeared in: {returned_names}"
        )

    # All directories must appear as container nodes
    for dname in dir_names:
        assert dname in returned_names, (
            f"Expected directory {dname!r} in results, got: {returned_names}"
        )
        assert dname in returned_containers, (
            f"Expected {dname!r} to be a container node (is_container=True)"
        )

    # node_type consistency
    for node in nodes:
        if node.is_container:
            assert node.node_type == "container", (
                f"Container node {node.name!r} has unexpected node_type: {node.node_type!r}"
            )
        else:
            assert node.node_type == "table", (
                f"Table node {node.name!r} has unexpected node_type: {node.node_type!r}"
            )

    # FileSelector must have been called with recursive=False
    call_args = mock_fs.get_file_info.call_args_list[0]
    selector = call_args[0][0]
    assert selector.recursive is False, "list_children must use recursive=False"


@settings(max_examples=100, deadline=None)
@given(
    path_depth=st.integers(min_value=1, max_value=4),
    file_name=st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True),
)
def test_property_list_children_path_is_immediate_only(
    path_depth: int, file_name: str
) -> None:
    """Property 7: returned nodes have path = parent_path + [child_name] (immediate children only)."""
    catalog = _make_catalog()
    parent_path = ["my-bucket"] + [f"seg{i}" for i in range(path_depth - 1)]
    child_name = f"{file_name}.parquet"

    fi = _make_mock_file(child_name)
    fi.path = "/".join(parent_path) + f"/{child_name}"

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = _PLUGIN.list_children(catalog, parent_path)

    assert len(nodes) == 1
    expected_path = parent_path + [child_name]
    assert nodes[0].path == expected_path, (
        f"Expected path {expected_path!r}, got {nodes[0].path!r}"
    )


@settings(max_examples=50, deadline=None)
@given(
    recognized_count=st.integers(min_value=0, max_value=8),
    unrecognized_count=st.integers(min_value=0, max_value=8),
)
def test_property_list_children_count_matches_recognized_plus_dirs(
    recognized_count: int,
    unrecognized_count: int,
) -> None:
    """Property 7: total returned count = recognized files + dirs (unrecognized hidden)."""
    catalog = _make_catalog()

    mock_items = []
    for i in range(recognized_count):
        mock_items.append(_make_mock_file(f"file{i}.parquet"))
    for i in range(unrecognized_count):
        mock_items.append(_make_mock_file(f"hidden{i}.xyz"))

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = mock_items

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = _PLUGIN.list_children(catalog, ["my-bucket"])

    assert len(nodes) == recognized_count, (
        f"Expected {recognized_count} nodes (unrecognized hidden), got {len(nodes)}"
    )
