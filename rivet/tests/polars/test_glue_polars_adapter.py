"""Tests for GluePolarsAdapter (task 30.2).

Uses mocked boto3 clients — no network calls.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, ComputeEngine, Joint, Material
from rivet_core.optimizer import AdapterPushdownResult
from rivet_polars.adapters.glue import (
    ALL_6_CAPABILITIES,
    GluePolarsAdapter,
    GluePolarsReadRef,
    _detect_format,
    _resolve_glue_table,
)

_CRED_PATCH = "rivet_polars.adapters.glue._make_resolver"


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _catalog(opts: dict[str, Any] | None = None) -> Catalog:
    base = {
        "database": "test_db",
        "region": "us-east-1",
        "access_key_id": "AKIATEST",
        "secret_access_key": "secret123",
        "_credential_resolver_factory": lambda opts, region: _mock_resolver(),
    }
    if opts:
        base.update(opts)
    return Catalog(name="glue_cat", type="glue", options=base)


def _engine() -> ComputeEngine:
    return ComputeEngine(name="polars_eng", engine_type="polars")


def _joint(table: str = "users", partition_filter: dict | None = None) -> Joint:
    j = Joint(name="j1", joint_type="source", table=table)
    if partition_filter:
        j.source_options = {"partition_filter": partition_filter}  # type: ignore[attr-defined]
    return j


def _glue_table_response(
    location: str = "s3://bucket/warehouse/test_db/users",
    input_format: str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    partition_keys: list[dict] | None = None,
) -> dict:
    return {
        "Table": {
            "Name": "users",
            "StorageDescriptor": {
                "Location": location,
                "InputFormat": input_format,
                "Columns": [
                    {"Name": "id", "Type": "bigint"},
                    {"Name": "name", "Type": "string"},
                ],
            },
            "PartitionKeys": partition_keys or [],
        }
    }


def _mock_creds() -> MagicMock:
    creds = MagicMock()
    creds.access_key_id = "AKIATEST"
    creds.secret_access_key = "secret123"
    creds.session_token = None
    return creds


def _mock_resolver(creds: MagicMock | None = None) -> MagicMock:
    resolver = MagicMock()
    resolver.resolve.return_value = creds or _mock_creds()
    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response()
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(return_value=[{"Partitions": []}])
    )
    resolver.create_client.return_value = glue_client
    return resolver


# ── Unit tests: _detect_format ────────────────────────────────────────────────


def test_detect_format_parquet() -> None:
    fmt = _detect_format("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
    assert fmt == "parquet"


def test_detect_format_csv() -> None:
    fmt = _detect_format("org.apache.hadoop.mapred.TextInputFormat")
    assert fmt == "csv"


def test_detect_format_json() -> None:
    fmt = _detect_format("org.apache.hive.hcatalog.data.JsonSerDe")
    assert fmt == "json"


def test_detect_format_orc_raises() -> None:
    with pytest.raises(ExecutionError) as exc_info:
        _detect_format("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
    err = exc_info.value.error
    assert err.code == "RVT-202"
    assert "ORC" in err.message


# ── Unit tests: _resolve_glue_table ──────────────────────────────────────────


def test_resolve_glue_table_no_partitions() -> None:
    with patch(_CRED_PATCH) as mock_make:
        mock_make.return_value = _mock_resolver()
        location, fmt, partition_keys, partition_locations = _resolve_glue_table(
            {"database": "test_db", "region": "us-east-1"},
            "users",
            None,
        )
    assert location == "s3://bucket/warehouse/test_db/users"
    assert fmt == "parquet"
    assert partition_keys == []
    assert partition_locations == []


def test_resolve_glue_table_with_partitions() -> None:
    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        location="s3://bucket/warehouse/test_db/events",
        partition_keys=[{"Name": "dt"}, {"Name": "region"}],
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(
            return_value=[
                {
                    "Partitions": [
                        {
                            "Values": ["2024-01-01", "us-east-1"],
                            "StorageDescriptor": {
                                "Location": "s3://bucket/warehouse/test_db/events/dt=2024-01-01/region=us-east-1"
                            },
                        },
                        {
                            "Values": ["2024-01-02", "eu-west-1"],
                            "StorageDescriptor": {
                                "Location": "s3://bucket/warehouse/test_db/events/dt=2024-01-02/region=eu-west-1"
                            },
                        },
                    ]
                }
            ]
        )
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        location, fmt, partition_keys, partition_locations = _resolve_glue_table(
            {"database": "test_db", "region": "us-east-1"},
            "events",
            None,
        )

    assert partition_keys == ["dt", "region"]
    assert len(partition_locations) == 2


def test_resolve_glue_table_partition_filter() -> None:
    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        location="s3://bucket/warehouse/test_db/events",
        partition_keys=[{"Name": "dt"}],
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(
            return_value=[
                {
                    "Partitions": [
                        {
                            "Values": ["2024-01-01"],
                            "StorageDescriptor": {
                                "Location": "s3://bucket/warehouse/test_db/events/dt=2024-01-01"
                            },
                        },
                        {
                            "Values": ["2024-01-02"],
                            "StorageDescriptor": {
                                "Location": "s3://bucket/warehouse/test_db/events/dt=2024-01-02"
                            },
                        },
                    ]
                }
            ]
        )
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        _, _, _, partition_locations = _resolve_glue_table(
            {"database": "test_db", "region": "us-east-1"},
            "events",
            {"dt": "2024-01-01"},
        )

    assert len(partition_locations) == 1
    assert "2024-01-01" in partition_locations[0]


def test_resolve_glue_table_not_found_raises() -> None:
    glue_client = MagicMock()
    glue_client.get_table.side_effect = Exception("Table not found")
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        with pytest.raises(ExecutionError) as exc_info:
            _resolve_glue_table(
                {"database": "test_db", "region": "us-east-1"},
                "missing_table",
                None,
            )
    assert exc_info.value.error.code == "RVT-503"


def test_resolve_glue_table_orc_raises() -> None:
    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        input_format="org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        with pytest.raises(ExecutionError) as exc_info:
            _resolve_glue_table(
                {"database": "test_db", "region": "us-east-1"},
                "orc_table",
                None,
            )
    assert exc_info.value.error.code == "RVT-202"
    assert "ORC" in exc_info.value.error.message


# ── Unit tests: GluePolarsAdapter registration ────────────────────────────────


def test_adapter_registration() -> None:
    adapter = GluePolarsAdapter()
    assert adapter.target_engine_type == "polars"
    assert adapter.catalog_type == "glue"
    assert adapter.source == "engine_plugin"
    assert adapter.source_plugin == "rivet_polars"


def test_adapter_capabilities() -> None:
    adapter = GluePolarsAdapter()
    assert set(adapter.capabilities) == set(ALL_6_CAPABILITIES)


def test_adapter_write_capabilities() -> None:
    adapter = GluePolarsAdapter()
    assert "write_append" in adapter.write_capabilities
    assert "write_replace" in adapter.write_capabilities
    assert "write_partition" in adapter.write_capabilities
    assert "write_delete_insert" in adapter.write_capabilities


# ── Unit tests: read_dispatch ─────────────────────────────────────────────────


def test_read_dispatch_returns_deferred_material() -> None:
    adapter = GluePolarsAdapter()
    catalog = _catalog()
    engine = _engine()
    joint = _joint()

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = _mock_resolver()
        result = adapter.read_dispatch(engine, catalog, joint)

    assert isinstance(result, AdapterPushdownResult)
    assert isinstance(result.material, Material)
    assert result.material.state == "deferred"
    assert isinstance(result.material.materialized_ref, GluePolarsReadRef)


def test_read_dispatch_with_partition_filter() -> None:
    adapter = GluePolarsAdapter()
    catalog = _catalog()
    engine = _engine()
    joint = _joint(partition_filter={"dt": "2024-01-01"})

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = _mock_resolver()
        result = adapter.read_dispatch(engine, catalog, joint)

    ref = result.material.materialized_ref
    assert isinstance(ref, GluePolarsReadRef)
    assert ref._partition_filter == {"dt": "2024-01-01"}


# ── Unit tests: GluePolarsReadRef.to_arrow ────────────────────────────────────


def test_read_ref_to_arrow_parquet() -> None:
    """to_arrow() reads parquet via polars.read_parquet with storage_options."""

    arrow_table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
    mock_df = MagicMock()
    mock_df.to_arrow.return_value = arrow_table

    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        location="s3://bucket/warehouse/test_db/users"
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(return_value=[{"Partitions": []}])
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client
    resolver.resolve.return_value = _mock_creds()

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        with patch("polars.read_parquet", return_value=mock_df) as mock_read:
            ref = GluePolarsReadRef(
                catalog_options={"database": "test_db", "region": "us-east-1"},
                table_name="users",
                partition_filter=None,
            )
            result = ref.to_arrow()

    mock_read.assert_called_once()
    call_kwargs = mock_read.call_args
    assert "storage_options" in call_kwargs.kwargs or (
        len(call_kwargs.args) > 1 or "storage_options" in str(call_kwargs)
    )
    assert result is arrow_table


def test_read_ref_to_arrow_csv() -> None:
    """to_arrow() reads csv via polars.read_csv with storage_options."""
    arrow_table = pa.table({"id": [1], "name": ["Alice"]})
    mock_df = MagicMock()
    mock_df.to_arrow.return_value = arrow_table

    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        input_format="org.apache.hadoop.mapred.TextInputFormat"
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(return_value=[{"Partitions": []}])
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client
    resolver.resolve.return_value = _mock_creds()

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        with patch("polars.read_csv", return_value=mock_df):
            ref = GluePolarsReadRef(
                catalog_options={"database": "test_db", "region": "us-east-1"},
                table_name="users",
                partition_filter=None,
            )
            result = ref.to_arrow()

    assert result is arrow_table


def test_read_ref_orc_raises_at_read_time() -> None:
    """ORC format raises ExecutionError with RVT-202."""
    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        input_format="org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(return_value=[{"Partitions": []}])
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client
    resolver.resolve.return_value = _mock_creds()

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        ref = GluePolarsReadRef(
            catalog_options={"database": "test_db", "region": "us-east-1"},
            table_name="orc_table",
            partition_filter=None,
        )
        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()

    assert exc_info.value.error.code == "RVT-202"
    assert "ORC" in exc_info.value.error.message


# ── Unit tests: write_dispatch ────────────────────────────────────────────────


def test_write_dispatch_replace() -> None:
    """write_dispatch replace writes parquet to S3 location via polars."""
    adapter = GluePolarsAdapter()
    catalog = _catalog()
    engine = _engine()
    joint = Joint(name="j1", joint_type="sink", table="users", write_strategy="replace")

    arrow_table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
    material = MagicMock()
    material.to_arrow.return_value = arrow_table

    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        location="s3://bucket/warehouse/test_db/users"
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(return_value=[{"Partitions": []}])
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client
    resolver.resolve.return_value = _mock_creds()


    mock_polars_df = MagicMock()

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        with patch("polars.from_arrow", return_value=mock_polars_df):
            adapter.write_dispatch(engine, catalog, joint, material)

    mock_polars_df.write_parquet.assert_called_once()
    call_args = mock_polars_df.write_parquet.call_args
    path_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("file")
    assert path_arg.startswith("s3://")


def test_write_dispatch_append() -> None:
    """write_dispatch append writes parquet to S3 location."""
    adapter = GluePolarsAdapter()
    catalog = _catalog()
    engine = _engine()
    joint = Joint(name="j1", joint_type="sink", table="users", write_strategy="append")

    arrow_table = pa.table({"id": [1], "name": ["Alice"]})
    material = MagicMock()
    material.to_arrow.return_value = arrow_table

    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        location="s3://bucket/warehouse/test_db/users"
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(return_value=[{"Partitions": []}])
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client
    resolver.resolve.return_value = _mock_creds()

    mock_polars_df = MagicMock()

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        with patch("polars.from_arrow", return_value=mock_polars_df):
            adapter.write_dispatch(engine, catalog, joint, material)

    mock_polars_df.write_parquet.assert_called_once()


def test_write_dispatch_partition() -> None:
    """write_dispatch partition writes partitioned parquet."""
    adapter = GluePolarsAdapter()
    catalog = _catalog()
    engine = _engine()
    joint = Joint(
        name="j1",
        joint_type="sink",
        table="events",
        write_strategy="partition",
    )
    joint.partition_by = ["dt"]  # type: ignore[attr-defined]

    arrow_table = pa.table({"id": [1], "dt": ["2024-01-01"]})
    material = MagicMock()
    material.to_arrow.return_value = arrow_table

    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        location="s3://bucket/warehouse/test_db/events",
        partition_keys=[{"Name": "dt"}],
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(return_value=[{"Partitions": []}])
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client
    resolver.resolve.return_value = _mock_creds()

    written_paths: list[str] = []

    import polars as pl

    # Patch write_parquet at the DataFrame class level to intercept all calls

    def mock_write_parquet(self: Any, path: Any, **kwargs: Any) -> None:
        written_paths.append(str(path))

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        with patch.object(pl.DataFrame, "write_parquet", mock_write_parquet):
            adapter.write_dispatch(engine, catalog, joint, material)

    assert len(written_paths) >= 1
    assert any("s3://" in p for p in written_paths)
    assert any("dt=2024-01-01" in p for p in written_paths)


def test_write_dispatch_orc_raises() -> None:
    """write_dispatch raises ExecutionError for ORC tables."""
    adapter = GluePolarsAdapter()
    catalog = _catalog()
    engine = _engine()
    joint = Joint(name="j1", joint_type="sink", table="orc_table", write_strategy="replace")

    material = MagicMock()
    material.to_arrow.return_value = pa.table({"id": [1]})

    glue_client = MagicMock()
    glue_client.get_table.return_value = _glue_table_response(
        input_format="org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    )
    glue_client.get_paginator.return_value = MagicMock(
        paginate=MagicMock(return_value=[{"Partitions": []}])
    )
    resolver = MagicMock()
    resolver.create_client.return_value = glue_client
    resolver.resolve.return_value = _mock_creds()

    with patch(_CRED_PATCH) as MockResolver:
        MockResolver.return_value = resolver
        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(engine, catalog, joint, material)

    assert exc_info.value.error.code == "RVT-202"
    assert "ORC" in exc_info.value.error.message
