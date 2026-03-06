"""Tests for GlueCatalogPlugin (task 18.1 + 18.3 + 18.4)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rivet_aws.glue_catalog import GlueCatalogPlugin, get_lf_credentials
from rivet_core.errors import PluginValidationError
from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema, PartitionInfo
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin

_VALID_OPTIONS = {"database": "my_db"}


def test_catalog_type():
    assert GlueCatalogPlugin().type == "glue"


def test_is_catalog_plugin():
    assert isinstance(GlueCatalogPlugin(), CatalogPlugin)


def test_database_is_optional():
    assert "database" not in GlueCatalogPlugin().required_options
    assert "database" in GlueCatalogPlugin().optional_options


def test_validate_accepts_valid_options():
    GlueCatalogPlugin().validate(_VALID_OPTIONS)


def test_validate_accepts_no_database():
    GlueCatalogPlugin().validate({})  # database is optional


def test_validate_rejects_unknown_option():
    with pytest.raises(PluginValidationError) as exc_info:
        GlueCatalogPlugin().validate({"database": "db", "unknown_opt": "x"})
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_opt" in exc_info.value.error.message


def test_validate_accepts_optional_region():
    GlueCatalogPlugin().validate({"database": "db", "region": "eu-west-1"})


def test_validate_accepts_optional_catalog_id():
    GlueCatalogPlugin().validate({"database": "db", "catalog_id": "123456789012"})


def test_validate_accepts_optional_lf_enabled():
    GlueCatalogPlugin().validate({"database": "db", "lf_enabled": True})


def test_validate_accepts_credential_options():
    GlueCatalogPlugin().validate({
        "database": "db",
        "access_key_id": "AKID",
        "secret_access_key": "SECRET",
    })


def test_instantiate_returns_catalog():
    plugin = GlueCatalogPlugin()
    catalog = plugin.instantiate("my_glue", _VALID_OPTIONS)
    assert isinstance(catalog, Catalog)
    assert catalog.name == "my_glue"
    assert catalog.type == "glue"


def test_default_table_reference_passthrough():
    plugin = GlueCatalogPlugin()
    assert plugin.default_table_reference("orders", _VALID_OPTIONS) == "orders"


# ── Task 18.3: Introspection tests ────────────────────────────────────────────

def _make_catalog(options: dict | None = None) -> Catalog:
    opts = {"database": "my_db", "access_key_id": "AKID", "secret_access_key": "SECRET"}
    if options:
        opts.update(options)
    return Catalog(name="glue_cat", type="glue", options=opts)


def _mock_glue_client() -> MagicMock:
    """Return a mock boto3 glue client."""
    client = MagicMock()
    # Default paginator returns empty pages
    paginator = MagicMock()
    paginator.paginate.return_value = iter([{"TableList": [], "Partitions": []}])
    client.get_paginator.return_value = paginator
    return client


def test_list_tables_returns_catalog_nodes():
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_paginator.return_value.paginate.return_value = iter([
        {
            "TableList": [
                {
                    "Name": "orders",
                    "Owner": "data-team",
                    "Description": "Order data",
                    "StorageDescriptor": {
                        "Location": "s3://bucket/orders/",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    },
                    "Parameters": {"totalSize": "1024", "numRows": "100"},
                },
                {
                    "Name": "customers",
                    "StorageDescriptor": {"Location": "s3://bucket/customers/", "InputFormat": ""},
                    "Parameters": {},
                },
            ]
        }
    ])

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        nodes = plugin.list_tables(catalog)

    assert len(nodes) == 2
    orders = next(n for n in nodes if n.name == "orders")
    assert isinstance(orders, CatalogNode)
    assert orders.node_type == "table"
    assert orders.path == ["glue_cat", "my_db", "orders"]
    assert orders.is_container is False
    assert orders.summary.size_bytes == 1024
    assert orders.summary.row_count == 100
    assert orders.summary.format == "parquet"
    assert orders.summary.owner == "data-team"
    assert orders.summary.comment == "Order data"


def test_list_tables_empty_database():
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_paginator.return_value.paginate.return_value = iter([{"TableList": []}])

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        nodes = plugin.list_tables(catalog)

    assert nodes == []


def test_list_tables_passes_catalog_id():
    catalog = _make_catalog({"catalog_id": "123456789012"})
    client = _mock_glue_client()
    paginator = MagicMock()
    paginator.paginate.return_value = iter([{"TableList": []}])
    client.get_paginator.return_value = paginator

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        plugin.list_tables(catalog)

    paginator.paginate.assert_called_once_with(
        DatabaseName="my_db", CatalogId="123456789012"
    )


def test_get_schema_returns_object_schema():
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "orders",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "bigint", "Comment": "primary key"},
                    {"Name": "amount", "Type": "decimal(10,2)"},
                    {"Name": "status", "Type": "string"},
                ],
            },
            "PartitionKeys": [
                {"Name": "dt", "Type": "date"},
            ],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        schema = plugin.get_schema(catalog, "orders")

    assert isinstance(schema, ObjectSchema)
    assert schema.path == ["glue_cat", "my_db", "orders"]
    assert schema.node_type == "table"
    # 3 regular columns + 1 partition key
    assert len(schema.columns) == 4

    id_col = schema.columns[0]
    assert id_col.name == "id"
    assert id_col.type == "int64"
    assert id_col.native_type == "bigint"
    assert id_col.comment == "primary key"
    assert id_col.is_partition_key is False
    assert id_col.is_primary_key is False

    amount_col = schema.columns[1]
    assert amount_col.type == "float64"  # decimal → float64

    dt_col = schema.columns[3]
    assert dt_col.name == "dt"
    assert dt_col.type == "date32"
    assert dt_col.is_partition_key is True


def test_get_schema_no_partition_keys():
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "simple",
            "StorageDescriptor": {
                "Columns": [{"Name": "val", "Type": "int"}],
            },
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        schema = plugin.get_schema(catalog, "simple")

    assert len(schema.columns) == 1
    assert schema.columns[0].name == "val"
    assert schema.columns[0].type == "int32"
    assert schema.columns[0].is_partition_key is False


def test_get_schema_passes_catalog_id():
    catalog = _make_catalog({"catalog_id": "999"})
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "t",
            "StorageDescriptor": {"Columns": []},
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        plugin.get_schema(catalog, "t")

    client.get_table.assert_called_once_with(
        DatabaseName="my_db", Name="t", CatalogId="999"
    )


def test_get_metadata_returns_object_metadata():
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "orders",
            "Owner": "data-team",
            "Description": "Order data",
            "StorageDescriptor": {
                "Location": "s3://bucket/orders/",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            },
            "Parameters": {
                "totalSize": "2048",
                "numRows": "200",
                "classification": "parquet",
            },
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        meta = plugin.get_metadata(catalog, "orders")

    assert isinstance(meta, ObjectMetadata)
    assert meta.path == ["glue_cat", "my_db", "orders"]
    assert meta.node_type == "table"
    assert meta.size_bytes == 2048
    assert meta.row_count == 200
    assert meta.format == "parquet"
    assert meta.location == "s3://bucket/orders/"
    assert meta.owner == "data-team"
    assert meta.comment == "Order data"
    assert meta.partitioning is None
    assert meta.properties["totalSize"] == "2048"


def test_get_metadata_with_partitions():
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "events",
            "StorageDescriptor": {"Location": "s3://bucket/events/", "InputFormat": ""},
            "Parameters": {},
            "PartitionKeys": [{"Name": "year", "Type": "string"}, {"Name": "month", "Type": "string"}],
        }
    }
    part_paginator = MagicMock()
    part_paginator.paginate.return_value = iter([
        {
            "Partitions": [
                {
                    "Values": ["2024", "01"],
                    "StorageDescriptor": {"Location": "s3://bucket/events/year=2024/month=01/"},
                    "Parameters": {"totalSize": "512", "numRows": "50"},
                },
                {
                    "Values": ["2024", "02"],
                    "StorageDescriptor": {"Location": "s3://bucket/events/year=2024/month=02/"},
                    "Parameters": {},
                },
            ]
        }
    ])
    client.get_paginator.return_value = part_paginator

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        meta = plugin.get_metadata(catalog, "events")

    assert meta.partitioning is not None
    assert isinstance(meta.partitioning, PartitionInfo)
    assert meta.partitioning.columns == ["year", "month"]
    assert len(meta.partitioning.partitions) == 2

    p1 = meta.partitioning.partitions[0]
    assert p1.values == {"year": "2024", "month": "01"}
    assert p1.size_bytes == 512
    assert p1.row_count == 50
    assert p1.location == "s3://bucket/events/year=2024/month=01/"

    p2 = meta.partitioning.partitions[1]
    assert p2.values == {"year": "2024", "month": "02"}
    assert p2.size_bytes is None
    assert p2.row_count is None


def test_get_metadata_passes_catalog_id():
    catalog = _make_catalog({"catalog_id": "111"})
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "t",
            "StorageDescriptor": {"Location": "s3://b/t/", "InputFormat": ""},
            "Parameters": {},
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        plugin.get_metadata(catalog, "t")

    client.get_table.assert_called_once_with(
        DatabaseName="my_db", Name="t", CatalogId="111"
    )


def test_get_metadata_no_size_or_rows():
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "sparse",
            "StorageDescriptor": {"Location": "s3://b/sparse/", "InputFormat": ""},
            "Parameters": {},
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        meta = plugin.get_metadata(catalog, "sparse")

    assert meta.size_bytes is None
    assert meta.row_count is None
    assert meta.format is None


# ── Task 18.4: Lake Formation credential handling ─────────────────────────────

def _make_lf_catalog(extra: dict | None = None) -> Catalog:
    opts = {
        "database": "my_db",
        "lf_enabled": True,
        "access_key_id": "AKID",
        "secret_access_key": "SECRET",
    }
    if extra:
        opts.update(extra)
    return Catalog(name="glue_cat", type="glue", options=opts)


def _mock_lf_response() -> dict:
    return {
        "AccessKeyId": "LF_AKID",
        "SecretAccessKey": "LF_SECRET",
        "SessionToken": "LF_TOKEN",
    }


def test_get_lf_credentials_calls_lakeformation():
    """get_lf_credentials calls GetTemporaryGlueTableCredentials and returns creds."""
    catalog = _make_lf_catalog()
    lf_client = MagicMock()
    lf_client.get_temporary_glue_table_credentials.return_value = _mock_lf_response()

    creds = get_lf_credentials(lf_client, catalog, "orders")

    lf_client.get_temporary_glue_table_credentials.assert_called_once_with(
        DatabaseName="my_db",
        TableName="orders",
        Permissions=["SELECT"],
    )
    assert creds["AccessKeyId"] == "LF_AKID"
    assert creds["SecretAccessKey"] == "LF_SECRET"
    assert creds["SessionToken"] == "LF_TOKEN"


def test_get_lf_credentials_passes_catalog_id():
    """get_lf_credentials includes CatalogId when set."""
    catalog = _make_lf_catalog({"catalog_id": "123456789012"})
    lf_client = MagicMock()
    lf_client.get_temporary_glue_table_credentials.return_value = _mock_lf_response()

    get_lf_credentials(lf_client, catalog, "orders")

    lf_client.get_temporary_glue_table_credentials.assert_called_once_with(
        DatabaseName="my_db",
        TableName="orders",
        Permissions=["SELECT"],
        CatalogId="123456789012",
    )


def test_make_glue_client_uses_lf_credentials_when_lf_enabled():
    """When lf_enabled=True, _make_glue_client_for_table uses LF-vended credentials."""
    from rivet_aws.glue_catalog import _make_glue_client_for_table

    catalog = _make_lf_catalog()
    base_client = MagicMock()
    lf_client = MagicMock()
    lf_client.get_temporary_glue_table_credentials.return_value = _mock_lf_response()

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=base_client), \
         patch("rivet_aws.glue_catalog._make_lf_client", return_value=lf_client), \
         patch("boto3.Session") as mock_session:
        mock_session.return_value.client.return_value = MagicMock()
        _make_glue_client_for_table(catalog, "orders")

    # LF credentials were fetched
    lf_client.get_temporary_glue_table_credentials.assert_called_once()
    # boto3.Session was called with LF credentials
    mock_session.assert_called_once_with(
        aws_access_key_id="LF_AKID",
        aws_secret_access_key="LF_SECRET",
        aws_session_token="LF_TOKEN",
        region_name="us-east-1",
    )


def test_make_glue_client_uses_base_credentials_when_lf_disabled():
    """When lf_enabled=False, _make_glue_client_for_table uses base credentials."""
    from rivet_aws.glue_catalog import _make_glue_client_for_table

    catalog = _make_catalog()  # lf_enabled not set (defaults to False)
    base_client = MagicMock()

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=base_client) as mock_make:
        result = _make_glue_client_for_table(catalog, "orders")

    mock_make.assert_called_once_with(catalog)
    assert result is base_client


def test_get_schema_uses_lf_credentials_when_lf_enabled():
    """get_schema uses LF-vended credentials when lf_enabled=True."""
    catalog = _make_lf_catalog()
    lf_glue_client = MagicMock()
    lf_glue_client.get_table.return_value = {
        "Table": {
            "Name": "orders",
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "bigint"}],
            },
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=lf_glue_client):
        schema = plugin.get_schema(catalog, "orders")

    assert len(schema.columns) == 1
    assert schema.columns[0].name == "id"


def test_get_metadata_uses_lf_credentials_when_lf_enabled():
    """get_metadata uses LF-vended credentials when lf_enabled=True."""
    catalog = _make_lf_catalog()
    lf_glue_client = MagicMock()
    lf_glue_client.get_table.return_value = {
        "Table": {
            "Name": "orders",
            "StorageDescriptor": {"Location": "s3://b/orders/", "InputFormat": ""},
            "Parameters": {"totalSize": "100"},
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=lf_glue_client):
        meta = plugin.get_metadata(catalog, "orders")

    assert meta.size_bytes == 100


# ── Task 7.2: Glue introspection — list_children + RVT-872 ───────────────────


def test_get_schema_raises_rvt_872_for_unsupported_serde():
    """get_schema raises RVT-872 for unsupported SerDe formats."""
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "avro_table",
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "int"}],
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
                },
            },
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.get_schema(catalog, "avro_table")

    assert exc_info.value.error.code == "RVT-872"
    assert "avro_table" in exc_info.value.error.message
    assert exc_info.value.error.context["table"] == "avro_table"
    assert "AvroSerDe" in exc_info.value.error.context["serde_format"]


def test_get_schema_accepts_supported_serde():
    """get_schema works for supported SerDe (parquet)."""
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "parquet_table",
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "bigint"}],
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            },
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        schema = plugin.get_schema(catalog, "parquet_table")

    assert len(schema.columns) == 1
    assert schema.columns[0].name == "id"


def test_get_schema_accepts_no_serde_info():
    """get_schema works when SerdeInfo is absent (best-effort)."""
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "no_serde",
            "StorageDescriptor": {
                "Columns": [{"Name": "val", "Type": "string"}],
            },
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        schema = plugin.get_schema(catalog, "no_serde")

    assert len(schema.columns) == 1


def test_list_children_root_returns_database():
    """list_children at root returns the database as a container."""
    catalog = _make_catalog()
    plugin = GlueCatalogPlugin()
    nodes = plugin.list_children(catalog, [])

    assert len(nodes) == 1
    assert nodes[0].name == "my_db"
    assert nodes[0].node_type == "database"
    assert nodes[0].is_container is True
    assert nodes[0].path == ["glue_cat", "my_db"]


def test_list_children_catalog_level_returns_database():
    """list_children at catalog level returns the database."""
    catalog = _make_catalog()
    plugin = GlueCatalogPlugin()
    nodes = plugin.list_children(catalog, ["glue_cat"])

    assert len(nodes) == 1
    assert nodes[0].name == "my_db"
    assert nodes[0].node_type == "database"
    assert nodes[0].is_container is True


def test_list_children_database_level_returns_tables():
    """list_children at database level returns tables."""
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_paginator.return_value.paginate.return_value = iter([
        {
            "TableList": [
                {
                    "Name": "orders",
                    "StorageDescriptor": {"Location": "s3://b/orders/", "InputFormat": ""},
                    "Parameters": {},
                },
                {
                    "Name": "users",
                    "StorageDescriptor": {"Location": "s3://b/users/", "InputFormat": ""},
                    "Parameters": {},
                },
            ]
        }
    ])

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        nodes = plugin.list_children(catalog, ["glue_cat", "my_db"])

    assert len(nodes) == 2
    assert nodes[0].name == "orders"
    assert nodes[0].node_type == "table"
    assert nodes[0].is_container is False
    assert nodes[1].name == "users"


def test_list_children_table_level_returns_columns():
    """list_children at table level returns columns."""
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "orders",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "bigint"},
                    {"Name": "amount", "Type": "double"},
                ],
            },
            "PartitionKeys": [{"Name": "dt", "Type": "date"}],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        nodes = plugin.list_children(catalog, ["glue_cat", "my_db", "orders"])

    assert len(nodes) == 3  # 2 regular + 1 partition key
    assert nodes[0].name == "id"
    assert nodes[0].node_type == "column"
    assert nodes[0].is_container is False
    assert nodes[0].path == ["glue_cat", "my_db", "orders", "id"]
    assert nodes[0].summary.format == "int64"  # Arrow type
    assert nodes[2].name == "dt"
    assert nodes[2].path == ["glue_cat", "my_db", "orders", "dt"]


def test_list_children_beyond_column_level_returns_empty():
    """list_children beyond column level returns empty list."""
    catalog = _make_catalog()
    plugin = GlueCatalogPlugin()
    nodes = plugin.list_children(catalog, ["glue_cat", "my_db", "orders", "id"])

    assert nodes == []


def test_list_children_table_level_handles_schema_error():
    """list_children at table level returns empty if get_schema fails."""
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_table.side_effect = Exception("Glue API error")

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        nodes = plugin.list_children(catalog, ["glue_cat", "my_db", "bad_table"])

    assert nodes == []


# ── Optional database (multi-database) tests ──────────────────────────────────


def _make_catalog_no_db(options: dict | None = None) -> Catalog:
    opts = {"access_key_id": "AKID", "secret_access_key": "SECRET"}
    if options:
        opts.update(options)
    return Catalog(name="glue_cat", type="glue", options=opts)


def test_instantiate_without_database():
    plugin = GlueCatalogPlugin()
    catalog = plugin.instantiate("my_glue", {})
    assert catalog.name == "my_glue"
    assert catalog.type == "glue"
    assert catalog.options.get("database") is None


def test_test_connection_no_database_calls_get_databases():
    catalog = _make_catalog_no_db()
    client = _mock_glue_client()
    client.get_databases.return_value = {"DatabaseList": []}

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        plugin.test_connection(catalog)

    client.get_databases.assert_called_once_with()


def test_test_connection_with_database_calls_get_database():
    catalog = _make_catalog()
    client = _mock_glue_client()
    client.get_database.return_value = {}

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        plugin.test_connection(catalog)

    client.get_database.assert_called_once_with(Name="my_db")


def test_list_tables_no_database_lists_all_databases():
    catalog = _make_catalog_no_db()
    client = _mock_glue_client()

    db_paginator = MagicMock()
    db_paginator.paginate.return_value = iter([
        {"DatabaseList": [{"Name": "db_a"}, {"Name": "db_b"}]}
    ])

    table_paginator = MagicMock()
    # Called twice — once per database
    table_paginator.paginate.side_effect = [
        iter([{"TableList": [
            {"Name": "t1", "StorageDescriptor": {"InputFormat": ""}, "Parameters": {}},
        ]}]),
        iter([{"TableList": [
            {"Name": "t2", "StorageDescriptor": {"InputFormat": ""}, "Parameters": {}},
        ]}]),
    ]

    def _get_paginator(name):
        if name == "get_databases":
            return db_paginator
        return table_paginator

    client.get_paginator.side_effect = _get_paginator

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        nodes = plugin.list_tables(catalog)

    assert len(nodes) == 2
    assert nodes[0].name == "t1"
    assert nodes[0].path == ["glue_cat", "db_a", "t1"]
    assert nodes[1].name == "t2"
    assert nodes[1].path == ["glue_cat", "db_b", "t2"]


def test_list_children_no_database_lists_all_databases():
    catalog = _make_catalog_no_db()
    client = _mock_glue_client()

    db_paginator = MagicMock()
    db_paginator.paginate.return_value = iter([
        {"DatabaseList": [{"Name": "analytics"}, {"Name": "raw"}]}
    ])
    client.get_paginator.return_value = db_paginator

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        nodes = plugin.list_children(catalog, [])

    assert len(nodes) == 2
    assert nodes[0].name == "analytics"
    assert nodes[0].node_type == "database"
    assert nodes[0].is_container is True
    assert nodes[0].path == ["glue_cat", "analytics"]
    assert nodes[1].name == "raw"


def test_list_children_no_database_depth2_lists_tables():
    """list_children at depth 2 uses the database from the path."""
    catalog = _make_catalog_no_db()
    client = _mock_glue_client()
    client.get_paginator.return_value.paginate.return_value = iter([
        {"TableList": [
            {"Name": "orders", "StorageDescriptor": {"InputFormat": ""}, "Parameters": {}},
        ]}
    ])

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        nodes = plugin.list_children(catalog, ["glue_cat", "analytics"])

    assert len(nodes) == 1
    assert nodes[0].name == "orders"
    assert nodes[0].path == ["glue_cat", "analytics", "orders"]


def test_get_schema_with_dot_notation():
    """get_schema resolves database from 'db.table' when no database configured."""
    catalog = _make_catalog_no_db()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "orders",
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "bigint"}],
            },
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        schema = plugin.get_schema(catalog, "analytics.orders")

    assert schema.path == ["glue_cat", "analytics", "orders"]
    client.get_table.assert_called_once_with(DatabaseName="analytics", Name="orders")


def test_get_schema_no_database_no_dot_raises():
    """get_schema raises when no database configured and table has no dot."""
    catalog = _make_catalog_no_db()

    plugin = GlueCatalogPlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.get_schema(catalog, "orders")

    assert "database" in exc_info.value.error.message.lower()


def test_get_metadata_with_dot_notation():
    """get_metadata resolves database from 'db.table' when no database configured."""
    catalog = _make_catalog_no_db()
    client = _mock_glue_client()
    client.get_table.return_value = {
        "Table": {
            "Name": "orders",
            "StorageDescriptor": {"Location": "s3://b/orders/", "InputFormat": ""},
            "Parameters": {"totalSize": "512"},
            "PartitionKeys": [],
        }
    }

    plugin = GlueCatalogPlugin()
    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        meta = plugin.get_metadata(catalog, "analytics.orders")

    assert meta.path == ["glue_cat", "analytics", "orders"]
    assert meta.size_bytes == 512
