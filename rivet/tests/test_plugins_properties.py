"""Property-based tests for rivet plugin correctness properties.

Feature: rivet-plugins, Properties 1-35
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

# ── Strategies ────────────────────────────────────────────────────────────────

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,30}", fullmatch=True)
_bucket_name = st.from_regex(r"[a-z0-9][a-z0-9\-]{1,30}[a-z0-9]", fullmatch=True)
_s3_format = st.sampled_from(["parquet", "csv", "json", "orc", "delta"])
_prefix = st.one_of(st.just(""), _identifier)


# ── Property 2: Default table reference format ────────────────────────────────


@settings(max_examples=100)
@given(logical_name=_identifier, schema=st.one_of(st.none(), _identifier))
def test_duckdb_table_reference_format(logical_name: str, schema: str | None) -> None:
    """Property 2 (duckdb): default_table_reference returns schema.name or name."""
    from rivet_duckdb.catalog import DuckDBCatalogPlugin

    plugin = DuckDBCatalogPlugin()
    options: dict = {}
    if schema is not None:
        options["schema"] = schema

    result = plugin.default_table_reference(logical_name, options)

    if schema:
        assert result == f"{schema}.{logical_name}"
    else:
        assert result == logical_name


@settings(max_examples=100)
@given(logical_name=_identifier, schema=_identifier)
def test_postgres_table_reference_format(logical_name: str, schema: str) -> None:
    """Property 2 (postgres): default_table_reference returns schema.name."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    options = {"host": "localhost", "database": "mydb", "schema": schema}

    result = plugin.default_table_reference(logical_name, options)

    assert result == f"{schema}.{logical_name}"


@settings(max_examples=100)
@given(logical_name=_identifier)
def test_postgres_table_reference_default_schema(logical_name: str) -> None:
    """Property 2 (postgres): default_table_reference uses 'public' when schema not set."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    options = {"host": "localhost", "database": "mydb"}

    result = plugin.default_table_reference(logical_name, options)

    assert result == f"public.{logical_name}"


@settings(max_examples=100)
@given(logical_name=_identifier, bucket=_bucket_name, prefix=_prefix, fmt=_s3_format)
def test_s3_table_reference_format(
    logical_name: str, bucket: str, prefix: str, fmt: str
) -> None:
    """Property 2 (s3): default_table_reference returns s3://bucket/[prefix/]name.format."""
    from rivet_aws.s3_catalog import S3CatalogPlugin

    plugin = S3CatalogPlugin()
    options: dict = {"bucket": bucket, "format": fmt}
    if prefix:
        options["prefix"] = prefix

    result = plugin.default_table_reference(logical_name, options)

    assert result.startswith("s3://")
    assert result.startswith(f"s3://{bucket}/")
    assert result.endswith(f".{fmt}")
    if prefix:
        assert result == f"s3://{bucket}/{prefix}/{logical_name}.{fmt}"
    else:
        assert result == f"s3://{bucket}/{logical_name}.{fmt}"


@settings(max_examples=100)
@given(logical_name=_identifier, bucket=_bucket_name)
def test_s3_table_reference_default_format(logical_name: str, bucket: str) -> None:
    """Property 2 (s3): default_table_reference uses 'parquet' format by default."""
    from rivet_aws.s3_catalog import S3CatalogPlugin

    plugin = S3CatalogPlugin()
    options = {"bucket": bucket}

    result = plugin.default_table_reference(logical_name, options)

    assert result == f"s3://{bucket}/{logical_name}.parquet"


@settings(max_examples=100)
@given(logical_name=_identifier, database=_identifier)
def test_glue_table_reference_format(logical_name: str, database: str) -> None:
    """Property 2 (glue): default_table_reference returns the logical name unchanged."""
    from rivet_aws.glue_catalog import GlueCatalogPlugin

    plugin = GlueCatalogPlugin()
    options = {"database": database}

    result = plugin.default_table_reference(logical_name, options)

    assert result == logical_name


@settings(max_examples=100)
@given(logical_name=_identifier, catalog_name=_identifier, schema=_identifier)
def test_unity_table_reference_format(
    logical_name: str, catalog_name: str, schema: str
) -> None:
    """Property 2 (unity): default_table_reference returns catalog.schema.name."""
    from rivet_databricks.unity_catalog import UnityCatalogPlugin

    plugin = UnityCatalogPlugin()
    options = {
        "host": "https://unity.example.com",
        "catalog_name": catalog_name,
        "schema": schema,
    }

    result = plugin.default_table_reference(logical_name, options)

    assert result == f"{catalog_name}.{schema}.{logical_name}"
    assert result.count(".") == 2


@settings(max_examples=100)
@given(logical_name=_identifier, catalog_name=_identifier)
def test_unity_table_reference_default_schema(
    logical_name: str, catalog_name: str
) -> None:
    """Property 2 (unity): default_table_reference uses 'default' schema when not set."""
    from rivet_databricks.unity_catalog import UnityCatalogPlugin

    plugin = UnityCatalogPlugin()
    options = {"host": "https://unity.example.com", "catalog_name": catalog_name}

    result = plugin.default_table_reference(logical_name, options)

    assert result == f"{catalog_name}.default.{logical_name}"


@settings(max_examples=100)
@given(logical_name=_identifier, catalog=_identifier, schema=_identifier)
def test_databricks_table_reference_format(
    logical_name: str, catalog: str, schema: str
) -> None:
    """Property 2 (databricks): default_table_reference returns catalog.schema.name."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    options = {
        "workspace_url": "https://adb-123.azuredatabricks.net",
        "catalog": catalog,
        "schema": schema,
    }

    result = plugin.default_table_reference(logical_name, options)

    assert result == f"{catalog}.{schema}.{logical_name}"
    assert result.count(".") == 2


@settings(max_examples=100)
@given(logical_name=_identifier, catalog=_identifier)
def test_databricks_table_reference_default_schema(
    logical_name: str, catalog: str
) -> None:
    """Property 2 (databricks): default_table_reference uses 'default' schema when not set."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    options = {
        "workspace_url": "https://adb-123.azuredatabricks.net",
        "catalog": catalog,
    }

    result = plugin.default_table_reference(logical_name, options)

    assert result == f"{catalog}.default.{logical_name}"
