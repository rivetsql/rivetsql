"""Property-based tests: option validation for all 6 plugin packages (Property 1).

For any CatalogPlugin or ComputeEnginePlugin instance, and for any options dict:
  (a) missing a required option → PluginValidationError with RVT-2xx
  (b) unrecognized option key → PluginValidationError
  (c) all required + only recognized options → no raise
"""

from __future__ import annotations

import re

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from rivet_aws.glue_catalog import GlueCatalogPlugin
from rivet_aws.s3_catalog import S3CatalogPlugin
from rivet_core.errors import PluginValidationError
from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
from rivet_databricks.engine import DatabricksComputeEnginePlugin
from rivet_databricks.unity_catalog import UnityCatalogPlugin

# ── Plugin instances ──────────────────────────────────────────────────
from rivet_duckdb.catalog import DuckDBCatalogPlugin
from rivet_duckdb.engine import DuckDBComputeEnginePlugin
from rivet_polars.engine import PolarsComputeEnginePlugin
from rivet_postgres.catalog import PostgresCatalogPlugin
from rivet_postgres.engine import PostgresComputeEnginePlugin
from rivet_pyspark.engine import PySparkComputeEnginePlugin

# ── Helpers ───────────────────────────────────────────────────────────

# Each entry: (plugin_instance, required_keys, all_known_keys, valid_options_factory)
# valid_options_factory returns a minimal valid options dict.

_RVT_2XX = re.compile(r"RVT-2\d\d")


def _duckdb_catalog_valid():
    return {}


def _duckdb_engine_valid():
    return {}


def _postgres_catalog_valid():
    return {"host": "localhost", "database": "testdb", "user": "u", "password": "p"}


def _postgres_engine_valid():
    return {}


def _s3_catalog_valid():
    return {"bucket": "my-bucket"}


def _glue_catalog_valid():
    return {"database": "mydb"}


def _unity_catalog_valid():
    return {"host": "https://unity.example.com", "catalog_name": "main"}


def _databricks_catalog_valid():
    return {"workspace_url": "https://db.example.com", "catalog": "main"}


def _databricks_engine_valid():
    return {"warehouse_id": "abc123", "workspace_url": "https://test.databricks.com", "token": "tok"}


def _polars_engine_valid():
    return {}


def _pyspark_engine_valid():
    return {}


# (plugin, required_keys, all_known_keys_minus_table_map, valid_factory)
PLUGIN_SPECS = [
    (
        DuckDBCatalogPlugin(),
        [],
        {"path", "read_only", "schema"},
        _duckdb_catalog_valid,
    ),
    (
        DuckDBComputeEnginePlugin(),
        [],
        {"threads", "memory_limit", "temp_directory", "extensions"},
        _duckdb_engine_valid,
    ),
    (
        PostgresCatalogPlugin(),
        ["host", "database", "user", "password"],
        {"host", "database", "user", "password", "port", "schema", "ssl_mode",
         "ssl_cert", "ssl_key", "ssl_root_cert", "read_only"},
        _postgres_catalog_valid,
    ),
    (
        PostgresComputeEnginePlugin(),
        [],
        {"statement_timeout", "pool_min_size", "pool_max_size",
         "application_name", "connect_timeout", "fetch_batch_size"},
        _postgres_engine_valid,
    ),
    (
        S3CatalogPlugin(),
        ["bucket"],
        {"bucket", "prefix", "region", "endpoint_url", "format", "path_style_access",
         "access_key_id", "secret_access_key", "session_token", "profile",
         "role_arn", "role_session_name", "web_identity_token_file", "credential_cache"},
        _s3_catalog_valid,
    ),
    (
        GlueCatalogPlugin(),
        [],
        {"database", "region", "catalog_id", "lf_enabled",
         "access_key_id", "secret_access_key", "session_token", "profile",
         "role_arn", "role_session_name", "web_identity_token_file", "credential_cache"},
        _glue_catalog_valid,
    ),
    (
        UnityCatalogPlugin(),
        ["host", "catalog_name"],
        {"host", "catalog_name", "schema", "token", "client_id", "client_secret", "auth_type"},
        _unity_catalog_valid,
    ),
    (
        DatabricksCatalogPlugin(),
        ["workspace_url", "catalog"],
        {"workspace_url", "catalog", "schema", "http_path", "token",
         "client_id", "client_secret", "azure_tenant_id", "azure_client_id",
         "azure_client_secret"},
        _databricks_catalog_valid,
    ),
    (
        DatabricksComputeEnginePlugin(),
        ["warehouse_id", "workspace_url", "token"],
        {"warehouse_id", "workspace_url", "token", "wait_timeout", "max_rows_per_chunk", "disposition"},
        _databricks_engine_valid,
    ),
    (
        PolarsComputeEnginePlugin(),
        [],
        {"streaming", "n_threads", "check_dtypes"},
        _polars_engine_valid,
    ),
    (
        PySparkComputeEnginePlugin(),
        [],
        {"master", "app_name", "config", "spark_home", "packages", "connect_url"},
        _pyspark_engine_valid,
    ),
]

# Strategy for generating random unrecognized option keys
_unrecognized_key = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_"),
    min_size=1,
    max_size=30,
)


def _plugin_id(spec):
    return type(spec[0]).__name__


# ── Property (a): missing required option → PluginValidationError RVT-2xx ──


@pytest.mark.parametrize(
    "plugin,required_keys,known_keys,valid_factory",
    [s for s in PLUGIN_SPECS if s[1]],  # only plugins with required options
    ids=[_plugin_id(s) for s in PLUGIN_SPECS if s[1]],
)
@given(data=st.data())
@settings(max_examples=100)
def test_missing_required_option_raises(plugin, required_keys, known_keys, valid_factory, data):
    """(a) Missing a required option raises PluginValidationError with RVT-2xx."""
    valid = valid_factory()
    # Pick a required key to remove
    key_to_remove = data.draw(st.sampled_from(required_keys))
    opts = {k: v for k, v in valid.items() if k != key_to_remove}
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate(opts)
    assert _RVT_2XX.search(str(exc_info.value))


# ── Property (b): unrecognized option → PluginValidationError ──


@pytest.mark.parametrize(
    "plugin,required_keys,known_keys,valid_factory",
    PLUGIN_SPECS,
    ids=[_plugin_id(s) for s in PLUGIN_SPECS],
)
@given(data=st.data())
@settings(max_examples=100)
def test_unrecognized_option_raises(plugin, required_keys, known_keys, valid_factory, data):
    """(b) Unrecognized option key raises PluginValidationError."""
    valid = valid_factory()
    bad_key = data.draw(_unrecognized_key)
    # Ensure the key is truly unrecognized (not in known_keys or table_map)
    assume(bad_key not in known_keys and bad_key != "table_map")
    opts = {**valid, bad_key: "anything"}
    with pytest.raises(PluginValidationError):
        plugin.validate(opts)


# ── Property (c): valid options → no raise ──


@pytest.mark.parametrize(
    "plugin,required_keys,known_keys,valid_factory",
    PLUGIN_SPECS,
    ids=[_plugin_id(s) for s in PLUGIN_SPECS],
)
@given(data=st.data())
@settings(max_examples=100)
def test_valid_options_accepted(plugin, required_keys, known_keys, valid_factory, data):
    """(c) All required + only recognized optional keys → no raise."""
    valid = valid_factory()
    # Optionally add some recognized optional keys with safe values
    optional_keys = known_keys - set(required_keys)
    if optional_keys:
        subset = data.draw(st.frozensets(st.sampled_from(sorted(optional_keys)), max_size=3))
        for k in subset:
            if k not in valid:
                valid[k] = _safe_value_for(k)
    plugin.validate(valid)


def _safe_value_for(key: str):
    """Return a type-safe placeholder value for a known optional key."""
    _SAFE = {
        # DuckDB catalog
        "path": ":memory:", "read_only": False, "schema": "main",
        # DuckDB engine
        "threads": 4, "memory_limit": "4GB", "temp_directory": None, "extensions": [],
        # Postgres catalog
        "port": 5432, "ssl_mode": "prefer", "ssl_cert": None, "ssl_key": None,
        "ssl_root_cert": None,
        # Postgres engine
        "statement_timeout": None, "pool_min_size": 1, "pool_max_size": 10,
        "application_name": "rivet", "connect_timeout": 30, "fetch_batch_size": 10000,
        # S3 catalog
        "bucket": "b", "prefix": "", "region": "us-east-1", "endpoint_url": None,
        "format": "parquet", "path_style_access": False,
        "access_key_id": "AK", "secret_access_key": "SK", "session_token": None,
        "profile": None, "role_arn": None, "role_session_name": None,
        "web_identity_token_file": None, "credential_cache": True,
        # Glue catalog
        "database": "db", "catalog_id": None, "lf_enabled": False,
        # Unity catalog
        "host": "https://h.example.com", "catalog_name": "c",
        "token": "tok", "client_id": None, "client_secret": None, "auth_type": "pat",
        # Databricks catalog
        "workspace_url": "https://db.example.com", "catalog": "main",
        "http_path": None,
        "azure_tenant_id": None, "azure_client_id": None, "azure_client_secret": None,
        # Databricks engine
        "warehouse_id": "wh1", "wait_timeout": "30s",
        "max_rows_per_chunk": 100000, "disposition": "EXTERNAL_LINKS",
        # Polars engine
        "streaming": False, "n_threads": None, "check_dtypes": True,
        # PySpark engine
        "master": "local[*]", "app_name": "rivet", "config": {},
        "spark_home": None, "packages": [], "connect_url": None,
    }
    return _SAFE.get(key)
