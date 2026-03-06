"""Tests for PostgresCatalogPlugin (tasks 9.1, 9.2, 9.3)."""

from __future__ import annotations

import os
import tempfile

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin, PluginRegistry
from rivet_postgres.catalog import PostgresCatalogPlugin

_VALID_OPTIONS = {"host": "localhost", "database": "mydb", "user": "alice", "password": "secret"}


def _make_temp_cert() -> str:
    """Create a temporary file to simulate a cert file on disk."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as f:
        return f.name


def test_catalog_type():
    assert PostgresCatalogPlugin().type == "postgres"


def test_is_catalog_plugin():
    assert isinstance(PostgresCatalogPlugin(), CatalogPlugin)


def test_required_options():
    plugin = PostgresCatalogPlugin()
    assert "host" in plugin.required_options
    assert "database" in plugin.required_options


def test_credential_options():
    plugin = PostgresCatalogPlugin()
    assert "user" in plugin.credential_options
    assert "password" in plugin.credential_options


def test_validate_accepts_valid_options():
    PostgresCatalogPlugin().validate(_VALID_OPTIONS)  # should not raise


def test_validate_rejects_missing_host():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "host"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "host" in exc_info.value.error.message


def test_validate_rejects_missing_database():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "database"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"


def test_validate_rejects_missing_user():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "user"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"


def test_validate_rejects_missing_password():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "password"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"


def test_validate_rejects_unknown_option():
    opts = {**_VALID_OPTIONS, "unknown_key": "value"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"


def test_instantiate_returns_catalog():
    catalog = PostgresCatalogPlugin().instantiate("pg", _VALID_OPTIONS)
    assert isinstance(catalog, Catalog)
    assert catalog.name == "pg"
    assert catalog.type == "postgres"


def test_registry_can_register_plugin():
    registry = PluginRegistry()
    plugin = PostgresCatalogPlugin()
    registry.register_catalog_plugin(plugin)
    assert registry.get_catalog_plugin("postgres") is plugin


def test_default_table_reference_uses_schema():
    plugin = PostgresCatalogPlugin()
    ref = plugin.default_table_reference("users", {**_VALID_OPTIONS, "schema": "myschema"})
    assert ref == "myschema.users"


def test_default_table_reference_defaults_to_public():
    plugin = PostgresCatalogPlugin()
    ref = plugin.default_table_reference("orders", _VALID_OPTIONS)
    assert ref == "public.orders"


# Task 9.2: Accept optional options


def test_optional_port_accepted():
    PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "port": 5433})


def test_optional_schema_accepted():
    PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "schema": "myschema"})


def test_optional_ssl_mode_accepted():
    PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "ssl_mode": "require"})


def test_optional_ssl_cert_accepted():
    cert = _make_temp_cert()
    try:
        PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "ssl_cert": cert})
    finally:
        os.unlink(cert)


def test_optional_ssl_key_accepted():
    key = _make_temp_cert()
    try:
        PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "ssl_key": key})
    finally:
        os.unlink(key)


def test_optional_ssl_root_cert_accepted():
    root = _make_temp_cert()
    try:
        PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "ssl_root_cert": root})
    finally:
        os.unlink(root)


def test_optional_read_only_accepted():
    PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "read_only": True})


def test_all_optional_options_accepted_together():
    cert = _make_temp_cert()
    key = _make_temp_cert()
    root = _make_temp_cert()
    try:
        opts = {
            **_VALID_OPTIONS,
            "port": 5433,
            "schema": "analytics",
            "ssl_mode": "verify-full",
            "ssl_cert": cert,
            "ssl_key": key,
            "ssl_root_cert": root,
            "read_only": True,
        }
        PostgresCatalogPlugin().validate(opts)  # should not raise
    finally:
        os.unlink(cert)
        os.unlink(key)
        os.unlink(root)


def test_optional_options_declared_on_plugin():
    plugin = PostgresCatalogPlugin()
    for key in ("port", "schema", "ssl_mode", "ssl_cert", "ssl_key", "ssl_root_cert", "read_only"):
        assert key in plugin.optional_options


# Task 9.3: Validate SSL cert paths exist on disk, require ssl_root_cert for verify-ca/verify-full


def test_ssl_cert_path_must_exist():
    opts = {**_VALID_OPTIONS, "ssl_cert": "/nonexistent/path/cert.pem"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "ssl_cert" in exc_info.value.error.message


def test_ssl_key_path_must_exist():
    opts = {**_VALID_OPTIONS, "ssl_key": "/nonexistent/path/key.pem"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "ssl_key" in exc_info.value.error.message


def test_ssl_root_cert_path_must_exist():
    opts = {**_VALID_OPTIONS, "ssl_root_cert": "/nonexistent/path/root.pem"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "ssl_root_cert" in exc_info.value.error.message


def test_ssl_cert_path_exists_accepted():
    cert = _make_temp_cert()
    try:
        PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "ssl_cert": cert})
    finally:
        os.unlink(cert)


def test_ssl_key_path_exists_accepted():
    key = _make_temp_cert()
    try:
        PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "ssl_key": key})
    finally:
        os.unlink(key)


def test_ssl_root_cert_path_exists_accepted():
    root = _make_temp_cert()
    try:
        PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "ssl_root_cert": root})
    finally:
        os.unlink(root)


def test_verify_ca_requires_ssl_root_cert():
    opts = {**_VALID_OPTIONS, "ssl_mode": "verify-ca"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "ssl_root_cert" in exc_info.value.error.message


def test_verify_full_requires_ssl_root_cert():
    opts = {**_VALID_OPTIONS, "ssl_mode": "verify-full"}
    with pytest.raises(PluginValidationError) as exc_info:
        PostgresCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "ssl_root_cert" in exc_info.value.error.message


def test_verify_ca_with_ssl_root_cert_accepted():
    root = _make_temp_cert()
    try:
        PostgresCatalogPlugin().validate(
            {**_VALID_OPTIONS, "ssl_mode": "verify-ca", "ssl_root_cert": root}
        )
    finally:
        os.unlink(root)


def test_verify_full_with_ssl_root_cert_accepted():
    root = _make_temp_cert()
    try:
        PostgresCatalogPlugin().validate(
            {**_VALID_OPTIONS, "ssl_mode": "verify-full", "ssl_root_cert": root}
        )
    finally:
        os.unlink(root)


def test_other_ssl_modes_do_not_require_ssl_root_cert():
    for mode in ("disable", "allow", "prefer", "require"):
        PostgresCatalogPlugin().validate({**_VALID_OPTIONS, "ssl_mode": mode})  # should not raise


# Task 9.5: Introspection via information_schema and pg_stat_user_tables


from unittest.mock import MagicMock, patch

from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema


def _make_catalog(schema: str = "public") -> Catalog:
    return Catalog(
        name="pg",
        type="postgres",
        options={**_VALID_OPTIONS, "schema": schema},
    )


def _mock_conn(fetchall_return=None, fetchone_return=None):
    """Build a mock psycopg connection with a cursor context manager."""
    cur = MagicMock()
    cur.fetchall.return_value = fetchall_return or []
    cur.fetchone.return_value = fetchone_return
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_list_tables_returns_catalog_nodes():
    conn, cur = _mock_conn(fetchall_return=[
        ("public", "users", "BASE TABLE"),
        ("public", "orders", "BASE TABLE"),
    ])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        nodes = plugin.list_tables(_make_catalog())
    assert len(nodes) == 2
    assert all(isinstance(n, CatalogNode) for n in nodes)
    assert nodes[0].name == "users"
    assert nodes[1].name == "orders"


def test_list_tables_node_type_table():
    conn, cur = _mock_conn(fetchall_return=[("public", "events", "BASE TABLE")])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        nodes = plugin.list_tables(_make_catalog())
    assert nodes[0].node_type == "table"


def test_list_tables_node_type_view():
    conn, cur = _mock_conn(fetchall_return=[("public", "v_summary", "VIEW")])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        nodes = plugin.list_tables(_make_catalog())
    assert nodes[0].node_type == "view"


def test_list_tables_path_includes_catalog_schema_table():
    conn, cur = _mock_conn(fetchall_return=[("public", "users", "BASE TABLE")])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        nodes = plugin.list_tables(_make_catalog())
    assert nodes[0].path == ["pg", "public", "users"]


def test_list_tables_queries_information_schema():
    conn, cur = _mock_conn(fetchall_return=[])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        plugin.list_tables(_make_catalog())
    sql = cur.execute.call_args[0][0]
    assert "information_schema.tables" in sql


def test_list_tables_filters_by_schema():
    conn, cur = _mock_conn(fetchall_return=[])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        plugin.list_tables(_make_catalog(schema="analytics"))
    args = cur.execute.call_args[0][1]
    assert "analytics" in args


def test_list_tables_empty_returns_empty_list():
    conn, cur = _mock_conn(fetchall_return=[])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        nodes = plugin.list_tables(_make_catalog())
    assert nodes == []


def test_get_schema_returns_object_schema():
    conn, cur = _mock_conn(fetchall_return=[
        ("id", "integer", "NO", None, True),
        ("name", "text", "YES", None, False),
    ])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        schema = plugin.get_schema(_make_catalog(), "users")
    assert isinstance(schema, ObjectSchema)
    assert len(schema.columns) == 2


def test_get_schema_column_names():
    conn, cur = _mock_conn(fetchall_return=[
        ("id", "integer", "NO", None, True),
        ("email", "text", "YES", None, False),
    ])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        schema = plugin.get_schema(_make_catalog(), "users")
    assert schema.columns[0].name == "id"
    assert schema.columns[1].name == "email"


def test_get_schema_arrow_type_mapping():
    conn, cur = _mock_conn(fetchall_return=[
        ("id", "integer", "NO", None, False),
        ("score", "double precision", "YES", None, False),
        ("label", "text", "YES", None, False),
        ("active", "boolean", "YES", None, False),
    ])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        schema = plugin.get_schema(_make_catalog(), "t")
    types = [c.type for c in schema.columns]
    assert types == ["int32", "float64", "large_utf8", "bool"]


def test_get_schema_nullable_flag():
    conn, cur = _mock_conn(fetchall_return=[
        ("id", "integer", "NO", None, False),
        ("name", "text", "YES", None, False),
    ])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        schema = plugin.get_schema(_make_catalog(), "t")
    assert schema.columns[0].nullable is False
    assert schema.columns[1].nullable is True


def test_get_schema_primary_key_detected():
    conn, cur = _mock_conn(fetchall_return=[
        ("id", "integer", "NO", None, True),
        ("name", "text", "YES", None, False),
    ])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        schema = plugin.get_schema(_make_catalog(), "users")
    assert schema.columns[0].is_primary_key is True
    assert schema.columns[1].is_primary_key is False
    assert schema.primary_key == ["id"]


def test_get_schema_no_primary_key():
    conn, cur = _mock_conn(fetchall_return=[
        ("name", "text", "YES", None, False),
    ])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        schema = plugin.get_schema(_make_catalog(), "t")
    assert schema.primary_key is None


def test_get_schema_path():
    conn, cur = _mock_conn(fetchall_return=[("id", "integer", "NO", None, False)])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        schema = plugin.get_schema(_make_catalog(), "public.users")
    assert schema.path == ["pg", "public", "users"]


def test_get_schema_queries_information_schema_columns():
    conn, cur = _mock_conn(fetchall_return=[])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        plugin.get_schema(_make_catalog(), "users")
    sql = cur.execute.call_args[0][0]
    assert "information_schema.columns" in sql


def test_get_schema_parses_schema_dot_table():
    conn, cur = _mock_conn(fetchall_return=[])
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        plugin.get_schema(_make_catalog(), "myschema.mytable")
    args = cur.execute.call_args[0][1]
    assert "myschema" in args
    assert "mytable" in args


def test_get_metadata_returns_object_metadata():
    conn, cur = _mock_conn(fetchone_return=(1000, 8192, "A test table", "alice"))
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        meta = plugin.get_metadata(_make_catalog(), "users")
    assert isinstance(meta, ObjectMetadata)


def test_get_metadata_row_count_and_size():
    conn, cur = _mock_conn(fetchone_return=(500, 4096, None, "bob"))
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        meta = plugin.get_metadata(_make_catalog(), "orders")
    assert meta.row_count == 500
    assert meta.size_bytes == 4096


def test_get_metadata_owner_and_comment():
    conn, cur = _mock_conn(fetchone_return=(0, 0, "My comment", "carol"))
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        meta = plugin.get_metadata(_make_catalog(), "t")
    assert meta.owner == "carol"
    assert meta.comment == "My comment"


def test_get_metadata_returns_none_when_not_found():
    conn, cur = _mock_conn(fetchone_return=None)
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        meta = plugin.get_metadata(_make_catalog(), "nonexistent")
    assert meta is None


def test_get_metadata_queries_pg_stat_user_tables():
    conn, cur = _mock_conn(fetchone_return=(0, 0, None, "owner"))
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        plugin.get_metadata(_make_catalog(), "t")
    sql = cur.execute.call_args[0][0]
    assert "pg_stat_user_tables" in sql


def test_get_metadata_queries_pg_total_relation_size():
    conn, cur = _mock_conn(fetchone_return=(0, 0, None, "owner"))
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        plugin.get_metadata(_make_catalog(), "t")
    sql = cur.execute.call_args[0][0]
    assert "pg_total_relation_size" in sql


def test_get_metadata_path():
    conn, cur = _mock_conn(fetchone_return=(10, 1024, None, "owner"))
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        meta = plugin.get_metadata(_make_catalog(), "public.users")
    assert meta.path == ["pg", "public", "users"]


def test_get_metadata_location_contains_host_and_db():
    conn, cur = _mock_conn(fetchone_return=(0, 0, None, "owner"))
    plugin = PostgresCatalogPlugin()
    with patch.object(plugin, "_connect", return_value=conn):
        meta = plugin.get_metadata(_make_catalog(), "t")
    assert "localhost" in meta.location
    assert "mydb" in meta.location


def test_pg_type_to_arrow_known_types():
    from rivet_postgres.catalog import _pg_type_to_arrow

    assert _pg_type_to_arrow("integer") == "int32"
    assert _pg_type_to_arrow("bigint") == "int64"
    assert _pg_type_to_arrow("text") == "large_utf8"
    assert _pg_type_to_arrow("boolean") == "bool"
    assert _pg_type_to_arrow("double precision") == "float64"
    assert _pg_type_to_arrow("timestamp without time zone") == "timestamp[us]"
    assert _pg_type_to_arrow("timestamp with time zone") == "timestamp[us, UTC]"
    assert _pg_type_to_arrow("bytea") == "large_binary"
    assert _pg_type_to_arrow("uuid") == "large_utf8"


def test_pg_type_to_arrow_unknown_falls_back_to_large_utf8():
    from rivet_postgres.catalog import _pg_type_to_arrow

    assert _pg_type_to_arrow("user_defined_type") == "large_utf8"


def test_pg_type_to_arrow_strips_precision():
    from rivet_postgres.catalog import _pg_type_to_arrow

    assert _pg_type_to_arrow("character varying(255)") == "large_utf8"
    assert _pg_type_to_arrow("numeric(10,2)") == "float64"
