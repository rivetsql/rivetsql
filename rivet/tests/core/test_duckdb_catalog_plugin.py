"""Tests for DuckDBCatalogPlugin — task 3.1."""

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin
from rivet_duckdb.catalog import DuckDBCatalogPlugin


@pytest.fixture
def plugin() -> DuckDBCatalogPlugin:
    return DuckDBCatalogPlugin()


def test_is_catalog_plugin(plugin: DuckDBCatalogPlugin) -> None:
    assert isinstance(plugin, CatalogPlugin)


def test_catalog_type(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.type == "duckdb"


def test_optional_options_path_default(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.optional_options["path"] == ":memory:"


def test_optional_options_read_only_default(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.optional_options["read_only"] is False


def test_no_required_options(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.required_options == []


def test_validate_empty_options(plugin: DuckDBCatalogPlugin) -> None:
    plugin.validate({})  # should not raise


def test_validate_path_option(plugin: DuckDBCatalogPlugin) -> None:
    plugin.validate({"path": "/tmp/test.db"})  # should not raise


def test_validate_read_only_option(plugin: DuckDBCatalogPlugin) -> None:
    plugin.validate({"read_only": True})  # should not raise


def test_validate_both_options(plugin: DuckDBCatalogPlugin) -> None:
    plugin.validate({"path": ":memory:", "read_only": False})  # should not raise


def test_validate_unknown_option_raises(plugin: DuckDBCatalogPlugin) -> None:
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"unknown_key": "value"})
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_key" in exc_info.value.error.message


def test_instantiate_returns_catalog(plugin: DuckDBCatalogPlugin) -> None:
    catalog = plugin.instantiate("my_db", {})
    assert isinstance(catalog, Catalog)
    assert catalog.name == "my_db"
    assert catalog.type == "duckdb"


def test_instantiate_with_options(plugin: DuckDBCatalogPlugin, tmp_path) -> None:
    db_path = str(tmp_path / "prod.db")
    catalog = plugin.instantiate("prod", {"path": db_path, "read_only": True})
    assert catalog.options["path"] == db_path
    assert catalog.options["read_only"] is True


def test_instantiate_invalid_option_raises(plugin: DuckDBCatalogPlugin) -> None:
    with pytest.raises(PluginValidationError):
        plugin.instantiate("bad", {"bad_option": 1})


def test_default_table_reference_passthrough(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.default_table_reference("my_table", {}) == "my_table"


def test_default_table_reference_any_name(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.default_table_reference("orders", {"path": ":memory:"}) == "orders"


# Task 3.3: default_table_reference with optional schema prefix


def test_default_table_reference_with_schema(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.default_table_reference("users", {"schema": "main"}) == "main.users"


def test_default_table_reference_no_schema_is_passthrough(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.default_table_reference("users", {}) == "users"


def test_default_table_reference_none_schema_is_passthrough(plugin: DuckDBCatalogPlugin) -> None:
    assert plugin.default_table_reference("orders", {"schema": None}) == "orders"


def test_validate_schema_option_accepted(plugin: DuckDBCatalogPlugin) -> None:
    plugin.validate({"schema": "main"})  # should not raise


def test_schema_in_optional_options(plugin: DuckDBCatalogPlugin) -> None:
    assert "schema" in plugin.optional_options
    assert plugin.optional_options["schema"] is None


def test_can_register_in_plugin_registry(plugin: DuckDBCatalogPlugin) -> None:
    from rivet_core.plugins import PluginRegistry

    registry = PluginRegistry()
    registry.register_catalog_plugin(plugin)
    assert registry.get_catalog_plugin("duckdb") is plugin


# Task 3.2: Validate parent directory exists when path is not :memory:


def test_validate_memory_path_always_passes(plugin: DuckDBCatalogPlugin) -> None:
    plugin.validate({"path": ":memory:"})  # should not raise


def test_validate_existing_parent_dir_passes(plugin: DuckDBCatalogPlugin, tmp_path) -> None:
    db_path = tmp_path / "test.db"
    plugin.validate({"path": str(db_path)})  # tmp_path exists, should not raise


def test_validate_nonexistent_parent_dir_raises(plugin: DuckDBCatalogPlugin, tmp_path) -> None:
    db_path = tmp_path / "nonexistent_dir" / "test.db"
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"path": str(db_path)})
    assert exc_info.value.error.code == "RVT-201"
    assert "nonexistent_dir" in exc_info.value.error.message


def test_validate_nonexistent_parent_includes_path_in_context(plugin: DuckDBCatalogPlugin, tmp_path) -> None:
    db_path = tmp_path / "missing" / "test.db"
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"path": str(db_path)})
    assert exc_info.value.error.context["plugin_name"] == "rivet_duckdb"
    assert "path" in exc_info.value.error.context


def test_validate_no_path_option_passes(plugin: DuckDBCatalogPlugin) -> None:
    plugin.validate({})  # defaults to :memory:, should not raise


# Task 3.4: Introspection — list_tables, get_schema, get_metadata


import duckdb as _duckdb
import pytest


@pytest.fixture
def in_memory_catalog_with_tables(tmp_path):
    """Create a file-backed DuckDB catalog with test tables."""
    db_path = str(tmp_path / "test.db")
    conn = _duckdb.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR, active BOOLEAN)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', false)")
    conn.execute("CREATE TABLE orders (order_id BIGINT NOT NULL, amount DOUBLE, user_id INTEGER)")
    conn.close()
    return Catalog(name="mydb", type="duckdb", options={"path": db_path})


def test_list_tables_returns_catalog_nodes(plugin, in_memory_catalog_with_tables):
    from rivet_core.introspection import CatalogNode

    nodes = plugin.list_tables(in_memory_catalog_with_tables)
    assert len(nodes) >= 2
    names = {n.name for n in nodes}
    assert "users" in names
    assert "orders" in names
    for node in nodes:
        assert isinstance(node, CatalogNode)
        assert node.node_type == "table"
        assert not node.is_container


def test_list_tables_path_includes_catalog_name(plugin, in_memory_catalog_with_tables):
    nodes = plugin.list_tables(in_memory_catalog_with_tables)
    for node in nodes:
        assert node.path[0] == "mydb"


def test_list_tables_empty_db_returns_empty(plugin, tmp_path):
    db_path = str(tmp_path / "empty.db")
    conn = _duckdb.connect(db_path)
    conn.close()
    catalog = Catalog(name="empty", type="duckdb", options={"path": db_path})
    nodes = plugin.list_tables(catalog)
    assert nodes == []


def test_list_tables_in_memory(plugin):
    catalog = Catalog(name="mem", type="duckdb", options={"path": ":memory:"})
    nodes = plugin.list_tables(catalog)
    assert nodes == []


def test_get_schema_returns_object_schema(plugin, in_memory_catalog_with_tables):
    from rivet_core.introspection import ObjectSchema

    schema = plugin.get_schema(in_memory_catalog_with_tables, "users")
    assert isinstance(schema, ObjectSchema)
    assert schema.node_type == "table"
    assert schema.path[0] == "mydb"


def test_get_schema_columns(plugin, in_memory_catalog_with_tables):
    schema = plugin.get_schema(in_memory_catalog_with_tables, "users")
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "active" in col_names


def test_get_schema_column_types(plugin, in_memory_catalog_with_tables):
    schema = plugin.get_schema(in_memory_catalog_with_tables, "users")
    col_map = {c.name: c for c in schema.columns}
    assert col_map["id"].type == "int32"
    assert col_map["name"].type == "large_utf8"
    assert col_map["active"].type == "bool"


def test_get_schema_native_type_preserved(plugin, in_memory_catalog_with_tables):
    schema = plugin.get_schema(in_memory_catalog_with_tables, "orders")
    col_map = {c.name: c for c in schema.columns}
    assert col_map["order_id"].native_type is not None
    assert "BIGINT" in col_map["order_id"].native_type.upper()


def test_get_schema_not_null_column(plugin, in_memory_catalog_with_tables):
    schema = plugin.get_schema(in_memory_catalog_with_tables, "orders")
    col_map = {c.name: c for c in schema.columns}
    # order_id is NOT NULL
    assert col_map["order_id"].nullable is False


def test_get_metadata_returns_object_metadata(plugin, in_memory_catalog_with_tables):
    from rivet_core.introspection import ObjectMetadata

    meta = plugin.get_metadata(in_memory_catalog_with_tables, "users")
    assert meta is not None
    assert isinstance(meta, ObjectMetadata)
    assert meta.node_type == "table"
    assert meta.format == "duckdb"


def test_get_metadata_path_includes_catalog_name(plugin, in_memory_catalog_with_tables):
    meta = plugin.get_metadata(in_memory_catalog_with_tables, "users")
    assert meta is not None
    assert meta.path[0] == "mydb"


def test_get_metadata_location_is_db_path(plugin, in_memory_catalog_with_tables):
    meta = plugin.get_metadata(in_memory_catalog_with_tables, "users")
    assert meta is not None
    assert meta.location == in_memory_catalog_with_tables.options["path"]


def test_get_metadata_nonexistent_table_returns_none(plugin, in_memory_catalog_with_tables):
    meta = plugin.get_metadata(in_memory_catalog_with_tables, "nonexistent_table_xyz")
    assert meta is None


def test_get_metadata_schema_qualified_table(plugin, in_memory_catalog_with_tables):
    from rivet_core.introspection import ObjectMetadata

    meta = plugin.get_metadata(in_memory_catalog_with_tables, "main.users")
    assert meta is not None
    assert isinstance(meta, ObjectMetadata)
