"""Preservation property tests for PostgreSQL asyncio event loop fix.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**

Property 2: Preservation - Non-PostgreSQL Plugin Behavior Unchanged

These tests verify that non-buggy code paths remain unchanged:
- Non-PostgreSQL data sources (DuckDB, CSV, REST API) continue working
- PostgreSQL operations in sync context (no event loop) work correctly
- PostgreSQL catalog operations (list_tables, get_schema) work correctly
- PostgreSQL connection configuration is respected

CRITICAL: These tests are EXPECTED TO PASS on unfixed code.
- Passing confirms baseline behavior to preserve
- After the fix, these tests must still pass (no regressions)

The tests use property-based testing to generate many test cases for stronger
guarantees that behavior is unchanged across the input domain.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.models import Catalog, Joint, Material
from rivet_core.strategies import MaterializedRef

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def non_postgres_catalog_strategy(draw: Any) -> Catalog:
    """Generate random non-PostgreSQL catalog configurations."""
    catalog_type = draw(st.sampled_from(["filesystem", "rest_api", "duckdb"]))

    if catalog_type == "filesystem":
        return Catalog(
            name="test_fs",
            type="filesystem",
            options={"path": "/tmp/test", "format": "csv"},
        )
    elif catalog_type == "rest_api":
        return Catalog(
            name="test_api",
            type="rest_api",
            options={
                "base_url": "https://api.example.com",
                "auth": "none",
                "endpoints": {
                    "test_table": {
                        "path": "/test",
                        "method": "GET",
                    }
                },
                "max_flatten_depth": 3,
                "response_format": "json",
            },
        )
    else:  # duckdb
        return Catalog(
            name="test_duckdb",
            type="duckdb",
            options={"path": ":memory:"},
        )


@st.composite
def postgres_connection_config_strategy(draw: Any) -> dict[str, Any]:
    """Generate random PostgreSQL connection configurations."""
    host = draw(st.sampled_from(["localhost", "127.0.0.1", "db.example.com"]))
    port = draw(st.integers(min_value=5432, max_value=5435))
    database = draw(
        st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L",)))
    )
    user = draw(
        st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L",)))
    )
    password = draw(
        st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N")))
    )

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }


# ---------------------------------------------------------------------------
# Property 2.1: Non-PostgreSQL Sources Continue Working
# ---------------------------------------------------------------------------


@pytest.mark.integration
@settings(max_examples=50)
@given(st.integers(min_value=1, max_value=100))
def test_property_rest_api_source_works(num_rows: int) -> None:
    """Property 2.1: Non-PostgreSQL data sources continue working.

    Validates: Requirement 3.1

    Test that REST API sources work correctly in sync context (no event loop).
    This behavior must be preserved after the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_rest.source import RestApiSource

    catalog = Catalog(
        name="test_api",
        type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "auth": "none",
            "endpoints": {
                "test_table": {
                    "path": "/test",
                    "method": "GET",
                }
            },
            "max_flatten_depth": 3,
            "response_format": "json",
        },
    )

    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="test_table",
    )

    source = RestApiSource()

    # Read should return deferred material without errors
    material = source.read(catalog, joint, pushdown=None)

    # Verify deferred state
    assert material.state == "deferred"
    assert material.materialized_ref is not None
    assert material.name == "test_joint"

    # Mock HTTP response and verify to_arrow() works
    mock_data = [{"id": i, "value": f"row_{i}"} for i in range(num_rows)]

    with patch("requests.Session.request") as mock_request:
        mock_resp = mock_request.return_value
        mock_resp.ok = True
        mock_resp.json.return_value = mock_data

        # Call to_arrow() in sync context - should work
        table = material.to_arrow()

        assert isinstance(table, pa.Table)
        assert table.num_rows == num_rows


@pytest.mark.integration
def test_property_duckdb_source_works(tmp_path: Path) -> None:
    """Property 2.1: Non-PostgreSQL data sources continue working.

    Validates: Requirement 3.1

    Test that DuckDB sources work correctly in sync context.
    This behavior must be preserved after the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_core.plugins import PluginRegistry
    from rivet_duckdb import DuckDBPlugin

    num_rows = 10  # Fixed value for non-property test

    # Create test CSV file
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_content = "id,value\n" + "\n".join([f"{i},row_{i}" for i in range(num_rows)])
    (data_dir / "test.csv").write_text(csv_content)

    # Setup registry and catalog
    registry = PluginRegistry()
    registry.register_builtins()
    DuckDBPlugin(registry)

    catalog = Catalog(
        name="local",
        type="filesystem",
        options={"path": str(data_dir), "format": "csv"},
    )

    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="local",
        table="test",
    )

    # Get filesystem source plugin
    source_plugin = registry._sources.get("filesystem")
    assert source_plugin is not None, "Filesystem source plugin not registered"

    # Read should work without errors
    material = source_plugin.read(catalog, joint, pushdown=None)

    # Verify material is created correctly
    assert material.state == "materialized"  # Filesystem source returns materialized
    assert material.materialized_ref is not None

    # Get Arrow table - should work
    table = material.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == num_rows
    assert "id" in table.column_names
    assert "value" in table.column_names


# ---------------------------------------------------------------------------
# Property 2.2: PostgreSQL Sync Context Operations Work
# ---------------------------------------------------------------------------


@pytest.mark.integration
@settings(max_examples=30)
@given(st.integers(min_value=1, max_value=50))
def test_property_postgres_source_works_in_sync_context(num_rows: int) -> None:
    """Property 2.2: PostgreSQL operations in sync context work correctly.

    Validates: Requirements 3.2, 3.3

    Test that PostgreSQL source.to_arrow() works when called from sync context
    (no existing event loop). This is the normal usage pattern and must continue
    working after the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_postgres.source import PostgresDeferredMaterializedRef

    # Create a deferred ref with mock connection info
    ref = PostgresDeferredMaterializedRef(
        conninfo="host=localhost port=5432 dbname=testdb user=testuser password=testpass",
        sql="SELECT * FROM test_table",
    )

    # Mock psycopg to avoid needing a real database
    mock_rows = [(i, f"row_{i}") for i in range(num_rows)]

    # Create proper mock column descriptions with type_code
    mock_col_id = MagicMock()
    mock_col_id.name = "id"
    mock_col_id.type_code = 23  # INTEGER type code in PostgreSQL

    mock_col_value = MagicMock()
    mock_col_value.name = "value"
    mock_col_value.type_code = 25  # TEXT type code in PostgreSQL

    mock_cursor = AsyncMock()
    mock_cursor.description = [mock_col_id, mock_col_value]
    mock_cursor.fetchall = AsyncMock(return_value=mock_rows)

    mock_conn = AsyncMock()
    mock_conn.cursor = MagicMock(return_value=mock_cursor)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)

    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=None)

    # Call to_arrow() from sync context (no event loop) - should work
    with patch("psycopg.AsyncConnection.connect", return_value=mock_conn):
        result = ref.to_arrow()

    # Verify the result is a valid Arrow table
    assert isinstance(result, pa.Table)
    assert result.num_rows == num_rows
    assert "id" in result.column_names
    assert "value" in result.column_names


@pytest.mark.integration
@settings(max_examples=30)
@given(st.integers(min_value=1, max_value=50))
def test_property_postgres_engine_works_in_sync_context(num_rows: int) -> None:
    """Property 2.2: PostgreSQL engine operations in sync context work correctly.

    Validates: Requirements 3.2, 3.3

    Test that PostgreSQL engine.execute_sql() works when called from sync context.
    This is the normal usage pattern and must continue working after the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_postgres.engine import PostgresComputeEnginePlugin

    plugin = PostgresComputeEnginePlugin()
    engine = plugin.create_engine(
        "test_engine",
        {
            "conninfo": "host=localhost port=5432 dbname=testdb user=testuser password=testpass",
            "pool_min_size": 1,
            "pool_max_size": 10,
        },
    )

    # Mock the pool and connection
    mock_rows = [(i, f"row_{i}") for i in range(num_rows)]
    mock_batch = pa.record_batch(
        [[row[0] for row in mock_rows], [row[1] for row in mock_rows]], names=["id", "value"]
    )

    async def mock_stream_arrow(sql: str):
        yield mock_batch

    # Call execute_sql() from sync context (no event loop) - should work
    with patch.object(engine, "stream_arrow", side_effect=mock_stream_arrow):
        result = plugin.execute_sql(
            engine,
            "SELECT * FROM test_table",
            input_tables={},
        )

    # Verify the result is a valid Arrow table
    assert isinstance(result, pa.Table)
    assert result.num_rows == num_rows
    assert "id" in result.column_names
    assert "value" in result.column_names


@pytest.mark.integration
@settings(max_examples=30)
@given(st.integers(min_value=1, max_value=50))
def test_property_postgres_sink_works_in_sync_context(num_rows: int) -> None:
    """Property 2.2: PostgreSQL sink operations in sync context work correctly.

    Validates: Requirements 3.2, 3.3

    Test that PostgreSQL sink.write() works when called from sync context.
    This is the normal usage pattern and must continue working after the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_postgres.sink import PostgresSink

    sink_plugin = PostgresSink()

    catalog = Catalog(
        name="test_pg",
        type="postgres",
        options={
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
        },
    )

    joint = Joint(
        name="test_sink",
        joint_type="sink",
        table="test_table",
        sql=None,
    )

    # Create a sample Arrow table
    sample_table = pa.table(
        {
            "id": list(range(num_rows)),
            "value": [f"row_{i}" for i in range(num_rows)],
        }
    )

    # Create a Material from the Arrow table
    class SimpleMaterializedRef(MaterializedRef):
        def __init__(self, table: pa.Table):
            self._table = table

        def to_arrow(self) -> pa.Table:
            return self._table

        @property
        def schema(self):
            from rivet_core.models import Column, Schema

            return Schema(
                columns=[
                    Column(name=field.name, type=str(field.type), nullable=field.nullable)
                    for field in self._table.schema
                ]
            )

        @property
        def row_count(self) -> int:
            return self._table.num_rows

        @property
        def size_bytes(self) -> int | None:
            return None

        @property
        def storage_type(self) -> str:
            return "memory"

    material = Material(
        name="test_material",
        catalog="test_pg",
        materialized_ref=SimpleMaterializedRef(sample_table),
        state="materialized",
    )

    # Mock psycopg connection
    mock_copy = AsyncMock()
    mock_copy.__aenter__ = AsyncMock(return_value=mock_copy)
    mock_copy.__aexit__ = AsyncMock(return_value=None)
    mock_copy.write_row = AsyncMock()

    mock_cursor = AsyncMock()
    mock_cursor.copy = MagicMock(return_value=mock_copy)
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=None)
    mock_cursor.execute = AsyncMock()

    mock_conn = AsyncMock()
    mock_conn.cursor = MagicMock(return_value=mock_cursor)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.commit = AsyncMock()

    # Call write() from sync context (no event loop) - should work
    with patch("psycopg.AsyncConnection.connect", return_value=mock_conn):
        sink_plugin.write(catalog, joint, material, strategy="replace")

    # If we get here, the write completed successfully


# ---------------------------------------------------------------------------
# Property 2.3: PostgreSQL Catalog Operations Work
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_property_postgres_catalog_list_tables_works() -> None:
    """Property 2.3: PostgreSQL catalog operations work correctly.

    Validates: Requirement 3.2

    Test that PostgreSQL catalog.list_tables() works correctly.
    Catalog operations use sync psycopg API and should be unaffected by the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_postgres.catalog import PostgresCatalogPlugin

    catalog = Catalog(
        name="test_pg",
        type="postgres",
        options={
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "schema": "public",
        },
    )

    plugin = PostgresCatalogPlugin()

    # Mock psycopg sync connection (catalog uses sync API)
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("public", "users", "BASE TABLE"),
        ("public", "orders", "BASE TABLE"),
        ("public", "products", "VIEW"),
    ]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

    # Call list_tables() - should work (uses sync API)
    with patch("psycopg.connect", return_value=mock_conn):
        nodes = plugin.list_tables(catalog)

    # Verify results
    assert len(nodes) == 3
    assert nodes[0].name == "users"
    assert nodes[0].node_type == "table"
    assert nodes[1].name == "orders"
    assert nodes[2].name == "products"
    assert nodes[2].node_type == "view"


@pytest.mark.integration
def test_property_postgres_catalog_get_schema_works() -> None:
    """Property 2.3: PostgreSQL catalog.get_schema() works correctly.

    Validates: Requirement 3.2

    Test that PostgreSQL catalog.get_schema() works correctly.
    Catalog operations use sync psycopg API and should be unaffected by the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_postgres.catalog import PostgresCatalogPlugin

    catalog = Catalog(
        name="test_pg",
        type="postgres",
        options={
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "schema": "public",
        },
    )

    plugin = PostgresCatalogPlugin()

    # Mock psycopg sync connection
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("id", "integer", "NO", None, False),
        ("name", "text", "YES", None, False),
        ("created_at", "timestamp without time zone", "NO", "CURRENT_TIMESTAMP", False),
    ]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

    # Call get_schema() - should work (uses sync API)
    with patch("psycopg.connect", return_value=mock_conn):
        schema = plugin.get_schema(catalog, "users")

    # Verify schema
    assert len(schema.columns) == 3
    assert schema.columns[0].name == "id"
    assert schema.columns[0].type == "int32"  # PostgreSQL integer maps to Arrow int32
    assert schema.columns[0].nullable is False
    assert schema.columns[1].name == "name"
    assert schema.columns[1].nullable is True


# ---------------------------------------------------------------------------
# Property 2.4: PostgreSQL Connection Configuration Respected
# ---------------------------------------------------------------------------


@pytest.mark.integration
@settings(max_examples=50)
@given(postgres_connection_config_strategy())
def test_property_postgres_connection_config_respected(config: dict[str, Any]) -> None:
    """Property 2.4: PostgreSQL connection configuration is respected.

    Validates: Requirement 3.4

    Test that PostgreSQL connection parameters (host, port, database, credentials)
    are correctly used when establishing connections. This behavior must be
    preserved after the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_postgres.source import PostgresDeferredMaterializedRef

    # Build conninfo string from config
    conninfo = (
        f"host={config['host']} "
        f"port={config['port']} "
        f"dbname={config['database']} "
        f"user={config['user']} "
        f"password={config['password']}"
    )

    ref = PostgresDeferredMaterializedRef(
        conninfo=conninfo,
        sql="SELECT 1 as id",
    )

    # Mock psycopg connection
    mock_col_id = MagicMock()
    mock_col_id.name = "id"
    mock_col_id.type_code = 23  # INTEGER

    mock_cursor = AsyncMock()
    mock_cursor.description = [mock_col_id]
    mock_cursor.fetchall = AsyncMock(return_value=[(1,)])

    mock_conn = AsyncMock()
    mock_conn.cursor = MagicMock(return_value=mock_cursor)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)

    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=None)

    # Call to_arrow() and verify connection string is used
    with patch("psycopg.AsyncConnection.connect") as mock_connect:
        mock_connect.return_value = mock_conn

        result = ref.to_arrow()

        # Verify connect was called with the correct conninfo
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[0][0]

        # Verify all connection parameters are in the conninfo string
        assert config["host"] in call_args
        assert str(config["port"]) in call_args
        assert config["database"] in call_args
        assert config["user"] in call_args
        assert config["password"] in call_args

    # Verify result is valid
    assert isinstance(result, pa.Table)
    assert result.num_rows == 1


# ---------------------------------------------------------------------------
# Property 2.5: PostgreSQL Adapters Work
# ---------------------------------------------------------------------------


@pytest.mark.integration
@settings(max_examples=30)
@given(st.integers(min_value=1, max_value=50))
def test_property_postgres_duckdb_adapter_works_in_sync_context(num_rows: int) -> None:
    """Property 2.5: PostgreSQL DuckDB adapter works in sync context.

    Validates: Requirement 3.5

    Test that PostgreSQL DuckDB adapter read_dispatch() works when called from
    sync context. This is the normal usage pattern and must continue working.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_postgres.adapters.duckdb import PostgresDuckDBAdapter

    adapter = PostgresDuckDBAdapter()

    catalog = Catalog(
        name="test_pg",
        type="postgres",
        options={
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
        },
    )

    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_pg",
        table="test_table",
    )

    # Mock DuckDB engine
    mock_engine = MagicMock()
    mock_engine.name = "test_duckdb"

    # Mock the DuckDB connection and query execution
    mock_result = MagicMock()
    mock_result.arrow.return_value = pa.table(
        {
            "id": list(range(num_rows)),
            "value": [f"row_{i}" for i in range(num_rows)],
        }
    )

    mock_duckdb_conn = MagicMock()
    mock_duckdb_conn.execute.return_value = mock_result
    mock_duckdb_conn.close = MagicMock()

    # Call read_dispatch() from sync context - should work
    with patch("duckdb.connect", return_value=mock_duckdb_conn):
        result = adapter.read_dispatch(mock_engine, catalog, joint, pushdown=None)

    # Verify result structure
    assert isinstance(result.material, Material)
    assert result.material.state == "deferred"  # Adapter returns deferred
    assert result.material.materialized_ref is not None

    # Verify the deferred ref can be materialized
    with patch("duckdb.connect", return_value=mock_duckdb_conn):
        table = result.material.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == num_rows


# ---------------------------------------------------------------------------
# Property 2.6: Multiple PostgreSQL Sources in Same Pipeline
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_property_multiple_postgres_sources_work_in_sync_context() -> None:
    """Property 2.6: Multiple PostgreSQL sources in same pipeline work correctly.

    Validates: Requirement 3.3

    Test that multiple PostgreSQL sources can be used in the same pipeline
    when called from sync context. This behavior must be preserved.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_postgres.source import PostgresDeferredMaterializedRef

    # Create two different PostgreSQL refs
    ref1 = PostgresDeferredMaterializedRef(
        conninfo="host=localhost port=5432 dbname=testdb user=testuser password=testpass",
        sql="SELECT * FROM table1",
    )

    ref2 = PostgresDeferredMaterializedRef(
        conninfo="host=localhost port=5432 dbname=testdb user=testuser password=testpass",
        sql="SELECT * FROM table2",
    )

    # Mock psycopg connections
    def create_mock_conn(table_name: str, num_rows: int):
        mock_rows = [(i, f"{table_name}_row_{i}") for i in range(num_rows)]

        mock_col_id = MagicMock()
        mock_col_id.name = "id"
        mock_col_id.type_code = 23  # INTEGER

        mock_col_value = MagicMock()
        mock_col_value.name = "value"
        mock_col_value.type_code = 25  # TEXT

        mock_cursor = AsyncMock()
        mock_cursor.description = [mock_col_id, mock_col_value]
        mock_cursor.fetchall = AsyncMock(return_value=mock_rows)

        mock_conn = AsyncMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        return mock_conn

    # Call to_arrow() on both refs from sync context - should work
    with patch("psycopg.AsyncConnection.connect") as mock_connect:
        # First ref
        mock_connect.return_value = create_mock_conn("table1", 5)
        result1 = ref1.to_arrow()

        # Second ref
        mock_connect.return_value = create_mock_conn("table2", 3)
        result2 = ref2.to_arrow()

    # Verify both results are valid
    assert isinstance(result1, pa.Table)
    assert result1.num_rows == 5
    assert isinstance(result2, pa.Table)
    assert result2.num_rows == 3


# ---------------------------------------------------------------------------
# Property 2.7: CSV Source Works (Filesystem Plugin)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_property_csv_source_works(tmp_path: Path) -> None:
    """Property 2.1: CSV sources continue working.

    Validates: Requirement 3.1

    Test that CSV (filesystem) sources work correctly. This is a non-PostgreSQL
    source that must continue working after the fix.

    EXPECTED ON UNFIXED CODE: Test PASSES
    EXPECTED ON FIXED CODE: Test PASSES (no regression)
    """
    from rivet_core.plugins import PluginRegistry

    num_rows = 10  # Fixed value for non-property test

    # Create test CSV file
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_content = "id,value\n" + "\n".join([f"{i},row_{i}" for i in range(num_rows)])
    (data_dir / "test.csv").write_text(csv_content)

    # Setup registry
    registry = PluginRegistry()
    registry.register_builtins()

    catalog = Catalog(
        name="local",
        type="filesystem",
        options={"path": str(data_dir), "format": "csv"},
    )

    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="local",
        table="test",
    )

    # Get filesystem source plugin
    source_plugin = registry._sources.get("filesystem")
    assert source_plugin is not None, "Filesystem source plugin not registered"

    # Read and materialize - should work
    material = source_plugin.read(catalog, joint, pushdown=None)
    table = material.to_arrow()

    # Verify result
    assert isinstance(table, pa.Table)
    assert table.num_rows == num_rows
    assert "id" in table.column_names
    assert "value" in table.column_names
