"""Bug condition exploration test for PostgreSQL asyncio event loop crash.

**Validates: Requirements 1.1, 2.1, 2.2**

This test demonstrates the bug where PostgreSQL operations crash with RuntimeError
when called from within an existing asyncio event loop (e.g., REPL/explore context).

CRITICAL: This test is EXPECTED TO FAIL on unfixed code.
- Failure confirms the bug exists
- When the fix is implemented, this test will pass

The test directly calls PostgreSQL plugin methods from within an async context
to reproduce the exact bug condition.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.models import Catalog, Joint, Material
from rivet_core.strategies import MaterializedRef


@pytest.fixture
def mock_postgres_catalog() -> Catalog:
    """Create a mock PostgreSQL catalog for testing."""
    return Catalog(
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


@pytest.fixture
def mock_postgres_engine_config() -> dict:
    """Create mock PostgreSQL engine config."""
    return {
        "conninfo": "host=localhost port=5432 dbname=testdb user=testuser password=testpass",
        "pool_min_size": 1,
        "pool_max_size": 10,
    }


@pytest.fixture
def sample_arrow_table() -> pa.Table:
    """Create a sample Arrow table for testing."""
    return pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300],
        }
    )


class TestPostgresAsyncioEventLoopBug:
    """Property 1: Fault Condition - PostgreSQL Operations Work in Async Context.

    These tests encode the EXPECTED behavior: PostgreSQL operations should work
    correctly when called from within an existing asyncio event loop.

    EXPECTED OUTCOME ON UNFIXED CODE: Tests FAIL (RuntimeError is raised)
    EXPECTED OUTCOME ON FIXED CODE: Tests PASS (operations complete successfully)

    The tests will FAIL on unfixed code because the bug prevents the expected behavior.
    """

    @pytest.mark.integration
    def test_postgres_source_works_in_async_context(self) -> None:
        """Test that PostgreSQL source.to_arrow() works when called from async context.

        Simulates REPL/explore context where an event loop is already running.

        EXPECTED ON UNFIXED CODE: Test FAILS with RuntimeError
        EXPECTED ON FIXED CODE: Test PASSES, returns Arrow table
        """
        from rivet_postgres.source import PostgresDeferredMaterializedRef

        # Create a deferred ref with mock connection info
        ref = PostgresDeferredMaterializedRef(
            conninfo="host=localhost port=5432 dbname=testdb user=testuser password=testpass",
            sql="SELECT 1 as id, 'test' as name",
        )

        # Mock psycopg to avoid needing a real database
        # Create proper mock description objects
        id_desc = MagicMock()
        id_desc.name = "id"
        name_desc = MagicMock()
        name_desc.name = "name"

        mock_cursor = AsyncMock()
        mock_cursor.description = [id_desc, name_desc]
        mock_cursor.fetchall = AsyncMock(return_value=[(1, "test")])

        mock_conn = AsyncMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)

        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=None)

        # Define async function that calls to_arrow() - simulates REPL context
        async def call_to_arrow():
            # Patch must be active when asyncio.run() is called inside to_arrow()
            with patch("psycopg.AsyncConnection.connect", return_value=mock_conn):
                # This should work on fixed code, crash on unfixed code
                return ref.to_arrow()

        # Run from within asyncio.run() - simulates existing event loop
        # EXPECTED: Returns Arrow table on fixed code, RuntimeError on unfixed code
        result = asyncio.run(call_to_arrow())

        # Verify the result is a valid Arrow table
        assert isinstance(result, pa.Table)
        assert result.num_rows == 1
        assert "id" in result.column_names
        assert "name" in result.column_names

    @pytest.mark.integration
    def test_postgres_engine_works_in_async_context(
        self, mock_postgres_engine_config: dict
    ) -> None:
        """Test that PostgreSQL engine.execute_sql() works when called from async context.

        Simulates REPL/explore context where an event loop is already running.

        EXPECTED ON UNFIXED CODE: Test FAILS with ExecutionError wrapping RuntimeError
        EXPECTED ON FIXED CODE: Test PASSES, returns Arrow table
        """
        from rivet_postgres.engine import PostgresComputeEnginePlugin

        plugin = PostgresComputeEnginePlugin()
        engine = plugin.create_engine("test_engine", mock_postgres_engine_config)

        # Mock the pool and connection
        mock_batch = pa.record_batch([[1], ["test"]], names=["id", "name"])

        async def mock_stream_arrow(sql: str):
            yield mock_batch

        # Define async function that calls execute_sql() - simulates REPL context
        async def call_execute_sql():
            with patch.object(engine, "stream_arrow", side_effect=mock_stream_arrow):
                # This should work on fixed code, crash on unfixed code
                return plugin.execute_sql(
                    engine,
                    "SELECT 1 as id, 'test' as name",
                    input_tables={},
                )

        # Run from within asyncio.run() - simulates existing event loop
        # EXPECTED: Returns Arrow table on fixed code, ExecutionError on unfixed code
        result = asyncio.run(call_execute_sql())

        # Verify the result is a valid Arrow table
        assert isinstance(result, pa.Table)
        assert result.num_rows == 1
        assert "id" in result.column_names
        assert "name" in result.column_names

    @pytest.mark.integration
    def test_postgres_sink_works_in_async_context(
        self, mock_postgres_catalog: Catalog, sample_arrow_table: pa.Table
    ) -> None:
        """Test that PostgreSQL sink.write() works when called from async context.

        Simulates REPL/explore context where an event loop is already running.

        EXPECTED ON UNFIXED CODE: Test FAILS with ExecutionError wrapping RuntimeError
        EXPECTED ON FIXED CODE: Test PASSES, write completes successfully
        """
        from rivet_postgres.sink import PostgresSink

        sink_plugin = PostgresSink()

        # Create a joint for the sink
        joint = Joint(
            name="test_sink",
            joint_type="sink",
            table="test_table",
            sql=None,
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
            materialized_ref=SimpleMaterializedRef(sample_arrow_table),
            state="materialized",
        )

        # Mock psycopg connection with proper async context manager support
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

        # Define async function that calls write() - simulates REPL context
        async def call_write():
            with patch("psycopg.AsyncConnection.connect", return_value=mock_conn):
                # This should work on fixed code, crash on unfixed code
                sink_plugin.write(mock_postgres_catalog, joint, material, strategy="replace")

        # Run from within asyncio.run() - simulates existing event loop
        # EXPECTED: Completes successfully on fixed code, ExecutionError on unfixed code
        asyncio.run(call_write())

        # If we get here, the write completed successfully (expected on fixed code)
