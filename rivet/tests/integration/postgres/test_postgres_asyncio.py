"""PostgreSQL plugin compatibility with running asyncio event loops.

Verifies that source/engine/sink methods on ``rivet_postgres`` work when
they are invoked from inside an existing event loop (REPL, explore session,
Textual TUI). The plugin internally uses ``asyncio.AsyncConnection`` and
must not crash with ``RuntimeError: This event loop is already running``
when the surrounding caller already owns a loop.
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

class TestPostgresAsyncioInRunningLoop:
    """The PostgreSQL plugin (source/engine/sink) must function when its
    methods are called from inside an already-running event loop.
    """

    @pytest.mark.integration
    def test_postgres_source_works_in_async_context(self) -> None:
        """``PostgresDeferredMaterializedRef.to_arrow()`` returns an Arrow table
        when called from an async function dispatched via ``asyncio.run``.
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
        """``PostgresComputeEnginePlugin.execute_sql`` returns an Arrow table
        when invoked from an async function.
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
        """``PostgresSink.write`` completes successfully when invoked from an
        async function.
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
