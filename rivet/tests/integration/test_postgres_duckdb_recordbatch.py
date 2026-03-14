"""Integration test for PostgreSQL DuckDB adapter RecordBatchReader handling.

Bug #5: Verifies that the adapter correctly handles RecordBatchReader objects
returned by DuckDB's postgres_scanner extension and converts them to Table
before accessing num_rows.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa

from rivet_core.models import Catalog, Joint
from rivet_postgres.adapters.duckdb import PostgresDuckDBAdapter


class TestPostgresDuckDBRecordBatchReader:
    """Test that PostgresDuckDBAdapter handles RecordBatchReader correctly."""

    def test_adapter_converts_recordbatchreader_to_table(self) -> None:
        """Verify adapter converts RecordBatchReader to Table before accessing num_rows.

        Bug #5: DuckDB's postgres_scanner can return RecordBatchReader instead of Table.
        RecordBatchReader doesn't have num_rows attribute, so we must convert to Table first.
        """
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
            table="customers",
        )

        # Create a mock RecordBatchReader that returns data
        test_data = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        mock_reader = MagicMock(spec=pa.RecordBatchReader)
        mock_reader.read_all.return_value = test_data

        # Mock DuckDB connection to return RecordBatchReader
        mock_conn = MagicMock()
        mock_execute_result = MagicMock()
        mock_execute_result.arrow.return_value = mock_reader
        mock_conn.execute.return_value = mock_execute_result

        with patch("duckdb.connect", return_value=mock_conn):
            result = adapter.read_dispatch(None, catalog, joint, pushdown=None)

            # Access row_count - this should NOT raise AttributeError
            # Bug #5: Before fix, this would fail with:
            # 'pyarrow.lib.RecordBatchReader' object has no attribute 'num_rows'
            assert result.material.materialized_ref is not None
            row_count = result.material.materialized_ref.row_count

            assert row_count == 3, f"Expected 3 rows, got {row_count}"

    def test_adapter_handles_table_directly(self) -> None:
        """Verify adapter also works when DuckDB returns Table directly."""
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
            table="orders",
        )

        # Create test data as Table (not RecordBatchReader)
        test_data = pa.table({"order_id": [101, 102], "amount": [50.0, 75.0]})

        # Mock DuckDB connection to return Table directly
        mock_conn = MagicMock()
        mock_execute_result = MagicMock()
        mock_execute_result.arrow.return_value = test_data
        mock_conn.execute.return_value = mock_execute_result

        with patch("duckdb.connect", return_value=mock_conn):
            result = adapter.read_dispatch(None, catalog, joint, pushdown=None)

            # Access row_count - should work for Table too
            assert result.material.materialized_ref is not None
            row_count = result.material.materialized_ref.row_count

            assert row_count == 2, f"Expected 2 rows, got {row_count}"

    def test_adapter_schema_access_with_recordbatchreader(self) -> None:
        """Verify schema property works with RecordBatchReader."""
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
            table="products",
        )

        # Create a mock RecordBatchReader
        test_data = pa.table({"product_id": [1, 2], "price": [10.99, 20.99]})
        mock_reader = MagicMock(spec=pa.RecordBatchReader)
        mock_reader.read_all.return_value = test_data

        # Mock DuckDB connection
        mock_conn = MagicMock()
        mock_execute_result = MagicMock()
        mock_execute_result.arrow.return_value = mock_reader
        mock_conn.execute.return_value = mock_execute_result

        with patch("duckdb.connect", return_value=mock_conn):
            result = adapter.read_dispatch(None, catalog, joint, pushdown=None)

            # Access schema - should work after RecordBatchReader conversion
            assert result.material.materialized_ref is not None
            schema = result.material.materialized_ref.schema

            assert len(schema.columns) == 2
            assert schema.columns[0].name == "product_id"
            assert schema.columns[1].name == "price"
