"""Integration tests for PostgreSQL engine connection configuration formats."""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_postgres.engine import PostgresComputeEngine, PostgresComputeEnginePlugin


class TestPostgresEngineConnectionConfig:
    """Test PostgreSQL engine accepts both conninfo and individual parameters."""

    def test_engine_accepts_individual_parameters(self) -> None:
        """Engine should build conninfo from individual parameters."""
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
        }
        engine = PostgresComputeEngine(name="test_pg", config=config)
        conninfo = engine._build_conninfo()

        assert "host=localhost" in conninfo
        assert "port=5432" in conninfo
        assert "dbname=testdb" in conninfo
        assert "user=testuser" in conninfo
        assert "password=testpass" in conninfo

    def test_engine_accepts_conninfo_string(self) -> None:
        """Engine should use explicit conninfo string when provided."""
        config = {
            "conninfo": "host=myhost port=5433 dbname=mydb user=myuser password=mypass",
        }
        engine = PostgresComputeEngine(name="test_pg", config=config)
        conninfo = engine._build_conninfo()

        assert conninfo == "host=myhost port=5433 dbname=mydb user=myuser password=mypass"

    def test_engine_rejects_both_formats(self) -> None:
        """Engine should reject config with both conninfo and individual parameters."""
        plugin = PostgresComputeEnginePlugin()
        config = {
            "conninfo": "host=localhost",
            "host": "otherhost",
        }

        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate(config)

        assert "cannot specify both" in str(exc_info.value).lower()

    def test_engine_uses_defaults_for_missing_parameters(self) -> None:
        """Engine should use default values for missing connection parameters."""
        config = {
            "database": "mydb",
        }
        engine = PostgresComputeEngine(name="test_pg", config=config)
        conninfo = engine._build_conninfo()

        assert "host=localhost" in conninfo  # default
        assert "port=5432" in conninfo  # default
        assert "dbname=mydb" in conninfo
        assert "user=" in conninfo  # empty default
        assert "password=" in conninfo  # empty default

    def test_validation_accepts_individual_parameters(self) -> None:
        """Validation should accept individual connection parameters."""
        plugin = PostgresComputeEnginePlugin()
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
        }

        # Should not raise
        plugin.validate(config)

    def test_validation_accepts_conninfo(self) -> None:
        """Validation should accept conninfo string."""
        plugin = PostgresComputeEnginePlugin()
        config = {
            "conninfo": "host=localhost port=5432 dbname=testdb",
        }

        # Should not raise
        plugin.validate(config)

    def test_validation_rejects_invalid_port(self) -> None:
        """Validation should reject invalid port values."""
        plugin = PostgresComputeEnginePlugin()

        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"port": 99999})
        assert "port" in str(exc_info.value).lower()

        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"port": 0})
        assert "port" in str(exc_info.value).lower()

    def test_validation_rejects_invalid_types(self) -> None:
        """Validation should reject invalid parameter types."""
        plugin = PostgresComputeEnginePlugin()

        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"host": 123})
        assert "host" in str(exc_info.value).lower()

        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"database": ["not", "a", "string"]})
        assert "database" in str(exc_info.value).lower()
