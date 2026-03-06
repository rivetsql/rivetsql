"""Tests for structured plugin errors: code, message, context, remediation for all plugins.

Validates Property 15: For any PluginValidationError, the contained error has a code
matching RVT-2xx, a non-empty message, and a non-None remediation string.

Also validates Requirement 34: Plugin errors include plugin_name, plugin_type,
adapter identity (if applicable), human-readable message, and remediation suggestion.
"""

import pytest

from rivet_core.errors import (
    ExecutionError,
    PluginValidationError,
    RivetError,
    plugin_error,
)

# ── plugin_error helper tests ──────────────────────────────────────────


class TestPluginErrorHelper:
    def test_creates_rivet_error_with_all_fields(self) -> None:
        err = plugin_error(
            "RVT-201",
            "Missing option 'host'.",
            plugin_name="rivet_postgres",
            plugin_type="catalog",
            remediation="Provide 'host' in the catalog options.",
        )
        assert isinstance(err, RivetError)
        assert err.code == "RVT-201"
        assert err.message == "Missing option 'host'."
        assert err.remediation == "Provide 'host' in the catalog options."
        assert err.context["plugin_name"] == "rivet_postgres"
        assert err.context["plugin_type"] == "catalog"

    def test_includes_adapter_in_context(self) -> None:
        err = plugin_error(
            "RVT-501",
            "S3 read failed.",
            plugin_name="rivet_duckdb",
            plugin_type="adapter",
            adapter="S3DuckDBAdapter",
            remediation="Check S3 credentials.",
        )
        assert err.context["adapter"] == "S3DuckDBAdapter"
        assert err.context["plugin_name"] == "rivet_duckdb"
        assert err.context["plugin_type"] == "adapter"

    def test_extra_context_kwargs(self) -> None:
        err = plugin_error(
            "RVT-201",
            "Unknown option.",
            plugin_name="rivet_duckdb",
            plugin_type="engine",
            remediation="Check docs.",
            option="bad_opt",
            value=42,
        )
        assert err.context["option"] == "bad_opt"
        assert err.context["value"] == 42

    def test_remediation_is_required(self) -> None:
        err = plugin_error(
            "RVT-201",
            "Test error.",
            plugin_name="rivet_duckdb",
            plugin_type="catalog",
            remediation="Fix it.",
        )
        assert err.remediation is not None
        assert err.remediation != ""

    def test_str_includes_code_and_remediation(self) -> None:
        err = plugin_error(
            "RVT-502",
            "Extension load failed.",
            plugin_name="rivet_duckdb",
            plugin_type="engine",
            remediation="Run INSTALL httpfs.",
        )
        s = str(err)
        assert "[RVT-502]" in s
        assert "Remediation:" in s


# ── DuckDB plugin error structure ──────────────────────────────────────


class TestDuckDBPluginErrors:
    def test_catalog_unknown_option(self) -> None:
        from rivet_duckdb.catalog import DuckDBCatalogPlugin

        plugin = DuckDBCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"bad_option": True})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.message
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_duckdb"
        assert err.context["plugin_type"] == "catalog"

    def test_engine_unknown_option(self) -> None:
        from rivet_duckdb.engine import DuckDBComputeEnginePlugin

        plugin = DuckDBComputeEnginePlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"unknown_opt": 1})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_duckdb"
        assert err.context["plugin_type"] == "engine"

    def test_engine_invalid_threads(self) -> None:
        from rivet_duckdb.engine import DuckDBComputeEnginePlugin

        plugin = DuckDBComputeEnginePlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"threads": "not_int"})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_duckdb"

    def test_filesystem_reader_unrecognized_extension(self) -> None:
        from rivet_duckdb.engine import infer_filesystem_reader

        with pytest.raises(ExecutionError) as exc_info:
            infer_filesystem_reader("data.xyz")
        err = exc_info.value.error
        assert err.code == "RVT-501"
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_duckdb"
        assert err.context["plugin_type"] == "engine"


# ── PostgreSQL plugin error structure ──────────────────────────────────


class TestPostgresPluginErrors:
    def test_catalog_missing_required(self) -> None:
        from rivet_postgres.catalog import PostgresCatalogPlugin

        plugin = PostgresCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_postgres"
        assert err.context["plugin_type"] == "catalog"

    def test_catalog_unknown_option(self) -> None:
        from rivet_postgres.catalog import PostgresCatalogPlugin

        plugin = PostgresCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"bad_opt": True})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.context["plugin_name"] == "rivet_postgres"

    def test_engine_unknown_option(self) -> None:
        from rivet_postgres.engine import PostgresComputeEnginePlugin

        plugin = PostgresComputeEnginePlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"bad_opt": True})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_postgres"
        assert err.context["plugin_type"] == "engine"

    def test_sink_unknown_option(self) -> None:
        from rivet_postgres.sink import PostgresSink

        sink = PostgresSink()
        with pytest.raises(PluginValidationError) as exc_info:
            sink.validate_options({"bad_opt": True})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_postgres"
        assert err.context["plugin_type"] == "sink"

    def test_sink_invalid_strategy(self) -> None:
        from rivet_postgres.sink import PostgresSink

        sink = PostgresSink()
        with pytest.raises(PluginValidationError) as exc_info:
            sink.validate_options({"table": "t", "write_strategy": "invalid"})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_postgres"


# ── AWS plugin error structure ─────────────────────────────────────────


class TestAWSPluginErrors:
    def test_s3_catalog_unknown_option(self) -> None:
        from rivet_aws.s3_catalog import S3CatalogPlugin

        plugin = S3CatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"bad_opt": True})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_aws"
        assert err.context["plugin_type"] == "catalog"

    def test_s3_catalog_missing_bucket(self) -> None:
        from rivet_aws.s3_catalog import S3CatalogPlugin

        plugin = S3CatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_aws"

    def test_glue_catalog_invalid_auth_type(self) -> None:
        from rivet_aws.glue_catalog import GlueCatalogPlugin

        plugin = GlueCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"auth_type": "bad"})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_aws"
        assert err.context["plugin_type"] == "catalog"


# ── Unity plugin error structure ───────────────────────────────────────


class TestDatabricksPluginErrors:
    def test_unity_catalog_unknown_option(self) -> None:
        from rivet_databricks.unity_catalog import UnityCatalogPlugin

        plugin = UnityCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"bad_opt": True})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_databricks"
        assert err.context["plugin_type"] == "catalog"

    def test_unity_catalog_missing_required(self) -> None:
        from rivet_databricks.unity_catalog import UnityCatalogPlugin

        plugin = UnityCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_databricks"

    def test_databricks_catalog_missing_workspace_url(self) -> None:
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

        plugin = DatabricksCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_databricks"

    def test_databricks_catalog_invalid_workspace_url(self) -> None:
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

        plugin = DatabricksCatalogPlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"workspace_url": "http://bad.com", "catalog": "main"})
        err = exc_info.value.error
        assert err.code == "RVT-202"
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_databricks"
        assert err.context["workspace_url"] == "http://bad.com"


# ── Polars plugin error structure ──────────────────────────────────────


class TestPolarsPluginErrors:
    def test_engine_unknown_option(self) -> None:
        from rivet_polars.engine import PolarsComputeEnginePlugin

        plugin = PolarsComputeEnginePlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"bad_opt": True})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_polars"
        assert err.context["plugin_type"] == "engine"


# ── PySpark plugin error structure ─────────────────────────────────────


class TestPySparkPluginErrors:
    def test_engine_unknown_option(self) -> None:
        from rivet_pyspark.engine import PySparkComputeEnginePlugin

        plugin = PySparkComputeEnginePlugin()
        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate({"bad_opt": True})
        err = exc_info.value.error
        assert err.code.startswith("RVT-2")
        assert err.remediation
        assert err.context["plugin_name"] == "rivet_pyspark"
        assert err.context["plugin_type"] == "engine"


# ── Cross-cutting: Property 15 validation ──────────────────────────────


class TestProperty15:
    """Property 15: For any PluginValidationError, the contained error has a code
    matching RVT-2xx, a non-empty message, and a non-None remediation string."""

    @pytest.mark.parametrize(
        "plugin_cls,options",
        [
            ("rivet_duckdb.catalog:DuckDBCatalogPlugin", {"bad": 1}),
            ("rivet_duckdb.engine:DuckDBComputeEnginePlugin", {"bad": 1}),
            ("rivet_postgres.catalog:PostgresCatalogPlugin", {}),
            ("rivet_postgres.engine:PostgresComputeEnginePlugin", {"bad": 1}),
            ("rivet_aws.s3_catalog:S3CatalogPlugin", {}),
            ("rivet_aws.glue_catalog:GlueCatalogPlugin", {"auth_type": "bad"}),
            ("rivet_databricks.unity_catalog:UnityCatalogPlugin", {}),
            ("rivet_databricks.databricks_catalog:DatabricksCatalogPlugin", {}),
            ("rivet_polars.engine:PolarsComputeEnginePlugin", {"bad": 1}),
            ("rivet_pyspark.engine:PySparkComputeEnginePlugin", {"bad": 1}),
        ],
    )
    def test_validation_error_structure(self, plugin_cls: str, options: dict) -> None:
        """Every PluginValidationError has RVT-2xx code, non-empty message, non-None remediation."""
        module_path, cls_name = plugin_cls.rsplit(":", 1)
        import importlib

        mod = importlib.import_module(module_path)
        cls = getattr(mod, cls_name)
        plugin = cls()

        with pytest.raises(PluginValidationError) as exc_info:
            plugin.validate(options)

        err = exc_info.value.error
        # Code matches RVT-2xx
        assert err.code.startswith("RVT-2"), f"Expected RVT-2xx, got {err.code}"
        # Non-empty message
        assert err.message, "Error message must not be empty"
        # Non-None remediation
        assert err.remediation is not None, "Remediation must not be None"
        assert err.remediation, "Remediation must not be empty"
        # Context includes plugin_name and plugin_type
        assert "plugin_name" in err.context, "Context must include plugin_name"
        assert "plugin_type" in err.context, "Context must include plugin_type"
