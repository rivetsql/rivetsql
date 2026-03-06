"""UnityPySparkAdapter: REST API + credential vending + Delta read/write."""

from __future__ import annotations

import logging
from typing import Any

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Material
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_pyspark.adapters._detection import _has_delta_jars
from rivet_pyspark.adapters.pushdown import _apply_pyspark_pushdown
from rivet_pyspark.engine import SparkDataFrameMaterializedRef

_logger = logging.getLogger(__name__)

_READ_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
]

_WRITE_CAPABILITIES = [
    "write_append",
    "write_replace",
    "write_partition",
    "write_merge",
    "write_scd2",
]


def _get_spark_version(session: Any) -> str:
    """Return the Spark version string, e.g. '3.5.1'."""
    try:
        return session.version  # type: ignore[no-any-return]
    except Exception:
        return "unknown"


# Spark major.minor → recommended delta-spark Maven coordinate
_DELTA_SPARK_COMPAT: dict[str, str] = {
    "4.1": "io.delta:delta-spark_2.13:4.0.0",
    "4.0": "io.delta:delta-spark_2.13:4.0.0",
    "3.5": "io.delta:delta-spark_2.12:3.2.1",
    "3.4": "io.delta:delta-spark_2.12:2.4.0",
    "3.3": "io.delta:delta-spark_2.12:2.3.0",
}

_DELTA_SPARK_DEFAULT = "io.delta:delta-spark_2.13:4.0.0"


def _recommended_delta_package(spark_version: str) -> str:
    """Pick the right delta-spark package for the detected Spark version."""
    parts = spark_version.split(".")
    if len(parts) >= 2:
        key = f"{parts[0]}.{parts[1]}"
        return _DELTA_SPARK_COMPAT.get(key, _DELTA_SPARK_DEFAULT)
    return _DELTA_SPARK_DEFAULT


def _resolve_full_name(joint: Any, catalog: Any) -> str:
    """Build three-part Unity table name from joint and catalog options."""
    table = getattr(joint, "table", None) or joint.name
    if "." in str(table):
        return table  # type: ignore[no-any-return]
    catalog_name = catalog.options.get("catalog_name", "")
    schema = catalog.options.get("schema", "default")
    return f"{catalog_name}.{schema}.{table}"


def _configure_spark_credentials(session: Any, credentials: dict[str, Any]) -> None:
    """Set Hadoop config on the SparkSession from vended credentials."""
    conf = session.sparkContext._jsc.hadoopConfiguration()

    aws_creds = credentials.get("aws_temp_credentials")
    if aws_creds:
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("fs.s3a.access.key", aws_creds.get("access_key_id", ""))
        conf.set("fs.s3a.secret.key", aws_creds.get("secret_access_key", ""))
        token = aws_creds.get("session_token")
        if token:
            conf.set("fs.s3a.session.token", token)
            conf.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )
        return

    azure_creds = credentials.get("azure_user_delegation_sas")
    if azure_creds:
        sas_token = azure_creds.get("sas_token", "")
        conf.set("fs.azure.sas.token", sas_token)
        return

    gcs_creds = credentials.get("gcp_oauth_token")
    if gcs_creds:
        conf.set("fs.gs.auth.access.token", gcs_creds.get("oauth_token", ""))
        return

    _logger.warning(
        "Unrecognized credential format from Unity vending: %s. "
        "Falling back to ambient cloud credentials.",
        list(credentials.keys()),
    )


def _delta_write_mode(strategy: str) -> str:
    """Map Rivet write strategy to Spark Delta write mode."""
    return {
        "append": "append",
        "replace": "overwrite",
        "merge": "overwrite",
        "scd2": "overwrite",
        "partition": "overwrite",
    }.get(strategy, "overwrite")


class UnityPySparkAdapter(ComputeEngineAdapter):
    """PySpark adapter for Unity catalog: REST API metadata + credential vending + Delta."""

    target_engine_type = "pyspark"
    catalog_type = "unity"
    capabilities = _READ_CAPABILITIES + _WRITE_CAPABILITIES
    source = "engine_plugin"
    source_plugin = "rivet_pyspark"

    def _get_unity_plugin(self) -> Any:
        """Retrieve the Unity catalog plugin from the registry (no cross-plugin import)."""
        if self._registry is None:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    "UnityPySparkAdapter has no plugin registry; cannot resolve Unity catalog plugin.",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Ensure the adapter is registered via PluginRegistry.register_adapter().",
                )
            )
        plugin = self._registry.get_catalog_plugin("unity")
        if plugin is None:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    "Unity catalog plugin not registered.",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Install and register the rivet_databricks plugin.",
                )
            )
        return plugin

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        session = engine.get_session()

        if not _has_delta_jars(session):
            spark_ver = _get_spark_version(session)
            delta_pkg = _recommended_delta_package(spark_ver)
            raise PluginValidationError(
                plugin_error(
                    "RVT-203",
                    f"Delta Lake JARs not found on Spark classpath (Spark {spark_ver}).",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation=(
                        "Add Delta Lake JARs to your PySpark engine in your Rivet profile:\n\n"
                        "  engines:\n"
                        "    - name: <your_engine>\n"
                        "      type: pyspark\n"
                        "      config:\n"
                        f'        spark.jars.packages: "{delta_pkg}"\n'
                        '        spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"\n'
                        '        spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"'
                    ),
                    spark_version=spark_ver,
                    recommended_package=delta_pkg,
                )
            )

        full_name = _resolve_full_name(joint, catalog)

        plugin = self._get_unity_plugin()
        table_meta = plugin.resolve_table_reference(full_name, catalog)

        storage_location = table_meta.get("storage_location")
        if not storage_location:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"No storage_location for Unity table '{full_name}'.",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Verify the table exists and has a storage location.",
                    table=full_name,
                )
            )

        credentials = table_meta.get("temporary_credentials")
        if credentials is None:
            raise PluginValidationError(
                plugin_error(
                    "RVT-204",
                    f"Credential vending is disabled for Unity table '{full_name}'.",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Enable credential vending on the Unity Catalog endpoint "
                    "or configure ambient cloud credentials.",
                    table=full_name,
                )
            )

        _configure_spark_credentials(session, credentials)

        try:
            df = session.read.format("delta").load(storage_location)
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"PySpark Unity read failed for '{full_name}': {exc}",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Check storage location accessibility and credential validity.",
                    table=full_name,
                    storage_location=storage_location,
                )
            ) from exc

        df, residual = _apply_pyspark_pushdown(df, pushdown)

        ref = SparkDataFrameMaterializedRef(df)
        material = Material(
            name=joint.name,
            catalog=getattr(catalog, "name", "unity"),
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        session = engine.get_session()

        if not _has_delta_jars(session):
            spark_ver = _get_spark_version(session)
            delta_pkg = _recommended_delta_package(spark_ver)
            raise PluginValidationError(
                plugin_error(
                    "RVT-203",
                    f"Delta Lake JARs not found on Spark classpath (Spark {spark_ver}).",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation=(
                        "Add Delta Lake JARs to your PySpark engine in your Rivet profile:\n\n"
                        "  engines:\n"
                        "    - name: <your_engine>\n"
                        "      type: pyspark\n"
                        "      config:\n"
                        f'        spark.jars.packages: "{delta_pkg}"\n'
                        '        spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"\n'
                        '        spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"'
                    ),
                    spark_version=spark_ver,
                    recommended_package=delta_pkg,
                )
            )

        full_name = _resolve_full_name(joint, catalog)

        plugin = self._get_unity_plugin()
        credentials = plugin.vend_credentials(full_name, catalog, operation="READ_WRITE")

        if credentials is None:
            raise PluginValidationError(
                plugin_error(
                    "RVT-204",
                    f"Credential vending is disabled for Unity table '{full_name}'.",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Enable credential vending on the Unity Catalog endpoint "
                    "or configure ambient cloud credentials.",
                    table=full_name,
                )
            )

        table_meta = plugin.resolve_table_reference(full_name, catalog)
        storage_location = table_meta.get("storage_location")
        if not storage_location:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"No storage_location for Unity table '{full_name}'.",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Verify the table exists and has a storage location.",
                    table=full_name,
                )
            )

        _configure_spark_credentials(session, credentials)

        strategy = getattr(joint, "write_strategy", None) or "replace"
        mode = _delta_write_mode(strategy)

        arrow_table = material.materialized_ref.to_arrow()
        df = session.createDataFrame(arrow_table.to_pandas())

        try:
            partition_by = getattr(joint, "partition_by", None)
            writer = df.write.format("delta").mode(mode)
            if strategy == "partition" and partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(storage_location)
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"PySpark Unity write failed for '{full_name}': {exc}",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Check storage location write permissions and credential validity.",
                    table=full_name,
                    storage_location=storage_location,
                    strategy=strategy,
                )
            ) from exc

        return material
