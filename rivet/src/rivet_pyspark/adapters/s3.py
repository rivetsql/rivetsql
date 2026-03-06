"""S3PySparkAdapter: S3A Hadoop properties, Delta capability promotion when jars present."""

from __future__ import annotations

from typing import Any

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Material
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_pyspark.adapters._detection import _has_delta_jars
from rivet_pyspark.adapters.pushdown import _apply_pyspark_pushdown
from rivet_pyspark.engine import ALL_6_CAPABILITIES, SparkDataFrameMaterializedRef

BASE_CAPABILITIES = [
    *ALL_6_CAPABILITIES,
    "write_append",
    "write_replace",
    "write_partition",
]

DELTA_WRITE_CAPABILITIES = [
    "write_merge",
    "write_scd2",
    "write_incremental_append",
]


def _has_hadoop_aws_jars(session: Any) -> bool:
    """Check if Hadoop AWS (S3A) jars are on the Spark classpath."""
    try:
        jvm = session._jvm
        jvm.java.lang.Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem")
        return True
    except Exception:
        return False


def _configure_s3a(session: Any, catalog_options: dict[str, Any]) -> None:
    """Set Hadoop S3A properties on the SparkSession from catalog credentials."""
    conf = session.sparkContext._jsc.hadoopConfiguration()

    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    key = catalog_options.get("access_key_id")
    secret = catalog_options.get("secret_access_key")
    if key and secret:
        conf.set("fs.s3a.access.key", key)
        conf.set("fs.s3a.secret.key", secret)
        token = catalog_options.get("session_token")
        if token:
            conf.set("fs.s3a.session.token", token)
            conf.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )

    endpoint = catalog_options.get("endpoint_url")
    if endpoint:
        conf.set("fs.s3a.endpoint", endpoint)

    if catalog_options.get("path_style_access"):
        conf.set("fs.s3a.path.style.access", "true")


def _build_s3a_path(catalog_options: dict[str, Any], table: str) -> str:
    """Build the s3a:// URI for Spark reads/writes."""
    bucket = catalog_options["bucket"]
    prefix = catalog_options.get("prefix", "")
    fmt = catalog_options.get("format", "parquet")
    path = f"{prefix}/{table}" if prefix else table
    if fmt == "delta":
        return f"s3a://{bucket}/{path}"
    return f"s3a://{bucket}/{path}.{fmt}"


class S3PySparkAdapter(ComputeEngineAdapter):
    """PySpark adapter for S3 catalog type.

    Configures Spark Hadoop S3A properties with catalog credentials and
    reads via spark.read.<format>("s3a://..."). Promotes Delta write
    capabilities when Delta jars are detected on the Spark classpath.
    """

    target_engine_type = "pyspark"
    catalog_type = "s3"
    capabilities = list(BASE_CAPABILITIES)
    source = "engine_plugin"
    source_plugin = "rivet_pyspark"

    def get_capabilities(self, engine: Any) -> list[str]:
        """Return capabilities, promoting Delta writes if Delta jars are present."""
        session = engine.get_session()
        caps = list(BASE_CAPABILITIES)
        if _has_delta_jars(session):
            caps.extend(DELTA_WRITE_CAPABILITIES)
        return caps

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        session = engine.get_session()

        if not _has_hadoop_aws_jars(session):
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    "Hadoop AWS JARs not found on Spark classpath.",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Add hadoop-aws JAR to Spark classpath via "
                        "'spark.jars.packages' option, e.g. 'org.apache.hadoop:hadoop-aws:3.3.4'.",
                )
            )

        _configure_s3a(session, catalog.options)

        table = getattr(joint, "table", None) or joint.name
        path = _build_s3a_path(catalog.options, table)
        fmt = catalog.options.get("format", "parquet")

        try:
            if fmt == "delta":
                df = session.read.format("delta").load(path)
            elif fmt == "parquet":
                df = session.read.parquet(path)
            elif fmt == "csv":
                df = session.read.csv(path, header=True, inferSchema=True)
            elif fmt == "json":
                df = session.read.json(path)
            elif fmt == "orc":
                df = session.read.orc(path)
            else:
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"Unsupported S3 format '{fmt}' for PySpark read.",
                        plugin_name="rivet_pyspark",
                        plugin_type="adapter",
                        remediation="Supported formats: parquet, csv, json, orc, delta",
                        format=fmt,
                    )
                )
        except (ExecutionError, PluginValidationError):
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"S3 PySpark read failed: {exc}",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Check S3 credentials, bucket name, and network connectivity.",
                    bucket=catalog.options.get("bucket"),
                )
            ) from exc

        df, residual = _apply_pyspark_pushdown(df, pushdown)

        ref = SparkDataFrameMaterializedRef(df)
        material = Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        session = engine.get_session()

        if not _has_hadoop_aws_jars(session):
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    "Hadoop AWS JARs not found on Spark classpath.",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Add hadoop-aws JAR to Spark classpath via "
                        "'spark.jars.packages' option, e.g. 'org.apache.hadoop:hadoop-aws:3.3.4'.",
                )
            )

        _configure_s3a(session, catalog.options)

        table = getattr(joint, "table", None) or joint.name
        path = _build_s3a_path(catalog.options, table)
        fmt = catalog.options.get("format", "parquet")
        strategy = getattr(joint, "write_strategy", None) or "replace"

        arrow_table = material.materialized_ref.to_arrow()
        df = session.createDataFrame(arrow_table.to_pandas())

        try:
            if fmt == "delta":
                mode = _delta_write_mode(strategy)
                df.write.format("delta").mode(mode).save(path)
            elif fmt == "parquet":
                partition_by = getattr(joint, "partition_by", None)
                writer = df.write.mode("append" if strategy == "append" else "overwrite")
                if strategy == "partition" and partition_by:
                    writer = writer.partitionBy(*partition_by)
                writer.parquet(path)
            elif fmt == "csv":
                df.write.mode("append" if strategy == "append" else "overwrite").csv(
                    path, header=True
                )
            elif fmt == "json":
                df.write.mode("append" if strategy == "append" else "overwrite").json(path)
            elif fmt == "orc":
                df.write.mode("append" if strategy == "append" else "overwrite").orc(path)
            else:
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"Unsupported S3 write format '{fmt}' for PySpark.",
                        plugin_name="rivet_pyspark",
                        plugin_type="adapter",
                        remediation="Supported write formats: parquet, csv, json, orc, delta",
                        format=fmt,
                    )
                )
        except (ExecutionError, PluginValidationError):
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"S3 PySpark write failed: {exc}",
                    plugin_name="rivet_pyspark",
                    plugin_type="adapter",
                    remediation="Check S3 credentials, bucket name, and write permissions.",
                    bucket=catalog.options.get("bucket"),
                )
            ) from exc


def _delta_write_mode(strategy: str) -> str:
    """Map Rivet write strategy to Delta/Spark write mode."""
    return {
        "append": "append",
        "replace": "overwrite",
        "merge": "overwrite",
        "incremental_append": "append",
        "scd2": "overwrite",
        "partition": "overwrite",
    }.get(strategy, "overwrite")
