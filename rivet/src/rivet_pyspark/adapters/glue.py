"""GluePySparkAdapter: AWSGlueDataCatalogHiveClientFactory before session creation.

Configures Spark to use AWS Glue Data Catalog as Hive metastore via
AWSGlueDataCatalogHiveClientFactory. Glue metastore properties must be
set before SparkSession creation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rivet_core.credentials import CredentialResolver

from rivet_core.errors import ExecutionError, PluginValidationError, RivetError, plugin_error
from rivet_core.models import Material
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_pyspark.adapters.pushdown import _apply_pyspark_pushdown
from rivet_pyspark.engine import ALL_6_CAPABILITIES, SparkDataFrameMaterializedRef

GLUE_METASTORE_FACTORY = (
    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
)
_GLUE_METASTORE_MAVEN = "com.amazonaws:aws-glue-datacatalog-spark-client:3.4.0"

CAPABILITIES = [
    *ALL_6_CAPABILITIES,
    "write_append",
    "write_replace",
    "write_partition",
    "write_delete_insert",
]


def _make_resolver(catalog_options: dict[str, Any]) -> CredentialResolver:
    """Create a CredentialResolver from the factory injected by the catalog plugin."""
    factory = catalog_options.get("_credential_resolver_factory")
    if factory is None:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                "No credential resolver factory in catalog options.",
                plugin_name="rivet_pyspark",
                plugin_type="adapter",
                adapter="GluePySparkAdapter",
                remediation="Ensure the Glue catalog plugin is registered.",
            )
        )
    region = catalog_options.get("region", "us-east-1")
    return factory(catalog_options, region)  # type: ignore[no-any-return]


def _has_glue_metastore_jar(session: Any) -> bool:
    """Check if the AWS Glue Data Catalog metastore factory JAR is on the Spark classpath."""
    try:
        jvm = session._jvm
        jvm.java.lang.Class.forName(GLUE_METASTORE_FACTORY)
        return True
    except Exception:
        return False


def _glue_spark_config(catalog_options: dict[str, Any]) -> dict[str, str]:
    """Build Spark config dict for Glue metastore integration."""
    region = catalog_options.get("region", "us-east-1")
    conf: dict[str, str] = {
        "spark.sql.catalogImplementation": "hive",
        "hive.metastore.client.factory.class": GLUE_METASTORE_FACTORY,
        "spark.hadoop.aws.glue.catalog.region": region,
    }
    catalog_id = catalog_options.get("catalog_id")
    if catalog_id:
        conf["spark.hadoop.aws.glue.catalog.catalogId"] = catalog_id

    # Set AWS credentials as Hadoop properties
    creds = _make_resolver(catalog_options).resolve()
    conf["spark.hadoop.fs.s3a.access.key"] = creds.access_key_id
    conf["spark.hadoop.fs.s3a.secret.key"] = creds.secret_access_key
    if creds.session_token:
        conf["spark.hadoop.fs.s3a.session.token"] = creds.session_token
        conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        )

    return conf


def _ensure_glue_config(engine: Any, catalog_options: dict[str, Any]) -> None:
    """Inject Glue metastore config into engine before session creation.

    If the session already exists, sets Hadoop config on the running session.
    If not, injects into engine._config so the session builder picks it up.
    """
    glue_conf = _glue_spark_config(catalog_options)

    if engine._session is not None:
        # Session already created — set Hadoop config directly
        hadoop_conf = engine._session.sparkContext._jsc.hadoopConfiguration()
        for k, v in glue_conf.items():
            if k.startswith("spark.hadoop."):
                hadoop_conf.set(k[len("spark.hadoop."):], v)
            else:
                engine._session.conf.set(k, v)
    else:
        # Session not yet created — inject into engine config
        engine._config.setdefault("config", {}).update(glue_conf)


class GluePySparkAdapter(ComputeEngineAdapter):
    """PySpark adapter for Glue catalog type.

    Configures Spark to use AWS Glue Data Catalog as Hive metastore via
    AWSGlueDataCatalogHiveClientFactory before SparkSession creation.
    Reads/writes via Spark SQL against Glue-registered tables.
    """

    target_engine_type = "pyspark"
    catalog_type = "glue"
    capabilities = list(CAPABILITIES)
    source = "engine_plugin"
    source_plugin = "rivet_pyspark"

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        catalog_options = catalog.options if hasattr(catalog, "options") else {}

        _ensure_glue_config(engine, catalog_options)

        session = engine.get_session()

        if not _has_glue_metastore_jar(session):
            raise PluginValidationError(
                RivetError(
                    code="RVT-202",
                    message="AWS Glue Data Catalog metastore factory JAR not found on Spark classpath.",
                    context={"adapter": "GluePySparkAdapter", "class": GLUE_METASTORE_FACTORY},
                    remediation=(
                        f"Add the AWS Glue Data Catalog Spark client JAR to Spark via "
                        f"'spark.jars.packages' option: '{_GLUE_METASTORE_MAVEN}'."
                    ),
                )
            )

        database = catalog_options.get("database", "default")
        table = getattr(joint, "table", None) or joint.name
        qualified = f"{database}.{table}"

        try:
            df = session.sql(f"SELECT * FROM {qualified}")
        except Exception as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"GluePySparkAdapter read failed for '{qualified}': {exc}",
                    context={"table": qualified, "database": database},
                    remediation=(
                        "Check that the Glue table exists, AWS credentials are valid, "
                        "and the AWSGlueDataCatalogHiveClientFactory JAR is on the classpath."
                    ),
                )
            ) from exc

        df, residual = _apply_pyspark_pushdown(df, pushdown)

        ref = SparkDataFrameMaterializedRef(df)
        material = Material(
            name=joint.name,
            catalog=getattr(catalog, "name", "glue"),
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        catalog_options = catalog.options if hasattr(catalog, "options") else {}

        _ensure_glue_config(engine, catalog_options)

        session = engine.get_session()

        if not _has_glue_metastore_jar(session):
            raise PluginValidationError(
                RivetError(
                    code="RVT-202",
                    message="AWS Glue Data Catalog metastore factory JAR not found on Spark classpath.",
                    context={"adapter": "GluePySparkAdapter", "class": GLUE_METASTORE_FACTORY},
                    remediation=(
                        f"Add the AWS Glue Data Catalog Spark client JAR to Spark via "
                        f"'spark.jars.packages' option: '{_GLUE_METASTORE_MAVEN}'."
                    ),
                )
            )

        database = catalog_options.get("database", "default")
        table = getattr(joint, "table", None) or joint.name
        qualified = f"{database}.{table}"
        strategy = getattr(joint, "write_strategy", None) or "replace"

        arrow_table = material.materialized_ref.to_arrow()
        df = session.createDataFrame(arrow_table.to_pandas())

        mode = "append" if strategy in ("append", "delete_insert") else "overwrite"

        try:
            df.write.mode(mode).saveAsTable(qualified)
        except Exception as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"GluePySparkAdapter write failed for '{qualified}': {exc}",
                    context={"table": qualified, "strategy": strategy},
                    remediation=(
                        "Check that AWS credentials have write permissions, "
                        "and the AWSGlueDataCatalogHiveClientFactory JAR is on the classpath."
                    ),
                )
            ) from exc
