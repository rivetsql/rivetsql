"""DatabricksUnityAdapter: read/write Unity Catalog tables via Databricks Statement API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.models import Column, Material, Schema
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult, ResidualPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    from rivet_core.optimizer import PushdownPlan
    from rivet_databricks.engine import DatabricksStatementAPI
    from rivet_databricks.unity_source import UnityDeferredMaterializedRef

_REQUIRED_FIELDS = ("workspace_url", "token", "warehouse_id")


def _resolve_credentials(engine: Any) -> tuple[str, str, str]:
    """Extract workspace_url, token, warehouse_id from engine.config.

    Raises ExecutionError(RVT-501) if any required field is missing.
    """
    config = engine.config
    for field in _REQUIRED_FIELDS:
        if not config.get(field):
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Databricks engine config missing '{field}'.",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksUnityAdapter",
                    remediation=f"Add '{field}' to the Databricks engine configuration.",
                )
            )
    return config["workspace_url"], config["token"], config["warehouse_id"]


def _resolve_table_name(joint: Any, catalog: Any) -> str:
    """Resolve fully qualified three-part table name from joint and catalog.

    Uses joint.table if set, otherwise delegates to
    UnityCatalogPlugin.default_table_reference(joint.name, catalog.options).

    Raises ExecutionError(RVT-503) if the resolved name is not three-part qualified.
    """
    table = getattr(joint, "table", None)
    if table:
        name = table
    else:
        from rivet_databricks.unity_catalog import UnityCatalogPlugin

        name = UnityCatalogPlugin.default_table_reference(
            UnityCatalogPlugin(), joint.name, catalog.options
        )
    if len(name.split(".")) != 3:
        raise ExecutionError(
            plugin_error(
                "RVT-503",
                f"Table name '{name}' is not fully qualified.",
                plugin_name="rivet_databricks",
                plugin_type="adapter",
                adapter="DatabricksUnityAdapter",
                remediation="Provide a three-part table name: catalog.schema.table.",
            )
        )
    return name  # type: ignore[no-any-return]


def _build_read_sql(
    table: str,
    deferred_ref: UnityDeferredMaterializedRef,
    pushdown: PushdownPlan | None,
) -> tuple[str, ResidualPlan]:
    """Build SELECT SQL with optional time travel and pushdown clauses.

    Returns (sql, residual) where residual contains operations not pushed down.
    """
    sql = f"SELECT * FROM {table}"

    version = deferred_ref.effective_version
    timestamp = deferred_ref.effective_timestamp

    if version is not None:
        sql += f" VERSION AS OF {version}"
    elif timestamp is not None:
        sql += f" TIMESTAMP AS OF '{timestamp}'"

    if pushdown is None:
        return sql, EMPTY_RESIDUAL

    # Apply projections
    if pushdown.projections.pushed_columns is not None:
        cols = ", ".join(pushdown.projections.pushed_columns)
        sql = sql.replace("SELECT *", f"SELECT {cols}", 1)

    # Apply predicates
    pushed_preds = pushdown.predicates.pushed
    if pushed_preds:
        where = " AND ".join(p.expression for p in pushed_preds)
        sql += f" WHERE {where}"

    # Apply limit
    if pushdown.limit.pushed_limit is not None:
        sql += f" LIMIT {pushdown.limit.pushed_limit}"

    residual = ResidualPlan(
        predicates=list(pushdown.predicates.residual),
        limit=pushdown.limit.residual_limit,
        casts=list(pushdown.casts.residual),
    )
    return sql, residual


class _DatabricksUnityMaterializedRef(MaterializedRef):
    """Deferred MaterializedRef that executes SQL via DatabricksStatementAPI on to_arrow()."""

    def __init__(
        self,
        sql: str,
        api: DatabricksStatementAPI,
        catalog_name: str | None,
        schema_name: str | None,
    ) -> None:
        self._sql = sql
        self._api = api
        self._catalog_name = catalog_name
        self._schema_name = schema_name
        self._table: pyarrow.Table | None = None

    def _materialize(self) -> pyarrow.Table:
        if self._table is None:
            self._table = self._api.execute(
                self._sql, catalog=self._catalog_name, schema=self._schema_name
            )
        return self._table

    def to_arrow(self) -> pyarrow.Table:
        import requests

        try:
            return self._materialize()
        except ExecutionError:
            raise
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    f"Databricks Statement API returned HTTP {status} during read: {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksUnityAdapter",
                    remediation="Check workspace URL and warehouse configuration.",
                )
            ) from exc
        except (requests.ConnectionError, requests.Timeout) as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Databricks SQL Warehouse unreachable: {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksUnityAdapter",
                    remediation="Check network connectivity and warehouse status.",
                )
            ) from exc

    @property
    def schema(self) -> Schema:
        table = self._materialize()
        return Schema(
            columns=[
                Column(name=f.name, type=str(f.type), nullable=f.nullable)
                for f in table.schema
            ]
        )

    @property
    def row_count(self) -> int:
        return self._materialize().num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return self._materialize().nbytes  # type: ignore[no-any-return]

    @property
    def storage_type(self) -> str:
        return "databricks"


class DatabricksUnityAdapter(ComputeEngineAdapter):
    """Adapter bridging Databricks SQL Warehouse engine to Unity Catalog sources."""

    target_engine_type = "databricks"
    catalog_type = "unity"
    capabilities = ["read", "write", "projection_pushdown", "predicate_pushdown", "limit_pushdown"]
    source = "catalog_plugin"
    source_plugin = "rivet_databricks"

    def read_dispatch(
        self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None
    ) -> AdapterPushdownResult:
        from rivet_databricks.engine import DatabricksStatementAPI
        from rivet_databricks.unity_source import UnitySource

        workspace_url, token, warehouse_id = _resolve_credentials(engine)
        table = _resolve_table_name(joint, catalog)

        source_material = UnitySource().read(catalog, joint, None)
        deferred_ref = source_material.materialized_ref
        sql, residual = _build_read_sql(table, deferred_ref, pushdown)  # type: ignore[arg-type]

        api = DatabricksStatementAPI(
            workspace_url=workspace_url,
            token=token,
            warehouse_id=warehouse_id,
            wait_timeout=engine.config.get("wait_timeout", "30s"),
        )

        parts = table.split(".")
        ref = _DatabricksUnityMaterializedRef(
            sql=sql, api=api, catalog_name=parts[0], schema_name=parts[1],
        )
        material = Material(
            name=joint.name, catalog=catalog.name, materialized_ref=ref, state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(
        self, engine: Any, catalog: Any, joint: Any, material: Any
    ) -> Any:
        """Write to Unity Catalog table via Databricks Statement Execution API.

        Delegates SQL generation to DatabricksSink helpers.
        """
        workspace_url, token, warehouse_id = _resolve_credentials(engine)
        table = _resolve_table_name(joint, catalog)

        import requests

        from rivet_databricks.databricks_sink import (
            _arrow_type_to_databricks,
            _build_values_sql,
            _create_table_sql,
            _generate_write_sql,
            _quote,
            _staging_table_name,
        )
        from rivet_databricks.engine import DatabricksStatementAPI

        arrow_table = material.to_arrow()
        columns = [f.name for f in arrow_table.schema]
        strategy = getattr(joint, "write_strategy", None) or "replace"
        fmt = "delta"
        parts = table.split(".")
        catalog_name, schema_name = parts[0], parts[1]

        api = DatabricksStatementAPI(
            workspace_url=workspace_url,
            token=token,
            warehouse_id=warehouse_id,
        )
        try:
            # Create table
            create_sql = _create_table_sql(table, arrow_table.schema, fmt, None, None)
            api.execute(create_sql, catalog=catalog_name, schema=schema_name)

            # Stage data
            staging = _staging_table_name(table)
            if arrow_table.num_rows > 0:
                values_sql = _build_values_sql(arrow_table)
                col_defs = ", ".join(
                    f"{_quote(f.name)} {_arrow_type_to_databricks(f.type)}"
                    for f in arrow_table.schema
                )
                stage_sql = (
                    f"CREATE OR REPLACE TEMPORARY VIEW {staging} ({col_defs})"
                    f" AS SELECT * FROM VALUES {values_sql}"
                )
                api.execute(stage_sql, catalog=catalog_name, schema=schema_name)
            else:
                col_defs = ", ".join(
                    f"CAST(NULL AS {_arrow_type_to_databricks(f.type)}) AS {_quote(f.name)}"
                    for f in arrow_table.schema
                )
                stage_sql = (
                    f"CREATE OR REPLACE TEMPORARY VIEW {staging}"
                    f" AS SELECT {col_defs} WHERE FALSE"
                )
                api.execute(stage_sql, catalog=catalog_name, schema=schema_name)

            # Execute write strategy
            stmts = _generate_write_sql(table, staging, strategy, columns, None)
            for stmt in stmts:
                api.execute(stmt, catalog=catalog_name, schema=schema_name)
        except ExecutionError:
            raise
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    f"Databricks Statement API returned HTTP {status} during write to '{table}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksUnityAdapter",
                    remediation="Check target table and write strategy.",
                )
            ) from exc
        except (requests.ConnectionError, requests.Timeout) as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Databricks SQL Warehouse unreachable: {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksUnityAdapter",
                    remediation="Check network connectivity and warehouse status.",
                )
            ) from exc
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    f"Databricks Statement API error during write to '{table}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksUnityAdapter",
                    remediation="Check target table and write strategy.",
                )
            ) from exc
        finally:
            api.close()

        return material
