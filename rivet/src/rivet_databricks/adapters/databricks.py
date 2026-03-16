"""DatabricksAdapter: read/write tables via Databricks Statement API for 'databricks' catalog type.

Handles both Unity Catalog namespaces and legacy hive_metastore tables.
The 'databricks' catalog type is a general-purpose catalog that can point at
any Databricks-accessible namespace — the Statement API resolves table
references server-side regardless of the underlying metastore.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.models import Column, Material, Schema
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult, ResidualPlan
from rivet_core.plugins import ComputeEngineAdapter, NativeSqlWriteContext
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    from rivet_core.optimizer import PushdownPlan
    from rivet_databricks.engine import DatabricksStatementAPI

_REQUIRED_FIELDS = ("workspace_url", "token", "warehouse_id")


def _resolve_credentials(engine: Any) -> tuple[str, str, str]:
    """Extract workspace_url, token, warehouse_id from engine.config."""
    config = engine.config
    for field in _REQUIRED_FIELDS:
        if not config.get(field):
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Databricks engine config missing '{field}'.",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksAdapter",
                    remediation=f"Add '{field}' to the Databricks engine configuration.",
                )
            )
    return config["workspace_url"], config["token"], config["warehouse_id"]


def _resolve_table_name(joint: Any, catalog: Any) -> str:
    """Resolve fully qualified three-part table name from joint and catalog.

    Uses joint.table if set, otherwise delegates to
    DatabricksCatalogPlugin.default_table_reference(joint.name, catalog.options).

    Raises ExecutionError(RVT-503) if the resolved name is not three-part qualified.
    """
    table = getattr(joint, "table", None)
    if table:
        name = table
    else:
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

        name = DatabricksCatalogPlugin.default_table_reference(
            DatabricksCatalogPlugin(), joint.name, catalog.options
        )
    parts = name.split(".")
    if len(parts) == 2:
        # Two-part name: prepend catalog from options
        db_catalog = catalog.options.get("catalog", "")
        if db_catalog:
            name = f"{db_catalog}.{name}"
    if len(name.split(".")) != 3:
        raise ExecutionError(
            plugin_error(
                "RVT-503",
                f"Table name '{name}' is not fully qualified.",
                plugin_name="rivet_databricks",
                plugin_type="adapter",
                adapter="DatabricksAdapter",
                remediation="Provide a three-part table name: catalog.schema.table.",
            )
        )
    return name  # type: ignore[no-any-return]


def _extract_time_travel_options(joint: Any) -> tuple[int | str | None, bool]:
    """Extract time travel / CDF options from joint source_options.

    Returns (version, change_data_feed).
    """
    source_options: dict[str, Any] = {}
    if hasattr(joint, "source_options") and joint.source_options:
        source_options.update(joint.source_options)
    version: int | str | None = source_options.get("version")
    change_data_feed: bool = source_options.get("change_data_feed", False)
    return version, change_data_feed


def _build_read_sql(
    table: str,
    joint: Any,
    pushdown: PushdownPlan | None,
) -> tuple[str, ResidualPlan]:
    """Build SELECT SQL with optional time travel, CDF, and pushdown clauses.

    Always uses the fully-qualified *table* name (from ``_resolve_table_name``)
    so the Statement API receives a three-part reference.  Time travel and CDF
    syntax is derived from ``joint.source_options`` directly.

    Returns (sql, residual) where residual contains operations not pushed down.
    """
    from rivet_databricks.databricks_source import build_source_sql

    version, change_data_feed = _extract_time_travel_options(joint)
    sql = build_source_sql(table, version=version, change_data_feed=change_data_feed)

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


class _DatabricksMaterializedRef(MaterializedRef):
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
                    adapter="DatabricksAdapter",
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
                    adapter="DatabricksAdapter",
                    remediation="Check network connectivity and warehouse status.",
                )
            ) from exc

    @property
    def schema(self) -> Schema:
        table = self._materialize()
        return Schema(
            columns=[
                Column(name=f.name, type=str(f.type), nullable=f.nullable) for f in table.schema
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


class DatabricksAdapter(ComputeEngineAdapter):
    """Adapter bridging Databricks SQL Warehouse engine to 'databricks' catalog sources.

    Handles both Unity Catalog namespaces and legacy hive_metastore tables.
    Reads and writes are executed via the Databricks Statement Execution API,
    which resolves table references server-side.
    """

    target_engine_type = "databricks"
    catalog_type = "databricks"
    capabilities = ["read", "write", "projection_pushdown", "predicate_pushdown", "limit_pushdown"]
    source = "catalog_plugin"
    source_plugin = "rivet_databricks"

    _NATIVE_WRITE_STRATEGIES = frozenset({"replace", "append", "truncate_insert"})

    def supports_native_sql_write(self, write_strategy: str) -> bool:
        return write_strategy in self._NATIVE_WRITE_STRATEGIES

    def read_dispatch(
        self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None
    ) -> AdapterPushdownResult:
        from rivet_databricks.engine import DatabricksStatementAPI

        workspace_url, token, warehouse_id = _resolve_credentials(engine)
        table = _resolve_table_name(joint, catalog)

        sql, residual = _build_read_sql(table, joint, pushdown)

        api = DatabricksStatementAPI(
            workspace_url=workspace_url,
            token=token,
            warehouse_id=warehouse_id,
            wait_timeout=engine.config.get("wait_timeout", "30s"),
        )

        parts = table.split(".")
        ref = _DatabricksMaterializedRef(
            sql=sql,
            api=api,
            catalog_name=parts[0],
            schema_name=parts[1],
        )
        material = Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        """Write to a Databricks table via Statement Execution API.

        Detects NativeSqlWriteContext for native SQL write (no Arrow round-trip),
        otherwise falls back to Arrow-based write via staging view.
        """
        if isinstance(material, NativeSqlWriteContext):
            return self._native_sql_write(engine, catalog, joint, material)
        return self._arrow_write(engine, catalog, joint, material)

    def _native_sql_write(
        self, engine: Any, catalog: Any, joint: Any, ctx: NativeSqlWriteContext
    ) -> None:
        """Execute fused SQL directly on Databricks via Statement API."""
        import requests

        from rivet_databricks.engine import DatabricksStatementAPI

        workspace_url, token, warehouse_id = _resolve_credentials(engine)
        table = _resolve_table_name(joint, catalog)
        parts = table.split(".")
        catalog_name, schema_name = parts[0], parts[1]
        strategy = ctx.write_strategy
        sql = ctx.fused_sql

        api = DatabricksStatementAPI(
            workspace_url=workspace_url,
            token=token,
            warehouse_id=warehouse_id,
        )
        try:
            if strategy == "replace":
                api.execute(
                    f"CREATE OR REPLACE TABLE {table} AS {sql}",
                    catalog=catalog_name,
                    schema=schema_name,
                )
            elif strategy == "append":
                api.execute(
                    f"INSERT INTO {table} {sql}",
                    catalog=catalog_name,
                    schema=schema_name,
                )
            elif strategy == "truncate_insert":
                api.execute(
                    f"TRUNCATE TABLE {table}",
                    catalog=catalog_name,
                    schema=schema_name,
                )
                api.execute(
                    f"INSERT INTO {table} {sql}",
                    catalog=catalog_name,
                    schema=schema_name,
                )
        except ExecutionError:
            raise
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    f"Databricks Statement API returned HTTP {status} during native write to '{table}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksAdapter",
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
                    adapter="DatabricksAdapter",
                    remediation="Check network connectivity and warehouse status.",
                )
            ) from exc
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    f"Databricks Statement API error during native write to '{table}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksAdapter",
                    remediation="Check target table and write strategy.",
                )
            ) from exc
        finally:
            api.close()

    def _arrow_write(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        """Arrow-based write fallback via staging view."""
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
            create_sql = _create_table_sql(table, arrow_table.schema, fmt, None, None)
            api.execute(create_sql, catalog=catalog_name, schema=schema_name)

            staging = _staging_table_name(table)
            if arrow_table.num_rows > 0:
                values_sql = _build_values_sql(arrow_table)
                col_names = ", ".join(_quote(f.name) for f in arrow_table.schema)
                cast_exprs = ", ".join(
                    f"CAST({_quote(f.name)} AS {_arrow_type_to_databricks(f.type)}) AS {_quote(f.name)}"
                    for f in arrow_table.schema
                )
                stage_sql = (
                    f"CREATE OR REPLACE TEMPORARY VIEW {staging} AS"
                    f" SELECT {cast_exprs} FROM"
                    f" (SELECT * FROM VALUES {values_sql}"
                    f" AS _t({col_names}))"
                )
                api.execute(stage_sql, catalog=catalog_name, schema=schema_name)
            else:
                col_defs = ", ".join(
                    f"CAST(NULL AS {_arrow_type_to_databricks(f.type)}) AS {_quote(f.name)}"
                    for f in arrow_table.schema
                )
                stage_sql = (
                    f"CREATE OR REPLACE TEMPORARY VIEW {staging} AS SELECT {col_defs} WHERE FALSE"
                )
                api.execute(stage_sql, catalog=catalog_name, schema=schema_name)

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
                    adapter="DatabricksAdapter",
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
                    adapter="DatabricksAdapter",
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
                    adapter="DatabricksAdapter",
                    remediation="Check target table and write strategy.",
                )
            ) from exc
        finally:
            api.close()

        return material
