"""Databricks compute engine plugin and Statement Execution API."""

from __future__ import annotations

import io
import json
import logging
import time
from typing import Any

import pyarrow
import pyarrow.ipc
import requests

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin, ReferenceResolver

_logger = logging.getLogger(__name__)

ALL_6_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]

ALL_8_WRITE_STRATEGIES = [
    "append",
    "replace",
    "truncate_insert",
    "merge",
    "delete_insert",
    "incremental_append",
    "scd2",
    "partition",
]

_STATEMENTS_PATH = "/api/2.0/sql/statements"

# Terminal states for statement execution
_TERMINAL_STATES = frozenset({"SUCCEEDED", "FAILED", "CANCELED"})


def _parse_wait_timeout(value: str) -> int:
    """Parse wait_timeout string (e.g. '30s') to seconds."""
    s = str(value).strip().lower()
    if s.endswith("s"):
        s = s[:-1]
    try:
        return int(s)
    except ValueError:
        return 30


class DatabricksReferenceResolver(ReferenceResolver):
    """Rewrite source joint name references to fully-qualified Unity Catalog table names.

    In a fused group, the SQL joint references upstream source joints by name.
    Databricks needs those replaced with catalog.schema.table so the SQL
    can execute server-side against Unity Catalog.
    """

    def resolve_references(
        self,
        sql: str,
        joint: Any,
        catalog: Any,
        compiled_joints: dict[str, Any] | None = None,
        catalog_map: dict[str, Any] | None = None,
        fused_group_joints: list[str] | None = None,
    ) -> str | None:
        if not compiled_joints or not catalog_map:
            return None

        import re

        upstream = getattr(joint, "upstream", [])
        if not upstream:
            return None

        # Only joints that actually contribute SQL to the CTE are true CTE aliases.
        # Source joints without SQL don't produce CTE entries, so they must still
        # be resolved to fully-qualified table names for server-side execution.
        cte_siblings: set[str] = set()
        if fused_group_joints and compiled_joints:
            for jn in fused_group_joints:
                cj = compiled_joints.get(jn)
                if cj and (getattr(cj, "sql", None) or getattr(cj, "sql_translated", None)):
                    cte_siblings.add(jn)

        result = sql
        changed = False
        for up_name in upstream:
            # Skip sources that are CTE siblings — they're referenced by alias, not table name.
            if up_name in cte_siblings:
                continue

            up_cj = compiled_joints.get(up_name)
            if not up_cj or getattr(up_cj, "type", None) != "source":
                continue
            up_catalog_name = getattr(up_cj, "catalog", None)
            if not up_catalog_name:
                continue
            cat = catalog_map.get(up_catalog_name)
            if not cat:
                continue

            opts = getattr(cat, "options", {})
            # Unity catalog uses catalog_name, databricks catalog uses catalog
            db_catalog = opts.get("catalog_name") or opts.get("catalog")
            if not db_catalog:
                continue
            db_schema = opts.get("schema", "default")

            table = getattr(up_cj, "table", None)
            if table:
                parts = table.split(".")
                if len(parts) == 3:
                    fq_name = table
                elif len(parts) == 2:
                    fq_name = f"{db_catalog}.{table}"
                else:
                    fq_name = f"{db_catalog}.{db_schema}.{table}"
            else:
                fq_name = f"{db_catalog}.{db_schema}.{up_name}"

            # Replace the source joint name with the fully-qualified table name
            # Use word boundary matching to avoid partial replacements
            pattern = re.compile(r"\b" + re.escape(up_name) + r"\b")
            new_result = pattern.sub(fq_name, result)
            if new_result != result:
                result = new_result
                changed = True

        return result if changed else None


class DatabricksStatementAPI:
    """Client for the Databricks SQL Statement Execution API.

    Handles: submit → poll → fetch results (Arrow IPC or JSONL).
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        warehouse_id: str,
        *,
        wait_timeout: str = "30s",
        max_rows_per_chunk: int = 100_000,
        disposition: str = "EXTERNAL_LINKS",
    ) -> None:
        self._base_url = workspace_url.rstrip("/")
        self._warehouse_id = warehouse_id
        self._wait_timeout_s = _parse_wait_timeout(wait_timeout)
        self._max_rows_per_chunk = max_rows_per_chunk
        self._disposition = disposition
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "rivet-databricks/0.1",
        })

    def execute(self, sql: str, *, catalog: str | None = None, schema: str | None = None) -> pyarrow.Table:
        """Submit SQL, poll to completion, and return results as a PyArrow Table."""
        try:
            statement_id = self._submit(sql, catalog=catalog, schema=schema)
            result = self._poll(statement_id)
            return self._fetch_result(result, statement_id)
        except ExecutionError:
            raise
        except requests.ConnectionError as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Cannot connect to Databricks workspace '{self._base_url}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="engine",
                    remediation="Check network connectivity and that the workspace URL is correct.",
                    workspace_url=self._base_url,
                )
            ) from exc

    def close(self) -> None:
        self._session.close()

    # ── Submit ────────────────────────────────────────────────────────

    def _submit(self, sql: str, *, catalog: str | None = None, schema: str | None = None) -> str:
        """POST /api/2.0/sql/statements → returns statement_id."""
        body: dict[str, Any] = {
            "warehouse_id": self._warehouse_id,
            "statement": sql,
            "wait_timeout": f"{self._wait_timeout_s}s",
            "disposition": self._disposition,
            "format": "ARROW_STREAM",
        }
        if catalog:
            body["catalog"] = catalog
        if schema:
            body["schema"] = schema

        url = f"{self._base_url}{_STATEMENTS_PATH}"
        resp = self._session.post(url, json=body)
        self._check_http_error(resp, sql=sql)
        data = resp.json()

        statement_id = data.get("statement_id")
        if not statement_id:
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    "Statement Execution API returned no statement_id.",
                    plugin_name="rivet_databricks",
                    plugin_type="engine",
                    remediation="Check warehouse_id and workspace_url configuration.",
                    sql=sql[:200],
                )
            )

        state = data.get("status", {}).get("state", "PENDING")
        if state in _TERMINAL_STATES:
            return statement_id  # type: ignore[no-any-return]

        return statement_id  # type: ignore[no-any-return]

    # ── Poll ──────────────────────────────────────────────────────────

    def _poll(self, statement_id: str) -> dict[str, Any]:
        """Poll GET /api/2.0/sql/statements/{id} until terminal state."""
        url = f"{self._base_url}{_STATEMENTS_PATH}/{statement_id}"
        poll_interval = 0.5

        while True:
            resp = self._session.get(url)
            self._check_http_error(resp, statement_id=statement_id)
            data = resp.json()
            state = data.get("status", {}).get("state", "PENDING")

            if state == "SUCCEEDED":
                return data  # type: ignore[no-any-return]
            if state == "FAILED":
                error_msg = data.get("status", {}).get("error", {}).get("message", "Unknown error")
                raise ExecutionError(
                    plugin_error(
                        "RVT-502",
                        f"Statement {statement_id} FAILED: {error_msg}",
                        plugin_name="rivet_databricks",
                        plugin_type="engine",
                        remediation="Check the SQL statement and warehouse status in the Databricks UI.",
                        statement_id=statement_id,
                        state="FAILED",
                        workspace_url=self._base_url,
                    )
                )
            if state == "CANCELED":
                raise ExecutionError(
                    plugin_error(
                        "RVT-502",
                        f"Statement {statement_id} was CANCELED.",
                        plugin_name="rivet_databricks",
                        plugin_type="engine",
                        remediation="Re-submit the statement. Check for cancellation policies on the warehouse.",
                        statement_id=statement_id,
                        state="CANCELED",
                        workspace_url=self._base_url,
                    )
                )

            # PENDING or RUNNING — wait and retry
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 5.0)

    # ── Fetch results ─────────────────────────────────────────────────

    def _fetch_result(self, data: dict[str, Any], statement_id: str) -> pyarrow.Table:
        """Extract result from a SUCCEEDED statement response."""
        manifest = data.get("manifest", {})
        result_format = manifest.get("format", "")
        result_data = data.get("result", {})

        # EXTERNAL_LINKS with ARROW_STREAM — preferred path
        if result_format == "ARROW_STREAM" and "external_links" in result_data:
            return self._fetch_arrow_chunks(result_data["external_links"], statement_id)

        # INLINE with JSON_ARRAY — fallback
        if "data_array" in result_data:
            return self._parse_jsonl_result(result_data, manifest)

        # No result data (DDL/DML statements)
        return pyarrow.table({})

    def _fetch_arrow_chunks(self, links: list[dict[str, Any]], statement_id: str) -> pyarrow.Table:
        """Fetch Arrow IPC stream chunks from pre-signed external URLs."""
        tables: list[pyarrow.Table] = []
        for link in links:
            url = link.get("external_link", "")
            if not url:
                continue
            resp = requests.get(url)
            if resp.status_code != 200:
                raise ExecutionError(
                    plugin_error(
                        "RVT-502",
                        f"Failed to fetch Arrow chunk for statement {statement_id} (HTTP {resp.status_code}).",
                        plugin_name="rivet_databricks",
                        plugin_type="engine",
                        remediation="Check that the pre-signed URL has not expired. Re-submit the statement.",
                        statement_id=statement_id,
                        chunk_url=url[:100],
                    )
                )
            reader = pyarrow.ipc.open_stream(io.BytesIO(resp.content))
            tables.append(reader.read_all())

            # Follow next_chunk_internal_link pagination
            next_link = link.get("next_chunk_internal_link")
            if next_link:
                tables.extend(self._fetch_next_chunks(next_link, statement_id))

        if not tables:
            return pyarrow.table({})
        return pyarrow.concat_tables(tables)

    def _fetch_next_chunks(self, path: str, statement_id: str) -> list[pyarrow.Table]:
        """Follow internal pagination links for additional chunks."""
        tables: list[pyarrow.Table] = []
        url = f"{self._base_url}{path}"
        while url:
            resp = self._session.get(url)
            self._check_http_error(resp, statement_id=statement_id)
            data = resp.json()
            ext_links = data.get("external_links", [])
            for link in ext_links:
                ext_url = link.get("external_link", "")
                if not ext_url:
                    continue
                chunk_resp = requests.get(ext_url)
                if chunk_resp.status_code == 200:
                    reader = pyarrow.ipc.open_stream(io.BytesIO(chunk_resp.content))
                    tables.append(reader.read_all())
            # Check for further pagination
            next_link = data.get("next_chunk_internal_link")
            url = f"{self._base_url}{next_link}" if next_link else ""
        return tables

    def _parse_jsonl_result(self, result_data: dict[str, Any], manifest: dict[str, Any]) -> pyarrow.Table:
        """Parse JSONL/JSON_ARRAY inline result into a PyArrow Table."""
        columns_meta = manifest.get("schema", {}).get("columns", [])
        col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns_meta)]
        rows = result_data.get("data_array", [])

        if not rows or not col_names:
            return pyarrow.table({})

        # Build column arrays from row-major data
        col_data: dict[str, list[Any]] = {name: [] for name in col_names}
        for row in rows:
            for i, name in enumerate(col_names):
                col_data[name].append(row[i] if i < len(row) else None)

        return pyarrow.table(col_data)

    # ── Error handling ────────────────────────────────────────────────

    def _check_http_error(
        self,
        resp: requests.Response,
        *,
        sql: str | None = None,
        statement_id: str | None = None,
    ) -> None:
        """Raise ExecutionError for non-2xx HTTP responses."""
        if resp.ok:
            return
        status = resp.status_code
        try:
            body = resp.json()
            msg = body.get("message", body.get("error", resp.text[:200]))
        except (json.JSONDecodeError, ValueError):
            msg = resp.text[:200]

        ctx: dict[str, Any] = {"status": status}
        if statement_id:
            ctx["statement_id"] = statement_id
        if sql:
            ctx["sql"] = sql[:200]

        if status == 401:
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    "Authentication failed (HTTP 401) for Databricks SQL Warehouse.",
                    plugin_name="rivet_databricks",
                    plugin_type="engine",
                    remediation="Check your Databricks token. Ensure it has SQL Warehouse access.",
                    workspace_url=self._base_url,
                    **ctx,
                )
            )
        if status == 403:
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    "Authorization denied (HTTP 403) for Databricks SQL Warehouse.",
                    plugin_name="rivet_databricks",
                    plugin_type="engine",
                    remediation="Check that your credentials have access to the SQL Warehouse.",
                    workspace_url=self._base_url,
                    **ctx,
                )
            )

        raise ExecutionError(
            plugin_error(
                "RVT-502",
                f"Databricks Statement API error (HTTP {status}): {msg}",
                plugin_name="rivet_databricks",
                plugin_type="engine",
                remediation="Check the Databricks workspace URL and warehouse configuration.",
                workspace_url=self._base_url,
                **ctx,
            )
        )


class DatabricksComputeEnginePlugin(ComputeEnginePlugin):
    engine_type = "databricks"
    dialect = "databricks"
    supported_catalog_types: dict[str, list[str]] = {
        "databricks": ALL_6_CAPABILITIES,
        "unity": ALL_6_CAPABILITIES,
    }
    supported_write_strategies: dict[str, list[str]] = {
        "databricks": ALL_8_WRITE_STRATEGIES,
        "unity": ALL_8_WRITE_STRATEGIES,
    }
    required_options: list[str] = ["warehouse_id", "workspace_url", "token"]
    optional_options: dict[str, Any] = {
        "wait_timeout": "30s",
        "max_rows_per_chunk": 100000,
        "disposition": "EXTERNAL_LINKS",
    }
    credential_options: list[str] = []

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        engine = ComputeEngine(name=name, engine_type="databricks")
        engine.config = dict(config)
        return engine

    def get_reference_resolver(self) -> ReferenceResolver | None:
        return DatabricksReferenceResolver()

    def validate(self, options: dict[str, Any]) -> None:
        recognized = set(self.optional_options) | set(self.required_options)
        for key in options:
            if key not in recognized:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for databricks engine.",
                        plugin_name="rivet_databricks",
                        plugin_type="engine",
                        remediation=f"Valid options: {', '.join(sorted(recognized))}",
                        option=key,
                    )
                )
        for key in self.required_options:
            if key not in options:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Missing required option '{key}' for databricks engine.",
                        plugin_name="rivet_databricks",
                        plugin_type="engine",
                        remediation=f"Provide '{key}' in the engine options.",
                        missing_option=key,
                    )
                )

    def create_statement_api(
        self,
        workspace_url: str,
        token: str,
        config: dict[str, Any],
    ) -> DatabricksStatementAPI:
        """Create a DatabricksStatementAPI from engine config and catalog credentials."""
        return DatabricksStatementAPI(
            workspace_url=workspace_url,
            token=token,
            warehouse_id=config["warehouse_id"],
            wait_timeout=config.get("wait_timeout", "30s"),
            max_rows_per_chunk=config.get("max_rows_per_chunk", 100_000),
            disposition=config.get("disposition", "EXTERNAL_LINKS"),
        )

    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        """Execute SQL on Databricks via Statement API.

        input_tables are ignored — Databricks resolves all table references
        server-side against Unity Catalog. Source joints in a fused group are
        already embedded as fully-qualified table names in the SQL.
        """
        api = self.create_statement_api(
            engine.config["workspace_url"],
            engine.config["token"],
            engine.config,
        )
        try:
            return api.execute(
                sql,
                catalog=engine.config.get("catalog"),
                schema=engine.config.get("schema"),
            )
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"Databricks SQL execution failed: {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="engine",
                    remediation="Check Databricks connectivity, warehouse status, and SQL syntax.",
                    sql=sql[:200],
                    warehouse_id=engine.config.get("warehouse_id", "unknown"),
                )
            ) from exc

    def collect_metrics(self, execution_context: Any) -> Any:
        """Return PluginMetrics with standard categories + Databricks-specific extensions."""
        from rivet_core.metrics import (
            IOMetrics,
            MemoryMetrics,
            PluginMetrics,
            QueryPlanningMetrics,
            ScanMetrics,
        )

        ctx = execution_context if isinstance(execution_context, dict) else {}
        return PluginMetrics(
            well_known={
                "query_planning": QueryPlanningMetrics(
                    planning_time_ms=ctx.get("planning_time_ms"),
                    actual_rows=ctx.get("rows_out"),
                ),
                "io": IOMetrics(
                    bytes_read=ctx.get("bytes_read"),
                    bytes_written=ctx.get("bytes_written"),
                ),
                "memory": MemoryMetrics(
                    peak_bytes=ctx.get("peak_bytes"),
                ),
                "scan": ScanMetrics(
                    rows_scanned=ctx.get("rows_scanned"),
                ),
            },
            extensions={
                "databricks.warehouse_id": ctx.get("warehouse_id"),
                "databricks.statement_id": ctx.get("statement_id"),
                "databricks.query_profile_link": ctx.get("query_profile_link"),
                "databricks.photon_accelerated": ctx.get("photon_accelerated"),
                "databricks.total_chunks_fetched": ctx.get("total_chunks_fetched"),
            },
            engine="databricks",
        )
