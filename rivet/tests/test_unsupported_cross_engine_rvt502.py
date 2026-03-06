"""Tests for task 10.6: Unsupported cross-engine paths raise RVT-502.

Validates:
- Databricks consuming from any local engine → RVT-502
- Postgres consuming from any local engine → RVT-502
- Databricks consuming from Postgres (and vice versa) → RVT-502
- Requirements: 10.1, 10.3, 14.2
"""

from __future__ import annotations

from typing import Any

import pyarrow
import pytest

from rivet_core.compiler import CompiledJoint
from rivet_core.errors import ExecutionError
from rivet_core.executor import Executor
from rivet_core.models import ComputeEngine
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import ComputeEnginePlugin, PluginRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_joint(
    name: str,
    joint_type: str = "sql",
    upstream: list[str] | None = None,
    engine: str = "eng",
    **kwargs: Any,
) -> CompiledJoint:
    defaults = dict(
        catalog=None, catalog_type=None, engine=engine,
        engine_resolution="project_default", adapter=None,
        sql=None, sql_translated=None, sql_resolved=None,
        sql_dialect=None, engine_dialect=None,
        upstream=upstream or [], eager=False, table=None,
        write_strategy=None, function=None, source_file=None,
        logical_plan=None, output_schema=None, column_lineage=[],
        optimizations=[], checks=[], fused_group_id=None,
        tags=[], description=None, fusion_strategy_override=None,
        materialization_strategy_override=None,
    )
    defaults.update(kwargs)
    return CompiledJoint(name=name, type=joint_type, **defaults)


def _make_group(
    group_id: str,
    joints: list[str],
    fused_sql: str = "SELECT * FROM upstream",
    engine: str = "eng",
    engine_type: str = "duckdb",
) -> FusedGroup:
    return FusedGroup(
        id=group_id, joints=joints, engine=engine, engine_type=engine_type,
        adapters={j: None for j in joints}, fused_sql=fused_sql,
        entry_joints=[joints[0]], exit_joints=[joints[-1]],
    )


class _RejectingPlugin(ComputeEnginePlugin):
    """Plugin that rejects non-empty input_tables with RVT-502 (like Databricks/Postgres)."""

    def __init__(self, engine_type_name: str, plugin_name: str):
        self.engine_type = engine_type_name
        self._plugin_name = plugin_name
        self.supported_catalog_types: dict[str, list[str]] = {}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self, engine: Any, sql: str, input_tables: dict[str, pyarrow.Table]
    ) -> pyarrow.Table:
        from rivet_core.errors import plugin_error

        if input_tables:
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    f"{self._plugin_name} engine cannot consume local Arrow tables. "
                    f"Input tables: {list(input_tables.keys())}",
                    plugin_name=self._plugin_name,
                    plugin_type="engine",
                    remediation="Ensure all upstream data is accessible natively.",
                )
            )
        return pyarrow.table({})


class _LocalPlugin(ComputeEnginePlugin):
    """Plugin that accepts input_tables (like DuckDB/Arrow/Polars/PySpark)."""

    def __init__(self, engine_type_name: str):
        self.engine_type = engine_type_name
        self.supported_catalog_types: dict[str, list[str]] = {}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self, engine: Any, sql: str, input_tables: dict[str, pyarrow.Table]
    ) -> pyarrow.Table:
        if input_tables:
            return next(iter(input_tables.values()))
        return pyarrow.table({})


def _setup_executor_with_remote_consumer(
    remote_engine_type: str, remote_plugin_name: str
) -> tuple[Executor, PluginRegistry, ComputeEngine]:
    """Set up an executor with a remote engine plugin that rejects input_tables."""
    registry = PluginRegistry()
    plugin = _RejectingPlugin(remote_engine_type, remote_plugin_name)
    registry.register_engine_plugin(plugin)
    engine = plugin.create_engine(f"{remote_engine_type}_eng", {})
    registry.register_compute_engine(engine)
    return Executor(registry=registry), registry, engine


# ---------------------------------------------------------------------------
# Tests: Databricks consuming from local engines
# ---------------------------------------------------------------------------


class TestDatabricksConsumingLocalEngines:
    """Databricks consuming from any local engine raises RVT-502."""

    @pytest.mark.parametrize("local_engine_type", ["duckdb", "arrow", "polars", "pyspark"])
    def test_databricks_rejects_local_arrow_input(self, local_engine_type: str) -> None:
        executor, registry, engine = _setup_executor_with_remote_consumer(
            "databricks", "rivet_databricks"
        )

        upstream_table = pyarrow.table({"col": [1, 2, 3]})
        materials = {"upstream_joint": upstream_table}

        consumer_joint = _make_joint(
            "consumer", "sql", upstream=["upstream_joint"],
            engine="databricks_eng",
        )
        joint_map = {"consumer": consumer_joint}
        group = _make_group(
            "g1", ["consumer"], fused_sql="SELECT * FROM upstream_joint",
            engine="databricks_eng", engine_type="databricks",
        )

        plugin = registry.get_engine_plugin("databricks")
        with pytest.raises(ExecutionError) as exc_info:
            executor._execute_via_plugin(group, materials, joint_map, None, plugin)

        assert exc_info.value.error.code == "RVT-502"
        assert "upstream_joint" in exc_info.value.error.message


# ---------------------------------------------------------------------------
# Tests: Postgres consuming from local engines
# ---------------------------------------------------------------------------


class TestPostgresConsumingLocalEngines:
    """Postgres consuming from any local engine raises RVT-502."""

    @pytest.mark.parametrize("local_engine_type", ["duckdb", "arrow", "polars", "pyspark"])
    def test_postgres_rejects_local_arrow_input(self, local_engine_type: str) -> None:
        executor, registry, engine = _setup_executor_with_remote_consumer(
            "postgres", "rivet_postgres"
        )

        upstream_table = pyarrow.table({"val": [10, 20]})
        materials = {"src": upstream_table}

        consumer_joint = _make_joint(
            "consumer", "sql", upstream=["src"], engine="postgres_eng",
        )
        joint_map = {"consumer": consumer_joint}
        group = _make_group(
            "g1", ["consumer"], fused_sql="SELECT * FROM src",
            engine="postgres_eng", engine_type="postgres",
        )

        plugin = registry.get_engine_plugin("postgres")
        with pytest.raises(ExecutionError) as exc_info:
            executor._execute_via_plugin(group, materials, joint_map, None, plugin)

        assert exc_info.value.error.code == "RVT-502"
        assert "src" in exc_info.value.error.message


# ---------------------------------------------------------------------------
# Tests: Remote-to-remote cross-engine boundaries
# ---------------------------------------------------------------------------


class TestRemoteToRemoteBoundaries:
    """Databricks↔Postgres: no adapter, default arrow_passthrough, RVT-502."""

    def test_databricks_consuming_from_postgres_raises_rvt502(self) -> None:
        """Postgres produces Arrow → Databricks rejects non-empty input_tables."""
        executor, registry, _ = _setup_executor_with_remote_consumer(
            "databricks", "rivet_databricks"
        )

        upstream_table = pyarrow.table({"pg_col": [1]})
        materials = {"pg_joint": upstream_table}

        consumer_joint = _make_joint(
            "db_consumer", "sql", upstream=["pg_joint"], engine="databricks_eng",
        )
        joint_map = {"db_consumer": consumer_joint}
        group = _make_group(
            "g1", ["db_consumer"], fused_sql="SELECT * FROM pg_joint",
            engine="databricks_eng", engine_type="databricks",
        )

        plugin = registry.get_engine_plugin("databricks")
        with pytest.raises(ExecutionError) as exc_info:
            executor._execute_via_plugin(group, materials, joint_map, None, plugin)

        assert exc_info.value.error.code == "RVT-502"

    def test_postgres_consuming_from_databricks_raises_rvt502(self) -> None:
        """Databricks produces Arrow → Postgres rejects non-empty input_tables."""
        executor, registry, _ = _setup_executor_with_remote_consumer(
            "postgres", "rivet_postgres"
        )

        upstream_table = pyarrow.table({"db_col": [42]})
        materials = {"db_joint": upstream_table}

        consumer_joint = _make_joint(
            "pg_consumer", "sql", upstream=["db_joint"], engine="postgres_eng",
        )
        joint_map = {"pg_consumer": consumer_joint}
        group = _make_group(
            "g1", ["pg_consumer"], fused_sql="SELECT * FROM db_joint",
            engine="postgres_eng", engine_type="postgres",
        )

        plugin = registry.get_engine_plugin("postgres")
        with pytest.raises(ExecutionError) as exc_info:
            executor._execute_via_plugin(group, materials, joint_map, None, plugin)

        assert exc_info.value.error.code == "RVT-502"


# ---------------------------------------------------------------------------
# Tests: RVT-502 error contains actionable information
# ---------------------------------------------------------------------------


class TestRvt502ErrorContent:
    """RVT-502 errors include input table names and remediation."""

    def test_error_lists_offending_input_tables(self) -> None:
        executor, registry, _ = _setup_executor_with_remote_consumer(
            "databricks", "rivet_databricks"
        )

        materials = {
            "t1": pyarrow.table({"a": [1]}),
            "t2": pyarrow.table({"b": [2]}),
        }
        consumer_joint = _make_joint(
            "consumer", "sql", upstream=["t1", "t2"], engine="databricks_eng",
        )
        joint_map = {"consumer": consumer_joint}
        group = _make_group(
            "g1", ["consumer"], fused_sql="SELECT * FROM t1 JOIN t2",
            engine="databricks_eng", engine_type="databricks",
        )

        plugin = registry.get_engine_plugin("databricks")
        with pytest.raises(ExecutionError) as exc_info:
            executor._execute_via_plugin(group, materials, joint_map, None, plugin)

        err = exc_info.value.error
        assert err.code == "RVT-502"
        assert "t1" in err.message
        assert "t2" in err.message
        assert err.remediation is not None

    def test_execution_error_not_double_wrapped_in_rvt503(self) -> None:
        """RVT-502 from execute_sql propagates directly, not wrapped in RVT-503."""
        executor, registry, _ = _setup_executor_with_remote_consumer(
            "databricks", "rivet_databricks"
        )

        materials = {"src": pyarrow.table({"x": [1]})}
        consumer_joint = _make_joint(
            "consumer", "sql", upstream=["src"], engine="databricks_eng",
        )
        joint_map = {"consumer": consumer_joint}
        group = _make_group(
            "g1", ["consumer"], fused_sql="SELECT * FROM src",
            engine="databricks_eng", engine_type="databricks",
        )

        plugin = registry.get_engine_plugin("databricks")
        with pytest.raises(ExecutionError) as exc_info:
            executor._execute_via_plugin(group, materials, joint_map, None, plugin)

        # Must be RVT-502, NOT wrapped in RVT-503
        assert exc_info.value.error.code == "RVT-502"
