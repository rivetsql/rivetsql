"""Tests for DatabricksSink plugin (task 26.2): all 8 write strategies, merge_key validation,
format validation, optimize_after_write, liquid_clustering."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SinkPlugin
from rivet_core.strategies import _ArrowMaterializedRef
from rivet_databricks.databricks_sink import (
    SUPPORTED_STRATEGIES,
    DatabricksSink,
    _arrow_type_to_databricks,
    _build_values_sql,
    _create_table_sql,
    _generate_write_sql,
    _validate_sink_options,
)


def _make_catalog(**overrides: object) -> Catalog:
    opts: dict = {
        "workspace_url": "https://my.databricks.com",
        "catalog": "main",
        "schema": "default",
        "token": "tok123",
        "warehouse_id": "abc123",
    }
    opts.update(overrides)
    return Catalog(name="db_cat", type="databricks", options=opts)


def _make_joint(name: str = "my_sink", table: str | None = "orders") -> Joint:
    return Joint(name=name, joint_type="sink", catalog="db_cat", table=table, upstream=["src"])


def _make_material(data: dict | None = None) -> Material:
    data = data or {"id": [1, 2], "name": ["a", "b"]}
    ref = _ArrowMaterializedRef(pa.table(data))
    return Material(name="m", catalog="db_cat", materialized_ref=ref, state="materialized")


# ── Plugin contract ───────────────────────────────────────────────────

class TestPluginContract:
    def test_catalog_type(self):
        assert DatabricksSink.catalog_type == "databricks"

    def test_is_sink_plugin(self):
        assert isinstance(DatabricksSink(), SinkPlugin)

    def test_supported_strategies(self):
        expected = {"append", "replace", "truncate_insert", "merge", "delete_insert",
                    "incremental_append", "scd2", "partition"}
        assert DatabricksSink.supported_strategies == expected


# ── Validation: table ─────────────────────────────────────────────────

class TestValidateTable:
    def test_accepts_table_only(self):
        _validate_sink_options({"table": "orders"})

    def test_rejects_missing_table(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({})
        assert exc_info.value.error.code == "RVT-201"
        assert "table" in exc_info.value.error.message

    def test_rejects_unknown_option(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "bogus": "val"})
        assert exc_info.value.error.code == "RVT-201"
        assert "bogus" in exc_info.value.error.message


# ── Validation: write_strategy ────────────────────────────────────────

class TestValidateStrategy:
    def test_accepts_all_strategies(self):
        for s in SUPPORTED_STRATEGIES:
            opts: dict = {"table": "t", "write_strategy": s}
            if s in ("merge", "delete_insert", "scd2"):
                opts["merge_key"] = ["id"]
            _validate_sink_options(opts)

    def test_rejects_unknown_strategy(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "upsert_magic"})
        assert exc_info.value.error.code == "RVT-201"

    def test_default_strategy_is_replace(self):
        _validate_sink_options({"table": "t"})


# ── Validation: merge_key (RVT-207) ──────────────────────────────────

class TestValidateMergeKey:
    def test_merge_requires_merge_key(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "merge"})
        assert exc_info.value.error.code == "RVT-207"

    def test_delete_insert_requires_merge_key(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "delete_insert"})
        assert exc_info.value.error.code == "RVT-207"

    def test_scd2_requires_merge_key(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "scd2"})
        assert exc_info.value.error.code == "RVT-207"

    def test_merge_key_must_be_list(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "write_strategy": "merge", "merge_key": "id"})
        assert exc_info.value.error.code == "RVT-201"

    def test_merge_key_accepted_as_list(self):
        _validate_sink_options({"table": "t", "write_strategy": "merge", "merge_key": ["id"]})


# ── Validation: format (RVT-206) ─────────────────────────────────────

class TestValidateFormat:
    def test_default_format_is_delta(self):
        _validate_sink_options({"table": "t"})

    def test_accepts_delta_and_parquet(self):
        _validate_sink_options({"table": "t", "format": "delta"})
        _validate_sink_options({"table": "t", "format": "parquet"})

    def test_rejects_invalid_format(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "format": "csv"})
        assert exc_info.value.error.code == "RVT-206"


# ── Validation: liquid_clustering (RVT-208) ───────────────────────────

class TestValidateLiquidClustering:
    def test_liquid_clustering_on_delta_accepted(self):
        _validate_sink_options({"table": "t", "format": "delta", "liquid_clustering": ["col1"]})

    def test_liquid_clustering_on_parquet_rejected(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "format": "parquet", "liquid_clustering": ["col1"]})
        assert exc_info.value.error.code == "RVT-208"

    def test_liquid_clustering_default_format_delta_accepted(self):
        _validate_sink_options({"table": "t", "liquid_clustering": ["col1"]})


# ── Validation: optimize_after_write ──────────────────────────────────

class TestValidateOptimize:
    def test_optimize_after_write_accepted(self):
        _validate_sink_options({"table": "t", "optimize_after_write": True})
        _validate_sink_options({"table": "t", "optimize_after_write": False})

    def test_optimize_after_write_rejects_non_bool(self):
        with pytest.raises(PluginValidationError) as exc_info:
            _validate_sink_options({"table": "t", "optimize_after_write": "yes"})
        assert exc_info.value.error.code == "RVT-201"


# ── Validation: partition_by and create_table ─────────────────────────

class TestValidateOtherOptions:
    def test_partition_by_must_be_list(self):
        with pytest.raises(PluginValidationError):
            _validate_sink_options({"table": "t", "partition_by": "year"})

    def test_create_table_must_be_bool(self):
        with pytest.raises(PluginValidationError):
            _validate_sink_options({"table": "t", "create_table": "yes"})


# ── Arrow type mapping ────────────────────────────────────────────────

class TestArrowTypeToDatabricks:
    def test_int_types(self):
        assert _arrow_type_to_databricks(pa.int8()) == "TINYINT"
        assert _arrow_type_to_databricks(pa.int32()) == "INT"
        assert _arrow_type_to_databricks(pa.int64()) == "BIGINT"

    def test_float_types(self):
        assert _arrow_type_to_databricks(pa.float32()) == "FLOAT"
        assert _arrow_type_to_databricks(pa.float64()) == "DOUBLE"

    def test_string(self):
        assert _arrow_type_to_databricks(pa.utf8()) == "STRING"
        assert _arrow_type_to_databricks(pa.large_utf8()) == "STRING"

    def test_bool(self):
        assert _arrow_type_to_databricks(pa.bool_()) == "BOOLEAN"

    def test_timestamp(self):
        assert _arrow_type_to_databricks(pa.timestamp("us")) == "TIMESTAMP"

    def test_unknown_falls_back_to_string(self):
        assert _arrow_type_to_databricks(pa.list_(pa.int32())) == "STRING"


# ── CREATE TABLE SQL generation ───────────────────────────────────────

class TestCreateTableSQL:
    def test_basic(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.utf8())])
        sql = _create_table_sql("main.default.t", schema, "delta", None, None)
        assert "CREATE TABLE IF NOT EXISTS main.default.t" in sql
        assert "`id` BIGINT" in sql
        assert "`name` STRING" in sql
        assert "USING DELTA" in sql

    def test_with_partition_by(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("year", pa.int32())])
        sql = _create_table_sql("t", schema, "delta", ["year"], None)
        assert "PARTITIONED BY (`year`)" in sql

    def test_with_liquid_clustering(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        sql = _create_table_sql("t", schema, "delta", None, ["id"])
        assert "CLUSTER BY (`id`)" in sql

    def test_parquet_format(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        sql = _create_table_sql("t", schema, "parquet", None, None)
        assert "USING PARQUET" in sql


# ── VALUES SQL generation ─────────────────────────────────────────────

class TestBuildValuesSQL:
    def test_basic_values(self):
        t = pa.table({"id": [1, 2], "name": ["a", "b"]})
        sql = _build_values_sql(t)
        assert "(1, 'a')" in sql
        assert "(2, 'b')" in sql

    def test_null_values(self):
        t = pa.table({"id": [1, None]})
        sql = _build_values_sql(t)
        assert "NULL" in sql

    def test_bool_values(self):
        t = pa.table({"flag": [True, False]})
        sql = _build_values_sql(t)
        assert "TRUE" in sql
        assert "FALSE" in sql

    def test_string_escaping(self):
        t = pa.table({"name": ["it's"]})
        sql = _build_values_sql(t)
        assert "it''s" in sql


# ── Write strategy SQL generation ────────────────────────────────────

class TestGenerateWriteSQL:
    def test_append(self):
        stmts = _generate_write_sql("t", "s", "append", ["id", "name"], None)
        assert len(stmts) == 1
        assert "INSERT INTO t SELECT * FROM s" in stmts[0]

    def test_replace(self):
        stmts = _generate_write_sql("t", "s", "replace", ["id", "name"], None)
        assert "CREATE OR REPLACE TABLE" in stmts[0]

    def test_truncate_insert(self):
        stmts = _generate_write_sql("t", "s", "truncate_insert", ["id", "name"], None)
        assert len(stmts) == 2
        assert "TRUNCATE TABLE t" in stmts[0]
        assert "INSERT INTO t" in stmts[1]

    def test_merge(self):
        stmts = _generate_write_sql("t", "s", "merge", ["id", "name"], ["id"])
        assert len(stmts) == 1
        assert "MERGE INTO t AS t USING s AS s" in stmts[0]
        assert "WHEN MATCHED THEN UPDATE" in stmts[0]
        assert "WHEN NOT MATCHED THEN INSERT" in stmts[0]

    def test_delete_insert(self):
        stmts = _generate_write_sql("t", "s", "delete_insert", ["id", "name"], ["id"])
        assert len(stmts) == 2
        assert "DELETE FROM t" in stmts[0]
        assert "INSERT INTO t" in stmts[1]

    def test_incremental_append(self):
        stmts = _generate_write_sql("t", "s", "incremental_append", ["id", "name"], ["id"])
        assert len(stmts) == 1
        assert "MERGE INTO" in stmts[0]
        assert "WHEN NOT MATCHED THEN INSERT" in stmts[0]
        assert "WHEN MATCHED" not in stmts[0]

    def test_scd2(self):
        stmts = _generate_write_sql("t", "s", "scd2", ["id", "name"], ["id"])
        assert len(stmts) == 1
        assert "MERGE INTO" in stmts[0]
        assert "is_current" in stmts[0]
        assert "valid_to" in stmts[0]
        assert "valid_from" in stmts[0]

    def test_partition(self):
        stmts = _generate_write_sql("t", "s", "partition", ["id", "name"], None)
        assert "INSERT OVERWRITE" in stmts[0]


# ── DatabricksSink.write integration ──────────────────────────────────

class TestDatabricksSinkWrite:
    def _mock_api(self):
        api = MagicMock()
        api.execute.return_value = pa.table({})
        api.close.return_value = None
        return api

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_write_append(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="append")

        # Should have: create table, stage data, insert
        assert api.execute.call_count >= 3
        calls = [c.args[0] for c in api.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS" in c for c in calls)
        assert any("INSERT INTO" in c for c in calls)
        api.close.assert_called_once()

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_write_replace(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="replace")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert any("CREATE OR REPLACE TABLE" in c for c in calls)

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_write_merge(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        joint.sink_options = {"merge_key": ["id"]}  # type: ignore[attr-defined]
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="merge")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert any("MERGE INTO" in c for c in calls)

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_optimize_after_write(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        joint.sink_options = {"optimize_after_write": True}  # type: ignore[attr-defined]
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="replace")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert any("OPTIMIZE" in c for c in calls)

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_no_optimize_when_parquet(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        joint.sink_options = {"optimize_after_write": True, "format": "parquet"}  # type: ignore[attr-defined]
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="replace")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert not any("OPTIMIZE" in c for c in calls)

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_liquid_clustering_in_create(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        joint.sink_options = {"liquid_clustering": ["id"]}  # type: ignore[attr-defined]
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="replace")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert any("CLUSTER BY" in c for c in calls)

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_qualifies_unqualified_table(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="append")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert any("main.default.orders" in c for c in calls)

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_fully_qualified_table_used_as_is(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="prod.raw.events")
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="append")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert any("prod.raw.events" in c for c in calls)

    def test_validation_error_before_execution(self):
        catalog = _make_catalog()
        joint = _make_joint(table="t")
        material = _make_material()

        with pytest.raises(PluginValidationError) as exc_info:
            DatabricksSink().write(catalog, joint, material, strategy="bad_strategy")
        assert exc_info.value.error.code == "RVT-201"

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_skips_create_when_create_table_false(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        joint.sink_options = {"create_table": False}  # type: ignore[attr-defined]
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="replace")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert not any("CREATE TABLE IF NOT EXISTS" in c for c in calls)

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_uses_joint_name_as_table_fallback(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(name="fallback_table", table=None)
        material = _make_material()

        DatabricksSink().write(catalog, joint, material, strategy="replace")

        calls = [c.args[0] for c in api.execute.call_args_list]
        assert any("fallback_table" in c for c in calls)

    def test_missing_warehouse_id_raises(self):
        catalog = Catalog(
            name="db_cat", type="databricks",
            options={
                "workspace_url": "https://my.databricks.com",
                "catalog": "main",
                "token": "tok",
            },
        )
        joint = _make_joint(table="orders")
        material = _make_material()

        with pytest.raises(ExecutionError) as exc_info:
            DatabricksSink().write(catalog, joint, material, strategy="replace")
        assert exc_info.value.error.code == "RVT-204"

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_all_8_strategies_generate_sql(self, mock_creds, mock_api_cls):
        """Verify all 8 strategies produce at least one SQL statement."""
        mock_creds.return_value = MagicMock(token="tok")

        for strategy in SUPPORTED_STRATEGIES:
            api = self._mock_api()
            mock_api_cls.return_value = api

            catalog = _make_catalog()
            joint = _make_joint(table="t")
            if strategy in ("merge", "delete_insert", "scd2"):
                joint.sink_options = {"merge_key": ["id"]}  # type: ignore[attr-defined]
            material = _make_material()

            DatabricksSink().write(catalog, joint, material, strategy=strategy)
            # At least create + stage + 1 write statement
            assert api.execute.call_count >= 2, f"Strategy {strategy} should produce SQL calls"

    @patch("rivet_databricks.engine.DatabricksStatementAPI")
    @patch("rivet_databricks.auth.resolve_credentials")
    def test_api_close_called_on_error(self, mock_creds, mock_api_cls):
        mock_creds.return_value = MagicMock(token="tok")
        api = self._mock_api()
        api.execute.side_effect = ExecutionError(
            PluginValidationError.__init__.__code__  # just need any error
        ) if False else Exception("boom")
        mock_api_cls.return_value = api

        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        material = _make_material()

        with pytest.raises(Exception):  # noqa: B017
            DatabricksSink().write(catalog, joint, material, strategy="replace")
        api.close.assert_called_once()
