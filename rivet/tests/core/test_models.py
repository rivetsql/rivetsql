"""Unit tests for Catalog, ComputeEngine, Column, Schema, Joint, Material models (tasks 3.1, 3.2, 3.3)."""

from __future__ import annotations

from typing import Any

import pytest

from rivet_core.checks import Assertion
from rivet_core.models import Catalog, Column, ComputeEngine, Joint, Schema


class TestCatalog:
    def test_basic_construction(self) -> None:
        cat = Catalog(name="my_catalog", type="arrow")
        assert cat.name == "my_catalog"
        assert cat.type == "arrow"
        assert cat.options == {}

    def test_options_default_empty_dict(self) -> None:
        cat = Catalog(name="c", type="filesystem")
        assert cat.options == {}

    def test_options_provided(self) -> None:
        cat = Catalog(name="c", type="filesystem", options={"path": "/data"})
        assert cat.options == {"path": "/data"}

    def test_immutable(self) -> None:
        cat = Catalog(name="c", type="arrow")
        with pytest.raises((AttributeError, TypeError)):
            cat.name = "other"  # type: ignore[misc]

    def test_frozen_options_not_shared(self) -> None:
        # Two catalogs with default options should not share the same dict object
        cat1 = Catalog(name="c1", type="arrow")
        cat2 = Catalog(name="c2", type="arrow")
        assert cat1.options is not cat2.options

    def test_equality(self) -> None:
        cat1 = Catalog(name="c", type="arrow", options={"k": "v"})
        cat2 = Catalog(name="c", type="arrow", options={"k": "v"})
        assert cat1 == cat2

    def test_inequality(self) -> None:
        cat1 = Catalog(name="c1", type="arrow")
        cat2 = Catalog(name="c2", type="arrow")
        assert cat1 != cat2


class TestComputeEngine:
    def test_basic_construction(self) -> None:
        engine = ComputeEngine(name="duckdb-memory", engine_type="duckdb")
        assert engine.name == "duckdb-memory"
        assert engine.engine_type == "duckdb"

    def test_single_instance_shorthand(self) -> None:
        # name may match engine_type for single-instance shorthand
        engine = ComputeEngine(name="arrow", engine_type="arrow")
        assert engine.name == engine.engine_type

    def test_mutable(self) -> None:
        # ComputeEngine is a regular dataclass (not frozen)
        engine = ComputeEngine(name="e", engine_type="duckdb")
        engine.name = "e2"
        assert engine.name == "e2"


class TestColumn:
    def test_basic_construction(self) -> None:
        col = Column(name="id", type="int64", nullable=False)
        assert col.name == "id"
        assert col.type == "int64"
        assert col.nullable is False

    def test_nullable_true(self) -> None:
        col = Column(name="value", type="utf8", nullable=True)
        assert col.nullable is True

    def test_immutable(self) -> None:
        col = Column(name="id", type="int64", nullable=False)
        with pytest.raises((AttributeError, TypeError)):
            col.name = "other"  # type: ignore[misc]

    def test_equality(self) -> None:
        assert Column("x", "float64", True) == Column("x", "float64", True)
        assert Column("x", "float64", True) != Column("x", "float64", False)


class TestSchema:
    def test_basic_construction(self) -> None:
        cols = [Column("id", "int64", False), Column("name", "utf8", True)]
        schema = Schema(columns=cols)
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "id"

    def test_empty_schema(self) -> None:
        schema = Schema(columns=[])
        assert schema.columns == []

    def test_immutable(self) -> None:
        schema = Schema(columns=[])
        with pytest.raises((AttributeError, TypeError)):
            schema.columns = []  # type: ignore[misc]

    def test_equality(self) -> None:
        cols = [Column("id", "int64", False)]
        assert Schema(columns=cols) == Schema(columns=cols)


class TestJoint:
    def test_source_joint(self) -> None:
        j = Joint(name="raw", joint_type="source", catalog="my_catalog")
        assert j.name == "raw"
        assert j.joint_type == "source"
        assert j.catalog == "my_catalog"
        assert j.upstream == []
        assert j.tags == []
        assert j.description is None
        assert j.assertions == []
        assert j.path is None

    def test_sql_joint(self) -> None:
        j = Joint(name="transformed", joint_type="sql", upstream=["raw"])
        assert j.joint_type == "sql"
        assert j.upstream == ["raw"]

    def test_sink_joint(self) -> None:
        j = Joint(name="output", joint_type="sink", catalog="dest", upstream=["transformed"])
        assert j.joint_type == "sink"
        assert j.upstream == ["transformed"]

    def test_python_joint(self) -> None:
        j = Joint(name="py_step", joint_type="python", upstream=["raw"])
        assert j.joint_type == "python"

    def test_invalid_joint_type(self) -> None:
        with pytest.raises(ValueError, match="Invalid joint_type"):
            Joint(name="bad", joint_type="unknown")

    def test_tags(self) -> None:
        j = Joint(name="j", joint_type="sql", tags=["daily", "finance"])
        assert j.tags == ["daily", "finance"]

    def test_description(self) -> None:
        j = Joint(name="j", joint_type="source", description="Raw events table")
        assert j.description == "Raw events table"

    def test_path(self) -> None:
        j = Joint(name="j", joint_type="source", catalog="fs", path="/data/events.parquet")
        assert j.path == "/data/events.parquet"

    def test_assertions_field(self) -> None:
        a = Assertion(type="not_null", config={"column": "id"})
        j = Joint(name="j", joint_type="sql", assertions=[a])
        assert len(j.assertions) == 1
        assert j.assertions[0].type == "not_null"

    def test_defaults_not_shared(self) -> None:
        j1 = Joint(name="j1", joint_type="source")
        j2 = Joint(name="j2", joint_type="source")
        j1.upstream.append("x")
        assert j2.upstream == []

    def test_mutable(self) -> None:
        # Joint is a regular dataclass (not frozen) — metadata can be updated
        j = Joint(name="j", joint_type="source")
        j.description = "updated"
        assert j.description == "updated"

    def test_all_valid_types(self) -> None:
        for jt in ("source", "sql", "sink", "python"):
            j = Joint(name=f"j_{jt}", joint_type=jt)
            assert j.joint_type == jt


class TestMaterial:
    def _make_ref(self) -> Any:
        """Create a materialized ref backed by a simple Arrow table."""
        import pyarrow as pa

        from rivet_core.strategies import ArrowMaterialization, MaterializationContext

        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        ctx = MaterializationContext(joint_name="test", strategy_name="arrow", options={})
        return ArrowMaterialization().materialize(table, ctx), table

    def test_deferred_defaults(self) -> None:
        from rivet_core.models import Material

        m = Material(name="m", catalog="c")
        assert m.state == "deferred"
        assert m.materialized_ref is None
        assert m.table is None
        assert m.schema is None

    def test_to_arrow_materialized(self) -> None:
        from rivet_core.models import Material

        ref, table = self._make_ref()
        m = Material(name="m", catalog="c", state="materialized", materialized_ref=ref)
        result = m.to_arrow()
        assert result.equals(table)

    def test_to_arrow_evicted_raises(self) -> None:
        from rivet_core.errors import ExecutionError
        from rivet_core.models import Material

        m = Material(name="m", catalog="c", state="evicted")
        with pytest.raises(ExecutionError, match="evicted"):
            m.to_arrow()

    def test_to_arrow_deferred_no_ref_raises(self) -> None:
        from rivet_core.errors import ExecutionError
        from rivet_core.models import Material

        m = Material(name="m", catalog="c", state="deferred")
        with pytest.raises(ExecutionError, match="deferred"):
            m.to_arrow()

    def test_columns_from_schema(self) -> None:
        from rivet_core.models import Material

        m = Material(name="m", catalog="c", schema={"id": "int64", "name": "utf8"})
        assert m.columns == ["id", "name"]

    def test_columns_from_ref(self) -> None:
        from rivet_core.models import Material

        ref, _ = self._make_ref()
        m = Material(name="m", catalog="c", state="materialized", materialized_ref=ref)
        assert m.columns == ["id", "name"]

    def test_columns_empty_when_deferred(self) -> None:
        from rivet_core.models import Material

        m = Material(name="m", catalog="c")
        assert m.columns == []

    def test_num_rows(self) -> None:
        from rivet_core.models import Material

        ref, _ = self._make_ref()
        m = Material(name="m", catalog="c", state="materialized", materialized_ref=ref)
        assert m.num_rows == 3

    def test_to_pandas_import_error(self) -> None:
        """to_pandas raises ImportError if pandas not installed (mocked)."""
        import builtins
        from unittest.mock import patch

        from rivet_core.models import Material

        ref, _ = self._make_ref()
        m = Material(name="m", catalog="c", state="materialized", materialized_ref=ref)

        original_import = builtins.__import__

        def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "pandas":
                raise ImportError("no pandas")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="pandas"):
                m.to_pandas()

    def test_to_polars_import_error(self) -> None:
        import builtins
        from unittest.mock import patch

        from rivet_core.models import Material

        ref, _ = self._make_ref()
        m = Material(name="m", catalog="c", state="materialized", materialized_ref=ref)

        original_import = builtins.__import__

        def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "polars":
                raise ImportError("no polars")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="polars"):
                m.to_polars()

    def test_to_duckdb_import_error(self) -> None:
        import builtins
        from unittest.mock import patch

        from rivet_core.models import Material

        ref, _ = self._make_ref()
        m = Material(name="m", catalog="c", state="materialized", materialized_ref=ref)

        original_import = builtins.__import__

        def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "duckdb":
                raise ImportError("no duckdb")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="duckdb"):
                m.to_duckdb()

    def test_to_spark_import_error(self) -> None:
        import builtins
        from unittest.mock import patch

        from rivet_core.models import Material

        ref, _ = self._make_ref()
        m = Material(name="m", catalog="c", state="materialized", materialized_ref=ref)

        original_import = builtins.__import__

        def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "pyspark.sql":
                raise ImportError("no pyspark")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="pyspark"):
                m.to_spark()

    def test_state_transitions(self) -> None:
        """Material state can be updated (not frozen)."""
        from rivet_core.models import Material

        ref, _ = self._make_ref()
        m = Material(name="m", catalog="c", state="deferred")
        m.state = "materialized"
        m.materialized_ref = ref
        assert m.to_arrow().num_rows == 3

        m.state = "retained"
        assert m.to_arrow().num_rows == 3

        m.state = "evicted"
        m.materialized_ref = None
        with pytest.raises(Exception, match="evicted"):
            m.to_arrow()

    def test_zero_copy_to_arrow(self) -> None:
        """to_arrow returns the same object (zero-copy) for Arrow-backed refs."""
        from rivet_core.models import Material

        ref, table = self._make_ref()
        m = Material(name="m", catalog="c", state="materialized", materialized_ref=ref)
        assert m.to_arrow() is table
