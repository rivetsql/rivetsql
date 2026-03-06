"""Unit tests for materialization strategy contracts (task 4.3)."""

from __future__ import annotations

import pyarrow
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.strategies import (
    ArrowMaterialization,
    MaterializationContext,
    MaterializationStrategy,
    MaterializedRef,
)


def _make_table() -> pyarrow.Table:
    return pyarrow.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def _make_context() -> MaterializationContext:
    return MaterializationContext(joint_name="test_joint", strategy_name="arrow", options={})


class TestMaterializationContext:
    def test_basic_construction(self) -> None:
        ctx = MaterializationContext(joint_name="j", strategy_name="arrow", options={})
        assert ctx.joint_name == "j"
        assert ctx.strategy_name == "arrow"
        assert ctx.options == {}

    def test_with_options(self) -> None:
        ctx = MaterializationContext(joint_name="j", strategy_name="arrow", options={"key": "val"})
        assert ctx.options == {"key": "val"}


class TestMaterializationStrategyABC:
    def test_is_abstract(self) -> None:
        with pytest.raises(TypeError):
            MaterializationStrategy()  # type: ignore[abstract]

    def test_concrete_subclass_must_implement_materialize_and_evict(self) -> None:
        class Incomplete(MaterializationStrategy):
            def materialize(self, data: pyarrow.Table, context: MaterializationContext) -> MaterializedRef:
                raise NotImplementedError

        # Missing evict — should raise TypeError on instantiation
        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]


class TestArrowMaterialization:
    def test_materialize_returns_materialized_ref(self) -> None:
        strategy = ArrowMaterialization()
        table = _make_table()
        ref = strategy.materialize(table, _make_context())
        assert isinstance(ref, MaterializedRef)

    def test_to_arrow_zero_copy(self) -> None:
        strategy = ArrowMaterialization()
        table = _make_table()
        ref = strategy.materialize(table, _make_context())
        result = ref.to_arrow()
        # Zero-copy: same object
        assert result is table

    def test_row_count(self) -> None:
        strategy = ArrowMaterialization()
        table = _make_table()
        ref = strategy.materialize(table, _make_context())
        assert ref.row_count == 3

    def test_size_bytes(self) -> None:
        strategy = ArrowMaterialization()
        table = _make_table()
        ref = strategy.materialize(table, _make_context())
        assert ref.size_bytes == table.nbytes

    def test_storage_type(self) -> None:
        strategy = ArrowMaterialization()
        ref = strategy.materialize(_make_table(), _make_context())
        assert ref.storage_type == "arrow"

    def test_schema_columns(self) -> None:
        strategy = ArrowMaterialization()
        table = _make_table()
        ref = strategy.materialize(table, _make_context())
        schema = ref.schema
        col_names = [c.name for c in schema.columns]
        assert "id" in col_names
        assert "name" in col_names

    def test_evict_makes_to_arrow_fail_with_actionable_error(self) -> None:
        strategy = ArrowMaterialization()
        ref = strategy.materialize(_make_table(), _make_context())
        strategy.evict(ref)
        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()
        assert "evicted" in exc_info.value.error.message.lower()
        assert exc_info.value.error.remediation is not None

    def test_evict_non_arrow_ref_is_noop(self) -> None:
        """evict() on an unknown ref type should not raise."""

        class OtherRef(MaterializedRef):
            def to_arrow(self) -> pyarrow.Table:
                return _make_table()

            @property
            def schema(self):  # type: ignore[override]
                raise NotImplementedError

            @property
            def row_count(self) -> int:
                return 0

            @property
            def size_bytes(self) -> int | None:
                return None

            @property
            def storage_type(self) -> str:
                return "other"

        strategy = ArrowMaterialization()
        other_ref = OtherRef()
        strategy.evict(other_ref)  # should not raise
