"""Tests for write_strategies.py and watermark.py."""

from __future__ import annotations

import pytest

from rivet_core.watermark import WatermarkBackend, WatermarkState
from rivet_core.write_strategies import VALID_WRITE_STRATEGY_TYPES, WriteStrategy


class TestWriteStrategy:
    def test_valid_types(self) -> None:
        for strategy_type in VALID_WRITE_STRATEGY_TYPES:
            ws = WriteStrategy(type=strategy_type)
            assert ws.type == strategy_type

    def test_default_config_is_empty(self) -> None:
        ws = WriteStrategy(type="append")
        assert ws.config == {}

    def test_config_stored(self) -> None:
        ws = WriteStrategy(type="merge", config={"key_columns": ["id"]})
        assert ws.config == {"key_columns": ["id"]}

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid write strategy type"):
            WriteStrategy(type="upsert")

    def test_frozen(self) -> None:
        ws = WriteStrategy(type="append")
        with pytest.raises((AttributeError, TypeError)):
            ws.type = "replace"  # type: ignore[misc]

    def test_all_seven_types_valid(self) -> None:
        expected = {"append", "replace", "truncate_insert", "merge", "delete_insert", "incremental_append", "scd2"}
        assert expected == VALID_WRITE_STRATEGY_TYPES


class TestWatermarkState:
    def test_fields(self) -> None:
        ws = WatermarkState(
            column="updated_at",
            value="2024-01-01T00:00:00",
            value_type="timestamp",
            last_run="2024-01-02T00:00:00Z",
            rows_loaded=100,
        )
        assert ws.column == "updated_at"
        assert ws.value == "2024-01-01T00:00:00"
        assert ws.value_type == "timestamp"
        assert ws.last_run == "2024-01-02T00:00:00Z"
        assert ws.rows_loaded == 100
        assert ws.metadata == {}

    def test_metadata_stored(self) -> None:
        ws = WatermarkState(
            column="id",
            value="42",
            value_type="integer",
            last_run="2024-01-01T00:00:00Z",
            rows_loaded=10,
            metadata={"source": "orders"},
        )
        assert ws.metadata == {"source": "orders"}

    def test_frozen(self) -> None:
        ws = WatermarkState(
            column="id", value="1", value_type="integer",
            last_run="2024-01-01T00:00:00Z", rows_loaded=0,
        )
        with pytest.raises((AttributeError, TypeError)):
            ws.value = "2"  # type: ignore[misc]


class TestWatermarkBackend:
    def test_is_abstract(self) -> None:
        with pytest.raises(TypeError):
            WatermarkBackend()  # type: ignore[abstract]

    def test_concrete_implementation(self) -> None:
        class InMemoryBackend(WatermarkBackend):
            def __init__(self) -> None:
                self._store: dict[tuple[str, str], WatermarkState] = {}

            def read(self, sink_name: str, profile: str) -> WatermarkState | None:
                return self._store.get((sink_name, profile))

            def write(self, sink_name: str, profile: str, state: WatermarkState) -> None:
                self._store[(sink_name, profile)] = state

            def delete(self, sink_name: str, profile: str) -> None:
                self._store.pop((sink_name, profile), None)

        backend = InMemoryBackend()
        assert backend.read("my_sink", "prod") is None

        state = WatermarkState(
            column="ts", value="2024-01-01", value_type="date",
            last_run="2024-01-02T00:00:00Z", rows_loaded=50,
        )
        backend.write("my_sink", "prod", state)
        assert backend.read("my_sink", "prod") == state

        backend.delete("my_sink", "prod")
        assert backend.read("my_sink", "prod") is None
