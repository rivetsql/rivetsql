"""Unit tests for JSON flattening edge cases.

Covers max_depth boundary behavior, null preservation, array serialization,
and schema evolution across record batches.
"""

from __future__ import annotations

import json

import pyarrow as pa

from rivet_rest.flatten import (
    arrow_to_records,
    flatten_records,
    infer_schema,
    records_to_arrow,
    unflatten_record,
)

# ---------------------------------------------------------------------------
# max_depth boundary
# ---------------------------------------------------------------------------


class TestMaxDepthBoundary:
    def test_objects_at_max_depth_are_flattened(self) -> None:
        """Objects at exactly max_depth are expanded into dot-separated keys."""
        record = {"a": {"b": "leaf"}}
        flat = flatten_records([record], max_depth=2)
        assert flat == [{"a.b": "leaf"}]

    def test_objects_beyond_max_depth_are_json_serialized(self) -> None:
        """Objects deeper than max_depth are JSON-serialized as strings."""
        record = {"a": {"b": {"c": "deep"}}}
        flat = flatten_records([record], max_depth=2)
        assert flat == [{"a.b": json.dumps({"c": "deep"})}]

    def test_max_depth_1_serializes_all_nested(self) -> None:
        """With max_depth=1, any nested object is serialized."""
        record = {"x": {"y": 1}}
        flat = flatten_records([record], max_depth=1)
        assert flat == [{"x": json.dumps({"y": 1})}]

    def test_max_depth_3_flattens_three_levels(self) -> None:
        record = {"a": {"b": {"c": "val"}}}
        flat = flatten_records([record], max_depth=3)
        assert flat == [{"a.b.c": "val"}]

    def test_max_depth_3_serializes_fourth_level(self) -> None:
        record = {"a": {"b": {"c": {"d": "deep"}}}}
        flat = flatten_records([record], max_depth=3)
        assert flat == [{"a.b.c": json.dumps({"d": "deep"})}]


# ---------------------------------------------------------------------------
# Null preservation
# ---------------------------------------------------------------------------


class TestNullPreservation:
    def test_null_preserved_through_flatten_unflatten(self) -> None:
        record = {"name": "Alice", "age": None}
        flat = flatten_records([record], max_depth=3)
        assert flat == [{"name": "Alice", "age": None}]
        unflat = unflatten_record(flat[0])
        assert unflat["age"] is None

    def test_null_preserved_through_arrow_round_trip(self) -> None:
        records = [{"x": None, "y": "hello"}]
        table = records_to_arrow(records, max_depth=3)
        result = arrow_to_records(table)
        assert result[0]["x"] is None
        assert result[0]["y"] == "hello"

    def test_nested_null_preserved(self) -> None:
        record = {"a": {"b": None}}
        flat = flatten_records([record], max_depth=3)
        assert flat == [{"a.b": None}]


# ---------------------------------------------------------------------------
# Array serialization
# ---------------------------------------------------------------------------


class TestArraySerialization:
    def test_arrays_json_serialized_as_strings(self) -> None:
        record = {"tags": [1, 2, 3], "name": "test"}
        flat = flatten_records([record], max_depth=3)
        assert flat[0]["tags"] == "[1, 2, 3]"
        assert flat[0]["name"] == "test"

    def test_nested_arrays_serialized(self) -> None:
        record = {"data": {"items": [{"id": 1}, {"id": 2}]}}
        flat = flatten_records([record], max_depth=3)
        assert flat[0]["data.items"] == json.dumps([{"id": 1}, {"id": 2}])

    def test_empty_array_serialized(self) -> None:
        record = {"items": []}
        flat = flatten_records([record], max_depth=3)
        assert flat[0]["items"] == "[]"


# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------


class TestSchemaEvolution:
    def test_new_column_added_with_null_backfill(self) -> None:
        """When a second record introduces a new column, earlier rows get None."""
        records = [{"a": 1}, {"a": 2, "b": "new"}]
        table = records_to_arrow(records, max_depth=3)
        result = arrow_to_records(table)
        assert result[0]["a"] == 1
        assert result[0]["b"] is None
        assert result[1]["a"] == 2
        assert result[1]["b"] == "new"

    def test_type_mismatch_coerced(self) -> None:
        """When a value doesn't match the inferred type, it's coerced."""
        # First record infers int64 for 'x', second has a float
        records = [{"x": 1}, {"x": 2.5}]
        table = records_to_arrow(records, max_depth=3)
        # int64 column — 2.5 should be coerced to 2
        result = arrow_to_records(table)
        assert result[0]["x"] == 1
        assert result[1]["x"] == 2  # float coerced to int

    def test_schema_provided_overrides_inference(self) -> None:
        """When a schema is provided, it's used instead of inference."""
        schema = pa.schema([("a", pa.utf8()), ("b", pa.int64())])
        records = [{"a": "hello", "b": 42}]
        table = records_to_arrow(records, schema=schema, max_depth=3)
        assert table.schema == schema

    def test_empty_records_with_schema(self) -> None:
        schema = pa.schema([("a", pa.utf8())])
        table = records_to_arrow([], schema=schema, max_depth=3)
        assert table.num_rows == 0
        assert table.schema == schema

    def test_empty_records_without_schema(self) -> None:
        table = records_to_arrow([], max_depth=3)
        assert table.num_rows == 0


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------


class TestSchemaInference:
    def test_string_inferred_as_utf8(self) -> None:
        schema = infer_schema([{"x": "hello"}])
        assert schema.field("x").type == pa.utf8()

    def test_int_inferred_as_int64(self) -> None:
        schema = infer_schema([{"x": 42}])
        assert schema.field("x").type == pa.int64()

    def test_float_inferred_as_float64(self) -> None:
        schema = infer_schema([{"x": 3.14}])
        assert schema.field("x").type == pa.float64()

    def test_bool_inferred_as_bool(self) -> None:
        schema = infer_schema([{"x": True}])
        assert schema.field("x").type == pa.bool_()

    def test_none_inferred_as_utf8(self) -> None:
        schema = infer_schema([{"x": None}])
        assert schema.field("x").type == pa.utf8()

    def test_none_upgraded_when_later_value_seen(self) -> None:
        """If first value is None (utf8), a later int upgrades the type."""
        schema = infer_schema([{"x": None}, {"x": 42}])
        assert schema.field("x").type == pa.int64()
