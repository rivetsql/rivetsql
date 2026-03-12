"""JSON flattening, schema inference, and Arrow conversion for REST API responses.

Converts nested JSON records into flat Arrow-compatible columns using
dot-separated naming (e.g. ``address.city``), with configurable maximum
nesting depth.  Structures beyond ``max_depth`` are JSON-serialized as
``large_utf8`` string columns.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

# ---------------------------------------------------------------------------
# Flattening
# ---------------------------------------------------------------------------


def flatten_records(records: list[dict[str, Any]], max_depth: int = 3) -> list[dict[str, Any]]:
    """Flatten nested JSON records using dot-separated keys.

    Objects at depth <= *max_depth* are expanded into ``parent.child`` keys.
    Objects and arrays beyond *max_depth* are JSON-serialized as strings.

    Args:
        records: List of JSON-decoded dicts.
        max_depth: Maximum nesting depth to flatten (1-based).

    Returns:
        List of flat dicts with dot-separated keys.
    """
    return [_flatten_one(record, max_depth) for record in records]


def _flatten_one(record: dict[str, Any], max_depth: int) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    _flatten_recurse(record, "", 1, max_depth, flat)
    return flat


def _flatten_recurse(
    obj: dict[str, Any],
    prefix: str,
    current_depth: int,
    max_depth: int,
    out: dict[str, Any],
) -> None:
    for key, value in obj.items():
        full_key = f"{prefix}{key}" if prefix else key
        if isinstance(value, dict) and current_depth < max_depth:
            _flatten_recurse(value, full_key + ".", current_depth + 1, max_depth, out)
        elif isinstance(value, (dict, list)):
            out[full_key] = json.dumps(value)
        else:
            out[full_key] = value


# ---------------------------------------------------------------------------
# Unflattening
# ---------------------------------------------------------------------------


def unflatten_record(flat: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct a nested JSON dict from dot-separated flat keys.

    Args:
        flat: A flat dict whose keys may contain dots.

    Returns:
        A nested dict.
    """
    result: dict[str, Any] = {}
    for key, value in flat.items():
        parts = key.split(".")
        target = result
        for part in parts[:-1]:
            if part not in target or not isinstance(target[part], dict):
                target[part] = {}
            target = target[part]
        target[parts[-1]] = value
    return result


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------


def infer_schema(records: list[dict[str, Any]]) -> pa.Schema:
    """Infer an Arrow schema from flat JSON records.

    Type mapping:
    - ``str``   → ``pa.utf8()``
    - ``int``   → ``pa.int64()``
    - ``float`` → ``pa.float64()``
    - ``bool``  → ``pa.bool_()``
    - ``None``  → ``pa.utf8()``
    - ``list``  → ``pa.large_utf8()``  (JSON-serialized by flatten step)
    - ``dict``  → ``pa.large_utf8()``  (JSON-serialized by flatten step)

    When a column has mixed types across records the first non-null type wins.

    Args:
        records: Flat JSON records (output of :func:`flatten_records`).

    Returns:
        A ``pyarrow.Schema``.
    """
    column_types: dict[str, pa.DataType] = {}
    for record in records:
        for key, value in record.items():
            if key not in column_types:
                column_types[key] = _json_type_to_arrow(value)
            elif column_types[key] == pa.utf8() and value is not None:
                # Upgrade from null-inferred utf8 to actual type
                inferred = _json_type_to_arrow(value)
                if inferred != pa.utf8() or isinstance(value, str):
                    column_types[key] = inferred
    return pa.schema([(name, dtype) for name, dtype in column_types.items()])


def _json_type_to_arrow(value: Any) -> pa.DataType:
    if value is None:
        return pa.utf8()
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, str):
        # Strings that came from JSON-serialized arrays/objects get large_utf8
        # but we can't distinguish here — the flatten step already serialized.
        return pa.utf8()
    # list or dict (shouldn't appear after flattening, but be safe)
    return pa.large_utf8()


# ---------------------------------------------------------------------------
# Arrow conversion
# ---------------------------------------------------------------------------


def records_to_arrow(
    records: list[dict[str, Any]],
    schema: pa.Schema | None = None,
    max_depth: int = 3,
) -> pa.Table:
    """Flatten JSON records and convert to an Arrow table.

    Args:
        records: Raw (possibly nested) JSON records.
        schema: Optional pre-existing schema.  If ``None``, inferred from
            the flattened records.
        max_depth: Maximum nesting depth for flattening.

    Returns:
        A ``pyarrow.Table``.
    """
    flat = flatten_records(records, max_depth)
    if not flat:
        if schema is not None:
            return pa.table(
                {name: pa.array([], type=field.type) for name, field in zip(schema.names, schema)}
            )
        return pa.table({})

    if schema is None:
        schema = infer_schema(flat)

    # Build columns aligned to schema, handling schema evolution
    all_columns: set[str] = set()
    for rec in flat:
        all_columns.update(rec.keys())

    # Extend schema with new columns not yet in it
    existing_names = set(schema.names)
    for col in sorted(all_columns - existing_names):
        # Infer type from first non-null value
        col_type = pa.utf8()
        for rec in flat:
            if col in rec and rec[col] is not None:
                col_type = _json_type_to_arrow(rec[col])
                break
        schema = schema.append(pa.field(col, col_type))

    arrays: dict[str, pa.Array] = {}
    for field in schema:
        raw_values = [_coerce_value(rec.get(field.name), field.type) for rec in flat]
        arrays[field.name] = pa.array(raw_values, type=field.type)

    return pa.table(arrays, schema=schema)


def _coerce_value(value: Any, target_type: pa.DataType) -> Any:
    """Coerce a JSON value to match the target Arrow type.

    Returns the value as-is when compatible, ``None`` for missing values,
    or a utf8 string fallback when coercion fails.
    """
    if value is None:
        return None

    try:
        if target_type == pa.int64():
            if isinstance(value, bool):
                return int(value)
            return int(value)
        if target_type == pa.float64():
            return float(value)
        if target_type == pa.bool_():
            if isinstance(value, bool):
                return value
            return bool(value)
        if target_type == pa.utf8() or target_type == pa.large_utf8():
            if isinstance(value, str):
                return value
            return json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        return value
    except (ValueError, TypeError):
        return str(value)


# ---------------------------------------------------------------------------
# Arrow → JSON records (for sink serialization)
# ---------------------------------------------------------------------------


def arrow_to_records(table: pa.Table) -> list[dict[str, Any]]:
    """Convert an Arrow table to a list of flat JSON-compatible dicts.

    Args:
        table: A ``pyarrow.Table``.

    Returns:
        List of flat dicts with Python-native values.
    """
    records: list[dict[str, Any]] = []
    columns = table.column_names
    for i in range(table.num_rows):
        row: dict[str, Any] = {}
        for col_name in columns:
            val = table.column(col_name)[i].as_py()
            row[col_name] = val
        records.append(row)
    return records
