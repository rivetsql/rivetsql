"""Fixture loading: file-based and inline Arrow table loading."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.ipc as pa_ipc
import pyarrow.json as pa_json
import pyarrow.parquet as pq

from rivet_core.errors import RivetError

_FILE_LOADERS = {
    ".parquet": lambda p: pq.read_table(p),
    ".csv": lambda p: pa_csv.read_csv(p),
    ".tsv": lambda p: pa_csv.read_csv(p),
    ".json": lambda p: pa_json.read_json(p),
    ".ndjson": lambda p: pa_json.read_json(p),
    ".jsonl": lambda p: pa_json.read_json(p),
    ".arrow": lambda p: pa_ipc.open_file(p).read_all(),
    ".ipc": lambda p: pa_ipc.open_file(p).read_all(),
}


class FixtureError(Exception):
    """Raised when fixture loading fails (RVT-9xx)."""

    def __init__(self, error: RivetError) -> None:
        self.error = error
        super().__init__(str(error))


def load_fixture_file(path: Path, project_root: Path) -> pyarrow.Table:
    """Load a fixture file as an Arrow table.

    Resolves *path* relative to *project_root*.
    Dispatches by extension: .parquet, .csv/.tsv, .json/.ndjson/.jsonl, .arrow/.ipc.
    Raises FixtureError wrapping RVT-901 if file not found, RVT-902 if unparseable.
    """
    resolved = path if path.is_absolute() else project_root / path
    if not resolved.exists():
        raise FixtureError(RivetError(
            code="RVT-901",
            message=f"Fixture file not found: {resolved}",
            context={"path": str(resolved)},
        ))
    ext = resolved.suffix.lower()
    loader = _FILE_LOADERS.get(ext)
    if loader is None:
        raise FixtureError(RivetError(
            code="RVT-902",
            message=f"Unsupported fixture file extension '{ext}': {resolved}",
            context={"path": str(resolved), "extension": ext},
        ))
    try:
        return loader(str(resolved))
    except Exception as exc:
        raise FixtureError(RivetError(
            code="RVT-902",
            message=f"Failed to parse fixture file: {resolved}: {exc}",
            context={"path": str(resolved), "error": str(exc)},
        )) from exc


# --- Arrow type map for explicit type casting ---

_DECIMAL_RE = re.compile(r"^decimal128\((\d+),\s*(\d+)\)$")

_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.utf8(),
    "bool": pa.bool_(),
    "date32": pa.date32(),
    "timestamp[us]": pa.timestamp("us"),
    "binary": pa.binary(),
}


def _resolve_arrow_type(type_str: str) -> pa.DataType:
    """Resolve a type string to a PyArrow DataType."""
    t = _ARROW_TYPE_MAP.get(type_str)
    if t is not None:
        return t
    m = _DECIMAL_RE.match(type_str)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))
    raise FixtureError(RivetError(
        code="RVT-902",
        message=f"Unknown Arrow type: '{type_str}'",
        context={"type": type_str},
    ))


# --- Type inference ---

_ISO_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"
)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _infer_value(v: Any) -> pa.DataType:
    """Infer Arrow type from a single Python value."""
    if isinstance(v, bool):
        return pa.bool_()
    if isinstance(v, int):
        return pa.int64()
    if isinstance(v, float):
        return pa.float64()
    if isinstance(v, str):
        if _ISO_DATETIME_RE.match(v):
            return pa.timestamp("us")
        if _ISO_DATE_RE.match(v):
            return pa.date32()
    return pa.utf8()


def _infer_column_type(values: list[Any]) -> pa.DataType:
    """Infer Arrow type for a column from its non-null values."""
    for v in values:
        if v is not None:
            return _infer_value(v)
    return pa.utf8()


def _cast_value(v: Any, target: pa.DataType) -> Any:
    """Cast a single Python value to match the target Arrow type."""
    if v is None:
        return None
    if pa.types.is_boolean(target):
        return bool(v)
    if pa.types.is_integer(target):
        return int(v)
    if pa.types.is_floating(target):
        return float(v)
    if pa.types.is_decimal(target):
        return Decimal(str(v))
    if pa.types.is_timestamp(target) and isinstance(v, str):
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    if pa.types.is_date(target) and isinstance(v, str):
        return date.fromisoformat(v)
    if pa.types.is_string(target) or pa.types.is_large_string(target):
        return str(v)
    return v


def load_inline_data(
    columns: list[str],
    rows: list[list],  # type: ignore[type-arg]
    types: list[str] | None = None,
) -> pa.Table:
    """Parse inline YAML data into an Arrow table.

    If *types* provided, cast columns to specified Arrow types.
    If *types* omitted, infer from values.
    Raises RVT-902 if types length != columns length.
    """
    if types is not None and len(types) != len(columns):
        raise FixtureError(RivetError(
            code="RVT-902",
            message=f"types length ({len(types)}) != columns length ({len(columns)})",
            context={"types_len": len(types), "columns_len": len(columns)},
        ))

    # Transpose rows → column-oriented lists
    col_data: list[list[Any]] = [[] for _ in columns]
    for row in rows:
        for i, val in enumerate(row):
            col_data[i].append(val)

    if types is not None:
        arrow_types = [_resolve_arrow_type(t) for t in types]
    else:
        arrow_types = [_infer_column_type(vals) for vals in col_data]

    arrays = []
    for vals, arrow_type in zip(col_data, arrow_types):
        casted = [_cast_value(v, arrow_type) for v in vals]
        arrays.append(pa.array(casted, type=arrow_type))

    return pa.table(dict(zip(columns, arrays)))


def load_fixture(spec: dict, project_root: Path) -> pa.Table:  # type: ignore[type-arg]
    """Load a fixture from either a file reference or inline data.

    Dispatches based on presence of 'file' key vs 'columns'+'rows' keys.
    """
    if "file" in spec:
        return load_fixture_file(Path(spec["file"]), project_root)
    return load_inline_data(
        columns=spec["columns"],
        rows=spec["rows"],
        types=spec.get("types"),
    )
