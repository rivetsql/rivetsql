"""Export Arrow tables to CSV, TSV, Parquet, JSON, and JSONL formats."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

ExportFormat = Literal["csv", "tsv", "parquet", "json", "jsonl"]


def export_table(table: pa.Table, path: str | Path, fmt: ExportFormat) -> None:
    """Write *table* to *path* in the given format.

    Raises:
        ValueError: if *fmt* is not a supported format.
        OSError: on I/O failure (disk full, permission denied, etc.).
    """
    dest = Path(path)
    if fmt == "parquet":
        pq.write_table(table, dest)
    elif fmt in ("csv", "tsv"):
        delimiter = "\t" if fmt == "tsv" else ","
        _write_delimited(table, dest, delimiter)
    elif fmt == "json":
        _write_json(table, dest)
    elif fmt == "jsonl":
        _write_jsonl(table, dest)
    else:
        raise ValueError(f"Unsupported export format: {fmt!r}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _arrow_value(val: object) -> object:
    """Convert a PyArrow scalar to a plain Python value."""
    if isinstance(val, pa.Scalar):
        return val.as_py()
    return val


def _rows(table: pa.Table) -> list[dict[str, object]]:
    return [
        {col: _arrow_value(table.column(col)[i]) for col in table.column_names}
        for i in range(table.num_rows)
    ]


def _write_delimited(table: pa.Table, dest: Path, delimiter: str) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=table.column_names, delimiter=delimiter)
    writer.writeheader()
    writer.writerows(_rows(table))
    dest.write_text(buf.getvalue(), encoding="utf-8")


def _write_json(table: pa.Table, dest: Path) -> None:
    dest.write_text(json.dumps(_rows(table), default=str), encoding="utf-8")


def _write_jsonl(table: pa.Table, dest: Path) -> None:
    lines = "\n".join(json.dumps(row, default=str) for row in _rows(table))
    dest.write_text(lines + ("\n" if lines else ""), encoding="utf-8")
