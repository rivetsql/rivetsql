"""Non-interactive query execution for `rivet repl execute`."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from rivet_core.interactive.types import QueryResult

# Exit codes (inline — same boundary rule as __init__.py).
_EXIT_SUCCESS = 0
_EXIT_GENERAL_ERROR = 1
_EXIT_USAGE_ERROR = 10

_VALID_FORMATS = ("table", "json", "csv")


def run_execute(
    *,
    sql: str,
    project: str,
    profile: str,
    engine: str | None,
    format: str,
    max_rows: int,
) -> int:
    """Execute a SQL query non-interactively and print the result."""
    if format not in _VALID_FORMATS:
        print(
            f"error: unsupported format '{format}'. Choose from: {', '.join(_VALID_FORMATS)}.",
            file=sys.stderr,
        )
        return _EXIT_USAGE_ERROR

    project_path = Path(project)
    if not project_path.exists():
        print(f"error: project directory does not exist: {project_path}", file=sys.stderr)
        return _EXIT_USAGE_ERROR

    # --- Build session (reuses the same loader as the interactive REPL) ---
    from rivet_core.interactive import InteractiveSession  # noqa: PLC0415

    from .import _make_loader  # noqa: PLC0415

    session = InteractiveSession(
        project_path=project_path,
        profile=profile,
        read_only=False,
        max_results=max_rows,
        loader=_make_loader(),
    )

    try:
        session.start()
    except Exception as exc:  # noqa: BLE001
        print(f"error: failed to start session: {exc}", file=sys.stderr)
        return _EXIT_GENERAL_ERROR

    # Override engine if requested
    if engine is not None:
        try:
            session.adhoc_engine = engine
        except Exception as exc:  # noqa: BLE001
            print(f"error: {exc}", file=sys.stderr)
            session.stop()
            return _EXIT_USAGE_ERROR

    # --- Execute ---
    try:
        result = session.execute_query(sql)
    except Exception as exc:  # noqa: BLE001
        print(f"error: query failed: {exc}", file=sys.stderr)
        session.stop()
        return _EXIT_GENERAL_ERROR
    finally:
        session.stop()

    # --- Format output ---
    _print_result(result, format)
    return _EXIT_SUCCESS


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def _print_result(result: QueryResult, fmt: str) -> None:
    if fmt == "json":
        _print_json(result)
    elif fmt == "csv":
        _print_csv(result)
    else:
        _print_table(result)


def _print_json(result: QueryResult) -> None:
    """Print result as JSON array of objects."""
    rows = []
    table = result.table
    for i in range(table.num_rows):
        row = {col: _arrow_scalar_to_python(table.column(col)[i]) for col in table.column_names}
        rows.append(row)
    json.dump(rows, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def _print_csv(result: QueryResult) -> None:
    """Print result as CSV."""
    import csv  # noqa: PLC0415

    writer = csv.writer(sys.stdout)
    writer.writerow(result.column_names)
    table = result.table
    for i in range(table.num_rows):
        writer.writerow(
            _arrow_scalar_to_python(table.column(col)[i]) for col in table.column_names
        )


def _print_table(result: QueryResult) -> None:
    """Print a simple text table (no scrolling, no TUI)."""
    table = result.table
    if table.num_rows == 0:
        print("(0 rows)")
        return

    columns = result.column_names
    # Convert all values to strings for width calculation
    str_cols: list[list[str]] = []
    for col in columns:
        arr = table.column(col)
        str_cols.append([_format_cell(arr[i]) for i in range(arr.length())])

    # Column widths (header vs data)
    widths = [
        max(len(col), max((len(cell) for cell in cells), default=0))
        for col, cells in zip(columns, str_cols)
    ]

    # Header
    header = " | ".join(h.ljust(w) for h, w in zip(columns, widths))
    separator = "-+-".join("-" * w for w in widths)
    print(header)
    print(separator)

    # Rows
    for row_idx in range(table.num_rows):
        row = " | ".join(
            str_cols[col_idx][row_idx].ljust(widths[col_idx])
            for col_idx in range(len(columns))
        )
        print(row)

    # Footer
    trunc = " (truncated)" if result.truncated else ""
    print(f"\n({result.row_count} rows, {result.elapsed_ms:.0f} ms{trunc})")


def _arrow_scalar_to_python(scalar: pa.Scalar) -> object:
    """Convert a PyArrow scalar to a plain Python value."""
    if scalar.as_py() is None:
        return None
    return scalar.as_py()


def _format_cell(scalar: pa.Scalar) -> str:
    val = scalar.as_py()
    if val is None:
        return "NULL"
    return str(val)
