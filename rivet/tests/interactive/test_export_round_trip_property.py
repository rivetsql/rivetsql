"""Property-based tests for export round-trip correctness.

Property 20: Export round-trip.

Properties verified:
- For any Arrow table, exporting to CSV/TSV/Parquet/JSON/JSONL and reading back
  preserves row count and column names.
- Parquet round-trip preserves exact values (lossless).
- JSON/JSONL round-trip preserves row count and column names.
- CSV/TSV round-trip preserves row count and column names.

Validates: Requirements 26.2
"""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.exporter import export_table

# ── Strategies ────────────────────────────────────────────────────────────────

_int_val = st.integers(min_value=-1000, max_value=1000)
_str_val = st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")), min_size=0, max_size=20)


def _int_table_strategy(min_rows: int = 0, max_rows: int = 30) -> st.SearchStrategy[pa.Table]:
    """Generate Arrow tables with integer columns only (safe for all formats)."""
    return st.lists(
        st.fixed_dictionaries({"id": _int_val, "value": _int_val}),
        min_size=min_rows,
        max_size=max_rows,
    ).map(lambda rows: pa.Table.from_pylist(rows) if rows else pa.table({"id": pa.array([], type=pa.int64()), "value": pa.array([], type=pa.int64())}))


# ── Property 20a: Parquet round-trip preserves row count and column names ─────


@given(table=_int_table_strategy())
@settings(max_examples=100)
def test_parquet_round_trip_row_count_and_columns(table: pa.Table) -> None:
    """Parquet export then read back preserves row count and column names (Req 26.2)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "out.parquet"
        export_table(table, dest, "parquet")
        back = pq.read_table(dest)
        assert back.num_rows == table.num_rows
        assert back.column_names == table.column_names


@given(table=_int_table_strategy(min_rows=1))
@settings(max_examples=100)
def test_parquet_round_trip_values(table: pa.Table) -> None:
    """Parquet export then read back preserves exact integer values (Req 26.2)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "out.parquet"
        export_table(table, dest, "parquet")
        back = pq.read_table(dest)
        assert back.column("id").to_pylist() == table.column("id").to_pylist()
        assert back.column("value").to_pylist() == table.column("value").to_pylist()


# ── Property 20b: CSV round-trip preserves row count and column names ─────────


@given(table=_int_table_strategy())
@settings(max_examples=100)
def test_csv_round_trip_row_count_and_columns(table: pa.Table) -> None:
    """CSV export then read back preserves row count and column names (Req 26.2)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "out.csv"
        export_table(table, dest, "csv")
        rows = list(csv.DictReader(dest.read_text().splitlines()))
        assert len(rows) == table.num_rows
        if table.num_rows > 0:
            assert set(rows[0].keys()) == set(table.column_names)


# ── Property 20c: TSV round-trip preserves row count and column names ─────────


@given(table=_int_table_strategy())
@settings(max_examples=100)
def test_tsv_round_trip_row_count_and_columns(table: pa.Table) -> None:
    """TSV export then read back preserves row count and column names (Req 26.2)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "out.tsv"
        export_table(table, dest, "tsv")
        rows = list(csv.DictReader(dest.read_text().splitlines(), delimiter="\t"))
        assert len(rows) == table.num_rows
        if table.num_rows > 0:
            assert set(rows[0].keys()) == set(table.column_names)


# ── Property 20d: JSON round-trip preserves row count and column names ────────


@given(table=_int_table_strategy())
@settings(max_examples=100)
def test_json_round_trip_row_count_and_columns(table: pa.Table) -> None:
    """JSON export then read back preserves row count and column names (Req 26.2)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "out.json"
        export_table(table, dest, "json")
        data = json.loads(dest.read_text())
        assert isinstance(data, list)
        assert len(data) == table.num_rows
        if table.num_rows > 0:
            assert set(data[0].keys()) == set(table.column_names)


# ── Property 20e: JSONL round-trip preserves row count and column names ───────


@given(table=_int_table_strategy())
@settings(max_examples=100)
def test_jsonl_round_trip_row_count_and_columns(table: pa.Table) -> None:
    """JSONL export then read back preserves row count and column names (Req 26.2)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "out.jsonl"
        export_table(table, dest, "jsonl")
        lines = [l for l in dest.read_text().splitlines() if l.strip()]
        assert len(lines) == table.num_rows
        if table.num_rows > 0:
            obj = json.loads(lines[0])
            assert set(obj.keys()) == set(table.column_names)


# ── Property 20f: unsupported format always raises ValueError ─────────────────


@given(fmt=st.text(min_size=1, max_size=10).filter(lambda s: s not in ("csv", "tsv", "parquet", "json", "jsonl")))
@settings(max_examples=50)
def test_unsupported_format_always_raises(fmt: str) -> None:
    """Any format not in the supported set raises ValueError (Req 26.2)."""
    table = pa.table({"x": [1, 2, 3]})
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / f"out.{fmt}"
        with pytest.raises(ValueError, match="Unsupported export format"):
            export_table(table, dest, fmt)  # type: ignore[arg-type]
