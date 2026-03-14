"""Integration tests for directory-based format detection.

Exercises FormatRegistry.detect_format with real filesystem directories
containing actual files, verifying Requirements 2.5, 2.6, 2.7.
"""

from __future__ import annotations

from pathlib import Path

from rivet_core.formats import FileFormat, FormatRegistry


def test_directory_with_parquet_files(tmp_path: Path):
    """Temp directory with multiple .parquet files → detect_format returns PARQUET."""
    (tmp_path / "part-0.parquet").write_bytes(b"")
    (tmp_path / "part-1.parquet").write_bytes(b"")
    (tmp_path / "part-2.parquet").write_bytes(b"")

    assert FormatRegistry.detect_format(tmp_path) == FileFormat.PARQUET


def test_directory_with_delta_log(tmp_path: Path):
    """Temp directory with _delta_log/ subdirectory → detect_format returns DELTA."""
    (tmp_path / "_delta_log").mkdir()
    (tmp_path / "_delta_log" / "00000.json").write_bytes(b"")
    (tmp_path / "part-0.parquet").write_bytes(b"")

    assert FormatRegistry.detect_format(tmp_path) == FileFormat.DELTA


def test_directory_with_mixed_csv_json_more_csv(tmp_path: Path):
    """Temp directory with mixed .csv and .json files (more csv) → detect_format returns CSV."""
    (tmp_path / "data1.csv").write_bytes(b"")
    (tmp_path / "data2.csv").write_bytes(b"")
    (tmp_path / "data3.csv").write_bytes(b"")
    (tmp_path / "meta.json").write_bytes(b"")

    assert FormatRegistry.detect_format(tmp_path) == FileFormat.CSV
