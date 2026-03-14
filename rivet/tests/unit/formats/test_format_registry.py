"""Example-based unit tests for the FormatRegistry.

Tests cover:
- FileFormat enum canonical values (Req 1.1)
- Extension-to-format mappings (Req 1.2)
- Primary extensions per format (Req 1.3)
- detect_format with no-extension path returns default (Req 2.3)
- detect_format with child_names containing _delta_log/ returns DELTA (Req 2.8)
- resolve_format with all-None candidates returns parquet (Req 11.2)
- validate_format raises for invalid name (Req 4.1)
- validate_plugin_support raises for unsupported format (Req 5.2)
"""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.formats import (
    EXT_TO_FORMAT,
    FORMAT_TO_EXT,
    FileFormat,
    FormatRegistry,
)

# -------------------------------------------------------------------
# Req 1.1 — FileFormat enum contains exactly 6 canonical values
# -------------------------------------------------------------------


def test_fileformat_has_exactly_six_values():
    assert set(FileFormat) == {
        FileFormat.PARQUET,
        FileFormat.CSV,
        FileFormat.JSON,
        FileFormat.IPC,
        FileFormat.ORC,
        FileFormat.DELTA,
    }


# -------------------------------------------------------------------
# Req 1.2 — Each extension maps to the correct FileFormat
# -------------------------------------------------------------------

_EXPECTED_EXT_MAPPINGS = {
    ".parquet": FileFormat.PARQUET,
    ".pq": FileFormat.PARQUET,
    ".csv": FileFormat.CSV,
    ".tsv": FileFormat.CSV,
    ".json": FileFormat.JSON,
    ".jsonl": FileFormat.JSON,
    ".ndjson": FileFormat.JSON,
    ".arrow": FileFormat.IPC,
    ".feather": FileFormat.IPC,
    ".ipc": FileFormat.IPC,
    ".orc": FileFormat.ORC,
}


@pytest.mark.parametrize("ext, expected", list(_EXPECTED_EXT_MAPPINGS.items()))
def test_extension_mapping(ext: str, expected: FileFormat):
    assert EXT_TO_FORMAT[ext] is expected


def test_extension_map_has_exactly_eleven_entries():
    assert len(EXT_TO_FORMAT) == 11


# -------------------------------------------------------------------
# Req 1.3 — Each format's primary extension
# -------------------------------------------------------------------

_EXPECTED_PRIMARY_EXT = {
    FileFormat.PARQUET: ".parquet",
    FileFormat.CSV: ".csv",
    FileFormat.JSON: ".json",
    FileFormat.IPC: ".arrow",
    FileFormat.ORC: ".orc",
    FileFormat.DELTA: "",
}


@pytest.mark.parametrize("fmt, expected", list(_EXPECTED_PRIMARY_EXT.items()))
def test_primary_extension(fmt: FileFormat, expected: str):
    assert FormatRegistry.primary_extension(fmt) == expected
    assert FORMAT_TO_EXT[fmt] == expected


# -------------------------------------------------------------------
# Req 2.3 — detect_format with no-extension path returns default
# -------------------------------------------------------------------


def test_detect_format_no_extension_returns_default():
    assert FormatRegistry.detect_format("some/path/no_ext") == FileFormat.PARQUET


def test_detect_format_no_extension_custom_default():
    assert FormatRegistry.detect_format("data", default=FileFormat.CSV) == FileFormat.CSV


# -------------------------------------------------------------------
# Req 2.8 — detect_format with child_names containing _delta_log/
# -------------------------------------------------------------------


def test_detect_format_child_names_delta_log():
    result = FormatRegistry.detect_format(
        "s3://bucket/prefix",
        child_names=["_delta_log/", "part-0.parquet"],
    )
    assert result is FileFormat.DELTA


def test_detect_format_child_names_delta_log_no_slash():
    result = FormatRegistry.detect_format(
        "s3://bucket/prefix",
        child_names=["_delta_log", "part-0.parquet"],
    )
    assert result is FileFormat.DELTA


# -------------------------------------------------------------------
# Req 11.2 — resolve_format with all-None candidates returns parquet
# -------------------------------------------------------------------


def test_resolve_format_all_none_no_path():
    assert FormatRegistry.resolve_format(None, None, None) == FileFormat.PARQUET


def test_resolve_format_empty_strings_no_path():
    assert FormatRegistry.resolve_format("", "", "") == FileFormat.PARQUET


# -------------------------------------------------------------------
# Req 4.1 — validate_format raises for invalid name
# -------------------------------------------------------------------


def test_validate_format_invalid_name():
    with pytest.raises(PluginValidationError) as exc_info:
        FormatRegistry.validate_format("xlsx")
    assert "Invalid format 'xlsx'" in str(exc_info.value)


def test_validate_format_valid_name():
    assert FormatRegistry.validate_format("parquet") is FileFormat.PARQUET


def test_validate_format_case_insensitive():
    assert FormatRegistry.validate_format("PARQUET") is FileFormat.PARQUET
    assert FormatRegistry.validate_format("Csv") is FileFormat.CSV


# -------------------------------------------------------------------
# Req 5.2 — validate_plugin_support raises for unsupported format
# -------------------------------------------------------------------


def test_validate_plugin_support_unsupported():
    with pytest.raises(PluginValidationError) as exc_info:
        FormatRegistry.validate_plugin_support(FileFormat.ORC, "filesystem", "sink")
    assert "not supported by filesystem sink" in str(exc_info.value)


def test_validate_plugin_support_supported():
    # Should not raise
    FormatRegistry.validate_plugin_support(FileFormat.PARQUET, "filesystem", "sink")


def test_is_supported_matches_validate():
    assert FormatRegistry.is_supported(FileFormat.PARQUET, "filesystem", "sink") is True
    assert FormatRegistry.is_supported(FileFormat.ORC, "filesystem", "sink") is False
