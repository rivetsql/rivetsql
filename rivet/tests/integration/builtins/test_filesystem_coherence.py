"""Integration tests for FilesystemCatalogPlugin validation fixes."""

from __future__ import annotations

import pytest

from rivet_core.builtins.filesystem_catalog import FilesystemCatalogPlugin
from rivet_core.errors import PluginValidationError


def test_filesystem_validate_rejects_unrecognized_option():
    plugin = FilesystemCatalogPlugin()

    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"path": "/tmp/data", "bogus_option": True})

    assert exc_info.value.error.code == "RVT-201"
    assert "bogus_option" in exc_info.value.error.message


def test_filesystem_validate_rejects_missing_path():
    plugin = FilesystemCatalogPlugin()

    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"format": "csv"})

    assert exc_info.value.error.code == "RVT-201"
    assert "path" in exc_info.value.error.message


def test_filesystem_validate_accepts_valid_options():
    plugin = FilesystemCatalogPlugin()
    # All recognized options — should not raise
    plugin.validate(
        {
            "path": "/tmp/data",
            "format": "csv",
            "csv_delimiter": ";",
            "csv_header": False,
        }
    )


def test_filesystem_validate_accepts_minimal_options():
    plugin = FilesystemCatalogPlugin()
    # Only the required option — should not raise
    plugin.validate({"path": "/tmp/data"})


def test_filesystem_validate_error_lists_valid_alternatives():
    plugin = FilesystemCatalogPlugin()

    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"path": "/tmp", "unknown_key": 42})

    remediation = exc_info.value.error.remediation or ""
    assert "path" in remediation
    assert "format" in remediation
