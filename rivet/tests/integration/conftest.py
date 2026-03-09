"""Integration test configuration — marks all tests and provides shared fixtures."""

import pathlib

import pytest

_THIS_DIR = pathlib.Path(__file__).resolve().parent


def pytest_collection_modifyitems(items):
    for item in items:
        if _THIS_DIR in pathlib.Path(item.fspath).resolve().parents:
            item.add_marker(pytest.mark.integration)


@pytest.fixture
def registry_with_duckdb():
    """Fresh PluginRegistry with DuckDB plugin registered."""
    from rivet_core.plugins import PluginRegistry
    from rivet_duckdb import DuckDBPlugin

    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg
