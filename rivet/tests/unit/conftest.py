"""Unit test configuration — marks all tests in this directory as unit tests."""

import pathlib

import pytest

_THIS_DIR = pathlib.Path(__file__).resolve().parent


def pytest_collection_modifyitems(items):
    for item in items:
        if _THIS_DIR in pathlib.Path(item.fspath).resolve().parents:
            item.add_marker(pytest.mark.unit)
