"""Tests for dialect_names property on InteractiveSession (task 3.1).

Validates: Requirement 3.7
"""

from __future__ import annotations

from pathlib import Path

import sqlglot

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.plugins import PluginRegistry


def _make_session() -> InteractiveSession:
    session = InteractiveSession(project_path=Path("."))
    session.init_from(
        assembly=Assembly([]),
        catalogs={},
        engines={},
        registry=PluginRegistry(),
    )
    return session


def test_dialect_names_is_non_empty() -> None:
    session = _make_session()
    assert len(session.dialect_names) > 0


def test_dialect_names_all_pass_get_or_raise() -> None:
    session = _make_session()
    for name in session.dialect_names:
        sqlglot.Dialect.get_or_raise(name)  # must not raise


def test_dialect_names_contains_common_dialects() -> None:
    session = _make_session()
    names = session.dialect_names
    for expected in ("duckdb", "spark", "postgres", "bigquery"):
        assert expected in names
