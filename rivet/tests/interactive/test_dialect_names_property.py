"""Property test: dialect_names returns valid sqlglot dialects (task 3.3).

Property 5: Every element in `dialect_names` is accepted by
`sqlglot.Dialect.get_or_raise()`.

Validates: Requirements 3.7
"""

from __future__ import annotations

from pathlib import Path

import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry

_engine_type_st = st.sampled_from(["duckdb", "spark", "postgres", "bigquery", "trino"])
_engines_st = st.dictionaries(
    keys=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_")),
    values=_engine_type_st,
    min_size=0,
    max_size=4,
)


def _make_session(engines: dict[str, str]) -> InteractiveSession:
    session = InteractiveSession(project_path=Path("."))
    session.init_from(
        assembly=Assembly([]),
        catalogs={},
        engines={name: ComputeEngine(name=name, engine_type=etype) for name, etype in engines.items()},
        registry=PluginRegistry(),
    )
    return session


@given(engines=_engines_st)
@settings(max_examples=30)
def test_dialect_names_all_valid_sqlglot_dialects(engines: dict[str, str]) -> None:
    """Every element in dialect_names passes sqlglot.Dialect.get_or_raise()."""
    session = _make_session(engines)
    for name in session.dialect_names:
        sqlglot.Dialect.get_or_raise(name)  # must not raise


def test_dialect_names_non_empty_no_engines() -> None:
    """dialect_names is non-empty even with no configured engines."""
    session = _make_session({})
    assert len(session.dialect_names) > 0
