# Feature: repl-ux-improvements, Property 4: Engine indicator text formatting
"""Property test: engine indicator text formatting.

Validates: Requirements 6.1, 6.2, 6.3, 6.4
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.widgets.status_bar import StatusBar


@settings(max_examples=100)
@given(
    adhoc_engine=st.one_of(st.none(), st.text(min_size=1, max_size=30)),
    engine_names=st.one_of(
        st.just([]),
        st.lists(st.text(min_size=1, max_size=30), min_size=1, max_size=5),
    ),
    data=st.data(),
)
def test_engine_text_formatting(
    adhoc_engine: str | None, engine_names: list[str], data: st.DataObject
) -> None:
    """Engine indicator text matches expected format for all input combos."""
    # Build a plausible engine_types dict
    engine_types: dict[str, str] = {}
    for name in engine_names:
        engine_types[name] = data.draw(st.text(min_size=1, max_size=15))
    if adhoc_engine is not None and adhoc_engine not in engine_types:
        engine_types[adhoc_engine] = data.draw(st.text(min_size=1, max_size=15))

    bar = StatusBar(
        adhoc_engine=adhoc_engine,
        engine_names=engine_names,
        engine_types=engine_types,
    )
    text = bar._engine_text()

    if adhoc_engine is not None:
        etype = engine_types.get(adhoc_engine, "")
        suffix = f" ({etype})" if etype else ""
        assert text == f"⚙ {adhoc_engine}{suffix}"
    elif engine_names:
        name = engine_names[0]
        assert text == f"⚙ {name} (default)"
    else:
        assert text == "⚙ no engine"


def test_engine_text_no_engines() -> None:
    bar = StatusBar()
    assert bar._engine_text() == "⚙ no engine"


def test_engine_text_adhoc_override() -> None:
    bar = StatusBar(
        adhoc_engine="duckdb",
        engine_names=["polars", "duckdb"],
        engine_types={"polars": "polars", "duckdb": "duckdb"},
    )
    assert bar._engine_text() == "⚙ duckdb (duckdb)"


def test_engine_text_default_engine() -> None:
    bar = StatusBar(
        engine_names=["duckdb", "polars"],
        engine_types={"duckdb": "duckdb", "polars": "polars"},
    )
    assert bar._engine_text() == "⚙ duckdb (default)"


def test_set_engine_updates_state() -> None:
    bar = StatusBar()
    assert bar._engine_text() == "⚙ no engine"
    bar.set_engine(adhoc_engine="spark", engine_types={"spark": "pyspark"})
    assert bar._engine_text() == "⚙ spark (pyspark)"
    bar.set_engine(
        adhoc_engine=None,
        engine_names=["duckdb"],
        engine_types={"duckdb": "duckdb"},
    )
    assert bar._engine_text() == "⚙ duckdb (default)"


def test_legacy_engine_kwarg_compat() -> None:
    """The old `engine='duckdb'` kwarg still works via adhoc_engine fallback."""
    bar = StatusBar(engine="duckdb")
    assert bar._engine_text() == "⚙ duckdb"
