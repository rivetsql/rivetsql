"""Property tests for rivet_core.interactive.completions.

# Feature: cli-repl, Property 24: Autocomplete error graceful degradation
# Validates: Requirements 34.5
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.completions import CompletionEngine

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Arbitrary strings including malformed SQL, binary-like content, etc.
_any_string = st.text(min_size=0, max_size=500)

# Arbitrary cursor positions (may be out of bounds)
_any_cursor = st.integers(min_value=-1000, max_value=10000)

# Corrupted catalog entries: dicts with arbitrary/missing keys
_bad_catalog_entry = st.fixed_dictionaries({}).map(lambda d: d) | st.dictionaries(
    keys=st.text(min_size=0, max_size=20),
    values=st.one_of(st.none(), st.integers(), st.text(), st.lists(st.text())),
    min_size=0,
    max_size=5,
)

# Corrupted joint entries: same idea
_bad_joint_entry = st.fixed_dictionaries({}).map(lambda d: d) | st.dictionaries(
    keys=st.text(min_size=0, max_size=20),
    values=st.one_of(st.none(), st.integers(), st.text(), st.lists(st.text())),
    min_size=0,
    max_size=5,
)


# ---------------------------------------------------------------------------
# Property 24a: complete() never raises on arbitrary SQL + cursor
# ---------------------------------------------------------------------------

@given(sql=_any_string, cursor_pos=_any_cursor)
@settings(max_examples=200)
def test_complete_never_raises_on_arbitrary_input(sql: str, cursor_pos: int) -> None:
    """Property 24: complete() returns [] rather than raising for any input."""
    engine = CompletionEngine()
    result = engine.complete(sql, cursor_pos)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Property 24b: complete() never raises after corrupted catalog index
# ---------------------------------------------------------------------------

@given(
    bad_entries=st.lists(_bad_catalog_entry, min_size=0, max_size=10),
    sql=_any_string,
    cursor_pos=_any_cursor,
)
@settings(max_examples=100)
def test_complete_never_raises_after_corrupted_catalog(
    bad_entries: list[dict],
    sql: str,
    cursor_pos: int,
) -> None:
    """Property 24: complete() degrades gracefully after corrupted catalog state."""
    engine = CompletionEngine()
    # Attempt to load corrupted entries; update_catalogs may or may not raise —
    # what matters is that complete() never raises afterward.
    try:
        engine.update_catalogs(bad_entries)  # type: ignore[arg-type]
    except Exception:
        pass  # corruption during index build is acceptable
    result = engine.complete(sql, cursor_pos)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Property 24c: complete() never raises after corrupted joint index
# ---------------------------------------------------------------------------

@given(
    bad_entries=st.lists(_bad_joint_entry, min_size=0, max_size=10),
    sql=_any_string,
    cursor_pos=_any_cursor,
)
@settings(max_examples=100)
def test_complete_never_raises_after_corrupted_joints(
    bad_entries: list[dict],
    sql: str,
    cursor_pos: int,
) -> None:
    """Property 24: complete() degrades gracefully after corrupted joint state."""
    engine = CompletionEngine()
    try:
        engine.update_assembly(bad_entries)  # type: ignore[arg-type]
    except Exception:
        pass
    result = engine.complete(sql, cursor_pos)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Property 24d: complete_annotation() never raises on arbitrary input
# ---------------------------------------------------------------------------

@given(line=_any_string, cursor_pos=_any_cursor)
@settings(max_examples=100)
def test_complete_annotation_never_raises(line: str, cursor_pos: int) -> None:
    """Property 24: complete_annotation() degrades gracefully on any input."""
    engine = CompletionEngine()
    result = engine.complete_annotation(line, cursor_pos)
    assert isinstance(result, list)
