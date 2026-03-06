"""Property 13: Fuzzy matching subsequence.

For any query string that is a subsequence of a completion label,
CompletionEngine.complete() must include that completion in its results.

Validates: Requirement 8.7
"""

from __future__ import annotations

import string

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.completions import CompletionEngine, _fuzzy_match

# ── Strategies ────────────────────────────────────────────────────────────────

_identifier_chars = string.ascii_lowercase + string.digits + "_"

_identifier = st.text(
    alphabet=_identifier_chars,
    min_size=2,
    max_size=20,
).filter(lambda s: s[0].isalpha())


def _subsequence_of(label: str) -> st.SearchStrategy[str]:
    """Generate a non-empty subsequence of label."""
    if not label:
        return st.just("")
    indices = st.lists(
        st.integers(min_value=0, max_value=len(label) - 1),
        min_size=1,
        max_size=min(len(label), 5),
        unique=True,
    ).map(sorted)
    return indices.map(lambda idxs: "".join(label[i] for i in idxs))


# ── Unit tests for _fuzzy_match ───────────────────────────────────────────────

def test_fuzzy_match_exact():
    assert _fuzzy_match("users", "users") is not None


def test_fuzzy_match_subsequence():
    assert _fuzzy_match("usr", "users") is not None


def test_fuzzy_match_no_match():
    assert _fuzzy_match("xyz", "users") is None


def test_fuzzy_match_empty_query():
    # Empty query matches everything (returns empty positions list)
    result = _fuzzy_match("", "users")
    assert result == []


def test_fuzzy_match_positions_are_valid():
    positions = _fuzzy_match("usr", "user_summary")
    assert positions is not None
    assert all(0 <= p < len("user_summary") for p in positions)


# ── Property: any subsequence of a label is matched ──────────────────────────

@given(label=_identifier)
@settings(max_examples=200)
def test_fuzzy_match_any_subsequence_matches(label: str) -> None:
    """_fuzzy_match returns non-None for any non-empty subsequence of label."""
    # Build a subsequence by taking every other character (at least 1 char)
    subseq = label[::2] or label[:1]
    result = _fuzzy_match(subseq, label)
    assert result is not None, (
        f"Expected subsequence '{subseq}' to match label '{label}'"
    )


@given(label=_identifier)
@settings(max_examples=200)
def test_fuzzy_match_non_subsequence_does_not_match(label: str) -> None:
    """_fuzzy_match returns None when query chars are not a subsequence."""
    # Build a query that cannot be a subsequence: use chars not in label
    non_chars = [c for c in "!@#$%^&*()" if c not in label]
    if not non_chars:
        return  # skip if all special chars happen to be in label (won't happen for identifiers)
    query = non_chars[0] * 2
    result = _fuzzy_match(query, label)
    assert result is None, (
        f"Expected '{query}' NOT to match label '{label}'"
    )


# ── Property: CompletionEngine returns matches for subsequence queries ────────

@given(
    joint_name=_identifier,
    query=_identifier.flatmap(lambda n: _subsequence_of(n).map(lambda q: (n, q))),
)
@settings(max_examples=100)
def test_complete_returns_joint_for_subsequence_query(
    joint_name: str,
    query: tuple[str, str],
) -> None:
    """complete() includes a joint when the query is a subsequence of its name."""
    name, subseq = query
    engine = CompletionEngine()
    engine.update_assembly([{"name": name, "joint_type": "sql"}])

    results = engine.complete(subseq, len(subseq))
    labels = [c.label for c in results]
    assert name in labels, (
        f"Expected joint '{name}' in completions for query '{subseq}', got {labels}"
    )


@given(
    table_name=_identifier,
    query=_identifier.flatmap(lambda n: _subsequence_of(n).map(lambda q: (n, q))),
)
@settings(max_examples=100)
def test_complete_returns_catalog_table_for_subsequence_query(
    table_name: str,
    query: tuple[str, str],
) -> None:
    """complete() includes a catalog table when the query is a subsequence of its name."""
    name, subseq = query
    engine = CompletionEngine()
    engine.update_catalogs([
        {"catalog": "mycat", "schema": "public", "table": name}
    ])

    results = engine.complete(subseq, len(subseq))
    labels = [c.label for c in results]
    assert name in labels, (
        f"Expected table '{name}' in completions for query '{subseq}', got {labels}"
    )


# ── Property: non-subsequence queries do not match ───────────────────────────

def test_complete_no_match_for_non_subsequence() -> None:
    """complete() excludes items when query is not a subsequence of any label."""
    engine = CompletionEngine()
    engine.update_assembly([{"name": "users", "joint_type": "sql"}])
    engine.update_catalogs([{"catalog": "mycat", "schema": "public", "table": "orders"}])

    # Query with chars not present in either name
    results = engine.complete("zzz", 3)
    labels = [c.label for c in results]
    assert "users" not in labels
    assert "orders" not in labels


# ── Requirement 8.7 examples from spec ───────────────────────────────────────

@pytest.mark.parametrize("query,expected_label", [
    ("usr", "users"),
    ("usr", "user_summary"),
    ("usr", "raw_users"),
])
def test_spec_examples(query: str, expected_label: str) -> None:
    """Typing 'usr' matches 'users', 'user_summary', 'raw_users' (Req 8.7)."""
    engine = CompletionEngine()
    engine.update_assembly([
        {"name": "users", "joint_type": "sql"},
        {"name": "user_summary", "joint_type": "sql"},
        {"name": "raw_users", "joint_type": "sql"},
    ])
    results = engine.complete(query, len(query))
    labels = [c.label for c in results]
    assert expected_label in labels, (
        f"Expected '{expected_label}' in completions for query '{query}', got {labels}"
    )
