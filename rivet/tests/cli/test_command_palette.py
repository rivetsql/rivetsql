"""Tests for the Command Palette screen.

Validates Requirements 17.1, 17.2, 17.3, 17.4:
  - Fuzzy-searchable overlay with all REPL commands and shortcuts
  - Substring and fuzzy matching
  - Recently used commands appear first
  - Full command list present
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.screens.command_palette import (
    COMMANDS,
    CommandPaletteState,
    _fuzzy_score,
)

# ---------------------------------------------------------------------------
# _fuzzy_score unit tests
# ---------------------------------------------------------------------------


class TestFuzzyScore:
    def test_empty_query_matches_all(self):
        assert _fuzzy_score("", "anything") == 3

    def test_exact_match(self):
        assert _fuzzy_score("quit", "quit") == 3

    def test_substring_match(self):
        assert _fuzzy_score("run", "Run Current Joint") == 2

    def test_fuzzy_match(self):
        # "rcj" matches "Run Current Joint" via subsequence
        assert _fuzzy_score("rcj", "Run Current Joint") == 1

    def test_no_match(self):
        assert _fuzzy_score("zzz", "Run Current Joint") is None

    def test_case_insensitive(self):
        assert _fuzzy_score("QUIT", "Quit") == 3
        assert _fuzzy_score("RUN", "Run All Sinks") == 2


# ---------------------------------------------------------------------------
# Requirement 17.4: Full command list
# ---------------------------------------------------------------------------


EXPECTED_COMMANDS = {
    "compile_project",
    "compile_current",
    "run_all_sinks",
    "run_current_query",
    "run_tests",
    "switch_profile",
    "toggle_catalog",
    "toggle_results",
    "new_tab",
    "open_file",
    "save",
    "export_data",
    "find",
    "format_sql",
    "goto_joint",
    "search_catalog",
    "pin_result",
    "unpin_result",
    "diff_results",
    "profile_data",
    "show_query_plan",
    "doctor",
    "refresh_catalogs",
    "view_history",
    "settings",
    "help",
    "quit",
    "debug_pipeline",
}


def test_all_required_commands_present():
    """Requirement 17.4: All required commands are in the registry."""
    actions = {cmd.action for cmd in COMMANDS}
    assert actions >= EXPECTED_COMMANDS


def test_commands_have_names_and_actions():
    """Every command has a non-empty name and action."""
    for cmd in COMMANDS:
        assert cmd.name, f"Command {cmd.action!r} has empty name"
        assert cmd.action, f"Command {cmd.name!r} has empty action"


# ---------------------------------------------------------------------------
# Requirement 17.2: Substring and fuzzy matching
# ---------------------------------------------------------------------------


class TestCommandPaletteSearch:
    def test_empty_query_returns_all(self):
        state = CommandPaletteState()
        results = state.search("")
        assert len(results) == len(COMMANDS)

    def test_substring_match(self):
        state = CommandPaletteState()
        results = state.search("run")
        names = [r.name for r in results]
        assert any("Run" in n for n in names)

    def test_fuzzy_match(self):
        state = CommandPaletteState()
        # "fmt" should fuzzy-match "Format SQL"
        results = state.search("fmt")
        actions = [r.action for r in results]
        assert "format_sql" in actions

    def test_no_match_returns_empty(self):
        state = CommandPaletteState()
        results = state.search("xyzzy_no_match_ever")
        assert results == []

    def test_substring_ranked_above_fuzzy(self):
        state = CommandPaletteState()
        # "run" is a substring of "Run All Sinks" (score 2)
        # but only fuzzy-matches something like "Refresh Catalogs" (r-u-n not in order)
        results = state.search("run")
        # All results with "run" as substring should come before pure fuzzy matches
        scores = [_fuzzy_score("run", r.name) for r in results]
        # Verify scores are non-increasing (sorted best-first)
        for i in range(len(scores) - 1):
            assert scores[i] is not None
            assert scores[i + 1] is not None
            assert scores[i] >= scores[i + 1]  # type: ignore[operator]

    def test_case_insensitive_search(self):
        state = CommandPaletteState()
        lower = state.search("quit")
        upper = state.search("QUIT")
        assert [r.action for r in lower] == [r.action for r in upper]


# ---------------------------------------------------------------------------
# Requirement 17.3: Recently used commands first
# ---------------------------------------------------------------------------


class TestRecentlyUsed:
    def test_record_used_moves_to_front(self):
        state = CommandPaletteState()
        state.record_used("quit")
        state.record_used("save")
        assert state._recent[0] == "save"
        assert state._recent[1] == "quit"

    def test_record_used_deduplicates(self):
        state = CommandPaletteState()
        state.record_used("quit")
        state.record_used("save")
        state.record_used("quit")  # re-use
        assert state._recent.count("quit") == 1
        assert state._recent[0] == "quit"

    def test_recent_commands_appear_first_in_search(self):
        state = CommandPaletteState()
        # Mark "save" as recently used
        state.record_used("save")
        results = state.search("")
        # "save" should be first (or at least before non-recent commands)
        assert results[0].action == "save"

    def test_multiple_recent_commands_ordered_by_recency(self):
        state = CommandPaletteState()
        state.record_used("quit")
        state.record_used("save")
        state.record_used("find")
        results = state.search("")
        actions = [r.action for r in results[:3]]
        assert actions == ["find", "save", "quit"]

    def test_recent_within_same_score_tier(self):
        """Within the same fuzzy score tier, recent commands come first."""
        state = CommandPaletteState()
        # Both "Run All Sinks" and "Run Current Joint" match "run" as substring
        state.record_used("run_all_sinks")
        results = state.search("run")
        run_actions = [r.action for r in results if r.action in ("run_all_sinks", "run_current_query")]
        assert run_actions[0] == "run_all_sinks"


# ---------------------------------------------------------------------------
# Property tests
# ---------------------------------------------------------------------------


@given(query=st.text(min_size=0, max_size=20))
@settings(max_examples=200)
def test_search_always_returns_subset_of_commands(query: str) -> None:
    """search() always returns a subset of the full command list."""
    state = CommandPaletteState()
    results = state.search(query)
    all_actions = {cmd.action for cmd in COMMANDS}
    for r in results:
        assert r.action in all_actions


@given(query=st.text(min_size=0, max_size=20))
@settings(max_examples=200)
def test_search_results_have_no_duplicates(query: str) -> None:
    """search() never returns duplicate commands."""
    state = CommandPaletteState()
    results = state.search(query)
    actions = [r.action for r in results]
    assert len(actions) == len(set(actions))


@given(
    actions=st.lists(
        st.sampled_from([cmd.action for cmd in COMMANDS]),
        min_size=1,
        max_size=5,
    )
)
@settings(max_examples=100)
def test_most_recently_used_is_first_in_empty_search(actions: list[str]) -> None:
    """After recording uses, the most recently used command is first in empty search."""
    state = CommandPaletteState()
    for action in actions:
        state.record_used(action)
    results = state.search("")
    assert results[0].action == actions[-1]
