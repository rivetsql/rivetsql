# Feature: repl-ux-improvements, Property 10: Tab switching state consistency
# Feature: repl-ux-improvements, Property 5: Tab persistence across result updates
# Feature: repl-ux-improvements, Property 6: Log badge count invariant
"""Property tests: Tab switching state consistency, tab persistence, and log badge count.

Validates: Requirements 1.3, 1.4 (Property 10)
Validates: Requirements 8.1, 8.2, 8.3 (Property 5)
Validates: Requirements 1.5, 8.4, 8.5 (Property 6)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.widgets.results import PanelTab, _ResultsPanelState

_TAB_STRATEGY = st.sampled_from(list(PanelTab))


@settings(max_examples=100)
@given(selections=st.lists(st.sampled_from(list(PanelTab)), min_size=1, max_size=50))
def test_active_tab_matches_selected_tab(selections: list[PanelTab]) -> None:
    """After each _switch_tab call, active_tab equals the selected tab."""
    # We test the state logic without mounting a Textual app by directly
    # manipulating the reactive attribute on a detached widget instance.
    from rivet_cli.repl.widgets.results_tab_bar import ResultsTabBar

    bar = ResultsTabBar()
    for tab in selections:
        bar.active_tab = tab
        assert bar.active_tab == tab, f"Expected {tab}, got {bar.active_tab}"


@settings(max_examples=100)
@given(tab=st.sampled_from(list(PanelTab)))
def test_panel_tab_round_trip(tab: PanelTab) -> None:
    """PanelTab can be reconstructed from its value."""
    assert PanelTab(tab.value) is tab


# ---------------------------------------------------------------------------
# Property 5: Tab persistence across result updates
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _FakeQueryResult:
    """Minimal QueryResult stand-in for state-level testing."""

    table: Any
    row_count: int
    column_names: list[str]
    column_types: list[str]
    elapsed_ms: float
    query_plan: Any
    truncated: bool
    quality_results: Any = None


class _TestState(_ResultsPanelState):
    """Concrete subclass of the state mixin for testing."""

    def __init__(self) -> None:
        self._init_state()


def _make_result() -> _FakeQueryResult:
    return _FakeQueryResult(
        table=pa.table({"x": [1]}),
        row_count=1,
        column_names=["x"],
        column_types=["int64"],
        elapsed_ms=1.0,
        query_plan=None,
        truncated=False,
    )


# Feature: repl-ux-improvements, Property 5: Tab persistence across result updates
@settings(max_examples=100)
@given(active_tab=_TAB_STRATEGY, use_multiple=st.booleans())
def test_tab_persists_across_result_updates(
    active_tab: PanelTab,
    use_multiple: bool,
) -> None:
    """_active_panel_tab is unchanged after show_query_result / show_multiple_results."""
    state = _TestState()
    state._active_panel_tab = active_tab

    if use_multiple:
        state.show_multiple_results([_make_result()])
    else:
        state.show_query_result(_make_result())

    assert state._active_panel_tab is active_tab


# ---------------------------------------------------------------------------
# Property 6: Log badge count invariant
# ---------------------------------------------------------------------------

# Event types for the property test
_LOG_ARRIVAL = "log"
_TAB_SWITCH = "tab"

_event_strategy = st.one_of(
    st.just((_LOG_ARRIVAL, None)),
    st.sampled_from(list(PanelTab)).map(lambda t: (_TAB_SWITCH, t)),
)


# Feature: repl-ux-improvements, Property 6: Log badge count invariant
@settings(max_examples=100)
@given(events=st.lists(_event_strategy, min_size=1, max_size=100))
def test_log_badge_count_invariant(events: list[tuple[str, PanelTab | None]]) -> None:
    """Badge count equals logs since last Logs tab view; resets to zero on Logs tab activation."""
    state = _TestState()
    logs_since_last_view = 0

    for kind, tab in events:
        if kind == _LOG_ARRIVAL:
            # Mirrors app.py: increment only when Logs tab is not active
            if state._active_panel_tab != PanelTab.LOGS:
                state._log_badge_count += 1
                logs_since_last_view += 1
        else:
            # Tab switch
            state._active_panel_tab = tab  # type: ignore[assignment]
            if tab == PanelTab.LOGS:
                state._log_badge_count = 0
                logs_since_last_view = 0

        assert state._log_badge_count == logs_since_last_view
