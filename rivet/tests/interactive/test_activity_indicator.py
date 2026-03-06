# Feature: repl-ux-improvements, Property 9: Activity indicator label mapping
"""Property test: activity indicator label mapping.

Validates: Requirements 9b.1, 9b.2, 9b.3
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.widgets.status_bar import ActivityChanged, StatusBar
from rivet_core.interactive.types import Activity_State

_EXPECTED_LABELS = {
    Activity_State.IDLE: "",
    Activity_State.COMPILING: "⟳ Compiling…",
    Activity_State.EXECUTING: "⟳ Executing…",
}


@settings(max_examples=100)
@given(state=st.sampled_from(list(Activity_State)))
def test_activity_indicator_label_mapping(state: Activity_State) -> None:
    """For any Activity_State, the label matches the expected mapping."""
    bar = StatusBar()
    msg = ActivityChanged(state)
    bar.on_activity_changed(msg)
    assert bar._activity_label == _EXPECTED_LABELS[state]


def test_idle_hides_indicator() -> None:
    bar = StatusBar()
    bar.on_activity_changed(ActivityChanged(Activity_State.IDLE))
    assert bar._activity_label == ""


def test_compiling_shows_label() -> None:
    bar = StatusBar()
    bar.on_activity_changed(ActivityChanged(Activity_State.COMPILING))
    assert bar._activity_label == "⟳ Compiling…"


def test_executing_shows_label() -> None:
    bar = StatusBar()
    bar.on_activity_changed(ActivityChanged(Activity_State.EXECUTING))
    assert bar._activity_label == "⟳ Executing…"
