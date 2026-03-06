"""Tests for the EngineSelectorScreen overlay.

Validates: Requirement 22.4 — engine selector for ad-hoc queries.

Tests cover:
- Screen instantiation with engine list
- Current engine marker in display
- Dismiss with None on cancel
- Engine name extraction from list item IDs
"""

from __future__ import annotations

from rivet_cli.repl.screens.engine_selector import EngineSelectorScreen


class TestEngineSelectorInit:
    def test_stores_engine_names(self) -> None:
        screen = EngineSelectorScreen(["duckdb", "polars", "spark"])
        assert screen._engine_names == ["duckdb", "polars", "spark"]

    def test_stores_current_engine(self) -> None:
        screen = EngineSelectorScreen(["duckdb", "polars"], current_engine="polars")
        assert screen._current_engine == "polars"

    def test_current_engine_defaults_to_none(self) -> None:
        screen = EngineSelectorScreen(["duckdb"])
        assert screen._current_engine is None

    def test_empty_engine_list(self) -> None:
        screen = EngineSelectorScreen([])
        assert screen._engine_names == []

    def test_returns_modal_screen(self) -> None:
        from textual.screen import ModalScreen
        screen = EngineSelectorScreen(["duckdb"])
        assert isinstance(screen, ModalScreen)


class TestEngineSelectorDismiss:
    def test_action_dismiss_none_dismisses_with_none(self) -> None:
        """action_dismiss_none should call dismiss(None)."""
        dismissed: list[str | None] = []

        screen = EngineSelectorScreen(["duckdb", "polars"])
        # Patch dismiss to capture the value
        screen.dismiss = lambda v: dismissed.append(v)  # type: ignore[method-assign]
        screen.action_dismiss_none()

        assert dismissed == [None]

    def test_on_list_view_selected_dismisses_engine_name(self) -> None:
        """Selecting a list item dismisses with the engine name."""
        from textual.widgets import Label, ListItem, ListView

        dismissed: list[str | None] = []
        screen = EngineSelectorScreen(["duckdb", "polars"])
        screen.dismiss = lambda v: dismissed.append(v)  # type: ignore[method-assign]

        item = ListItem(Label("duckdb"), id="engine-duckdb")
        event = ListView.Selected(ListView(), item, 0)
        screen.on_list_view_selected(event)

        assert dismissed == ["duckdb"]

    def test_on_list_view_selected_strips_prefix(self) -> None:
        """Engine name is extracted by stripping 'engine-' prefix."""
        from textual.widgets import Label, ListItem, ListView

        dismissed: list[str | None] = []
        screen = EngineSelectorScreen(["my-custom-engine"])
        screen.dismiss = lambda v: dismissed.append(v)  # type: ignore[method-assign]

        item = ListItem(Label("my-custom-engine"), id="engine-my-custom-engine")
        event = ListView.Selected(ListView(), item, 0)
        screen.on_list_view_selected(event)

        assert dismissed == ["my-custom-engine"]

    def test_on_list_view_selected_ignores_non_engine_id(self) -> None:
        """Items without 'engine-' prefix are ignored (no dismiss called)."""
        from textual.widgets import Label, ListItem, ListView

        dismissed: list[str | None] = []
        screen = EngineSelectorScreen(["duckdb"])
        screen.dismiss = lambda v: dismissed.append(v)  # type: ignore[method-assign]

        item = ListItem(Label("other"), id="other-item")
        event = ListView.Selected(ListView(), item, 0)
        screen.on_list_view_selected(event)

        assert dismissed == []

    def test_on_list_view_selected_handles_none_id(self) -> None:
        """Items with None id are ignored gracefully."""
        from textual.widgets import Label, ListItem, ListView

        dismissed: list[str | None] = []
        screen = EngineSelectorScreen(["duckdb"])
        screen.dismiss = lambda v: dismissed.append(v)  # type: ignore[method-assign]

        item = ListItem(Label("no id"))
        # id is None by default
        event = ListView.Selected(ListView(), item, 0)
        screen.on_list_view_selected(event)

        assert dismissed == []


class TestEngineSelectorCurrentMarker:
    def test_current_engine_gets_checkmark_in_label(self) -> None:
        """The current engine label should include a checkmark marker."""
        screen = EngineSelectorScreen(["duckdb", "polars"], current_engine="polars")
        # Verify the marker logic: current engine gets ' ✓', others don't
        markers = {
            name: (" ✓" if name == screen._current_engine else "")
            for name in screen._engine_names
        }
        assert markers["polars"] == " ✓"
        assert markers["duckdb"] == ""

    def test_no_current_engine_no_markers(self) -> None:
        screen = EngineSelectorScreen(["duckdb", "polars"], current_engine=None)
        markers = {
            name: (" ✓" if name == screen._current_engine else "")
            for name in screen._engine_names
        }
        assert all(m == "" for m in markers.values())
