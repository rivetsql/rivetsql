"""Tests for the ResultsPanel widget.

Validates: Requirements 12.1–12.7, 13.1–13.3, 13.5, 13.7, 14.1, 15.1–15.5, 39.5

Tests cover:
- Data model types (ViewMode, SortDirection, SortState, ResultSet)
- show_query_result populates result sets
- show_multiple_results creates multiple result set tabs
- show_error sets error state
- clear resets to empty state
- Pin/unpin lifecycle (Property 28: pinning replaces previous pin)
- View mode transitions (data, diff, profile, plan, error)
- Result set switching
- Empty/zero-row/error states
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa

from rivet_cli.repl.widgets.results import (
    ResultSet,
    SortDirection,
    SortState,
    ViewMode,
    _ResultsPanelState,
)
from rivet_core.interactive.types import (
    QueryPlan,
    QueryResult,
    Verbosity,
)
from rivet_core.models import Joint

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_table(n: int = 3) -> pa.Table:
    return pa.table({"id": list(range(n)), "name": [f"row_{i}" for i in range(n)]})


def _make_joint(name: str, jtype: str = "sql") -> Joint:
    return Joint(name=name, joint_type=jtype, sql="SELECT 1", upstream=[])


def _make_query_plan() -> QueryPlan:
    src = _make_joint("src_orders", "source")
    qj = _make_joint("__repl_query__", "sql")
    sink = _make_joint("__repl_sink__", "sink")
    asm = MagicMock()
    return QueryPlan(
        sources=[src],
        query_joint=qj,
        sink=sink,
        resolved_references={"orders": "joint:src_orders"},
        assembly=asm,
    )


def _make_query_result(n: int = 3, elapsed_ms: float = 42.0, truncated: bool = False) -> QueryResult:
    table = _make_table(n)
    return QueryResult(
        table=table,
        row_count=n,
        column_names=["id", "name"],
        column_types=["int64", "string"],
        elapsed_ms=elapsed_ms,
        query_plan=_make_query_plan(),
        quality_results=None,
        truncated=truncated,
    )


def _make_panel(**kwargs: object) -> _ResultsPanelState:
    """Create a _ResultsPanelState instance for testing pure state logic."""
    panel = _ResultsPanelState()
    panel._init_state(
        session=kwargs.get("session"),
        max_results=kwargs.get("max_results", 10_000),
    )
    return panel


# ---------------------------------------------------------------------------
# Data model tests
# ---------------------------------------------------------------------------


class TestViewMode:
    def test_all_modes_exist(self) -> None:
        assert ViewMode.DATA.value == "data"
        assert ViewMode.DIFF.value == "diff"
        assert ViewMode.PROFILE.value == "profile"
        assert ViewMode.PLAN.value == "plan"
        assert ViewMode.ERROR.value == "error"


class TestSortDirection:
    def test_values(self) -> None:
        assert SortDirection.NONE.value == "none"
        assert SortDirection.ASC.value == "asc"
        assert SortDirection.DESC.value == "desc"


class TestSortState:
    def test_default(self) -> None:
        s = SortState()
        assert s.column is None
        assert s.direction == SortDirection.NONE

    def test_with_values(self) -> None:
        s = SortState(column="id", direction=SortDirection.ASC)
        assert s.column == "id"
        assert s.direction == SortDirection.ASC


class TestResultSet:
    def test_basic(self) -> None:
        t = _make_table()
        rs = ResultSet(
            table=t,
            column_names=["id", "name"],
            column_types=["int64", "string"],
            row_count=3,
            elapsed_ms=10.0,
        )
        assert rs.row_count == 3
        assert rs.column_names == ["id", "name"]
        assert rs.error is None

    def test_error_result_set(self) -> None:
        rs = ResultSet(
            table=None,
            column_names=[],
            column_types=[],
            row_count=0,
            elapsed_ms=0.0,
            error="connection refused",
        )
        assert rs.error == "connection refused"
        assert rs.table is None


# ---------------------------------------------------------------------------
# ResultsPanel state management tests
# ---------------------------------------------------------------------------


class TestShowQueryResult:
    def test_populates_result_set(self) -> None:
        panel = _make_panel()
        qr = _make_query_result(5)
        panel.show_query_result(qr)
        assert len(panel.result_sets) == 1
        assert panel.active_result is not None
        assert panel.active_result.row_count == 5

    def test_resets_view_mode_to_data(self) -> None:
        panel = _make_panel()
        panel._view_mode = ViewMode.ERROR
        panel.show_query_result(_make_query_result())
        assert panel.view_mode == ViewMode.DATA

    def test_resets_sort_state(self) -> None:
        panel = _make_panel()
        panel._sort_state = SortState(column="id", direction=SortDirection.ASC)
        panel.show_query_result(_make_query_result())
        assert panel.sort_state.column is None

    def test_increments_query_counter(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result())
        assert panel._query_counter == 1
        panel.show_query_result(_make_query_result())
        assert panel._query_counter == 2

    def test_clears_diff_and_profile(self) -> None:
        panel = _make_panel()
        panel._diff_result = "something"
        panel._profile_result = "something"
        panel.show_query_result(_make_query_result())
        assert panel._diff_result is None
        assert panel._profile_result is None

    def test_truncated_flag_preserved(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result(truncated=True))
        assert panel.active_result.truncated is True

    def test_column_types_preserved(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result())
        assert panel.active_result.column_types == ["int64", "string"]

    def test_query_plan_preserved(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result())
        assert panel.active_result.query_plan is not None


class TestShowMultipleResults:
    def test_creates_multiple_result_sets(self) -> None:
        panel = _make_panel()
        results = [_make_query_result(3), _make_query_result(5)]
        panel.show_multiple_results(results)
        assert len(panel.result_sets) == 2

    def test_active_index_starts_at_zero(self) -> None:
        panel = _make_panel()
        panel.show_multiple_results([_make_query_result(), _make_query_result()])
        assert panel._active_result_index == 0

    def test_resets_view_mode(self) -> None:
        panel = _make_panel()
        panel._view_mode = ViewMode.PROFILE
        panel.show_multiple_results([_make_query_result()])
        assert panel.view_mode == ViewMode.DATA


class TestShowError:
    def test_sets_error_view_mode(self) -> None:
        panel = _make_panel()
        panel.show_error("connection refused")
        assert panel.view_mode == ViewMode.ERROR

    def test_stores_error_in_result_set(self) -> None:
        panel = _make_panel()
        panel.show_error("timeout")
        assert panel.active_result is not None
        assert panel.active_result.error == "timeout"

    def test_error_result_has_no_table(self) -> None:
        panel = _make_panel()
        panel.show_error("fail")
        assert panel.active_result.table is None


class TestClear:
    def test_resets_result_sets(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result())
        panel.clear()
        assert len(panel.result_sets) == 0
        assert panel.active_result is None

    def test_resets_view_mode(self) -> None:
        panel = _make_panel()
        panel._view_mode = ViewMode.DIFF
        panel.clear()
        assert panel.view_mode == ViewMode.DATA

    def test_clears_diff_and_profile(self) -> None:
        panel = _make_panel()
        panel._diff_result = "x"
        panel._profile_result = "y"
        panel.clear()
        assert panel._diff_result is None
        assert panel._profile_result is None


class TestSwitchResultSet:
    def test_switches_active_index(self) -> None:
        panel = _make_panel()
        panel.show_multiple_results([_make_query_result(1), _make_query_result(2)])
        panel.switch_result_set(1)
        assert panel._active_result_index == 1
        assert panel.active_result.row_count == 2

    def test_ignores_out_of_range(self) -> None:
        panel = _make_panel()
        panel.show_multiple_results([_make_query_result()])
        panel.switch_result_set(5)
        assert panel._active_result_index == 0

    def test_ignores_negative(self) -> None:
        panel = _make_panel()
        panel.show_multiple_results([_make_query_result()])
        panel.switch_result_set(-1)
        assert panel._active_result_index == 0


# ---------------------------------------------------------------------------
# Pin/Unpin tests — Property 28: Pinning replaces previous pin
# ---------------------------------------------------------------------------


class TestPinning:
    """# Feature: cli-repl, Property 28: Pinning replaces previous pin"""

    def test_pin_stores_table(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result(3))
        panel.pin()
        assert panel.pinned_table is not None
        assert panel.pinned_table.num_rows == 3

    def test_pin_sets_label(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result(3))
        panel.pin()
        assert "3 rows" in panel.pinned_label

    def test_pin_replaces_previous_pin(self) -> None:
        """Property 28: Only the most recently pinned table is retained."""
        panel = _make_panel()
        panel.show_query_result(_make_query_result(3))
        panel.pin()
        first_table = panel.pinned_table

        panel.show_query_result(_make_query_result(7))
        panel.pin()
        assert panel.pinned_table is not first_table
        assert panel.pinned_table.num_rows == 7
        assert "7 rows" in panel.pinned_label

    def test_unpin_clears_table(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result())
        panel.pin()
        panel.unpin()
        assert panel.pinned_table is None
        assert panel.pinned_label == ""

    def test_unpin_clears_diff_result(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result())
        panel.pin()
        panel._diff_result = "something"
        panel.unpin()
        assert panel._diff_result is None

    def test_unpin_resets_diff_view_to_data(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result())
        panel.pin()
        panel._view_mode = ViewMode.DIFF
        panel.unpin()
        assert panel.view_mode == ViewMode.DATA

    def test_pin_no_result_is_noop(self) -> None:
        panel = _make_panel()
        panel.pin()
        assert panel.pinned_table is None

    def test_pin_error_result_is_noop(self) -> None:
        panel = _make_panel()
        panel.show_error("fail")
        panel.pin()
        assert panel.pinned_table is None

    def test_multiple_pins_only_keeps_last(self) -> None:
        """Property 28: Sequence of pins retains only the last."""
        panel = _make_panel()
        tables = []
        for n in [2, 4, 6]:
            panel.show_query_result(_make_query_result(n))
            panel.pin()
            tables.append(panel.pinned_table)
        # Only the last pin should be retained
        assert panel.pinned_table.num_rows == 6
        assert "6 rows" in panel.pinned_label


# ---------------------------------------------------------------------------
# Empty / zero-row / error state tests
# ---------------------------------------------------------------------------


class TestEmptyStates:
    def test_initial_state_no_results(self) -> None:
        panel = _make_panel()
        assert panel.active_result is None
        assert panel.view_mode == ViewMode.DATA

    def test_zero_row_result(self) -> None:
        """Requirement 12.5: zero-row state shows header with columns, empty body."""
        table = pa.table({"id": pa.array([], type=pa.int64()), "name": pa.array([], type=pa.string())})
        qr = QueryResult(
            table=table,
            row_count=0,
            column_names=["id", "name"],
            column_types=["int64", "string"],
            elapsed_ms=5.0,
            query_plan=_make_query_plan(),
            quality_results=None,
            truncated=False,
        )
        panel = _make_panel()
        panel.show_query_result(qr)
        assert panel.active_result.row_count == 0
        assert panel.active_result.column_names == ["id", "name"]

    def test_error_state(self) -> None:
        """Requirement 12.6: error state shows red error message."""
        panel = _make_panel()
        panel.show_error("engine timeout after 30s")
        assert panel.view_mode == ViewMode.ERROR
        assert panel.active_result.error == "engine timeout after 30s"


# ---------------------------------------------------------------------------
# View mode transition tests
# ---------------------------------------------------------------------------


class TestViewModeTransitions:
    def test_show_result_sets_data_mode(self) -> None:
        panel = _make_panel()
        panel.show_query_result(_make_query_result())
        assert panel.view_mode == ViewMode.DATA

    def test_show_error_sets_error_mode(self) -> None:
        panel = _make_panel()
        panel.show_error("fail")
        assert panel.view_mode == ViewMode.ERROR

    def test_show_result_after_error_resets_to_data(self) -> None:
        panel = _make_panel()
        panel.show_error("fail")
        panel.show_query_result(_make_query_result())
        assert panel.view_mode == ViewMode.DATA


# ---------------------------------------------------------------------------
# INSPECT view mode tests (Requirement 10.1, 10.2, 10.3)
# ---------------------------------------------------------------------------


def _make_inspection(total_joints: int = 5, verbosity_value: str = "normal") -> MagicMock:
    """Create a mock AssemblyInspection for testing."""
    inspection = MagicMock()

    # Verbosity
    inspection.verbosity = Verbosity(verbosity_value)

    # Overview
    ov = MagicMock()
    ov.profile_name = "dev"
    ov.joint_counts = {"source": 2, "sql": 2, "sink": 1}
    ov.total_joints = total_joints
    ov.fused_group_count = 1
    ov.materialization_count = 1
    ov.engines = [MagicMock(name="duckdb", engine_type="duckdb", joint_count=5)]
    ov.catalogs = [MagicMock(name="main", type="duckdb")]
    ov.adapters = []
    ov.success = True
    ov.warnings = []
    ov.errors = []
    inspection.overview = ov

    # Execution order
    eo = MagicMock()
    step = MagicMock()
    step.step_number = 1
    step.id = "group-1"
    step.engine = "duckdb"
    step.joints = ["src", "transform"]
    step.is_fused = True
    step.has_materialization = False
    eo.steps = [step]
    inspection.execution_order = eo

    # Fused groups / materializations / dag / joint_details — None for NORMAL
    inspection.fused_groups = None
    inspection.materializations = None
    inspection.dag = None
    inspection.joint_details = None
    inspection.filter_applied = None

    return inspection


class TestInspectViewMode:
    def test_inspect_enum_value(self) -> None:
        assert ViewMode.INSPECT.value == "inspect"

    def test_show_inspection_sets_inspect_mode(self) -> None:
        panel = _make_panel()
        inspection = _make_inspection()
        panel.show_inspect_result(inspection)
        assert panel.view_mode == ViewMode.INSPECT
        assert panel._inspect_result is inspection

    def test_show_inspection_stores_inspection(self) -> None:
        panel = _make_panel()
        inspection = _make_inspection()
        panel.show_inspect_result(inspection)
        assert panel._inspect_result is inspection

    def test_clear_resets_inspection(self) -> None:
        panel = _make_panel()
        panel.show_inspect_result(_make_inspection())
        panel.clear()
        assert panel._inspect_result is None
        assert panel.view_mode == ViewMode.DATA

    def test_show_result_after_inspect_resets_to_data(self) -> None:
        panel = _make_panel()
        panel.show_inspect_result(_make_inspection())
        panel.show_query_result(_make_query_result())
        assert panel.view_mode == ViewMode.DATA

    def test_initial_inspection_is_none(self) -> None:
        panel = _make_panel()
        assert panel._inspect_result is None
