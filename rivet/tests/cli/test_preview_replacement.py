"""Tests for preview replacement logic — task 9.3.

# Feature: repl-state-improvements, Requirement 5.7
When the user executes a query or uses explicit preview (F4), the
Results_Panel replaces the cursor-driven preview with the full result.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa

from rivet_cli.repl.widgets.results import ViewMode, _ResultsPanelState
from rivet_core.interactive.types import JointPreviewData, QueryPlan, QueryResult, SchemaField
from rivet_core.models import Joint

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_panel() -> _ResultsPanelState:
    panel = _ResultsPanelState()
    panel._init_state()
    return panel


def _make_preview() -> JointPreviewData:
    return JointPreviewData(
        joint_name="my_joint",
        engine="duckdb",
        fusion_group=None,
        upstream=["src_a"],
        tags=[],
        schema=[SchemaField(name="id", type="int64")],
        preview_rows=None,
    )


def _make_query_result(n: int = 3) -> QueryResult:
    table = pa.table({"id": list(range(n)), "val": [f"v{i}" for i in range(n)]})
    src = Joint(name="src", joint_type="source", sql="SELECT 1", upstream=[])
    qj = Joint(name="__repl_query__", joint_type="sql", sql="SELECT 1", upstream=[])
    sink = Joint(name="__repl_sink__", joint_type="sink", sql="SELECT 1", upstream=[])
    plan = QueryPlan(
        sources=[src], query_joint=qj, sink=sink,
        resolved_references={}, assembly=MagicMock(),
    )
    return QueryResult(
        table=table, row_count=n, column_names=["id", "val"],
        column_types=["int64", "string"], elapsed_ms=10.0,
        query_plan=plan, quality_results=None, truncated=False,
    )


# ---------------------------------------------------------------------------
# Requirement 5.7: preview replaced by query result
# ---------------------------------------------------------------------------


class TestPreviewReplacedByQueryResult:
    """show_query_result replaces cursor-driven preview (Requirement 5.7)."""

    def test_show_query_result_clears_preview_mode(self) -> None:
        panel = _make_panel()
        panel.show_joint_preview(_make_preview())
        assert panel.view_mode == ViewMode.PREVIEW

        panel.show_query_result(_make_query_result())

        assert panel.view_mode == ViewMode.DATA

    def test_show_query_result_clears_preview_data(self) -> None:
        panel = _make_panel()
        panel.show_joint_preview(_make_preview())
        assert panel._preview_data is not None

        panel.show_query_result(_make_query_result())

        assert panel._preview_data is None

    def test_show_query_result_populates_result_set(self) -> None:
        panel = _make_panel()
        panel.show_joint_preview(_make_preview())

        panel.show_query_result(_make_query_result(5))

        assert len(panel.result_sets) == 1
        assert panel.active_result.row_count == 5

    def test_show_error_after_preview_sets_error_mode(self) -> None:
        """Query failure also replaces preview."""
        panel = _make_panel()
        panel.show_joint_preview(_make_preview())

        panel.show_error("execution failed")

        assert panel.view_mode == ViewMode.ERROR

    def test_preview_data_not_cleared_by_show_error(self) -> None:
        """show_error doesn't need to clear preview_data — view mode change is sufficient."""
        panel = _make_panel()
        panel.show_joint_preview(_make_preview())

        panel.show_error("fail")

        # view mode is ERROR, not PREVIEW — preview is effectively replaced
        assert panel.view_mode != ViewMode.PREVIEW
