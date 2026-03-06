"""Tests for JointPreviewRequested handler in app.py — task 9.2.

# Feature: repl-state-improvements, Requirements 5.2, 5.3, 5.4, 5.6
"""

from __future__ import annotations

import pyarrow as pa

from rivet_cli.repl.widgets.results import ResultsPanel, ViewMode
from rivet_core.interactive.types import JointPreviewData, SchemaField


class TestShowJointPreview:
    """ResultsPanel.show_joint_preview sets PREVIEW view mode."""

    def test_show_joint_preview_sets_view_mode(self) -> None:
        panel = ResultsPanel()
        preview = JointPreviewData(
            joint_name="my_joint",
            engine="duckdb",
            fusion_group=None,
            upstream=["src_a"],
            tags=["tag1"],
            schema=[SchemaField(name="id", type="int64")],
            preview_rows=None,
        )
        panel.show_joint_preview(preview)
        assert panel.view_mode == ViewMode.PREVIEW
        assert panel._preview_data is preview

    def test_show_joint_preview_with_rows(self) -> None:
        panel = ResultsPanel()
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        preview = JointPreviewData(
            joint_name="my_joint",
            engine="duckdb",
            fusion_group="fg_1",
            upstream=["src_a", "src_b"],
            tags=[],
            schema=[
                SchemaField(name="id", type="int64"),
                SchemaField(name="name", type="utf8"),
            ],
            preview_rows=table,
        )
        panel.show_joint_preview(preview)
        assert panel.view_mode == ViewMode.PREVIEW
        assert panel._preview_data.preview_rows.num_rows == 3

    def test_show_joint_preview_no_rows(self) -> None:
        panel = ResultsPanel()
        preview = JointPreviewData(
            joint_name="my_joint",
            engine="spark",
            fusion_group=None,
            upstream=[],
            tags=["analytics"],
            schema=None,
            preview_rows=None,
        )
        panel.show_joint_preview(preview)
        assert panel._preview_data.preview_rows is None


class TestAppHandlerExists:
    """The app has a handler for CatalogPanel.JointPreviewRequested."""

    def test_handler_method_exists(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        assert hasattr(RivetRepl, "on_catalog_panel_joint_preview_requested")


class TestViewModePreviewExists:
    """ViewMode.PREVIEW is defined."""

    def test_preview_enum_value(self) -> None:
        assert ViewMode.PREVIEW.value == "preview"
