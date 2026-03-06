"""Tests for task 10.2: Ensure extracted SQL is executable.

# Feature: repl-state-improvements, Requirement 6.5
"""

from __future__ import annotations

from unittest.mock import MagicMock

from rivet_cli.repl.widgets.editor import EditorTab, TabKind


class TestNewAdHocTabSignature:
    """new_ad_hoc_tab accepts a content parameter."""

    def test_new_ad_hoc_tab_accepts_content_kwarg(self) -> None:
        import inspect

        from rivet_cli.repl.widgets.editor import EditorPanel

        sig = inspect.signature(EditorPanel.new_ad_hoc_tab)
        assert "content" in sig.parameters

    def test_content_parameter_default_is_empty_string(self) -> None:
        import inspect

        from rivet_cli.repl.widgets.editor import EditorPanel

        sig = inspect.signature(EditorPanel.new_ad_hoc_tab)
        assert sig.parameters["content"].default == ""


class TestEditorTabForExtractedSQL:
    """EditorTab created for extracted SQL is AD_HOC and not read-only."""

    def test_ad_hoc_tab_is_not_read_only(self) -> None:
        tab = EditorTab(
            kind=TabKind.AD_HOC,
            title="[my_joint] (extracted)",
            content="SELECT id FROM orders",
            read_only=False,
        )
        assert tab.kind == TabKind.AD_HOC
        assert tab.read_only is False
        assert tab.content == "SELECT id FROM orders"

    def test_preview_tab_is_read_only(self) -> None:
        """Contrast: PREVIEW tabs are read-only, AD_HOC tabs are not."""
        preview_tab = EditorTab(
            kind=TabKind.PREVIEW,
            title="preview",
            content="SELECT 1",
            read_only=True,
        )
        assert preview_tab.read_only is True


class TestAppExtractHandlerUsesContentParam:
    """on_catalog_panel_joint_sql_extract_requested passes SQL via content param."""

    def test_extract_handler_calls_new_ad_hoc_tab_with_content(self) -> None:
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        sql = "SELECT customer_id, SUM(amount) FROM orders GROUP BY 1"

        session = MagicMock()
        session.get_joint_sql.return_value = sql

        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        new_ad_hoc_calls: list[dict] = []

        class FakeEditor:
            def new_ad_hoc_tab(self, title=None, content=""):
                new_ad_hoc_calls.append({"title": title, "content": content})

        app.query_one = MagicMock(return_value=FakeEditor())  # type: ignore[assignment]

        msg = CatalogPanel.JointSqlExtractRequested("my_joint")
        app.on_catalog_panel_joint_sql_extract_requested(msg)

        assert len(new_ad_hoc_calls) == 1
        call = new_ad_hoc_calls[0]
        assert call["title"] == "[my_joint] (extracted)"
        assert call["content"] == sql

    def test_extract_handler_no_longer_accesses_private_internals(self) -> None:
        """Handler uses new_ad_hoc_tab(content=...) — no direct _tabs manipulation."""
        import inspect

        from rivet_cli.repl.app import RivetRepl

        source = inspect.getsource(
            RivetRepl.on_catalog_panel_joint_sql_extract_requested
        )
        # Should not directly access _tabs or call area.load_text after new_ad_hoc_tab
        assert "editor._tabs" not in source
        assert "area.load_text" not in source

    def test_extract_handler_error_notifies(self) -> None:
        """If get_joint_sql raises, handler calls notify with error."""
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        session = MagicMock()
        session.get_joint_sql.side_effect = Exception("Joint not found")

        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        notify_calls: list[dict] = []
        app.notify = lambda msg, severity="information": notify_calls.append(  # type: ignore[assignment]
            {"msg": msg, "severity": severity}
        )

        msg = CatalogPanel.JointSqlExtractRequested("missing_joint")
        app.on_catalog_panel_joint_sql_extract_requested(msg)

        assert len(notify_calls) == 1
        assert notify_calls[0]["severity"] == "error"
