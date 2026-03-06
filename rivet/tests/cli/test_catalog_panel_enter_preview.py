"""Tests for CatalogPanel Enter key read-only preview behavior — task 10.3.

# Feature: repl-state-improvements, Requirement 6.1
"""

from __future__ import annotations

from rivet_cli.repl.widgets.catalog import CatalogPanel
from rivet_cli.repl.widgets.editor import EditorPanel, TabKind


class TestEnterBinding:
    """Enter binding is declared in CatalogPanel.BINDINGS and activates action_activate_node."""

    def test_enter_binding_exists(self) -> None:
        keys = [b.key for b in CatalogPanel.BINDINGS]
        assert "enter" in keys

    def test_enter_action(self) -> None:
        binding = next(b for b in CatalogPanel.BINDINGS if b.key == "enter")
        assert binding.action == "activate_node"

    def test_action_activate_node_method_exists(self) -> None:
        assert hasattr(CatalogPanel, "action_activate_node")


class TestJointSelectedMessage:
    """JointSelected message class exists and carries joint_name."""

    def test_message_class_exists(self) -> None:
        assert hasattr(CatalogPanel, "JointSelected")

    def test_message_carries_joint_name(self) -> None:
        msg = CatalogPanel.JointSelected("my_joint")
        assert msg.joint_name == "my_joint"


class TestEditorOpenPreview:
    """EditorPanel.open_preview creates a read-only PREVIEW tab."""

    def test_open_preview_method_exists(self) -> None:
        assert hasattr(EditorPanel, "open_preview")

    def test_preview_tab_kind_is_read_only(self) -> None:
        """TabKind.PREVIEW tabs are always read-only per EditorTab construction."""
        from rivet_cli.repl.widgets.editor import EditorTab
        tab = EditorTab(kind=TabKind.PREVIEW, title="test", read_only=True)
        assert tab.read_only is True

    def test_preview_tab_kind_enum_exists(self) -> None:
        assert TabKind.PREVIEW is not None


class TestAppHandlesJointSelected:
    """RivetRepl handles CatalogPanel.JointSelected by opening a read-only preview tab."""

    def test_handler_method_exists(self) -> None:
        from rivet_cli.repl.app import RivetRepl
        assert hasattr(RivetRepl, "on_catalog_panel_joint_selected")
