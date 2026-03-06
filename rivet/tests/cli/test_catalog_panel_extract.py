"""Tests for CatalogPanel Shift+Enter SQL extraction binding — task 10.1.

# Feature: repl-state-improvements, Requirement 6.2, 6.3, 6.4
"""

from __future__ import annotations

from rivet_cli.repl.widgets.catalog import CatalogPanel


class TestJointSqlExtractRequestedMessage:
    """JointSqlExtractRequested message class exists and carries joint_name."""

    def test_message_class_exists(self) -> None:
        assert hasattr(CatalogPanel, "JointSqlExtractRequested")

    def test_message_carries_joint_name(self) -> None:
        msg = CatalogPanel.JointSqlExtractRequested("my_joint")
        assert msg.joint_name == "my_joint"


class TestShiftEnterBinding:
    """Shift+Enter binding is declared in CatalogPanel.BINDINGS."""

    def test_shift_enter_binding_exists(self) -> None:
        keys = [b.key for b in CatalogPanel.BINDINGS]
        assert "shift+enter" in keys

    def test_shift_enter_action(self) -> None:
        binding = next(b for b in CatalogPanel.BINDINGS if b.key == "shift+enter")
        assert binding.action == "extract_joint_sql"

    def test_action_extract_joint_sql_method_exists(self) -> None:
        assert hasattr(CatalogPanel, "action_extract_joint_sql")
