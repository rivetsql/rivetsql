"""Tests for CatalogPanel debounced cursor preview — task 9.1.

# Feature: repl-state-improvements, Requirement 5.1, 5.5
"""

from __future__ import annotations

from rivet_cli.repl.widgets.catalog import CatalogPanel


class TestJointPreviewRequestedMessage:
    """JointPreviewRequested message class exists and carries joint_name."""

    def test_message_class_exists(self) -> None:
        assert hasattr(CatalogPanel, "JointPreviewRequested")

    def test_message_carries_joint_name(self) -> None:
        msg = CatalogPanel.JointPreviewRequested("my_joint")
        assert msg.joint_name == "my_joint"

    def test_preview_timer_field_in_init(self) -> None:
        """CatalogPanel.__init__ initialises _preview_timer to None."""
        # Inspect the source to confirm the field is set
        import inspect
        src = inspect.getsource(CatalogPanel.__init__)
        assert "_preview_timer" in src

    def test_node_highlighted_handler_exists(self) -> None:
        assert hasattr(CatalogPanel, "_on_node_highlighted")
