"""Tests for RivetContext."""

from __future__ import annotations

import pytest

from rivet_core.context import RivetContext
from rivet_core.logging import RivetLogger


def test_rivet_context_minimal():
    ctx = RivetContext(joint_name="my_joint")
    assert ctx.joint_name == "my_joint"
    assert ctx.options == {}
    assert ctx.run_metadata == {}
    assert isinstance(ctx.logger, RivetLogger)


def test_rivet_context_with_all_fields():
    logger = RivetLogger()
    ctx = RivetContext(
        joint_name="transform_users",
        options={"threshold": 100, "mode": "strict"},
        logger=logger,
        run_metadata={"run_id": "abc-123", "profile": "prod"},
    )
    assert ctx.joint_name == "transform_users"
    assert ctx.options["threshold"] == 100
    assert ctx.options["mode"] == "strict"
    assert ctx.logger is logger
    assert ctx.run_metadata["run_id"] == "abc-123"
    assert ctx.run_metadata["profile"] == "prod"


def test_rivet_context_is_frozen():
    ctx = RivetContext(joint_name="my_joint")
    with pytest.raises((AttributeError, TypeError)):
        ctx.joint_name = "other"  # type: ignore[misc]


def test_rivet_context_defaults_are_independent():
    ctx1 = RivetContext(joint_name="a")
    ctx2 = RivetContext(joint_name="b")
    # Mutable defaults must not be shared
    assert ctx1.options is not ctx2.options
    assert ctx1.run_metadata is not ctx2.run_metadata
