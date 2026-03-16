"""Unit tests for checkpoint joint — type registration and assembly validation."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from rivet_core.assembly import Assembly, AssemblyError
from rivet_core.models import Joint


class TestCheckpointTypeRegistration:
    """Verify checkpoint is accepted as a valid joint type."""

    def test_checkpoint_joint_type_accepted(self) -> None:
        joint = Joint(
            name="cp",
            joint_type="checkpoint",
            catalog="c",
            table="t",
            upstream=["src"],
        )
        assert joint.joint_type == "checkpoint"
        assert joint.catalog == "c"
        assert joint.table == "t"

    def test_invalid_joint_type_raises(self) -> None:
        excluded = frozenset({"source", "sql", "sink", "python"})
        with patch("rivet_core.models.JOINT_TYPES", excluded):
            with pytest.raises(ValueError, match="Invalid joint_type.*checkpoint"):
                Joint(name="x", joint_type="checkpoint")


class TestCheckpointAssemblyValidation:
    """Verify assembly structural rules for checkpoint joints."""

    def test_assembly_checkpoint_no_upstream_raises(self) -> None:
        joints = [
            Joint(name="cp", joint_type="checkpoint", catalog="c", table="t"),
        ]
        with pytest.raises(AssemblyError) as exc_info:
            Assembly(joints)
        assert exc_info.value.error.code == "RVT-304"

    def test_assembly_checkpoint_no_downstream_valid(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c"),
            Joint(name="cp", joint_type="checkpoint", catalog="c", table="t", upstream=["src"]),
        ]
        assembly = Assembly(joints)
        assert "cp" in assembly.joints
        assert "cp" in assembly.topological_order()

    def test_assembly_checkpoint_with_downstream_valid(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c"),
            Joint(
                name="cp",
                joint_type="checkpoint",
                catalog="c",
                table="t",
                upstream=["src"],
            ),
            Joint(name="transform", joint_type="sql", sql="SELECT 1", upstream=["cp"]),
            Joint(
                name="out",
                joint_type="sink",
                catalog="c",
                table="out_t",
                upstream=["transform"],
            ),
        ]
        assembly = Assembly(joints)
        order = assembly.topological_order()
        assert (
            order.index("src") < order.index("cp") < order.index("transform") < order.index("out")
        )
