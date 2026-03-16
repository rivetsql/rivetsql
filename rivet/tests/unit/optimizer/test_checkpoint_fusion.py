"""Unit tests for checkpoint joint fusion boundary behavior.

Verifies that the optimizer's fusion pass treats checkpoint joints as
downstream fusion boundaries: upstream joints fuse with the checkpoint
normally, but nothing downstream can fuse into the checkpoint's group.

Requirements: 5.1, 5.2, 5.3, 5.4
"""

from __future__ import annotations

from rivet_core.optimizer import FusionJoint, fusion_pass


def _make_joint(
    name: str,
    upstream: list[str] | None = None,
    engine: str = "eng1",
    engine_type: str = "duckdb",
    joint_type: str = "sql",
    sql: str | None = None,
) -> FusionJoint:
    return FusionJoint(
        name=name,
        joint_type=joint_type,
        upstream=upstream or [],
        engine=engine,
        engine_type=engine_type,
        sql=sql,
    )


def test_upstream_fuses_into_checkpoint_group() -> None:
    """source → sql → checkpoint produces a single FusedGroup with all three joints."""
    joints = [
        _make_joint("src", joint_type="source"),
        _make_joint("transform", upstream=["src"], sql="SELECT * FROM src"),
        _make_joint("cp", upstream=["transform"], joint_type="checkpoint"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 1
    assert set(groups[0].joints) == {"src", "transform", "cp"}
    assert "cp" in groups[0].exit_joints


def test_downstream_does_not_fuse_with_checkpoint() -> None:
    """source → checkpoint → sql produces two groups: [source, checkpoint] and [sql]."""
    joints = [
        _make_joint("src", joint_type="source"),
        _make_joint("cp", upstream=["src"], joint_type="checkpoint"),
        _make_joint("query", upstream=["cp"], sql="SELECT * FROM cp"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 2
    cp_group = next(g for g in groups if "cp" in g.joints)
    query_group = next(g for g in groups if "query" in g.joints)
    assert set(cp_group.joints) == {"src", "cp"}
    assert query_group.joints == ["query"]


def test_checkpoint_exit_joint_is_checkpoint() -> None:
    """In source → sql → checkpoint → sink, the exit joint of the first group is checkpoint."""
    joints = [
        _make_joint("src", joint_type="source"),
        _make_joint("transform", upstream=["src"], sql="SELECT * FROM src"),
        _make_joint("cp", upstream=["transform"], joint_type="checkpoint"),
        _make_joint("out", upstream=["cp"], joint_type="sink"),
    ]
    groups = fusion_pass(joints)
    cp_group = next(g for g in groups if "cp" in g.joints)
    assert cp_group.exit_joints == ["cp"]
    # checkpoint and its upstreams are in the same group
    assert set(cp_group.joints) == {"src", "transform", "cp"}
    # sink is in a separate group
    sink_group = next(g for g in groups if "out" in g.joints)
    assert sink_group.joints == ["out"]
