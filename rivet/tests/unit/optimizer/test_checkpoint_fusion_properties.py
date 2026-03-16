"""Property-based tests for checkpoint fusion boundary behavior.

Covers Property 3 from the checkpoint-joint design document.

- Property 3: Checkpoint blocks downstream fusion
  Validates: Requirements 5.2, 5.3, 5.4
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.optimizer import FusionJoint, fusion_pass

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def _checkpoint_pipeline(draw: st.DrawFn) -> list[FusionJoint]:
    """Generate a linear pipeline of the form:

        source → sql* → checkpoint → sql* → sink

    The number of SQL joints before and after the checkpoint varies (0..4 each).
    All joints share the same engine so fusion eligibility is purely structural.
    """
    engine = "eng1"
    joints: list[FusionJoint] = []

    # 1. Source
    joints.append(
        FusionJoint(
            name="src",
            joint_type="source",
            upstream=[],
            engine=engine,
            engine_type="duckdb",
            sql="SELECT * FROM raw",
        )
    )

    # 2. Pre-checkpoint SQL joints (0..4)
    n_pre = draw(st.integers(min_value=0, max_value=4))
    for i in range(n_pre):
        prev = joints[-1].name
        joints.append(
            FusionJoint(
                name=f"pre_sql_{i}",
                joint_type="sql",
                upstream=[prev],
                engine=engine,
                engine_type="duckdb",
                sql=f"SELECT * FROM {prev}",
            )
        )

    # 3. Checkpoint
    prev = joints[-1].name
    joints.append(
        FusionJoint(
            name="cp",
            joint_type="checkpoint",
            upstream=[prev],
            engine=engine,
            engine_type="duckdb",
        )
    )

    # 4. Post-checkpoint SQL joints (0..4)
    n_post = draw(st.integers(min_value=0, max_value=4))
    for i in range(n_post):
        prev = joints[-1].name
        joints.append(
            FusionJoint(
                name=f"post_sql_{i}",
                joint_type="sql",
                upstream=[prev],
                engine=engine,
                engine_type="duckdb",
                sql=f"SELECT * FROM {prev}",
            )
        )

    # 5. Sink
    prev = joints[-1].name
    joints.append(
        FusionJoint(
            name="sink",
            joint_type="sink",
            upstream=[prev],
            engine=engine,
            engine_type="duckdb",
        )
    )

    return joints


# ---------------------------------------------------------------------------
# Property Tests
# ---------------------------------------------------------------------------


# Feature: checkpoint-joint, Property 3: checkpoint blocks downstream fusion
@given(pipeline=_checkpoint_pipeline())
@settings(max_examples=100)
def test_property3_checkpoint_blocks_downstream_fusion(
    pipeline: list[FusionJoint],
) -> None:
    """For any linear pipeline [source, sql*, checkpoint, sql*, sink],
    no joint downstream of the checkpoint shall share a FusedGroup with
    that checkpoint.

    The checkpoint itself may fuse with its upstream joints, but it acts
    as a downstream fusion boundary — nothing after it joins its group.

    Validates: Requirements 5.2, 5.3, 5.4
    """
    groups = fusion_pass(pipeline)

    # Build a lookup: joint name → group id
    group_for: dict[str, str] = {}
    for g in groups:
        for name in g.joints:
            group_for[name] = g.id

    cp_group_id = group_for["cp"]

    # Identify all joints downstream of the checkpoint
    joint_names = [j.name for j in pipeline]
    cp_index = joint_names.index("cp")
    downstream_names = joint_names[cp_index + 1 :]

    # No downstream joint may be in the checkpoint's group
    for name in downstream_names:
        assert group_for[name] != cp_group_id, (
            f"Downstream joint '{name}' is in the same group as checkpoint "
            f"(group {cp_group_id}). Groups: "
            f"{[(g.id, g.joints) for g in groups]}"
        )

    # The checkpoint must be an exit joint of its group
    cp_group = next(g for g in groups if g.id == cp_group_id)
    assert "cp" in cp_group.exit_joints, (
        f"Checkpoint should be an exit joint of its group but exit_joints="
        f"{cp_group.exit_joints}. Group joints: {cp_group.joints}"
    )
