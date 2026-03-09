"""Property-based tests for multi-upstream fusion in the optimizer.

Covers Properties 1, 2, 6, and 7 from the multi-upstream fusion design document.

- Property 1: Multi-upstream merging produces a single group
  Validates: Requirements 1.1, 1.2
- Property 2: Partial eligibility merges only eligible upstreams
  Validates: Requirements 1.3
- Property 6: Topological order invariant in merged groups
  Validates: Requirements 3.1, 3.2, 3.3
- Property 7: Entry and exit joints correctly computed after merging
  Validates: Requirements 4.1, 4.2
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.optimizer import FusionJoint, fusion_pass

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_name_alphabet = st.characters(categories=("L", "N"))


@st.composite
def _all_eligible_multi_upstream_dag(draw: st.DrawFn) -> list[FusionJoint]:
    """Generate a DAG where a multi-input joint has N upstream sources, all on
    the same engine and all eligible for fusion.

    Structure: src_0, src_1, ..., src_{n-1} → join_joint
    All joints share the same engine, no barriers.
    """
    n_upstreams = draw(st.integers(min_value=2, max_value=6))
    engine = "eng1"
    sources = []
    for i in range(n_upstreams):
        sources.append(
            FusionJoint(
                name=f"src_{i}",
                joint_type="source",
                upstream=[],
                engine=engine,
                engine_type="duckdb",
                sql=f"SELECT * FROM table_{i}",
            )
        )
    join_joint = FusionJoint(
        name="join_joint",
        joint_type="sql",
        upstream=[s.name for s in sources],
        engine=engine,
        engine_type="duckdb",
        sql="SELECT * FROM joined",
    )
    return sources + [join_joint]


@st.composite
def _partial_eligible_multi_upstream_dag(
    draw: st.DrawFn,
) -> tuple[list[FusionJoint], list[str], list[str]]:
    """Generate a DAG where a multi-input joint has some eligible and some
    ineligible upstream joints.

    Returns (joints, eligible_names, ineligible_names).
    Ineligibility is caused by a different engine instance.
    """
    n_eligible = draw(st.integers(min_value=1, max_value=4))
    n_ineligible = draw(st.integers(min_value=1, max_value=4))
    engine = "eng1"
    other_engine = "eng2"

    eligible: list[FusionJoint] = []
    for i in range(n_eligible):
        eligible.append(
            FusionJoint(
                name=f"elig_{i}",
                joint_type="source",
                upstream=[],
                engine=engine,
                engine_type="duckdb",
                sql=f"SELECT * FROM elig_table_{i}",
            )
        )

    ineligible: list[FusionJoint] = []
    for i in range(n_ineligible):
        ineligible.append(
            FusionJoint(
                name=f"inelig_{i}",
                joint_type="source",
                upstream=[],
                engine=other_engine,
                engine_type="duckdb",
                sql=f"SELECT * FROM inelig_table_{i}",
            )
        )

    all_upstreams = eligible + ineligible
    join_joint = FusionJoint(
        name="join_joint",
        joint_type="sql",
        upstream=[j.name for j in all_upstreams],
        engine=engine,
        engine_type="duckdb",
        sql="SELECT * FROM joined",
    )
    joints = all_upstreams + [join_joint]
    return joints, [j.name for j in eligible], [j.name for j in ineligible]


# ---------------------------------------------------------------------------
# Property Tests
# ---------------------------------------------------------------------------


# Feature: multi-upstream-fusion
# Property 1: Multi-upstream merging produces a single group
@given(dag=_all_eligible_multi_upstream_dag())
@settings(max_examples=100)
def test_property1_multi_upstream_merging_produces_single_group(
    dag: list[FusionJoint],
) -> None:
    """For any DAG where a multi-input joint has N upstream joints that all
    pass the _can_fuse check, the fusion pass shall produce a single fused
    group containing the multi-input joint and all upstream joints.

    Validates: Requirements 1.1, 1.2
    """
    groups = fusion_pass(dag)

    # All joints should end up in a single group
    assert len(groups) == 1, (
        f"Expected 1 group but got {len(groups)}: "
        f"{[g.joints for g in groups]}"
    )

    merged_group = groups[0]
    all_names = {j.name for j in dag}
    assert set(merged_group.joints) == all_names, (
        f"Merged group joints {merged_group.joints} != expected {all_names}"
    )


# Feature: multi-upstream-fusion
# Property 2: Partial eligibility merges only eligible upstreams
@given(data=_partial_eligible_multi_upstream_dag())
@settings(max_examples=100)
def test_property2_partial_eligibility_merges_only_eligible(
    data: tuple[list[FusionJoint], list[str], list[str]],
) -> None:
    """For any DAG where a multi-input joint has some eligible and some
    ineligible upstreams, the fusion pass shall merge only the eligible
    upstream groups with the multi-input joint, leaving ineligible upstreams
    in their own groups.

    Validates: Requirements 1.3
    """
    joints, eligible_names, ineligible_names = data
    groups = fusion_pass(joints)

    # Find the group containing the join joint
    join_group = next(g for g in groups if "join_joint" in g.joints)

    # All eligible upstreams must be in the same group as the join
    for name in eligible_names:
        assert name in join_group.joints, (
            f"Eligible upstream '{name}' not in join group {join_group.joints}"
        )

    # No ineligible upstream should be in the join group
    for name in ineligible_names:
        assert name not in join_group.joints, (
            f"Ineligible upstream '{name}' should not be in join group "
            f"{join_group.joints}"
        )

    # Each ineligible upstream should be in its own standalone group
    for name in ineligible_names:
        inelig_group = next(g for g in groups if name in g.joints)
        assert inelig_group.joints == [name], (
            f"Ineligible upstream '{name}' should be standalone but is in "
            f"group {inelig_group.joints}"
        )


# ---------------------------------------------------------------------------
# General DAG Strategy (for Properties 6 and 7)
# ---------------------------------------------------------------------------


@st.composite
def _arbitrary_fusion_dag(draw: st.DrawFn) -> list[FusionJoint]:
    """Generate an arbitrary DAG of FusionJoints in topological order.

    Produces varied topologies: chains, diamonds, multi-input joints, mixed
    engines, and barrier flags.  This exercises the fusion pass across a wide
    range of shapes so that topological-order and entry/exit invariants are
    validated broadly.
    """
    n_joints = draw(st.integers(min_value=2, max_value=10))
    engines = draw(st.lists(st.sampled_from(["eng1", "eng2"]), min_size=1, max_size=2))

    joints: list[FusionJoint] = []
    for i in range(n_joints):
        # Each joint may reference 0..min(i, 3) previously-created joints
        max_ups = min(i, 3)
        n_ups = draw(st.integers(min_value=0, max_value=max_ups))
        upstream_indices = draw(
            st.lists(
                st.integers(min_value=0, max_value=max(i - 1, 0)),
                min_size=n_ups,
                max_size=n_ups,
                unique=True,
            )
            if n_ups > 0
            else st.just([])
        )
        upstream_names = [joints[idx].name for idx in upstream_indices]

        engine = draw(st.sampled_from(engines))
        eager = draw(st.booleans()) if draw(st.integers(min_value=0, max_value=4)) == 0 else False
        has_assertions = draw(st.booleans()) if draw(st.integers(min_value=0, max_value=4)) == 0 else False
        jtype = draw(st.sampled_from(["source", "sql", "sql"])) if not upstream_names else "sql"

        joints.append(
            FusionJoint(
                name=f"j_{i}",
                joint_type=jtype,
                upstream=upstream_names,
                engine=engine,
                engine_type="duckdb",
                eager=eager,
                has_assertions=has_assertions,
                sql=f"SELECT * FROM t_{i}",
            )
        )
    return joints


# ---------------------------------------------------------------------------
# Property 6 & 7 Tests
# ---------------------------------------------------------------------------


# Feature: multi-upstream-fusion
# Property 6: Topological order invariant in merged groups
@given(dag=_arbitrary_fusion_dag())
@settings(max_examples=100)
def test_property6_topological_order_invariant_in_merged_groups(
    dag: list[FusionJoint],
) -> None:
    """For any fused group produced by the fusion pass, every joint in the
    group's joint list shall appear after all of its in-group upstream
    dependencies.  Equivalently, for every joint j at index i in the list,
    all of j's upstream joints that are in the group appear at indices < i.

    Validates: Requirements 3.1, 3.2, 3.3
    """
    joints_by_name = {j.name: j for j in dag}
    groups = fusion_pass(dag)

    for group in groups:
        group_set = set(group.joints)
        index_of = {name: idx for idx, name in enumerate(group.joints)}

        for name in group.joints:
            j = joints_by_name[name]
            for up in j.upstream:
                if up in group_set:
                    assert index_of[up] < index_of[name], (
                        f"Topological order violated in group {group.id}: "
                        f"upstream '{up}' (index {index_of[up]}) must appear "
                        f"before '{name}' (index {index_of[name]}). "
                        f"Group joints: {group.joints}"
                    )


# Feature: multi-upstream-fusion
# Property 7: Entry and exit joints correctly computed after merging
@given(dag=_arbitrary_fusion_dag())
@settings(max_examples=100)
def test_property7_entry_and_exit_joints_correctly_computed(
    dag: list[FusionJoint],
) -> None:
    """For any fused group produced by the fusion pass, the entry joints shall
    be exactly those joints whose upstream dependencies are all outside the
    group (or have no upstream), and the exit joints shall be exactly those
    joints that have no in-group downstream consumers.

    Validates: Requirements 4.1, 4.2
    """
    joints_by_name = {j.name: j for j in dag}
    groups = fusion_pass(dag)

    for group in groups:
        group_set = set(group.joints)

        # --- Entry joints ---
        expected_entries = []
        for name in group.joints:
            j = joints_by_name[name]
            if not any(up in group_set for up in j.upstream):
                expected_entries.append(name)

        assert set(group.entry_joints) == set(expected_entries), (
            f"Entry joints mismatch for group {group.id}: "
            f"got {group.entry_joints}, expected {expected_entries}. "
            f"Group joints: {group.joints}"
        )

        # --- Exit joints ---
        # A joint is an exit if no other joint in the group lists it as upstream
        has_in_group_downstream: set[str] = set()
        for name in group.joints:
            j = joints_by_name[name]
            for up in j.upstream:
                if up in group_set:
                    has_in_group_downstream.add(up)

        expected_exits = [
            name for name in group.joints if name not in has_in_group_downstream
        ]

        assert set(group.exit_joints) == set(expected_exits), (
            f"Exit joints mismatch for group {group.id}: "
            f"got {group.exit_joints}, expected {expected_exits}. "
            f"Group joints: {group.joints}"
        )
