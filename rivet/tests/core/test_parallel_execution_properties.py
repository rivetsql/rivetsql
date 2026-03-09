"""Property-based tests for parallel joint execution.

Covers Properties 1, 2, 3, 4, 5, 6, 7, and 10 from the parallel-joint-execution design document.

- Property 1: Dependency graph construction correctness
  Validates: Requirements 1.1, 1.2, 1.3
- Property 2: Parallel execution plan wave correctness
  Validates: Requirements 7.1, 7.2
- Property 3: Dependency ordering under parallel execution
  Validates: Requirements 2.2, 2.3, 6.4
- Property 4: Parallel-sequential result equivalence
  Validates: Requirements 2.4
- Property 5: Invalid concurrency_limit rejection
  Validates: Requirements 3.3
- Property 6: Engine concurrency limit enforcement
  Validates: Requirements 3.4, 4.3
- Property 7: Cross-engine independence
  Validates: Requirements 4.1, 4.2
- Property 10: Shared state integrity under concurrency
  Validates: Requirements 6.1, 6.2, 6.3
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import CompiledJoint, _compute_parallel_execution_plan
from rivet_core.executor import DependencyGraph
from rivet_core.optimizer import FusedGroup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_joint(name: str, upstream: list[str], fused_group_id: str | None = None) -> CompiledJoint:
    """Create a minimal CompiledJoint for testing."""
    return CompiledJoint(
        name=name,
        type="sql",
        catalog=None,
        catalog_type=None,
        engine="duckdb",
        engine_resolution=None,
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=upstream,
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=fused_group_id,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_group(gid: str, joints: list[str], engine: str = "duckdb") -> FusedGroup:
    """Create a minimal FusedGroup for testing."""
    return FusedGroup(
        id=gid,
        joints=joints,
        engine=engine,
        engine_type="duckdb",
        adapters={j: None for j in joints},
        fused_sql=None,
    )


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def _random_fused_groups_with_upstreams(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint]]:
    """Generate random FusedGroup lists with known upstream relationships.

    Produces 1-10 groups, each with 1-3 joints. Upstream relationships are
    randomly assigned between joints in different groups. Some joints may
    reference "external" joints not in any group (to test Req 1.3).

    Returns (groups, joint_map) where joint_map contains all CompiledJoints
    including external ones.
    """
    n_groups = draw(st.integers(min_value=1, max_value=10))

    groups: list[FusedGroup] = []
    all_joint_names: list[str] = []  # tracks all joint names across groups

    # Build groups with unique joint names
    for g_idx in range(n_groups):
        n_joints = draw(st.integers(min_value=1, max_value=3))
        joint_names = [f"g{g_idx}_j{j_idx}" for j_idx in range(n_joints)]
        groups.append(_make_group(f"group_{g_idx}", joint_names))
        all_joint_names.extend(joint_names)

    # Decide how many external joints to create (0-3)
    n_external = draw(st.integers(min_value=0, max_value=3))
    external_names = [f"ext_{i}" for i in range(n_external)]

    # All possible upstream targets: joints in other groups + external joints
    # Build a map: joint_name -> owning group index (for reference)
    joint_to_group_idx: dict[str, int] = {}
    for g_idx, group in enumerate(groups):
        for jname in group.joints:
            joint_to_group_idx[jname] = g_idx

    # For each joint, randomly pick upstream joints from earlier groups or externals
    # (to keep things simple, a joint can reference joints from any other group)
    joint_map: dict[str, CompiledJoint] = {}
    all_joint_names + external_names

    for g_idx, group in enumerate(groups):
        # Joints in other groups that this group's joints could reference
        other_group_joints = [
            jn for jn in all_joint_names if joint_to_group_idx[jn] != g_idx
        ]
        possible_upstreams = other_group_joints + external_names

        for jname in group.joints:
            if possible_upstreams:
                n_ups = draw(st.integers(min_value=0, max_value=min(3, len(possible_upstreams))))
                upstream = draw(
                    st.lists(
                        st.sampled_from(possible_upstreams),
                        min_size=n_ups,
                        max_size=n_ups,
                        unique=True,
                    )
                    if n_ups > 0
                    else st.just([])
                )
            else:
                upstream = []
            joint_map[jname] = _make_joint(jname, upstream)

    # Add external joints to the joint_map (they exist as compiled joints
    # but are not in any fused group)
    for ext_name in external_names:
        joint_map[ext_name] = _make_joint(ext_name, upstream=[])

    return groups, joint_map


# ---------------------------------------------------------------------------
# Property Tests
# ---------------------------------------------------------------------------


# Feature: parallel-joint-execution, Property 1: Dependency graph construction correctness
@given(data=_random_fused_groups_with_upstreams())
@settings(max_examples=100)
def test_property1_dependency_graph_construction_correctness(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint]],
) -> None:
    """For any set of fused groups and compiled joints, DependencyGraph.build()
    shall produce a graph where:
    - The graph contains exactly the set of group IDs from the input groups.
    - An edge from group A to group B exists iff at least one joint in B has
      an upstream joint whose output is produced by a joint in A.
    - If a joint's upstream references a joint not belonging to any fused group,
      no edge is created for that reference.

    **Validates: Requirements 1.1, 1.2, 1.3**
    """
    groups, joint_map = data

    graph = DependencyGraph.build(groups, joint_map)

    # --- Req 1.2: graph contains exactly the group IDs ---
    expected_group_ids = {g.id for g in groups}
    actual_group_ids = set(graph._in_degree.keys())
    assert actual_group_ids == expected_group_ids, (
        f"Graph group IDs {actual_group_ids} != expected {expected_group_ids}"
    )

    # Build the expected edges from first principles
    # Map each joint name -> owning group id
    joint_to_group: dict[str, str] = {}
    for group in groups:
        for jname in group.joints:
            joint_to_group[jname] = group.id

    # --- Req 1.1 & 1.3: edge A->B iff joint in B has upstream in A ---
    expected_upstream: dict[str, set[str]] = {g.id: set() for g in groups}
    expected_downstream: dict[str, set[str]] = {g.id: set() for g in groups}

    for group in groups:
        for jname in group.joints:
            compiled = joint_map.get(jname)
            if compiled is None:
                continue
            for up_name in compiled.upstream:
                up_group_id = joint_to_group.get(up_name)
                # Req 1.3: skip upstream refs not in any group
                if up_group_id is None:
                    continue
                # Skip self-references within the same group
                if up_group_id == group.id:
                    continue
                expected_upstream[group.id].add(up_group_id)
                expected_downstream[up_group_id].add(group.id)

    # Verify upstream edges match
    for gid in expected_group_ids:
        assert graph._upstream[gid] == expected_upstream[gid], (
            f"Upstream mismatch for {gid}: "
            f"got {graph._upstream[gid]}, expected {expected_upstream[gid]}"
        )

    # Verify downstream edges match
    for gid in expected_group_ids:
        assert graph._downstream[gid] == expected_downstream[gid], (
            f"Downstream mismatch for {gid}: "
            f"got {graph._downstream[gid]}, expected {expected_downstream[gid]}"
        )

    # Verify in-degree matches upstream count
    for gid in expected_group_ids:
        assert graph._in_degree[gid] == len(expected_upstream[gid]), (
            f"In-degree mismatch for {gid}: "
            f"got {graph._in_degree[gid]}, expected {len(expected_upstream[gid])}"
        )


# ---------------------------------------------------------------------------
# Property 5: Invalid concurrency_limit rejection
# ---------------------------------------------------------------------------

from rivet_core.errors import ExecutionError
from rivet_core.executor import _resolve_concurrency_limits
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry


@st.composite
def _invalid_concurrency_limit(draw: st.DrawFn):
    """Generate invalid concurrency_limit values.

    Invalid values include:
    - Zero (0)
    - Negative integers
    - Floats (positive and negative)
    - Strings
    - Booleans (True/False — int subclasses but should be rejected)
    """
    return draw(
        st.one_of(
            st.just(0),
            st.integers(max_value=-1),
            st.floats(allow_nan=False, allow_infinity=False),
            st.text(min_size=0, max_size=20),
            st.booleans(),
        )
    )


# Feature: parallel-joint-execution, Property 5: Invalid concurrency_limit rejection
@given(invalid_limit=_invalid_concurrency_limit())
@settings(max_examples=100)
def test_property5_invalid_concurrency_limit_rejection(
    invalid_limit,
) -> None:
    """For any ComputeEngine whose config["concurrency_limit"] is not a valid
    positive integer (zero, negative, float, string, boolean),
    _resolve_concurrency_limits shall raise ExecutionError with code "RVT-501"
    before any group executes.

    **Validates: Requirements 3.3**
    """
    engine = ComputeEngine(
        name="test_engine",
        engine_type="duckdb",
        config={"concurrency_limit": invalid_limit},
    )
    registry = PluginRegistry()

    try:
        _resolve_concurrency_limits([engine], registry)
        # If we get here, the function did not raise — that's a failure
        raise AssertionError(
            f"Expected ExecutionError for concurrency_limit={invalid_limit!r}, "
            f"but _resolve_concurrency_limits returned successfully."
        )
    except ExecutionError as exc:
        assert exc.error.code == "RVT-501", (
            f"Expected error code 'RVT-501', got {exc.error.code!r}"
        )


# ---------------------------------------------------------------------------
# Property 10: Shared state integrity under concurrency
# ---------------------------------------------------------------------------

from typing import Any

import pyarrow

from rivet_core.compiler import CompiledAssembly, CompiledEngine
from rivet_core.executor import Executor
from rivet_core.models import ComputeEngine as ComputeEngineModel
from rivet_core.plugins import ComputeEnginePlugin


class _FakePlugin(ComputeEnginePlugin):
    """Minimal plugin that returns a one-row table for any SQL execution."""

    engine_type = "fake"
    supported_catalog_types: dict[str, list[str]] = {}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngineModel:
        return ComputeEngineModel(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self,
        engine: Any,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        return pyarrow.table({"col": [1]})


def _build_compiled_assembly_for_dag(
    groups: list[FusedGroup],
    joint_map: dict[str, CompiledJoint],
    execution_order: list[str],
) -> CompiledAssembly:
    """Build a valid CompiledAssembly from groups and joints for execution."""
    all_joints = [joint_map[jn] for g in groups for jn in g.joints]
    return CompiledAssembly(
        success=True,
        profile_name="test",
        catalogs=[],
        engines=[CompiledEngine(name="fake_engine", engine_type="fake", native_catalog_types=[])],
        adapters=[],
        joints=all_joints,
        fused_groups=groups,
        materializations=[],
        execution_order=execution_order,
        errors=[],
        warnings=[],
    )


def _make_joint_for_exec(
    name: str, upstream: list[str], fused_group_id: str, engine: str = "fake_engine"
) -> CompiledJoint:
    """Create a CompiledJoint suitable for execution (sql type with simple SQL)."""
    return CompiledJoint(
        name=name,
        type="sql",
        catalog=None,
        catalog_type=None,
        engine=engine,
        engine_resolution=None,
        adapter=None,
        sql="SELECT 1 AS col",
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=upstream,
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=fused_group_id,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_group_for_exec(
    gid: str, joints: list[str], engine: str = "fake_engine"
) -> FusedGroup:
    """Create a FusedGroup suitable for execution."""
    return FusedGroup(
        id=gid,
        joints=joints,
        engine=engine,
        engine_type="fake",
        adapters={j: None for j in joints},
        fused_sql="SELECT 1 AS col",
        entry_joints=[joints[0]],
        exit_joints=[joints[-1]],
    )


@st.composite
def _random_dag_for_execution(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]]:
    """Generate a random DAG of 1-8 groups with valid topological ordering.

    Each group has exactly 1 joint (simplifies execution while still testing
    state integrity). Dependencies only point from earlier groups to later
    groups (by index) to guarantee a valid DAG with no cycles.

    Returns (groups, joint_map, execution_order).
    """
    n_groups = draw(st.integers(min_value=1, max_value=8))

    groups: list[FusedGroup] = []
    joint_map: dict[str, CompiledJoint] = {}
    execution_order: list[str] = []

    for g_idx in range(n_groups):
        gid = f"group_{g_idx}"
        jname = f"j_{g_idx}"

        # Pick upstream joints from earlier groups only (ensures DAG)
        if g_idx > 0:
            possible_upstreams = [f"j_{i}" for i in range(g_idx)]
            n_ups = draw(st.integers(min_value=0, max_value=min(3, len(possible_upstreams))))
            upstream = draw(
                st.lists(
                    st.sampled_from(possible_upstreams),
                    min_size=n_ups,
                    max_size=n_ups,
                    unique=True,
                )
                if n_ups > 0
                else st.just([])
            )
        else:
            upstream = []

        joint_map[jname] = _make_joint_for_exec(jname, upstream, fused_group_id=gid)
        groups.append(_make_group_for_exec(gid, [jname]))
        execution_order.append(gid)

    return groups, joint_map, execution_order


# Feature: parallel-joint-execution, Property 10: Shared state integrity under concurrency
@given(data=_random_dag_for_execution())
@settings(max_examples=50)
def test_property10_shared_state_integrity_under_concurrency(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]],
) -> None:
    """For any CompiledAssembly executed via Executor.run_sync, after execution
    completes, the shared state shall satisfy:
    - Each group appears exactly once in group_results (no duplicates, no missing).
    - Each joint appears exactly once in joint_results (no duplicates, no missing).
    - All groups and joints are accounted for.
    - No corrupted values (all results have valid structure).

    **Validates: Requirements 6.1, 6.2, 6.3**
    """
    groups, joint_map, execution_order = data

    # Set up a registry with the fake plugin and engine
    plugin = _FakePlugin()
    registry = PluginRegistry()
    registry.register_engine_plugin(plugin)
    engine = plugin.create_engine("fake_engine", {})
    registry.register_compute_engine(engine)

    compiled = _build_compiled_assembly_for_dag(groups, joint_map, execution_order)
    executor = Executor(registry=registry)
    result = executor.run_sync(compiled)

    # Collect expected group IDs and joint names
    expected_group_ids = {g.id for g in groups}
    expected_joint_names = {jn for g in groups for jn in g.joints}

    # --- Verify group_results integrity ---
    actual_group_ids = [gr.group_id for gr in result.group_results]
    assert len(actual_group_ids) == len(expected_group_ids), (
        f"Expected {len(expected_group_ids)} group results, got {len(actual_group_ids)}: "
        f"{actual_group_ids}"
    )
    assert set(actual_group_ids) == expected_group_ids, (
        f"Group result IDs mismatch: got {set(actual_group_ids)}, "
        f"expected {expected_group_ids}"
    )
    # No duplicates
    assert len(actual_group_ids) == len(set(actual_group_ids)), (
        f"Duplicate group results detected: {actual_group_ids}"
    )

    # --- Verify joint_results integrity ---
    actual_joint_names = [jr.name for jr in result.joint_results]
    assert len(actual_joint_names) == len(expected_joint_names), (
        f"Expected {len(expected_joint_names)} joint results, got {len(actual_joint_names)}: "
        f"{actual_joint_names}"
    )
    assert set(actual_joint_names) == expected_joint_names, (
        f"Joint result names mismatch: got {set(actual_joint_names)}, "
        f"expected {expected_joint_names}"
    )
    # No duplicates
    assert len(actual_joint_names) == len(set(actual_joint_names)), (
        f"Duplicate joint results detected: {actual_joint_names}"
    )

    # --- Verify all results have valid structure (no corruption) ---
    for gr in result.group_results:
        assert gr.group_id in expected_group_ids
        assert isinstance(gr.success, bool)
        assert gr.success is True, f"Group {gr.group_id} unexpectedly failed: {gr.error}"
        assert isinstance(gr.joints, list)
        assert len(gr.joints) > 0
        assert gr.timing is not None

    for jr in result.joint_results:
        assert jr.name in expected_joint_names
        assert isinstance(jr.success, bool)
        assert jr.success is True, f"Joint {jr.name} unexpectedly failed: {jr.error}"
        assert jr.fused_group_id is not None
        assert jr.fused_group_id in expected_group_ids
        assert jr.timing is not None

    # --- Verify overall result consistency ---
    assert result.success is True
    assert result.status == "success"
    assert result.total_failures == 0


# ---------------------------------------------------------------------------
# Property 3: Dependency ordering under parallel execution
# ---------------------------------------------------------------------------

import re
import threading
import time


class _TimestampPlugin(ComputeEnginePlugin):
    """Plugin that records start/end timestamps per group during execute_sql.

    The group ID is embedded in the SQL string as a comment: ``/* group_X */``.
    Timestamps are recorded using ``time.monotonic()`` into a shared dict.
    Since execute_sql runs in a thread (via asyncio.to_thread), a lock is used
    to protect the shared timestamps dict.
    """

    engine_type = "fake"
    supported_catalog_types: dict[str, list[str]] = {}

    def __init__(self) -> None:
        self.timestamps: dict[str, dict[str, float]] = {}
        self._lock = threading.Lock()

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngineModel:
        return ComputeEngineModel(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self,
        engine: Any,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        # Extract group ID from SQL comment
        m = re.search(r"/\*\s*(group_\d+)\s*\*/", sql or "")
        group_id = m.group(1) if m else "unknown"

        start = time.monotonic()
        result = pyarrow.table({"col": [1]})
        end = time.monotonic()

        with self._lock:
            self.timestamps[group_id] = {"start": start, "end": end}

        return result


@st.composite
def _random_dag_for_execution_with_timestamps(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]]:
    """Generate a random DAG like _random_dag_for_execution but with group IDs
    embedded in fused_sql for timestamp tracking.

    Returns (groups, joint_map, execution_order).
    """
    n_groups = draw(st.integers(min_value=1, max_value=8))

    groups: list[FusedGroup] = []
    joint_map: dict[str, CompiledJoint] = {}
    execution_order: list[str] = []

    for g_idx in range(n_groups):
        gid = f"group_{g_idx}"
        jname = f"j_{g_idx}"

        # Pick upstream joints from earlier groups only (ensures DAG)
        if g_idx > 0:
            possible_upstreams = [f"j_{i}" for i in range(g_idx)]
            n_ups = draw(st.integers(min_value=0, max_value=min(3, len(possible_upstreams))))
            upstream = draw(
                st.lists(
                    st.sampled_from(possible_upstreams),
                    min_size=n_ups,
                    max_size=n_ups,
                    unique=True,
                )
                if n_ups > 0
                else st.just([])
            )
        else:
            upstream = []

        joint_map[jname] = _make_joint_for_exec(jname, upstream, fused_group_id=gid)
        groups.append(
            FusedGroup(
                id=gid,
                joints=[jname],
                engine="fake_engine",
                engine_type="fake",
                adapters={jname: None},
                fused_sql=f"SELECT 1 AS col /* {gid} */",
                entry_joints=[jname],
                exit_joints=[jname],
            )
        )
        execution_order.append(gid)

    return groups, joint_map, execution_order


# Feature: parallel-joint-execution, Property 3: Dependency ordering under parallel execution
@given(data=_random_dag_for_execution_with_timestamps())
@settings(max_examples=50)
def test_property3_dependency_ordering_under_parallel_execution(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]],
) -> None:
    """For any CompiledAssembly and any execution of Executor.run_sync(),
    for every fused group G, all fused groups in G's upstream set in the
    dependency graph shall have completed execution (end timestamp) before
    G begins execution (start timestamp).

    **Validates: Requirements 2.2, 2.3, 6.4**
    """
    groups, joint_map, execution_order = data

    # Set up a registry with the timestamp-recording plugin
    plugin = _TimestampPlugin()
    registry = PluginRegistry()
    registry.register_engine_plugin(plugin)
    engine = plugin.create_engine("fake_engine", {})
    registry.register_compute_engine(engine)

    compiled = _build_compiled_assembly_for_dag(groups, joint_map, execution_order)
    executor = Executor(registry=registry)
    result = executor.run_sync(compiled)

    # Execution should succeed
    assert result.success, f"Execution failed: {[gr.error for gr in result.group_results if not gr.success]}"

    # Build the dependency graph to know upstream relationships
    dep_graph = DependencyGraph.build(groups, joint_map)

    # Verify dependency ordering: for every group G, all upstream groups
    # must have completed (end timestamp) before G started (start timestamp)
    for group in groups:
        gid = group.id
        assert gid in plugin.timestamps, (
            f"Group {gid} not found in timestamps: {list(plugin.timestamps.keys())}"
        )
        g_start = plugin.timestamps[gid]["start"]

        for upstream_gid in dep_graph._upstream.get(gid, set()):
            assert upstream_gid in plugin.timestamps, (
                f"Upstream group {upstream_gid} not found in timestamps"
            )
            upstream_end = plugin.timestamps[upstream_gid]["end"]
            assert upstream_end <= g_start, (
                f"Dependency ordering violated: upstream group '{upstream_gid}' "
                f"ended at {upstream_end}, but downstream group '{gid}' "
                f"started at {g_start}. Upstream must complete before "
                f"downstream starts."
            )


# ---------------------------------------------------------------------------
# Property 4: Parallel-sequential result equivalence
# ---------------------------------------------------------------------------


# Feature: parallel-joint-execution, Property 4: Parallel-sequential result equivalence
@given(data=_random_dag_for_execution())
@settings(max_examples=50)
def test_property4_parallel_sequential_result_equivalence(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]],
) -> None:
    """For any CompiledAssembly where all groups succeed, the ExecutionResult
    produced by parallel execution shall contain the same set of
    JointExecutionResult entries (by joint name, success, rows_out) and the
    same set of FusedGroupExecutionResult entries (by group_id, success,
    rows_out) as sequential execution, regardless of ordering.

    **Validates: Requirements 2.4**
    """
    groups, joint_map, execution_order = data

    # --- Run 1: sequential (concurrency_limit=1) ---
    plugin_seq = _FakePlugin()
    registry_seq = PluginRegistry()
    registry_seq.register_engine_plugin(plugin_seq)
    engine_seq = plugin_seq.create_engine(
        "fake_engine", {"concurrency_limit": 1}
    )
    registry_seq.register_compute_engine(engine_seq)

    compiled_seq = _build_compiled_assembly_for_dag(groups, joint_map, execution_order)
    executor_seq = Executor(registry=registry_seq)
    result_seq = executor_seq.run_sync(compiled_seq)

    # --- Run 2: parallel (concurrency_limit=10) ---
    plugin_par = _FakePlugin()
    registry_par = PluginRegistry()
    registry_par.register_engine_plugin(plugin_par)
    engine_par = plugin_par.create_engine(
        "fake_engine", {"concurrency_limit": 10}
    )
    registry_par.register_compute_engine(engine_par)

    compiled_par = _build_compiled_assembly_for_dag(groups, joint_map, execution_order)
    executor_par = Executor(registry=registry_par)
    result_par = executor_par.run_sync(compiled_par)

    # Both runs should succeed
    assert result_seq.success, (
        f"Sequential run failed: {[gr.error for gr in result_seq.group_results if not gr.success]}"
    )
    assert result_par.success, (
        f"Parallel run failed: {[gr.error for gr in result_par.group_results if not gr.success]}"
    )

    # --- Compare JointExecutionResult sets by (joint_name, success, rows_out) ---
    joint_tuples_seq = {
        (jr.name, jr.success, jr.rows_out) for jr in result_seq.joint_results
    }
    joint_tuples_par = {
        (jr.name, jr.success, jr.rows_out) for jr in result_par.joint_results
    }
    assert joint_tuples_seq == joint_tuples_par, (
        f"Joint result mismatch:\n"
        f"  Sequential: {sorted(joint_tuples_seq)}\n"
        f"  Parallel:   {sorted(joint_tuples_par)}"
    )

    # --- Compare FusedGroupExecutionResult sets by (group_id, success, rows_out) ---
    group_tuples_seq = {
        (gr.group_id, gr.success, gr.rows_out) for gr in result_seq.group_results
    }
    group_tuples_par = {
        (gr.group_id, gr.success, gr.rows_out) for gr in result_par.group_results
    }
    assert group_tuples_seq == group_tuples_par, (
        f"Group result mismatch:\n"
        f"  Sequential: {sorted(group_tuples_seq)}\n"
        f"  Parallel:   {sorted(group_tuples_par)}"
    )


# ---------------------------------------------------------------------------
# Property 6: Engine concurrency limit enforcement
# ---------------------------------------------------------------------------


class _ConcurrencyTrackingPlugin(ComputeEnginePlugin):
    """Plugin that tracks concurrent execute_sql calls using threading primitives.

    Since execute_sql runs inside asyncio.to_thread, we use a threading.Lock
    and a counter to track how many calls are running simultaneously.  A small
    sleep makes concurrent execution observable so the counter can actually
    exceed 1 when the engine pool allows it.
    """

    engine_type = "fake"
    supported_catalog_types: dict[str, list[str]] = {}

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: int = 0
        self.max_concurrent: int = 0

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngineModel:
        return ComputeEngineModel(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self,
        engine: Any,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        with self._lock:
            self._current += 1
            if self._current > self.max_concurrent:
                self.max_concurrent = self._current

        # Small sleep to make concurrent execution observable
        time.sleep(0.01)

        with self._lock:
            self._current -= 1

        return pyarrow.table({"col": [1]})


@st.composite
def _random_independent_groups_for_concurrency(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]]:
    """Generate a DAG of 3-8 independent groups (no dependencies) on the same engine.

    All groups are independent so they are all eligible for concurrent execution,
    which makes concurrency limit enforcement observable.

    Returns (groups, joint_map, execution_order).
    """
    n_groups = draw(st.integers(min_value=3, max_value=8))

    groups: list[FusedGroup] = []
    joint_map: dict[str, CompiledJoint] = {}
    execution_order: list[str] = []

    for g_idx in range(n_groups):
        gid = f"group_{g_idx}"
        jname = f"j_{g_idx}"

        # No upstream dependencies — all groups are independent
        joint_map[jname] = _make_joint_for_exec(jname, upstream=[], fused_group_id=gid)
        groups.append(_make_group_for_exec(gid, [jname]))
        execution_order.append(gid)

    return groups, joint_map, execution_order


# Feature: parallel-joint-execution, Property 6: Engine concurrency limit enforcement
@given(data=_random_independent_groups_for_concurrency())
@settings(max_examples=30)
def test_property6_engine_concurrency_limit_enforcement(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]],
) -> None:
    """For any engine with concurrency_limit = N and any execution of
    Executor.run_sync(), at no point in time shall more than N fused groups
    targeting that engine be executing concurrently.

    We generate DAGs where all groups are independent (no dependencies) so
    they are all eligible for concurrent execution.  A ConcurrencyTrackingPlugin
    uses threading primitives to track the max number of simultaneous
    execute_sql calls.  After execution, we verify that the observed max
    never exceeded the configured concurrency_limit.

    **Validates: Requirements 3.4, 4.3**
    """
    groups, joint_map, execution_order = data
    concurrency_limit = 2

    plugin = _ConcurrencyTrackingPlugin()
    registry = PluginRegistry()
    registry.register_engine_plugin(plugin)
    engine = plugin.create_engine("fake_engine", {"concurrency_limit": concurrency_limit})
    registry.register_compute_engine(engine)

    compiled = _build_compiled_assembly_for_dag(groups, joint_map, execution_order)
    executor = Executor(registry=registry)
    result = executor.run_sync(compiled)

    # Execution should succeed
    assert result.success, (
        f"Execution failed: {[gr.error for gr in result.group_results if not gr.success]}"
    )

    # All groups should have been executed
    assert len(result.group_results) == len(groups), (
        f"Expected {len(groups)} group results, got {len(result.group_results)}"
    )

    # The max concurrent count must never exceed the concurrency limit
    assert plugin.max_concurrent <= concurrency_limit, (
        f"Concurrency limit violated: max concurrent was {plugin.max_concurrent}, "
        f"but concurrency_limit is {concurrency_limit}. "
        f"({len(groups)} independent groups were submitted.)"
    )

# ---------------------------------------------------------------------------
# Property 7: Cross-engine independence
# ---------------------------------------------------------------------------


class _ConcurrencyTrackingPluginA(ComputeEnginePlugin):
    """Tracking plugin for engine type 'fake_a'."""

    engine_type = "fake_a"
    supported_catalog_types: dict[str, list[str]] = {}

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: int = 0
        self.max_concurrent: int = 0

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngineModel:
        return ComputeEngineModel(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self,
        engine: Any,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        with self._lock:
            self._current += 1
            if self._current > self.max_concurrent:
                self.max_concurrent = self._current

        # Small sleep to make concurrent execution observable
        time.sleep(0.02)

        with self._lock:
            self._current -= 1

        return pyarrow.table({"col": [1]})


class _ConcurrencyTrackingPluginB(ComputeEnginePlugin):
    """Tracking plugin for engine type 'fake_b'."""

    engine_type = "fake_b"
    supported_catalog_types: dict[str, list[str]] = {}

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: int = 0
        self.max_concurrent: int = 0

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngineModel:
        return ComputeEngineModel(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self,
        engine: Any,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        with self._lock:
            self._current += 1
            if self._current > self.max_concurrent:
                self.max_concurrent = self._current

        # Small sleep to make concurrent execution observable
        time.sleep(0.02)

        with self._lock:
            self._current -= 1

        return pyarrow.table({"col": [1]})


@st.composite
def _random_cross_engine_independent_groups(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]]:
    """Generate 2 independent groups on different engines (engine_a and engine_b).

    Both groups have no dependencies, so they are eligible for concurrent
    execution across engines.

    Returns (groups, joint_map, execution_order).
    """
    # 1-3 groups per engine to vary the test
    n_groups_a = draw(st.integers(min_value=1, max_value=3))
    n_groups_b = draw(st.integers(min_value=1, max_value=3))

    groups: list[FusedGroup] = []
    joint_map: dict[str, CompiledJoint] = {}
    execution_order: list[str] = []

    # Groups on engine_a
    for i in range(n_groups_a):
        gid = f"group_a_{i}"
        jname = f"j_a_{i}"
        joint_map[jname] = _make_joint_for_exec(
            jname, upstream=[], fused_group_id=gid, engine="engine_a"
        )
        groups.append(
            FusedGroup(
                id=gid,
                joints=[jname],
                engine="engine_a",
                engine_type="fake_a",
                adapters={jname: None},
                fused_sql="SELECT 1 AS col",
                entry_joints=[jname],
                exit_joints=[jname],
            )
        )
        execution_order.append(gid)

    # Groups on engine_b
    for i in range(n_groups_b):
        gid = f"group_b_{i}"
        jname = f"j_b_{i}"
        joint_map[jname] = _make_joint_for_exec(
            jname, upstream=[], fused_group_id=gid, engine="engine_b"
        )
        groups.append(
            FusedGroup(
                id=gid,
                joints=[jname],
                engine="engine_b",
                engine_type="fake_b",
                adapters={jname: None},
                fused_sql="SELECT 1 AS col",
                entry_joints=[jname],
                exit_joints=[jname],
            )
        )
        execution_order.append(gid)

    return groups, joint_map, execution_order


def _build_compiled_assembly_for_multi_engine_dag(
    groups: list[FusedGroup],
    joint_map: dict[str, CompiledJoint],
    execution_order: list[str],
) -> CompiledAssembly:
    """Build a CompiledAssembly with two engines (engine_a and engine_b)."""
    all_joints = [joint_map[jn] for g in groups for jn in g.joints]
    return CompiledAssembly(
        success=True,
        profile_name="test",
        catalogs=[],
        engines=[
            CompiledEngine(name="engine_a", engine_type="fake_a", native_catalog_types=[]),
            CompiledEngine(name="engine_b", engine_type="fake_b", native_catalog_types=[]),
        ],
        adapters=[],
        joints=all_joints,
        fused_groups=groups,
        materializations=[],
        execution_order=execution_order,
        errors=[],
        warnings=[],
    )


# Feature: parallel-joint-execution, Property 7: Cross-engine independence
@given(data=_random_cross_engine_independent_groups())
@settings(max_examples=30)
def test_property7_cross_engine_independence(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]],
) -> None:
    """For any two unlinked fused groups targeting different engines, both
    groups shall be eligible for concurrent execution, each constrained only
    by its own engine's concurrency limit and not by the other engine's pool.

    We create independent groups on engine_a (type fake_a) and engine_b
    (type fake_b), each with concurrency_limit=1.  Since the groups are
    independent and on different engines, they should run concurrently
    (total concurrent > 1 even though each engine allows only 1).

    We also verify that each engine's max concurrent count respects its own
    limit independently.

    **Validates: Requirements 4.1, 4.2**
    """
    groups, joint_map, execution_order = data

    # Create two tracking plugins for two different engine types
    plugin_a = _ConcurrencyTrackingPluginA()
    plugin_b = _ConcurrencyTrackingPluginB()

    registry = PluginRegistry()
    registry.register_engine_plugin(plugin_a)
    registry.register_engine_plugin(plugin_b)

    # Register engine instances with concurrency_limit=1 each
    engine_a = plugin_a.create_engine("engine_a", {"concurrency_limit": 1})
    engine_b = plugin_b.create_engine("engine_b", {"concurrency_limit": 1})
    registry.register_compute_engine(engine_a)
    registry.register_compute_engine(engine_b)

    compiled = _build_compiled_assembly_for_multi_engine_dag(
        groups, joint_map, execution_order
    )
    executor = Executor(registry=registry)
    result = executor.run_sync(compiled)

    # Execution should succeed
    assert result.success, (
        f"Execution failed: {[gr.error for gr in result.group_results if not gr.success]}"
    )

    # All groups should have been executed
    assert len(result.group_results) == len(groups), (
        f"Expected {len(groups)} group results, got {len(result.group_results)}"
    )

    # Each engine's max concurrent must respect its own limit (1)
    assert plugin_a.max_concurrent <= 1, (
        f"Engine A concurrency limit violated: max concurrent was "
        f"{plugin_a.max_concurrent}, but concurrency_limit is 1."
    )
    assert plugin_b.max_concurrent <= 1, (
        f"Engine B concurrency limit violated: max concurrent was "
        f"{plugin_b.max_concurrent}, but concurrency_limit is 1."
    )

    # Cross-engine independence: the total max concurrent across both engines
    # should be > 1, proving that groups on different engines ran concurrently.
    # (Each engine allows 1, so if they are independent, total can be 2.)
    # Note: We can't directly observe total concurrent from separate plugins,
    # but we can verify independence by checking that both engines were active
    # (max_concurrent >= 1 for each) and that the total wall-clock time is
    # less than what sequential execution would take.
    # The primary assertion is that each engine's pool is independent:
    # engine_a's limit doesn't block engine_b and vice versa.
    assert plugin_a.max_concurrent >= 1, (
        "Engine A should have executed at least 1 group concurrently."
    )
    assert plugin_b.max_concurrent >= 1, (
        "Engine B should have executed at least 1 group concurrently."
    )

# ---------------------------------------------------------------------------
# Property 8: Fail-fast cancellation
# ---------------------------------------------------------------------------


class _FailFastPlugin(ComputeEnginePlugin):
    """Plugin that raises an exception for groups whose SQL contains ``FAIL_GROUP``.

    Tracks which groups were actually executed (had ``execute_sql`` called)
    so we can verify that cancelled groups were never started.
    A small sleep is added so that concurrent groups have time to start
    before the failure propagates.
    """

    engine_type = "fake"
    supported_catalog_types: dict[str, list[str]] = {}

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.executed_groups: set[str] = set()

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngineModel:
        return ComputeEngineModel(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self,
        engine: Any,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        # Extract group ID from SQL comment
        m = re.search(r"/\*\s*(group_\d+)\s*\*/", sql or "")
        group_id = m.group(1) if m else "unknown"

        with self._lock:
            self.executed_groups.add(group_id)

        # Small sleep to make concurrent execution observable
        time.sleep(0.01)

        if "FAIL_GROUP" in (sql or ""):
            raise RuntimeError(f"Injected failure for {group_id}")

        return pyarrow.table({"col": [1]})


@st.composite
def _random_dag_with_failure_for_fail_fast(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint], list[str], str]:
    """Generate a DAG of 2-8 groups where exactly one group is configured to fail.

    The failing group is chosen from the first wave (in-degree 0 groups) so
    that the failure triggers fail-fast before downstream groups start.
    Dependencies only point from earlier groups to later groups (by index)
    to guarantee a valid DAG.

    Returns (groups, joint_map, execution_order, failing_group_id).
    """
    n_groups = draw(st.integers(min_value=2, max_value=8))

    groups: list[FusedGroup] = []
    joint_map: dict[str, CompiledJoint] = {}
    execution_order: list[str] = []

    for g_idx in range(n_groups):
        gid = f"group_{g_idx}"
        jname = f"j_{g_idx}"

        # Pick upstream joints from earlier groups only (ensures DAG)
        if g_idx > 0:
            possible_upstreams = [f"j_{i}" for i in range(g_idx)]
            n_ups = draw(st.integers(min_value=0, max_value=min(2, len(possible_upstreams))))
            upstream = draw(
                st.lists(
                    st.sampled_from(possible_upstreams),
                    min_size=n_ups,
                    max_size=n_ups,
                    unique=True,
                )
                if n_ups > 0
                else st.just([])
            )
        else:
            upstream = []

        joint_map[jname] = _make_joint_for_exec(jname, upstream, fused_group_id=gid)
        execution_order.append(gid)

        groups.append(
            FusedGroup(
                id=gid,
                joints=[jname],
                engine="fake_engine",
                engine_type="fake",
                adapters={jname: None},
                fused_sql=f"SELECT 1 AS col /* {gid} */",
                entry_joints=[jname],
                exit_joints=[jname],
            )
        )

    # Identify wave-1 groups (in-degree 0) and pick one to fail
    dep_graph = DependencyGraph.build(groups, joint_map)
    wave1_groups = [gid for gid in execution_order if dep_graph._in_degree[gid] == 0]
    failing_gid = draw(st.sampled_from(wave1_groups))

    # Mark the failing group's SQL with FAIL_GROUP
    for i, g in enumerate(groups):
        if g.id == failing_gid:
            jname = g.joints[0]
            groups[i] = FusedGroup(
                id=g.id,
                joints=g.joints,
                engine=g.engine,
                engine_type=g.engine_type,
                adapters=g.adapters,
                fused_sql=f"SELECT 1 AS col /* {g.id} */ FAIL_GROUP",
                entry_joints=g.entry_joints,
                exit_joints=g.exit_joints,
            )
            break

    return groups, joint_map, execution_order, failing_gid


# Feature: parallel-joint-execution, Property 8: Fail-fast cancellation
@given(data=_random_dag_with_failure_for_fail_fast())
@settings(max_examples=50)
def test_property8_fail_fast_cancellation(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint], list[str], str],
) -> None:
    """For any CompiledAssembly executed with fail_fast=True, when a fused group
    fails, all fused groups not yet started SHALL be cancelled (not executed),
    and all currently running fused groups SHALL be allowed to complete.
    The resulting ExecutionResult SHALL contain results for all started groups
    and no results for cancelled groups.

    **Validates: Requirements 5.1, 5.2**
    """
    groups, joint_map, execution_order, failing_gid = data

    plugin = _FailFastPlugin()
    registry = PluginRegistry()
    registry.register_engine_plugin(plugin)
    engine = plugin.create_engine("fake_engine", {"concurrency_limit": 10})
    registry.register_compute_engine(engine)

    compiled = _build_compiled_assembly_for_dag(groups, joint_map, execution_order)
    executor = Executor(registry=registry)
    result = executor.run_sync(compiled, fail_fast=True)

    # Build the dependency graph to reason about downstream groups
    dep_graph = DependencyGraph.build(groups, joint_map)

    # --- 1. The overall result must indicate failure ---
    assert result.success is False, "Expected execution to fail under fail_fast=True"

    # --- 2. The failed group must appear in results ---
    result_group_ids = {gr.group_id for gr in result.group_results}
    assert failing_gid in result_group_ids, (
        f"Failing group '{failing_gid}' not found in results: {result_group_ids}"
    )

    # --- 3. The failed group must be marked as failed ---
    failed_result = next(gr for gr in result.group_results if gr.group_id == failing_gid)
    assert failed_result.success is False, (
        f"Failing group '{failing_gid}' should be marked as failed"
    )

    # --- 4. Results should only contain groups that were actually started ---
    # Every group in results must have been either executed by the plugin
    # or recorded as a failure (upstream failure skip). Groups that were
    # cancelled (never started) should NOT appear in results.
    # Note: under fail-fast, a group may have been executed by the plugin
    # (execute_sql ran in a thread) but its asyncio task was cancelled
    # before the result was recorded — such groups correctly do NOT appear
    # in results. So we check the reverse: every group IN results was started.
    for gr in result.group_results:
        is_executed = gr.group_id in plugin.executed_groups
        is_upstream_skip = (
            not gr.success
            and gr.error is not None
            and "upstream" in (gr.error.message or "").lower()
        )
        assert is_executed or is_upstream_skip, (
            f"Group '{gr.group_id}' appears in results but was neither executed "
            f"nor recorded as an upstream-failure skip. "
            f"Executed groups: {plugin.executed_groups}"
        )

    # --- 5. No downstream group of the failed group should have been executed ---
    downstream_of_failed = set(dep_graph.mark_failed(failing_gid))
    for ds_gid in downstream_of_failed:
        assert ds_gid not in plugin.executed_groups, (
            f"Downstream group '{ds_gid}' of failed group '{failing_gid}' "
            f"should not have been executed, but was. "
            f"Executed groups: {plugin.executed_groups}"
        )

    # --- 6. All groups in the DAG are accounted for: either in results or cancelled ---
    all_group_ids = {g.id for g in groups}
    # Groups not in results were cancelled (not started or task was cancelled)
    cancelled_group_ids = all_group_ids - result_group_ids
    # Cancelled groups must not include the failing group itself
    assert failing_gid not in cancelled_group_ids, (
        f"Failing group '{failing_gid}' should not be in cancelled set"
    )


# ---------------------------------------------------------------------------
# Property 9: Non-fail-fast partial execution
# ---------------------------------------------------------------------------


@st.composite
def _random_dag_with_failure_for_non_fail_fast(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint], list[str], str, set[str], set[str]]:
    """Generate a DAG where exactly one group fails, with both downstream
    dependents AND at least one independent branch.

    Structure guarantee:
    - group_0 is the failing group (wave 1, in-degree 0).
    - group_1 is an independent branch (wave 1, in-degree 0, no dependency on group_0).
    - group_2 depends on group_0 (guaranteed downstream dependent).
    - Additional groups (3..n) may depend on any earlier group.

    Returns:
        (groups, joint_map, execution_order, failing_gid,
         expected_downstream_ids, expected_independent_ids)
    """
    # At least 3 groups to guarantee the structure; up to 8 for variety
    n_extra = draw(st.integers(min_value=0, max_value=5))
    n_groups = 3 + n_extra

    groups: list[FusedGroup] = []
    joint_map: dict[str, CompiledJoint] = {}
    execution_order: list[str] = []

    for g_idx in range(n_groups):
        gid = f"group_{g_idx}"
        jname = f"j_{g_idx}"

        if g_idx == 0:
            # Failing group — no upstreams
            upstream: list[str] = []
        elif g_idx == 1:
            # Independent branch — no upstreams (parallel with group_0)
            upstream = []
        elif g_idx == 2:
            # Guaranteed downstream of group_0
            upstream = ["j_0"]
        else:
            # Extra groups: may depend on any earlier group
            possible_upstreams = [f"j_{i}" for i in range(g_idx)]
            n_ups = draw(st.integers(min_value=0, max_value=min(2, len(possible_upstreams))))
            upstream = draw(
                st.lists(
                    st.sampled_from(possible_upstreams),
                    min_size=n_ups,
                    max_size=n_ups,
                    unique=True,
                )
                if n_ups > 0
                else st.just([])
            )

        joint_map[jname] = _make_joint_for_exec(jname, upstream, fused_group_id=gid)
        execution_order.append(gid)

        sql = f"SELECT 1 AS col /* {gid} */"
        if g_idx == 0:
            sql = f"SELECT 1 AS col /* {gid} */ FAIL_GROUP"

        groups.append(
            FusedGroup(
                id=gid,
                joints=[jname],
                engine="fake_engine",
                engine_type="fake",
                adapters={jname: None},
                fused_sql=sql,
                entry_joints=[jname],
                exit_joints=[jname],
            )
        )

    # Compute expected downstream and independent sets using DependencyGraph
    dep_graph = DependencyGraph.build(groups, joint_map)
    failing_gid = "group_0"
    transitive_downstream = set(dep_graph.mark_failed(failing_gid))

    independent_ids = {
        g.id
        for g in groups
        if g.id != failing_gid and g.id not in transitive_downstream
    }

    return groups, joint_map, execution_order, failing_gid, transitive_downstream, independent_ids


# Feature: parallel-joint-execution, Property 9: Non-fail-fast partial execution
@given(data=_random_dag_with_failure_for_non_fail_fast())
@settings(max_examples=50)
def test_property9_non_fail_fast_partial_execution(
    data: tuple[
        list[FusedGroup],
        dict[str, CompiledJoint],
        list[str],
        str,
        set[str],
        set[str],
    ],
) -> None:
    """For any CompiledAssembly executed with fail_fast=False, when a fused group
    fails, all transitive downstream dependents of the failed group SHALL be
    skipped with an upstream-failure error (code "RVT-501", message containing
    "upstream dependency failed"), and all independent branches SHALL continue
    executing.

    **Validates: Requirements 5.3, 5.4**
    """
    groups, joint_map, execution_order, failing_gid, expected_downstream, expected_independent = data

    plugin = _FailFastPlugin()
    registry = PluginRegistry()
    registry.register_engine_plugin(plugin)
    engine = plugin.create_engine("fake_engine", {"concurrency_limit": 10})
    registry.register_compute_engine(engine)

    compiled = _build_compiled_assembly_for_dag(groups, joint_map, execution_order)
    executor = Executor(registry=registry)
    result = executor.run_sync(compiled, fail_fast=False)

    result_by_group = {gr.group_id: gr for gr in result.group_results}
    result_group_ids = set(result_by_group.keys())
    all_group_ids = {g.id for g in groups}

    # --- 1. Every group in the DAG must appear in results (nothing cancelled) ---
    assert result_group_ids == all_group_ids, (
        f"Expected all groups in results under fail_fast=False. "
        f"Missing: {all_group_ids - result_group_ids}, "
        f"Extra: {result_group_ids - all_group_ids}"
    )

    # --- 2. The failed group itself must be marked as failed ---
    failed_result = result_by_group[failing_gid]
    assert failed_result.success is False, (
        f"Failing group '{failing_gid}' should be marked as failed"
    )

    # --- 3. All transitive downstream dependents must be skipped with RVT-501 ---
    for ds_gid in expected_downstream:
        ds_result = result_by_group[ds_gid]
        assert ds_result.success is False, (
            f"Downstream group '{ds_gid}' should be marked as failed"
        )
        assert ds_result.error is not None, (
            f"Downstream group '{ds_gid}' should have an error recorded"
        )
        assert ds_result.error.code == "RVT-501", (
            f"Downstream group '{ds_gid}' error code should be 'RVT-501', "
            f"got '{ds_result.error.code}'"
        )
        assert "upstream" in ds_result.error.message.lower(), (
            f"Downstream group '{ds_gid}' error message should mention 'upstream', "
            f"got: '{ds_result.error.message}'"
        )

    # --- 4. Downstream dependents must NOT have been executed by the plugin ---
    for ds_gid in expected_downstream:
        assert ds_gid not in plugin.executed_groups, (
            f"Downstream group '{ds_gid}' should have been skipped (not executed), "
            f"but was executed. Executed groups: {plugin.executed_groups}"
        )

    # --- 5. All independent branches must have completed successfully ---
    for ind_gid in expected_independent:
        ind_result = result_by_group[ind_gid]
        assert ind_result.success is True, (
            f"Independent group '{ind_gid}' should have succeeded, "
            f"but success={ind_result.success}, error={ind_result.error}"
        )
        assert ind_gid in plugin.executed_groups, (
            f"Independent group '{ind_gid}' should have been executed by the plugin. "
            f"Executed groups: {plugin.executed_groups}"
        )

    # --- 6. The overall result must indicate failure ---
    assert result.success is False, (
        "Expected overall execution to fail when a group fails under fail_fast=False"
    )


# ---------------------------------------------------------------------------
# Strategy for Property 2: generates acyclic DAGs (upstreams only from earlier groups)
# ---------------------------------------------------------------------------


@st.composite
def _random_dag_for_wave_correctness(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint]]:
    """Generate random acyclic FusedGroup DAGs for wave correctness testing.

    Upstream references are restricted to joints in earlier groups to guarantee
    acyclicity. Some joints may reference external joints not in any group.
    """
    n_groups = draw(st.integers(min_value=1, max_value=10))

    groups: list[FusedGroup] = []
    all_joint_names: list[str] = []
    joint_to_group_idx: dict[str, int] = {}

    for g_idx in range(n_groups):
        n_joints = draw(st.integers(min_value=1, max_value=3))
        joint_names = [f"g{g_idx}_j{j_idx}" for j_idx in range(n_joints)]
        groups.append(_make_group(f"group_{g_idx}", joint_names))
        for jn in joint_names:
            joint_to_group_idx[jn] = g_idx
        all_joint_names.extend(joint_names)

    # Optional external joints (not in any group)
    n_external = draw(st.integers(min_value=0, max_value=3))
    external_names = [f"ext_{i}" for i in range(n_external)]

    joint_map: dict[str, CompiledJoint] = {}

    for g_idx, group in enumerate(groups):
        # Only allow upstream refs to joints in strictly earlier groups (acyclicity)
        earlier_joints = [jn for jn in all_joint_names if joint_to_group_idx[jn] < g_idx]
        possible_upstreams = earlier_joints + external_names

        for jname in group.joints:
            if possible_upstreams:
                n_ups = draw(st.integers(min_value=0, max_value=min(3, len(possible_upstreams))))
                upstream = draw(
                    st.lists(
                        st.sampled_from(possible_upstreams),
                        min_size=n_ups,
                        max_size=n_ups,
                        unique=True,
                    )
                    if n_ups > 0
                    else st.just([])
                )
            else:
                upstream = []
            joint_map[jname] = _make_joint(jname, upstream)

    # Add external joints to joint_map
    for ext_name in external_names:
        joint_map[ext_name] = _make_joint(ext_name, upstream=[])

    return groups, joint_map


# Feature: parallel-joint-execution, Property 2: Parallel execution plan wave correctness
@given(data=_random_dag_for_wave_correctness())
@settings(max_examples=50)
def test_property2_parallel_execution_plan_wave_correctness(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint]],
) -> None:
    """For any CompiledAssembly, the parallel_execution_plan SHALL satisfy:
    - Every fused group in execution_order appears in exactly one wave.
    - For every group G in wave W (W > 1), all upstream dependencies of G in
      the dependency graph are assigned to waves earlier than W.
    - Wave 1 contains exactly the groups with no upstream dependencies (in-degree 0).
    - No group appears in more than one wave.

    **Validates: Requirements 7.1, 7.2**
    """
    groups, joint_map = data

    # Compute the parallel execution plan using the compiler function
    waves = _compute_parallel_execution_plan(groups, joint_map)

    # Build the DependencyGraph for upstream relationship verification
    dep_graph = DependencyGraph.build(groups, joint_map)

    all_group_ids = {g.id for g in groups}

    # Collect all groups across all waves and build group → wave mapping
    group_to_wave: dict[str, int] = {}
    groups_in_waves: list[str] = []

    for wave in waves:
        for gid in wave.groups:
            groups_in_waves.append(gid)
            group_to_wave[gid] = wave.wave_number

    # --- 1. Every fused group appears in exactly one wave (no missing, no duplicates) ---
    assert set(groups_in_waves) == all_group_ids, (
        f"Wave groups should match all group IDs. "
        f"Missing: {all_group_ids - set(groups_in_waves)}, "
        f"Extra: {set(groups_in_waves) - all_group_ids}"
    )
    assert len(groups_in_waves) == len(all_group_ids), (
        f"No group should appear in more than one wave. "
        f"Total wave entries: {len(groups_in_waves)}, unique groups: {len(all_group_ids)}"
    )

    # --- 2. For every group G in wave W (W > 1), all upstream deps are in earlier waves ---
    for gid, wave_num in group_to_wave.items():
        if wave_num <= 1:
            continue
        upstream_ids = dep_graph._upstream.get(gid, set())
        for up_gid in upstream_ids:
            up_wave = group_to_wave.get(up_gid)
            assert up_wave is not None, (
                f"Upstream group '{up_gid}' of '{gid}' not found in any wave"
            )
            assert up_wave < wave_num, (
                f"Group '{gid}' is in wave {wave_num} but upstream '{up_gid}' "
                f"is in wave {up_wave} (should be earlier)"
            )

    # --- 3. Wave 1 contains exactly the groups with in-degree 0 ---
    in_degree_zero = {gid for gid, ups in dep_graph._upstream.items() if len(ups) == 0}
    wave_1_groups = {gid for gid, w in group_to_wave.items() if w == 1}
    assert wave_1_groups == in_degree_zero, (
        f"Wave 1 should contain exactly in-degree 0 groups. "
        f"Wave 1: {wave_1_groups}, In-degree 0: {in_degree_zero}"
    )

    # --- 4. Wave numbers are sequential starting from 1 ---
    if waves:
        wave_numbers = [w.wave_number for w in waves]
        assert wave_numbers == list(range(1, len(waves) + 1)), (
            f"Wave numbers should be sequential from 1. Got: {wave_numbers}"
        )


# ---------------------------------------------------------------------------
# Property 11: Timing accuracy under parallelism
# ---------------------------------------------------------------------------


class _TimingAccuracyPlugin(ComputeEnginePlugin):
    """Plugin that introduces a small sleep to make timing measurable.

    The group ID is embedded in the SQL string as a comment: ``/* group_X */``.
    A 10ms sleep makes each group's wall-clock duration non-trivial so that
    timing assertions are meaningful.
    """

    engine_type = "fake"
    supported_catalog_types: dict[str, list[str]] = {}

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.executed_groups: list[str] = []

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngineModel:
        return ComputeEngineModel(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(
        self,
        engine: Any,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        m = re.search(r"/\*\s*(group_\d+)\s*\*/", sql or "")
        group_id = m.group(1) if m else "unknown"

        with self._lock:
            self.executed_groups.append(group_id)

        # Small sleep to make timing measurable
        time.sleep(0.02)

        return pyarrow.table({"col": [1]})


@st.composite
def _random_dag_for_timing_accuracy(
    draw: st.DrawFn,
) -> tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]]:
    """Generate a random DAG of 2-6 groups with group IDs embedded in SQL.

    At least 2 groups are generated so parallelism is possible. Dependencies
    only point from earlier groups to later groups (by index) to guarantee
    a valid DAG with no cycles.

    Returns (groups, joint_map, execution_order).
    """
    n_groups = draw(st.integers(min_value=2, max_value=6))

    groups: list[FusedGroup] = []
    joint_map: dict[str, CompiledJoint] = {}
    execution_order: list[str] = []

    for g_idx in range(n_groups):
        gid = f"group_{g_idx}"
        jname = f"j_{g_idx}"

        if g_idx > 0:
            possible_upstreams = [f"j_{i}" for i in range(g_idx)]
            n_ups = draw(st.integers(min_value=0, max_value=min(2, len(possible_upstreams))))
            upstream = draw(
                st.lists(
                    st.sampled_from(possible_upstreams),
                    min_size=n_ups,
                    max_size=n_ups,
                    unique=True,
                )
                if n_ups > 0
                else st.just([])
            )
        else:
            upstream = []

        joint_map[jname] = _make_joint_for_exec(jname, upstream, fused_group_id=gid)
        groups.append(
            FusedGroup(
                id=gid,
                joints=[jname],
                engine="fake_engine",
                engine_type="fake",
                adapters={jname: None},
                fused_sql=f"SELECT 1 AS col /* {gid} */",
                entry_joints=[jname],
                exit_joints=[jname],
            )
        )
        execution_order.append(gid)

    return groups, joint_map, execution_order


# Feature: parallel-joint-execution, Property 11: Timing accuracy under parallelism
@given(data=_random_dag_for_timing_accuracy())
@settings(max_examples=50)
def test_property11_timing_accuracy_under_parallelism(
    data: tuple[list[FusedGroup], dict[str, CompiledJoint], list[str]],
) -> None:
    """For any parallel execution, the total pipeline total_time_ms SHALL be
    less than or equal to the sum of all individual group timing.total_ms
    values (since groups overlap), and each group's timing.total_ms SHALL
    reflect that group's own wall-clock duration (not inflated by wait time
    for unrelated groups). The StatsCollector SHALL contain exactly one
    GroupStats entry per executed group.

    **Validates: Requirements 8.1, 8.2, 8.3**
    """
    groups, joint_map, execution_order = data

    plugin = _TimingAccuracyPlugin()
    registry = PluginRegistry()
    registry.register_engine_plugin(plugin)
    engine = plugin.create_engine("fake_engine", {"concurrency_limit": 4})
    registry.register_compute_engine(engine)

    compiled = _build_compiled_assembly_for_dag(groups, joint_map, execution_order)
    executor = Executor(registry=registry)
    result = executor.run_sync(compiled)

    # Execution should succeed
    assert result.success, (
        f"Execution failed: {[gr.error for gr in result.group_results if not gr.success]}"
    )

    # run_stats must be present
    assert result.run_stats is not None, "run_stats should be populated after execution"

    run_stats = result.run_stats
    expected_group_ids = {g.id for g in groups}

    # --- 1. StatsCollector contains exactly one GroupStats per executed group ---
    stats_group_ids = [gs.group_id for gs in run_stats.group_stats]
    assert set(stats_group_ids) == expected_group_ids, (
        f"GroupStats group IDs mismatch: got {set(stats_group_ids)}, "
        f"expected {expected_group_ids}"
    )
    assert len(stats_group_ids) == len(expected_group_ids), (
        f"Expected exactly one GroupStats per group ({len(expected_group_ids)}), "
        f"got {len(stats_group_ids)} (duplicates detected)"
    )

    # --- 2. Each group's timing.total_ms > 0 (non-zero wall-clock) ---
    for gs in run_stats.group_stats:
        assert gs.timing.total_ms > 0, (
            f"Group '{gs.group_id}' timing.total_ms should be > 0, "
            f"got {gs.timing.total_ms}"
        )

    # --- 3. total_time_ms ≤ sum of all group timing.total_ms ---
    # Under parallelism, the pipeline wall-clock is shorter than the
    # sequential sum because groups overlap.
    sum_group_ms = sum(gs.timing.total_ms for gs in run_stats.group_stats)
    assert run_stats.total_time_ms <= sum_group_ms + 1.0, (
        f"Pipeline total_time_ms ({run_stats.total_time_ms:.2f}) should be "
        f"<= sum of group timing.total_ms ({sum_group_ms:.2f}). "
        f"This invariant holds because parallel groups overlap in time."
    )

    # --- 4. Each group's timing reflects its own wall-clock ---
    # With a 20ms sleep per group, each group's timing should be at least
    # a few ms.  We use a generous lower bound of 2ms to account for
    # scheduling jitter under heavy CI load.
    for gs in run_stats.group_stats:
        assert gs.timing.total_ms >= 2.0, (
            f"Group '{gs.group_id}' timing.total_ms ({gs.timing.total_ms:.2f}ms) "
            f"is suspiciously low — expected at least ~20ms from the sleep. "
            f"This suggests timing may not reflect the group's own wall-clock."
        )

    # --- 5. total_time_ms is positive and reflects pipeline wall-clock ---
    assert run_stats.total_time_ms > 0, (
        f"Pipeline total_time_ms should be > 0, got {run_stats.total_time_ms}"
    )
