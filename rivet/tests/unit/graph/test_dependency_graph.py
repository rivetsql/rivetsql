"""Unit tests for DependencyGraph."""

from __future__ import annotations

from rivet_core.compiler import CompiledJoint
from rivet_core.executor import DependencyGraph
from rivet_core.optimizer import FusedGroup


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


class TestDependencyGraphBuild:
    """Tests for DependencyGraph.build()."""

    def test_single_group_no_deps(self):
        """Single group with no upstream produces a graph with one node and no edges."""
        groups = [_make_group("g1", ["j1"])]
        joint_map = {"j1": _make_joint("j1", upstream=[])}

        graph = DependencyGraph.build(groups, joint_map)

        assert graph._upstream == {"g1": set()}
        assert graph._downstream == {"g1": set()}
        assert graph._in_degree == {"g1": 0}

    def test_linear_chain(self):
        """A -> B -> C produces edges A->B and B->C."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
            _make_group("gC", ["jC"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
            "jC": _make_joint("jC", upstream=["jB"]),
        }

        graph = DependencyGraph.build(groups, joint_map)

        assert graph._upstream == {"gA": set(), "gB": {"gA"}, "gC": {"gB"}}
        assert graph._downstream == {"gA": {"gB"}, "gB": {"gC"}, "gC": set()}
        assert graph._in_degree == {"gA": 0, "gB": 1, "gC": 1}

    def test_diamond_dag(self):
        """Diamond: A -> B, A -> C, B -> D, C -> D."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
            _make_group("gC", ["jC"]),
            _make_group("gD", ["jD"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
            "jC": _make_joint("jC", upstream=["jA"]),
            "jD": _make_joint("jD", upstream=["jB", "jC"]),
        }

        graph = DependencyGraph.build(groups, joint_map)

        assert graph._upstream["gD"] == {"gB", "gC"}
        assert graph._downstream["gA"] == {"gB", "gC"}
        assert graph._in_degree == {"gA": 0, "gB": 1, "gC": 1, "gD": 2}

    def test_upstream_ref_outside_any_group_ignored(self):
        """Upstream ref to a joint not in any group creates no edge (Req 1.3)."""
        groups = [_make_group("g1", ["j1"])]
        # j1 references "external_joint" which is not in any group
        joint_map = {"j1": _make_joint("j1", upstream=["external_joint"])}

        graph = DependencyGraph.build(groups, joint_map)

        assert graph._upstream == {"g1": set()}
        assert graph._downstream == {"g1": set()}
        assert graph._in_degree == {"g1": 0}

    def test_self_reference_within_group_ignored(self):
        """Joints within the same group referencing each other create no edge."""
        groups = [_make_group("g1", ["j1", "j2"])]
        joint_map = {
            "j1": _make_joint("j1", upstream=[]),
            "j2": _make_joint("j2", upstream=["j1"]),  # same group
        }

        graph = DependencyGraph.build(groups, joint_map)

        assert graph._upstream == {"g1": set()}
        assert graph._in_degree == {"g1": 0}

    def test_all_groups_present(self):
        """Graph contains every fused group, even those with no edges."""
        groups = [
            _make_group("g1", ["j1"]),
            _make_group("g2", ["j2"]),
            _make_group("g3", ["j3"]),
        ]
        joint_map = {
            "j1": _make_joint("j1", upstream=[]),
            "j2": _make_joint("j2", upstream=[]),
            "j3": _make_joint("j3", upstream=[]),
        }

        graph = DependencyGraph.build(groups, joint_map)

        assert set(graph._in_degree.keys()) == {"g1", "g2", "g3"}

    def test_empty_groups(self):
        """Empty fused_groups produces an empty graph."""
        graph = DependencyGraph.build([], {})

        assert graph._upstream == {}
        assert graph._downstream == {}
        assert graph._in_degree == {}


class TestReadyGroups:
    """Tests for DependencyGraph.ready_groups()."""

    def test_all_independent_groups_ready(self):
        """All groups with no deps are ready."""
        groups = [
            _make_group("g1", ["j1"]),
            _make_group("g2", ["j2"]),
        ]
        joint_map = {
            "j1": _make_joint("j1", upstream=[]),
            "j2": _make_joint("j2", upstream=[]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        ready = graph.ready_groups()
        assert set(ready) == {"g1", "g2"}

    def test_submitted_groups_excluded(self):
        """Groups already submitted are not returned."""
        groups = [
            _make_group("g1", ["j1"]),
            _make_group("g2", ["j2"]),
        ]
        joint_map = {
            "j1": _make_joint("j1", upstream=[]),
            "j2": _make_joint("j2", upstream=[]),
        }
        graph = DependencyGraph.build(groups, joint_map)
        graph._submitted.add("g1")

        ready = graph.ready_groups()
        assert ready == ["g2"]

    def test_dependent_group_not_ready(self):
        """Group with unsatisfied deps is not ready."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        ready = graph.ready_groups()
        assert ready == ["gA"]


class TestMarkComplete:
    """Tests for DependencyGraph.mark_complete()."""

    def test_decrements_downstream_in_degree(self):
        """Completing a group decrements downstream in-degrees."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        newly_ready = graph.mark_complete("gA")
        assert "gB" in newly_ready
        assert graph._in_degree["gB"] == 0
        assert "gA" in graph._completed

    def test_diamond_needs_both_parents(self):
        """Diamond: D only becomes ready after both B and C complete."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
            _make_group("gC", ["jC"]),
            _make_group("gD", ["jD"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
            "jC": _make_joint("jC", upstream=["jA"]),
            "jD": _make_joint("jD", upstream=["jB", "jC"]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        # Complete A -> B and C become ready
        graph.mark_complete("gA")
        assert graph._in_degree["gD"] == 2  # still waiting

        # Complete B -> D still waiting on C
        newly_ready = graph.mark_complete("gB")
        assert "gD" not in newly_ready
        assert graph._in_degree["gD"] == 1

        # Complete C -> D now ready
        newly_ready = graph.mark_complete("gC")
        assert "gD" in newly_ready
        assert graph._in_degree["gD"] == 0

    def test_already_submitted_not_returned(self):
        """If a downstream group was already submitted, it's not in newly_ready."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
        }
        graph = DependencyGraph.build(groups, joint_map)
        graph._submitted.add("gB")

        newly_ready = graph.mark_complete("gA")
        assert "gB" not in newly_ready


class TestMarkFailed:
    """Tests for DependencyGraph.mark_failed()."""

    def test_collects_direct_downstream(self):
        """Failing a group returns its direct downstream."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        failed_downstream = graph.mark_failed("gA")
        assert set(failed_downstream) == {"gB"}

    def test_collects_transitive_downstream(self):
        """Failing a group returns all transitive downstream."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
            _make_group("gC", ["jC"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
            "jC": _make_joint("jC", upstream=["jB"]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        failed_downstream = graph.mark_failed("gA")
        assert set(failed_downstream) == {"gB", "gC"}

    def test_diamond_transitive_downstream(self):
        """Diamond: failing A returns B, C, D."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
            _make_group("gC", ["jC"]),
            _make_group("gD", ["jD"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
            "jC": _make_joint("jC", upstream=["jA"]),
            "jD": _make_joint("jD", upstream=["jB", "jC"]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        failed_downstream = graph.mark_failed("gA")
        assert set(failed_downstream) == {"gB", "gC", "gD"}

    def test_leaf_node_failure_returns_empty(self):
        """Failing a leaf node returns empty list."""
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        failed_downstream = graph.mark_failed("gB")
        assert failed_downstream == []

    def test_independent_branch_not_affected(self):
        """Failing one branch doesn't affect independent branches."""
        # A -> B, C -> D (two independent chains)
        groups = [
            _make_group("gA", ["jA"]),
            _make_group("gB", ["jB"]),
            _make_group("gC", ["jC"]),
            _make_group("gD", ["jD"]),
        ]
        joint_map = {
            "jA": _make_joint("jA", upstream=[]),
            "jB": _make_joint("jB", upstream=["jA"]),
            "jC": _make_joint("jC", upstream=[]),
            "jD": _make_joint("jD", upstream=["jC"]),
        }
        graph = DependencyGraph.build(groups, joint_map)

        failed_downstream = graph.mark_failed("gA")
        assert set(failed_downstream) == {"gB"}
        # gC and gD are not affected
