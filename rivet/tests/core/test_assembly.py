"""Unit tests for Assembly (DAG) construction and validation (task 7.2)."""

from __future__ import annotations

import pytest

from rivet_core.assembly import Assembly, AssemblyError
from rivet_core.models import Joint

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def src(name: str, tags: list[str] | None = None) -> Joint:
    return Joint(name=name, joint_type="source", tags=tags or [])


def sql(name: str, upstream: list[str], tags: list[str] | None = None) -> Joint:
    return Joint(name=name, joint_type="sql", upstream=upstream, tags=tags or [])


def sink(name: str, upstream: list[str], tags: list[str] | None = None) -> Joint:
    return Joint(name=name, joint_type="sink", upstream=upstream, tags=tags or [])


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------

class TestCycleDetection:
    def test_direct_self_loop_raises(self) -> None:
        # A → A
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([
                Joint(name="a", joint_type="sql", upstream=["a"]),
            ])
        err = exc_info.value.error
        assert err.code == "RVT-305"
        assert "a" in err.message

    def test_two_node_cycle_raises(self) -> None:
        # a → b → a
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([
                sql("a", upstream=["b"]),
                sql("b", upstream=["a"]),
            ])
        err = exc_info.value.error
        assert err.code == "RVT-305"
        assert "cycle" in err.message.lower()

    def test_three_node_cycle_raises(self) -> None:
        # src → a → b → a (cycle between a and b)
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([
                src("s"),
                sql("a", upstream=["s", "b"]),
                sql("b", upstream=["a"]),
            ])
        err = exc_info.value.error
        assert err.code == "RVT-305"

    def test_cycle_error_names_nodes_in_cycle(self) -> None:
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([
                sql("x", upstream=["y"]),
                sql("y", upstream=["x"]),
            ])
        err = exc_info.value.error
        assert "x" in err.message or "y" in err.message
        assert err.remediation is not None

    def test_acyclic_graph_no_error(self) -> None:
        # s → a → b → out (no cycle)
        asm = Assembly([src("s"), sql("a", ["s"]), sql("b", ["a"]), sink("out", ["b"])])
        assert set(asm.joints.keys()) == {"s", "a", "b", "out"}


# ---------------------------------------------------------------------------
# Upstream reference validation
# ---------------------------------------------------------------------------

class TestUpstreamReferenceValidation:
    def test_unknown_upstream_raises(self) -> None:
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([sql("a", upstream=["nonexistent"])])
        err = exc_info.value.error
        assert err.code == "RVT-302"
        assert "nonexistent" in err.message

    def test_error_includes_joint_and_upstream_names(self) -> None:
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([sql("child", upstream=["missing_parent"])])
        err = exc_info.value.error
        assert "child" in err.message
        assert "missing_parent" in err.message
        assert err.remediation is not None

    def test_valid_upstream_reference_no_error(self) -> None:
        asm = Assembly([src("s"), sql("t", ["s"])])
        assert "t" in asm.joints


# ---------------------------------------------------------------------------
# Upstream constraints per joint type
# ---------------------------------------------------------------------------

class TestUpstreamConstraints:
    def test_source_with_upstream_raises(self) -> None:
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([src("s"), Joint(name="bad_src", joint_type="source", upstream=["s"])])
        err = exc_info.value.error
        assert err.code == "RVT-303"
        assert "bad_src" in err.message

    def test_sink_without_upstream_raises(self) -> None:
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([Joint(name="orphan_sink", joint_type="sink", upstream=[])])
        err = exc_info.value.error
        assert err.code == "RVT-304"
        assert "orphan_sink" in err.message

    def test_sql_joint_no_upstream_allowed(self) -> None:
        # sql joints may have zero upstream (e.g. literal queries)
        asm = Assembly([sql("q", upstream=[])])
        assert "q" in asm.joints

    def test_python_joint_with_upstream_allowed(self) -> None:
        asm = Assembly([src("s"), Joint(name="py", joint_type="python", upstream=["s"])])
        assert "py" in asm.joints

    def test_duplicate_joint_name_raises(self) -> None:
        with pytest.raises(AssemblyError) as exc_info:
            Assembly([src("dup"), src("dup")])
        err = exc_info.value.error
        assert err.code == "RVT-301"
        assert "dup" in err.message


# ---------------------------------------------------------------------------
# Topological ordering
# ---------------------------------------------------------------------------

class TestTopologicalOrder:
    def test_linear_chain(self) -> None:
        asm = Assembly([src("a"), sql("b", ["a"]), sink("c", ["b"])])
        order = asm.topological_order()
        assert order.index("a") < order.index("b")
        assert order.index("b") < order.index("c")

    def test_all_joints_included(self) -> None:
        asm = Assembly([src("a"), sql("b", ["a"]), sink("c", ["b"])])
        assert set(asm.topological_order()) == {"a", "b", "c"}

    def test_diamond_dag(self) -> None:
        # s → left, s → right, left+right → out
        asm = Assembly([
            src("s"),
            sql("left", ["s"]),
            sql("right", ["s"]),
            sink("out", ["left", "right"]),
        ])
        order = asm.topological_order()
        assert order.index("s") < order.index("left")
        assert order.index("s") < order.index("right")
        assert order.index("left") < order.index("out")
        assert order.index("right") < order.index("out")

    def test_deterministic_alphabetical_tiebreak(self) -> None:
        # Two independent sources: "alpha" and "beta" — alpha should come first
        asm = Assembly([src("beta"), src("alpha")])
        order = asm.topological_order()
        assert order.index("alpha") < order.index("beta")

    def test_single_joint(self) -> None:
        asm = Assembly([src("only")])
        assert asm.topological_order() == ["only"]

    def test_empty_assembly(self) -> None:
        asm = Assembly([])
        assert asm.topological_order() == []


# ---------------------------------------------------------------------------
# Subgraph — target_sink pruning
# ---------------------------------------------------------------------------

class TestSubgraphTargetSink:
    def _pipeline(self) -> Assembly:
        # s1 → a → out1
        # s2 → b → out2
        return Assembly([
            src("s1"), sql("a", ["s1"]), sink("out1", ["a"]),
            src("s2"), sql("b", ["s2"]), sink("out2", ["b"]),
        ])

    def test_target_sink_prunes_unrelated_joints(self) -> None:
        sub = self._pipeline().subgraph(target_sink="out1")
        assert set(sub.joints.keys()) == {"s1", "a", "out1"}

    def test_target_sink_includes_all_upstream(self) -> None:
        sub = self._pipeline().subgraph(target_sink="out2")
        assert set(sub.joints.keys()) == {"s2", "b", "out2"}

    def test_unknown_target_sink_raises(self) -> None:
        with pytest.raises(AssemblyError) as exc_info:
            self._pipeline().subgraph(target_sink="ghost")
        err = exc_info.value.error
        assert err.code == "RVT-306"
        assert "ghost" in err.message

    def test_no_filter_returns_full_assembly(self) -> None:
        asm = self._pipeline()
        sub = asm.subgraph()
        assert set(sub.joints.keys()) == set(asm.joints.keys())


# ---------------------------------------------------------------------------
# Subgraph — tag filtering
# ---------------------------------------------------------------------------

class TestSubgraphTagFiltering:
    def _tagged_pipeline(self) -> Assembly:
        # s (daily) → a (daily, finance) → out (finance)
        # s2 (weekly) → b (weekly) → out2 (weekly)
        return Assembly([
            src("s", tags=["daily"]),
            sql("a", ["s"], tags=["daily", "finance"]),
            sink("out", ["a"], tags=["finance"]),
            src("s2", tags=["weekly"]),
            sql("b", ["s2"], tags=["weekly"]),
            sink("out2", ["b"], tags=["weekly"]),
        ])

    def test_or_mode_matches_any_tag(self) -> None:
        sub = self._tagged_pipeline().subgraph(tags=["finance"], tag_mode="or")
        # "a" and "out" match finance; their upstream "s" is included
        assert "a" in sub.joints
        assert "out" in sub.joints
        assert "s" in sub.joints
        assert "b" not in sub.joints
        assert "out2" not in sub.joints

    def test_and_mode_requires_all_tags(self) -> None:
        sub = self._tagged_pipeline().subgraph(tags=["daily", "finance"], tag_mode="and")
        # Only "a" has both tags; upstream "s" is included
        assert "a" in sub.joints
        assert "s" in sub.joints
        assert "out" not in sub.joints  # only has "finance"
        assert "b" not in sub.joints

    def test_or_mode_is_default(self) -> None:
        asm = self._tagged_pipeline()
        sub_explicit = asm.subgraph(tags=["weekly"], tag_mode="or")
        sub_default = asm.subgraph(tags=["weekly"])
        assert set(sub_explicit.joints.keys()) == set(sub_default.joints.keys())

    def test_tag_filter_includes_upstream_deps(self) -> None:
        # "out" has tag "finance" but its upstream "a" and "s" have no finance tag
        # They should still be included as upstream deps
        sub = self._tagged_pipeline().subgraph(tags=["finance"], tag_mode="or")
        assert "s" in sub.joints  # upstream dep of "a"

    def test_no_matching_tags_returns_empty_assembly(self) -> None:
        sub = self._tagged_pipeline().subgraph(tags=["nonexistent"])
        assert len(sub.joints) == 0


# ---------------------------------------------------------------------------
# Subgraph — combined target_sink + tags
# ---------------------------------------------------------------------------

class TestSubgraphCombined:
    def test_target_sink_and_tags_intersection(self) -> None:
        # s1 → a (daily) → out1
        # s2 → b (weekly) → out1 (out1 has two upstreams)
        asm = Assembly([
            src("s1"),
            sql("a", ["s1"], tags=["daily"]),
            src("s2"),
            sql("b", ["s2"], tags=["weekly"]),
            sink("out1", ["a", "b"]),
        ])
        # target_sink=out1 gives {s1, a, s2, b, out1}
        # tags=["daily"] within that set matches "a" → include "a" + upstream "s1"
        sub = asm.subgraph(target_sink="out1", tags=["daily"])
        assert "a" in sub.joints
        assert "s1" in sub.joints
        assert "b" not in sub.joints
        assert "s2" not in sub.joints
        assert "out1" not in sub.joints  # out1 has no "daily" tag
