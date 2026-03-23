"""Integration tests: compiler phase pipeline with real DuckDB engine plugin.

Exercises the full 10-phase compilation pipeline and the ``compile_until()``
API using real DuckDB engine and filesystem catalog plugins — no mocks for
Rivet internals.
"""

from __future__ import annotations

import pytest

from rivet_core.assembly import Assembly
from rivet_core.compiler import (
    PHASE_COMPILE_SQL,
    PHASE_ENGINE_BOUNDARIES,
    PHASE_FINALIZATION,
    PHASE_FUSION,
    PHASE_INTROSPECT_SOURCES,
    PHASE_MATERIALIZATION,
    PHASE_OPTIMIZATION,
    PHASE_PRUNE_DAG,
    PHASE_RESOLVE_METADATA,
    PHASE_STRATEGY_RESOLUTION,
    CompiledAssembly,
    CompileOptions,
    PhaseState,
    compile,
    compile_until,
)
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin

ALL_PHASES = (
    PHASE_PRUNE_DAG,
    PHASE_RESOLVE_METADATA,
    PHASE_INTROSPECT_SOURCES,
    PHASE_COMPILE_SQL,
    PHASE_FUSION,
    PHASE_OPTIMIZATION,
    PHASE_STRATEGY_RESOLUTION,
    PHASE_ENGINE_BOUNDARIES,
    PHASE_MATERIALIZATION,
    PHASE_FINALIZATION,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg


def _make_catalogs() -> list[Catalog]:
    return [
        Catalog(name="local", type="filesystem", options={"path": "/tmp/fake", "format": "csv"})
    ]


def _make_engines(registry: PluginRegistry) -> list[ComputeEngine]:
    return [registry.get_engine_plugin("duckdb").create_engine("duckdb_primary", {})]


def _linear_joints() -> list[Joint]:
    """Source → SQL → Sink linear pipeline."""
    return [
        Joint(name="src", joint_type="source", catalog="local", table="orders"),
        Joint(
            name="transform",
            joint_type="sql",
            upstream=["src"],
            sql="SELECT id, amount FROM src WHERE amount > 0",
        ),
        Joint(
            name="sink", joint_type="sink", catalog="local", table="output", upstream=["transform"]
        ),
    ]


def _diamond_joints() -> list[Joint]:
    """Source → two transforms → merge → sink (diamond shape)."""
    return [
        Joint(name="src", joint_type="source", catalog="local", table="data"),
        Joint(
            name="left",
            joint_type="sql",
            upstream=["src"],
            sql="SELECT id, amount FROM src WHERE amount > 0",
        ),
        Joint(
            name="right",
            joint_type="sql",
            upstream=["src"],
            sql="SELECT id, amount FROM src WHERE amount <= 0",
        ),
        Joint(
            name="merged",
            joint_type="sql",
            upstream=["left", "right"],
            sql="SELECT * FROM left UNION ALL SELECT * FROM right",
        ),
        Joint(name="sink", joint_type="sink", catalog="local", table="output", upstream=["merged"]),
    ]


def _compile_pipeline(joints: list[Joint], *, introspect: bool = False) -> CompiledAssembly:
    registry = _setup_registry()
    return compile(
        Assembly(joints),
        catalogs=_make_catalogs(),
        engines=_make_engines(registry),
        registry=registry,
        default_engine="duckdb_primary",
        introspect=introspect,
    )


def _compile_until_phase(
    joints: list[Joint],
    stop_after: str,
    *,
    introspect: bool = False,
) -> PhaseState:
    registry = _setup_registry()
    return compile_until(
        Assembly(joints),
        catalogs=_make_catalogs(),
        engines=_make_engines(registry),
        registry=registry,
        stop_after=stop_after,
        options=CompileOptions(
            default_engine="duckdb_primary",
            introspect=introspect,
        ),
    )


# ---------------------------------------------------------------------------
# Tests: Full pipeline equivalence
# ---------------------------------------------------------------------------


class TestFullPipelineEquivalence:
    """Pipeline produces same result as compile() for multi-joint pipelines."""

    def test_linear_pipeline_round_trip(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        assert len(result.joints) == 3
        assert len(result.fused_groups) >= 1
        assert len(result.execution_order) >= 1
        assert result.diagnostics.stats is not None

    def test_diamond_pipeline_round_trip(self):
        result = _compile_pipeline(_diamond_joints())

        assert result.success
        assert len(result.joints) == 5
        all_grouped = set()
        for g in result.fused_groups:
            all_grouped.update(g.joints)
        assert all_grouped == {"src", "left", "right", "merged", "sink"}

    def test_pipeline_preserves_joint_names_and_types(self):
        result = _compile_pipeline(_diamond_joints())

        assert result.success
        joint_map = {j.name: j for j in result.joints}
        assert joint_map["src"].type == "source"
        assert joint_map["left"].type == "sql"
        assert joint_map["right"].type == "sql"
        assert joint_map["merged"].type == "sql"
        assert joint_map["sink"].type == "sink"

    def test_pipeline_assigns_engine_to_all_joints(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        for j in result.joints:
            assert j.engine == "duckdb_primary"


# ---------------------------------------------------------------------------
# Tests: compile_until at each phase
# ---------------------------------------------------------------------------


class TestCompileUntilPhases:
    """compile_until at each phase returns valid intermediate state."""

    @pytest.mark.parametrize("phase_idx", range(len(ALL_PHASES)))
    def test_compile_until_returns_correct_completed_phases(self, phase_idx: int):
        phase_name = ALL_PHASES[phase_idx]
        state = _compile_until_phase(_linear_joints(), stop_after=phase_name)

        expected = ALL_PHASES[: phase_idx + 1]
        assert state.completed_phases == expected

    def test_compile_until_prune_dag_has_pruned_assembly(self):
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_PRUNE_DAG)

        assert state.pruned is not None
        assert state.topo_order is None
        assert state.compiled_assembly is None

    def test_compile_until_resolve_metadata_has_topo_order(self):
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_RESOLVE_METADATA)

        assert state.pruned is not None
        assert state.topo_order is not None
        assert state.joint_metadata is not None
        assert len(state.topo_order) == 3
        assert state.compiled_joints is None

    def test_compile_until_compile_sql_has_compiled_joints(self):
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_COMPILE_SQL)

        assert state.compiled_joints is not None
        assert state.cj_map is not None
        assert len(state.compiled_joints) == 3
        assert state.fused_groups is None

    def test_compile_until_fusion_has_fused_groups(self):
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_FUSION)

        assert state.fused_groups is not None
        assert state.joint_to_group is not None
        assert len(state.fused_groups) >= 1
        assert state.materializations is None

    def test_compile_until_materialization_has_materializations(self):
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_MATERIALIZATION)

        assert state.materializations is not None
        assert state.compiled_assembly is None

    def test_compile_until_finalization_has_compiled_assembly(self):
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_FINALIZATION)

        assert state.compiled_assembly is not None
        assert state.compiled_assembly.success

    def test_later_fields_none_when_stopped_early(self):
        """Fields populated by phases after stop_after should be None."""
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_FUSION)

        # Phases after fusion haven't run
        assert state.materializations is None
        assert state.engine_boundaries is None
        assert state.compiled_assembly is None


# ---------------------------------------------------------------------------
# Tests: Phase timing
# ---------------------------------------------------------------------------


class TestPhaseTiming:
    """Phase timing is recorded for all 10 phases."""

    def test_all_phase_timings_recorded(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        stats = result.diagnostics.stats
        assert stats is not None
        # Finalization builds CompilationStats from phase_timings *before*
        # the pipeline records finalization's own timing, so it's absent
        # from stats.phase_durations_ms. All other 9 phases must be present.
        expected = set(ALL_PHASES) - {PHASE_FINALIZATION}
        for phase_name in expected:
            assert phase_name in stats.phase_durations_ms, f"Missing timing for phase: {phase_name}"

    def test_phase_timings_non_negative(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        stats = result.diagnostics.stats
        assert stats is not None
        for phase_name, duration in stats.phase_durations_ms.items():
            assert duration >= 0, f"Negative timing for phase: {phase_name}"

    def test_phase_timings_sum_within_total(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        stats = result.diagnostics.stats
        assert stats is not None
        phase_sum = sum(stats.phase_durations_ms.values())
        # Phase sum should not exceed total compile time (with small tolerance
        # for measurement overhead)
        assert phase_sum <= stats.compile_duration_ms + 50

    def test_finalization_timing_on_phase_state(self):
        """Pipeline records finalization timing on PhaseState even though
        CompilationStats can't include it (built before timing is recorded)."""
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_FINALIZATION)

        for phase_name in ALL_PHASES:
            assert phase_name in state.phase_timings, (
                f"Missing timing on PhaseState for phase: {phase_name}"
            )

    def test_compile_until_records_partial_timings(self):
        state = _compile_until_phase(_linear_joints(), stop_after=PHASE_FUSION)

        # Only phases up to fusion should have timings
        for phase_name in ALL_PHASES[:5]:  # prune_dag through fusion
            assert phase_name in state.phase_timings
        for phase_name in ALL_PHASES[5:]:  # optimization through finalization
            assert phase_name not in state.phase_timings


# ---------------------------------------------------------------------------
# Tests: Adapter decisions
# ---------------------------------------------------------------------------


class TestAdapterDecisions:
    """Adapter decisions are recorded for DuckDB + filesystem catalog."""

    def test_adapter_decisions_present_on_compiled_assembly(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        assert len(result.adapter_decisions) > 0

    def test_adapter_decisions_have_valid_engine_type(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        for ad in result.adapter_decisions:
            assert ad.engine_type == "duckdb"

    def test_adapter_decisions_have_valid_resolution_method(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        valid_methods = {"exact_match", "wildcard_fallback", "none"}
        for ad in result.adapter_decisions:
            assert ad.resolution_method in valid_methods, (
                f"Invalid resolution_method: {ad.resolution_method}"
            )

    def test_adapter_decisions_reference_pipeline_joints(self):
        result = _compile_pipeline(_linear_joints())

        assert result.success
        joint_names = {j.name for j in result.joints}
        for ad in result.adapter_decisions:
            assert ad.joint_name in joint_names

    def test_filesystem_catalog_adapter_found(self):
        """DuckDB + filesystem should resolve an adapter via exact or wildcard match."""
        result = _compile_pipeline(_linear_joints())

        assert result.success
        # Source joint uses filesystem catalog — should have an adapter decision
        src_decisions = [ad for ad in result.adapter_decisions if ad.joint_name == "src"]
        assert len(src_decisions) >= 1
        ad = src_decisions[0]
        assert ad.catalog_type == "filesystem"
        assert ad.resolution_method in {"exact_match", "wildcard_fallback"}

    def test_adapter_decisions_on_phase_state(self):
        """compile_until at resolve_metadata should have adapter_decisions populated."""
        state = _compile_until_phase(
            _linear_joints(),
            stop_after=PHASE_RESOLVE_METADATA,
        )

        assert state.adapter_decisions is not None
        assert len(state.adapter_decisions) > 0
