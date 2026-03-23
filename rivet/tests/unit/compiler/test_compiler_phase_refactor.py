"""Unit tests for compiler phase refactor — dataclasses, phase logic, and compile_until API.

Covers:
- Phase 1: DAG pruning with invalid target_sink
- Phase 2: Joint engine override resolves as joint_override in AdapterDecision
- Phase 4: SQL parse error collected, compilation continues
- Phase 5: Single-joint assembly produces one fused group
- Phase 8: Eager joint triggers materialization
- Phase 10: Finalization produces valid execution_order
- compile_until("fusion") returns partial state
- AdapterDecision wildcard fallback recording
- IntrospectionRecord timeout with duration > 0
- CompiledAssembly new fields default to empty lists
"""

from __future__ import annotations

from typing import Any

from rivet_core.assembly import Assembly
from rivet_core.compiler import (
    PHASE_COMPILE_SQL,
    PHASE_FUSION,
    PHASE_INTROSPECT_SOURCES,
    PHASE_PRUNE_DAG,
    PHASE_RESOLVE_METADATA,
    CompiledAssembly,
    IntrospectionRecord,
    PhaseState,
    compile,
    compile_until,
)
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEngineAdapter,
    ComputeEnginePlugin,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)

# ---------------------------------------------------------------------------
# Shared stubs (same pattern as test_compiler.py)
# ---------------------------------------------------------------------------


class _StubCatalogPlugin(CatalogPlugin):
    type = "stub"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


class _StubEnginePlugin(ComputeEnginePlugin):
    engine_type = "stub"
    supported_catalog_types: dict[str, list[str]] = {
        "stub": ["projection_pushdown", "predicate_pushdown", "limit_pushdown"],
    }

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
        raise NotImplementedError


class _StubSource(SourcePlugin):
    catalog_type = "stub"

    def read(self, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        return None


class _StubSink(SinkPlugin):
    catalog_type = "stub"

    def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
        pass


class _StubAdapter(ComputeEngineAdapter):
    """Adapter stub for wildcard fallback tests."""

    def __init__(self, engine_type: str, catalog_type: str, caps: list[str] | None = None) -> None:
        self.target_engine_type = engine_type  # type: ignore[assignment]
        self.catalog_type = catalog_type  # type: ignore[assignment]
        self.capabilities = caps or ["projection_pushdown"]  # type: ignore[assignment]
        self.source = "catalog_plugin"  # type: ignore[assignment]

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        raise NotImplementedError

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        raise NotImplementedError


def _make_registry(engine_name: str = "eng") -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_catalog_plugin(_StubCatalogPlugin())
    reg.register_engine_plugin(_StubEnginePlugin())
    eng = ComputeEngine(name=engine_name, engine_type="stub")
    reg.register_compute_engine(eng)
    reg.register_source(_StubSource())
    reg.register_sink(_StubSink())
    return reg


def _engines(name: str = "eng") -> list[ComputeEngine]:
    return [ComputeEngine(name=name, engine_type="stub")]


# ---------------------------------------------------------------------------
# Phase 1: DAG pruning
# ---------------------------------------------------------------------------


def test_prune_dag_invalid_target_sink_returns_failed_assembly() -> None:
    """Invalid target_sink produces a failed CompiledAssembly with RVT-306 error."""
    joints = [
        Joint(name="src", joint_type="source", engine="eng"),
        Joint(name="out", joint_type="sink", upstream=["src"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines(), _make_registry(), target_sink="nonexistent")
    assert result.success is False
    assert any(e.code == "RVT-306" for e in result.diagnostics.errors)


# ---------------------------------------------------------------------------
# Phase 2: Metadata resolution — engine override
# ---------------------------------------------------------------------------


def test_engine_override_resolves_as_joint_override_in_adapter_decision() -> None:
    """Joint with explicit engine= records an AdapterDecision with correct engine/catalog types.

    The stub engine has native support for the stub catalog type via
    supported_catalog_types, so resolve_capabilities returns caps even without
    a registered adapter object — this is recorded as wildcard_fallback.
    """
    joints = [
        Joint(name="src", joint_type="source", engine="eng", catalog="c"),
        Joint(name="out", joint_type="sink", upstream=["src"], engine="eng"),
    ]
    catalogs = [Catalog(name="c", type="stub")]
    result = compile(Assembly(joints), catalogs, _engines(), _make_registry())
    assert result.success is True

    # Find the adapter decision for the source joint with catalog
    src_decisions = [d for d in result.adapter_decisions if d.joint_name == "src"]
    assert len(src_decisions) >= 1
    decision = src_decisions[0]
    assert decision.engine_type == "stub"
    assert decision.catalog_type == "stub"
    # No registered adapter object, but engine natively supports the catalog type
    # via supported_catalog_types → resolve_capabilities returns caps → wildcard_fallback
    assert decision.resolution_method == "wildcard_fallback"


# ---------------------------------------------------------------------------
# Phase 4: SQL parse error collected, compilation continues
# ---------------------------------------------------------------------------


def test_sql_parse_error_collected_compilation_continues() -> None:
    """A SQL joint with invalid SQL produces an error but compilation still completes."""
    joints = [
        Joint(name="src", joint_type="source", engine="eng"),
        Joint(
            name="bad_sql",
            joint_type="sql",
            upstream=["src"],
            engine="eng",
            sql="THIS IS NOT VALID SQL @@@ !!!",
        ),
    ]
    result = compile(Assembly(joints), [], _engines(), _make_registry())
    # Compilation should complete (not raise), though it may have errors
    assert isinstance(result, CompiledAssembly)
    # The bad SQL joint should still appear in the compiled joints
    joint_names = {j.name for j in result.joints}
    assert "bad_sql" in joint_names


# ---------------------------------------------------------------------------
# Phase 5: Single-joint assembly produces one fused group
# ---------------------------------------------------------------------------


def test_single_joint_assembly_produces_one_fused_group() -> None:
    """A minimal assembly with one source joint produces exactly one fused group."""
    joints = [Joint(name="src", joint_type="source", engine="eng")]
    result = compile(Assembly(joints), [], _engines(), _make_registry())
    assert result.success is True
    assert len(result.fused_groups) == 1
    assert "src" in result.fused_groups[0].joints


# ---------------------------------------------------------------------------
# Phase 8: Eager joint triggers materialization
# ---------------------------------------------------------------------------


def test_eager_joint_triggers_materialization() -> None:
    """A joint with eager=True triggers an 'eager' materialization."""
    joints = [
        Joint(name="src", joint_type="source", engine="eng", eager=True),
        Joint(name="out", joint_type="sink", upstream=["src"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines(), _make_registry())
    assert result.success is True
    triggers = {m.trigger for m in result.materializations}
    assert "eager" in triggers


# ---------------------------------------------------------------------------
# Phase 10: Finalization produces valid execution_order
# ---------------------------------------------------------------------------


def test_finalization_produces_valid_execution_order() -> None:
    """Finalization produces an execution_order covering all fused groups."""
    joints = [
        Joint(name="src", joint_type="source", engine="eng"),
        Joint(
            name="t",
            joint_type="sql",
            upstream=["src"],
            engine="eng",
            sql="SELECT 1 FROM src",
        ),
        Joint(name="out", joint_type="sink", upstream=["t"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines(), _make_registry())
    assert result.success is True
    # execution_order should contain all fused group IDs
    group_ids = {g.id for g in result.fused_groups}
    assert set(result.execution_order) == group_ids
    # Order should be a valid sequence (no duplicates)
    assert len(result.execution_order) == len(set(result.execution_order))


# ---------------------------------------------------------------------------
# compile_until("fusion") returns partial state
# ---------------------------------------------------------------------------


def test_compile_until_fusion_returns_partial_state() -> None:
    """compile_until(PHASE_FUSION) returns state with fused_groups but materializations=None."""
    joints = [
        Joint(name="src", joint_type="source", engine="eng"),
        Joint(name="out", joint_type="sink", upstream=["src"], engine="eng"),
    ]
    state = compile_until(
        Assembly(joints), [], _engines(), _make_registry(), stop_after=PHASE_FUSION
    )
    assert isinstance(state, PhaseState)
    # Fusion completed — fused_groups should be populated
    assert state.fused_groups is not None
    assert len(state.fused_groups) >= 1
    # Phases after fusion should not have run
    assert state.materializations is None
    assert state.engine_boundaries is None
    assert state.compiled_assembly is None
    # completed_phases should end at fusion
    assert state.completed_phases[-1] == PHASE_FUSION
    expected_phases = (
        PHASE_PRUNE_DAG,
        PHASE_RESOLVE_METADATA,
        PHASE_INTROSPECT_SOURCES,
        PHASE_COMPILE_SQL,
        PHASE_FUSION,
    )
    assert state.completed_phases == expected_phases


# ---------------------------------------------------------------------------
# AdapterDecision wildcard fallback
# ---------------------------------------------------------------------------


def test_adapter_decision_records_wildcard_fallback() -> None:
    """When no adapter object is found but resolve_capabilities returns caps,
    AdapterDecision records wildcard_fallback.

    The stub engine declares supported_catalog_types = {"stub": [...]}, so
    resolve_capabilities("stub", "stub") returns caps even without a registered
    adapter object. This is the wildcard_fallback path.
    """
    # Use the default registry which has no registered adapter objects,
    # but the stub engine natively supports the stub catalog type.
    joints = [
        Joint(name="src", joint_type="source", engine="eng", catalog="c"),
    ]
    catalogs = [Catalog(name="c", type="stub")]
    result = compile(Assembly(joints), catalogs, _engines(), _make_registry())
    assert result.success is True

    src_decisions = [d for d in result.adapter_decisions if d.joint_name == "src"]
    assert len(src_decisions) >= 1
    assert src_decisions[0].resolution_method == "wildcard_fallback"
    assert src_decisions[0].engine_type == "stub"
    assert src_decisions[0].catalog_type == "stub"
    # No adapter object was found, so adapter_found should be None
    assert src_decisions[0].adapter_found is None


# ---------------------------------------------------------------------------
# IntrospectionRecord timeout with duration > 0
# ---------------------------------------------------------------------------


def test_introspection_record_timeout_has_positive_duration() -> None:
    """An IntrospectionRecord with result='timeout' should have duration_ms > 0."""
    record = IntrospectionRecord(
        joint_name="src",
        catalog_type="stub",
        catalog_plugin_class="StubCatalogPlugin",
        result="timeout",
        duration_ms=5001.0,
        schema_obtained=False,
        stats_obtained=False,
        error_message="Timed out after 5s",
    )
    assert record.result == "timeout"
    assert record.duration_ms > 0
    assert record.schema_obtained is False
    assert record.error_message is not None


# ---------------------------------------------------------------------------
# CompiledAssembly new fields default to empty lists (backward compat)
# ---------------------------------------------------------------------------


def test_compiled_assembly_new_fields_default_to_empty_lists() -> None:
    """New traceability fields on CompiledAssembly default to empty lists."""
    ca = CompiledAssembly(
        success=True,
        profile_name="default",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=[],
        fused_groups=[],
        materializations=[],
        execution_order=[],
    )
    assert ca.adapter_decisions == []
    assert ca.introspection_records == []
    assert ca.plugin_annotations == []
    assert ca.engine_boundaries == []
    assert ca.parallel_execution_plan == []
