"""Property tests for compiler assertion boundary suppression.

Property 1:  Assertion boundary suppression for capable engines (Req 2.1)
Property 2:  Other materialization triggers preserved alongside assertions (Req 2.3)
Property 3:  Audit-only checks never trigger assertion boundary (Req 2.4)
Property 7:  Fused group preservation when assertion boundary suppressed (Req 5.1)
Property 8:  Checks preserved in CompiledJoint when boundary suppressed (Req 5.2)
Property 10: Optimization result recording for assertion boundary decisions (Req 7.3, 7.4)
Property 11: Backward compatibility when no engine supports native assertions (Req 8.1, 2.2)
Property 12: Materialization strategy override respected (Req 8.3)
"""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.checks import Assertion
from rivet_core.compiler import compile
from rivet_core.executor import _SQL_TRANSLATABLE_CHECKS
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEnginePlugin,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)

# ---------------------------------------------------------------------------
# Stubs
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


class _IncapableEnginePlugin(ComputeEnginePlugin):
    engine_type = "incapable"
    supported_catalog_types: dict[str, list[str]] = {"stub": []}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
        raise NotImplementedError


class _CapableEnginePlugin(ComputeEnginePlugin):
    engine_type = "capable"
    supported_catalog_types: dict[str, list[str]] = {"stub": []}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
        raise NotImplementedError

    @property
    def supports_native_assertions(self) -> bool:
        return True

    def execute_assertion_sql(self, engine: Any, sql: str, input_tables: Any) -> Any:
        return self.execute_sql(engine, sql, input_tables)


class _StubSource(SourcePlugin):
    catalog_type = "stub"

    def read(self, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        return None


class _StubSink(SinkPlugin):
    catalog_type = "stub"

    def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registry(engine_plugin: ComputeEnginePlugin, engine_name: str = "eng") -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_catalog_plugin(_StubCatalogPlugin())
    reg.register_engine_plugin(engine_plugin)
    eng = ComputeEngine(name=engine_name, engine_type=engine_plugin.engine_type)
    reg.register_compute_engine(eng)
    reg.register_source(_StubSource())
    reg.register_sink(_StubSink())
    return reg


def _engines(engine_type: str, name: str = "eng") -> list[ComputeEngine]:
    return [ComputeEngine(name=name, engine_type=engine_type)]


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_SQL_CHECK_TYPES = sorted(_SQL_TRANSLATABLE_CHECKS)
_RESIDUAL_CHECK_TYPES = ["custom", "schema", "freshness", "relationship"]

# Configs that satisfy each check type's requirements
_CHECK_CONFIGS: dict[str, dict[str, Any]] = {
    "not_null": {"column": "col"},
    "unique": {"column": "col"},
    "row_count": {"min": 0},
    "accepted_values": {"column": "col", "values": ["a", "b"]},
    "expression": {"expression": "col > 0"},
    "custom": {"function": "mod:fn"},
    "schema": {},
    "freshness": {},
    "relationship": {},
}


def _make_assertion(check_type: str, phase: str = "assertion") -> Assertion:
    return Assertion(type=check_type, config=_CHECK_CONFIGS[check_type], phase=phase)


_sql_check_type = st.sampled_from(_SQL_CHECK_TYPES)
_residual_check_type = st.sampled_from(_RESIDUAL_CHECK_TYPES)
_any_check_type = st.sampled_from(_SQL_CHECK_TYPES + _RESIDUAL_CHECK_TYPES)

# Generate 1-3 SQL-translatable assertion checks
_sql_assertions = st.lists(_sql_check_type, min_size=1, max_size=3).map(
    lambda types: [_make_assertion(t) for t in types]
)

# Generate at least one residual check mixed with SQL-translatable
_mixed_assertions = st.tuples(
    st.lists(_sql_check_type, min_size=0, max_size=2),
    st.lists(_residual_check_type, min_size=1, max_size=2),
).map(lambda pair: [_make_assertion(t) for t in pair[0]] + [_make_assertion(t) for t in pair[1]])


# ---------------------------------------------------------------------------
# Property 1: Assertion boundary suppression for capable engines (Req 2.1)
# ---------------------------------------------------------------------------


@given(assertions=_sql_assertions)
@settings(max_examples=100)
def test_property1_assertion_boundary_suppressed_capable_engine(
    assertions: list[Assertion],
) -> None:
    """For any joint with SQL-translatable assertion checks on a capable engine,
    the compiler shall not produce an assertion_boundary trigger."""
    reg = _make_registry(_CapableEnginePlugin())
    joints = [
        Joint(name="a", joint_type="source", engine="eng", assertions=assertions),
        Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("capable"), reg)
    assert result.success is True
    triggers = {m.trigger for m in result.materializations}
    assert "assertion_boundary" not in triggers


# ---------------------------------------------------------------------------
# Property 2: Other materialization triggers preserved alongside assertions (Req 2.3)
# ---------------------------------------------------------------------------

_other_trigger_configs = st.sampled_from(
    [
        # eager trigger
        {"eager": True},
    ]
)


@given(assertions=_sql_assertions, trigger_config=_other_trigger_configs)
@settings(max_examples=100)
def test_property2_other_triggers_preserved_with_assertions(
    assertions: list[Assertion],
    trigger_config: dict[str, Any],
) -> None:
    """For any joint with assertions that also satisfies another materialization
    trigger, the compiler shall produce that other trigger regardless of native
    assertion support."""
    reg = _make_registry(_CapableEnginePlugin())
    joints = [
        Joint(
            name="a",
            joint_type="source",
            engine="eng",
            assertions=assertions,
            **trigger_config,
        ),
        Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("capable"), reg)
    assert result.success is True
    triggers = {m.trigger for m in result.materializations}
    assert "eager" in triggers


# ---------------------------------------------------------------------------
# Property 3: Audit-only checks never trigger assertion boundary (Req 2.4)
# ---------------------------------------------------------------------------

_audit_check_types = st.sampled_from(_SQL_CHECK_TYPES + _RESIDUAL_CHECK_TYPES)
_audit_assertions = st.lists(_audit_check_types, min_size=1, max_size=3).map(
    lambda types: [_make_assertion(t, phase="audit") for t in types]
)


@given(assertions=_audit_assertions, capable=st.booleans())
@settings(max_examples=100)
def test_property3_audit_only_checks_no_assertion_boundary(
    assertions: list[Assertion],
    capable: bool,
) -> None:
    """For any joint with only audit-phase checks, the compiler shall not
    produce an assertion_boundary trigger, regardless of engine capability."""
    plugin = _CapableEnginePlugin() if capable else _IncapableEnginePlugin()
    engine_type = plugin.engine_type
    reg = _make_registry(plugin)
    # Audit checks are only valid on sink joints
    joints = [
        Joint(name="a", joint_type="source", engine="eng"),
        Joint(
            name="b",
            joint_type="sink",
            upstream=["a"],
            engine="eng",
            assertions=assertions,
        ),
    ]
    result = compile(Assembly(joints), [], _engines(engine_type), reg)
    assert result.success is True
    triggers = {m.trigger for m in result.materializations}
    assert "assertion_boundary" not in triggers


# ---------------------------------------------------------------------------
# Property 7: Fused group preservation when assertion boundary suppressed (Req 5.1)
# ---------------------------------------------------------------------------


@given(assertions=_sql_assertions)
@settings(max_examples=100)
def test_property7_fused_group_preserved_when_suppressed(
    assertions: list[Assertion],
) -> None:
    """For any pipeline where a joint has SQL-translatable assertions on a
    capable engine and no other trigger applies, the joint and its downstream
    consumer shall be in the same fused group."""
    reg = _make_registry(_CapableEnginePlugin())
    joints = [
        Joint(name="a", joint_type="source", engine="eng", assertions=assertions),
        Joint(
            name="b",
            joint_type="sql",
            upstream=["a"],
            engine="eng",
            sql="SELECT * FROM a",
        ),
        Joint(name="c", joint_type="sink", upstream=["b"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("capable"), reg)
    assert result.success is True
    cj_a = next(cj for cj in result.joints if cj.name == "a")
    cj_b = next(cj for cj in result.joints if cj.name == "b")
    assert cj_a.fused_group_id == cj_b.fused_group_id


# ---------------------------------------------------------------------------
# Property 8: Checks preserved in CompiledJoint when boundary suppressed (Req 5.2)
# ---------------------------------------------------------------------------


@given(assertions=_sql_assertions)
@settings(max_examples=100)
def test_property8_checks_preserved_when_boundary_suppressed(
    assertions: list[Assertion],
) -> None:
    """For any compiled joint where the assertion boundary is suppressed,
    the CompiledJoint.checks list shall still contain all original assertion
    checks."""
    reg = _make_registry(_CapableEnginePlugin())
    joints = [
        Joint(name="a", joint_type="source", engine="eng", assertions=assertions),
        Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("capable"), reg)
    assert result.success is True
    cj_a = next(cj for cj in result.joints if cj.name == "a")
    assert len(cj_a.checks) == len(assertions)
    compiled_types = sorted(c.type for c in cj_a.checks)
    input_types = sorted(a.type for a in assertions)
    assert compiled_types == input_types


# ---------------------------------------------------------------------------
# Property 10: Optimization result recording for assertion boundary decisions (Req 7.3, 7.4)
# ---------------------------------------------------------------------------


@given(assertions=_sql_assertions)
@settings(max_examples=100)
def test_property10_optimization_result_applied_for_capable(
    assertions: list[Assertion],
) -> None:
    """For any joint with assertion checks on a capable engine where all checks
    are SQL-translatable, the compiler shall record an OptimizationResult with
    rule='assertion_boundary_suppressed' and status='applied'."""
    reg = _make_registry(_CapableEnginePlugin())
    joints = [
        Joint(name="a", joint_type="source", engine="eng", assertions=assertions),
        Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("capable"), reg)
    assert result.success is True
    cj_a = next(cj for cj in result.joints if cj.name == "a")
    opts = [o for o in cj_a.optimizations if o.rule == "assertion_boundary_suppressed"]
    assert len(opts) == 1
    assert opts[0].status == "applied"


@given(assertions=_sql_assertions)
@settings(max_examples=100)
def test_property10_optimization_result_not_applicable_for_incapable(
    assertions: list[Assertion],
) -> None:
    """For any joint with assertion checks on an incapable engine, the compiler
    shall record an OptimizationResult with rule='assertion_boundary_suppressed'
    and status='not_applicable'."""
    reg = _make_registry(_IncapableEnginePlugin())
    joints = [
        Joint(name="a", joint_type="source", engine="eng", assertions=assertions),
        Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("incapable"), reg)
    assert result.success is True
    cj_a = next(cj for cj in result.joints if cj.name == "a")
    opts = [o for o in cj_a.optimizations if o.rule == "assertion_boundary_suppressed"]
    assert len(opts) == 1
    assert opts[0].status == "not_applicable"


@given(assertions=_mixed_assertions)
@settings(max_examples=100)
def test_property10_optimization_result_not_applicable_for_residual(
    assertions: list[Assertion],
) -> None:
    """For any joint with mixed checks (including residual) on a capable engine,
    the compiler shall record status='not_applicable'."""
    reg = _make_registry(_CapableEnginePlugin())
    joints = [
        Joint(name="a", joint_type="source", engine="eng", assertions=assertions),
        Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("capable"), reg)
    assert result.success is True
    cj_a = next(cj for cj in result.joints if cj.name == "a")
    opts = [o for o in cj_a.optimizations if o.rule == "assertion_boundary_suppressed"]
    assert len(opts) == 1
    assert opts[0].status == "not_applicable"


# ---------------------------------------------------------------------------
# Property 11: Backward compatibility when no engine supports native assertions (Req 8.1, 2.2)
# ---------------------------------------------------------------------------


@given(
    assertions=st.lists(_any_check_type, min_size=1, max_size=3).map(
        lambda types: [_make_assertion(t) for t in types]
    )
)
@settings(max_examples=100)
def test_property11_backward_compat_incapable_engine(
    assertions: list[Assertion],
) -> None:
    """For any pipeline compiled with an engine that does not support native
    assertions, the materialization list shall contain an assertion_boundary
    trigger for every joint with assertion-phase checks."""
    reg = _make_registry(_IncapableEnginePlugin())
    joints = [
        Joint(name="a", joint_type="source", engine="eng", assertions=assertions),
        Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("incapable"), reg)
    assert result.success is True
    # Filter to assertion-phase checks only
    has_assertion_phase = any(a.phase == "assertion" for a in assertions)
    if has_assertion_phase:
        triggers = {m.trigger for m in result.materializations}
        assert "assertion_boundary" in triggers


# ---------------------------------------------------------------------------
# Property 12: Materialization strategy override respected (Req 8.3)
# ---------------------------------------------------------------------------


@given(
    assertions=_sql_assertions,
    strategy=st.sampled_from(["arrow", "temp_table"]),
)
@settings(max_examples=100)
def test_property12_materialization_strategy_override_respected(
    assertions: list[Assertion],
    strategy: str,
) -> None:
    """For any joint with a materialization_strategy_override, the strategy
    in the output shall match the override regardless of native assertion
    support."""
    reg = _make_registry(_CapableEnginePlugin())
    # Use eager=True to force a materialization so we can check the strategy
    joints = [
        Joint(
            name="a",
            joint_type="source",
            engine="eng",
            eager=True,
            assertions=assertions,
            materialization_strategy_override=strategy,
        ),
        Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
    ]
    result = compile(Assembly(joints), [], _engines("capable"), reg)
    assert result.success is True
    mat = next(m for m in result.materializations if m.from_joint == "a")
    assert mat.strategy == strategy
