"""Unit tests for compiler assertion boundary suppression logic.

Verifies that the compiler skips the assertion_boundary materialization
trigger when the engine supports native assertions and all assertion-phase
checks are SQL-translatable, and preserves it otherwise.
"""

from __future__ import annotations

from typing import Any

from rivet_core.assembly import Assembly
from rivet_core.checks import Assertion
from rivet_core.compiler import compile
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
    """Engine that does NOT support native assertions."""

    engine_type = "incapable"
    supported_catalog_types: dict[str, list[str]] = {"stub": []}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
        raise NotImplementedError


class _CapableEnginePlugin(ComputeEnginePlugin):
    """Engine that supports native assertions."""

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
# Tests
# ---------------------------------------------------------------------------


class TestAssertionBoundarySuppression:
    def test_assertion_boundary_suppressed_capable_engine(self) -> None:
        """Capable engine + SQL-translatable checks → no assertion_boundary."""
        reg = _make_registry(_CapableEnginePlugin())
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                assertions=[Assertion(type="not_null", config={"column": "id"})],
            ),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines("capable"), reg)
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "assertion_boundary" not in triggers
        # Verify optimization result recorded
        cj_a = next(cj for cj in result.joints if cj.name == "a")
        opt = next(o for o in cj_a.optimizations if o.rule == "assertion_boundary_suppressed")
        assert opt.status == "applied"

    def test_assertion_boundary_inserted_incapable_engine(self) -> None:
        """Incapable engine → assertion_boundary present."""
        reg = _make_registry(_IncapableEnginePlugin())
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                assertions=[Assertion(type="not_null", config={"column": "id"})],
            ),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines("incapable"), reg)
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "assertion_boundary" in triggers
        # Verify optimization result recorded as not_applicable
        cj_a = next(cj for cj in result.joints if cj.name == "a")
        opt = next(o for o in cj_a.optimizations if o.rule == "assertion_boundary_suppressed")
        assert opt.status == "not_applicable"

    def test_assertion_boundary_with_residual_checks(self) -> None:
        """Mix of SQL-translatable and residual → assertion_boundary present."""
        reg = _make_registry(_CapableEnginePlugin())
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                assertions=[
                    Assertion(type="not_null", config={"column": "id"}),
                    Assertion(type="custom", config={"function": "mod:fn"}),
                ],
            ),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines("capable"), reg)
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "assertion_boundary" in triggers
        cj_a = next(cj for cj in result.joints if cj.name == "a")
        opt = next(o for o in cj_a.optimizations if o.rule == "assertion_boundary_suppressed")
        assert opt.status == "not_applicable"
        assert "custom" in opt.detail

    def test_other_trigger_preserved_with_native_assertions(self) -> None:
        """Eager joint with assertions on capable engine → eager trigger present."""
        reg = _make_registry(_CapableEnginePlugin())
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                eager=True,
                assertions=[Assertion(type="not_null", config={"column": "id"})],
            ),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines("capable"), reg)
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "eager" in triggers
        # assertion_boundary should NOT be present (suppressed by native support)
        assert "assertion_boundary" not in triggers

    def test_fusion_preserved_with_native_assertions(self) -> None:
        """Joint with assertions on capable engine fuses with downstream."""
        reg = _make_registry(_CapableEnginePlugin())
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                assertions=[Assertion(type="not_null", config={"column": "id"})],
            ),
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
        # a and b should be in the same fused group
        cj_a = next(cj for cj in result.joints if cj.name == "a")
        cj_b = next(cj for cj in result.joints if cj.name == "b")
        assert cj_a.fused_group_id == cj_b.fused_group_id
        # Checks should still be preserved on the compiled joint
        assert len(cj_a.checks) == 1
        assert cj_a.checks[0].type == "not_null"
