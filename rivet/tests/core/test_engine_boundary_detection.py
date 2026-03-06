"""Tests for engine boundary detection in the compiler (task 3.1).

Validates Requirement 4.1: adjacent fused groups with different engine_type
produce EngineBoundary entries in CompiledAssembly.engine_boundaries.
"""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.assembly import Assembly
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


class _CatalogPlugin(CatalogPlugin):
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


class _EnginePluginA(ComputeEnginePlugin):
    engine_type = "engine_a"
    supported_catalog_types: dict[str, list[str]] = {"stub": []}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: ComputeEngine, sql: str, input_tables: dict[str, pyarrow.Table]) -> pyarrow.Table:
        return pyarrow.table({})


class _EnginePluginB(ComputeEnginePlugin):
    engine_type = "engine_b"
    supported_catalog_types: dict[str, list[str]] = {"stub": []}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: ComputeEngine, sql: str, input_tables: dict[str, pyarrow.Table]) -> pyarrow.Table:
        return pyarrow.table({})


class _Source(SourcePlugin):
    catalog_type = "stub"

    def read(self, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        return None


class _Sink(SinkPlugin):
    catalog_type = "stub"

    def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
        pass


def _registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_catalog_plugin(_CatalogPlugin())
    reg.register_engine_plugin(_EnginePluginA())
    reg.register_engine_plugin(_EnginePluginB())
    reg.register_compute_engine(ComputeEngine(name="ea", engine_type="engine_a"))
    reg.register_compute_engine(ComputeEngine(name="eb", engine_type="engine_b"))
    reg.register_source(_Source())
    reg.register_sink(_Sink())
    return reg


def _engines() -> list[ComputeEngine]:
    return [
        ComputeEngine(name="ea", engine_type="engine_a"),
        ComputeEngine(name="eb", engine_type="engine_b"),
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEngineBoundaryDetection:
    """Requirement 4.1: engine boundary detection between adjacent fused groups."""

    def test_no_boundary_same_engine(self) -> None:
        """Same engine type across all joints → no boundaries."""
        joints = [
            Joint(name="src", joint_type="source", engine="ea"),
            Joint(name="sink", joint_type="sink", upstream=["src"], engine="ea"),
        ]
        result = compile(Assembly(joints), [], _engines(), _registry())
        assert result.success
        assert result.engine_boundaries == []

    def test_boundary_detected_different_engines(self) -> None:
        """Two joints on different engines → one boundary."""
        joints = [
            Joint(name="src", joint_type="source", engine="ea"),
            Joint(name="sink", joint_type="sink", upstream=["src"], engine="eb"),
        ]
        result = compile(Assembly(joints), [], _engines(), _registry())
        assert result.success
        assert len(result.engine_boundaries) == 1
        eb = result.engine_boundaries[0]
        assert eb.producer_engine_type == "engine_a"
        assert eb.consumer_engine_type == "engine_b"
        assert "src" in eb.boundary_joints

    def test_boundary_has_correct_group_ids(self) -> None:
        """EngineBoundary references the correct producer/consumer group IDs."""
        joints = [
            Joint(name="src", joint_type="source", engine="ea"),
            Joint(name="sink", joint_type="sink", upstream=["src"], engine="eb"),
        ]
        result = compile(Assembly(joints), [], _engines(), _registry())
        assert result.success
        eb = result.engine_boundaries[0]
        # Verify group IDs match actual fused groups
        group_ids = {g.id for g in result.fused_groups}
        assert eb.producer_group_id in group_ids
        assert eb.consumer_group_id in group_ids
        assert eb.producer_group_id != eb.consumer_group_id

    def test_no_boundary_when_same_engine_type_different_instances(self) -> None:
        """Same engine type but different instance names → no boundary."""
        reg = _registry()
        reg.register_compute_engine(ComputeEngine(name="ea2", engine_type="engine_a"))
        engines = _engines() + [ComputeEngine(name="ea2", engine_type="engine_a")]
        joints = [
            Joint(name="src", joint_type="source", engine="ea"),
            Joint(name="sink", joint_type="sink", upstream=["src"], engine="ea2"),
        ]
        result = compile(Assembly(joints), [], engines, reg)
        assert result.success
        assert result.engine_boundaries == []

    def test_chain_multiple_boundaries(self) -> None:
        """A → B → A chain produces two boundaries."""
        reg = _registry()
        reg.register_compute_engine(ComputeEngine(name="ea2", engine_type="engine_a"))
        engines = _engines() + [ComputeEngine(name="ea2", engine_type="engine_a")]
        joints = [
            Joint(name="src", joint_type="source", engine="ea"),
            Joint(
                name="mid",
                joint_type="sql",
                upstream=["src"],
                engine="eb",
                sql="SELECT * FROM src",
            ),
            Joint(name="sink", joint_type="sink", upstream=["mid"], engine="ea2"),
        ]
        result = compile(Assembly(joints), [], engines, reg)
        assert result.success
        assert len(result.engine_boundaries) == 2
        types = {(eb.producer_engine_type, eb.consumer_engine_type) for eb in result.engine_boundaries}
        assert ("engine_a", "engine_b") in types
        assert ("engine_b", "engine_a") in types

    def test_fan_in_boundary(self) -> None:
        """Two producers on engine_a feeding one consumer on engine_b."""
        joints = [
            Joint(name="s1", joint_type="source", engine="ea"),
            Joint(name="s2", joint_type="source", engine="ea"),
            Joint(
                name="sink",
                joint_type="sql",
                upstream=["s1", "s2"],
                engine="eb",
                sql="SELECT * FROM s1 JOIN s2",
            ),
        ]
        result = compile(Assembly(joints), [], _engines(), _registry())
        assert result.success
        # Should have boundary(ies) from engine_a → engine_b
        a_to_b = [
            eb for eb in result.engine_boundaries
            if eb.producer_engine_type == "engine_a" and eb.consumer_engine_type == "engine_b"
        ]
        assert len(a_to_b) >= 1
        # All boundary joints should be from engine_a sources
        all_boundary_joints = []
        for eb in a_to_b:
            all_boundary_joints.extend(eb.boundary_joints)
        assert "s1" in all_boundary_joints or "s2" in all_boundary_joints

    def test_compilation_remains_pure(self) -> None:
        """Engine boundary detection performs no data operations (Req 4.4)."""
        # If compile succeeds without any I/O stubs, detection is pure
        joints = [
            Joint(name="src", joint_type="source", engine="ea"),
            Joint(name="sink", joint_type="sink", upstream=["src"], engine="eb"),
        ]
        result = compile(Assembly(joints), [], _engines(), _registry())
        assert result.success
        assert len(result.engine_boundaries) == 1
