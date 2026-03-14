"""Unit tests for compiler — strategy resolution, materialization triggers,
reference resolution, and tag-based scoping (task 12.4)."""

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
    ReferenceResolver,
    SinkPlugin,
    SourcePlugin,
)

# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


class StubCatalogPlugin(CatalogPlugin):
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


class StubEnginePlugin(ComputeEnginePlugin):
    engine_type = "stub"
    supported_catalog_types: dict[str, list[str]] = {
        "stub": ["projection_pushdown", "predicate_pushdown", "limit_pushdown"],
    }

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine, sql, input_tables):
        raise NotImplementedError


class StubSource(SourcePlugin):
    catalog_type = "stub"

    def read(self, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        return None


class StubSink(SinkPlugin):
    catalog_type = "stub"

    def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
        pass


def _make_registry(engine_name: str = "eng") -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_catalog_plugin(StubCatalogPlugin())
    reg.register_engine_plugin(StubEnginePlugin())
    eng = ComputeEngine(name=engine_name, engine_type="stub")
    reg.register_compute_engine(eng)
    reg.register_source(StubSource())
    reg.register_sink(StubSink())
    return reg


class StubReferenceResolver(ReferenceResolver):
    """Concrete stub for ReferenceResolver — replaces MagicMock usage."""

    def __init__(self, *, return_value: str | None = None, side_effect=None):
        self._return_value = return_value
        self._side_effect = side_effect

    def resolve_references(
        self, sql, joint, catalog, compiled_joints=None, catalog_map=None, fused_group_joints=None
    ):
        if self._side_effect is not None:
            if callable(self._side_effect) and not isinstance(self._side_effect, BaseException):
                return self._side_effect(sql, joint, catalog)
            raise self._side_effect
        return self._return_value if self._return_value is not None else sql


def _catalogs() -> list[Catalog]:
    return [Catalog(name="c", type="stub")]


def _engines(name: str = "eng") -> list[ComputeEngine]:
    return [ComputeEngine(name=name, engine_type="stub")]


# ---------------------------------------------------------------------------
# Strategy resolution
# ---------------------------------------------------------------------------


class TestStrategyResolution:
    """Test fusion and materialization strategy resolution (step 6)."""

    def test_default_fusion_strategy_is_cte(self) -> None:
        joints = [
            Joint(name="a", joint_type="source", engine="eng"),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        group = result.fused_groups[0]
        assert group.fusion_strategy == "cte"

    def test_fusion_strategy_temp_view(self) -> None:
        joints = [
            Joint(name="a", joint_type="source", engine="eng"),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(
            Assembly(joints),
            [],
            _engines(),
            _make_registry(),
            default_fusion_strategy="temp_view",
        )
        assert result.success is True
        group = result.fused_groups[0]
        assert group.fusion_strategy == "temp_view"

    def test_joint_fusion_strategy_override_applied(self) -> None:
        joints = [
            Joint(
                name="a", joint_type="source", engine="eng", fusion_strategy_override="temp_view"
            ),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        group = result.fused_groups[0]
        assert group.fusion_strategy == "temp_view"

    def test_conflicting_fusion_overrides_produce_rvt_603(self) -> None:
        # Two joints in the same fused group with different overrides
        joints = [
            Joint(name="a", joint_type="source", engine="eng", fusion_strategy_override="cte"),
            Joint(
                name="b",
                joint_type="sql",
                upstream=["a"],
                engine="eng",
                sql="SELECT 1 FROM a",
                fusion_strategy_override="temp_view",
            ),
            Joint(name="c", joint_type="sink", upstream=["b"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is False
        assert any(e.code == "RVT-603" for e in result.errors)

    def test_invalid_fusion_strategy_produces_rvt_601(self) -> None:
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                fusion_strategy_override="invalid_strategy",
            ),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is False
        assert any(e.code == "RVT-601" for e in result.errors)

    def test_invalid_materialization_strategy_produces_rvt_602(self) -> None:
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                materialization_strategy_override="bad_strategy",
            ),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is False
        assert any(e.code == "RVT-602" for e in result.errors)

    def test_valid_materialization_strategy_override(self) -> None:
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                materialization_strategy_override="temp_table",
            ),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True


# ---------------------------------------------------------------------------
# Materialization trigger detection
# ---------------------------------------------------------------------------


class TestMaterializationTriggers:
    """Test that materialization triggers are correctly detected (step 8)."""

    def test_eager_trigger(self) -> None:
        joints = [
            Joint(name="a", joint_type="source", engine="eng", eager=True),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "eager" in triggers

    def test_python_boundary_trigger(self) -> None:
        joints = [
            Joint(name="a", joint_type="source", engine="eng"),
            Joint(
                name="py",
                joint_type="python",
                upstream=["a"],
                engine="eng",
                function="os.path:exists",
            ),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "python_boundary" in triggers

    def test_assertion_boundary_trigger(self) -> None:
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                assertions=[Assertion(type="not_null", config={"column": "id"})],
            ),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "assertion_boundary" in triggers

    def test_multi_consumer_trigger(self) -> None:
        joints = [
            Joint(name="a", joint_type="source", engine="eng"),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
            Joint(name="c", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "multi_consumer" in triggers

    def test_engine_instance_change_trigger(self) -> None:
        # Two different engine instances
        class StubEnginePlugin2(StubEnginePlugin):
            engine_type = "stub2"
            supported_catalog_types: dict[str, list[str]] = {"stub": []}

        reg = PluginRegistry()
        reg.register_catalog_plugin(StubCatalogPlugin())
        reg.register_engine_plugin(StubEnginePlugin())
        reg.register_engine_plugin(StubEnginePlugin2())
        eng1 = ComputeEngine(name="eng1", engine_type="stub")
        eng2 = ComputeEngine(name="eng2", engine_type="stub2")
        reg.register_compute_engine(eng1)
        reg.register_compute_engine(eng2)
        reg.register_source(StubSource())
        reg.register_sink(StubSink())

        joints = [
            Joint(name="a", joint_type="source", engine="eng1"),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng2"),
        ]
        result = compile(Assembly(joints), [], [eng1, eng2], reg)
        assert result.success is True
        triggers = {m.trigger for m in result.materializations}
        assert "engine_instance_change" in triggers

    def test_no_materialization_when_fused(self) -> None:
        # Two joints on same engine, no barriers → fused, no materialization
        joints = [
            Joint(name="a", joint_type="source", engine="eng"),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        # No materialization triggers between a and b since they're fused
        mat_pairs = {(m.from_joint, m.to_joint) for m in result.materializations}
        assert ("a", "b") not in mat_pairs

    def test_materialization_strategy_from_override(self) -> None:
        joints = [
            Joint(
                name="a",
                joint_type="source",
                engine="eng",
                eager=True,
                materialization_strategy_override="temp_table",
            ),
            Joint(name="b", joint_type="sink", upstream=["a"], engine="eng"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        mat = next(m for m in result.materializations if m.from_joint == "a")
        assert mat.strategy == "temp_table"


# ---------------------------------------------------------------------------
# Tag-based compilation scoping
# ---------------------------------------------------------------------------


class TestTagBasedScoping:
    """Test tag-based DAG pruning during compilation."""

    def test_or_mode_includes_any_matching_tag(self) -> None:
        joints = [
            Joint(name="s1", joint_type="source", engine="eng", tags=["etl"]),
            Joint(name="s2", joint_type="source", engine="eng", tags=["ml"]),
            Joint(name="out1", joint_type="sink", upstream=["s1"], engine="eng", tags=["etl"]),
            Joint(name="out2", joint_type="sink", upstream=["s2"], engine="eng", tags=["ml"]),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry(), tags=["etl"])
        names = {cj.name for cj in result.joints}
        assert "s1" in names and "out1" in names
        assert "s2" not in names and "out2" not in names

    def test_and_mode_requires_all_tags(self) -> None:
        joints = [
            Joint(name="a", joint_type="source", engine="eng", tags=["etl", "daily"]),
            Joint(name="b", joint_type="source", engine="eng", tags=["etl"]),
            Joint(
                name="out", joint_type="sink", upstream=["a"], engine="eng", tags=["etl", "daily"]
            ),
        ]
        result = compile(
            Assembly(joints),
            [],
            _engines(),
            _make_registry(),
            tags=["etl", "daily"],
            tag_mode="and",
        )
        names = {cj.name for cj in result.joints}
        assert "a" in names and "out" in names
        assert "b" not in names

    def test_target_sink_with_tags_intersection(self) -> None:
        joints = [
            Joint(name="s1", joint_type="source", engine="eng", tags=["etl"]),
            Joint(name="s2", joint_type="source", engine="eng", tags=["ml"]),
            Joint(name="out", joint_type="sink", upstream=["s1", "s2"], engine="eng", tags=["etl"]),
        ]
        result = compile(
            Assembly(joints),
            [],
            _engines(),
            _make_registry(),
            target_sink="out",
            tags=["etl"],
        )
        names = {cj.name for cj in result.joints}
        assert "out" in names
        assert "s1" in names  # upstream of out, included


# ---------------------------------------------------------------------------
# Reference resolution pass
# ---------------------------------------------------------------------------


class TestReferenceResolution:
    """Test reference resolution pass (step 7)."""

    def test_resolver_called_and_sql_resolved_set(self) -> None:
        resolver = StubReferenceResolver(return_value="SELECT id FROM schema.src")

        joints = [
            Joint(name="src", joint_type="source", engine="eng"),
            Joint(
                name="t", joint_type="sql", upstream=["src"], engine="eng", sql="SELECT id FROM src"
            ),
        ]
        result = compile(
            Assembly(joints),
            [],
            _engines(),
            _make_registry(),
            resolve_references=resolver,
        )
        assert result.success is True
        cj = next(j for j in result.joints if j.name == "t")
        assert cj.sql_resolved == "SELECT id FROM schema.src"

    def test_resolver_no_change_leaves_sql_resolved_none(self) -> None:
        resolver = StubReferenceResolver(side_effect=lambda sql, joint, cat: sql)

        joints = [
            Joint(name="t", joint_type="sql", engine="eng", sql="SELECT 1"),
        ]
        result = compile(
            Assembly(joints),
            [],
            _engines(),
            _make_registry(),
            resolve_references=resolver,
        )
        assert result.success is True
        cj = result.joints[0]
        assert cj.sql_resolved is None

    def test_resolver_failure_produces_warning(self) -> None:
        resolver = StubReferenceResolver(side_effect=RuntimeError("resolver error"))

        joints = [
            Joint(name="t", joint_type="sql", engine="eng", sql="SELECT 1"),
        ]
        result = compile(
            Assembly(joints),
            [],
            _engines(),
            _make_registry(),
            resolve_references=resolver,
        )
        assert result.success is True
        assert any("Reference resolution failed" in w for w in result.warnings)

    def test_resolved_sql_recomposed_in_fused_group(self) -> None:
        resolver = StubReferenceResolver(return_value="SELECT id FROM schema.src")

        joints = [
            Joint(name="src", joint_type="source", engine="eng"),
            Joint(
                name="t", joint_type="sql", upstream=["src"], engine="eng", sql="SELECT id FROM src"
            ),
            Joint(name="out", joint_type="sink", upstream=["t"], engine="eng"),
        ]
        result = compile(
            Assembly(joints),
            [],
            _engines(),
            _make_registry(),
            resolve_references=resolver,
        )
        assert result.success is True
        # The fused group should have resolved_sql set
        groups_with_resolved = [g for g in result.fused_groups if g.resolved_sql is not None]
        assert len(groups_with_resolved) >= 1

    def test_no_resolver_leaves_sql_resolved_none(self) -> None:
        joints = [
            Joint(name="t", joint_type="sql", engine="eng", sql="SELECT 1"),
        ]
        result = compile(Assembly(joints), [], _engines(), _make_registry())
        assert result.success is True
        assert result.joints[0].sql_resolved is None

    def test_multi_engine_resolver_scoped_to_own_engine_type(self) -> None:
        """Regression: a resolver from engine B must not rewrite SQL in engine A groups.

        Previously _discover_resolver picked the first resolver globally and
        applied it to all fused groups, causing DuckDB groups to get postgres-
        resolved SQL in multi-engine plans.
        """

        class EngineAPlugin(ComputeEnginePlugin):
            """Engine with no reference resolver (like DuckDB)."""

            engine_type = "engine_a"
            supported_catalog_types: dict[str, list[str]] = {"stub": []}

            def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
                return ComputeEngine(name=name, engine_type=self.engine_type)

            def validate(self, options: dict[str, Any]) -> None:
                pass

            def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
                raise NotImplementedError

        class EngineBResolver(ReferenceResolver):
            """Resolver that rewrites src → schema.src."""

            def resolve_references(
                self,
                sql: Any,
                joint: Any,
                catalog: Any,
                compiled_joints: Any = None,
                catalog_map: Any = None,
                fused_group_joints: Any = None,
            ) -> str | None:
                if "src" in sql:
                    return sql.replace("src", "schema.src")
                return None

        class EngineBPlugin(ComputeEnginePlugin):
            """Engine with a reference resolver (like Postgres)."""

            engine_type = "engine_b"
            supported_catalog_types: dict[str, list[str]] = {"stub": []}

            def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
                return ComputeEngine(name=name, engine_type=self.engine_type)

            def validate(self, options: dict[str, Any]) -> None:
                pass

            def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
                raise NotImplementedError

            def get_reference_resolver(self) -> ReferenceResolver | None:
                return EngineBResolver()

        reg = PluginRegistry()
        reg.register_catalog_plugin(StubCatalogPlugin())
        reg.register_engine_plugin(EngineAPlugin())
        reg.register_engine_plugin(EngineBPlugin())
        reg.register_compute_engine(ComputeEngine(name="eng_a", engine_type="engine_a"))
        reg.register_compute_engine(ComputeEngine(name="eng_b", engine_type="engine_b"))
        reg.register_source(StubSource())
        reg.register_sink(StubSink())

        engines = [
            ComputeEngine(name="eng_a", engine_type="engine_a"),
            ComputeEngine(name="eng_b", engine_type="engine_b"),
        ]

        joints = [
            Joint(name="src_a", joint_type="source", engine="eng_a"),
            Joint(
                name="t_a",
                joint_type="sql",
                upstream=["src_a"],
                engine="eng_a",
                sql="SELECT id FROM src_a",
            ),
            Joint(name="src_b", joint_type="source", engine="eng_b"),
            Joint(
                name="t_b",
                joint_type="sql",
                upstream=["src_b"],
                engine="eng_b",
                sql="SELECT id FROM src_b",
            ),
        ]

        result = compile(Assembly(joints), [], engines, reg)
        assert result.success is True

        cj_a = next(j for j in result.joints if j.name == "t_a")
        cj_b = next(j for j in result.joints if j.name == "t_b")

        # Engine A has no resolver — its SQL must NOT be rewritten
        assert cj_a.sql_resolved is None, (
            f"Engine A joint was incorrectly resolved: {cj_a.sql_resolved}"
        )
        # Engine B has a resolver — its SQL should be rewritten
        assert cj_b.sql_resolved is not None, "Engine B joint should have resolved SQL"
