"""Unit tests for compile() — resolution steps (task 12.2)."""

from __future__ import annotations

from typing import Any

from rivet_core.assembly import Assembly
from rivet_core.checks import Assertion
from rivet_core.compiler import (
    CompiledJoint,
    CompileOptions,
    _compile_joint,
    _compute_parallel_execution_plan,
    compile,
)
from rivet_core.introspection import ColumnDetail, ObjectSchema
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult, FusedGroup
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEngineAdapter,
    ComputeEnginePlugin,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)
from rivet_core.sql_parser import SQLParser

# ---------------------------------------------------------------------------
# Helpers — minimal plugin stubs
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


class IntrospectableCatalogPlugin(StubCatalogPlugin):
    type = "introspectable"

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        return ObjectSchema(
            path=[table],
            node_type="table",
            columns=[
                ColumnDetail(
                    name="id",
                    type="int64",
                    native_type="INTEGER",
                    nullable=False,
                    default=None,
                    comment=None,
                    is_primary_key=True,
                    is_partition_key=False,
                ),
                ColumnDetail(
                    name="name",
                    type="utf8",
                    native_type="VARCHAR",
                    nullable=True,
                    default=None,
                    comment=None,
                    is_primary_key=False,
                    is_partition_key=False,
                ),
            ],
            primary_key=["id"],
            comment=None,
        )


class StubEnginePlugin(ComputeEnginePlugin):
    engine_type = "stub"
    supported_catalog_types: dict[str, list[str]] = {
        "stub": ["projection_pushdown", "predicate_pushdown", "limit_pushdown"],
        "introspectable": ["projection_pushdown", "predicate_pushdown", "limit_pushdown"],
        "failing": ["projection_pushdown", "predicate_pushdown", "limit_pushdown"],
        "duplicate-failing": ["projection_pushdown", "predicate_pushdown", "limit_pushdown"],
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


def _make_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_catalog_plugin(StubCatalogPlugin())
    reg.register_engine_plugin(StubEnginePlugin())
    eng = StubEnginePlugin().create_engine("stub-engine", {})
    reg.register_compute_engine(eng)
    reg.register_source(StubSource())
    reg.register_sink(StubSink())
    return reg


def _make_registry_with_introspection() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_catalog_plugin(IntrospectableCatalogPlugin())
    reg.register_engine_plugin(StubEnginePlugin())
    eng = StubEnginePlugin().create_engine("stub-engine", {})
    reg.register_compute_engine(eng)
    reg.register_source(StubSource())
    reg.register_sink(StubSink())
    return reg


def _compiled_joint(name: str, upstream: list[str]) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type="sql",
        catalog=None,
        catalog_type=None,
        engine="stub-engine",
        engine_resolution="joint_override",
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
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _warning_messages(result: Any) -> list[str]:
    return [warning.message for warning in result.diagnostics.warnings]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCompileBasic:
    """Test basic compile() function behavior."""

    def test_empty_assembly(self) -> None:
        assembly = Assembly([])
        reg = _make_registry()
        result = compile(assembly, [], [], reg)
        assert result.success is True
        assert result.joints == []
        assert result.execution_order == []

    def test_simple_source_sink_pipeline(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="my_cat", engine="stub-engine"),
            Joint(
                name="out",
                joint_type="sink",
                catalog="my_cat",
                upstream=["src"],
                engine="stub-engine",
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="my_cat", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        assert len(result.joints) == 2
        # execution_order contains fused group IDs
        assert len(result.execution_order) >= 1
        # All joints should be covered by fused groups
        all_joints_in_groups = set()
        for g in result.fused_groups:
            all_joints_in_groups.update(g.joints)
        assert {"src", "out"} == all_joints_in_groups

    def test_profile_name_defaults_to_default(self) -> None:
        assembly = Assembly([])
        result = compile(assembly, [], [], _make_registry())
        assert result.profile_name == "default"

    def test_compile_adds_structured_diagnostics(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(
            assembly,
            catalogs,
            engines,
            reg,
            options=CompileOptions(default_engine="stub-engine"),
        )

        assert result.success is True
        assert result.diagnostics.errors == []
        assert _warning_messages(result) == []
        assert result.diagnostics.stats is not None

    def test_compile_failure_includes_structured_diagnostics(self) -> None:
        assembly = Assembly([])
        reg = _make_registry()

        result = compile(assembly, [], [], reg, target_sink="missing")

        assert result.success is False
        assert result.diagnostics.errors
        assert _warning_messages(result) == []
        assert result.diagnostics.stats is None

    def test_compile_options_object_applies_defaults(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(
            assembly,
            catalogs,
            engines,
            reg,
            options=CompileOptions(profile_name="custom", default_engine="stub-engine"),
        )

        assert result.success is True
        assert result.profile_name == "custom"
        assert result.joints[0].engine == "stub-engine"

    def test_explicit_compile_args_override_options(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", engine="stub-engine"),
            Joint(name="sink", joint_type="sink", upstream=["src"], engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(
            assembly,
            [],
            engines,
            reg,
            default_fusion_strategy="cte",
            options=CompileOptions(default_fusion_strategy="temp_view"),
        )

        assert result.success is True
        assert result.fused_groups[0].fusion_strategy == "cte"


class TestEngineResolution:
    """Test engine resolution: joint override → profile default → RVT-401."""

    def test_joint_override_engine(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        cj = result.joints[0]
        assert cj.engine == "stub-engine"
        assert cj.engine_resolution == "joint_override"

    def test_default_engine_fallback(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        cj = result.joints[0]
        assert cj.engine == "stub-engine"
        assert cj.engine_resolution == "project_default"

    def test_missing_engine_produces_rvt_401(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, [], reg)
        assert result.success is False
        assert any(e.code == "RVT-401" for e in result.diagnostics.errors)


class TestAdapterLookup:
    """Test adapter lookup and capability resolution."""

    def test_adapter_found(self) -> None:
        class StubAdapter(ComputeEngineAdapter):
            target_engine_type = "stub"
            catalog_type = "other"
            capabilities = ["projection_pushdown"]
            source = "engine_plugin"

            def read_dispatch(
                self, engine: Any, catalog: Any, joint: Any, pushdown: Any = None
            ) -> AdapterPushdownResult:
                return AdapterPushdownResult(material=None, residual=EMPTY_RESIDUAL)

            def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
                return None

        reg = _make_registry()
        reg.register_catalog_plugin(type("OtherCat", (StubCatalogPlugin,), {"type": "other"})())
        reg.register_adapter(StubAdapter())

        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="other")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        assert result.joints[0].adapter is not None

    def test_no_capabilities_produces_error(self) -> None:
        reg = PluginRegistry()
        reg.register_catalog_plugin(type("UnknownCat", (StubCatalogPlugin,), {"type": "unknown"})())
        reg.register_engine_plugin(StubEnginePlugin())
        eng = StubEnginePlugin().create_engine("stub-engine", {})
        reg.register_compute_engine(eng)

        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="unknown")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is False
        assert any(e.code == "RVT-402" for e in result.diagnostics.errors)


class TestIntrospection:
    """Test source introspection during compilation."""

    def test_source_introspection_populates_schema(self) -> None:
        reg = _make_registry_with_introspection()

        joints = [
            Joint(
                name="src", joint_type="source", catalog="c", engine="stub-engine", table="users"
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="introspectable")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        cj = result.joints[0]
        assert cj.output_schema is not None
        assert len(cj.output_schema.columns) == 2
        assert cj.output_schema.columns[0].name == "id"

    def test_introspection_failure_produces_warning(self) -> None:
        class FailingCatalogPlugin(StubCatalogPlugin):
            type = "failing"

            def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
                raise RuntimeError("Connection failed")

        reg = PluginRegistry()
        reg.register_catalog_plugin(FailingCatalogPlugin())
        reg.register_engine_plugin(StubEnginePlugin())
        eng = StubEnginePlugin().create_engine("stub-engine", {})
        reg.register_compute_engine(eng)

        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="failing")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]

        result = compile(assembly, catalogs, engines, reg)
        # Introspection failure should not cause compilation failure
        assert any("Introspection failed" in w for w in _warning_messages(result))

    def test_duplicate_introspection_warnings_are_deduped(self) -> None:
        class DuplicateFailingCatalogPlugin(StubCatalogPlugin):
            type = "duplicate-failing"

            def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
                raise RuntimeError("Connection failed")

            def get_metadata(self, catalog: Catalog, table: str):
                raise RuntimeError("Connection failed")

        reg = PluginRegistry()
        reg.register_catalog_plugin(DuplicateFailingCatalogPlugin())
        reg.register_engine_plugin(StubEnginePlugin())
        eng = StubEnginePlugin().create_engine("stub-engine", {})
        reg.register_compute_engine(eng)

        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="duplicate-failing")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]

        result = compile(assembly, catalogs, engines, reg)

        matching = [
            w for w in _warning_messages(result) if "Introspection failed for source 'src'" in w
        ]
        assert matching == ["Introspection failed for source 'src': Connection failed"]


class TestSQLParsing:
    """Test SQL parsing for SQLJoints during compilation."""

    def test_sql_joint_parsed(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
            Joint(
                name="transform",
                joint_type="sql",
                catalog="c",
                upstream=["src"],
                engine="stub-engine",
                sql="SELECT id, name FROM src",
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        cj = result.joints[1]
        assert cj.logical_plan is not None
        assert cj.sql == "SELECT id, name FROM src"

    def test_invalid_sql_produces_error(self) -> None:
        joints = [
            Joint(
                name="bad",
                joint_type="sql",
                catalog="c",
                engine="stub-engine",
                sql="DROP TABLE users",
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is False
        assert any(e.code == "RVT-702" for e in result.diagnostics.errors)

    def test_sql_lineage_extracted(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
            Joint(
                name="t",
                joint_type="sql",
                catalog="c",
                upstream=["src"],
                engine="stub-engine",
                sql="SELECT id FROM src",
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        cj = result.joints[1]
        assert len(cj.column_lineage) > 0


class TestPythonJointHandling:
    """Test PythonJoint compilation."""

    def test_python_joint_opaque_lineage(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
            Joint(
                name="py",
                joint_type="python",
                catalog="c",
                upstream=["src"],
                engine="stub-engine",
                function="os.path:exists",  # valid importable callable
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        cj = result.joints[1]
        assert len(cj.column_lineage) == 1
        assert cj.column_lineage[0].transform == "opaque"

    def test_python_joint_non_importable_callable_rvt_753(self) -> None:
        joints = [
            Joint(
                name="py",
                joint_type="python",
                catalog="c",
                upstream=[],
                engine="stub-engine",
                function="nonexistent.module.func",
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is False
        assert any(e.code == "RVT-753" for e in result.diagnostics.errors)
        assert any(
            "module:callable" in e.remediation
            for e in result.diagnostics.errors
            if e.code == "RVT-753"
        )


class TestSourceSelfReferenceRewrite:
    """Test source self-reference normalization preserves joint metadata."""

    def test_self_reference_sql_rewrite_preserves_assertions_and_tags(self) -> None:
        joints = [
            Joint(
                name="src",
                joint_type="source",
                catalog="c",
                engine="stub-engine",
                table="public.users",
                sql="SELECT * FROM __self",
                tags=["etl"],
                assertions=[Assertion(type="not_null", config={"column": "id"})],
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg, introspect=False)

        assert result.success is True
        cj = result.joints[0]
        assert cj.sql is not None
        assert "__self" not in cj.sql
        assert cj.tags == ["etl"]
        assert len(cj.checks) == 1

    def test_source_sql_is_parsed_once_during_compile_joint(self) -> None:
        class CountingSQLParser(SQLParser):
            def __init__(self) -> None:
                super().__init__()
                self.parse_calls = 0

            def parse(self, sql: str, dialect: str | None = None):
                self.parse_calls += 1
                return super().parse(sql, dialect=dialect)

        parser = CountingSQLParser()
        errors = []
        warnings = []
        joint = Joint(
            name="src",
            joint_type="source",
            catalog="c",
            engine="stub-engine",
            table="public.users",
            sql="SELECT id FROM __self WHERE id > 0",
        )

        cj = _compile_joint(
            joint,
            {"c": Catalog(name="c", type="stub")},
            {"stub-engine": ComputeEngine(name="stub-engine", engine_type="stub")},
            _make_registry(),
            "stub-engine",
            parser,
            {},
            errors,
            warnings,
            introspect=False,
        )

        assert errors == []
        assert parser.parse_calls == 1
        assert cj.logical_plan is not None
        assert cj.sql is not None
        assert "__self" not in cj.sql


class TestAssertionValidation:
    """Test assertion validation during compilation."""

    def test_audit_on_non_sink_produces_rvt_651(self) -> None:
        joints = [
            Joint(
                name="t",
                joint_type="sql",
                catalog="c",
                engine="stub-engine",
                sql="SELECT 1",
                assertions=[Assertion(type="row_count", phase="audit", config={"min": 1})],
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is False
        assert any(e.code == "RVT-651" for e in result.diagnostics.errors)

    def test_audit_on_sink_is_valid(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
            Joint(
                name="out",
                joint_type="sink",
                catalog="c",
                upstream=["src"],
                engine="stub-engine",
                assertions=[Assertion(type="row_count", phase="audit", config={"min": 1})],
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        cj = result.joints[1]
        assert len(cj.checks) == 1
        assert cj.checks[0].phase == "audit"

    def test_assertion_on_sql_joint_is_valid(self) -> None:
        joints = [
            Joint(
                name="t",
                joint_type="sql",
                catalog="c",
                engine="stub-engine",
                sql="SELECT 1",
                assertions=[Assertion(type="not_null", config={"column": "id"})],
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert result.success is True
        assert len(result.joints[0].checks) == 1


class TestDAGPruning:
    """Test DAG pruning via target_sink and tags."""

    def test_target_sink_prunes_unreachable(self) -> None:
        joints = [
            Joint(name="s1", joint_type="source", engine="stub-engine"),
            Joint(name="s2", joint_type="source", engine="stub-engine"),
            Joint(name="out1", joint_type="sink", upstream=["s1"], engine="stub-engine"),
            Joint(name="out2", joint_type="sink", upstream=["s2"], engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, [], engines, reg, target_sink="out1")
        assert result.success is True
        names = {cj.name for cj in result.joints}
        assert "s1" in names
        assert "out1" in names
        assert "s2" not in names
        assert "out2" not in names

    def test_tag_filtering(self) -> None:
        joints = [
            Joint(name="s1", joint_type="source", engine="stub-engine", tags=["etl"]),
            Joint(name="s2", joint_type="source", engine="stub-engine", tags=["ml"]),
            Joint(
                name="out", joint_type="sink", upstream=["s1"], engine="stub-engine", tags=["etl"]
            ),
        ]
        assembly = Assembly(joints)
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, [], engines, reg, tags=["etl"])
        names = {cj.name for cj in result.joints}
        assert "s1" in names
        assert "out" in names
        assert "s2" not in names


class TestErrorCollection:
    """Test that compile collects all errors, not just the first."""

    def test_multiple_errors_collected(self) -> None:
        joints = [
            Joint(
                name="bad_sql",
                joint_type="sql",
                engine="stub-engine",
                sql="DROP TABLE x",
            ),
            Joint(
                name="bad_audit",
                joint_type="sql",
                engine="stub-engine",
                sql="SELECT 1",
                assertions=[Assertion(type="row_count", phase="audit", config={"min": 1})],
            ),
        ]
        assembly = Assembly(joints)
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, [], engines, reg)
        assert result.success is False
        codes = [e.code for e in result.diagnostics.errors]
        assert "RVT-702" in codes
        assert "RVT-651" in codes


class TestCompiledOutputStructure:
    """Test the structure of CompiledAssembly output."""

    def test_compiled_catalogs_populated(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", catalog="c", engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        assert len(result.catalogs) == 1
        assert result.catalogs[0].name == "c"
        assert result.catalogs[0].type == "stub"

    def test_compiled_engines_populated(self) -> None:
        joints = [
            Joint(name="src", joint_type="source", engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, [], engines, reg)
        assert len(result.engines) == 1
        assert result.engines[0].name == "stub-engine"
        assert result.engines[0].engine_type == "stub"

    def test_execution_order_is_topological(self) -> None:
        joints = [
            Joint(name="a", joint_type="source", engine="stub-engine"),
            Joint(
                name="b",
                joint_type="sql",
                upstream=["a"],
                engine="stub-engine",
                sql="SELECT 1 FROM a",
            ),
            Joint(name="c", joint_type="sink", upstream=["b"], engine="stub-engine"),
        ]
        assembly = Assembly(joints)
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, [], engines, reg)
        # execution_order contains fused group IDs in topological order
        assert len(result.execution_order) >= 1
        # All joints should be covered
        all_joints = set()
        for g in result.fused_groups:
            all_joints.update(g.joints)
        assert all_joints == {"a", "b", "c"}

    def test_joint_fields_carried_through(self) -> None:
        joints = [
            Joint(
                name="src",
                joint_type="source",
                catalog="c",
                engine="stub-engine",
                tags=["etl"],
                description="My source",
                eager=True,
                table="my_table",
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="stub")]
        engines = [ComputeEngine(name="stub-engine", engine_type="stub")]
        reg = _make_registry()

        result = compile(assembly, catalogs, engines, reg)
        cj = result.joints[0]
        assert cj.tags == ["etl"]
        assert cj.description == "My source"
        assert cj.eager is True
        assert cj.table == "my_table"

    def test_cross_group_optimization_attaches_to_target_joint(self) -> None:
        joints = [
            Joint(
                name="src",
                joint_type="source",
                catalog="c",
                engine="stub-engine",
                table="users",
            ),
            Joint(
                name="consumer",
                joint_type="sql",
                catalog="c",
                upstream=["src"],
                engine="stub-engine-2",
                sql="SELECT id FROM src WHERE id > 0",
            ),
        ]
        assembly = Assembly(joints)
        catalogs = [Catalog(name="c", type="introspectable")]
        engines = [
            ComputeEngine(name="stub-engine", engine_type="stub"),
            ComputeEngine(name="stub-engine-2", engine_type="stub"),
        ]
        reg = _make_registry_with_introspection()
        reg.register_compute_engine(engines[1])

        result = compile(assembly, catalogs, engines, reg)

        assert result.success is True
        src_joint = next(cj for cj in result.joints if cj.name == "src")
        applied = [
            opt
            for opt in src_joint.optimizations
            if opt.rule == "cross_group_predicate_pushdown" and opt.status == "applied"
        ]
        assert len(applied) >= 1
        assert all(opt.target_joint == "src" for opt in applied)
        assert all(opt.target_group is not None for opt in applied)


class TestParallelExecutionPlan:
    """Test safety behavior in parallel execution planning."""

    def test_cycle_in_group_dependencies_produces_warning(self) -> None:
        fused_groups = [
            FusedGroup(
                id="g1",
                joints=["a"],
                engine="stub-engine",
                engine_type="stub",
                adapters={},
                fused_sql=None,
            ),
            FusedGroup(
                id="g2",
                joints=["b"],
                engine="stub-engine",
                engine_type="stub",
                adapters={},
                fused_sql=None,
            ),
        ]
        cj_map = {
            "a": _compiled_joint("a", ["b"]),
            "b": _compiled_joint("b", ["a"]),
        }
        warnings: list[str] = []

        plan = _compute_parallel_execution_plan(fused_groups, cj_map, warnings)

        assert plan == []
        assert any("cyclic or inconsistent" in w for w in warnings)
