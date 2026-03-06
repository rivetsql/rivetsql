"""Roundtrip property test: forward → reverse → forward semantic equivalence.

Feature: rivet-bridge, Property 23: Forward-reverse-forward roundtrip semantic equivalence
Validates: Requirements 15.1, 15.2
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from rivet_bridge.forward import build_assembly
from rivet_bridge.models import BridgeResult, RoundtripDifference, RoundtripResult
from rivet_bridge.roundtrip import RoundtripVerifier
from rivet_config import (
    CatalogConfig,
    ColumnDecl,
    ConfigResult,
    EngineConfig,
    JointDeclaration,
    QualityCheck,
    ResolvedProfile,
    WriteStrategyDecl,
)
from rivet_core import Joint, PluginRegistry

_FILE = Path("test.yaml")


def _decl(**kwargs: object) -> JointDeclaration:
    defaults: dict[str, object] = {"name": "j1", "joint_type": "source", "source_path": _FILE}
    defaults.update(kwargs)
    return JointDeclaration(**defaults)  # type: ignore[arg-type]


class TestRoundtripVerifierEquivalent:
    def test_identical_declarations(self) -> None:
        orig = [_decl()]
        gen = [_decl()]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is True
        assert result.differences == []

    def test_empty_lists(self) -> None:
        result = RoundtripVerifier().verify_roundtrip([], [])
        assert result.equivalent is True

    def test_sql_whitespace_ignored(self) -> None:
        orig = [_decl(sql="SELECT  a,  b\n  FROM  t")]
        gen = [_decl(sql="SELECT a, b FROM t")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is True

    def test_tag_order_ignored(self) -> None:
        orig = [_decl(tags=["b", "a"])]
        gen = [_decl(tags=["a", "b"])]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is True

    def test_upstream_order_ignored(self) -> None:
        orig = [_decl(name="j1", joint_type="sql", upstream=["b", "a"])]
        gen = [_decl(name="j1", joint_type="sql", upstream=["a", "b"])]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is True

    def test_source_file_ignored(self) -> None:
        orig = [_decl(source_path=Path("a.yaml"))]
        gen = [_decl(source_path=Path("b.yaml"))]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is True


class TestRoundtripVerifierDifferences:
    def test_missing_joint(self) -> None:
        orig = [_decl(name="a"), _decl(name="b")]
        gen = [_decl(name="a")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert len(result.differences) == 1
        assert result.differences[0].joint_name == "b"
        assert result.differences[0].field == "name"

    def test_extra_joint(self) -> None:
        orig = [_decl(name="a")]
        gen = [_decl(name="a"), _decl(name="b")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert any(d.joint_name == "b" for d in result.differences)

    def test_type_differs(self) -> None:
        orig = [_decl(joint_type="source")]
        gen = [_decl(joint_type="sink")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "joint_type"

    def test_sql_differs(self) -> None:
        orig = [_decl(sql="SELECT a FROM t")]
        gen = [_decl(sql="SELECT b FROM t")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "sql"

    def test_catalog_differs(self) -> None:
        orig = [_decl(catalog="cat1")]
        gen = [_decl(catalog="cat2")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "catalog"

    def test_engine_differs(self) -> None:
        orig = [_decl(engine="e1")]
        gen = [_decl(engine="e2")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "engine"

    def test_upstream_differs(self) -> None:
        orig = [_decl(upstream=["a"])]
        gen = [_decl(upstream=["b"])]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "upstream"

    def test_tags_differs(self) -> None:
        orig = [_decl(tags=["x"])]
        gen = [_decl(tags=["y"])]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "tags"

    def test_description_differs(self) -> None:
        orig = [_decl(description="foo")]
        gen = [_decl(description="bar")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "description"

    def test_write_strategy_differs(self) -> None:
        orig = [_decl(write_strategy=WriteStrategyDecl(mode="append", options={}))]
        gen = [_decl(write_strategy=WriteStrategyDecl(mode="replace", options={}))]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "write_strategy"

    def test_quality_checks_differs(self) -> None:
        qc = QualityCheck(check_type="not_null", phase="assertion", severity="error", config={"column": "id"}, source="inline", source_file=_FILE)
        orig = [_decl(quality_checks=[qc])]
        gen = [_decl(quality_checks=[])]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "quality_checks"

    def test_multiple_differences_reported(self) -> None:
        orig = [_decl(catalog="c1", engine="e1")]
        gen = [_decl(catalog="c2", engine="e2")]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        fields = {d.field for d in result.differences}
        assert "catalog" in fields
        assert "engine" in fields

    def test_eager_differs(self) -> None:
        orig = [_decl(eager=False)]
        gen = [_decl(eager=True)]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "eager"

    def test_fusion_strategy_differs(self) -> None:
        orig = [_decl(fusion_strategy="merge")]
        gen = [_decl(fusion_strategy=None)]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "fusion_strategy"

    def test_materialization_strategy_differs(self) -> None:
        orig = [_decl(materialization_strategy="eager")]
        gen = [_decl(materialization_strategy=None)]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is False
        assert result.differences[0].field == "materialization_strategy"

    def test_quality_check_source_and_source_file_ignored(self) -> None:
        """Quality checks should compare semantically, ignoring source/source_file."""
        qc1 = QualityCheck(check_type="not_null", phase="assertion", severity="error", config={"column": "id"}, source="inline", source_file=Path("a.yaml"))
        qc2 = QualityCheck(check_type="not_null", phase="assertion", severity="error", config={"column": "id"}, source="file", source_file=Path("b.yaml"))
        orig = [_decl(quality_checks=[qc1])]
        gen = [_decl(quality_checks=[qc2])]
        result = RoundtripVerifier().verify_roundtrip(orig, gen)
        assert result.equivalent is True


class TestRoundtripResultModel:
    def test_frozen(self) -> None:
        r = RoundtripResult(equivalent=True, differences=[])
        try:
            r.equivalent = False  # type: ignore[misc]
            assert False, "should be frozen"  # noqa: B011
        except AttributeError:
            pass

    def test_difference_frozen(self) -> None:
        d = RoundtripDifference(joint_name="j", field="sql", description="differs")
        try:
            d.field = "x"  # type: ignore[misc]
            assert False, "should be frozen"  # noqa: B011
        except AttributeError:
            pass


# ── Hypothesis strategies ────────────────────────────────────────────

_IDENTIFIER = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)


def _column_decl_strategy() -> st.SearchStrategy[ColumnDecl]:
    return st.builds(
        ColumnDecl,
        name=_IDENTIFIER,
        expression=st.none(),
    )


def _source_decl_strategy(name: str) -> st.SearchStrategy[JointDeclaration]:
    return st.builds(
        JointDeclaration,
        name=st.just(name),
        joint_type=st.just("source"),
        source_path=st.just(Path(f"sources/{name}.yaml")),
        sql=st.just(f"SELECT * FROM {name}"),
        upstream=st.just(None),
        tags=st.just(None) | st.lists(_IDENTIFIER, max_size=3),
        description=st.none() | st.text(min_size=1, max_size=30, alphabet="abcdefghijklmnopqrstuvwxyz "),
    )


def _sql_decl_strategy(name: str, upstream: list[str]) -> st.SearchStrategy[JointDeclaration]:
    return st.builds(
        JointDeclaration,
        name=st.just(name),
        joint_type=st.just("sql"),
        source_path=st.just(Path(f"joints/{name}.sql")),
        sql=st.just(f"SELECT * FROM {upstream[0]}") if upstream else st.just("SELECT 1"),
        upstream=st.just(upstream),
        tags=st.just(None) | st.lists(_IDENTIFIER, max_size=3),
        description=st.none(),
    )


def _sink_decl_strategy(name: str, upstream: list[str]) -> st.SearchStrategy[JointDeclaration]:
    return st.builds(
        JointDeclaration,
        name=st.just(name),
        joint_type=st.just("sink"),
        source_path=st.just(Path(f"sinks/{name}.yaml")),
        sql=st.just(f"SELECT * FROM {upstream[0]}"),
        upstream=st.just(upstream),
        tags=st.just(None) | st.lists(_IDENTIFIER, max_size=3),
        description=st.none(),
    )


@st.composite
def _pipeline_strategy(draw: st.DrawFn) -> list[JointDeclaration]:
    """Generate a valid DAG of JointDeclarations: 1+ sources, 0+ sql joints, 1 sink."""
    num_sources = draw(st.integers(min_value=1, max_value=3))
    source_names = [f"src_{i}" for i in range(num_sources)]
    decls: list[JointDeclaration] = []

    for sname in source_names:
        decls.append(draw(_source_decl_strategy(sname)))

    # Optional sql joints that reference sources
    num_sql = draw(st.integers(min_value=0, max_value=2))
    available = list(source_names)
    for i in range(num_sql):
        sql_name = f"transform_{i}"
        up = [draw(st.sampled_from(available))]
        decls.append(draw(_sql_decl_strategy(sql_name, up)))
        available.append(sql_name)

    # One sink referencing an available joint
    sink_up = [draw(st.sampled_from(available))]
    decls.append(draw(_sink_decl_strategy("output_sink", sink_up)))

    return decls


# ── Helpers ──────────────────────────────────────────────────────────


def _make_profile() -> ResolvedProfile:
    return ResolvedProfile(
        name="test",
        default_engine="test_engine",
        catalogs={"test_catalog": CatalogConfig(name="test_catalog", type="arrow", options={})},
        engines=[EngineConfig(name="test_engine", type="arrow", catalogs=["test_catalog"], options={})],
    )


def _make_config_result(declarations: list[JointDeclaration]) -> ConfigResult:
    return ConfigResult(
        manifest=MagicMock(),
        profile=_make_profile(),
        declarations=declarations,
        errors=[],
        warnings=[],
    )


def _make_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register_builtins()
    return registry


def _joint_to_declaration(joint: Joint) -> JointDeclaration:
    """Convert a core Joint back to a JointDeclaration (reverse path at semantic level)."""
    return JointDeclaration(
        name=joint.name,
        joint_type=joint.joint_type,
        source_path=Path(joint.source_file) if joint.source_file else Path("unknown.yaml"),
        sql=joint.sql,
        catalog=joint.catalog,
        table=joint.table,
        upstream=joint.upstream if joint.upstream else [],
        tags=joint.tags if joint.tags else None,
        description=joint.description,
        engine=joint.engine,
        eager=joint.eager,
        write_strategy=WriteStrategyDecl(mode=joint.write_strategy, options={}) if joint.write_strategy else None,
        function=joint.function,
        fusion_strategy=joint.fusion_strategy_override,
        materialization_strategy=joint.materialization_strategy_override,
    )


# ── Property test ────────────────────────────────────────────────────


class TestRoundtripSemanticEquivalence:
    """Property 23: Forward-reverse-forward roundtrip semantic equivalence."""

    @given(pipeline=_pipeline_strategy())
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_forward_reverse_forward_roundtrip(self, pipeline: list[JointDeclaration]) -> None:
        """For any valid pipeline, forward → reverse → forward produces a semantically equivalent Assembly."""
        registry = _make_registry()

        # Forward pass 1: declarations → Assembly
        config1 = _make_config_result(pipeline)
        result1 = build_assembly(config1, registry)
        assert isinstance(result1, BridgeResult)

        # Reverse: Assembly joints → JointDeclarations
        reversed_decls = [
            _joint_to_declaration(result1.assembly.joints[name])
            for name in result1.assembly.topological_order()
        ]

        # Forward pass 2: reversed declarations → Assembly
        config2 = _make_config_result(reversed_decls)
        result2 = build_assembly(config2, _make_registry())
        assert isinstance(result2, BridgeResult)

        # Verify semantic equivalence
        verifier = RoundtripVerifier()
        reversed_decls_2 = [
            _joint_to_declaration(result2.assembly.joints[name])
            for name in result2.assembly.topological_order()
        ]
        rt_result = verifier.verify_roundtrip(reversed_decls, reversed_decls_2)
        assert rt_result.equivalent, f"Roundtrip differences: {rt_result.differences}"

        # Also verify structural equivalence of the assemblies
        assert set(result1.assembly.joints.keys()) == set(result2.assembly.joints.keys())
        for name in result1.assembly.joints:
            j1 = result1.assembly.joints[name]
            j2 = result2.assembly.joints[name]
            assert j1.name == j2.name
            assert j1.joint_type == j2.joint_type
            assert j1.upstream == j2.upstream
            assert j1.catalog == j2.catalog
            assert j1.engine == j2.engine

    @given(pipeline=_pipeline_strategy())
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_roundtrip_preserves_dag_structure(self, pipeline: list[JointDeclaration]) -> None:
        """The DAG structure (topological order) is preserved across roundtrip."""
        registry = _make_registry()

        config1 = _make_config_result(pipeline)
        result1 = build_assembly(config1, registry)

        reversed_decls = [
            _joint_to_declaration(result1.assembly.joints[name])
            for name in result1.assembly.topological_order()
        ]

        config2 = _make_config_result(reversed_decls)
        result2 = build_assembly(config2, _make_registry())

        # Topological order should be the same
        assert result1.assembly.topological_order() == result2.assembly.topological_order()
