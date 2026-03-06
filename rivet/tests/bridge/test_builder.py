"""Tests for AssemblyBuilder."""

from __future__ import annotations

from rivet_bridge.builder import AssemblyBuilder
from rivet_config import ResolvedProfile
from rivet_core import Assembly, Catalog, ComputeEngine, Joint


def _profile() -> ResolvedProfile:
    return ResolvedProfile(name="test", default_engine="arrow", catalogs={}, engines=[])


def _catalog(name: str = "default") -> dict[str, Catalog]:
    return {name: Catalog(name=name, type="mock", options={})}


def _engines() -> dict[str, ComputeEngine]:
    return {}


class TestAssemblyBuilderSuccess:
    """Requirement 7.1, 7.6: Successful build produces complete BridgeResult."""

    def test_build_single_source(self) -> None:
        joints = [Joint(name="src", joint_type="source")]
        builder = AssemblyBuilder()
        result, errors = builder.build(joints, _catalog(), _engines(), _profile(), {})
        assert errors == []
        assert result is not None
        assert isinstance(result.assembly, Assembly)
        assert "src" in result.assembly.joints

    def test_build_result_contains_all_fields(self) -> None:
        joints = [Joint(name="src", joint_type="source")]
        catalogs = _catalog()
        engines = _engines()
        profile = _profile()
        formats = {"src": "yaml"}
        result, errors = AssemblyBuilder().build(joints, catalogs, engines, profile, formats)
        assert result is not None
        assert result.catalogs is catalogs
        assert result.engines is engines
        assert result.profile_snapshot is profile
        assert result.source_formats == {"src": "yaml"}

    def test_build_source_and_sink(self) -> None:
        joints = [
            Joint(name="src", joint_type="source"),
            Joint(name="snk", joint_type="sink", upstream=["src"]),
        ]
        result, errors = AssemblyBuilder().build(joints, _catalog(), _engines(), _profile(), {})
        assert errors == []
        assert result is not None
        assert set(result.assembly.joints.keys()) == {"src", "snk"}


class TestAssemblyBuilderErrorTranslation:
    """Requirements 7.2, 7.3, 7.4, 7.5, 5.6: AssemblyError → BRG-2xx."""

    def test_duplicate_joint_brg208(self) -> None:
        joints = [
            Joint(name="a", joint_type="source"),
            Joint(name="a", joint_type="source"),
        ]
        result, errors = AssemblyBuilder().build(joints, {}, {}, _profile(), {})
        assert result is None
        assert len(errors) == 1
        assert errors[0].code == "BRG-208"
        assert errors[0].joint_name == "a"

    def test_unknown_upstream_brg209(self) -> None:
        joints = [Joint(name="a", joint_type="sql", upstream=["nonexistent"])]
        result, errors = AssemblyBuilder().build(joints, {}, {}, _profile(), {})
        assert result is None
        assert len(errors) == 1
        assert errors[0].code == "BRG-209"

    def test_cycle_brg210(self) -> None:
        joints = [
            Joint(name="a", joint_type="sql", upstream=["b"]),
            Joint(name="b", joint_type="sql", upstream=["a"]),
        ]
        result, errors = AssemblyBuilder().build(joints, {}, {}, _profile(), {})
        assert result is None
        assert len(errors) == 1
        assert errors[0].code == "BRG-210"

    def test_source_with_upstream_brg211(self) -> None:
        joints = [
            Joint(name="other", joint_type="source"),
            Joint(name="src", joint_type="source", upstream=["other"]),
        ]
        result, errors = AssemblyBuilder().build(joints, {}, {}, _profile(), {})
        assert result is None
        assert len(errors) == 1
        assert errors[0].code == "BRG-211"

    def test_sink_no_upstream_brg205(self) -> None:
        joints = [Joint(name="snk", joint_type="sink", upstream=[])]
        result, errors = AssemblyBuilder().build(joints, {}, {}, _profile(), {})
        assert result is None
        assert len(errors) == 1
        assert errors[0].code == "BRG-205"
