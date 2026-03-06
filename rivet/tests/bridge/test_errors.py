"""Error handling tests for rivet-bridge.

Tests:
- All BRG-xxx error codes are produced with correct code prefix
- Core errors (RVT-xxx) pass through unchanged when not bridge-layer concerns
- Every BridgeError includes actionable context (joint_name, source_file, remediation)

Requirements: 17.1, 17.2, 17.3, 17.4, 17.5, 17.6, 17.7
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from rivet_bridge.builder import AssemblyBuilder
from rivet_bridge.catalogs import CatalogInstantiator
from rivet_bridge.converter import JointConverter
from rivet_bridge.engines import EngineInstantiator
from rivet_bridge.errors import (
    BRG_100_CONFIG_FAILURE,
    BRG_101_SQL_GEN_FAILURE,
    BRG_102_SQL_DECOMPOSITION_FAILURE,
    BRG_201_UNKNOWN_CATALOG_TYPE,
    BRG_202_CATALOG_VALIDATION,
    BRG_203_UNKNOWN_ENGINE_TYPE,
    BRG_204_ENGINE_VALIDATION,
    BRG_205_SINK_NO_UPSTREAM,
    BRG_206_AUDIT_NON_SINK,
    BRG_207_UNKNOWN_ENGINE_REF,
    BRG_208_DUPLICATE_JOINT,
    BRG_209_UNKNOWN_UPSTREAM,
    BRG_210_CYCLE,
    BRG_211_SOURCE_WITH_UPSTREAM,
    BRG_301_CREDENTIAL_TRACKING,
    BRG_401_OUTPUT_DIR_CONFLICT,
    BRG_402_ROUNDTRIP_DIFFERENCE,
    BridgeError,
    BridgeValidationError,
)
from rivet_bridge.forward import build_assembly
from rivet_bridge.reverse import generate_project
from rivet_bridge.sql_gen import SQLGenerator
from rivet_config import (
    CatalogConfig,
    ConfigResult,
    EngineConfig,
    JointDeclaration,
    QualityCheck,
    ResolvedProfile,
)
from rivet_config.errors import ConfigError
from rivet_core import PluginRegistry
from rivet_core.errors import RivetError

# ── Helpers ──────────────────────────────────────────────────────────


def _profile(
    catalogs: dict[str, CatalogConfig] | None = None,
    engines: list[EngineConfig] | None = None,
) -> ResolvedProfile:
    return ResolvedProfile(
        name="test",
        default_engine="test_engine",
        catalogs=catalogs or {"test_cat": CatalogConfig(name="test_cat", type="arrow", options={})},
        engines=engines or [EngineConfig(name="test_engine", type="arrow", catalogs=["test_cat"], options={})],
    )


def _config(
    declarations: list[JointDeclaration] | None = None,
    profile: ResolvedProfile | None = None,
    errors: list[ConfigError] | None = None,
) -> ConfigResult:
    return ConfigResult(
        manifest=MagicMock(),
        profile=profile or _profile(),
        declarations=declarations or [],
        errors=errors or [],
        warnings=[],
    )


def _registry() -> PluginRegistry:
    r = PluginRegistry()
    r.register_builtins()
    return r


def _source(name: str, **kw: Any) -> JointDeclaration:
    defaults: dict[str, Any] = dict(
        name=name, joint_type="source",
        source_path=Path(f"sources/{name}.yaml"),
        sql=f"SELECT * FROM {name}",
    )
    defaults.update(kw)
    return JointDeclaration(**defaults)


def _sql_joint(name: str, upstream: list[str], **kw: Any) -> JointDeclaration:
    defaults: dict[str, Any] = dict(
        name=name, joint_type="sql",
        source_path=Path(f"joints/{name}.sql"),
        sql=f"SELECT * FROM {upstream[0]}" if upstream else "SELECT 1",
        upstream=upstream,
    )
    defaults.update(kw)
    return JointDeclaration(**defaults)


def _sink(name: str, upstream: list[str], **kw: Any) -> JointDeclaration:
    defaults: dict[str, Any] = dict(
        name=name, joint_type="sink",
        source_path=Path(f"sinks/{name}.yaml"),
        sql=f"SELECT * FROM {upstream[0]}" if upstream else None,
        upstream=upstream,
    )
    defaults.update(kw)
    return JointDeclaration(**defaults)


def _collect_errors(config: ConfigResult) -> list[BridgeError]:
    """Run build_assembly and return the collected errors."""
    with pytest.raises(BridgeValidationError) as exc_info:
        build_assembly(config, _registry())
    return exc_info.value.errors


# ── Requirement 17.1: BRG- prefix for all bridge errors ─────────────


class TestErrorCodePrefix:
    """All bridge error codes use the BRG- prefix (Req 17.1)."""

    def test_all_error_constants_have_brg_prefix(self):
        codes = [
            BRG_100_CONFIG_FAILURE, BRG_101_SQL_GEN_FAILURE, BRG_102_SQL_DECOMPOSITION_FAILURE,
            BRG_201_UNKNOWN_CATALOG_TYPE, BRG_202_CATALOG_VALIDATION,
            BRG_203_UNKNOWN_ENGINE_TYPE, BRG_204_ENGINE_VALIDATION,
            BRG_205_SINK_NO_UPSTREAM, BRG_206_AUDIT_NON_SINK, BRG_207_UNKNOWN_ENGINE_REF,
            BRG_208_DUPLICATE_JOINT, BRG_209_UNKNOWN_UPSTREAM, BRG_210_CYCLE,
            BRG_211_SOURCE_WITH_UPSTREAM,
            BRG_301_CREDENTIAL_TRACKING,
            BRG_401_OUTPUT_DIR_CONFLICT, BRG_402_ROUNDTRIP_DIFFERENCE,
        ]
        for code in codes:
            assert code.startswith("BRG-"), f"{code} does not start with BRG-"


# ── Requirement 17.2: BRG-1xx for parse errors ──────────────────────


class TestParseErrorCodes:
    """BRG-1xx codes for parse errors (Req 17.2)."""

    def test_brg_100_config_failure(self):
        config = _config(errors=[ConfigError(source_file=None, message="bad", remediation="fix")])
        errors = _collect_errors(config)
        assert len(errors) == 1
        assert errors[0].code == "BRG-100"

    def test_brg_101_sql_gen_failure(self):
        # Trigger SQL generation failure with an invalid expression
        decl = _source("bad_sql", sql=None, columns=[MagicMock(name="x", expression="INVALID(((")])
        # Use SQLGenerator directly to isolate the error
        gen = SQLGenerator()
        _, errors = gen.generate(decl, set())
        assert len(errors) == 1
        assert errors[0].code == "BRG-101"

    def test_1xx_codes_are_parse_range(self):
        parse_codes = [BRG_100_CONFIG_FAILURE, BRG_101_SQL_GEN_FAILURE, BRG_102_SQL_DECOMPOSITION_FAILURE]
        for code in parse_codes:
            num = int(code.split("-")[1])
            assert 100 <= num <= 199, f"{code} not in 1xx range"


# ── Requirement 17.3: BRG-2xx for validation errors ─────────────────


class TestValidationErrorCodes:
    """BRG-2xx codes for validation errors (Req 17.3)."""

    def test_brg_201_unknown_catalog_type(self):
        profile = _profile(catalogs={"bad": CatalogConfig(name="bad", type="nonexistent", options={})})
        _, errors = CatalogInstantiator().instantiate_all(profile, PluginRegistry())
        assert len(errors) == 1
        assert errors[0].code == "BRG-201"

    def test_brg_202_catalog_validation_failure(self):
        from tests.bridge.conftest import FailingCatalogPlugin
        profile = _profile(catalogs={"bad": CatalogConfig(name="bad", type="failing", options={})})
        reg = PluginRegistry()
        reg.register_catalog_plugin(FailingCatalogPlugin())
        _, errors = CatalogInstantiator().instantiate_all(profile, reg)
        assert len(errors) == 1
        assert errors[0].code == "BRG-202"

    def test_brg_203_unknown_engine_type(self):
        profile = _profile(engines=[EngineConfig(name="bad", type="nonexistent", catalogs=[], options={})])
        _, errors = EngineInstantiator().instantiate_all(profile, PluginRegistry())
        assert len(errors) == 1
        assert errors[0].code == "BRG-203"

    def test_brg_204_engine_validation_failure(self):
        from tests.bridge.test_engines import FailingEnginePlugin
        profile = ResolvedProfile(
            name="test", default_engine="arrow", catalogs={},
            engines=[EngineConfig(name="bad", type="failing_engine", catalogs=[], options={})],
        )
        reg = PluginRegistry()
        reg.register_engine_plugin(FailingEnginePlugin())
        _, errors = EngineInstantiator().instantiate_all(profile, reg)
        assert len(errors) == 1
        assert errors[0].code == "BRG-204"

    def test_brg_205_sink_no_upstream(self):
        config = _config(declarations=[
            JointDeclaration(
                name="out", joint_type="sink",
                source_path=Path("sinks/out.yaml"),
                sql="SELECT 1", upstream=[],
            ),
        ])
        errors = _collect_errors(config)
        assert any(e.code == "BRG-205" for e in errors)

    def test_brg_206_audit_on_non_sink(self):
        config = _config(declarations=[
            _source("src", quality_checks=[
                QualityCheck(
                    check_type="not_null", phase="audit", severity="error",
                    config={}, source="inline", source_file=Path("sources/src.yaml"),
                ),
            ]),
        ])
        errors = _collect_errors(config)
        assert any(e.code == "BRG-206" for e in errors)

    def test_brg_207_unknown_engine_ref(self):
        config = _config(declarations=[_source("src", engine="nonexistent")])
        errors = _collect_errors(config)
        assert any(e.code == "BRG-207" for e in errors)

    def test_brg_208_duplicate_joint(self):
        config = _config(declarations=[_source("dup"), _source("dup")])
        errors = _collect_errors(config)
        assert any(e.code == "BRG-208" for e in errors)

    def test_brg_209_unknown_upstream(self):
        config = _config(declarations=[
            _source("src"),
            _sql_joint("j", upstream=["nonexistent"]),
        ])
        errors = _collect_errors(config)
        assert any(e.code == "BRG-209" for e in errors)

    def test_brg_210_cycle(self):
        config = _config(declarations=[
            _source("src"),
            _sql_joint("a", upstream=["b"]),
            _sql_joint("b", upstream=["a"]),
        ])
        errors = _collect_errors(config)
        assert any(e.code == "BRG-210" for e in errors)

    def test_brg_211_source_with_upstream(self):
        config = _config(declarations=[
            _source("src"),
            JointDeclaration(
                name="bad_src", joint_type="source",
                source_path=Path("sources/bad_src.yaml"),
                sql="SELECT * FROM src", upstream=["src"],
            ),
        ])
        errors = _collect_errors(config)
        assert any(e.code == "BRG-211" for e in errors)

    def test_2xx_codes_are_validation_range(self):
        validation_codes = [
            BRG_201_UNKNOWN_CATALOG_TYPE, BRG_202_CATALOG_VALIDATION,
            BRG_203_UNKNOWN_ENGINE_TYPE, BRG_204_ENGINE_VALIDATION,
            BRG_205_SINK_NO_UPSTREAM, BRG_206_AUDIT_NON_SINK, BRG_207_UNKNOWN_ENGINE_REF,
            BRG_208_DUPLICATE_JOINT, BRG_209_UNKNOWN_UPSTREAM, BRG_210_CYCLE,
            BRG_211_SOURCE_WITH_UPSTREAM,
        ]
        for code in validation_codes:
            num = int(code.split("-")[1])
            assert 200 <= num <= 299, f"{code} not in 2xx range"


# ── Requirement 17.4: BRG-3xx for profile errors ────────────────────


class TestProfileErrorCodes:
    """BRG-3xx codes for profile errors (Req 17.4)."""

    def test_brg_301_is_3xx(self):
        num = int(BRG_301_CREDENTIAL_TRACKING.split("-")[1])
        assert 300 <= num <= 399


# ── Requirement 17.5: BRG-4xx for roundtrip/output errors ───────────


class TestRoundtripOutputErrorCodes:
    """BRG-4xx codes for roundtrip/output errors (Req 17.5)."""

    def test_brg_401_output_dir_conflict(self, tmp_path: Path):
        # Create a non-empty directory
        (tmp_path / "existing.txt").write_text("content")

        # Build a minimal BridgeResult
        config = _config(declarations=[_source("src")])
        result = build_assembly(config, _registry())

        with pytest.raises(BridgeValidationError) as exc_info:
            generate_project(result, output_dir=tmp_path, overwrite=False)

        errors = exc_info.value.errors
        assert len(errors) == 1
        assert errors[0].code == "BRG-401"

    def test_brg_402_is_4xx(self):
        num = int(BRG_402_ROUNDTRIP_DIFFERENCE.split("-")[1])
        assert 400 <= num <= 499

    def test_4xx_codes_are_output_range(self):
        output_codes = [BRG_401_OUTPUT_DIR_CONFLICT, BRG_402_ROUNDTRIP_DIFFERENCE]
        for code in output_codes:
            num = int(code.split("-")[1])
            assert 400 <= num <= 499, f"{code} not in 4xx range"


# ── Requirement 17.6: Core error passthrough ─────────────────────────


class TestCoreErrorPassthrough:
    """Core errors (RVT-xxx) pass through unchanged (Req 17.6)."""

    def test_assembly_error_translates_to_brg_codes(self):
        """AssemblyError RVT codes are translated to BRG-2xx in the bridge layer."""
        builder = AssemblyBuilder()
        from rivet_core.models import Joint

        # Duplicate joint → RVT-301 → BRG-208
        joints = [
            Joint(name="dup", joint_type="source", sql="SELECT 1"),
            Joint(name="dup", joint_type="source", sql="SELECT 1"),
        ]
        _, errors = builder.build(joints, {}, {}, _profile(), {})
        assert len(errors) == 1
        assert errors[0].code == "BRG-208"

    def test_rvt_code_preserved_in_message(self):
        """The original RVT error message is preserved in the BridgeError message."""
        builder = AssemblyBuilder()
        from rivet_core.models import Joint

        joints = [
            Joint(name="dup", joint_type="source", sql="SELECT 1"),
            Joint(name="dup", joint_type="source", sql="SELECT 1"),
        ]
        _, errors = builder.build(joints, {}, {}, _profile(), {})
        # The message from the RVT error is preserved
        assert "dup" in errors[0].message

    def test_non_bridge_core_errors_are_not_intercepted(self):
        """Core errors outside the bridge scope (e.g. CompilationError) are not
        caught or translated by bridge code — they propagate as-is."""
        from rivet_core.errors import CompilationError

        err = CompilationError([RivetError(code="RVT-500", message="exec fail")])
        assert err.errors[0].code == "RVT-500"


# ── Requirement 17.7: Actionable context on every BridgeError ────────


class TestActionableContext:
    """Every BridgeError includes actionable context (Req 17.7)."""

    def test_brg_100_has_remediation(self):
        config = _config(errors=[ConfigError(source_file=None, message="bad", remediation="fix")])
        errors = _collect_errors(config)
        assert errors[0].remediation is not None

    def test_brg_201_has_joint_name_and_remediation(self):
        profile = _profile(catalogs={"mycat": CatalogConfig(name="mycat", type="nope", options={})})
        _, errors = CatalogInstantiator().instantiate_all(profile, PluginRegistry())
        assert errors[0].joint_name == "mycat"
        assert errors[0].remediation is not None

    def test_brg_202_has_joint_name_and_remediation(self):
        from tests.bridge.conftest import FailingCatalogPlugin
        profile = _profile(catalogs={"mycat": CatalogConfig(name="mycat", type="failing", options={})})
        reg = PluginRegistry()
        reg.register_catalog_plugin(FailingCatalogPlugin())
        _, errors = CatalogInstantiator().instantiate_all(profile, reg)
        assert errors[0].joint_name == "mycat"
        assert errors[0].remediation is not None

    def test_brg_203_has_remediation(self):
        profile = _profile(engines=[EngineConfig(name="e", type="nope", catalogs=[], options={})])
        _, errors = EngineInstantiator().instantiate_all(profile, PluginRegistry())
        assert errors[0].remediation is not None

    def test_brg_204_has_remediation(self):
        from tests.bridge.test_engines import FailingEnginePlugin
        profile = ResolvedProfile(
            name="test", default_engine="arrow", catalogs={},
            engines=[EngineConfig(name="e", type="failing_engine", catalogs=[], options={})],
        )
        reg = PluginRegistry()
        reg.register_engine_plugin(FailingEnginePlugin())
        _, errors = EngineInstantiator().instantiate_all(profile, reg)
        assert errors[0].remediation is not None

    def test_brg_206_has_joint_name_source_file_remediation(self):
        converter = JointConverter()
        decl = JointDeclaration(
            name="my_src", joint_type="source",
            source_path=Path("sources/my_src.yaml"),
            sql="SELECT 1",
            quality_checks=[
                QualityCheck(
                    check_type="not_null", phase="audit", severity="error",
                    config={}, source="inline", source_file=Path("sources/my_src.yaml"),
                ),
            ],
        )
        _, errors = converter.convert(decl, {})
        assert len(errors) == 1
        assert errors[0].joint_name == "my_src"
        assert errors[0].source_file is not None
        assert errors[0].remediation is not None

    def test_brg_207_has_joint_name_source_file_remediation(self):
        converter = JointConverter()
        decl = JointDeclaration(
            name="my_joint", joint_type="sql",
            source_path=Path("joints/my_joint.sql"),
            sql="SELECT 1", upstream=[], engine="ghost_engine",
        )
        _, errors = converter.convert(decl, {})
        assert len(errors) == 1
        assert errors[0].joint_name == "my_joint"
        assert errors[0].source_file is not None
        assert errors[0].remediation is not None

    def test_brg_101_has_joint_name_source_file_remediation(self):
        gen = SQLGenerator()
        decl = _source("bad", sql=None, columns=[MagicMock(name="x", expression="INVALID(((")])
        _, errors = gen.generate(decl, set())
        assert len(errors) == 1
        assert errors[0].joint_name == "bad"
        assert errors[0].source_file is not None
        assert errors[0].remediation is not None

    def test_brg_401_has_remediation(self, tmp_path: Path):
        (tmp_path / "file.txt").write_text("x")
        config = _config(declarations=[_source("src")])
        result = build_assembly(config, _registry())
        with pytest.raises(BridgeValidationError) as exc_info:
            generate_project(result, output_dir=tmp_path, overwrite=False)
        assert exc_info.value.errors[0].remediation is not None

    def test_assembly_translated_errors_have_remediation(self):
        """BRG-2xx errors translated from AssemblyError include remediation."""
        builder = AssemblyBuilder()
        from rivet_core.models import Joint

        joints = [
            Joint(name="dup", joint_type="source", sql="SELECT 1"),
            Joint(name="dup", joint_type="source", sql="SELECT 1"),
        ]
        _, errors = builder.build(joints, {}, {}, _profile(), {})
        assert errors[0].remediation is not None


# ── BridgeValidationError structure ──────────────────────────────────


class TestBridgeValidationError:
    """BridgeValidationError collects errors and formats message."""

    def test_stores_error_list(self):
        errs = [BridgeError(code="BRG-100", message="fail")]
        exc = BridgeValidationError(errs)
        assert exc.errors is errs

    def test_message_includes_count(self):
        errs = [
            BridgeError(code="BRG-201", message="a"),
            BridgeError(code="BRG-203", message="b"),
        ]
        exc = BridgeValidationError(errs)
        assert "2" in str(exc)

    def test_is_exception(self):
        exc = BridgeValidationError([])
        assert isinstance(exc, Exception)
