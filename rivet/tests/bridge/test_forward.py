"""Tests for build_assembly forward path orchestrator."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from rivet_bridge.errors import BridgeValidationError
from rivet_bridge.forward import build_assembly
from rivet_bridge.models import BridgeResult
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
from rivet_config.errors import ConfigError
from rivet_core import PluginRegistry

# ── Helpers ──────────────────────────────────────────────────────────


def _make_profile(
    catalogs: dict[str, CatalogConfig] | None = None,
    engines: list[EngineConfig] | None = None,
    default_engine: str = "test_engine",
) -> ResolvedProfile:
    return ResolvedProfile(
        name="test",
        default_engine=default_engine,
        catalogs=catalogs or {"test_catalog": CatalogConfig(name="test_catalog", type="arrow", options={})},
        engines=engines or [EngineConfig(name="test_engine", type="arrow", catalogs=["test_catalog"], options={})],
    )


def _make_config_result(
    declarations: list[JointDeclaration] | None = None,
    profile: ResolvedProfile | None = None,
    errors: list[ConfigError] | None = None,
) -> ConfigResult:
    return ConfigResult(
        manifest=MagicMock(),
        profile=profile or _make_profile(),
        declarations=declarations or [],
        errors=errors or [],
        warnings=[],
    )


def _make_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register_builtins()
    return registry


def _source_decl(name: str, **kwargs: Any) -> JointDeclaration:
    defaults = dict(
        name=name,
        joint_type="source",
        source_path=Path(f"sources/{name}.yaml"),
        sql=f"SELECT * FROM {name}",
    )
    defaults.update(kwargs)
    return JointDeclaration(**defaults)


def _sql_decl(name: str, upstream: list[str], **kwargs: Any) -> JointDeclaration:
    defaults = dict(
        name=name,
        joint_type="sql",
        source_path=Path(f"joints/{name}.sql"),
        sql=f"SELECT * FROM {upstream[0]}" if upstream else "SELECT 1",
        upstream=upstream,
    )
    defaults.update(kwargs)
    return JointDeclaration(**defaults)


def _sink_decl(name: str, upstream: list[str], **kwargs: Any) -> JointDeclaration:
    defaults = dict(
        name=name,
        joint_type="sink",
        source_path=Path(f"sinks/{name}.yaml"),
        sql=f"SELECT * FROM {upstream[0]}" if upstream else None,
        upstream=upstream,
    )
    defaults.update(kwargs)
    return JointDeclaration(**defaults)


# ── Tests ────────────────────────────────────────────────────────────


class TestBuildAssemblyFailedConfig:
    """Test build_assembly with failed ConfigResult → BRG-100."""

    def test_failed_config_raises_brg_100(self):
        config = _make_config_result(
            errors=[ConfigError(source_file=None, message="bad config", remediation="fix it")],
        )
        with pytest.raises(BridgeValidationError) as exc_info:
            build_assembly(config, _make_registry())

        assert len(exc_info.value.errors) == 1
        assert exc_info.value.errors[0].code == "BRG-100"


class TestBuildAssemblyMinimal:
    """Test build_assembly with valid minimal config → successful BridgeResult."""

    def test_single_source(self):
        decls = [_source_decl("raw_users")]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        assert isinstance(result, BridgeResult)
        assert "raw_users" in result.assembly.joints
        assert result.assembly.joints["raw_users"].joint_type == "source"

    def test_source_and_sink(self):
        decls = [
            _source_decl("raw_users"),
            _sink_decl("output_users", upstream=["raw_users"], sql="SELECT * FROM raw_users"),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        assert isinstance(result, BridgeResult)
        assert len(result.assembly.joints) == 2
        assert "raw_users" in result.assembly.joints
        assert "output_users" in result.assembly.joints

    def test_source_sql_sink_chain(self):
        decls = [
            _source_decl("raw_users"),
            _sql_decl("clean_users", upstream=["raw_users"]),
            _sink_decl("output_users", upstream=["clean_users"], sql="SELECT * FROM clean_users"),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        assert len(result.assembly.joints) == 3
        topo = result.assembly.topological_order()
        assert topo.index("raw_users") < topo.index("clean_users")
        assert topo.index("clean_users") < topo.index("output_users")


class TestBuildAssemblySQLGeneration:
    """Test SQL generation for YAML source/sink joints with columns."""

    def test_source_with_columns_generates_sql(self):
        decls = [
            _source_decl(
                "raw_users",
                sql=None,
                columns=[ColumnDecl(name="id", expression=None), ColumnDecl(name="name", expression=None)],
                table="users_table",
            ),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["raw_users"]
        assert joint.sql is not None
        assert "id" in joint.sql
        assert "name" in joint.sql

    def test_source_with_expression_columns(self):
        decls = [
            _source_decl(
                "raw_users",
                sql=None,
                columns=[
                    ColumnDecl(name="user_id", expression=None),
                    ColumnDecl(name="full_name", expression="UPPER(name)"),
                ],
                table="users_table",
            ),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["raw_users"]
        assert joint.sql is not None
        assert "full_name" in joint.sql
        assert "UPPER" in joint.sql

    def test_source_with_filter(self):
        decls = [
            _source_decl(
                "raw_users",
                sql=None,
                columns=[ColumnDecl(name="id", expression=None)],
                table="users_table",
                filter="active = TRUE",
            ),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["raw_users"]
        assert joint.sql is not None
        assert "WHERE" in joint.sql


class TestBuildAssemblyUpstreamInference:
    """Test upstream inference from SQL."""

    def test_infers_upstream_from_sql(self):
        decls = [
            _source_decl("raw_users"),
            JointDeclaration(
                name="clean_users",
                joint_type="sql",
                source_path=Path("joints/clean_users.sql"),
                sql="SELECT * FROM raw_users WHERE active = TRUE",
                upstream=None,  # should be inferred
            ),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["clean_users"]
        assert "raw_users" in joint.upstream

    def test_explicit_upstream_not_overridden(self):
        decls = [
            _source_decl("raw_users"),
            _source_decl("raw_orders"),
            _sql_decl("clean_users", upstream=["raw_users"], sql="SELECT * FROM raw_orders"),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        # Explicit upstream should be used, not inferred from SQL
        joint = result.assembly.joints["clean_users"]
        assert joint.upstream == ["raw_users"]


class TestBuildAssemblyErrorCollection:
    """Test that errors from multiple phases are collected together."""

    def test_unknown_catalog_and_engine(self):
        profile = ResolvedProfile(
            name="test",
            default_engine="bad_engine",
            catalogs={"bad_cat": CatalogConfig(name="bad_cat", type="nonexistent_type", options={})},
            engines=[EngineConfig(name="bad_engine", type="nonexistent_engine", catalogs=[], options={})],
        )
        config = _make_config_result(
            declarations=[_source_decl("raw_users")],
            profile=profile,
        )
        registry = PluginRegistry()  # empty registry, no builtins

        with pytest.raises(BridgeValidationError) as exc_info:
            build_assembly(config, registry)

        errors = exc_info.value.errors
        codes = [e.code for e in errors]
        assert "BRG-201" in codes  # unknown catalog
        assert "BRG-203" in codes  # unknown engine

    def test_unknown_engine_reference_in_joint(self):
        decls = [
            _source_decl("raw_users", engine="nonexistent_engine"),
        ]
        config = _make_config_result(declarations=decls)

        with pytest.raises(BridgeValidationError) as exc_info:
            build_assembly(config, _make_registry())

        errors = exc_info.value.errors
        codes = [e.code for e in errors]
        assert "BRG-207" in codes

    def test_cycle_detection(self):
        decls = [
            _source_decl("raw_users"),
            _sql_decl("a", upstream=["b"]),
            _sql_decl("b", upstream=["a"]),
        ]
        config = _make_config_result(declarations=decls)

        with pytest.raises(BridgeValidationError) as exc_info:
            build_assembly(config, _make_registry())

        errors = exc_info.value.errors
        codes = [e.code for e in errors]
        assert "BRG-210" in codes

    def test_duplicate_joint_name(self):
        decls = [
            _source_decl("raw_users"),
            _source_decl("raw_users"),
        ]
        config = _make_config_result(declarations=decls)

        with pytest.raises(BridgeValidationError) as exc_info:
            build_assembly(config, _make_registry())

        errors = exc_info.value.errors
        codes = [e.code for e in errors]
        assert "BRG-208" in codes

    def test_sink_without_upstream(self):
        decls = [
            JointDeclaration(
                name="output_users",
                joint_type="sink",
                source_path=Path("sinks/output_users.yaml"),
                sql="SELECT 1",
                upstream=[],
            ),
        ]
        config = _make_config_result(declarations=decls)

        with pytest.raises(BridgeValidationError) as exc_info:
            build_assembly(config, _make_registry())

        errors = exc_info.value.errors
        codes = [e.code for e in errors]
        assert "BRG-205" in codes


class TestBuildAssemblyQualityChecks:
    """Test quality check conversion."""

    def test_assertion_quality_checks_preserved(self):
        decls = [
            _source_decl(
                "raw_users",
                quality_checks=[
                    QualityCheck(
                        check_type="not_null",
                        phase="assertion",
                        severity="error",
                        config={"column": "id"},
                        source="inline",
                        source_file=Path("sources/raw_users.yaml"),
                    ),
                ],
            ),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["raw_users"]
        assert len(joint.assertions) == 1
        assert joint.assertions[0].type == "not_null"
        assert joint.assertions[0].config == {"column": "id"}

    def test_audit_on_non_sink_produces_error(self):
        decls = [
            _source_decl(
                "raw_users",
                quality_checks=[
                    QualityCheck(
                        check_type="not_null",
                        phase="audit",
                        severity="error",
                        config={},
                        source="inline",
                        source_file=Path("sources/raw_users.yaml"),
                    ),
                ],
            ),
        ]
        config = _make_config_result(declarations=decls)

        with pytest.raises(BridgeValidationError) as exc_info:
            build_assembly(config, _make_registry())

        codes = [e.code for e in exc_info.value.errors]
        assert "BRG-206" in codes


class TestBuildAssemblyFieldMapping:
    """Test that declaration fields are correctly mapped to Joint fields."""

    def test_write_strategy_mapped(self):
        decls = [
            _source_decl("raw_users"),
            _sink_decl(
                "output_users",
                upstream=["raw_users"],
                sql="SELECT * FROM raw_users",
                write_strategy=WriteStrategyDecl(mode="append", options={}),
            ),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["output_users"]
        assert joint.write_strategy == "append"

    def test_fusion_strategy_mapped(self):
        decls = [
            _source_decl("raw_users", fusion_strategy="never"),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["raw_users"]
        assert joint.fusion_strategy_override == "never"

    def test_materialization_strategy_mapped(self):
        decls = [
            _source_decl("raw_users", materialization_strategy="eager"),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["raw_users"]
        assert joint.materialization_strategy_override == "eager"

    def test_tags_and_description_mapped(self):
        decls = [
            _source_decl("raw_users", tags=["pii", "raw"], description="Raw user data"),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint = result.assembly.joints["raw_users"]
        assert joint.tags == ["pii", "raw"]
        assert joint.description == "Raw user data"


class TestBuildAssemblyDeterministicOrder:
    """Test deterministic joint processing order matches input order."""

    def test_joint_order_matches_input_order(self):
        decls = [
            _source_decl("z_source"),
            _source_decl("a_source"),
            _sql_decl("m_joint", upstream=["a_source"]),
            _sink_decl("b_sink", upstream=["m_joint"], sql="SELECT * FROM m_joint"),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        joint_names = list(result.assembly.joints.keys())
        assert joint_names == ["z_source", "a_source", "m_joint", "b_sink"]

    def test_repeated_builds_produce_same_order(self):
        decls = [
            _source_decl("c_source"),
            _source_decl("a_source"),
            _sql_decl("b_joint", upstream=["a_source"]),
        ]
        config = _make_config_result(declarations=decls)

        result1 = build_assembly(config, _make_registry())
        result2 = build_assembly(config, _make_registry())

        assert list(result1.assembly.joints.keys()) == list(result2.assembly.joints.keys())


class TestBuildAssemblyMultiPhaseErrors:
    """Test that errors from multiple distinct phases are all collected."""

    def test_errors_from_catalog_engine_and_converter_phases(self):
        profile = ResolvedProfile(
            name="test",
            default_engine="bad_engine",
            catalogs={"bad_cat": CatalogConfig(name="bad_cat", type="nonexistent_type", options={})},
            engines=[EngineConfig(name="bad_engine", type="nonexistent_engine", catalogs=[], options={})],
        )
        decls = [
            _source_decl("raw_users", engine="missing_engine"),
        ]
        config = _make_config_result(declarations=decls, profile=profile)
        registry = PluginRegistry()

        with pytest.raises(BridgeValidationError) as exc_info:
            build_assembly(config, registry)

        codes = [e.code for e in exc_info.value.errors]
        # Catalog phase error
        assert "BRG-201" in codes
        # Engine phase error
        assert "BRG-203" in codes
        # Converter phase error (unknown engine ref)
        assert "BRG-207" in codes
        # At least 3 errors from 3 different phases
        assert len(exc_info.value.errors) >= 3


class TestBuildAssemblyBridgeResult:
    """Test BridgeResult contents."""

    def test_result_contains_catalogs_and_engines(self):
        decls = [_source_decl("raw_users")]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        assert "test_catalog" in result.catalogs
        assert "test_engine" in result.engines
        assert result.profile_snapshot.name == "test"

    def test_source_formats_tracked(self):
        decls = [
            _source_decl("raw_users", columns=[ColumnDecl(name="id", expression=None)], sql=None, table="users"),
            _sql_decl("clean_users", upstream=["raw_users"]),
        ]
        config = _make_config_result(declarations=decls)
        result = build_assembly(config, _make_registry())

        assert result.source_formats["raw_users"] == "yaml"
        assert result.source_formats["clean_users"] == "sql"
