"""Unit tests for rivet_config data models: immutability, constants, and defaults."""

from pathlib import Path

import pytest

from rivet_config.models import (
    CHECK_TYPES,
    JOINT_NAME_MAX_LENGTH,
    JOINT_NAME_PATTERN,
    JOINT_TYPES,
    MANIFEST_DEPRECATED_KEYS,
    MANIFEST_OPTIONAL_KEYS,
    MANIFEST_REQUIRED_KEYS,
    WRITE_STRATEGY_MODES,
    YAML_JOINT_FIELDS,
    CatalogConfig,
    ColumnDecl,
    EngineConfig,
    JointDeclaration,
    ProjectManifest,
    QualityCheck,
    ResolvedProfile,
    WriteStrategyDecl,
)

# --- Immutability tests ---


def test_project_manifest_frozen():
    m = ProjectManifest(
        project_root=Path("/p"),
        profiles_path=Path("/p/profiles.yaml"),
        sources_dir=Path("/p/sources"),
        joints_dir=Path("/p/joints"),
        sinks_dir=Path("/p/sinks"),
        quality_dir=None,
        tests_dir=Path("/p/tests"),
        fixtures_dir=Path("/p/fixtures"),
    )
    with pytest.raises((AttributeError, TypeError)):
        m.project_root = Path("/other")  # type: ignore[misc]


def test_catalog_config_frozen():
    c = CatalogConfig(name="cat", type="duckdb", options={})
    with pytest.raises((AttributeError, TypeError)):
        c.name = "other"  # type: ignore[misc]


def test_engine_config_frozen():
    e = EngineConfig(name="eng", type="duckdb", catalogs=["cat"], options={})
    with pytest.raises((AttributeError, TypeError)):
        e.name = "other"  # type: ignore[misc]


def test_resolved_profile_frozen():
    rp = ResolvedProfile(name="dev", default_engine="eng", catalogs={}, engines=[])
    with pytest.raises((AttributeError, TypeError)):
        rp.name = "other"  # type: ignore[misc]


def test_column_decl_frozen():
    c = ColumnDecl(name="col", expression=None)
    with pytest.raises((AttributeError, TypeError)):
        c.name = "other"  # type: ignore[misc]


def test_write_strategy_decl_frozen():
    w = WriteStrategyDecl(mode="append", options={})
    with pytest.raises((AttributeError, TypeError)):
        w.mode = "replace"  # type: ignore[misc]


def test_quality_check_frozen():
    qc = QualityCheck(
        check_type="not_null",
        phase="assertion",
        severity="error",
        config={},
        source="inline",
        source_file=Path("f.yaml"),
    )
    with pytest.raises((AttributeError, TypeError)):
        qc.check_type = "unique"  # type: ignore[misc]


def test_joint_declaration_frozen():
    jd = JointDeclaration(name="j", joint_type="sql", source_path=Path("j.sql"))
    with pytest.raises((AttributeError, TypeError)):
        jd.name = "other"  # type: ignore[misc]


# --- Validation constants ---


def test_manifest_required_keys():
    assert {"profiles", "sources", "joints", "sinks"} == MANIFEST_REQUIRED_KEYS


def test_manifest_optional_keys():
    assert "quality" in MANIFEST_OPTIONAL_KEYS
    assert "tests" in MANIFEST_OPTIONAL_KEYS
    assert "fixtures" in MANIFEST_OPTIONAL_KEYS


def test_manifest_deprecated_keys():
    assert "assertions" in MANIFEST_DEPRECATED_KEYS
    assert "audits" in MANIFEST_DEPRECATED_KEYS


def test_joint_types():
    assert {"source", "sql", "sink", "python"} == JOINT_TYPES


def test_write_strategy_modes():
    assert "append" in WRITE_STRATEGY_MODES
    assert "replace" in WRITE_STRATEGY_MODES
    assert "merge" in WRITE_STRATEGY_MODES
    assert len(WRITE_STRATEGY_MODES) == 7


def test_check_types():
    expected = {"not_null", "unique", "row_count", "accepted_values", "expression",
                "custom", "schema", "freshness", "relationship"}
    assert expected == CHECK_TYPES


def test_joint_name_pattern_valid():
    assert JOINT_NAME_PATTERN.match("my_joint")
    assert JOINT_NAME_PATTERN.match("a")
    assert JOINT_NAME_PATTERN.match("abc123")


def test_joint_name_pattern_invalid():
    assert not JOINT_NAME_PATTERN.match("_bad")
    assert not JOINT_NAME_PATTERN.match("1bad")
    assert not JOINT_NAME_PATTERN.match("Bad")
    assert not JOINT_NAME_PATTERN.match("")


def test_joint_name_max_length():
    assert JOINT_NAME_MAX_LENGTH == 128


def test_yaml_joint_fields_contains_core_fields():
    for field in ("name", "type", "sql", "columns", "catalog", "table", "engine",
                  "upstream", "tags", "write_strategy", "function"):
        assert field in YAML_JOINT_FIELDS


# --- JointDeclaration defaults ---


def test_joint_declaration_defaults():
    jd = JointDeclaration(name="j", joint_type="sql", source_path=Path("j.sql"))
    assert jd.sql is None
    assert jd.catalog is None
    assert jd.table is None
    assert jd.columns is None
    assert jd.filter is None
    assert jd.write_strategy is None
    assert jd.function is None
    assert jd.engine is None
    assert jd.eager is False
    assert jd.upstream is None
    assert jd.tags is None
    assert jd.description is None
    assert jd.fusion_strategy is None
    assert jd.materialization_strategy is None
    assert jd.quality_checks == []


def test_joint_declaration_quality_checks_default_is_empty_list():
    jd1 = JointDeclaration(name="a", joint_type="sql", source_path=Path("a.sql"))
    jd2 = JointDeclaration(name="b", joint_type="sql", source_path=Path("b.sql"))
    # Each instance gets its own list (field default_factory)
    assert jd1.quality_checks is not jd2.quality_checks
