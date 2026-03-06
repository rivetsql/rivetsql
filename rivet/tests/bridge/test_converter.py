"""Tests for JointConverter."""

from pathlib import Path

from rivet_bridge.converter import JointConverter
from rivet_config import (
    JointDeclaration,
    QualityCheck,
    WriteStrategyDecl,
)
from rivet_core import ComputeEngine


def _decl(
    name: str = "test_joint",
    joint_type: str = "sql",
    **kwargs,
) -> JointDeclaration:
    return JointDeclaration(
        name=name,
        joint_type=joint_type,
        source_path=kwargs.pop("source_path", Path("joints/test_joint.sql")),
        **kwargs,
    )


def _engines(*names: str) -> dict[str, ComputeEngine]:
    return {n: ComputeEngine(name=n, engine_type="duckdb") for n in names}


class TestFieldMapping:
    """Requirement 6.1: direct field mapping."""

    def test_basic_fields(self):
        converter = JointConverter()
        decl = _decl(
            name="my_joint",
            joint_type="sql",
            catalog="warehouse",
            upstream=["src_a", "src_b"],
            tags=["finance", "daily"],
            description="A test joint",
            sql="SELECT * FROM src_a",
            engine="eng1",
            eager=True,
            table="my_table",
        )
        joint, errors = converter.convert(decl, _engines("eng1"))
        assert errors == []
        assert joint is not None
        assert joint.name == "my_joint"
        assert joint.joint_type == "sql"
        assert joint.catalog == "warehouse"
        assert joint.upstream == ["src_a", "src_b"]
        assert joint.tags == ["finance", "daily"]
        assert joint.description == "A test joint"
        assert joint.sql == "SELECT * FROM src_a"
        assert joint.engine == "eng1"
        assert joint.eager is True
        assert joint.table == "my_table"
        assert joint.source_file == "joints/test_joint.sql"

    def test_none_upstream_becomes_empty_list(self):
        converter = JointConverter()
        decl = _decl(upstream=None)
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert joint.upstream == []

    def test_none_tags_becomes_empty_list(self):
        converter = JointConverter()
        decl = _decl(tags=None)
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert joint.tags == []

    def test_function_field(self):
        converter = JointConverter()
        decl = _decl(joint_type="python", function="my_module.transform")
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert joint.function == "my_module.transform"


class TestStrategyMapping:
    """Requirements 6.2, 6.3, 6.4: strategy field mapping."""

    def test_write_strategy(self):
        converter = JointConverter()
        decl = _decl(write_strategy=WriteStrategyDecl(mode="merge", options={"key": "id"}))
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert joint.write_strategy == "merge"

    def test_no_write_strategy(self):
        converter = JointConverter()
        decl = _decl()
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert joint.write_strategy is None

    def test_fusion_strategy(self):
        converter = JointConverter()
        decl = _decl(fusion_strategy="never")
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert joint.fusion_strategy_override == "never"

    def test_materialization_strategy(self):
        converter = JointConverter()
        decl = _decl(materialization_strategy="eager")
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert joint.materialization_strategy_override == "eager"


class TestQualityCheckConversion:
    """Requirements 6.5, 19.1-19.4: quality check → Assertion."""

    def _qc(self, phase="assertion", check_type="not_null", severity="error", **kwargs):
        return QualityCheck(
            check_type=check_type,
            phase=phase,
            severity=severity,
            config=kwargs.get("config", {"column": "id"}),
            source="inline",
            source_file=Path("quality/test.yaml"),
        )

    def test_assertion_conversion(self):
        converter = JointConverter()
        decl = _decl(quality_checks=[self._qc(check_type="not_null", severity="warning", config={"column": "id"})])
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert len(joint.assertions) == 1
        a = joint.assertions[0]
        assert a.type == "not_null"
        assert a.severity == "warning"
        assert a.config == {"column": "id"}
        assert a.phase == "assertion"

    def test_multiple_assertions(self):
        converter = JointConverter()
        checks = [
            self._qc(check_type="not_null"),
            self._qc(check_type="unique", config={"column": "email"}),
        ]
        decl = _decl(quality_checks=checks)
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert len(joint.assertions) == 2

    def test_audit_on_sink_allowed(self):
        converter = JointConverter()
        decl = _decl(
            joint_type="sink",
            upstream=["src"],
            quality_checks=[self._qc(phase="audit")],
        )
        joint, errors = converter.convert(decl, {})
        # Audit on sink is fine — no assertions created (audits are not assertions)
        assert errors == []
        assert joint is not None
        assert joint.assertions == []

    def test_audit_checks_not_converted_to_assertions(self):
        converter = JointConverter()
        decl = _decl(
            joint_type="sink",
            upstream=["src"],
            quality_checks=[
                self._qc(phase="assertion", check_type="not_null"),
                self._qc(phase="audit", check_type="row_count"),
            ],
        )
        joint, errors = converter.convert(decl, {})
        assert joint is not None
        assert len(joint.assertions) == 1
        assert joint.assertions[0].type == "not_null"


class TestValidation:
    """Requirements 19.5, 21.3: validation errors."""

    def test_unknown_engine_brg207(self):
        converter = JointConverter()
        decl = _decl(engine="nonexistent")
        joint, errors = converter.convert(decl, _engines("eng1"))
        assert joint is None
        assert len(errors) == 1
        assert errors[0].code == "BRG-207"
        assert errors[0].joint_name == "test_joint"
        assert "nonexistent" in errors[0].message

    def test_known_engine_no_error(self):
        converter = JointConverter()
        decl = _decl(engine="eng1")
        joint, errors = converter.convert(decl, _engines("eng1"))
        assert errors == []
        assert joint is not None

    def test_no_engine_no_error(self):
        converter = JointConverter()
        decl = _decl(engine=None)
        joint, errors = converter.convert(decl, _engines("eng1"))
        assert errors == []
        assert joint is not None
        assert joint.engine is None

    def test_audit_on_non_sink_brg206(self):
        converter = JointConverter()
        qc = QualityCheck(
            check_type="row_count",
            phase="audit",
            severity="error",
            config={},
            source="inline",
            source_file=Path("quality/test.yaml"),
        )
        decl = _decl(joint_type="sql", quality_checks=[qc])
        joint, errors = converter.convert(decl, {})
        assert joint is None
        assert len(errors) == 1
        assert errors[0].code == "BRG-206"
        assert errors[0].joint_name == "test_joint"

    def test_audit_on_source_brg206(self):
        converter = JointConverter()
        qc = QualityCheck(
            check_type="row_count",
            phase="audit",
            severity="error",
            config={},
            source="inline",
            source_file=Path("quality/test.yaml"),
        )
        decl = _decl(joint_type="source", quality_checks=[qc])
        joint, errors = converter.convert(decl, {})
        assert joint is None
        assert errors[0].code == "BRG-206"

    def test_both_errors_reported(self):
        converter = JointConverter()
        qc = QualityCheck(
            check_type="row_count",
            phase="audit",
            severity="error",
            config={},
            source="inline",
            source_file=Path("quality/test.yaml"),
        )
        decl = _decl(joint_type="sql", engine="bad_engine", quality_checks=[qc])
        joint, errors = converter.convert(decl, _engines("eng1"))
        assert joint is None
        codes = {e.code for e in errors}
        assert "BRG-206" in codes
        assert "BRG-207" in codes
