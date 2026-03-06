"""Tests for rivet_core.errors."""

from rivet_core.errors import (
    CompilationError,
    ExecutionError,
    PluginValidationError,
    RivetError,
    SQLParseError,
)


def test_rivet_error_frozen() -> None:
    err = RivetError(code="RVT-301", message="Cycle detected")
    assert err.code == "RVT-301"
    assert err.context == {}
    assert err.remediation is None
    assert err.original_sql is None


def test_rivet_error_str_with_remediation() -> None:
    err = RivetError(
        code="RVT-401",
        message="No engine resolved",
        remediation="Set default_engine in profile",
    )
    s = str(err)
    assert "[RVT-401]" in s
    assert "Remediation:" in s


def test_rivet_error_sql_fields() -> None:
    err = RivetError(
        code="RVT-701",
        message="Parse failure",
        original_sql="SELECT * FORM t",
        dialect="duckdb",
        error_position=(1, 14),
        failing_construct="FORM",
    )
    assert err.error_position == (1, 14)
    assert err.failing_construct == "FORM"


def test_compilation_error() -> None:
    errors = [RivetError(code="RVT-301", message="Cycle")]
    exc = CompilationError(errors)
    assert exc.errors == errors
    assert "1 error(s)" in str(exc)


def test_execution_error() -> None:
    err = RivetError(code="RVT-501", message="Runtime failure")
    exc = ExecutionError(err)
    assert exc.error is err
    assert "RVT-501" in str(exc)


def test_plugin_validation_error() -> None:
    err = RivetError(code="RVT-201", message="Missing option")
    exc = PluginValidationError(err)
    assert exc.error is err


def test_sql_parse_error() -> None:
    err = RivetError(code="RVT-701", message="Bad SQL", original_sql="DROP TABLE t")
    exc = SQLParseError(err)
    assert exc.error is err
    assert exc.error.original_sql == "DROP TABLE t"


def test_rivet_error_context() -> None:
    err = RivetError(
        code="RVT-101",
        message="Engine error",
        context={"engine": "duckdb", "joint": "my_joint"},
    )
    assert err.context["engine"] == "duckdb"
    assert err.context["joint"] == "my_joint"
