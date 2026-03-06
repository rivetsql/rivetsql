from pathlib import Path

from rivet_config.errors import ConfigError, ConfigWarning


def test_config_error_frozen():
    err = ConfigError(source_file=Path("foo.yaml"), message="bad", remediation="fix it")
    try:
        err.message = "changed"  # type: ignore[misc]
        assert False, "should be frozen"  # noqa: B011
    except Exception:
        pass


def test_config_error_fields():
    err = ConfigError(source_file=Path("a.yaml"), message="msg", remediation="rem", line_number=5)
    assert err.source_file == Path("a.yaml")
    assert err.message == "msg"
    assert err.remediation == "rem"
    assert err.line_number == 5


def test_config_error_line_number_default():
    err = ConfigError(source_file=None, message="m", remediation="r")
    assert err.line_number is None
    assert err.source_file is None


def test_config_warning_frozen():
    w = ConfigWarning(source_file=None, message="warn", remediation="fix")
    try:
        w.message = "changed"  # type: ignore[misc]
        assert False, "should be frozen"  # noqa: B011
    except Exception:
        pass


def test_config_warning_fields():
    w = ConfigWarning(source_file=Path("b.yaml"), message="w", remediation="r")
    assert w.source_file == Path("b.yaml")
    assert w.message == "w"
    assert w.remediation == "r"


def test_config_warning_remediation_default():
    w = ConfigWarning(source_file=None, message="w")
    assert w.remediation is None
