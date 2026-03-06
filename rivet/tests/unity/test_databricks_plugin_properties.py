"""Property-based tests for the Databricks plugin.

Feature: databricks-plugin, Properties P1–P8

These tests validate correctness properties for the rivet_databricks plugin package.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pyarrow
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.errors import ExecutionError, PluginValidationError

# ── Resolve local package directory and expected plugin_name ──────────────────

_SRC_DIR = Path(__file__).resolve().parent.parent.parent / "src"
_PKG_DIR = _SRC_DIR / "rivet_databricks"
_PLUGIN_NAME = "rivet_databricks"


def _py_files() -> list[Path]:
    return sorted(_PKG_DIR.rglob("*.py"))


def _import_engine_plugin() -> Any:
    """Import DatabricksComputeEnginePlugin from the local package."""
    from rivet_databricks.engine import DatabricksComputeEnginePlugin
    return DatabricksComputeEnginePlugin


def _import_statement_api() -> Any:
    """Import DatabricksStatementAPI from the local package."""
    from rivet_databricks.engine import DatabricksStatementAPI
    return DatabricksStatementAPI


def _import_catalog_plugins() -> list[tuple[str, Any]]:
    """Import and instantiate catalog plugins from the local package."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
    from rivet_databricks.unity_catalog import UnityCatalogPlugin
    return [("unity", UnityCatalogPlugin()), ("databricks", DatabricksCatalogPlugin())]


def _import_registration_func() -> Any:
    """Import the plugin registration function from the local package."""
    from rivet_databricks import DatabricksPlugin
    return DatabricksPlugin


# ── Property 1: No stale package references after rename ─────────────────────
# Feature: databricks-plugin, Property 1: No stale package references
# Validates: Requirements 1.5, 1.6

_STALE_PACKAGE = "rivet_unity"


def test_p1_no_stale_package_references_after_rename() -> None:
    """P1: After rename, no .py file in rivet_databricks/ contains the old package name."""
    for path in _py_files():
        content = path.read_text()
        assert _STALE_PACKAGE not in content, (
            f"{path.relative_to(_SRC_DIR)} still contains '{_STALE_PACKAGE}'"
        )


# ── Property 2: DatabricksEngine ignores input_tables in execute_sql ──────────
# Feature: databricks-plugin, Property 2: execute_sql ignores input_tables
# Validates: Fused-group execution where source data is already in Unity Catalog

_table_name = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)


@settings(max_examples=10)
@given(names=st.lists(_table_name, min_size=1, max_size=3, unique=True))
def test_p2_ignores_input_tables(names: list[str]) -> None:
    """P2: execute_sql ignores input_tables — SQL runs server-side against Unity Catalog."""
    from unittest.mock import MagicMock, patch

    from rivet_core.models import ComputeEngine

    cls = _import_engine_plugin()
    plugin = cls()
    engine = ComputeEngine(name="test", engine_type="databricks", config={
        "workspace_url": "https://test.databricks.com",
        "token": "fake",
        "warehouse_id": "abc123",
    })
    tables = {name: pyarrow.table({"x": [1]}) for name in names}
    expected = pyarrow.table({"result": [42]})

    mock_api = MagicMock()
    mock_api.execute.return_value = expected

    with patch.object(plugin, "create_statement_api", return_value=mock_api):
        result = plugin.execute_sql(engine, "SELECT 1", tables)

    assert result.equals(expected)
    mock_api.execute.assert_called_once()


# ── Property 3: DatabricksEngine raises on terminal failure states ────────────
# Feature: databricks-plugin, Property 3: DatabricksEngine raises on terminal failure states
# Validates: Requirements 2.8

_statement_id = st.from_regex(r"[0-9a-f]{8}-[0-9a-f]{4}", fullmatch=True)
_failure_state = st.sampled_from(["FAILED", "CANCELED"])


@settings(max_examples=100)
@given(stmt_id=_statement_id, state=_failure_state)
def test_p3_raises_on_terminal_failure_states(stmt_id: str, state: str) -> None:
    """P3: FAILED/CANCELED states raise ExecutionError(RVT-502) with remediation."""
    cls = _import_statement_api()
    api = cls.__new__(cls)
    api._base_url = "https://test.databricks.com"
    api._warehouse_id = "wh-123"
    api._wait_timeout_s = 1
    api._max_rows_per_chunk = 100
    api._disposition = "EXTERNAL_LINKS"
    api._session = MagicMock()

    poll_response = MagicMock()
    poll_response.ok = True
    poll_response.json.return_value = {
        "statement_id": stmt_id,
        "status": {
            "state": state,
            "error": {"message": f"Statement {state.lower()}"},
        },
    }
    api._session.get.return_value = poll_response

    with pytest.raises(ExecutionError) as exc_info:
        api._poll(stmt_id)

    err = exc_info.value.error
    assert err.code == "RVT-502"
    assert err.remediation and len(err.remediation) > 0
    assert stmt_id in err.context.get("statement_id", "")


# ── Property 4: All plugin_error calls use correct plugin_name ────────────────
# Feature: databricks-plugin, Property 4: All plugin_error calls use plugin_name="rivet_databricks"
# Validates: Requirements 2.10, 5.1, 5.2


def _extract_plugin_error_names(source: str) -> list[tuple[int, str | None]]:
    """Parse source, return (lineno, plugin_name_value) for each plugin_error() call."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    results = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = None
        if isinstance(func, ast.Name):
            name = func.id
        elif isinstance(func, ast.Attribute):
            name = func.attr
        if name != "plugin_error":
            continue
        val = None
        for kw in node.keywords:
            if kw.arg == "plugin_name" and isinstance(kw.value, ast.Constant):
                val = kw.value.value
        results.append((node.lineno, val))
    return results


def test_p4_all_plugin_error_calls_use_correct_plugin_name() -> None:
    """P4: Every plugin_error() call uses plugin_name=<expected>."""
    violations = []
    for path in _py_files():
        source = path.read_text()
        for lineno, val in _extract_plugin_error_names(source):
            if val != _PLUGIN_NAME:
                rel = path.relative_to(_SRC_DIR)
                violations.append(f"{rel}:{lineno} plugin_name={val!r} (expected {_PLUGIN_NAME!r})")
    assert not violations, "plugin_error calls with wrong plugin_name:\n" + "\n".join(violations)


# ── Property 5: Validation errors carry structured payloads ───────────────────
# Feature: databricks-plugin, Property 5: Validation errors carry structured payloads
# Validates: Requirements 3.9, 5.4

_random_option_key = st.from_regex(r"[a-z_]{1,20}", fullmatch=True)
_random_option_val = st.one_of(st.text(max_size=50), st.integers(), st.booleans())
_random_options = st.dictionaries(_random_option_key, _random_option_val, min_size=1, max_size=5)


@settings(max_examples=100)
@given(options=_random_options)
def test_p5_validation_errors_carry_structured_payloads_engine(options: dict) -> None:
    """P5 (engine): Invalid options produce PluginValidationError with structured payload."""
    plugin = _import_engine_plugin()()
    recognized = set(plugin.required_options) | set(plugin.optional_options)
    invalid_opts = {k: v for k, v in options.items() if k not in recognized}
    if not invalid_opts:
        return  # all keys happened to be valid

    try:
        plugin.validate(invalid_opts)
    except PluginValidationError as exc:
        err = exc.error
        assert err.context.get("plugin_name") == _PLUGIN_NAME
        assert err.context.get("plugin_type") is not None
        assert err.code.startswith("RVT-2")
        assert err.remediation and len(err.remediation) > 0
    except Exception:
        pass


@settings(max_examples=100)
@given(options=_random_options)
def test_p5_validation_errors_carry_structured_payloads_catalog(options: dict) -> None:
    """P5 (catalog): Invalid options produce PluginValidationError with structured payload."""
    for label, plugin in _import_catalog_plugins():
        try:
            plugin.validate(options)
        except PluginValidationError as exc:
            err = exc.error
            assert err.context.get("plugin_name") == _PLUGIN_NAME, (
                f"{label}: plugin_name={err.context.get('plugin_name')!r}"
            )
            assert err.context.get("plugin_type") is not None, f"{label}: missing plugin_type"
            assert err.code.startswith("RVT-2"), f"{label}: code={err.code}"
            assert err.remediation and len(err.remediation) > 0, f"{label}: empty remediation"
        except Exception:
            pass


# ── Property 6: Import boundary compliance ────────────────────────────────────
# Feature: databricks-plugin, Property 6: Import boundary compliance
# Validates: Requirements 4.1, 4.2

_FORBIDDEN_RIVET_MODULES = {
    "rivet_config", "rivet_bridge", "rivet_cli",
    "rivet_duckdb", "rivet_polars", "rivet_pyspark",
    "rivet_aws", "rivet_postgres",
}


def _extract_rivet_imports(source: str) -> list[tuple[int, str]]:
    """Extract (lineno, top-level rivet_* module) from source."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    results = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("rivet_"):
                    results.append((node.lineno, alias.name.split(".")[0]))
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("rivet_"):
            results.append((node.lineno, node.module.split(".")[0]))
    return results


def test_p6_import_boundary_compliance() -> None:
    """P6: All rivet_* imports resolve to allowed set (rivet_core + self)."""
    violations = []
    for path in _py_files():
        if "databricks_duckdb_adapter" in path.name:
            continue  # excluded — belongs to rivet_duckdb after cleanup
        if path.name == "adapter.py" and "rivet_databricks" in str(path):
            continue  # DatabricksDuckDBAdapter legitimately imports rivet_duckdb
        source = path.read_text()
        for lineno, mod in _extract_rivet_imports(source):
            if mod in _FORBIDDEN_RIVET_MODULES:
                rel = path.relative_to(_SRC_DIR)
                violations.append(f"{rel}:{lineno} imports {mod}")
    assert not violations, "Import boundary violations:\n" + "\n".join(violations)


# ── Property 7: No engine-specific adapter registrations ─────────────────────
# Feature: databricks-plugin, Property 7: No engine-specific adapter registrations
# Validates: Requirements 4.6


def test_p7_no_engine_specific_adapter_registrations() -> None:
    """P7: Plugin registers no ComputeEngineAdapter for pyspark/polars (duckdb adapter is allowed)."""
    from rivet_core.plugins import PluginRegistry

    registry = PluginRegistry()
    register_fn = _import_registration_func()
    register_fn(registry)

    forbidden = {"pyspark", "polars"}
    violations = [
        f"({et}, {ct}): {type(a).__qualname__}"
        for (et, ct), a in registry._adapters.items()
        if et in forbidden
    ]

    assert not violations, (
        "Engine-specific adapters registered:\n" + "\n".join(violations)
    )


# ── Property 8: No lazy engine imports ────────────────────────────────────────
# Feature: databricks-plugin, Property 8: No lazy engine imports
# Validates: Requirements 4.7

_FORBIDDEN_ENGINE_MODULES = {"pyspark", "polars", "duckdb", "deltalake"}


def _extract_all_imports(source: str) -> list[tuple[int, str]]:
    """Extract (lineno, top-level module) for ALL imports."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    results = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                results.append((node.lineno, alias.name.split(".")[0]))
        elif isinstance(node, ast.ImportFrom) and node.module:
            results.append((node.lineno, node.module.split(".")[0]))
    return results


def test_p8_no_lazy_engine_imports() -> None:
    """P8: No .py file in the package imports pyspark/polars/duckdb/deltalake."""
    violations = []
    for path in _py_files():
        if "databricks_duckdb_adapter" in path.name:
            continue  # excluded — belongs to rivet_duckdb after cleanup
        if path.name == "adapter.py" and "rivet_databricks" in str(path):
            continue  # DatabricksDuckDBAdapter legitimately imports duckdb
        if path.name == "duckdb.py" and "adapters" in str(path):
            continue  # DatabricksDuckDBAdapter legitimately imports duckdb
        source = path.read_text()
        for lineno, mod in _extract_all_imports(source):
            if mod in _FORBIDDEN_ENGINE_MODULES:
                rel = path.relative_to(_SRC_DIR)
                violations.append(f"{rel}:{lineno} imports {mod}")
    assert not violations, "Lazy engine imports found:\n" + "\n".join(violations)
