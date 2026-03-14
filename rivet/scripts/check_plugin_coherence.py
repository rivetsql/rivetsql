#!/usr/bin/env python3
"""Plugin coherence audit script.

Scans all Rivet plugin packages against an expected capability matrix and
reports gaps.  Checks plugin types, catalog introspection methods, engine
optional methods, sink write strategies, error handling patterns, validation
patterns, adapter coverage, registration conventions, source pushdown,
MaterializedRef contracts, and documentation.

Exit code 0 means all plugins are coherent; exit code 1 means gaps found.

Follows the same standalone-script pattern as
``scripts/check_module_boundaries.py``.
"""

from __future__ import annotations

import importlib
import inspect
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AuditFinding:
    """A single gap discovered by the coherence checker."""

    category: str  # e.g. "capability_matrix", "catalog_introspection", …
    package: str  # e.g. "rivet_duckdb", "arrow", …
    plugin_class: str  # e.g. "DuckDBCatalogPlugin", "ArrowSink", …
    severity: str  # "error" or "warning"
    message: str


@dataclass
class AuditReport:
    """Aggregated audit results."""

    findings: list[AuditFinding] = field(default_factory=list)
    capability_matrix: dict[str, dict[str, bool]] = field(default_factory=dict)
    adapter_matrix: dict[tuple[str, str], bool] = field(default_factory=dict)
    sink_strategies: dict[str, list[str]] = field(default_factory=dict)
    skipped_packages: list[str] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "warning")


# ---------------------------------------------------------------------------
# Expected capability matrix
# ---------------------------------------------------------------------------

EXPECTED_CAPABILITIES: dict[str, dict[str, bool]] = {
    # Full-stack packages
    "rivet_duckdb": {
        "catalog": True,
        "engine": True,
        "adapter": True,
        "source": True,
        "sink": True,
        "cross_joint": False,
    },
    "rivet_postgres": {
        "catalog": True,
        "engine": True,
        "adapter": True,
        "source": True,
        "sink": True,
        "cross_joint": True,
    },
    "rivet_databricks": {
        "catalog": True,
        "engine": True,
        "adapter": True,
        "source": True,
        "sink": True,
        "cross_joint": True,
    },
    # Engine-only packages
    "rivet_polars": {
        "catalog": False,
        "engine": True,
        "adapter": True,
        "source": False,
        "sink": False,
        "cross_joint": False,
    },
    "rivet_pyspark": {
        "catalog": False,
        "engine": True,
        "adapter": True,
        "source": False,
        "sink": False,
        "cross_joint": False,
    },
    # Catalog/Source/Sink-only packages
    "rivet_aws": {
        "catalog": True,
        "engine": False,
        "adapter": False,
        "source": True,
        "sink": True,
        "cross_joint": False,
    },
    # Catalog/Source/Sink + Adapter packages
    "rivet_rest": {
        "catalog": True,
        "engine": False,
        "adapter": True,
        "source": True,
        "sink": True,
        "cross_joint": False,
    },
    # Built-in plugins
    "arrow": {
        "catalog": True,
        "engine": True,
        "adapter": False,
        "source": True,
        "sink": True,
        "cross_joint": False,
    },
    "filesystem": {
        "catalog": True,
        "engine": False,
        "adapter": False,
        "source": True,
        "sink": False,
        "cross_joint": False,
    },
}

# Base class label map
_BASE_CLASS_NAMES: dict[str, str] = {
    "catalog": "CatalogPlugin",
    "engine": "ComputeEnginePlugin",
    "adapter": "ComputeEngineAdapter",
    "source": "SourcePlugin",
    "sink": "SinkPlugin",
    "cross_joint": "CrossJointAdapter",
}

_CATALOG_INTROSPECTION_METHODS = [
    "list_tables",
    "get_schema",
    "get_metadata",
    "list_children",
    "test_connection",
    "get_fingerprint",
]
_CATALOG_OPTIONAL_METHODS = {"get_fingerprint"}

_ENGINE_OPTIONAL_METHODS = [
    "collect_metrics",
    "default_concurrency_limit",
    "get_reference_resolver",
    "materialization_strategy_name",
]

_BASELINE_STRATEGIES = {"append", "replace"}

_MATERIALIZED_REF_METHODS = [
    "to_arrow",
    "schema",
    "row_count",
    "storage_type",
    "size_bytes",
]

_BARE_EXCEPTION_RE = re.compile(
    r"raise\s+(?:Exception|ValueError|KeyError|NotImplementedError)\s*\("
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _src_dir() -> Path:
    """Return the ``rivet/src`` directory (same as check_module_boundaries.py)."""
    return Path(__file__).resolve().parent.parent / "src"


def _import_package(package_name: str) -> Any | None:
    """Try to import *package_name*; return the module or ``None`` on failure."""
    try:
        return importlib.import_module(package_name)
    except Exception:
        return None


def _get_base_classes() -> dict[str, type]:
    """Import and return the base plugin classes from rivet_core."""
    from rivet_core.plugins import (
        CatalogPlugin,
        ComputeEngineAdapter,
        ComputeEnginePlugin,
        CrossJointAdapter,
        SinkPlugin,
        SourcePlugin,
    )

    return {
        "catalog": CatalogPlugin,
        "engine": ComputeEnginePlugin,
        "adapter": ComputeEngineAdapter,
        "source": SourcePlugin,
        "sink": SinkPlugin,
        "cross_joint": CrossJointAdapter,
    }


def _find_subclasses(module: Any, base_class: type) -> list[type]:
    """Find concrete subclasses of *base_class* whose ``__module__`` belongs to *module*."""
    results: list[type] = []
    for _name, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, base_class) and obj is not base_class:
            obj_mod = getattr(obj, "__module__", "") or ""
            if obj_mod.startswith(module.__name__):
                results.append(obj)
    return results


def _scan_package_modules(package_name: str) -> list[Any]:
    """Import every ``.py`` file under a package directory and return the modules."""
    pkg_dir = _src_dir() / package_name
    if not pkg_dir.is_dir():
        return []
    modules: list[Any] = []
    src = _src_dir()
    for py_file in sorted(pkg_dir.rglob("*.py")):
        mod_name = str(py_file.relative_to(src)).replace(os.sep, ".").removesuffix(".py")
        try:
            modules.append(importlib.import_module(mod_name))
        except Exception:
            continue
    return modules


def _scan_builtin_modules(builtin_name: str) -> list[Any]:
    """Import built-in plugin modules (arrow, filesystem)."""
    mod_map = {
        "arrow": "rivet_core.builtins.arrow_catalog",
        "filesystem": "rivet_core.builtins.filesystem_catalog",
    }
    mod_name = mod_map.get(builtin_name)
    if mod_name is None:
        return []
    try:
        return [importlib.import_module(mod_name)]
    except Exception:
        return []


def _is_method_overridden(cls: type, method_name: str, base_class: type) -> bool:
    """Return ``True`` if *cls* overrides *method_name* from *base_class*."""
    child_attr = getattr(cls, method_name, None)
    base_attr = getattr(base_class, method_name, None)
    if child_attr is None or base_attr is None:
        return False
    # Handle properties
    child_static = inspect.getattr_static(cls, method_name, None)
    base_static = inspect.getattr_static(base_class, method_name, None)
    if isinstance(child_static, property) and isinstance(base_static, property):
        return child_static.fget is not base_static.fget
    return child_attr is not base_attr


# ---------------------------------------------------------------------------
# Check functions (tasks 1.2 – 1.9)
# ---------------------------------------------------------------------------


def check_capability_matrix(
    package: str,
    modules: list[Any],
    base_classes: dict[str, type],
    report: AuditReport,
) -> dict[str, list[type]]:
    """Task 1.2 — scan for plugin subclasses and compare against expected."""
    expected = EXPECTED_CAPABILITIES[package]
    actual: dict[str, list[type]] = {k: [] for k in _BASE_CLASS_NAMES}

    for mod in modules:
        for pt, base in base_classes.items():
            actual[pt].extend(_find_subclasses(mod, base))
    # Deduplicate while preserving order
    for k in actual:
        actual[k] = list(dict.fromkeys(actual[k]))

    report.capability_matrix[package] = {pt: len(actual[pt]) > 0 for pt in _BASE_CLASS_NAMES}

    for pt, is_expected in expected.items():
        if is_expected and not actual[pt]:
            report.findings.append(
                AuditFinding(
                    category="capability_matrix",
                    package=package,
                    plugin_class="(none)",
                    severity="error",
                    message=f"Missing expected {_BASE_CLASS_NAMES[pt]} implementation",
                )
            )
    return actual


def check_catalog_introspection(
    package: str,
    catalog_classes: list[type],
    base_classes: dict[str, type],
    report: AuditReport,
) -> None:
    """Task 1.3 — check catalog introspection method overrides."""
    base = base_classes["catalog"]
    for cls in catalog_classes:
        for method in _CATALOG_INTROSPECTION_METHODS:
            if not _is_method_overridden(cls, method, base):
                is_opt = method in _CATALOG_OPTIONAL_METHODS
                report.findings.append(
                    AuditFinding(
                        category="catalog_introspection",
                        package=package,
                        plugin_class=cls.__name__,
                        severity="warning" if is_opt else "error",
                        message=f"Does not override {method}{' (optional)' if is_opt else ''}",
                    )
                )


def check_engine_optional_methods(
    package: str,
    engine_classes: list[type],
    base_classes: dict[str, type],
    report: AuditReport,
) -> None:
    """Task 1.4 — check engine optional method overrides and supported_catalog_types."""
    base = base_classes["engine"]
    for cls in engine_classes:
        for method in _ENGINE_OPTIONAL_METHODS:
            if not _is_method_overridden(cls, method, base):
                report.findings.append(
                    AuditFinding(
                        category="engine_methods",
                        package=package,
                        plugin_class=cls.__name__,
                        severity="warning",
                        message=f"Does not override {method}",
                    )
                )
        sct = getattr(cls, "supported_catalog_types", None)
        if not sct:
            report.findings.append(
                AuditFinding(
                    category="engine_methods",
                    package=package,
                    plugin_class=cls.__name__,
                    severity="error",
                    message="supported_catalog_types is empty or missing",
                )
            )


def check_sink_strategies(
    package: str,
    sink_classes: list[type],
    report: AuditReport,
) -> None:
    """Task 1.5 — check sink write strategy declarations."""
    for cls in sink_classes:
        strategies = getattr(cls, "supported_strategies", None)
        cat_type = getattr(cls, "catalog_type", cls.__name__)
        if strategies is None:
            report.findings.append(
                AuditFinding(
                    category="sink_strategies",
                    package=package,
                    plugin_class=cls.__name__,
                    severity="warning",
                    message="No supported_strategies attribute declared",
                )
            )
            continue
        report.sink_strategies[cat_type] = list(strategies)
        for baseline in sorted(_BASELINE_STRATEGIES):
            if baseline not in set(strategies):
                report.findings.append(
                    AuditFinding(
                        category="sink_strategies",
                        package=package,
                        plugin_class=cls.__name__,
                        severity="error",
                        message=f"Missing baseline strategy '{baseline}'",
                    )
                )


def check_error_handling(package: str, report: AuditReport) -> None:
    """Task 1.6 — scan plugin source files for bare exception patterns."""
    src = _src_dir()
    if package == "arrow":
        files = [src / "rivet_core" / "builtins" / "arrow_catalog.py"]
    elif package == "filesystem":
        files = [src / "rivet_core" / "builtins" / "filesystem_catalog.py"]
    else:
        pkg_dir = src / package
        if not pkg_dir.is_dir():
            return
        files = sorted(pkg_dir.rglob("*.py"))

    for py_file in files:
        try:
            text = py_file.read_text()
        except Exception:
            continue
        for line_no, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if _BARE_EXCEPTION_RE.search(stripped):
                rel = py_file.relative_to(src)
                report.findings.append(
                    AuditFinding(
                        category="error_handling",
                        package=package,
                        plugin_class=f"{rel}:{line_no}",
                        severity="error",
                        message=f"Bare exception raise: {stripped[:120]}",
                    )
                )


def check_validation_patterns(
    package: str,
    catalog_classes: list[type],
    engine_classes: list[type],
    report: AuditReport,
) -> None:
    """Task 1.7 — check that validate methods are not no-ops."""
    for cls in catalog_classes + engine_classes:
        validate = getattr(cls, "validate", None)
        if validate is None:
            continue
        try:
            src_text = inspect.getsource(validate)
        except (OSError, TypeError):
            continue
        # Strip comments, docstrings, blank lines, and the def line
        body: list[str] = []
        in_docstring = False
        for ln in src_text.splitlines():
            s = ln.strip()
            if s.startswith("def "):
                continue
            if s.startswith('"""') or s.startswith("'''"):
                # Toggle docstring state; skip if single-line docstring
                if s.count('"""') >= 2 or s.count("'''") >= 2:
                    continue
                in_docstring = not in_docstring
                continue
            if in_docstring:
                continue
            if not s or s.startswith("#"):
                continue
            body.append(s)
        if body == ["pass"]:
            kind = "engine" if cls in engine_classes else "catalog"
            report.findings.append(
                AuditFinding(
                    category="validation",
                    package=package,
                    plugin_class=cls.__name__,
                    severity="error",
                    message=f"{kind} validate is a no-op (empty pass)",
                )
            )


def check_adapter_coverage(
    all_engine_classes: dict[str, list[type]],
    all_adapter_classes: dict[str, list[type]],
    report: AuditReport,
) -> None:
    """Task 1.8 — build adapter coverage matrix."""
    engine_support: dict[str, dict[str, list[str]]] = {}
    for _pkg, engines in all_engine_classes.items():
        for cls in engines:
            etype = getattr(cls, "engine_type", None)
            sct = getattr(cls, "supported_catalog_types", {})
            if etype:
                engine_support[etype] = sct

    adapter_pairs: dict[tuple[str, str], type] = {}
    for _pkg, adapters in all_adapter_classes.items():
        for cls in adapters:
            etype = getattr(cls, "target_engine_type", None)
            ctype = getattr(cls, "catalog_type", None)
            if etype and ctype:
                adapter_pairs[(etype, ctype)] = cls

    all_ctypes: set[str] = set()
    for sct in engine_support.values():
        all_ctypes.update(sct)
    for _e, c in adapter_pairs:
        all_ctypes.add(c)

    for etype in engine_support:
        for ctype in all_ctypes:
            has = (etype, ctype) in adapter_pairs or ctype in engine_support[etype]
            report.adapter_matrix[(etype, ctype)] = has

    for (etype, ctype), cls in adapter_pairs.items():
        caps = getattr(cls, "capabilities", None)
        if not caps:
            pkg = getattr(cls, "__module__", "unknown").split(".")[0]
            report.findings.append(
                AuditFinding(
                    category="adapter_coverage",
                    package=pkg,
                    plugin_class=cls.__name__,
                    severity="error",
                    message=f"Adapter ({etype}, {ctype}) has empty capabilities list",
                )
            )
        write_fn = getattr(cls, "write_dispatch", None)
        if write_fn is not None:
            try:
                src_text = inspect.getsource(write_fn)
                if "raise NotImplementedError" in src_text:
                    pkg = getattr(cls, "__module__", "unknown").split(".")[0]
                    report.findings.append(
                        AuditFinding(
                            category="adapter_coverage",
                            package=pkg,
                            plugin_class=cls.__name__,
                            severity="warning",
                            message="write_dispatch raises NotImplementedError",
                        )
                    )
            except (OSError, TypeError):
                pass


def check_registration_pattern(package: str, report: AuditReport) -> None:
    """Task 1.9 (partial) — verify __init__.py has a {Name}Plugin function."""
    if package in ("arrow", "filesystem"):
        return  # Built-ins registered via PluginRegistry.register_builtins
    init_file = _src_dir() / package / "__init__.py"
    if not init_file.exists():
        report.findings.append(
            AuditFinding(
                category="registration",
                package=package,
                plugin_class="__init__.py",
                severity="error",
                message="Missing __init__.py",
            )
        )
        return
    suffix = package.removeprefix("rivet_")
    # Known naming overrides — the convention isn't a simple capitalize
    _NAME_OVERRIDES: dict[str, str] = {
        "duckdb": "DuckDB",
        "postgres": "Postgres",
        "databricks": "Databricks",
        "polars": "Polars",
        "pyspark": "PySpark",
        "aws": "AWS",
        "rest": "Rest",
    }
    pascal = _NAME_OVERRIDES.get(suffix, "".join(p.capitalize() for p in suffix.split("_")))
    expected_fn = f"{pascal}Plugin"
    try:
        text = init_file.read_text()
    except Exception:
        return
    if f"def {expected_fn}" not in text:
        report.findings.append(
            AuditFinding(
                category="registration",
                package=package,
                plugin_class="__init__.py",
                severity="warning",
                message=f"Registration function '{expected_fn}' not found",
            )
        )


def check_source_pushdown(
    package: str,
    source_classes: list[type],
    report: AuditReport,
) -> None:
    """Task 1.9 (partial) — verify SourcePlugin.read accepts a pushdown parameter."""
    for cls in source_classes:
        read_fn = getattr(cls, "read", None)
        if read_fn is None:
            continue
        try:
            sig = inspect.signature(read_fn)
        except (ValueError, TypeError):
            continue
        if "pushdown" not in sig.parameters:
            report.findings.append(
                AuditFinding(
                    category="source_pushdown",
                    package=package,
                    plugin_class=cls.__name__,
                    severity="error",
                    message="read() method missing 'pushdown' parameter",
                )
            )


def check_materialized_ref(
    package: str,
    modules: list[Any],
    report: AuditReport,
) -> None:
    """Task 1.9 (partial) — verify MaterializedRef subclasses implement required methods."""
    from rivet_core.strategies import MaterializedRef

    for mod in modules:
        for _name, cls in inspect.getmembers(mod, inspect.isclass):
            if not issubclass(cls, MaterializedRef) or cls is MaterializedRef:
                continue
            obj_mod = getattr(cls, "__module__", "") or ""
            if package not in ("arrow", "filesystem") and not obj_mod.startswith(package):
                continue
            for method in _MATERIALIZED_REF_METHODS:
                if getattr(cls, method, None) is None:
                    report.findings.append(
                        AuditFinding(
                            category="materialized_ref",
                            package=package,
                            plugin_class=cls.__name__,
                            severity="error",
                            message=f"Missing {method} implementation",
                        )
                    )


def check_documentation(
    package: str,
    modules: list[Any],
    plugin_classes: list[type],
    report: AuditReport,
) -> None:
    """Task 1.9 (partial) — check README.md, module docstrings, public method docstrings."""
    src = _src_dir()
    if package in ("arrow", "filesystem"):
        pkg_dir = src / "rivet_core" / "builtins"
    else:
        pkg_dir = src / package

    if not (pkg_dir / "README.md").exists() and package not in ("arrow", "filesystem"):
        report.findings.append(
            AuditFinding(
                category="documentation",
                package=package,
                plugin_class="(package)",
                severity="warning",
                message="Missing README.md",
            )
        )

    for mod in modules:
        if not mod.__doc__:
            report.findings.append(
                AuditFinding(
                    category="documentation",
                    package=package,
                    plugin_class=mod.__name__,
                    severity="warning",
                    message="Missing module-level docstring",
                )
            )

    for cls in plugin_classes:
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if name.startswith("_"):
                continue
            if not method.__doc__:
                report.findings.append(
                    AuditFinding(
                        category="documentation",
                        package=package,
                        plugin_class=f"{cls.__name__}.{name}",
                        severity="warning",
                        message="Missing docstring on public method",
                    )
                )


def check_source_sink_catalog_alignment(
    all_sources: dict[str, list[type]],
    all_sinks: dict[str, list[type]],
    all_catalogs: dict[str, list[type]],
    report: AuditReport,
) -> None:
    """Task 1.9 (partial) — verify source/sink catalog_type matches a registered catalog."""
    catalog_types: set[str] = set()
    for _pkg, cats in all_catalogs.items():
        for cls in cats:
            ct = getattr(cls, "type", None)
            if ct:
                catalog_types.add(ct)

    for pkg, classes in list(all_sources.items()) + list(all_sinks.items()):
        for cls in classes:
            ct = getattr(cls, "catalog_type", None)
            if ct and ct not in catalog_types:
                report.findings.append(
                    AuditFinding(
                        category="catalog_alignment",
                        package=pkg,
                        plugin_class=cls.__name__,
                        severity="error",
                        message=f"catalog_type '{ct}' has no matching CatalogPlugin",
                    )
                )


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def _print_report(report: AuditReport) -> None:
    """Print a structured text report to stdout."""
    print("Plugin Coherence Audit Report")
    print("=" * 50)
    print()

    # 1. Capability Matrix
    print("1. Capability Matrix")
    for pkg, caps in sorted(report.capability_matrix.items()):
        parts: list[str] = []
        expected = EXPECTED_CAPABILITIES.get(pkg, {})
        for pt in _BASE_CLASS_NAMES:
            if not expected.get(pt, False) and not caps.get(pt, False):
                continue
            mark = "✓" if caps.get(pt, False) else "✗"
            suffix = ""
            if not expected.get(pt, False):
                suffix = " (not expected)"
            elif not caps.get(pt, False):
                suffix = " (MISSING)"
            parts.append(f"{pt} {mark}{suffix}")
        print(f"   {pkg:25s} {', '.join(parts)}")
    print()

    # 2. Skipped packages
    if report.skipped_packages:
        print("2. Skipped Packages (import failed)")
        for pkg in report.skipped_packages:
            print(f"   {pkg}")
        print()

    # 3. Sink strategies
    if report.sink_strategies:
        print("3. Sink Write Strategies")
        for cat_type, strategies in sorted(report.sink_strategies.items()):
            print(f"   {cat_type:25s} {', '.join(strategies)}")
        print()

    # 4. Adapter coverage matrix
    if report.adapter_matrix:
        print("4. Adapter Coverage Matrix")
        etypes = sorted({e for e, _c in report.adapter_matrix})
        ctypes = sorted({c for _e, c in report.adapter_matrix})
        header = f"   {'engine':<20s}" + "".join(f"{c:<15s}" for c in ctypes)
        print(header)
        for et in etypes:
            row = f"   {et:<20s}"
            for ct in ctypes:
                has = report.adapter_matrix.get((et, ct), False)
                row += f"{'✓':<15s}" if has else f"{'✗':<15s}"
            print(row)
        print()

    # 5. Findings by category
    if report.findings:
        print("5. Findings")
        cats = sorted(set(f.category for f in report.findings))
        for cat in cats:
            cat_findings = [f for f in report.findings if f.category == cat]
            errors = [f for f in cat_findings if f.severity == "error"]
            warnings = [f for f in cat_findings if f.severity == "warning"]
            print(f"   [{cat}] {len(errors)} error(s), {len(warnings)} warning(s)")
            for f in cat_findings:
                sev = "ERROR" if f.severity == "error" else "WARN "
                print(f"     {sev}  {f.package}/{f.plugin_class}: {f.message}")
        print()

    # Summary
    print(
        f"Summary: {report.error_count} error(s), {report.warning_count} warning(s) "
        f"across {len(report.findings)} finding(s)."
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Orchestrate all coherence checks and print the report."""
    report = AuditReport()

    try:
        base_classes = _get_base_classes()
    except ImportError as exc:
        print(f"FATAL: Cannot import rivet_core base classes: {exc}", file=sys.stderr)
        return 1

    # Collect classes per package
    all_catalogs: dict[str, list[type]] = {}
    all_engines: dict[str, list[type]] = {}
    all_adapters: dict[str, list[type]] = {}
    all_sources: dict[str, list[type]] = {}
    all_sinks: dict[str, list[type]] = {}

    for package in EXPECTED_CAPABILITIES:
        # Import modules
        if package in ("arrow", "filesystem"):
            modules = _scan_builtin_modules(package)
        else:
            pkg_mod = _import_package(package)
            if pkg_mod is None:
                report.skipped_packages.append(package)
                print(f"  SKIP  {package} (import failed)", file=sys.stderr)
                continue
            modules = _scan_package_modules(package)

        if not modules:
            report.skipped_packages.append(package)
            print(f"  SKIP  {package} (no modules found)", file=sys.stderr)
            continue

        # 1.2 Capability matrix scan
        actual = check_capability_matrix(package, modules, base_classes, report)

        all_catalogs[package] = actual["catalog"]
        all_engines[package] = actual["engine"]
        all_adapters[package] = actual["adapter"]
        all_sources[package] = actual["source"]
        all_sinks[package] = actual["sink"]

        # 1.3 Catalog introspection
        if actual["catalog"]:
            check_catalog_introspection(package, actual["catalog"], base_classes, report)

        # 1.4 Engine optional methods
        if actual["engine"]:
            check_engine_optional_methods(package, actual["engine"], base_classes, report)

        # 1.5 Sink strategies
        if actual["sink"]:
            check_sink_strategies(package, actual["sink"], report)

        # 1.6 Error handling
        check_error_handling(package, report)

        # 1.7 Validation patterns
        check_validation_patterns(package, actual["catalog"], actual["engine"], report)

        # 1.9 Registration, source pushdown, MaterializedRef, documentation
        check_registration_pattern(package, report)

        if actual["source"]:
            check_source_pushdown(package, actual["source"], report)

        check_materialized_ref(package, modules, report)
        check_documentation(
            package,
            modules,
            actual["catalog"] + actual["engine"] + actual["source"] + actual["sink"],
            report,
        )

    # 1.8 Adapter coverage (cross-package)
    check_adapter_coverage(all_engines, all_adapters, report)

    # 1.9 Source/sink catalog alignment (cross-package)
    check_source_sink_catalog_alignment(all_sources, all_sinks, all_catalogs, report)

    _print_report(report)

    # Exit 0 if no errors (warnings are acceptable)
    return 1 if report.error_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
