"""Test command: run tests against fixtures."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import pyarrow
import pyarrow.parquet as pq
import yaml

from rivet_cli.app import GlobalOptions
from rivet_cli.errors import CLIError, format_cli_error
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, TEST_FAILURE
from rivet_cli.rendering.test_text import render_test_results, render_test_results_json
from rivet_core.assembly import Assembly
from rivet_core.errors import RivetError
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry
from rivet_core.testing.comparison import compare_tables
from rivet_core.testing.fixtures import load_fixture
from rivet_core.testing.models import ComparisonResult, TestDef, TestResult

# ---------------------------------------------------------------------------
# New test discovery (task 7.1)
# ---------------------------------------------------------------------------


class TestDiscoveryError(Exception):
    """Raised when test discovery fails (RVT-9xx)."""

    def __init__(self, error: RivetError) -> None:
        self.error = error
        super().__init__(str(error))


def _find_test_yaml_files(tests_dir: Path, joints_dir: Path) -> list[Path]:
    """Find all *.test.yaml files in tests/ (recursive) and joints/."""
    files: list[Path] = []
    if tests_dir.is_dir():
        files.extend(sorted(tests_dir.rglob("*.test.yaml")))
    if joints_dir.is_dir():
        files.extend(sorted(joints_dir.glob("*.test.yaml")))
    return files


def _parse_yaml_to_testdefs(path: Path) -> list[TestDef]:
    """Parse a YAML file (possibly multi-doc) into TestDef instances."""
    text = path.read_text()
    defs: list[TestDef] = []
    for doc in yaml.safe_load_all(text):
        if not isinstance(doc, dict):
            continue
        name = doc.get("name")
        target = doc.get("target")
        targets = doc.get("targets")
        if not name:
            continue
        if not target and not targets:
            continue
        if not target and targets:
            target = next(iter(targets))
        defs.append(TestDef(
            name=name,
            target=target,  # type: ignore[arg-type]
            targets=targets,
            scope=doc.get("scope", "joint"),
            inputs=doc.get("inputs", {}),
            expected=doc.get("expected"),
            compare=doc.get("compare", "exact"),
            compare_function=doc.get("compare_function"),
            tags=doc.get("tags", []),
            description=doc.get("description"),
            options=doc.get("options", {}),
            extends=doc.get("extends"),
            source_file=path,
            engine=doc.get("engine"),
        ))
    return defs


def discover_tests(
    project_root: Path,
    tests_dir: Path,
    joints_dir: Path,
) -> list[TestDef]:
    """Discover all *.test.yaml files, parse YAML streams, validate uniqueness.

    Scans tests/ recursively and joints/ for *.test.yaml files.
    Raises TestDiscoveryError(RVT-908) on duplicate test names.
    """
    files = _find_test_yaml_files(tests_dir, joints_dir)
    all_tests: list[TestDef] = []
    seen_names: dict[str, Path] = {}
    for f in files:
        for td in _parse_yaml_to_testdefs(f):
            if td.name in seen_names:
                raise TestDiscoveryError(RivetError(
                    code="RVT-908",
                    message=f"Duplicate test name '{td.name}'",
                    context={
                        "name": td.name,
                        "first": str(seen_names[td.name]),
                        "second": str(f),
                    },
                    remediation="Rename one of the tests to have a unique name.",
                ))
            seen_names[td.name] = f
            all_tests.append(td)
    return all_tests


# ---------------------------------------------------------------------------
# Task 7.2: Fixture inheritance resolution
# ---------------------------------------------------------------------------

_TESTDEF_DEFAULTS = {  # type: ignore[var-annotated]
    "scope": "joint",
    "compare": "exact",
    "tags": [],
    "description": None,
    "options": {},
}


def resolve_extends(tests: list[TestDef]) -> list[TestDef]:
    """Resolve fixture inheritance for tests that declare `extends`.

    For each test with `extends`, find the base test by name and merge:
    - inputs: merged by key (extending overrides base)
    - compare, tags, options, description: inherited from base if not redeclared
      (i.e., if the extending test's value equals the field default)
    - all other fields: extending test's values take precedence

    Raises TestDiscoveryError(RVT-909) if the base test is not found.
    """
    by_name = {t.name: t for t in tests}
    resolved: list[TestDef] = []
    for test in tests:
        if test.extends is None:
            resolved.append(test)
            continue
        base_name = test.extends
        if base_name not in by_name:
            raise TestDiscoveryError(RivetError(
                code="RVT-909",
                message=f"Base test '{base_name}' not found for extends in '{test.name}'",
                context={"test": test.name, "base": base_name},
                remediation=f"Ensure a test named '{base_name}' is defined before '{test.name}'.",
            ))
        base = by_name[base_name]
        # Merge inputs: base first, then extending overrides
        merged_inputs = {**base.inputs, **test.inputs}
        # Inherit compare/tags/options/description from base if not redeclared
        compare = base.compare if test.compare == _TESTDEF_DEFAULTS["compare"] else test.compare
        tags = base.tags if test.tags == _TESTDEF_DEFAULTS["tags"] else test.tags
        options = base.options if test.options == _TESTDEF_DEFAULTS["options"] else test.options
        description = base.description if test.description == _TESTDEF_DEFAULTS["description"] else test.description
        resolved.append(TestDef(
            name=test.name,
            target=test.target,
            targets=test.targets,
            scope=test.scope if test.scope != _TESTDEF_DEFAULTS["scope"] else base.scope,
            inputs=merged_inputs,
            expected=test.expected if test.expected is not None else base.expected,
            compare=compare,
            compare_function=test.compare_function if test.compare_function is not None else base.compare_function,
            tags=tags,
            description=description,
            options=options,
            extends=test.extends,
            source_file=test.source_file,
            engine=test.engine if test.engine is not None else base.engine,
        ))
    return resolved


# ---------------------------------------------------------------------------
# Task 7.3: Test filtering
# ---------------------------------------------------------------------------


def filter_tests(
    tests: list[TestDef],
    tags: list[str],
    target: str | None,
    file_paths: list[Path],
    tag_all: bool = False,
) -> list[TestDef]:
    """Filter tests by tag, target joint, and/or source file paths.

    - tags: OR-mode by default; AND-mode when tag_all=True
    - target: match tests whose target equals the given joint name
    - file_paths: match tests whose source_file is in the given paths
    """
    result = tests
    if tags:
        if tag_all:
            result = [t for t in result if all(tag in t.tags for tag in tags)]
        else:
            result = [t for t in result if any(tag in t.tags for tag in tags)]
    if target is not None:
        result = [t for t in result if t.target == target]
    if file_paths:
        path_set = set(file_paths)
        result = [t for t in result if t.source_file in path_set]
    return result


# ---------------------------------------------------------------------------
# Task 8.1: Test isolation — build isolated assembly
# ---------------------------------------------------------------------------


def build_isolated_assembly(
    test_def: TestDef,
    fixtures: dict[str, pyarrow.Table],
    assembly: Assembly,
    registry: PluginRegistry,
) -> tuple[Assembly, list[Catalog], list[ComputeEngine]]:
    """Build an isolated assembly with in-memory Arrow catalogs.

    Replaces real catalogs with Arrow catalogs populated with fixture data.

    scope=joint: replace all upstreams of target with source joints backed by fixtures.
    scope=assembly: replace only leaf sources (joints with no upstream) with fixture data.

    Raises TestDiscoveryError(RVT-903) if joint-scope inputs don't cover all
    required upstreams.

    Returns (Assembly, catalogs, engines) ready for compile().
    """
    from rivet_core.builtins.arrow_catalog import _get_shared_store

    target = test_def.target
    if target not in assembly.joints:
        raise TestDiscoveryError(RivetError(
            code="RVT-907",
            message=f"Target joint '{target}' not found in assembly.",
            context={"target": target},
            remediation="Check the 'target' field in your test definition.",
        ))

    catalog_name = f"__test_{test_def.name}"

    if test_def.scope == "joint":
        # All direct upstreams of target must be provided as fixtures
        target_joint = assembly.joints[target]
        required_upstreams = set(target_joint.upstream)
        provided = set(fixtures.keys())
        missing = required_upstreams - provided
        if missing:
            raise TestDiscoveryError(RivetError(
                code="RVT-903",
                message=f"Missing fixture inputs for upstream joints: {sorted(missing)}",
                context={"target": target, "missing": sorted(missing), "provided": sorted(provided)},
                remediation="Add inputs for all upstream joints of the target.",
            ))

        # Build source joints for each fixture + keep the target joint
        joints: list[Joint] = []
        for name, table in fixtures.items():
            _get_shared_store()[(catalog_name, name)] = table
            joints.append(Joint(
                name=name,
                joint_type="source",
                catalog=catalog_name,
                path=name,
            ))

        # Re-create target joint pointing at the test catalog
        joints.append(Joint(
            name=target_joint.name,
            joint_type=target_joint.joint_type,
            catalog=catalog_name,
            upstream=target_joint.upstream,
            tags=target_joint.tags,
            description=target_joint.description,
            assertions=target_joint.assertions,
            path=target_joint.path,
            sql=target_joint.sql,
            engine=target_joint.engine,
            eager=target_joint.eager,
            table=target_joint.table,
            write_strategy=target_joint.write_strategy,
            function=target_joint.function,
            source_file=target_joint.source_file,
            dialect=target_joint.dialect,
        ))

    else:
        # scope=assembly: walk upstream closure from target, replace leaf sources
        upstream_closure = assembly._upstream_closure({target})
        # Identify leaf sources: joints in the closure with no upstream
        leaf_sources = {
            name for name in upstream_closure
            if not assembly.joints[name].upstream
        }

        # Register fixture data for leaf sources
        for name in leaf_sources:
            if name in fixtures:
                _get_shared_store()[(catalog_name, name)] = fixtures[name]

        # Rebuild all joints in the upstream closure
        topo = assembly.topological_order()
        joints = []
        for jname in topo:
            if jname not in upstream_closure:
                continue
            orig = assembly.joints[jname]
            if jname in leaf_sources:
                # Replace with source joint backed by fixture
                joints.append(Joint(
                    name=orig.name,
                    joint_type="source",
                    catalog=catalog_name,
                    path=orig.name,
                ))
            else:
                # Keep joint but point catalog to test catalog
                joints.append(Joint(
                    name=orig.name,
                    joint_type=orig.joint_type,
                    catalog=catalog_name,
                    upstream=orig.upstream,
                    tags=orig.tags,
                    description=orig.description,
                    assertions=orig.assertions,
                    path=orig.path,
                    sql=orig.sql,
                    engine=orig.engine,
                    eager=orig.eager,
                    table=orig.table,
                    write_strategy=orig.write_strategy,
                    function=orig.function,
                    source_file=orig.source_file,
                    dialect=orig.dialect,
                ))

    isolated_assembly = Assembly(joints)
    catalog = Catalog(name=catalog_name, type="arrow", options={})
    engine = ComputeEngine(name="arrow", engine_type="arrow", config={})
    return isolated_assembly, [catalog], [engine]


# ---------------------------------------------------------------------------
# Task 8.2: Single test execution
# ---------------------------------------------------------------------------


def run_single_test(
    test_def: TestDef,
    config_result: Any,
    registry: PluginRegistry,
    update_snapshots: bool = False,
) -> TestResult:
    """Execute a single test: load fixtures, isolate, compile, execute, compare.

    Returns a TestResult with timing, comparison result, and check results.
    """
    from rivet_bridge import build_assembly
    from rivet_core import compile as rivet_compile
    from rivet_core.executor import Executor

    start = time.monotonic()

    try:
        bridge_result = build_assembly(config_result, registry)
        project_root = config_result.manifest.project_root if config_result.manifest else Path(".")

        # Load fixture data for inputs
        fixtures: dict[str, pyarrow.Table] = {}
        for name, spec in test_def.inputs.items():
            if isinstance(spec, dict):
                fixtures[name] = load_fixture(spec, project_root)

        # Build isolated assembly
        iso_asm, catalogs, engines = build_isolated_assembly(
            test_def, fixtures, bridge_result.assembly, registry,
        )

        # Compile (same pipeline as production)
        compiled = rivet_compile(iso_asm, catalogs, engines, registry)
        if not compiled.success:
            elapsed = (time.monotonic() - start) * 1000
            msgs = "; ".join(e.message for e in compiled.errors)
            return TestResult(name=test_def.name, passed=False, duration_ms=elapsed, error=f"Compilation failed: {msgs}")

        executor = Executor(registry)

        # Multi-target tests
        if test_def.targets:
            return _run_multi_target(test_def, executor, compiled, project_root, start, update_snapshots)

        # Single-target: get output table via run_query
        has_expected = test_def.expected is not None

        if has_expected:
            result_table = executor.run_query(compiled, test_def.target)

            # Snapshot update: write actual output to expected file path (file refs only)
            if update_snapshots and isinstance(test_def.expected, dict) and "file" in test_def.expected:
                snapshot_path = project_root / test_def.expected["file"]
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(result_table, str(snapshot_path))
                elapsed = (time.monotonic() - start) * 1000
                return TestResult(name=test_def.name, passed=True, duration_ms=elapsed)

            expected_table = load_fixture(test_def.expected, project_root)  # type: ignore[arg-type]
            comparison = compare_tables(
                result_table, expected_table,
                mode=test_def.compare,
                options=test_def.options,
                compare_function=test_def.compare_function,
            )
            elapsed = (time.monotonic() - start) * 1000
            return TestResult(
                name=test_def.name,
                passed=comparison.passed,
                duration_ms=elapsed,
                comparison_result=comparison,
            )

        # Assertion-only test (no expected): run full execution for check results
        exec_result = executor.run(compiled, fail_fast=False)
        check_results: list[Any] = []
        target_passed = True
        for jr in exec_result.joint_results:
            if jr.name == test_def.target:
                check_results = list(jr.check_results)
                target_passed = jr.success
                break

        elapsed = (time.monotonic() - start) * 1000
        return TestResult(
            name=test_def.name,
            passed=target_passed,
            duration_ms=elapsed,
            check_results=check_results,
        )

    except Exception as exc:
        elapsed = (time.monotonic() - start) * 1000
        return TestResult(
            name=test_def.name,
            passed=False,
            duration_ms=elapsed,
            error=f"[RVT-910] {exc}",
        )


def _run_multi_target(
    test_def: TestDef,
    executor: Any,
    compiled: Any,
    project_root: Path,
    start: float,
    update_snapshots: bool = False,
) -> TestResult:
    """Handle multi-target tests: compare each target independently."""
    all_passed = True
    all_comparisons: list[ComparisonResult] = []

    for target_name, target_spec in test_def.targets.items():  # type: ignore[union-attr]
        result_table = executor.run_query(compiled, target_name)
        expected_spec = target_spec.get("expected")
        if expected_spec is not None:
            # Snapshot update for file-based expected
            if update_snapshots and isinstance(expected_spec, dict) and "file" in expected_spec:
                snapshot_path = project_root / expected_spec["file"]
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(result_table, str(snapshot_path))
                continue
            expected_table = load_fixture(expected_spec, project_root)
            comparison = compare_tables(
                result_table, expected_table,
                mode=test_def.compare,
                options=test_def.options,
                compare_function=test_def.compare_function,
            )
            if not comparison.passed:
                all_passed = False
            all_comparisons.append(comparison)

    elapsed = (time.monotonic() - start) * 1000
    # Use first failure or first result as the combined comparison
    combined = next((c for c in all_comparisons if not c.passed), None)
    if combined is None and all_comparisons:
        combined = all_comparisons[0]

    return TestResult(
        name=test_def.name,
        passed=all_passed,
        duration_ms=elapsed,
        comparison_result=combined,
    )


# ---------------------------------------------------------------------------
# Task 10.1: CLI entry point
# ---------------------------------------------------------------------------


def run_test(
    tags: list[str],
    tag_all: bool,
    target: str | None,
    file_paths: list[Path],
    update_snapshots: bool,
    fail_fast: bool,
    format: str,
    globals: GlobalOptions,
) -> int:
    """Run tests against fixtures in isolation.

    Pipeline: discover → resolve extends → filter → execute → render.
    Returns exit code 0 (all pass) or 3 (failures).
    """
    from rivet_config import load_config

    config_result = load_config(globals.project_path, globals.profile)

    if not config_result.success:
        for err in config_result.errors:
            cli_err = CLIError(
                code="RVT-850",
                message=str(err.message),
                remediation=err.remediation,
            )
            print(format_cli_error(cli_err, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    assert config_result.manifest is not None
    manifest = config_result.manifest

    # Discover
    try:
        all_tests = discover_tests(manifest.project_root, manifest.tests_dir, manifest.joints_dir)
        all_tests = resolve_extends(all_tests)
    except TestDiscoveryError as exc:
        cli_err = CLIError(code=exc.error.code, message=exc.error.message, remediation=exc.error.remediation)  # type: ignore[arg-type]
        print(format_cli_error(cli_err, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    # Filter
    filtered = filter_tests(all_tests, tags, target, file_paths, tag_all=tag_all)

    if not filtered:
        print("No tests found.", file=sys.stderr)
        return SUCCESS

    # Build plugin registry
    registry = PluginRegistry()
    registry.register_builtins()
    from rivet_bridge import register_optional_plugins
    register_optional_plugins(registry)

    # Execute
    results: list[TestResult] = []
    for test_def in filtered:
        result = run_single_test(test_def, config_result, registry, update_snapshots)
        results.append(result)
        if fail_fast and not result.passed:
            break

    # Render
    if format == "json":
        print(render_test_results_json(results))
    else:
        print(render_test_results(results, globals.verbosity, globals.color))

    has_failures = any(not r.passed for r in results)
    return TEST_FAILURE if has_failures else SUCCESS
