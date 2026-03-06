"""Doctor command: progressive health checks."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

from rivet_cli.app import GlobalOptions
from rivet_cli.errors import CLIError, format_cli_error
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS
from rivet_cli.rendering.doctor_text import DoctorCheckResult, render_doctor_text


def run_doctor(
    globals: GlobalOptions,
    check_connections: bool = False,
    check_schemas: bool = False,
) -> int:
    """Run progressive health checks on the project.

    Levels 1-6, stops at first level with errors. Warnings don't block.
    Returns 0 (warnings only) or 1 (errors found).
    """
    project = globals.project_path
    checks: list[DoctorCheckResult] = []

    # Verify project is initialized
    rivet_yaml = project / "rivet.yaml"
    if not rivet_yaml.exists():
        err = CLIError(
            code="RVT-850",
            message=f"rivet.yaml not found in {project}",
            remediation="Run 'rivet init' to create a new project, or use --project to specify the path.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    # Level 1: YAML syntax and required fields
    _check_level1(project, rivet_yaml, checks)
    if _has_errors(checks):
        print(render_doctor_text(checks, globals.color))
        return GENERAL_ERROR

    # Level 2: Profile completeness
    _check_level2(project, rivet_yaml, globals.profile, checks)
    if _has_errors(checks):
        print(render_doctor_text(checks, globals.color))
        return GENERAL_ERROR

    # Level 3: SQL syntax
    declarations = _discover_declarations(project, rivet_yaml)
    _check_level3(project, declarations, checks)
    if _has_errors(checks):
        print(render_doctor_text(checks, globals.color))
        return GENERAL_ERROR

    # Level 4: DAG health
    _check_level4(declarations, checks)
    if _has_errors(checks):
        print(render_doctor_text(checks, globals.color))
        return GENERAL_ERROR

    # Level 5: Unused declarations
    _check_level5(declarations, project, rivet_yaml, checks)
    if _has_errors(checks):
        print(render_doctor_text(checks, globals.color))
        return GENERAL_ERROR

    # Level 6: Best practices
    _check_level6(declarations, project, rivet_yaml, checks)

    # Optional deep checks
    if check_connections:
        _check_connections(project, rivet_yaml, globals.profile, checks)

    if check_schemas:
        _check_schemas(project, rivet_yaml, globals.profile, checks)

    print(render_doctor_text(checks, globals.color))
    return GENERAL_ERROR if _has_errors(checks) else SUCCESS


def _has_errors(checks: list[DoctorCheckResult]) -> bool:
    return any(c.status == "error" for c in checks)


def _safe_load_yaml(path: Path) -> tuple[object, str | None]:
    """Load YAML, returning (data, error_message)."""
    try:
        data = yaml.safe_load(path.read_text())
        return data, None
    except yaml.YAMLError as e:
        return None, str(e)
    except OSError as e:
        return None, str(e)


# --- Level 1: YAML syntax and required fields ---

def _check_level1(
    project: Path, rivet_yaml: Path, checks: list[DoctorCheckResult]
) -> None:
    data, err = _safe_load_yaml(rivet_yaml)
    if err:
        checks.append(DoctorCheckResult(
            level=1, name="YAML syntax", status="error",
            message=f"rivet.yaml: {err}",
        ))
        return

    if not isinstance(data, dict):
        checks.append(DoctorCheckResult(
            level=1, name="YAML syntax", status="error",
            message="rivet.yaml must be a YAML mapping.",
        ))
        return

    checks.append(DoctorCheckResult(
        level=1, name="YAML syntax", status="pass",
        message="rivet.yaml is valid YAML.",
    ))

    # Check required directory keys exist
    for key in ("sources", "joints", "sinks"):
        if key in data:
            dir_path = project / data[key]
            if dir_path.is_dir():
                checks.append(DoctorCheckResult(
                    level=1, name=f"Directory '{key}'", status="pass",
                    message=f"{dir_path} exists.",
                ))
            else:
                checks.append(DoctorCheckResult(
                    level=1, name=f"Directory '{key}'", status="error",
                    message=f"{dir_path} does not exist.",
                    details=f"Create the directory or update '{key}' in rivet.yaml.",
                ))

    # Check profiles file
    profiles_key = data.get("profiles")
    if profiles_key:
        profiles_path = project / profiles_key
        if profiles_path.exists():
            pdata, perr = _safe_load_yaml(profiles_path)
            if perr:
                checks.append(DoctorCheckResult(
                    level=1, name="Profiles YAML", status="error",
                    message=f"{profiles_path}: {perr}",
                ))
            else:
                checks.append(DoctorCheckResult(
                    level=1, name="Profiles YAML", status="pass",
                    message=f"{profiles_path} is valid YAML.",
                ))
        else:
            checks.append(DoctorCheckResult(
                level=1, name="Profiles YAML", status="error",
                message=f"Profiles file not found: {profiles_path}",
            ))

    # Validate YAML syntax of all declaration files
    for dir_key in ("sources", "joints", "sinks"):
        dir_val = data.get(dir_key)
        if not dir_val:
            continue
        dir_path = project / dir_val
        if not dir_path.is_dir():
            continue
        for f in sorted(dir_path.rglob("*.yaml")) + sorted(dir_path.rglob("*.yml")):
            fdata, ferr = _safe_load_yaml(f)
            if ferr:
                checks.append(DoctorCheckResult(
                    level=1, name="YAML syntax", status="error",
                    message=f"{f.relative_to(project)}: {ferr}",
                ))


# --- Level 2: Profile completeness ---

def _check_level2(
    project: Path, rivet_yaml: Path, profile_name: str, checks: list[DoctorCheckResult]
) -> None:
    data, _ = _safe_load_yaml(rivet_yaml)
    if not isinstance(data, dict):
        return

    profiles_key = data.get("profiles")
    if not profiles_key:
        checks.append(DoctorCheckResult(
            level=2, name=f"Profile '{profile_name}'", status="error",
            message="No 'profiles' key in rivet.yaml.",
        ))
        return

    profiles_path = project / profiles_key
    pdata, perr = _safe_load_yaml(profiles_path)
    if perr or not isinstance(pdata, dict):
        checks.append(DoctorCheckResult(
            level=2, name=f"Profile '{profile_name}'", status="error",
            message=f"Cannot read profiles: {perr or 'not a mapping'}",
        ))
        return

    if profile_name not in pdata:
        available = sorted(pdata.keys())
        checks.append(DoctorCheckResult(
            level=2, name=f"Profile '{profile_name}'", status="error",
            message=f"Profile '{profile_name}' not found.",
            details=f"Available profiles: {available}",
        ))
        return

    profile = pdata[profile_name]
    if not isinstance(profile, dict):
        checks.append(DoctorCheckResult(
            level=2, name=f"Profile '{profile_name}'", status="error",
            message=f"Profile '{profile_name}' must be a mapping.",
        ))
        return

    # Normalize engines: list-of-dicts (canonical) or dict-keyed-by-name
    raw_engines = profile.get("engines", [])
    if isinstance(raw_engines, dict):
        engine_names = set(raw_engines.keys())
        engine_count = len(raw_engines)
    elif isinstance(raw_engines, list):
        engine_names = {
            e["name"] for e in raw_engines if isinstance(e, dict) and "name" in e
        }
        engine_count = len(raw_engines)
    else:
        engine_names = set()
        engine_count = 0

    # Check default_engine
    if "default_engine" not in profile:
        checks.append(DoctorCheckResult(
            level=2, name="default_engine", status="error",
            message=f"Profile '{profile_name}' missing 'default_engine'.",
        ))
    else:
        engine_name = profile["default_engine"]
        if engine_name in engine_names:
            checks.append(DoctorCheckResult(
                level=2, name="default_engine", status="pass",
                message=f"default_engine '{engine_name}' exists.",
            ))
        else:
            checks.append(DoctorCheckResult(
                level=2, name="default_engine", status="error",
                message=f"default_engine '{engine_name}' not found in engines.",
            ))

    # Check catalogs and engines exist
    catalogs = profile.get("catalogs", {})
    if isinstance(catalogs, dict) and catalogs:
        checks.append(DoctorCheckResult(
            level=2, name="Catalogs", status="pass",
            message=f"{len(catalogs)} catalog(s) declared.",
        ))
    else:
        checks.append(DoctorCheckResult(
            level=2, name="Catalogs", status="warning",
            message="No catalogs declared in profile.",
        ))

    if engine_count > 0:
        checks.append(DoctorCheckResult(
            level=2, name="Engines", status="pass",
            message=f"{engine_count} engine(s) declared.",
        ))
    else:
        checks.append(DoctorCheckResult(
            level=2, name="Engines", status="warning",
            message="No engines declared in profile.",
        ))


# --- Level 3: SQL syntax ---

def _discover_declarations(
    project: Path, rivet_yaml: Path
) -> list[dict]:  # type: ignore[type-arg]
    """Discover all declaration files and return parsed info dicts."""
    data, _ = _safe_load_yaml(rivet_yaml)
    if not isinstance(data, dict):
        return []

    declarations: list[dict] = []  # type: ignore[type-arg]
    for dir_key in ("sources", "joints", "sinks"):
        dir_val = data.get(dir_key)
        if not dir_val:
            continue
        dir_path = project / dir_val
        if not dir_path.is_dir():
            continue

        for f in sorted(dir_path.rglob("*")):
            if not f.is_file():
                continue
            if f.suffix == ".sql":
                try:
                    sql_text = f.read_text()
                except OSError:
                    continue
                # Extract name and upstream from annotations
                name = _extract_sql_name(sql_text, f)
                upstream = _extract_sql_upstream(sql_text)
                declarations.append({
                    "name": name,
                    "type": "sql",
                    "file": f,
                    "sql": sql_text,
                    "dir_key": dir_key,
                    "upstream": upstream,
                })
            elif f.suffix in (".yaml", ".yml"):
                fdata, _ = _safe_load_yaml(f)
                if isinstance(fdata, dict) and "name" in fdata and "type" in fdata:
                    declarations.append({
                        "name": fdata["name"],
                        "type": fdata.get("type", "unknown"),
                        "file": f,
                        "yaml": fdata,
                        "dir_key": dir_key,
                        "upstream": fdata.get("upstream", []),
                        "catalog": fdata.get("catalog"),
                        "description": fdata.get("description"),
                        "tags": fdata.get("tags", []),
                        "quality": fdata.get("quality"),
                    })

    # Infer upstream from SQL body for declarations without explicit upstream
    all_names = {d["name"] for d in declarations}
    for decl in declarations:
        if decl.get("sql") and not decl.get("upstream"):
            decl["upstream"] = _infer_sql_table_refs(decl["sql"], all_names)

    return declarations


def _extract_sql_name(sql_text: str, file_path: Path) -> str:
    """Extract joint name from SQL annotation or fall back to filename."""
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") and "rivet:name:" in stripped:
            return stripped.split("rivet:name:", 1)[1].strip()
        if stripped.startswith("--") and "@joint:" in stripped:
            return stripped.split("@joint:", 1)[1].strip()
        if stripped.startswith("--") and "@name:" in stripped:
            return stripped.split("@name:", 1)[1].strip()
    return file_path.stem


def _extract_sql_upstream(sql_text: str) -> list[str]:
    """Extract upstream from ``-- rivet:upstream: [...]`` annotation."""
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") and "rivet:upstream:" in stripped:
            raw = stripped.split("rivet:upstream:", 1)[1].strip()
            # Parse bracket list: [a, b, c]
            raw = raw.strip("[] ")
            if not raw:
                return []
            return [t.strip() for t in raw.split(",") if t.strip()]
    return []


def _infer_sql_table_refs(sql_text: str, known_names: set[str]) -> list[str]:
    """Extract table names from FROM/JOIN clauses that match known declarations."""
    import re

    refs: set[str] = set()
    # Match FROM <name> and JOIN <name>, case-insensitive
    for m in re.finditer(r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", sql_text, re.IGNORECASE):
        table = m.group(1)
        if table.lower() in {n.lower() for n in known_names}:
            # Return the known name with original casing
            for n in known_names:
                if n.lower() == table.lower():
                    refs.add(n)
                    break
    return sorted(refs)
    return file_path.stem


def _check_level3(
    project: Path, declarations: list[dict], checks: list[DoctorCheckResult]  # type: ignore[type-arg]
) -> None:
    sql_files = [d for d in declarations if d.get("sql")]
    if not sql_files:
        checks.append(DoctorCheckResult(
            level=3, name="SQL parsing", status="pass",
            message="No SQL files to check.",
        ))
        return

    import sqlglot

    errors_found = False
    for decl in sql_files:
        sql = decl["sql"]
        # Strip annotation lines before parsing
        lines = sql.splitlines()
        body_lines = [l for l in lines if not l.strip().startswith("--")]
        body = "\n".join(body_lines).strip()
        if not body:
            continue
        try:
            result = sqlglot.parse(body)
            # Check for None results (empty parse)
            valid = [s for s in result if s is not None]
            if not valid:
                rel = decl["file"].relative_to(project)
                checks.append(DoctorCheckResult(
                    level=3, name="SQL parsing", status="error",
                    message=f"{rel}: empty SQL statement.",
                ))
                errors_found = True
        except sqlglot.errors.ParseError as e:
            rel = decl["file"].relative_to(project)
            checks.append(DoctorCheckResult(
                level=3, name="SQL parsing", status="error",
                message=f"{rel}: {e}",
            ))
            errors_found = True

    if not errors_found:
        checks.append(DoctorCheckResult(
            level=3, name="SQL parsing", status="pass",
            message=f"{len(sql_files)} SQL file(s) parsed successfully.",
        ))


# --- Level 4: DAG health ---

def _check_level4(declarations: list[dict], checks: list[DoctorCheckResult]) -> None:  # type: ignore[type-arg]
    # Build adjacency from declarations
    names = {d["name"] for d in declarations}
    edges: dict[str, list[str]] = {d["name"]: [] for d in declarations}
    {d["name"]: d["type"] for d in declarations}

    for decl in declarations:
        upstream = decl.get("upstream") or []
        if isinstance(upstream, list):
            for up in upstream:
                if up in edges:
                    edges[up].append(decl["name"])

    # Check for orphaned joints (referenced upstream that don't exist)
    for decl in declarations:
        upstream = decl.get("upstream") or []
        if isinstance(upstream, list):
            for up in upstream:
                if up not in names:
                    checks.append(DoctorCheckResult(
                        level=4, name="DAG references", status="error",
                        message=f"Joint '{decl['name']}' references unknown upstream '{up}'.",
                    ))

    # Check for cycles using DFS
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {n: WHITE for n in names}

    def dfs(node: str) -> list[str] | None:
        color[node] = GRAY
        for neighbor in edges.get(node, []):
            if neighbor not in color:
                continue
            if color[neighbor] == GRAY:
                return [neighbor, node]
            if color[neighbor] == WHITE:
                result = dfs(neighbor)
                if result is not None:
                    return result
        color[node] = BLACK
        return None

    for name in sorted(names):
        if color[name] == WHITE:
            cycle = dfs(name)
            if cycle is not None:
                cycle_str = " -> ".join(cycle)
                checks.append(DoctorCheckResult(
                    level=4, name="DAG cycles", status="error",
                    message=f"Cycle detected: {cycle_str}",
                ))

    # Check for unreachable sinks (sinks with no upstream)
    sinks = [d for d in declarations if d["type"] == "sink"]
    for s in sinks:
        upstream = s.get("upstream") or []
        if not upstream:
            checks.append(DoctorCheckResult(
                level=4, name="DAG health", status="warning",
                message=f"Sink '{s['name']}' has no upstream joints.",
            ))

    if not any(c.level == 4 for c in checks):
        checks.append(DoctorCheckResult(
            level=4, name="DAG health", status="pass",
            message="No cycles, orphans, or unreachable sinks.",
        ))


# --- Level 5: Unused declarations ---

def _check_level5(
    declarations: list[dict], project: Path, rivet_yaml: Path,  # type: ignore[type-arg]
    checks: list[DoctorCheckResult],
) -> None:
    names = {d["name"] for d in declarations}
    # Collect all referenced upstreams
    referenced: set[str] = set()
    for decl in declarations:
        upstream = decl.get("upstream") or []
        if isinstance(upstream, list):
            referenced.update(upstream)

    # Sources not consumed by any joint
    sources = [d for d in declarations if d["type"] == "source"]
    for s in sources:
        if s["name"] not in referenced:
            checks.append(DoctorCheckResult(
                level=5, name="Unused source", status="warning",
                message=f"Source '{s['name']}' is not consumed by any joint.",
            ))

    # Quality checks on nonexistent joints
    data, _ = _safe_load_yaml(rivet_yaml)
    if isinstance(data, dict):
        quality_dir = data.get("quality")
        if quality_dir:
            qpath = project / quality_dir
            if qpath.is_dir():
                for f in sorted(qpath.rglob("*.yaml")) + sorted(qpath.rglob("*.yml")):
                    qdata, _ = _safe_load_yaml(f)
                    if isinstance(qdata, dict):
                        target = qdata.get("joint", f.stem)
                        if target not in names:
                            checks.append(DoctorCheckResult(
                                level=5, name="Quality target", status="warning",
                                message=f"Quality file '{f.name}' targets nonexistent joint '{target}'.",
                            ))

    if not any(c.level == 5 for c in checks):
        checks.append(DoctorCheckResult(
            level=5, name="Unused declarations", status="pass",
            message="No unused sources or orphaned quality checks.",
        ))


# --- Level 6: Best practices ---

def _check_level6(
    declarations: list[dict], project: Path, rivet_yaml: Path,  # type: ignore[type-arg]
    checks: list[DoctorCheckResult],
) -> None:
    {d["name"] for d in declarations}

    # Sinks without quality checks
    sinks = [d for d in declarations if d["type"] == "sink"]
    data, _ = _safe_load_yaml(rivet_yaml)
    quality_targets: set[str] = set()
    if isinstance(data, dict):
        quality_dir = data.get("quality")
        if quality_dir:
            qpath = project / quality_dir
            if qpath.is_dir():
                for f in sorted(qpath.rglob("*.yaml")) + sorted(qpath.rglob("*.yml")):
                    qdata, _ = _safe_load_yaml(f)
                    if isinstance(qdata, dict):
                        quality_targets.add(qdata.get("joint", f.stem))

    # Also check inline quality
    for decl in declarations:
        if decl.get("quality"):
            quality_targets.add(decl["name"])

    for s in sinks:
        if s["name"] not in quality_targets:
            checks.append(DoctorCheckResult(
                level=6, name="Best practice", status="warning",
                message=f"Sink '{s['name']}' has no quality checks.",
            ))

    # Joints without descriptions
    for decl in declarations:
        if not decl.get("description") and decl["type"] in ("sql", "sink"):
            checks.append(DoctorCheckResult(
                level=6, name="Best practice", status="warning",
                message=f"Joint '{decl['name']}' has no description.",
            ))

    # Check for tests directory
    tests_dir = None
    if isinstance(data, dict):
        tests_val = data.get("tests")
        if tests_val:
            tests_dir = project / tests_val

    joints_with_tests: set[str] = set()
    if tests_dir and tests_dir.is_dir():
        for f in sorted(tests_dir.rglob("*.yaml")) + sorted(tests_dir.rglob("*.yml")):
            tdata, _ = _safe_load_yaml(f)
            if isinstance(tdata, dict) and "joint" in tdata:
                joints_with_tests.add(tdata["joint"])

    for decl in declarations:
        if decl["type"] in ("sql", "sink") and decl["name"] not in joints_with_tests:
            checks.append(DoctorCheckResult(
                level=6, name="Best practice", status="warning",
                message=f"Joint '{decl['name']}' has no tests.",
            ))

    if not any(c.level == 6 for c in checks):
        checks.append(DoctorCheckResult(
            level=6, name="Best practices", status="pass",
            message="All best practices satisfied.",
        ))


# --- Optional: Connection checks ---

def _check_connections(
    project: Path, rivet_yaml: Path, profile_name: str,
    checks: list[DoctorCheckResult],
) -> None:
    """Test catalog connectivity (opt-in)."""
    data, _ = _safe_load_yaml(rivet_yaml)
    if not isinstance(data, dict):
        return

    profiles_key = data.get("profiles")
    if not profiles_key:
        return

    profiles_path = project / profiles_key
    pdata, _ = _safe_load_yaml(profiles_path)
    if not isinstance(pdata, dict) or profile_name not in pdata:
        return

    profile = pdata[profile_name]
    if not isinstance(profile, dict):
        return

    catalogs = profile.get("catalogs", {})
    if not isinstance(catalogs, dict):
        return

    for name, config in catalogs.items():
        # We can only report that connectivity checking is not implemented
        # for catalog types that don't have a built-in connection test
        checks.append(DoctorCheckResult(
            level=7, name=f"Connection '{name}'", status="warning",
            message=f"Connection check for catalog '{name}' (type: {config.get('type', 'unknown')}) not available.",
            details="Install a catalog plugin that supports connectivity testing.",
        ))


# --- Optional: Schema drift checks ---

def _check_schemas(
    project: Path, rivet_yaml: Path, profile_name: str,
    checks: list[DoctorCheckResult],
) -> None:
    """Check schema drift (opt-in)."""
    checks.append(DoctorCheckResult(
        level=7, name="Schema drift", status="warning",
        message="Schema drift checking requires catalog connectivity.",
        details="Use --check-connections with a catalog plugin that supports schema introspection.",
    ))
