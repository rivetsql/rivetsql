#!/usr/bin/env python3
"""Dead code analysis for Rivet codebase.

Identifies unreachable functions, classes, and module-level variables across
all modules under rivet/src/. Excludes decorated symbols (pytest fixtures,
click commands, etc.) and cross-references against __init__.py exports,
__all__ lists, dynamic references, same-file usage, and test usage.

Output: printed to stdout (pipe to a file if needed).
"""

import ast
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

EXEMPT_DECORATORS = frozenset({
    "pytest.fixture", "fixture",
    "click.command", "click.group", "click.option", "click.argument", "click.pass_context",
    "app.route",
    "property", "staticmethod", "classmethod",
    "abstractmethod",
    "overload",
    "dataclass",
    "cached_property",
})

DUNDER_RE = re.compile(r"^__\w+__$")


@dataclass
class SymbolDef:
    name: str
    kind: str  # "function", "class", "variable"
    file: str
    line: int
    decorators: list[str] = field(default_factory=list)


@dataclass
class DeadCodeCandidate:
    symbol: SymbolDef
    reason: str
    references_in_tests: list[str] = field(default_factory=list)
    in_all: bool = False
    in_init_import: bool = False
    risk: str = "LOW"


def get_decorator_names(node) -> list[str]:
    names = []
    for dec in node.decorator_list:
        if isinstance(dec, ast.Name):
            names.append(dec.id)
        elif isinstance(dec, ast.Attribute):
            parts = []
            obj = dec
            while isinstance(obj, ast.Attribute):
                parts.append(obj.attr)
                obj = obj.value
            if isinstance(obj, ast.Name):
                parts.append(obj.id)
            names.append(".".join(reversed(parts)))
        elif isinstance(dec, ast.Call):
            func = dec.func
            if isinstance(func, ast.Name):
                names.append(func.id)
            elif isinstance(func, ast.Attribute):
                parts = []
                obj = func
                while isinstance(obj, ast.Attribute):
                    parts.append(obj.attr)
                    obj = obj.value
                if isinstance(obj, ast.Name):
                    parts.append(obj.id)
                names.append(".".join(reversed(parts)))
    return names


def is_exempt(decorators: list[str]) -> bool:
    for dec in decorators:
        if dec in EXEMPT_DECORATORS:
            return True
        for exempt in EXEMPT_DECORATORS:
            if dec.endswith(exempt) or exempt.endswith(dec):
                return True
    return False


def extract_symbols(filepath: str) -> list[SymbolDef]:
    """Extract top-level functions, classes, and module-level variable assignments."""
    try:
        with open(filepath) as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
    except (SyntaxError, UnicodeDecodeError):
        return []

    symbols = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            decorators = get_decorator_names(node)
            symbols.append(SymbolDef(
                name=node.name, kind="function", file=filepath,
                line=node.lineno, decorators=decorators,
            ))
        elif isinstance(node, ast.ClassDef):
            decorators = get_decorator_names(node)
            symbols.append(SymbolDef(
                name=node.name, kind="class", file=filepath,
                line=node.lineno, decorators=decorators,
            ))
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    name = target.id
                    if DUNDER_RE.match(name) or name == "TYPE_CHECKING":
                        continue
                    symbols.append(SymbolDef(
                        name=name, kind="variable", file=filepath, line=node.lineno,
                    ))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            name = node.target.id
            if not DUNDER_RE.match(name) and name != "TYPE_CHECKING":
                symbols.append(SymbolDef(
                    name=name, kind="variable", file=filepath, line=node.lineno,
                ))
    return symbols


def extract_all_exports(filepath: str) -> set[str]:
    try:
        with open(filepath) as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
    except (SyntaxError, UnicodeDecodeError):
        return set()

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        return {
                            elt.value for elt in node.value.elts
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                        }
    return set()


def extract_init_imports(filepath: str) -> set[str]:
    try:
        with open(filepath) as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
    except (SyntaxError, UnicodeDecodeError):
        return set()

    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.names:
                for alias in node.names:
                    names.add(alias.asname if alias.asname else alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname if alias.asname else alias.name)
    return names


def count_name_references_in_file(filepath: str, name: str, kind: str) -> int:
    """Count how many times `name` is referenced in the file, excluding its definition.

    For functions/classes, `def`/`class` statements don't create ast.Name nodes,
    so all ast.Name occurrences are references.
    For variables, the assignment target creates one ast.Name node, so subtract 1.
    """
    try:
        with open(filepath) as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
    except (SyntaxError, UnicodeDecodeError):
        return 0

    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == name or isinstance(node, ast.Attribute) and node.attr == name:
            count += 1

    # For variables, subtract 1 for the assignment target
    if kind == "variable":
        count = max(0, count - 1)

    return count


def find_references_in_other_files(name: str, all_files: list[str], defining_file: str) -> tuple[list[str], list[str]]:
    """Find references in other source and test files. Returns (source_refs, test_refs)."""
    source_refs = []
    test_refs = []
    pattern = re.compile(r'\b' + re.escape(name) + r'\b')

    for fpath in all_files:
        if fpath == defining_file:
            continue
        try:
            with open(fpath) as f:
                content = f.read()
        except (OSError, UnicodeDecodeError):
            continue

        if pattern.search(content):
            if "/tests/" in fpath or fpath.endswith("_test.py"):
                test_refs.append(fpath)
            else:
                source_refs.append(fpath)

    return source_refs, test_refs


def has_dynamic_reference(name: str, all_files: list[str]) -> bool:
    """Check if name is referenced dynamically (getattr, string lookups)."""
    pattern = re.compile(
        r'getattr\s*\([^)]*["\']' + re.escape(name) + r'["\']'
        r'|registry.*["\']' + re.escape(name) + r'["\']'
    )
    for fpath in all_files:
        try:
            with open(fpath) as f:
                content = f.read()
        except (OSError, UnicodeDecodeError):
            continue
        if pattern.search(content):
            return True
    return False


def main():
    repo_root = Path(__file__).resolve().parent.parent.parent
    src_dir = repo_root / "rivet" / "src"
    tests_dir = repo_root / "rivet" / "tests"

    if not src_dir.exists():
        print(f"ERROR: {src_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    src_files = sorted(str(p) for p in src_dir.rglob("*.py"))
    test_files = sorted(str(p) for p in tests_dir.rglob("*.py")) if tests_dir.exists() else []
    all_files = src_files + test_files

    # Build __all__ and __init__ import maps per module
    all_exports: dict[str, set[str]] = {}
    init_imports: dict[str, set[str]] = {}

    for fpath in src_files:
        if os.path.basename(fpath) == "__init__.py":
            module_dir = os.path.dirname(fpath)
            all_exports[module_dir] = extract_all_exports(fpath)
            init_imports[module_dir] = extract_init_imports(fpath)

    # Extract all symbols from non-__init__ source files
    all_symbols: list[SymbolDef] = []
    for fpath in src_files:
        if os.path.basename(fpath) == "__init__.py":
            continue
        all_symbols.extend(extract_symbols(fpath))

    print(f"Found {len(all_symbols)} symbols across {len(src_files)} source files")

    candidates: list[DeadCodeCandidate] = []
    for sym in all_symbols:
        if DUNDER_RE.match(sym.name):
            continue
        if is_exempt(sym.decorators):
            continue

        # Check same-file usage (AST-based)
        same_file_refs = count_name_references_in_file(sym.file, sym.name, sym.kind)
        if same_file_refs > 0:
            # Used within the same file — not dead at file level
            # But we still need to check if the file-level entry point is reachable
            # For now, skip — internal helpers called within the same file are alive
            continue

        # Check if in __all__ or __init__ imports of its module
        in_all = False
        in_init = False
        check_dir = os.path.dirname(sym.file)
        while check_dir and str(check_dir).startswith(str(src_dir)):
            if check_dir in all_exports and sym.name in all_exports[check_dir]:
                in_all = True
            if check_dir in init_imports and sym.name in init_imports[check_dir]:
                in_init = True
            parent = os.path.dirname(check_dir)
            if parent == check_dir:
                break
            check_dir = parent

        if in_all or in_init:
            continue

        # Check references in other files
        source_refs, test_refs = find_references_in_other_files(sym.name, all_files, sym.file)

        if source_refs:
            continue

        # Check dynamic references
        if has_dynamic_reference(sym.name, all_files):
            continue

        reason_parts = ["No source references found outside defining file"]
        risk = "LOW"

        if test_refs:
            reason_parts.append(f"referenced in {len(test_refs)} test file(s) only")
            risk = "MEDIUM"

        candidates.append(DeadCodeCandidate(
            symbol=sym,
            reason="; ".join(reason_parts),
            references_in_tests=[os.path.relpath(t, repo_root) for t in test_refs],
            in_all=in_all,
            in_init_import=in_init,
            risk=risk,
        ))

    candidates.sort(key=lambda c: (c.symbol.file, c.symbol.line))

    # Generate report
    report_path = repo_root / "dead_code_analysis.md"
    rel_src = str(src_dir.relative_to(repo_root))

    with open(report_path, "w") as f:
        f.write("# Dead Code Analysis Report\n\n")
        f.write(f"**Scope**: `{rel_src}/`\n\n")
        f.write(f"**Total symbols analyzed**: {len(all_symbols)}\n")
        f.write(f"**Dead code candidates found**: {len(candidates)}\n\n")

        if not candidates:
            f.write("No dead code candidates found.\n")
        else:
            high = [c for c in candidates if c.risk == "HIGH"]
            medium = [c for c in candidates if c.risk == "MEDIUM"]
            low = [c for c in candidates if c.risk == "LOW"]

            if high:
                f.write("## HIGH Risk (may affect public API or tests)\n\n")
                for c in high:
                    _write_candidate(f, c, repo_root)

            if medium:
                f.write("## MEDIUM Risk (test-only references)\n\n")
                for c in medium:
                    _write_candidate(f, c, repo_root)

            if low:
                f.write("## LOW Risk (no references anywhere)\n\n")
                for c in low:
                    _write_candidate(f, c, repo_root)

        f.write("\n---\n")
        f.write("*Generated by `rivet/scripts/find_dead_code.py`*\n")

    print(f"\nReport written to: {report_path}")
    print(f"  HIGH risk: {len([c for c in candidates if c.risk == 'HIGH'])}")
    print(f"  MEDIUM risk: {len([c for c in candidates if c.risk == 'MEDIUM'])}")
    print(f"  LOW risk: {len([c for c in candidates if c.risk == 'LOW'])}")

    if candidates:
        print("\n--- Dead Code Candidates ---\n")
        for c in candidates:
            rel_file = os.path.relpath(c.symbol.file, repo_root)
            test_note = f" [tests: {', '.join(c.references_in_tests)}]" if c.references_in_tests else ""
            print(f"  [{c.risk}] {c.symbol.kind} `{c.symbol.name}` in {rel_file}:{c.symbol.line}{test_note}")


def _write_candidate(f, c: DeadCodeCandidate, repo_root: Path):
    rel_file = os.path.relpath(c.symbol.file, repo_root)
    f.write(f"### `{c.symbol.name}` ({c.symbol.kind})\n")
    f.write(f"- **File**: `{rel_file}:{c.symbol.line}`\n")
    f.write(f"- **Reason**: {c.reason}\n")
    if c.symbol.decorators:
        f.write(f"- **Decorators**: {', '.join(c.symbol.decorators)}\n")
    if c.references_in_tests:
        f.write(f"- **Test references**: {', '.join(f'`{t}`' for t in c.references_in_tests)}\n")
    f.write(f"- **Risk**: {c.risk}\n\n")


if __name__ == "__main__":
    main()
