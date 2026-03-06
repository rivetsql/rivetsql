"""Cross-reference declared dependencies against actual imports.

Scans all pyproject.toml files and verifies each declared dependency
has at least one corresponding import in the relevant source scope.
"""

import re
import sys
from pathlib import Path

# Map from PyPI package name to Python import name(s)
PACKAGE_TO_IMPORT = {
    "pyarrow": ["pyarrow"],
    "sqlglot": ["sqlglot"],
    "pyyaml": ["yaml"],
    "textual": ["textual"],
    "textual-textarea": ["textual_textarea"],
    "textual-fastdatatable": ["textual_fastdatatable"],
    "pytest": ["pytest"],
    "ruff": ["ruff"],
    "mypy": ["mypy"],
    "hypothesis": ["hypothesis"],
    "rivet-core": ["rivet_core"],
    "duckdb": ["duckdb"],
    "psycopg": ["psycopg"],
    "polars": ["polars"],
    "deltalake": ["deltalake"],
    "pyspark": ["pyspark"],
    "boto3": ["boto3", "botocore"],
    "requests": ["requests"],
    "hatchling": [],  # build-system only
}

IMPORT_RE = re.compile(r"^\s*(?:import|from)\s+([\w.]+)", re.MULTILINE)
# Match quoted dependency strings like "pkg>=1.0" or "pkg[extra]>=1.0"
DEP_STR_RE = re.compile(r'"([^"]+)"')
PKG_NAME_RE = re.compile(r'^([a-zA-Z0-9_-]+)')


def extract_imports(src_dir: Path) -> set[str]:
    """Extract all top-level import names from .py files under src_dir."""
    imports = set()
    for py_file in src_dir.rglob("*.py"):
        text = py_file.read_text(errors="ignore")
        for m in IMPORT_RE.finditer(text):
            imports.add(m.group(1).split(".")[0])
    return imports


def parse_dep_strings(text: str) -> list[str]:
    """Extract package names from a TOML array of dependency strings."""
    names = []
    for dep_str in DEP_STR_RE.findall(text):
        m = PKG_NAME_RE.match(dep_str)
        if m:
            names.append(m.group(1))
    return names


def find_toml_array(text: str, start: int) -> str:
    """Extract content of a TOML array starting at '[', handling nested brackets."""
    depth = 0
    i = start
    while i < len(text):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                return text[start + 1 : i]
        elif text[i] == '"':
            i += 1
            while i < len(text) and text[i] != '"':
                i += 1
        i += 1
    return text[start + 1 :]


def parse_deps_from_toml(text: str) -> dict[str, list[str]]:
    """Parse dependencies from pyproject.toml text."""
    result: dict[str, list[str]] = {}

    # Core dependencies
    dep_match = re.search(r'\[project\].*?(?:^|\n)dependencies\s*=\s*(\[)', text, re.DOTALL)
    if dep_match:
        array_content = find_toml_array(text, dep_match.start(1))
        result["dependencies"] = parse_dep_strings(array_content)

    # Optional dependencies sections
    opt_section = re.search(
        r'\[project\.optional-dependencies\]\s*\n(.*?)(?=\n\[(?!^\s)|\Z)', text, re.DOTALL
    )
    if opt_section:
        section_text = opt_section.group(1)
        for m in re.finditer(r'(\w+)\s*=\s*(\[)', section_text):
            section_name = m.group(1)
            abs_start = opt_section.start(1) + m.start(2)
            array_content = find_toml_array(text, abs_start)
            result[f"optional[{section_name}]"] = parse_dep_strings(array_content)

    return result


def main() -> int:
    rivet_root = Path(__file__).resolve().parent.parent
    src_root = rivet_root / "src"
    all_findings: list[str] = []

    # 1. Root pyproject.toml
    root_pyproject = rivet_root / "pyproject.toml"
    print(f"=== Auditing {root_pyproject.relative_to(rivet_root)} against src/ ===")
    deps = parse_deps_from_toml(root_pyproject.read_text())
    all_imports = extract_imports(src_root)

    for section, pkg_names in deps.items():
        for pkg in pkg_names:
            pkg_lower = pkg.lower()
            import_names = PACKAGE_TO_IMPORT.get(pkg_lower, [pkg_lower.replace("-", "_")])
            if not import_names:
                continue
            if section == "optional[cli]":
                scope_imports = extract_imports(src_root / "rivet_cli")
                scope = "rivet_cli"
            elif section == "optional[dev]":
                continue  # dev deps used in tests, not src
            else:
                scope_imports = all_imports
                scope = "all src"
            found = any(name in scope_imports for name in import_names)
            status = "OK" if found else "UNUSED"
            print(f"  [{status}] {section} :: {pkg} (imports: {import_names}, scope: {scope})")
            if status == "UNUSED":
                all_findings.append(
                    f"root :: {section} :: {pkg} (imports: {import_names}, scope: {scope})"
                )

    # 2. Per-plugin pyproject.toml files
    for plugin_dir in sorted(src_root.iterdir()):
        plugin_pyproject = plugin_dir / "pyproject.toml"
        if not plugin_pyproject.exists():
            continue
        label = plugin_dir.name
        print(f"\n=== Auditing {label}/pyproject.toml against {label}/ ===")
        deps = parse_deps_from_toml(plugin_pyproject.read_text())
        plugin_imports = extract_imports(plugin_dir)

        for section, pkg_names in deps.items():
            for pkg in pkg_names:
                pkg_lower = pkg.lower()
                import_names = PACKAGE_TO_IMPORT.get(
                    pkg_lower, [pkg_lower.replace("-", "_")]
                )
                if not import_names:
                    continue
                if pkg_lower == "rivet-core":
                    continue  # self-reference
                found = any(name in plugin_imports for name in import_names)
                status = "OK" if found else "UNUSED"
                print(f"  [{status}] {section} :: {pkg} (imports: {import_names})")
                if status == "UNUSED":
                    all_findings.append(f"{label} :: {section} :: {pkg}")

    # Summary
    print("\n" + "=" * 60)
    if all_findings:
        print(f"FINDINGS: {len(all_findings)} dependency(ies) with zero import references:")
        for f in all_findings:
            print(f"  - {f}")
        return 1
    else:
        print("All declared dependencies have matching imports. No issues found.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
