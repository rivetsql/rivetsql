"""Flag stale documentation references in docs/_build/.

Scans docs/_build/api/ for symbol references and verifies they exist in the
current source tree under rivet/src/. Outputs a report of stale references
that need documentation regeneration.
"""

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SRC_DIR = REPO_ROOT / "rivet" / "src"
DOCS_API_DIR = REPO_ROOT / "docs" / "_build" / "api"


def collect_source_symbols(src_dir: Path) -> set[str]:
    """Collect all defined symbols (classes, functions, methods, attributes) from source."""
    symbols: set[str] = set()
    for py_file in src_dir.rglob("*.py"):
        rel = py_file.relative_to(src_dir)
        parts = list(rel.with_suffix("").parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if not parts:
            continue
        module_path = ".".join(parts)
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        symbols.add(module_path)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                symbols.add(f"{module_path}.{node.name}")
                if isinstance(node, ast.ClassDef):
                    for item in node.body:
                        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            symbols.add(f"{module_path}.{node.name}.{item.name}")
    return symbols


def collect_doc_symbols(api_dir: Path) -> list[str]:
    """Extract symbol paths from docs/_build/api/ filenames."""
    symbols = []
    if not api_dir.is_dir():
        return symbols
    for md_file in sorted(api_dir.iterdir()):
        if md_file.suffix == ".md":
            symbols.append(md_file.stem)
    return symbols


def flag_stale_references() -> list[str]:
    """Return list of doc symbols not found in current source."""
    source_symbols = collect_source_symbols(SRC_DIR)
    doc_symbols = collect_doc_symbols(DOCS_API_DIR)
    stale = []
    for sym in doc_symbols:
        # Skip plugin.* docs — these reference abstract plugin interfaces, not concrete modules
        if sym.startswith("plugin."):
            continue
        if sym not in source_symbols:
            stale.append(sym)
    return stale


def main() -> int:
    stale = flag_stale_references()
    if not stale:
        print("No stale documentation references found in docs/_build/api/.")
        return 0
    print(f"⚠ Found {len(stale)} stale documentation reference(s) in docs/_build/api/.")
    print("These symbols are referenced in generated docs but no longer exist in source.")
    print("Action: Regenerate documentation to remove stale references.\n")
    for sym in stale:
        print(f"  - {sym}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
