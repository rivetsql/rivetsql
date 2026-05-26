#!/usr/bin/env python3
"""Scan all `from rivet_*` / `import rivet_*` imports under rivet/src/ and verify
boundary compliance.  Also checks for vendor SDK usage in modules where it
doesn't belong (e.g. AWS/boto3 calls in rivet_core).

Exit code 0 means no violations; exit code 1 means violations found.

Boundary rules (from rivet steering doc):
  - rivet_core: only stdlib + pyarrow + sqlglot (no rivet_* imports)
  - rivet_config: only rivet_core
  - rivet_bridge: only rivet_core + rivet_config
  - rivet_cli: only rivet_core + rivet_config + rivet_bridge
  - Plugins: only rivet_core
"""

from __future__ import annotations

import io
import re
import sys
import tokenize
from dataclasses import dataclass
from pathlib import Path

ALLOWED_RIVET_IMPORTS: dict[str, set[str]] = {
    "rivet_core": set(),
    "rivet_config": {"rivet_core"},
    "rivet_bridge": {"rivet_core", "rivet_config"},
    "rivet_cli": {"rivet_core", "rivet_config", "rivet_bridge"},
    # Plugins — only rivet_core
    "rivet_duckdb": {"rivet_core"},
    "rivet_postgres": {"rivet_core"},
    "rivet_polars": {"rivet_core"},
    "rivet_pyspark": {"rivet_core"},
    "rivet_aws": {"rivet_core"},
    "rivet_databricks": {"rivet_core"},
    "rivet_rest": {"rivet_core"},
}

_IMPORT_RE = re.compile(r"(?:from|import)\s+(rivet_[a-z_]+)")

# Vendor SDK patterns that should NOT appear in certain modules.  Each entry
# maps a module name to a list of (compiled_regex, description) pairs.
# Patterns are tightened to match real code, not bare words in docstrings or
# comments — line content is pre-tokenized to drop STRING and COMMENT tokens
# before regex matching, so e.g. a docstring saying "for AWS Glue" no longer
# trips the boundary check.
FORBIDDEN_SDK_PATTERNS: dict[str, list[tuple[re.Pattern[str], str]]] = {
    "rivet_core": [
        (
            re.compile(
                r"\bboto3\b|\bbotocore\b|\.get_table\(|\.get_partitions\(|\bglue_client\b",
            ),
            "AWS SDK / Glue usage (belongs in rivet_aws)",
        ),
    ],
}


@dataclass(frozen=True)
class Violation:
    file: str
    line_no: int
    source_module: str
    imported_module: str
    line_text: str


def _strip_comments_and_strings(source: str) -> str:
    """Return source with all STRING and COMMENT tokens replaced by whitespace,
    preserving line numbers and column offsets for caller-side line indexing.
    """
    out_lines = source.splitlines()
    try:
        tokens = list(tokenize.tokenize(io.BytesIO(source.encode("utf-8")).readline))
    except (tokenize.TokenizeError, IndentationError):
        return source
    for tok in tokens:
        if tok.type not in (tokenize.STRING, tokenize.COMMENT):
            continue
        sr, sc = tok.start
        er, ec = tok.end
        if sr == er:
            line = out_lines[sr - 1]
            out_lines[sr - 1] = line[:sc] + (" " * (ec - sc)) + line[ec:]
        else:
            # Multi-line string: blank out from sc to end of line, then full
            # lines, then 0 to ec on the closing line.
            first = out_lines[sr - 1]
            out_lines[sr - 1] = first[:sc] + (" " * (len(first) - sc))
            for r in range(sr, er - 1):
                out_lines[r] = " " * len(out_lines[r])
            last = out_lines[er - 1]
            out_lines[er - 1] = (" " * ec) + last[ec:]
    return "\n".join(out_lines)


def scan_boundary_violations(src_dir: str | Path) -> list[Violation]:
    """Return all boundary violations found under *src_dir*."""
    src_dir = Path(src_dir)
    violations: list[Violation] = []

    for module_dir in sorted(src_dir.iterdir()):
        if not module_dir.is_dir() or not module_dir.name.startswith("rivet_"):
            continue
        module_name = module_dir.name
        allowed = ALLOWED_RIVET_IMPORTS.get(module_name, {"rivet_core"})
        forbidden_patterns = FORBIDDEN_SDK_PATTERNS.get(module_name, [])

        for py_file in sorted(module_dir.rglob("*.py")):
            rel = str(py_file.relative_to(src_dir))
            try:
                source = py_file.read_text()
            except OSError:
                continue
            code_only = _strip_comments_and_strings(source)
            raw_lines = source.splitlines()
            code_lines = code_only.splitlines()

            for line_no, raw_line in enumerate(raw_lines, 1):
                stripped = raw_line.strip()
                if stripped.startswith("#") or not stripped:
                    continue
                code_line = code_lines[line_no - 1] if line_no - 1 < len(code_lines) else ""

                # Check rivet_* import boundaries (use the tokenized line so
                # docstrings cannot smuggle imports past the check).
                m = _IMPORT_RE.search(code_line)
                if m:
                    imported = m.group(1)
                    if (
                        imported != module_name
                        and imported.startswith("rivet_")
                        and imported not in allowed
                    ):
                        violations.append(
                            Violation(rel, line_no, module_name, imported, stripped)
                        )

                # Check vendor SDK usage in code only (not comments/strings)
                for pattern, description in forbidden_patterns:
                    if pattern.search(code_line):
                        violations.append(
                            Violation(rel, line_no, module_name, description, stripped)
                        )

    return violations


def main() -> int:
    src_dir = Path(__file__).resolve().parent.parent / "src"
    violations = scan_boundary_violations(src_dir)

    if not violations:
        print("No module boundary violations found.")
        return 0

    print(f"Found {len(violations)} module boundary violation(s):\n")
    for v in violations:
        print(f"  {v.file}:{v.line_no}  [{v.source_module} -> {v.imported_module}]")
        print(f"    {v.line_text}\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
