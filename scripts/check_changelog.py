#!/usr/bin/env python3
"""Lint CHANGELOG.md for duplicate sub-headers within a single release block.

Exits non-zero if any release contains two ``###`` sub-headers with the same
title (e.g. two ``### Added`` blocks). This guards against the assembly bug
that produced the original 0.1.14 / 0.1.15 / 0.1.16 release entries with
multiple ``### Added`` sections each.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHANGELOG = ROOT / "CHANGELOG.md"


def main() -> int:
    if not CHANGELOG.is_file():
        print(f"CHANGELOG not found at {CHANGELOG}", file=sys.stderr)
        return 1

    lines = CHANGELOG.read_text(encoding="utf-8").splitlines()

    current_release: str | None = None
    seen_in_release: set[str] = set()
    duplicates: list[tuple[str, str, int]] = []

    for line_no, line in enumerate(lines, start=1):
        if line.startswith("## ") and not line.startswith("### "):
            current_release = line.strip()
            seen_in_release = set()
            continue
        if line.startswith("### "):
            sub = line[4:].strip()
            if current_release is None:
                continue
            if sub in seen_in_release:
                duplicates.append((current_release, sub, line_no))
            else:
                seen_in_release.add(sub)

    if duplicates:
        print("CHANGELOG.md has duplicate sub-headers:", file=sys.stderr)
        for release, sub, line_no in duplicates:
            print(f"  {release}: '### {sub}' at line {line_no}", file=sys.stderr)
        print(
            "\nMerge each duplicated sub-header into a single block per release.",
            file=sys.stderr,
        )
        return 1

    print("CHANGELOG.md OK (no duplicate sub-headers).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
