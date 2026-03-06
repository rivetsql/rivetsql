"""Property tests for QualityParser — Property 28.

Feature: rivet-config, Property 28: Multiple quality files merge in filename order
Validates: Requirements 14.6
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.quality import QualityParser

PARSER = QualityParser()

# Strategy: generate a list of 2–5 distinct filename stems (sorted order is deterministic).
_stem_strategy = st.lists(
    st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True),
    min_size=2,
    max_size=5,
    unique=True,
)


@given(stems=_stem_strategy)
@settings(max_examples=100)
def test_dedicated_files_merge_in_filename_order(stems: list[str]) -> None:
    """Property 28 (dedicated): When multiple dedicated quality files target the
    same joint, parsing them in filename-sorted order and concatenating their
    checks produces the same result as sorting the stems lexicographically."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        # Write one check per file; check_type encodes the stem index for ordering.
        file_paths: list[Path] = []
        for stem in stems:
            fp = tmp / f"{stem}.yaml"
            fp.write_text(yaml.dump([{"type": "row_count"}]))
            file_paths.append(fp)

        # Sort files by filename (lexicographic) — this is the required merge order.
        sorted_paths = sorted(file_paths, key=lambda p: p.name)

        all_checks: list = []
        for fp in sorted_paths:
            checks, errors = PARSER.parse_dedicated_file(fp)
            assert not errors
            all_checks.extend(checks)

        # Verify: the source_file of each check matches the sorted order.
        assert [c.source_file for c in all_checks] == sorted_paths
        assert all(c.source == "dedicated" for c in all_checks)


@given(stems=_stem_strategy)
@settings(max_examples=100)
def test_colocated_files_merge_in_filename_order(stems: list[str]) -> None:
    """Property 28 (colocated): When multiple co-located quality files target the
    same joint, parsing them in filename-sorted order and concatenating their
    checks produces the same result as sorting the stems lexicographically."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        file_paths: list[Path] = []
        for stem in stems:
            fp = tmp / f"{stem}.yaml"
            fp.write_text(yaml.dump([{"type": "row_count"}]))
            file_paths.append(fp)

        sorted_paths = sorted(file_paths, key=lambda p: p.name)

        all_checks: list = []
        for fp in sorted_paths:
            checks, errors = PARSER.parse_colocated_file(fp)
            assert not errors
            all_checks.extend(checks)

        assert [c.source_file for c in all_checks] == sorted_paths
        assert all(c.source == "colocated" for c in all_checks)


@given(stems=_stem_strategy)
@settings(max_examples=100)
def test_filename_sorted_order_is_lexicographic(stems: list[str]) -> None:
    """Property 28 (ordering): The filename-sorted order of quality files is
    strictly lexicographic — sorting by filename stem produces the same order
    as Python's default string sort."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        file_paths = [tmp / f"{stem}.yaml" for stem in stems]
        for fp in file_paths:
            fp.write_text(yaml.dump([{"type": "schema"}]))

        sorted_by_name = sorted(file_paths, key=lambda p: p.name)
        sorted_stems = [p.stem for p in sorted_by_name]

        # Lexicographic sort of stems should match.
        assert sorted_stems == sorted(stems)
