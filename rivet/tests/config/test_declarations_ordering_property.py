"""Property test for DeclarationLoader — output ordering.

Feature: rivet-config, Property 14: Declaration output is lexicographically ordered
Validates: Requirements 8.7, 23.1

For any set of discovered joint declaration files, the output JointDeclaration list
should be sorted by source_file path in lexicographic order.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.declarations import DeclarationLoader
from rivet_config.models import ProjectManifest

_joint_name = st.from_regex(r"[a-z][a-z0-9]{0,10}", fullmatch=True)
_filename_stem = st.from_regex(r"[a-z][a-z0-9]{0,8}", fullmatch=True)


def _make_manifest(tmp_path: Path) -> ProjectManifest:
    sources = tmp_path / "sources"
    joints = tmp_path / "joints"
    sinks = tmp_path / "sinks"
    for d in (sources, joints, sinks):
        d.mkdir(parents=True, exist_ok=True)
    return ProjectManifest(
        project_root=tmp_path,
        profiles_path=tmp_path / "profiles.yaml",
        sources_dir=sources,
        joints_dir=joints,
        sinks_dir=sinks,
        quality_dir=None,
        tests_dir=tmp_path / "tests",
        fixtures_dir=tmp_path / "fixtures",
    )


@settings(max_examples=100, deadline=500)
@given(
    names=st.lists(_joint_name, min_size=1, max_size=8, unique=True),
    stems=st.lists(_filename_stem, min_size=1, max_size=8, unique=True),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop14_ordering")),
)
def test_declarations_sorted_lexicographically_by_source_file(
    names: list[str],
    stems: list[str],
    tmp_path: Path,
) -> None:
    """Property 14: Declaration output is lexicographically ordered by source_file.

    Validates: Requirements 8.7, 23.1
    """
    # Clean up from previous runs to avoid stale files
    if tmp_path.exists():
        shutil.rmtree(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    manifest = _make_manifest(tmp_path)

    # Use as many names as we have stems (take the shorter list length)
    count = min(len(names), len(stems))
    if count == 0:
        return

    used_names = names[:count]
    used_stems = stems[:count]

    # Write joint files into sources dir with the given stems (ensure unique filenames)
    for name, stem in zip(used_names, used_stems):
        file_path = manifest.sources_dir / f"{stem}.yaml"
        # If file already exists (stem collision after truncation), skip
        if file_path.exists():
            continue
        file_path.write_text(yaml.dump({"name": name, "type": "source", "catalog": "pg"}))

    loader = DeclarationLoader()
    decls, _ = loader.load(manifest)

    # The output must be sorted by source_path lexicographically
    paths = [d.source_path for d in decls]
    assert paths == sorted(paths), (
        f"Declarations not sorted: {[str(p) for p in paths]}"
    )
