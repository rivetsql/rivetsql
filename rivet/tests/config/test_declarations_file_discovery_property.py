"""Property test for DeclarationLoader — file discovery rules.

Feature: rivet-config, Property 13: File discovery respects extension and visibility rules
Validates: Requirements 8.1, 8.2, 8.3, 8.4, 8.6

For any directory tree containing files with various extensions and visibility
(hidden files, _-prefixed directories), the declaration loader should discover
exactly the files with .yaml, .yml, or .sql extensions that are not hidden and
not under _-prefixed directories, recursively.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.declarations import DeclarationLoader
from rivet_config.models import ProjectManifest

_VALID_EXTENSIONS = (".yaml", ".yml", ".sql")
_IGNORED_EXTENSIONS = (".txt", ".md", ".csv", ".json", ".py")

_identifier = st.from_regex(r"[a-z][a-z0-9]{0,10}", fullmatch=True)

_SOURCE_YAML = "name: {name}\ntype: source\ncatalog: pg"
_SQL_CONTENT = "-- rivet:name: {name}\nSELECT 1"


def _make_manifest(tmp_path: Path) -> ProjectManifest:
    sources = tmp_path / "sources"
    joints = tmp_path / "joints"
    sinks = tmp_path / "sinks"
    for d in (sources, joints, sinks):
        d.mkdir(exist_ok=True)
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


@settings(max_examples=100)
@given(
    names=st.lists(_identifier, min_size=1, max_size=5, unique=True),
)
def test_only_recognized_extensions_discovered(names: list[str]) -> None:
    """Property 13: Only .yaml, .yml, .sql files are discovered; other extensions are ignored."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        manifest = _make_manifest(tmp_path)
        sources = manifest.sources_dir

        expected_names: set[str] = set()
        for i, name in enumerate(names):
            ext = _VALID_EXTENSIONS[i % len(_VALID_EXTENSIONS)]
            content = _SQL_CONTENT.format(name=name) if ext == ".sql" else _SOURCE_YAML.format(name=name)
            (sources / f"{name}{ext}").write_text(content)
            expected_names.add(name)

        # Write ignored files (should not be discovered)
        for i, ext in enumerate(_IGNORED_EXTENSIONS):
            (sources / f"ignored_{i}{ext}").write_text("irrelevant content")

        loader = DeclarationLoader()
        decls, _ = loader.load(manifest)

        discovered_names = {d.name for d in decls}
        assert discovered_names == expected_names, (
            f"Expected {expected_names}, got {discovered_names}"
        )


@settings(max_examples=100)
@given(name=_identifier)
def test_hidden_files_not_discovered(name: str) -> None:
    """Property 13: Files with a leading dot are not discovered."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        manifest = _make_manifest(tmp_path)
        sources = manifest.sources_dir

        # Hidden file (dot-prefixed) — should be ignored
        (sources / f".{name}.yaml").write_text(_SOURCE_YAML.format(name=name))
        # Visible file — should be discovered
        visible_name = f"v{name}"
        (sources / f"{visible_name}.yaml").write_text(_SOURCE_YAML.format(name=visible_name))

        loader = DeclarationLoader()
        decls, _ = loader.load(manifest)

        discovered_names = {d.name for d in decls}
        assert name not in discovered_names, f"Hidden file for '{name}' should not be discovered"
        assert visible_name in discovered_names, f"Visible file '{visible_name}' should be discovered"


@settings(max_examples=100)
@given(name=_identifier)
def test_underscore_prefixed_dirs_not_scanned(name: str) -> None:
    """Property 13: Files inside _-prefixed directories are not discovered."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        manifest = _make_manifest(tmp_path)
        sources = manifest.sources_dir

        # File inside _-prefixed directory — should be ignored
        excluded_dir = sources / f"_{name}_internal"
        excluded_dir.mkdir(exist_ok=True)
        (excluded_dir / f"{name}.yaml").write_text(_SOURCE_YAML.format(name=name))

        # File in normal subdirectory — should be discovered
        normal_dir = sources / "normal"
        normal_dir.mkdir(exist_ok=True)
        visible_name = f"v{name}"
        (normal_dir / f"{visible_name}.yaml").write_text(_SOURCE_YAML.format(name=visible_name))

        loader = DeclarationLoader()
        decls, _ = loader.load(manifest)

        discovered_names = {d.name for d in decls}
        assert name not in discovered_names, f"File in _-prefixed dir for '{name}' should not be discovered"
        assert visible_name in discovered_names, f"File in normal dir '{visible_name}' should be discovered"


@settings(max_examples=100)
@given(
    names=st.lists(_identifier, min_size=1, max_size=4, unique=True),
    depth=st.integers(min_value=1, max_value=3),
)
def test_recursive_discovery(names: list[str], depth: int) -> None:
    """Property 13: Files are discovered recursively in nested subdirectories."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        manifest = _make_manifest(tmp_path)
        sources = manifest.sources_dir

        expected_names: set[str] = set()
        for i, name in enumerate(names):
            nested = sources
            for level in range(i % depth + 1):
                nested = nested / f"sub{level}"
                nested.mkdir(exist_ok=True)
            (nested / f"{name}.yaml").write_text(_SOURCE_YAML.format(name=name))
            expected_names.add(name)

        loader = DeclarationLoader()
        decls, _ = loader.load(manifest)

        discovered_names = {d.name for d in decls}
        assert expected_names.issubset(discovered_names), (
            f"Expected {expected_names} to be discovered, got {discovered_names}"
        )
