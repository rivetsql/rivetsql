"""Tests for test discovery (task 7.1) and extends resolution (task 7.2)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rivet_cli.commands.test import (
    TestDiscoveryError,
    discover_tests,
    filter_tests,
    resolve_extends,
)
from rivet_core.testing.models import TestDef


class TestDiscoverTests:
    """Tests for discover_tests()."""

    def test_discovers_from_tests_dir(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "basic.test.yaml").write_text(
            yaml.dump({"name": "t1", "target": "j1"})
        )
        result = discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        assert len(result) == 1
        assert result[0].name == "t1"
        assert result[0].target == "j1"

    def test_discovers_from_joints_dir(self, tmp_path: Path) -> None:
        joints_dir = tmp_path / "joints"
        joints_dir.mkdir()
        (joints_dir / "orders.test.yaml").write_text(
            yaml.dump({"name": "t1", "target": "orders"})
        )
        result = discover_tests(tmp_path, tmp_path / "tests", joints_dir)
        assert len(result) == 1
        assert result[0].name == "t1"

    def test_discovers_recursively_in_tests(self, tmp_path: Path) -> None:
        nested = tmp_path / "tests" / "sub"
        nested.mkdir(parents=True)
        (nested / "deep.test.yaml").write_text(
            yaml.dump({"name": "deep", "target": "j1"})
        )
        result = discover_tests(tmp_path, tmp_path / "tests", tmp_path / "joints")
        assert len(result) == 1
        assert result[0].name == "deep"

    def test_ignores_non_test_yaml(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "config.yaml").write_text(yaml.dump({"name": "x", "target": "j"}))
        (tests_dir / "real.test.yaml").write_text(yaml.dump({"name": "x", "target": "j"}))
        result = discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        assert len(result) == 1

    def test_yaml_stream_multiple_docs(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        content = (
            "name: t1\ntarget: j1\n---\nname: t2\ntarget: j2\n"
        )
        (tests_dir / "multi.test.yaml").write_text(content)
        result = discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        assert len(result) == 2
        assert {r.name for r in result} == {"t1", "t2"}

    def test_duplicate_name_raises_rvt908(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "a.test.yaml").write_text(yaml.dump({"name": "dup", "target": "j1"}))
        (tests_dir / "b.test.yaml").write_text(yaml.dump({"name": "dup", "target": "j2"}))
        with pytest.raises(TestDiscoveryError) as exc_info:
            discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        assert exc_info.value.error.code == "RVT-908"

    def test_duplicate_name_within_stream_raises_rvt908(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        content = "name: dup\ntarget: j1\n---\nname: dup\ntarget: j2\n"
        (tests_dir / "multi.test.yaml").write_text(content)
        with pytest.raises(TestDiscoveryError) as exc_info:
            discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        assert exc_info.value.error.code == "RVT-908"

    def test_skips_docs_missing_name(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        content = "target: j1\n---\nname: valid\ntarget: j2\n"
        (tests_dir / "partial.test.yaml").write_text(content)
        result = discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        assert len(result) == 1
        assert result[0].name == "valid"

    def test_skips_docs_missing_target(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "no_target.test.yaml").write_text(
            yaml.dump({"name": "orphan"})
        )
        result = discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        assert len(result) == 0

    def test_targets_plural_accepted(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "multi_target.test.yaml").write_text(yaml.dump({
            "name": "mt",
            "targets": {"j1": {"expected": {}}, "j2": {"expected": {}}},
        }))
        result = discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        assert len(result) == 1
        assert result[0].target == "j1"
        assert result[0].targets is not None

    def test_returns_testdef_instances(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "typed.test.yaml").write_text(yaml.dump({
            "name": "typed",
            "target": "j1",
            "scope": "assembly",
            "compare": "unordered",
            "tags": ["fast"],
            "description": "A test",
            "inputs": {"src": {"file": "data.parquet"}},
            "options": {"tolerance": 0.01},
        }))
        result = discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        td = result[0]
        assert isinstance(td, TestDef)
        assert td.scope == "assembly"
        assert td.compare == "unordered"
        assert td.tags == ["fast"]
        assert td.description == "A test"
        assert td.options == {"tolerance": 0.01}
        assert td.source_file is not None

    def test_defaults_applied(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "minimal.test.yaml").write_text(
            yaml.dump({"name": "min", "target": "j1"})
        )
        result = discover_tests(tmp_path, tests_dir, tmp_path / "joints")
        td = result[0]
        assert td.scope == "joint"
        assert td.compare == "exact"
        assert td.tags == []
        assert td.inputs == {}
        assert td.expected is None

    def test_empty_dirs_returns_empty(self, tmp_path: Path) -> None:
        result = discover_tests(
            tmp_path, tmp_path / "tests", tmp_path / "joints"
        )
        assert result == []

    def test_both_dirs_combined(self, tmp_path: Path) -> None:
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        joints_dir = tmp_path / "joints"
        joints_dir.mkdir()
        (tests_dir / "a.test.yaml").write_text(yaml.dump({"name": "a", "target": "j1"}))
        (joints_dir / "b.test.yaml").write_text(yaml.dump({"name": "b", "target": "j2"}))
        result = discover_tests(tmp_path, tests_dir, joints_dir)
        assert len(result) == 2
        assert {r.name for r in result} == {"a", "b"}


def _make_testdef(**kwargs: object) -> TestDef:
    defaults = dict(name="t", target="j", scope="joint", inputs={}, compare="exact",
                    tags=[], options={}, description=None, expected=None,
                    compare_function=None, extends=None, source_file=None, engine=None, targets=None)
    defaults.update(kwargs)
    return TestDef(**defaults)  # type: ignore[arg-type]


class TestResolveExtends:
    """Tests for resolve_extends()."""

    def test_no_extends_returns_unchanged(self) -> None:
        tests = [_make_testdef(name="a"), _make_testdef(name="b")]
        result = resolve_extends(tests)
        assert result == tests

    def test_extends_inherits_inputs_from_base(self) -> None:
        base = _make_testdef(name="base", inputs={"src": {"file": "data.parquet"}})
        child = _make_testdef(name="child", extends="base", inputs={})
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.inputs == {"src": {"file": "data.parquet"}}

    def test_extends_child_inputs_override_base(self) -> None:
        base = _make_testdef(name="base", inputs={"src": {"file": "a.parquet"}, "ref": {"file": "r.parquet"}})
        child = _make_testdef(name="child", extends="base", inputs={"src": {"file": "b.parquet"}})
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.inputs == {"src": {"file": "b.parquet"}, "ref": {"file": "r.parquet"}}

    def test_extends_inherits_compare_from_base(self) -> None:
        base = _make_testdef(name="base", compare="unordered")
        child = _make_testdef(name="child", extends="base")  # compare defaults to "exact"
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.compare == "unordered"

    def test_extends_child_compare_overrides_base(self) -> None:
        base = _make_testdef(name="base", compare="unordered")
        child = _make_testdef(name="child", extends="base", compare="schema_only")
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.compare == "schema_only"

    def test_extends_inherits_tags_from_base(self) -> None:
        base = _make_testdef(name="base", tags=["slow", "integration"])
        child = _make_testdef(name="child", extends="base")  # tags defaults to []
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.tags == ["slow", "integration"]

    def test_extends_child_tags_override_base(self) -> None:
        base = _make_testdef(name="base", tags=["slow"])
        child = _make_testdef(name="child", extends="base", tags=["fast"])
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.tags == ["fast"]

    def test_extends_inherits_options_from_base(self) -> None:
        base = _make_testdef(name="base", options={"tolerance": 0.01})
        child = _make_testdef(name="child", extends="base")  # options defaults to {}
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.options == {"tolerance": 0.01}

    def test_extends_child_options_override_base(self) -> None:
        base = _make_testdef(name="base", options={"tolerance": 0.01})
        child = _make_testdef(name="child", extends="base", options={"tolerance": 0.1})
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.options == {"tolerance": 0.1}

    def test_extends_inherits_description_from_base(self) -> None:
        base = _make_testdef(name="base", description="Base description")
        child = _make_testdef(name="child", extends="base")  # description defaults to None
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.description == "Base description"

    def test_extends_child_description_overrides_base(self) -> None:
        base = _make_testdef(name="base", description="Base description")
        child = _make_testdef(name="child", extends="base", description="Child description")
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.description == "Child description"

    def test_extends_missing_base_raises_rvt909(self) -> None:
        child = _make_testdef(name="child", extends="nonexistent")
        with pytest.raises(TestDiscoveryError) as exc_info:
            resolve_extends([child])
        assert exc_info.value.error.code == "RVT-909"

    def test_extends_missing_base_error_mentions_names(self) -> None:
        child = _make_testdef(name="child", extends="missing_base")
        with pytest.raises(TestDiscoveryError) as exc_info:
            resolve_extends([child])
        assert "missing_base" in exc_info.value.error.message
        assert "child" in exc_info.value.error.message

    def test_extends_preserves_child_name_and_target(self) -> None:
        base = _make_testdef(name="base", target="j_base")
        child = _make_testdef(name="child", target="j_child", extends="base")
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.name == "child"
        assert child_resolved.target == "j_child"

    def test_extends_preserves_source_file(self) -> None:
        p = Path("/some/file.test.yaml")
        base = _make_testdef(name="base")
        child = _make_testdef(name="child", extends="base", source_file=p)
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.source_file == p

    def test_extends_inherits_expected_from_base(self) -> None:
        base = _make_testdef(name="base", expected={"columns": ["a"], "rows": [[1]]})
        child = _make_testdef(name="child", extends="base")
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.expected == {"columns": ["a"], "rows": [[1]]}

    def test_extends_child_expected_overrides_base(self) -> None:
        base = _make_testdef(name="base", expected={"columns": ["a"], "rows": [[1]]})
        child = _make_testdef(name="child", extends="base", expected={"columns": ["a"], "rows": [[2]]})
        result = resolve_extends([base, child])
        child_resolved = next(r for r in result if r.name == "child")
        assert child_resolved.expected == {"columns": ["a"], "rows": [[2]]}

    def test_resolve_extends_returns_all_tests(self) -> None:
        base = _make_testdef(name="base")
        child = _make_testdef(name="child", extends="base")
        other = _make_testdef(name="other")
        result = resolve_extends([base, child, other])
        assert len(result) == 3
        assert {r.name for r in result} == {"base", "child", "other"}


class TestFilterTests:
    """Tests for filter_tests() — task 7.3."""

    def _tests(self) -> list[TestDef]:
        return [
            _make_testdef(name="a", target="j1", tags=["fast", "core"], source_file=Path("a.test.yaml")),
            _make_testdef(name="b", target="j2", tags=["slow"], source_file=Path("b.test.yaml")),
            _make_testdef(name="c", target="j1", tags=["fast"], source_file=Path("c.test.yaml")),
        ]

    def test_no_filters_returns_all(self) -> None:
        result = filter_tests(self._tests(), tags=[], target=None, file_paths=[])
        assert len(result) == 3

    def test_filter_by_tag_or_mode(self) -> None:
        result = filter_tests(self._tests(), tags=["slow"], target=None, file_paths=[])
        assert [t.name for t in result] == ["b"]

    def test_filter_by_tag_or_mode_multiple(self) -> None:
        result = filter_tests(self._tests(), tags=["fast", "slow"], target=None, file_paths=[])
        assert {t.name for t in result} == {"a", "b", "c"}

    def test_filter_by_tag_and_mode(self) -> None:
        result = filter_tests(self._tests(), tags=["fast", "core"], target=None, file_paths=[], tag_all=True)
        assert [t.name for t in result] == ["a"]

    def test_filter_by_tag_and_mode_no_match(self) -> None:
        result = filter_tests(self._tests(), tags=["fast", "slow"], target=None, file_paths=[], tag_all=True)
        assert result == []

    def test_filter_by_target(self) -> None:
        result = filter_tests(self._tests(), tags=[], target="j1", file_paths=[])
        assert {t.name for t in result} == {"a", "c"}

    def test_filter_by_target_no_match(self) -> None:
        result = filter_tests(self._tests(), tags=[], target="j99", file_paths=[])
        assert result == []

    def test_filter_by_file_paths(self) -> None:
        result = filter_tests(self._tests(), tags=[], target=None, file_paths=[Path("a.test.yaml")])
        assert [t.name for t in result] == ["a"]

    def test_filter_by_file_paths_multiple(self) -> None:
        result = filter_tests(self._tests(), tags=[], target=None, file_paths=[Path("a.test.yaml"), Path("c.test.yaml")])
        assert {t.name for t in result} == {"a", "c"}

    def test_filter_by_file_paths_no_match(self) -> None:
        result = filter_tests(self._tests(), tags=[], target=None, file_paths=[Path("z.test.yaml")])
        assert result == []

    def test_filter_combined_tag_and_target(self) -> None:
        result = filter_tests(self._tests(), tags=["fast"], target="j1", file_paths=[])
        assert {t.name for t in result} == {"a", "c"}

    def test_filter_combined_target_and_file(self) -> None:
        result = filter_tests(self._tests(), tags=[], target="j1", file_paths=[Path("a.test.yaml")])
        assert [t.name for t in result] == ["a"]

    def test_empty_tests_returns_empty(self) -> None:
        result = filter_tests([], tags=["fast"], target="j1", file_paths=[Path("x.yaml")])
        assert result == []
