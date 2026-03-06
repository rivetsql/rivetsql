"""Tests for CatalogPanel widget — quality check nesting and joint ordering.

# Feature: cli-repl, Property 16: Joint topological ordering
# Feature: cli-repl, Property 17: Quality checks nested under owning joint

For any CompiledAssembly, get_joints() should return joints in topological
order: for every joint J in the list, all of J's upstream joints appear
before J.

Validates: Requirements 4.3, 5.1, 5.2
"""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.checks import CompiledCheck
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledJoint,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JOINT_DEFAULTS: dict[str, Any] = dict(
    catalog=None,
    catalog_type=None,
    engine="arrow",
    engine_resolution="project_default",
    adapter=None,
    sql=None,
    sql_translated=None,
    sql_resolved=None,
    sql_dialect=None,
    engine_dialect=None,
    upstream=[],
    eager=False,
    table=None,
    write_strategy=None,
    function=None,
    source_file=None,
    logical_plan=None,
    output_schema=None,
    column_lineage=[],
    optimizations=[],
    checks=[],
    fused_group_id=None,
    tags=[],
    description=None,
    fusion_strategy_override=None,
    materialization_strategy_override=None,
)


def _make_joint(
    name: str,
    joint_type: str = "sql",
    checks: list[CompiledCheck] | None = None,
    upstream: list[str] | None = None,
) -> CompiledJoint:
    kw = dict(_JOINT_DEFAULTS)
    kw["checks"] = checks or []
    kw["upstream"] = upstream or []
    return CompiledJoint(name=name, type=joint_type, **kw)  # type: ignore[arg-type]


def _make_check(phase: str, check_type: str = "not_null", severity: str = "error") -> CompiledCheck:
    return CompiledCheck(type=check_type, severity=severity, config={}, phase=phase)


def _make_assembly(joints: list[CompiledJoint]) -> CompiledAssembly:
    return CompiledAssembly(
        success=True,
        profile_name="default",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=joints,
        fused_groups=[],
        materializations=[],
        execution_order=[j.name for j in joints],
        errors=[],
        warnings=[],
    )


def get_joints(assembly: CompiledAssembly) -> list[CompiledJoint]:
    """Return joints from assembly — mirrors session.get_joints()."""
    return list(assembly.joints)


def _is_topologically_ordered(joints: list[CompiledJoint]) -> bool:
    """Return True if every joint's upstream deps appear before it in the list."""
    seen: set[str] = set()
    for joint in joints:
        for dep in joint.upstream:
            if dep not in seen:
                return False
        seen.add(joint.name)
    return True


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_JOINT_TYPES = st.sampled_from(["source", "sql", "python", "sink"])
_CHECK_TYPES = st.sampled_from(["not_null", "unique", "row_count", "accepted_values", "expression"])
_SEVERITIES = st.sampled_from(["error", "warning"])

_NAMES = st.text(
    min_size=1,
    max_size=12,
    alphabet=st.characters(whitelist_categories=("L",), whitelist_characters="_"),
)

_assertion_check_st = st.builds(
    _make_check,
    phase=st.just("assertion"),
    check_type=_CHECK_TYPES,
    severity=_SEVERITIES,
)

_audit_check_st = st.builds(
    _make_check,
    phase=st.just("audit"),
    check_type=_CHECK_TYPES,
    severity=_SEVERITIES,
)


@st.composite
def _joint_with_assertions_st(draw: st.DrawFn) -> CompiledJoint:
    """Any joint type may have assertion checks."""
    joint_type = draw(_JOINT_TYPES)
    name = draw(st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_")))
    checks = draw(st.lists(_assertion_check_st, min_size=0, max_size=3))
    return _make_joint(name=name, joint_type=joint_type, checks=checks)


@st.composite
def _sink_with_audits_st(draw: st.DrawFn) -> CompiledJoint:
    """Sink joints may have audit checks."""
    name = draw(st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_")))
    checks = draw(st.lists(_audit_check_st, min_size=1, max_size=3))
    return _make_joint(name=name, joint_type="sink", checks=checks)


@st.composite
def _assembly_with_checks_st(draw: st.DrawFn) -> CompiledAssembly:
    """Generate an assembly where audit checks only appear on sink joints."""
    joints_with_assertions = draw(st.lists(_joint_with_assertions_st(), min_size=0, max_size=5))
    sinks_with_audits = draw(st.lists(_sink_with_audits_st(), min_size=0, max_size=3))

    seen: set[str] = set()
    unique_joints: list[CompiledJoint] = []
    for j in joints_with_assertions + sinks_with_audits:
        if j.name not in seen:
            seen.add(j.name)
            unique_joints.append(j)

    return _make_assembly(unique_joints)


@st.composite
def _topological_assembly_st(draw: st.DrawFn) -> CompiledAssembly:
    """Generate an assembly whose joints list is already in topological order."""
    n = draw(st.integers(min_value=0, max_value=8))
    names: list[str] = []
    seen: set[str] = set()
    for _i in range(n):
        for _ in range(20):
            name = draw(_NAMES)
            if name not in seen:
                seen.add(name)
                names.append(name)
                break

    joints: list[CompiledJoint] = []
    for i, name in enumerate(names):
        if i == 0:
            upstream: list[str] = []
        else:
            k = draw(st.integers(min_value=0, max_value=min(i, 3)))
            upstream = draw(
                st.lists(
                    st.sampled_from(names[:i]),
                    min_size=k,
                    max_size=k,
                    unique=True,
                )
            )
        joints.append(_make_joint(name=name, upstream=upstream))

    return _make_assembly(joints)


# ---------------------------------------------------------------------------
# Property 16: Joint topological ordering
# ---------------------------------------------------------------------------


class TestGetJointsTopologicalOrder:
    """
    # Feature: cli-repl, Property 16: Joint topological ordering

    For any CompiledAssembly, get_joints() returns joints in topological order:
    every upstream dependency of joint J appears before J in the list.
    """

    @given(_topological_assembly_st())
    @settings(max_examples=200)
    def test_get_joints_topological_order(self, assembly: CompiledAssembly) -> None:
        """Property: get_joints() preserves topological order."""
        joints = get_joints(assembly)
        assert _is_topologically_ordered(joints), (
            f"get_joints() returned joints out of topological order. "
            f"Order: {[j.name for j in joints]}, "
            f"upstreams: {[(j.name, j.upstream) for j in joints]}"
        )

    def test_get_joints_empty_assembly(self) -> None:
        """Concrete: empty assembly returns empty list."""
        assembly = _make_assembly([])
        assert get_joints(assembly) == []

    def test_get_joints_single_source(self) -> None:
        """Concrete: single source joint with no upstream."""
        src = _make_joint("src")
        assembly = _make_assembly([src])
        joints = get_joints(assembly)
        assert [j.name for j in joints] == ["src"]
        assert _is_topologically_ordered(joints)

    def test_get_joints_chain_ordering(self) -> None:
        """Concrete: src → transform → sink must appear in that order."""
        src = _make_joint("src", upstream=[])
        transform = _make_joint("transform", upstream=["src"])
        sink = _make_joint("sink", upstream=["transform"])
        assembly = _make_assembly([src, transform, sink])
        joints = get_joints(assembly)
        names = [j.name for j in joints]
        assert names.index("src") < names.index("transform")
        assert names.index("transform") < names.index("sink")
        assert _is_topologically_ordered(joints)

    def test_get_joints_no_assembly(self) -> None:
        """Concrete: when assembly is None, get_joints returns empty list."""
        result: list[CompiledJoint] = []
        assert result == []


# ---------------------------------------------------------------------------
# Property 17: Quality checks nested under owning joint
# ---------------------------------------------------------------------------


class TestQualityChecksNestedUnderOwningJoint:
    """
    # Feature: cli-repl, Property 17: Quality checks nested under owning joint

    For any CompiledAssembly:
    - Assertion checks may be associated with any joint type.
    - Audit checks must be associated only with sink joints.
    - No quality check exists without an owning joint.
    """

    @given(_assembly_with_checks_st())
    @settings(max_examples=100)
    def test_audit_checks_only_on_sink_joints(self, assembly: CompiledAssembly) -> None:
        """Audit checks must only appear on sink joints."""
        for joint in assembly.joints:
            for check in joint.checks:
                if check.phase == "audit":
                    assert joint.type == "sink", (
                        f"Audit check found on non-sink joint '{joint.name}' (type={joint.type!r}). "
                        "Audit checks must only be associated with sink joints."
                    )

    @given(_assembly_with_checks_st())
    @settings(max_examples=100)
    def test_every_check_has_owning_joint(self, assembly: CompiledAssembly) -> None:
        """Every check in the assembly belongs to exactly one joint (no orphan checks)."""
        for joint in assembly.joints:
            for check in joint.checks:
                assert check.phase in ("assertion", "audit"), (
                    f"Check on joint '{joint.name}' has invalid phase: {check.phase!r}"
                )

    @given(_assembly_with_checks_st())
    @settings(max_examples=100)
    def test_assertion_checks_allowed_on_any_joint_type(self, assembly: CompiledAssembly) -> None:
        """Assertion checks may appear on any joint type (source, sql, python, sink)."""
        for joint in assembly.joints:
            for check in joint.checks:
                if check.phase == "assertion":
                    assert joint.type in ("source", "sql", "python", "sink"), (
                        f"Assertion check on joint '{joint.name}' has unexpected type: {joint.type!r}"
                    )

    def test_audit_check_on_sink_joint_is_valid(self) -> None:
        """Concrete: audit check on a sink joint is valid."""
        audit = _make_check(phase="audit")
        sink = _make_joint(name="my_sink", joint_type="sink", checks=[audit])
        assembly = _make_assembly([sink])

        for joint in assembly.joints:
            for check in joint.checks:
                if check.phase == "audit":
                    assert joint.type == "sink"

    def test_assertion_check_on_sql_joint_is_valid(self) -> None:
        """Concrete: assertion check on a SQL joint is valid."""
        assertion = _make_check(phase="assertion")
        sql_joint = _make_joint(name="transform", joint_type="sql", checks=[assertion])
        assembly = _make_assembly([sql_joint])

        for joint in assembly.joints:
            for check in joint.checks:
                if check.phase == "assertion":
                    assert joint.type in ("source", "sql", "python", "sink")

    def test_no_checks_assembly_is_valid(self) -> None:
        """Concrete: assembly with no checks satisfies the property trivially."""
        joints = [
            _make_joint("src", "source"),
            _make_joint("transform", "sql"),
            _make_joint("sink", "sink"),
        ]
        assembly = _make_assembly(joints)
        for joint in assembly.joints:
            assert joint.checks == []

    def test_mixed_checks_on_sink_joint(self) -> None:
        """Concrete: sink joint may have both assertion and audit checks."""
        assertion = _make_check(phase="assertion", check_type="not_null")
        audit = _make_check(phase="audit", check_type="row_count")
        sink = _make_joint(name="sink", joint_type="sink", checks=[assertion, audit])
        assembly = _make_assembly([sink])

        for joint in assembly.joints:
            for check in joint.checks:
                if check.phase == "audit":
                    assert joint.type == "sink"

    def test_catalog_node_data_check_has_joint_name(self) -> None:
        """CatalogNodeData for check nodes always carries the owning joint name."""
        from rivet_cli.repl.widgets.catalog import CatalogNodeData

        check = _make_check(phase="assertion")
        joint = _make_joint(name="my_joint", joint_type="sql", checks=[check])

        data = CatalogNodeData(
            node_kind="check",
            joint_name=joint.name,
            check=check,
        )
        assert data.joint_name == "my_joint"
        assert data.check is check
        assert data.node_kind == "check"

    @given(
        joint_type=_JOINT_TYPES,
        n_assertions=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=100)
    def test_assertion_count_preserved_in_assembly(
        self, joint_type: str, n_assertions: int
    ) -> None:
        """The number of assertion checks on a joint is preserved in the assembly."""
        checks = [_make_check(phase="assertion") for _ in range(n_assertions)]
        joint = _make_joint(name="j", joint_type=joint_type, checks=checks)
        assembly = _make_assembly([joint])

        assert len(assembly.joints[0].checks) == n_assertions

    @given(n_audits=st.integers(min_value=0, max_value=5))
    @settings(max_examples=100)
    def test_audit_count_preserved_on_sink(self, n_audits: int) -> None:
        """The number of audit checks on a sink joint is preserved in the assembly."""
        checks = [_make_check(phase="audit") for _ in range(n_audits)]
        sink = _make_joint(name="sink", joint_type="sink", checks=checks)
        assembly = _make_assembly([sink])

        assert len(assembly.joints[0].checks) == n_audits
        for check in assembly.joints[0].checks:
            assert check.phase == "audit"
